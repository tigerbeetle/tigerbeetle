const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const constants = @import("../constants.zig");

const TableType = @import("table.zig").TableType;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);

const snapshot_latest = @import("tree.zig").snapshot_latest;
const compaction_snapshot_for_op = @import("tree.zig").compaction_snapshot_for_op;

/// This type wraps a single LSM tree in the API needed to integrate it with the Forest.
/// TigerBeetle's state machine requires a map from u128 ID to posted boolean for transfers
/// and this type implements that.
/// TODO Make the LSM Forest library flexible enough to be able to get rid of this special case.
pub fn PostedGrooveType(comptime Storage: type, value_count_max: usize) type {
    return struct {
        const PostedGroove = @This();

        const Value = extern struct {
            id: u128,
            data: enum(u8) {
                posted,
                voided,
                tombstone,
            },
            padding: [15]u8 = [_]u8{0} ** 15,

            comptime {
                // Assert that there is no implicit padding.
                assert(@sizeOf(Value) == 32);
                assert(@bitSizeOf(Value) == 32 * 8);
            }

            inline fn compare_keys(a: u128, b: u128) math.Order {
                return math.order(a, b);
            }

            inline fn key_from_value(value: *const Value) u128 {
                return value.id;
            }

            const sentinel_key = math.maxInt(u128);

            inline fn tombstone(value: *const Value) bool {
                return value.data == .tombstone;
            }

            inline fn tombstone_from_key(id: u128) Value {
                return .{
                    .id = id,
                    .data = .tombstone,
                };
            }
        };

        const Table = TableType(
            u128,
            Value,
            Value.compare_keys,
            Value.key_from_value,
            Value.sentinel_key,
            Value.tombstone,
            Value.tombstone_from_key,
            value_count_max,
            .general,
        );

        const Tree = TreeType(Table, Storage, "posted_groove");
        const Grid = GridType(Storage);

        const PrefetchIDs = std.AutoHashMapUnmanaged(u128, void);
        const PrefetchObjects = std.AutoHashMapUnmanaged(u128, bool); // true:posted, false:voided

        tree: Tree,

        /// Object IDs enqueued to be prefetched.
        /// Prefetching ensures that point lookups against the latest snapshot are synchronous.
        /// This shields state machine implementations from the challenges of concurrency and I/O,
        /// and enables simple state machine function signatures that commit writes atomically.
        prefetch_ids: PrefetchIDs,

        /// The prefetched Objects. This hash map holds the subset of objects in the LSM tree
        /// that are required for the current commit. All get()/put()/remove() operations during
        /// the commit are both passed to the LSM trees and mirrored in this hash map. It is always
        /// sufficient to query this hashmap alone to know the state of the LSM trees.
        prefetch_objects: PrefetchObjects,

        /// The snapshot to prefetch from.
        prefetch_snapshot: ?u64,

        /// This field is necessary to expose the same open()/compact()/checkpoint() function
        /// signatures as the real Groove type.
        callback: ?fn (*PostedGroove) void = null,

        /// See comments for Groove.Options.
        pub const Options = struct {
            cache_entries_max: u32,
            prefetch_entries_max: u32,
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            options: Options,
        ) !PostedGroove {
            var tree = try Tree.init(
                allocator,
                node_pool,
                grid,
                .{
                    .cache_entries_max = options.cache_entries_max,
                },
            );
            errdefer tree.deinit(allocator);

            var prefetch_ids = PrefetchIDs{};
            try prefetch_ids.ensureTotalCapacity(allocator, options.prefetch_entries_max);
            errdefer prefetch_ids.deinit(allocator);

            var prefetch_objects = PrefetchObjects{};
            try prefetch_objects.ensureTotalCapacity(allocator, options.prefetch_entries_max);
            errdefer prefetch_objects.deinit(allocator);

            return PostedGroove{
                .tree = tree,

                .prefetch_ids = prefetch_ids,
                .prefetch_objects = prefetch_objects,
                .prefetch_snapshot = null,
            };
        }

        pub fn deinit(groove: *PostedGroove, allocator: mem.Allocator) void {
            groove.tree.deinit(allocator);

            groove.prefetch_ids.deinit(allocator);
            groove.prefetch_objects.deinit(allocator);

            groove.* = undefined;
        }

        pub fn get(groove: *const PostedGroove, id: u128) ?bool {
            return groove.prefetch_objects.get(id);
        }

        /// Must be called directly before the state machine begins queuing ids for prefetch.
        /// When `snapshot` is null, prefetch from the current snapshot.
        pub fn prefetch_setup(groove: *PostedGroove, snapshot: ?u64) void {
            // We may query the input tables of an ongoing compaction, but must not query the
            // output tables until the compaction is complete. (Until then, the output tables may
            // be in the manifest but not yet on disk).
            const snapshot_max = groove.tree.lookup_snapshot_max;
            const snapshot_target = snapshot orelse snapshot_max;
            assert(snapshot_target <= snapshot_max);

            if (groove.prefetch_snapshot == null) {
                groove.prefetch_objects.clearRetainingCapacity();
            } else {
                // If there is a snapshot already set from the previous prefetch_setup(), then its
                // prefetch() was never called, so there must already be no queued objects or ids.
            }

            groove.prefetch_snapshot = snapshot_target;
            assert(groove.prefetch_objects.count() == 0);
            assert(groove.prefetch_ids.count() == 0);
        }

        /// This must be called by the state machine for every key to be prefetched.
        /// We tolerate duplicate IDs enqueued by the state machine.
        /// For example, if all unique operations require the same two dependencies.
        pub fn prefetch_enqueue(groove: *PostedGroove, id: u128) void {
            if (groove.tree.lookup_from_memory(groove.prefetch_snapshot.?, id)) |value| {
                assert(value.id == id);
                switch (value.data) {
                    .posted => groove.prefetch_objects.putAssumeCapacity(value.id, true),
                    .voided => groove.prefetch_objects.putAssumeCapacity(value.id, false),
                    .tombstone => {}, // Leave the ID out of prefetch_objects.
                }
            } else {
                groove.prefetch_ids.putAssumeCapacity(id, {});
            }
        }

        /// Ensure the objects corresponding to all ids enqueued with prefetch_enqueue() are
        /// available in `prefetch_objects`.
        pub fn prefetch(
            groove: *PostedGroove,
            callback: fn (*PrefetchContext) void,
            context: *PrefetchContext,
        ) void {
            context.* = .{
                .groove = groove,
                .callback = callback,
                .snapshot = groove.prefetch_snapshot.?,
                .id_iterator = groove.prefetch_ids.keyIterator(),
            };
            groove.prefetch_snapshot = null;
            context.start_workers();
        }

        pub const PrefetchContext = struct {
            groove: *PostedGroove,
            callback: fn (*PrefetchContext) void,
            snapshot: u64,

            id_iterator: PrefetchIDs.KeyIterator,

            /// The goal is to fully utilize the disk I/O to ensure the prefetch completes as
            /// quickly as possible, so we run multiple lookups in parallel based on the max
            /// I/O depth of the Grid.
            workers: [Grid.read_iops_max]PrefetchWorker = undefined,
            /// The number of workers that are currently running in parallel.
            workers_busy: u32 = 0,

            fn start_workers(context: *PrefetchContext) void {
                assert(context.workers_busy == 0);

                // Track an extra "worker" that will finish after the loop.
                //
                // This prevents `context.finish()` from being called within the loop body when
                // every worker finishes synchronously. `context.finish()` calls the user-provided
                // callback which may re-use the memory of this `PrefetchContext`. However, we
                // rely on `context` being well-defined for the loop condition.
                context.workers_busy += 1;

                for (context.workers) |*worker| {
                    worker.* = .{ .context = context };
                    context.workers_busy += 1;
                    worker.lookup_start_next();
                }

                assert(context.workers_busy >= 1);
                context.worker_finished();
            }

            fn worker_finished(context: *PrefetchContext) void {
                context.workers_busy -= 1;
                if (context.workers_busy == 0) context.finish();
            }

            fn finish(context: *PrefetchContext) void {
                assert(context.workers_busy == 0);

                assert(context.id_iterator.next() == null);
                context.groove.prefetch_ids.clearRetainingCapacity();
                assert(context.groove.prefetch_ids.count() == 0);

                context.callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            context: *PrefetchContext,
            lookup_id: Tree.LookupContext = undefined,

            /// Returns true if asynchronous I/O has been started.
            /// Returns false if there are no more IDs to prefetch.
            fn lookup_start_next(worker: *PrefetchWorker) void {
                const id = worker.context.id_iterator.next() orelse {
                    worker.context.worker_finished();
                    return;
                };

                if (constants.verify) {
                    // This was checked in prefetch_enqueue().
                    assert(worker.context.groove.tree.lookup_from_memory(worker.context.snapshot, id.*) == null);
                }

                // If not in the LSM tree's cache, the object must be read from disk and added
                // to the auxillary prefetch_objects hash map.
                // TODO: this LSM tree function needlessly checks the LSM tree's cache a
                // second time. Adding API to the LSM tree to avoid this may be worthwhile.
                worker.context.groove.tree.lookup_from_levels(
                    lookup_id_callback,
                    &worker.lookup_id,
                    worker.context.snapshot,
                    id.*,
                );
            }

            fn lookup_id_callback(
                completion: *Tree.LookupContext,
                result: ?*const Value,
            ) void {
                const worker = @fieldParentPtr(PrefetchWorker, "lookup_id", completion);
                const groove = worker.context.groove;

                if (result) |value| {
                    switch (value.data) {
                        .posted => {
                            groove.prefetch_objects.putAssumeCapacityNoClobber(value.id, true);
                        },
                        .voided => {
                            groove.prefetch_objects.putAssumeCapacityNoClobber(value.id, false);
                        },
                        .tombstone => {
                            // Leave the ID out of prefetch_objects.
                        },
                    }
                }
                worker.lookup_start_next();
            }
        };

        pub fn put_no_clobber(groove: *PostedGroove, id: u128, posted: bool) void {
            const gop = groove.prefetch_objects.getOrPutAssumeCapacity(id);
            assert(!gop.found_existing);

            const value: Value = .{
                .id = id,
                .data = if (posted) .posted else .voided,
            };
            groove.tree.put(&value);
            gop.value_ptr.* = posted;
        }

        pub fn remove(groove: *PostedGroove, id: u128) void {
            assert(groove.prefetch_objects.remove(id));
            groove.tree.remove(&Value{ .id = id, .data = .tombstone });
        }

        fn tree_callback(tree: *Tree) void {
            const groove = @fieldParentPtr(PostedGroove, "tree", tree);
            const callback = groove.callback.?;
            groove.callback = null;
            callback(groove);
        }

        pub fn open(groove: *PostedGroove, callback: fn (*PostedGroove) void) void {
            assert(groove.callback == null);
            groove.callback = callback;
            groove.tree.open(tree_callback);
        }

        pub fn compact(
            groove: *PostedGroove,
            callback: fn (*anyopaque) void,
            context: *anyopaque,
            op: u64,
        ) void {
            groove.tree.compact(callback, context, op);
        }

        pub fn op_done(groove: *PostedGroove, op: u64) void {
            assert(groove.callback == null);
            groove.tree.op_done(op);
        }

        pub fn checkpoint(groove: *PostedGroove, callback: fn (*PostedGroove) void, op: u64) void {
            assert(groove.callback == null);
            groove.callback = callback;
            groove.tree.checkpoint(tree_callback, op);
        }
    };
}

test "PostedGroove" {
    const Storage = @import("../storage.zig").Storage;

    const PostedGroove = PostedGrooveType(
        Storage,
        // Doesn't matter for this test.
        1,
    );

    _ = PostedGroove.init;
    _ = PostedGroove.deinit;

    _ = PostedGroove.get;
    _ = PostedGroove.put_no_clobber;
    _ = PostedGroove.remove;

    _ = PostedGroove.compact;
    _ = PostedGroove.checkpoint;

    _ = PostedGroove.prefetch_enqueue;
    _ = PostedGroove.prefetch;
    _ = PostedGroove.prefetch_setup;

    std.testing.refAllDecls(PostedGroove.PrefetchWorker);
    std.testing.refAllDecls(PostedGroove.PrefetchContext);
}
