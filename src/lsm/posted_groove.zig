const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

const TableType = @import("table.zig").TableType;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);

const snapshot_latest = @import("tree.zig").snapshot_latest;

/// This type wraps a single LSM tree in the API needed to integrate it with the Forest.
/// TigerBeetle's state machine requires a map from u128 ID to posted boolean for transfers
/// and this type implements that.
/// TODO Make the LSM Forest library flexible enough to be able to get rid of this special case.
pub fn PostedGrooveType(comptime Storage: type) type {
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

            // TODO(ifreund): disallow this id in the state machine.
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
        );

        const Tree = TreeType(Table, Storage, "groove");
        const Grid = GridType(Storage);

        const PrefetchIDs = std.AutoHashMapUnmanaged(u128, void);
        const PrefetchObjects = std.AutoHashMapUnmanaged(u128, bool);

        cache: *Tree.ValueCache,
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

        /// This field is necessary to expose the same open()/compact_cpu()/compact_io() function
        /// signatures as the real Groove type.
        callback: ?*const fn (*PostedGroove) void = null,

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            // The cache size is meant to be computed based on the left over available memory
            // that tigerbeetle was given to allocate from CLI arguments.
            cache_size: u32,
            // In general, the commit count max for a field, depends on the field's object,
            // how many objects might be changed by a batch:
            //   (config.message_size_max - sizeOf(vsr.header))
            // For example, there are at most 8191 transfers in a batch.
            // So commit_count_max=8191 for transfer objects and indexes.
            //
            // However, if a transfer is ever mutated, then this will double commit_count_max
            // since the old index might need to be removed, and the new index inserted.
            //
            // A way to see this is by looking at the state machine. If a transfer is inserted,
            // how many accounts and transfer put/removes will be generated?
            //
            // This also means looking at the state machine operation that will generate the
            // most put/removes in the worst case.
            // For example, create_accounts will put at most 8191 accounts.
            // However, create_transfers will put 2 accounts (8191 * 2) for every transfer, and
            // some of these accounts may exist, requiring a remove/put to update the index.
            commit_count_max: u32,
        ) !PostedGroove {
            // Cache is dynamically allocated to pass a pointer into the Object tree.
            const cache = try allocator.create(Tree.ValueCache);
            errdefer allocator.destroy(cache);

            cache.* = .{};
            try cache.ensureTotalCapacity(allocator, cache_size);
            errdefer cache.deinit(allocator);

            var tree = try Tree.init(
                allocator,
                node_pool,
                grid,
                cache,
                .{
                    .commit_count_max = commit_count_max,
                },
            );
            errdefer tree.deinit(allocator);

            // TODO: document why this is twice the commit count max.
            const prefetch_count_max = commit_count_max * 2;

            var prefetch_ids = PrefetchIDs{};
            try prefetch_ids.ensureTotalCapacity(allocator, prefetch_count_max);
            errdefer prefetch_ids.deinit(allocator);

            var prefetch_objects = PrefetchObjects{};
            try prefetch_objects.ensureTotalCapacity(allocator, prefetch_count_max);
            errdefer prefetch_objects.deinit(allocator);

            return PostedGroove{
                .cache = cache,
                .tree = tree,

                .prefetch_ids = prefetch_ids,
                .prefetch_objects = prefetch_objects,
            };
        }

        pub fn deinit(groove: *PostedGroove, allocator: mem.Allocator) void {
            assert(groove.callback == null);

            groove.tree.deinit(allocator);
            groove.cache.deinit(allocator);
            allocator.destroy(groove.cache);

            groove.prefetch_ids.deinit(allocator);
            groove.prefetch_objects.deinit(allocator);

            groove.* = undefined;
        }

        pub fn get(groove: *const PostedGroove, id: u128) ?bool {
            return groove.prefetch_objects.get(id);
        }

        /// Must be called directly after the state machine commit is finished and prefetch results
        /// are no longer needed.
        pub fn prefetch_clear(groove: *PostedGroove) void {
            groove.prefetch_objects.clearRetainingCapacity();
            assert(groove.prefetch_objects.count() == 0);
            assert(groove.prefetch_ids.count() == 0);
        }

        /// This must be called by the state machine for every key to be prefetched.
        /// We tolerate duplicate IDs enqueued by the state machine.
        /// For example, if all unique operations require the same two dependencies.
        pub fn prefetch_enqueue(groove: *PostedGroove, id: u128) void {
            if (groove.tree.get_cached(id)) |value| {
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
        /// in memory, either in the value cache of the object tree or in the prefetch_objects
        /// backup hash map.
        pub fn prefetch(
            groove: *PostedGroove,
            callback: *const fn (*PrefetchContext) void,
            context: *PrefetchContext,
        ) void {
            context.* = .{
                .groove = groove,
                .callback = callback,
                .id_iterator = groove.prefetch_ids.keyIterator(),
            };
            context.start_workers();
        }

        pub const PrefetchContext = struct {
            groove: *PostedGroove,
            callback: *const fn (*PrefetchContext) void,

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
                // This prevents `context.finish()` from being called within the loop body when every
                // worker finishes synchronously. `context.finish()` sets the `context` to undefined,
                // but `context` is required for the last loop condition check.
                context.workers_busy += 1;

                // -1 to ignore the extra worker.
                while (context.workers_busy - 1 < context.workers.len) {
                    const worker = &context.workers[context.workers_busy - 1];
                    worker.* = .{ .context = context };
                    context.workers_busy += 1;
                    if (!worker.lookup_start()) break;
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
                assert(context.groove.prefetch_ids.count() == 0);
                assert(context.id_iterator.next() == null);

                const callback = context.callback;
                context.* = undefined;
                callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            context: *PrefetchContext,
            lookup_id: Tree.LookupContext = undefined,

            /// Returns true if asynchronous I/O has been started.
            /// Returns false if there are no more IDs to prefetch.
            fn lookup_start(worker: *PrefetchWorker) bool {
                const groove = worker.context.groove;

                const id = worker.context.id_iterator.next() orelse {
                    groove.prefetch_ids.clearRetainingCapacity();
                    assert(groove.prefetch_ids.count() == 0);
                    worker.context.worker_finished();
                    return false;
                };

                if (config.verify) {
                    // This is checked in prefetch_enqueue()
                    assert(groove.tree.get_cached(id.*) == null);
                }

                // If not in the LSM tree's cache, the object must be read from disk and added
                // to the auxillary prefetch_objects hash map.
                // TODO: this LSM tree function needlessly checks the LSM tree's cache a
                // second time. Adding API to the LSM tree to avoid this may be worthwhile.
                groove.tree.lookup(
                    lookup_id_callback,
                    &worker.lookup_id,
                    snapshot_latest,
                    id.*,
                );

                return true;
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
                worker.lookup_finish();
            }

            fn lookup_finish(worker: *PrefetchWorker) void {
                if (!worker.lookup_start()) {
                    worker.* = undefined;
                }
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

        pub fn open(groove: *PostedGroove, callback: *const fn (*PostedGroove) void) void {
            assert(groove.callback == null);
            groove.callback = callback;
            groove.tree.open(tree_callback);
        }

        pub fn compact(groove: *PostedGroove, callback: *const fn (*PostedGroove) void, op: u64) void {
            assert(groove.callback == null);
            groove.callback = callback;
            groove.tree.compact(tree_callback, op);
        }

        pub fn checkpoint(groove: *PostedGroove, callback: *const fn (*PostedGroove) void) void {
            assert(groove.callback == null);
            groove.callback = callback;
            groove.tree.checkpoint(tree_callback);
        }
    };
}

test "PostedGroove" {
    const Storage = @import("../storage.zig").Storage;

    const PostedGroove = PostedGrooveType(Storage);

    _ = PostedGroove.init;
    _ = PostedGroove.deinit;

    _ = PostedGroove.get;
    _ = PostedGroove.put_no_clobber;
    _ = PostedGroove.remove;

    _ = PostedGroove.compact;
    _ = PostedGroove.checkpoint;

    _ = PostedGroove.prefetch_enqueue;
    _ = PostedGroove.prefetch;
    _ = PostedGroove.prefetch_clear;

    std.testing.refAllDecls(PostedGroove.PrefetchWorker);
    std.testing.refAllDecls(PostedGroove.PrefetchContext);
}
