//! Memory tables for an LSM (log‑structured merge) stage.
//!
//! Each `tree.zig` maintains two in‑memory tables:
//! - Mutable table: accepts all updates (e.g., `Tree.put`).
//! - Immutable table: sorted read‑only staging area for the next flush to disk.
//!
//! New puts are appended to the mutable table. When the mutable table fills up
//! — or rather when we reach a bar boundary - the mutable table is compacted into
//! the immutable table. Compaction sorts and deduplicates entries and prepares them for the LSM
//! `compaction.zig` to be flushed to disk.
//!
//! Optimizations
//! 1) Sorted runs on every beat:
//!    - At the end of each beat, the mutable table’s suffix is sorted and
//!      deduplicated, and the suffix offset advances.
//!    - By the end of a bar, the mutable table is a sequence of sorted arrays,
//!      which are then merged and deduplicated.
//!
//! 2) Deferred disk flush:
//!    - If the immutable table is not sufficiently full, `compaction.zig` may
//!      choose not to flush to disk. Instead, we retain the current immutable
//!      table and absorb the mutable table into it.

const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const stdx = @import("stdx");
const maybe = stdx.maybe;

const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;
const ScratchMemory = @import("scratch_memory.zig").ScratchMemory;
const Pending = error{Pending};

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        values: []Value,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        // Maintains per-table mutable state that must be snapshotted for “scopes”.
        // When a scope is opened (e.g., in `tree.zig`), we copy `ValueContext` so we can
        // roll back both the count and the sorted-run tracker if the scope is discarded.
        pub const ValueContext = struct {
            count: u32 = 0,
            run_tracker: SortedRunTracker,
        };

        const Mutability = union(enum) {
            mutable: struct {
                // This buffer is shared between all `Tables` for the radix sort.
                // It is passed down from the forest.
                radix_buffer: *ScratchMemory,
            },
            immutable: struct {
                // An empty table has nothing to flush.
                flushed: bool = true,
                // This field is only used for assertions, to verify that we don't absorb the
                // mutable table immediately prior to checkpoint.
                absorbed: bool = false,
                snapshot_min: u64 = 0,
            },
        };

        // Where the sorted run originated; affects merge precedence on equal keys.
        // We prefer the immutable ones to keep ordering for deduplication (last key wins).
        const RunOrigin = enum { mutable, immutable };

        // A contiguous sorted range in `values[min..max)`.
        const SortedRun = struct {
            min: u32,
            max: u32, // exclusive.
            origin: RunOrigin, // Where the run originated; affects merge precedence on equal keys.
        };

        // At most: LSM compactions + one sort() call + one immutable run for `absorb`.
        const sorted_runs_max = constants.lsm_compaction_ops + 2;

        // Merge context for the k-way iterator across all runs in the tracker.
        const MergeContext = struct {
            streams: [sorted_runs_max][]const Value,
            streams_count: u32,

            fn stream_peek(
                context: *const MergeContext,
                stream_index: u32,
            ) Pending!?Key {
                // TODO: Enable the asserts once `constants.verify` is disabled on release.
                //assert(stream_index < context.streams_count);
                const stream = context.streams[stream_index];
                if (stream.len == 0) return null;
                return key_from_value(&stream[0]);
            }

            fn stream_pop(context: *MergeContext, stream_index: u32) Value {
                // TODO: Enable the asserts once `constants.verify` is disabled on release.
                //assert(stream_index < context.streams_count);
                const stream = context.streams[stream_index];
                context.streams[stream_index] = stream[1..];
                return stream[0];
            }
        };

        const KWayMergeIterator = KWayMergeIteratorType(
            MergeContext,
            Key,
            Value,
            .{
                .streams_max = sorted_runs_max,
                .deduplicate = false,
            },
            key_from_value,
            MergeContext.stream_peek,
            MergeContext.stream_pop,
        );

        const SortedRunTracker = struct {
            /// Invariants:
            /// - Runs are in ascending order.
            /// - Runs have no gaps between them.
            /// - There is at most one run with run.origin=.immutable
            runs: [sorted_runs_max]SortedRun,
            runs_count: u8,

            fn init() SortedRunTracker {
                return .{
                    .runs = undefined,
                    .runs_count = 0,
                };
            }

            fn add(tracker: *SortedRunTracker, run: SortedRun) void {
                if (run.min == run.max) return; // Ignore empty runs.

                tracker.runs[tracker.runs_count] = run;
                tracker.runs_count += 1;
            }

            // Adds a new run at the front and shifts the other runs by the offset to maintain
            // the invariant that no run overlaps and they have no gaps.
            fn add_front_and_propagate_offset(tracker: *SortedRunTracker, run: SortedRun) void {
                if (run.min == run.max) return; // Ignore empty runs.
                assert(tracker.runs_count + 1 <= tracker.runs.len);
                stdx.copy_right(
                    .exact,
                    SortedRun,
                    tracker.runs[1 .. tracker.runs_count + 1],
                    tracker.runs[0..tracker.runs_count],
                );

                tracker.runs[0] = run;
                tracker.runs_count += 1;

                //Propagate the new offset to the remaining runs.
                for (tracker.runs[1..tracker.count()]) |*run_old| {
                    run_old.min += run.max;
                    run_old.max += run.max;
                }
            }

            fn reset(tracker: *SortedRunTracker) void {
                tracker.* = .{
                    .runs = undefined,
                    .runs_count = 0,
                };
            }

            fn count(tracker: *const SortedRunTracker) u32 {
                return tracker.runs_count;
            }

            fn merge_context(tracker: *const SortedRunTracker, values: []const Value) MergeContext {
                var context = MergeContext{
                    .streams = undefined,
                    .streams_count = undefined,
                };

                var stream_idx: u32 = 0;

                // Place the immutable run first so smaller stream_id wins on ties.
                for (tracker.runs[0..tracker.count()]) |run| {
                    if (run.origin != .immutable) continue;
                    context.streams[stream_idx] = values[run.min..run.max];
                    stream_idx += 1;
                    break;
                }
                // Now place all the mutable runs.
                for (tracker.runs[0..tracker.count()]) |run| {
                    if (run.origin == .immutable) continue;
                    context.streams[stream_idx] = values[run.min..run.max];
                    stream_idx += 1;
                }
                context.streams_count = stream_idx;
                return context;
            }

            fn last(tracker: *const SortedRunTracker) ?*const SortedRun {
                if (tracker.count() == 0) return null;
                return &tracker.runs[tracker.count() - 1];
            }

            fn assert_invariants(tracker: *const SortedRunTracker, table_count: u32) void {
                const runs_count = tracker.count();

                if (runs_count == 0) return;

                assert(tracker.runs[0].min == 0);
                assert(tracker.runs[runs_count - 1].max == table_count);

                for (tracker.runs[0 .. runs_count - 1], tracker.runs[1..runs_count]) |a, b| {
                    assert(a.min < b.min); // Ordered and we ignore empty runs.
                    assert(a.max == b.min); // No gaps.
                }

                var immutable_runs: u1 = 0;
                for (tracker.runs[0..runs_count]) |run| {
                    immutable_runs += @intFromBool(run.origin == .immutable);
                }
                assert(immutable_runs == 0 or immutable_runs == 1);
            }
        };

        // Merge values with identical keys (last one wins) and collapse tombstones for
        // secondary indexes in a streaming fashion.
        const DedupSink = struct {
            out: []Value,
            target_index: u32 = 0,

            // Holds the current candidate that may merge with the next item.
            pending: ?Value = null,

            pub fn init(out: []Value) DedupSink {
                return .{ .out = out };
            }

            // Streamed equivalent of `deduplicate`.
            pub fn push(self: *DedupSink, value: Value) void {
                const pending = self.pending orelse {
                    // Starting a new run with a pending `value`.
                    self.pending = value;
                    return;
                };

                // If we're at the end of the source, there is no next value, so the next value
                // can't be equal.
                if (key_from_value(&pending) == key_from_value(&value)) {
                    if (Table.usage == .secondary_index) {
                        // Secondary index optimization --- cancel out put and remove.
                        // NB: while this prevents redundant tombstones from getting to disk, we
                        // still spend some extra CPU work to sort the entries in memory. Ideally,
                        // we annihilate tombstones immediately, before sorting, but that's tricky
                        // to do with scopes.
                        assert(Table.tombstone(&pending) != Table.tombstone(&value));
                        // Effect: consume both and produce nothing for this key.
                        self.pending = null; // drop both
                    } else {
                        // The last value in a run of duplicates needs to be the one that ends up in
                        // target.
                        // Effect: keep the slot, overwrite winner with the newer value.
                        self.pending = value; // last wins
                    }
                } else {
                    // New key encountered: flush previous winner, start a new run.
                    self.out[self.target_index] = pending;
                    self.target_index += 1;
                    self.pending = value;
                }
            }

            pub fn finish(self: *DedupSink) u32 {
                // Flush the final pending value, if any.
                if (self.pending) |p| {
                    self.out[self.target_index] = p;
                    self.target_index += 1;
                    self.pending = null;
                }

                // At this point, target_index is the number of deduplicated items written.
                if (constants.verify) {
                    if (0 < self.target_index) {
                        for (
                            self.out[0 .. self.target_index - 1],
                            self.out[1..self.target_index],
                        ) |*value, *value_next| {
                            assert(key_from_value(value) < key_from_value(value_next));
                        }
                    }
                }

                return self.target_index;
            }
        };

        pub fn init(
            table: *TableMemory,
            allocator: mem.Allocator,
            radix_buffer: *ScratchMemory,
            mutability: std.meta.Tag(Mutability),
            name: []const u8,
            options: struct {
                value_count_limit: u32,
            },
        ) !void {
            assert(options.value_count_limit <= Table.value_count_max);
            assert(radix_buffer.state == .free);

            table.* = .{
                .value_context = .{
                    .run_tracker = SortedRunTracker.init(),
                },
                .mutability = switch (mutability) {
                    .mutable => .{ .mutable = .{
                        .radix_buffer = radix_buffer,
                    } },
                    .immutable => .{ .immutable = .{} },
                },
                .name = name,
                .values = undefined,
            };

            // TODO This would ideally be value_count_limit, but needs to be value_count_max to
            // ensure that memory table coalescing is deterministic even if the batch limit changes.
            table.values = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.values);
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            allocator.free(table.values);
        }

        pub fn reset(table: *TableMemory) void {
            const mutability: Mutability = switch (table.mutability) {
                .immutable => .{ .immutable = .{} },
                .mutable => .{ .mutable = .{
                    .radix_buffer = table.mutability.mutable.radix_buffer,
                } },
            };

            table.value_context.run_tracker.reset();

            table.* = .{
                .values = table.values,
                .value_context = .{
                    .run_tracker = table.value_context.run_tracker,
                },
                .mutability = mutability,
                .name = table.name,
            };
        }

        pub fn count(table: *const TableMemory) u32 {
            return table.value_context.count;
        }

        pub fn values_used(table: *const TableMemory) []Value {
            return table.values[0..table.count()];
        }

        // Appends a `value`. If it is strictly greater than the previous key,
        // expand the last run by 1; otherwise the suffix will be sorted later.
        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.count() < table.values.len);

            const run_count = table.value_context.run_tracker.count();
            if (run_count > 0 and
                table.value_context.run_tracker.runs[run_count - 1].max == table.count())
            {
                const expand: bool = table.count() == 0 or
                    key_from_value(&table.values[table.count() - 1]) <
                        key_from_value(value);
                table.value_context.run_tracker.runs[run_count - 1].max += @intFromBool(expand);
            }

            table.values[table.count()] = value.*;
            table.value_context.count += 1;
        }

        /// This must be called on sorted tables (single run from 0..count).
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            assert(table.count() <= table.values.len);
            assert(table.sorted());

            if (!table.key_range_contains(key)) {
                return null;
            }

            return binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                table.values_used(),
                key,
                .{ .mode = .upper_bound },
            );
        }

        fn finalize(table_immutable: *TableMemory, snapshot_min: u64) void {
            assert(table_immutable.mutability == .immutable);

            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.count() == 0,
                .snapshot_min = snapshot_min,
            } };
            table_immutable.value_context.run_tracker.reset();
            table_immutable.value_context.run_tracker.add(.{
                .min = 0,
                .max = table_immutable.count(),
                .origin = .immutable,
            });
        }

        // Merge `table_mutable` runs into `table_immutable`.
        pub fn compact(
            table_immutable: *TableMemory,
            table_mutable: *TableMemory,
            snapshot_min: u64,
        ) void {
            assert(table_immutable.mutability == .immutable);
            maybe(table_immutable.mutability.immutable.absorbed);
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.sorted());
            defer assert(table_immutable.sorted());
            defer assert(table_mutable.count() == 0);

            table_mutable.value_context.run_tracker.assert_invariants(table_mutable.count());

            if (table_mutable.sorted()) {
                // Fast-path: single contiguous sorted run: swap buffers.
                assert(table_mutable.values.len == table_immutable.values.len);
                std.mem.swap([]Value, &table_mutable.values, &table_immutable.values);

                table_immutable.value_context.count = table_mutable.count();
            } else {
                var merge_context = table_mutable.value_context
                    .run_tracker.merge_context(table_mutable.values_used());
                var merge_iterator = KWayMergeIterator.init(
                    &merge_context,
                    @intCast(merge_context.streams_count),
                    .ascending,
                );

                // Deduplicate values in streaming fashion.
                var dedup_sink = DedupSink.init(table_immutable.values);
                while (merge_iterator.pop() catch unreachable) |value| {
                    dedup_sink.push(value);
                }
                table_immutable.value_context.count = dedup_sink.finish();
            }

            table_mutable.reset();
            table_immutable.finalize(snapshot_min);
            assert(table_immutable.sorted());
        }

        // Absorb the current immutable table into the mutable one,
        // then re-materialize a compact immutable table.
        pub fn absorb(
            table_immutable: *TableMemory,
            table_mutable: *TableMemory,
            snapshot_min: u64,
        ) void {
            assert(table_immutable.mutability == .immutable);
            maybe(table_immutable.mutability.immutable.absorbed);
            assert(table_immutable.sorted());
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.sorted());
            defer assert(table_immutable.sorted());

            const values_count_limit = table_immutable.values.len;
            assert(table_immutable.count() <= values_count_limit);
            assert(table_mutable.count() <= values_count_limit);
            assert(table_immutable.count() + table_mutable.count() <= values_count_limit);

            if (table_mutable.count() == 0) return;

            // Copy immutable after the current mutable tail and mark as an immutable run,
            // so the merge prefers its entries on key ties.
            const tables_combined_count = table_immutable.count() + table_mutable.count();

            // Because `table_mutable` is likely to be smaller then `tabel_immutable` we:
            // 1. Copy the values from `table_mutable` into `table_immutable`.
            // 2. We swap the backing arrays so that `table_mutable` has all the values.
            // 3. We add the new run from `table_immutable` at the beginning.
            stdx.copy_disjoint(
                .inexact,
                Value,
                table_immutable.values[table_immutable.count()..],
                table_mutable.values[0..table_mutable.count()],
            );
            std.mem.swap([]Value, &table_mutable.values, &table_immutable.values);

            table_mutable.value_context.run_tracker.add_front_and_propagate_offset(.{
                .min = 0,
                .max = table_immutable.count(),
                .origin = .immutable,
            });

            table_mutable.value_context.count = tables_combined_count;
            table_mutable.value_context.run_tracker.assert_invariants(table_mutable.count());

            table_immutable.mutability.immutable.absorbed = true;
            table_immutable.compact(table_mutable, snapshot_min);

            // One fully sorted run or all keys are annihilated.
            assert(table_immutable.value_context.run_tracker.count() <= 1);
            assert(table_mutable.value_context.run_tracker.count() == 0);
        }

        // Fully sort the table if needed. Produces a single run [0..count).
        pub fn sort(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            defer table.value_context.run_tracker.assert_invariants(table.count());

            if (!table.sorted()) {
                _ = table.mutable_sort_suffix_from_offset(0);
                table.value_context.run_tracker.reset();
                table.value_context.run_tracker.add(.{
                    .min = 0,
                    .max = table.count(),
                    .origin = .mutable,
                });
            }
        }

        // When true, `values` is strictly ascending-ordered (no duplicates).
        fn sorted(table: *const TableMemory) bool {
            // Empty table is considered sorted.
            if (table.count() == 0) return true;

            // Only one sorted run can exist if it is sorted.
            if (table.value_context.run_tracker.count() != 1) return false;

            const last_run = table.value_context.run_tracker.last().?;
            assert(last_run.min == 0);
            assert(last_run.max <= table.count());

            return table.count() == last_run.max;
        }

        pub fn sort_suffix(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            defer table.value_context.run_tracker.assert_invariants(table.count());

            if (table.sorted()) return;

            const sort_suffix_offset = if (table.value_context.run_tracker.last()) |last_run|
                last_run.max
            else
                0;

            assert(sort_suffix_offset <= table.count());

            if (sort_suffix_offset == table.count()) return;

            const run = table.mutable_sort_suffix_from_offset(sort_suffix_offset);
            assert(run.min <= run.max);
            assert(run.max == table.count());
            assert(sort_suffix_offset <= run.max);
            table.value_context.run_tracker.add(run);
        }

        fn mutable_sort_suffix_from_offset(table: *TableMemory, offset: u32) SortedRun {
            assert(table.mutability == .mutable);
            assert(offset == 0 or offset == table.value_context.run_tracker.last().?.max);
            assert(offset <= table.count());

            const radix_buffer_values = table.mutability.mutable.radix_buffer.acquire(
                Value,
                table.count(),
            );
            defer table.mutability.mutable.radix_buffer.release(Value, radix_buffer_values);

            const target_count = sort_suffix_from_offset(
                table.values_used(),
                radix_buffer_values,
                offset,
            );
            table.value_context.count = target_count;
            return .{ .min = offset, .max = target_count, .origin = .mutable };
        }

        // Returns the new length of `values`. Values are deduplicated after sorting, so the
        // returned count may be less than or equal to the original `values.len`.
        fn sort_suffix_from_offset(values: []Value, values_scratch: []Value, offset: u32) u32 {
            assert(values.len == values_scratch.len);
            assert(offset <= values.len);

            stdx.radix_sort(Key, Value, key_from_value, values[offset..], values_scratch[offset..]);

            // Deduplicate values in streaming fashion.
            var dedup_sink = DedupSink.init(values[offset..]);
            for (values[offset..]) |value| {
                dedup_sink.push(value);
            }
            const target_count = offset + dedup_sink.finish();

            return target_count;
        }

        pub fn key_range_contains(table: *const TableMemory, key: Key) bool {
            assert(table.sorted());

            if (table.count() == 0) return false;
            return table.key_min() <= key and key <= table.key_max();
        }

        pub fn key_min(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.sorted());

            return key_from_value(&values[0]);
        }

        pub fn key_max(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.sorted());

            return key_from_value(&values[values.len - 1]);
        }
    };
}

const TestHelper = struct {
    pub const TableUsage = enum {
        general,
        secondary_index,
    };

    fn TestTableType(comptime mode: TableUsage) type {
        return struct {
            const Key = u32;
            const Value = struct { key: Key, version: u32, tombstone: bool };
            const value_count_max = 16;
            const usage = mode;

            pub inline fn key_from_value(v: *const Value) Key {
                return v.key;
            }
            pub fn tombstone(v: *const Value) bool {
                return v.tombstone;
            }
        };
    }

    fn create_table_immutable(
        comptime TableType: type,
        gpa: std.mem.Allocator,
        value_count_limit: u32,
        radix_buffer: *ScratchMemory,
    ) !TableType {
        var table_immutable: TableType = undefined;
        try table_immutable.init(
            gpa,
            radix_buffer,
            .immutable,
            "immutable",
            .{ .value_count_limit = value_count_limit },
        );
        return table_immutable;
    }

    fn create_table_mutable(
        comptime TableType: type,
        gpa: std.mem.Allocator,
        value_count_limit: u32,
        radix_buffer: *ScratchMemory,
    ) !TableType {
        var table_mutable: TableType = undefined;
        try table_mutable.init(
            gpa,
            radix_buffer,
            .mutable,
            "mutable",
            .{ .value_count_limit = value_count_limit },
        );
        return table_mutable;
    }
};

test "table_memory: merge and absorb (last wins across streams)" {
    const testing = std.testing;
    const Snap = stdx.Snap;
    const snap = Snap.snap_fn("src");

    const Table = TestHelper.TestTableType(.general);
    const Value = Table.Value;
    const TableMemory = TableMemoryType(Table);

    const alloc = testing.allocator;

    var radix_buffer: ScratchMemory = try .init(alloc, Table.value_count_max * @sizeOf(Value));
    defer radix_buffer.deinit(alloc);

    var table_immutable: TableMemory = try TestHelper.create_table_immutable(
        TableMemory,
        alloc,
        Table.value_count_max,
        &radix_buffer,
    );
    defer table_immutable.deinit(alloc);

    var table_mutable: TableMemory = try TestHelper.create_table_mutable(
        TableMemory,
        alloc,
        Table.value_count_max,
        &radix_buffer,
    );
    defer table_mutable.deinit(alloc);

    table_mutable.put(&Value{ .key = 2, .version = 0, .tombstone = false });
    table_mutable.put(&Value{ .key = 4, .version = 0, .tombstone = false });
    table_mutable.sort();

    table_immutable.compact(&table_mutable, 0);
    assert(table_immutable.sorted());
    assert(table_mutable.count() == 0);

    table_mutable.put(&Value{ .key = 2, .version = 1, .tombstone = false });
    table_mutable.put(&Value{ .key = 5, .version = 0, .tombstone = false });
    table_mutable.sort();

    table_immutable.absorb(&table_mutable, 0);

    assert(table_immutable.sorted());
    assert(table_mutable.count() == 0);
    assert(table_immutable.count() == 3);

    var keys: [3]struct { Table.Key, u32 } = undefined;

    for (table_immutable.values_used(), 0..) |value, i| {
        keys[i] = .{ value.key, value.version };
    }

    try snap(@src(),
        \\{ { 2, 1 }, { 4, 0 }, { 5, 0 } }
    ).diff_fmt("{any}", .{keys});
}

test "table_memory: compact and deduplicate across runs" {
    const testing = std.testing;
    const Snap = stdx.Snap;
    const snap = Snap.snap_fn("src");

    const Table = TestHelper.TestTableType(.general);
    const Value = Table.Value;
    const TableMemory = TableMemoryType(Table);

    const alloc = testing.allocator;

    var radix_buffer: ScratchMemory = try .init(alloc, Table.value_count_max * @sizeOf(Value));
    defer radix_buffer.deinit(alloc);

    var table_immutable: TableMemory = try TestHelper.create_table_immutable(
        TableMemory,
        alloc,
        Table.value_count_max,
        &radix_buffer,
    );
    defer table_immutable.deinit(alloc);

    var table_mutable: TableMemory = try TestHelper.create_table_mutable(
        TableMemory,
        alloc,
        Table.value_count_max,
        &radix_buffer,
    );
    defer table_mutable.deinit(alloc);

    table_mutable.put(&Value{ .key = 2, .version = 0, .tombstone = false });
    table_mutable.put(&Value{ .key = 2, .version = 1, .tombstone = false });
    table_mutable.sort_suffix();

    table_mutable.put(&Value{ .key = 2, .version = 2, .tombstone = false });
    table_mutable.put(&Value{ .key = 2, .version = 3, .tombstone = false });
    table_mutable.sort_suffix();

    table_immutable.compact(&table_mutable, 0);
    assert(table_immutable.sorted());
    assert(table_mutable.count() == 0);
    assert(table_immutable.count() == 1);

    var keys: [1]struct { Table.Key, u32 } = undefined;

    for (table_immutable.values_used(), 0..) |value, i| {
        keys[i] = .{ value.key, value.version };
    }

    try snap(@src(),
        \\{ { 2, 3 } }
    ).diff_fmt("{any}", .{keys});
}

test "table_memory (secondary): annhiliation yields zero after deduplicate" {
    const testing = std.testing;

    const Table = TestHelper.TestTableType(.secondary_index);
    const Value = Table.Value;
    const TableMemory = TableMemoryType(Table);

    const alloc = testing.allocator;

    var radix_buffer: ScratchMemory = try .init(alloc, Table.value_count_max * @sizeOf(Value));
    defer radix_buffer.deinit(alloc);

    var table_immutable: TableMemory = try TestHelper.create_table_immutable(
        TableMemory,
        alloc,
        Table.value_count_max,
        &radix_buffer,
    );
    defer table_immutable.deinit(alloc);

    var table_mutable: TableMemory = try TestHelper.create_table_mutable(
        TableMemory,
        alloc,
        Table.value_count_max,
        &radix_buffer,
    );
    defer table_mutable.deinit(alloc);

    table_mutable.put(&Value{ .key = 2, .version = 0, .tombstone = false });
    table_mutable.put(&Value{ .key = 2, .version = 0, .tombstone = true });
    table_mutable.sort_suffix();

    table_immutable.compact(&table_mutable, 0);
    assert(table_immutable.sorted());
    assert(table_mutable.count() == 0);
    assert(table_immutable.count() == 0);
}
