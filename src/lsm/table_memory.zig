const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const stdx = @import("stdx");
const maybe = stdx.maybe;

const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: u32 = 0,
        };

        const Mutability = union(enum) {
            mutable: struct {},
            immutable: struct {
                /// An empty table has nothing to flush.
                flushed: bool = true,
                /// This field is only used for assertions, to verify that we don't absorb the
                /// mutable table immediately prior to checkpoint.
                absorbed: bool = false,
                snapshot_min: u64 = 0,
            },
        };

        const SortedRun = struct {
            min: u32,
            max: u32, // exclusive.
            src: Source, // this defines where the sorted run initially came from and determines the priority
        };

        // One sort() call and one immutable table run.
        const sorted_runs_max = constants.lsm_compaction_ops + 2;

        const Source = enum { mutable, immutable };

        const MergeContext = struct {
            const streams_max = sorted_runs_max; // TODO(TZ): clean up

            const log = false;

            streams: [streams_max][]const Value,
            streams_count: u32,
            source: [streams_max]Source,

            fn stream_peek(
                context: *const MergeContext,
                stream_index: u32,
            ) error{ Empty, Drained }!Key {
                const stream = context.streams[stream_index];
                if (stream.len == 0) return error.Empty;
                return key_from_value(&stream[0]);
            }

            fn stream_pop(context: *MergeContext, stream_index: u32) Value {
                const stream = context.streams[stream_index];
                context.streams[stream_index] = stream[1..];
                return stream[0];
            }

            fn stream_precedence(context: *const MergeContext, a: u32, b: u32) bool {
                // Prefer immutable over mutable on ties.
                if (context.source[a] != context.source[b]) return context.source[a] == .immutable;
                return a < b;
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
            MergeContext.stream_precedence,
        );

        const SortedRunTracker = struct {
            runs: [sorted_runs_max]SortedRun,
            runs_count: u8,

            fn init() SortedRunTracker {
                return .{
                    .runs = undefined,
                    .runs_count = 0,
                };
            }

            fn add(tracker: *SortedRunTracker, run: SortedRun) void {
                // ignore empty runs
                if (run.min == run.max) return;
                tracker.runs[tracker.runs_count] = run;
                tracker.runs_count += 1;
            }

            fn reset(tracker: *SortedRunTracker) void {
                tracker.runs_count = 0;
            }

            // returns number of runs.
            // mostly for invariants
            fn count(tracker: *const SortedRunTracker) u32 {
                return tracker.runs_count;
            }

            fn merge_context(tracker: *const SortedRunTracker, values: []const Value) MergeContext {
                // TODO: What are the invariants for this?
                var context = MergeContext{
                    .streams = undefined,
                    .streams_count = 0,
                    .source = undefined,
                };

                for (tracker.runs[0..tracker.count()], 0..) |run, i| {
                    context.streams[i] = values[run.min..run.max];
                    context.source[i] = run.src;
                }
                context.streams_count = tracker.count();
                return context;
            }
            fn last_ref(tracker: *SortedRunTracker) ?*SortedRun {
                if (tracker.count() == 0) return null;
                return &tracker.runs[tracker.count() - 1];
            }

            fn last(tracker: *const SortedRunTracker) ?*const SortedRun {
                if (tracker.count() == 0) return null;
                return &tracker.runs[tracker.count() - 1];
            }

            fn invariant(tracker: *const SortedRunTracker, table_count: u32) void {
                if (tracker.count() == 0) return;

                assert(tracker.runs[0].min == 0);

                const runs_count = tracker.count();
                for (tracker.runs[0 .. runs_count - 1], tracker.runs[1..runs_count]) |a, b| {
                    assert(a.min < b.min); // ordered.
                    assert(a.max == b.min); // no holes.
                }
                assert(tracker.runs[runs_count - 1].max == table_count);
                var immutable_runs: u1 = 0;
                for (tracker.runs[0..runs_count]) |run| {
                    immutable_runs += if (run.src == .immutable) 1 else 0;
                }
                assert(immutable_runs == 0 or immutable_runs == 1);
            }
        };
        const DedupSink = struct {
            out: []Value,
            target_index: u32 = 0,
            // Holds the current candidate that may merge with the next item.
            pending: ?Value = null,

            pub fn init(out: []Value) DedupSink {
                return .{ .out = out };
            }

            inline fn sameKey(a: *const Value, b: *const Value) bool {
                return key_from_value(a) == key_from_value(b);
            }

            pub fn push(self: *DedupSink, v: Value) void {
                if (self.pending) |p| {
                    if (sameKey(&p, &v)) {
                        if (Table.usage == .secondary_index) {
                            // Annihilate put/remove pair for secondary indexes.
                            assert(Table.tombstone(&p) != Table.tombstone(&v));
                            self.pending = null; // drop both
                        } else {
                            // Primary index: last one wins in a run of dups.
                            self.pending = v;
                        }
                    } else {
                        // Key changed: flush pending and keep new as pending.
                        self.out[self.target_index] = p;
                        self.target_index += 1;
                        self.pending = v;
                    }
                } else {
                    self.pending = v;
                }
            }

            pub fn finish(self: *DedupSink) u32 {
                if (self.pending) |p| {
                    self.out[self.target_index] = p;
                    self.target_index += 1;
                    self.pending = null;
                }
                return self.target_index;
            }
        };

        values: []Value,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        run_tracker: SortedRunTracker,

        pub fn init(
            table: *TableMemory,
            allocator: mem.Allocator,
            mutability: std.meta.Tag(Mutability),
            name: []const u8,
            options: struct {
                value_count_limit: u32,
            },
        ) !void {
            assert(options.value_count_limit <= Table.value_count_max);

            table.* = .{
                .value_context = .{},
                .mutability = switch (mutability) {
                    .mutable => .{ .mutable = .{} },
                    .immutable => .{ .immutable = .{} },
                },
                .name = name,
                .run_tracker = SortedRunTracker.init(),
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
                .mutable => .{ .mutable = .{} },
            };

            table.run_tracker.reset();

            table.* = .{
                .values = table.values,
                .run_tracker = table.run_tracker,
                .value_context = .{},
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

        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.count() < table.values.len);

            if (table.run_tracker.last_ref()) |last_run| {
                if (last_run.max == table.count()) {
                    const expand: bool = table.count() == 0 or
                        key_from_value(&table.values[table.count() - 1]) <
                            key_from_value(value);
                    last_run.max += @intFromBool(expand);
                }
            }

            table.values[table.count()] = value.*;
            table.value_context.count += 1;
        }

        /// This must be called on sorted tables.
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

        pub fn merge(table_immutable: *TableMemory, table_mutable: *TableMemory, snapshot_min: u64) void {
            table_mutable.run_tracker.invariant(table_mutable.count());

            if (table_mutable.run_tracker.count() == 1) {
                std.debug.print("table already sorted {s} \n", .{table_mutable.name});
                std.mem.swap([]Value, &table_mutable.values, &table_immutable.values);
                table_immutable.value_context.count = table_mutable.count();
                table_immutable.mutability = .{ .immutable = .{
                    .flushed = table_immutable.count() == 0,
                    .snapshot_min = snapshot_min,
                } };
                table_immutable.run_tracker.reset();
                table_immutable.run_tracker.add(.{ .min = 0, .max = table_immutable.count(), .src = .immutable });
                table_mutable.reset();
                return;
            }

            // TODO(TZ) : what happens if it is empty here?
            // and what should we do? Why does it crash then?
            // here the fuzzer model does not fit.
            //if (table_mutable.count() == 0)
            // this is uper strange maybe hit by absorb case?

            var merge_context = table_mutable.run_tracker.merge_context(table_mutable.values_used());
            var merge_iterator = KWayMergeIterator.init(&merge_context, @intCast(merge_context.streams_count), .ascending);

            var target_index: u32 = 0;
            var dedup_sink = DedupSink.init(table_immutable.values);
            while (merge_iterator.pop() catch unreachable) |value| : (target_index += 1) {
                //table_immutable.values[target_index] = value;
                dedup_sink.push(value);
            }
            // we could do dedpulication in place should we ?
            //table_immutable.value_context.count = deduplicate(table_immutable.values[0..target_index]);
            table_immutable.value_context.count = dedup_sink.finish();

            // set data correctly e.g. immutable and mutable.reset();
            // If we have no values, then we can consider ourselves flushed right away.
            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.count() == 0,
                .snapshot_min = snapshot_min,
            } };

            table_immutable.run_tracker.reset();
            table_immutable.run_tracker.add(.{ .min = 0, .max = table_immutable.count(), .src = .immutable });

            assert(table_immutable.sorted());

            table_mutable.reset();
        }

        pub fn absorb_new(
            table_immutable: *TableMemory,
            table_mutable: *TableMemory,
            snapshot_min: u64,
        ) void {
            assert(table_immutable.mutability == .immutable);
            maybe(table_immutable.mutability.immutable.absorbed);
            assert(table_immutable.sorted());
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.sorted());

            const values_count_limit = table_immutable.values.len;
            assert(table_immutable.count() <= values_count_limit);
            assert(table_mutable.count() <= values_count_limit);
            assert(table_immutable.count() + table_mutable.count() <= values_count_limit);

            if (table_mutable.count() == 0) {
                return;
            }

            // copy now from immutable to mutable and
            stdx.copy_disjoint(
                .inexact,
                Value,
                table_mutable.values[table_mutable.count()..],
                table_immutable.values[0..table_immutable.count()],
            );
            const tables_combined_count = table_immutable.count() + table_mutable.count();
            // Here we need to src. immutable to prioritze this run in the merge.
            table_mutable.run_tracker.add(.{
                .min = table_mutable.count(),
                .max = tables_combined_count,
                .src = .immutable,
            });
            table_mutable.value_context.count = tables_combined_count;
            table_immutable.merge(table_mutable, snapshot_min);
        }

        pub fn sort(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            defer table.run_tracker.invariant(table.count());

            if (!table.sorted()) {
                _ = table.mutable_sort_suffix_from_offset(0);
                table.run_tracker.reset();
                table.run_tracker.add(.{ .min = 0, .max = table.count(), .src = .mutable });
            }
        }

        fn sorted(table: *const TableMemory) bool {
            // Empty table is considered sorted.
            if (table.count() == 0) return true;

            // Only one sorted run can exist if it is sorted.
            if (table.run_tracker.count() != 1) return false;

            const last_run = table.run_tracker.last().?;
            assert(last_run.min == 0);
            assert(last_run.max <= table.count());

            return table.count() == last_run.max;
        }

        pub fn sort_suffix(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            defer table.run_tracker.invariant(table.count());

            if (table.sorted()) return;

            const sort_suffix_offset = if (table.run_tracker.last()) |last_run| last_run.max else 0;
            assert(sort_suffix_offset <= table.count());

            if (sort_suffix_offset == table.count()) return;

            const run = table.mutable_sort_suffix_from_offset(sort_suffix_offset);
            assert(run.min < run.max);
            assert(run.max == table.count()); // the invariant checks this too.
            assert(sort_suffix_offset <= run.max);
            table.run_tracker.add(run);
        }

        fn mutable_sort_suffix_from_offset(table: *TableMemory, offset: u32) SortedRun {
            assert(table.mutability == .mutable);
            //assert(offset == table.mutability.mutable.suffix_offset or offset == 0);
            assert(offset == 0 or offset == table.run_tracker.last().?.max);
            assert(offset <= table.count());

            const target_count = sort_suffix_from_offset(table.values_used(), offset);
            table.value_context.count = target_count;
            return .{ .min = offset, .max = target_count, .src = .mutable };
        }

        fn deduplicate(values: []Value) u32 {
            // Merge values with identical keys (last one wins) and collapse tombstones for
            // secondary indexes.
            const source_count: u32 = @intCast(values.len);
            var source_index: u32 = 0;
            var target_index: u32 = 0;
            while (source_index < source_count) {
                const value = values[source_index];
                values[target_index] = value;

                // If we're at the end of the source, there is no next value, so the next value
                // can't be equal.
                const value_next_equal = source_index + 1 < source_count and
                    key_from_value(&values[source_index]) ==
                        key_from_value(&values[source_index + 1]);

                if (value_next_equal) {
                    if (Table.usage == .secondary_index) {
                        // Secondary index optimization --- cancel out put and remove.
                        // NB: while this prevents redundant tombstones from getting to disk, we
                        // still spend some extra CPU work to sort the entries in memory. Ideally,
                        // we annihilate tombstones immediately, before sorting, but that's tricky
                        // to do with scopes.
                        assert(Table.tombstone(&values[source_index]) !=
                            Table.tombstone(&values[source_index + 1]));
                        source_index += 2;
                        target_index += 0;
                    } else {
                        // The last value in a run of duplicates needs to be the one that ends up in
                        // target.
                        source_index += 1;
                        target_index += 0;
                    }
                } else {
                    source_index += 1;
                    target_index += 1;
                }
            }

            // At this point, source_index and target_index are actually counts.
            // source_index will always be incremented after the final iteration as part of the
            // continue expression.
            // target_index will always be incremented, since either source_index runs out first
            // so value_next_equal is false, or a new value is hit, which will increment it.
            const target_count = target_index;
            assert(target_count <= source_count);
            assert(source_count == source_index);

            if (constants.verify) {
                if (0 < target_count) {
                    for (
                        values[0 .. target_count - 1],
                        values[1..target_count],
                    ) |*value, *value_next| {
                        assert(key_from_value(value) < key_from_value(value_next));
                    }
                }
            }
            return target_count;
        }

        /// Returns the new length of `values`. (Values are deduplicated after sorting, so the
        /// returned count may be less than or equal to the original `values.len`.)
        fn sort_suffix_from_offset(values: []Value, offset: u32) u32 {
            assert(offset <= values.len);

            std.mem.sort(Value, values[offset..], {}, sort_values_by_key_in_ascending_order);
            const target_count = offset + deduplicate(values[offset..]);

            return target_count;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return key_from_value(&a) < key_from_value(&b);
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

const TestTable = struct {
    const Key = u32;
    const Value = struct { key: Key, value: u32, tombstone: bool };
    const value_count_max = 16;
    const usage = .general;

    inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }
};

// add more tests.

test "table_memory: unit" {
    const testing = std.testing;
    const TableMemory = TableMemoryType(TestTable);

    const allocator = testing.allocator;
    var table_memory: TableMemory = undefined;
    try table_memory.init(allocator, .mutable, "test", .{
        .value_count_limit = TestTable.value_count_max,
    });
    defer table_memory.deinit(allocator);

    table_memory.put(&.{ .key = 1, .value = 1, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 3, .tombstone = false });
    table_memory.put(&.{ .key = 5, .value = 5, .tombstone = false });

    assert(table_memory.count() == 3 and table_memory.value_context.count == 3);
    assert(table_memory.value_context.sorted);

    table_memory.put(&.{ .key = 0, .value = 0, .tombstone = false });
    table_memory.make_immutable(0);

    assert(table_memory.count() == 4 and table_memory.value_context.count == 4);
    assert(table_memory.key_min() == 0);
    assert(table_memory.key_max() == 5);
    assert(table_memory.value_context.sorted);

    // "Flush" and make mutable again
    table_memory.mutability.immutable.flushed = true;

    table_memory.make_mutable();
    assert(table_memory.count() == 0 and table_memory.value_context.count == 0);
    assert(table_memory.value_context.sorted);
    assert(table_memory.mutability == .mutable);
}
