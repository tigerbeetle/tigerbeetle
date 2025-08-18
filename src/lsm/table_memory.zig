const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const stdx = @import("stdx");
const maybe = stdx.maybe;

const KWayMergeIteratorType = @import("./k_way_merge.zig").KWayMergeIteratorType;

// NOTE: High-level question, should we later on divide immutable table to mutable.
//       Also is it really immutable or frozen.

pub fn SortedRunsType(
    comptime Key: type,
    comptime Value: type,
    comptime runs_max: u32,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
) type {
    return struct {
        const SortedRuns = @This();
        runs_count: u16,
        elements_covered: u32 = 0,
        //remember the streams now and check overlap
        runs: [runs_max][]const Value,

        key_min: ?Key = null,
        key_max: ?Key = null,

        // We could pass here in a range it should cover
        fn invariants(self: *SortedRuns, full_range: []const Value) void {
            assert(self.runs_count <= runs_max);
            assert(self.elements_covered == full_range.len);

            // Quite heavy invariants for now!
            if (self.runs_count > 0) {
                // 1. Create a slice of runs that we can sort by start pointer
                const sorted_runs = self.runs[0..self.runs_count];

                std.mem.sort([]const Value, sorted_runs, {}, struct {
                    fn lessThan(_: void, a: []const Value, b: []const Value) bool {
                        return @intFromPtr(a.ptr) < @intFromPtr(b.ptr);
                    }
                }.lessThan);

                // 3. Check no overlaps and no gaps
                var covered_count: usize = 0;
                for (sorted_runs, 0..) |run, i| {
                    covered_count += run.len;

                    if (i > 0) {
                        const prev_run = sorted_runs[i - 1];
                        const prev_end_ptr = prev_run.ptr + prev_run.len;
                        assert(prev_end_ptr == run.ptr); // ensures no gap & no overlap
                    }
                }

                assert(covered_count == self.elements_covered);
                // 4. Ensure we covered exactly full_range
                assert(covered_count == full_range.len);
                assert(sorted_runs[0].ptr == full_range.ptr);
                const last_run = sorted_runs[sorted_runs.len - 1];
                assert(last_run.ptr + last_run.len == full_range.ptr + full_range.len);
            }
        }

        // TODO: sorted runs
        pub fn add(self: *SortedRuns, sorted_values: []const Value) void {
            if (sorted_values.len == 0) return;

            // add new run
            assert(self.runs_count < runs_max);
            self.runs[self.runs_count] = sorted_values;
            self.runs_count += 1;
            self.elements_covered += @intCast(sorted_values.len);
            // min or max key.
            const key_min_run = key_from_value(&sorted_values[0]);
            const key_max_run = key_from_value(&sorted_values[sorted_values.len - 1]);

            if (self.key_min == null) self.key_min = key_min_run;
            if (self.key_max == null) self.key_max = key_max_run;

            self.key_min = @min(self.key_min.?, key_min_run);
            self.key_max = @max(self.key_max.?, key_max_run);
        }

        pub fn count(self: *const SortedRuns) u16 {
            return self.runs_count;
        }
        pub fn elements(self: *const SortedRuns) u32 {
            return self.elements_covered;
        }

        pub fn reset(self: *SortedRuns) void {
            self.runs_count = 0;
            self.elements_covered = 0;
        }

        pub fn stream_peek(
            context: *const SortedRuns,
            stream_index: u32,
        ) error{ Empty, Drained }!Key {
            // TODO: test for Drained somehow as well.
            const stream = context.runs[stream_index];
            if (stream.len == 0) return error.Empty;
            return key_from_value(&stream[0]);
        }

        pub fn stream_pop(context: *SortedRuns, stream_index: u32) Value {
            const stream = context.runs[stream_index];
            context.runs[stream_index] = stream[1..];
            return stream[0];
        }

        pub fn stream_precedence(context: *const SortedRuns, a: u32, b: u32) bool {
            _ = context;

            // Higher streams have higher precedence.
            return a < b;
        }
    };
}

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        const runs_max = constants.lsm_compaction_ops + 1;

        pub const SortedRuns = SortedRunsType(Key, Value, runs_max, key_from_value);
        pub const KWay = KWayMergeIteratorType(
            SortedRuns,
            Key,
            Value,
            key_from_value,
            runs_max,
            SortedRuns.stream_peek,
            SortedRuns.stream_pop,
            SortedRuns.stream_precedence,
            true,
        );

        pub const ValueContext = struct {
            count: u32 = 0,
            /// When true, `values` is strictly ascending-ordered (no duplicates).
            sorted: bool = true,
        };

        const Mutability = union(enum) {
            mutable: struct {
                /// The offset (within `values`) of the unsorted suffix.
                /// - At the end of each beat, the mutable table's suffix is sorted and
                ///   deduplicated, and the suffix offset advances.
                /// - (At the end of the bar, the mutable table consists of a sequence of sorted
                ///   arrays, which is then itself finally sorted and deduplicated.)
                suffix_offset: u32 = 0,
            },
            immutable: struct {
                /// An empty table has nothing to flush.
                flushed: bool = true,
                /// This field is only used for assertions, to verify that we don't absorb the
                /// mutable table immediately prior to checkpoint.
                absorbed: bool = false,
                snapshot_min: u64 = 0,
            },
        };

        values: []Value,
        values_shadow: []Value,

        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        sorted_runs: SortedRuns,

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
                .sorted_runs = SortedRuns{
                    .runs_count = 0,
                    .runs = undefined,
                },
                .values = undefined,
                .values_shadow = undefined,
            };

            // TODO This would ideally be value_count_limit, but needs to be value_count_max to
            // ensure that memory table coalescing is deterministic even if the batch limit changes.
            table.values = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.values);

            table.values_shadow = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.values_shadow);
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            allocator.free(table.values);
        }

        pub fn reset(table: *TableMemory) void {
            const mutability: Mutability = switch (table.mutability) {
                .immutable => .{ .immutable = .{} },
                .mutable => .{ .mutable = .{} },
            };

            table.* = .{
                .values = table.values,
                .values_shadow = table.values_shadow,
                .value_context = .{},
                .mutability = mutability,
                .sorted_runs = SortedRuns{
                    .runs_count = 0,
                    .runs = undefined,
                },
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
            assert(table.value_context.count < table.values.len);
            if (table.value_context.sorted) {
                table.value_context.sorted = table.value_context.count == 0 or
                    key_from_value(&table.values[table.value_context.count - 1]) <
                        key_from_value(value);
            } else {
                assert(table.value_context.count > 0);
            }

            table.values[table.value_context.count] = value.*;
            table.value_context.count += 1;
        }

        /// This must be called on sorted tables.
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            assert(table.value_context.sorted);
            assert(table.value_context.count <= table.values.len);

            const maybe_value_gold = binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                table.values_used(),
                key,
                .{ .mode = .upper_bound },
            );

            return maybe_value_gold;
            //// TODO(TZ): fix this and rewrite clean.
            //// on primary table we scan simply backwards the sorted_runs.
            ////if (Table.usage == .secondary_index) return maybe_value_gold;

            //// reverse binary search
            //// BUG(TZ): why does this work for .secondary_index?
            //var idx: usize = 1;
            //const length = table.sorted_runs.count();
            //while (idx <= length) : (idx += 1) {
            //const run = table.sorted_runs.runs[length - idx];
            //const maybe_value = binary_search.binary_search_values(
            //Key,
            //Value,
            //key_from_value,
            //run,
            //key,
            //.{ .mode = .upper_bound },
            //);

            //if (maybe_value) |value| {
            //std.debug.print("key a {} key b {} \n", .{ key_from_value(value), key_from_value(maybe_value_gold.?) });
            //std.debug.print(" a {}  b {} \n", .{ value, maybe_value_gold.? });
            //assert(key_from_value(value) == key_from_value(maybe_value_gold.?));
            //return value;
            //}
            //}
            //assert(maybe_value_gold == null);
            //return null;
        }

        fn merge(table: *TableMemory) void {
            if (table.value_context.sorted) {
                assert(table.sorted_runs.count() == 0 or table.sorted_runs.count() == 1);
                std.mem.swap([]Value, &table.values, &table.values_shadow);
                return;
            }

            // BUG: Afaik this is only for the fuzzer currently.
            //      So we should rather fix the fuzzer
            if (table.mutability.mutable.suffix_offset != table.count()) {
                table.sort_suffix();
            }

            var iter = KWay.init(&table.sorted_runs, table.sorted_runs.count(), .ascending);

            var target_index: usize = 0;

            const output = table.values_shadow;
            const maybe_value = iter.pop() catch unreachable;
            output[target_index] = maybe_value.?;
            target_index += 1;
            // deduplicate merging based.

            while (iter.pop() catch unreachable) |value_next| {
                // TODO: we could do the dedupclication logic here
                // could we push the deduplication logic in the tree?
                // We could either push the deduplication logic in the tree.
                // or rather only the precedence. But does not matter for now.

                const value_next_equal = target_index > 0 and
                    key_from_value(&output[target_index - 1]) ==
                        key_from_value(&value_next);

                if (value_next_equal) {
                    if (Table.usage == .secondary_index) {
                        assert(Table.tombstone(&output[target_index - 1]) !=
                            Table.tombstone(&value_next));
                        // annihilate both
                        target_index -= 1;
                    } else {
                        // overwrite old value - last writer wins
                        output[target_index - 1] = value_next;
                    }
                } else {
                    output[target_index] = value_next;
                    target_index += 1;
                }
            }

            // At this point, source_index and target_index are actually counts.
            // source_index will always be incremented after the final iteration as part of the
            // continue expression.
            // target_index will always be incremented, since either source_index runs out first
            // so value_next_equal is false, or a new value is hit, which will increment it.
            const target_count = target_index;

            if (constants.verify) {
                if (0 < target_count) {
                    for (
                        output[0 .. target_count - 1],
                        output[1..target_count],
                    ) |*value, *value_next| {
                        assert(key_from_value(value) < key_from_value(value_next));
                    }
                }
            }
            // we need to finally set the new target count
            table.value_context.count = @intCast(target_count);
        }

        pub fn iterator(table: *TableMemory) KWay {
            const iter = KWay.init(&table.sorted_runs, table.sorted_runs.count(), .ascending);
            return iter;
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count <= table.values.len);
            defer assert(table.value_context.sorted);

            if (table.value_context.sorted) {
                // TODO(TZ): Fix again to normal values
                @memcpy(table.values_shadow[0..table.count()], table.values_used());
                table.sorted_runs.reset();
                table.sorted_runs.add(table.values_shadow[0..table.count()]);
            }

            // BUG: Afaik this is only for the fuzzer currently.
            //      So we should rather fix the fuzzer
            if (table.mutability.mutable.suffix_offset != table.count()) {
                table.sort_suffix();
            }

            std.debug.print("table.count {} covered {} \n", .{ table.count(), table.sorted_runs.elements_covered });
            std.debug.print("sorted runs  {} \n", .{table.sorted_runs.count()});

            if (constants.verify) table.sorted_runs.invariants(table.values_shadow[0..table.count()]);

            for (table.values_used(), table.values_shadow[0..table.count()]) |a, b| {
                assert(key_from_value(&a) == key_from_value(&b));
            }

            table.sort();

            table.value_context.sorted = true;

            // If we have no values, then we can consider ourselves flushed right away.
            table.mutability = .{ .immutable = .{
                .flushed = table.value_context.count == 0,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn make_mutable(table: *TableMemory) void {
            assert(table.mutability == .immutable);
            assert(table.mutability.immutable.flushed == true);
            assert(table.value_context.count <= table.values.len);
            assert(table.value_context.sorted);

            table.* = .{
                .sorted_runs = SortedRuns{
                    .runs_count = 0,
                    .runs = undefined,
                },
                .values = table.values,
                .values_shadow = table.values_shadow,
                .value_context = .{},
                .mutability = .{ .mutable = .{} },
                .name = table.name,
            };
        }

        /// Merge and sort the immutable/mutable tables (favoring values in the latter) into the
        /// immutable table. Then reset the mutable table.
        pub fn absorb(
            table_immutable: *TableMemory,
            table_mutable: *TableMemory,
            snapshot_min: u64,
        ) void {
            assert(table_immutable.mutability == .immutable);
            maybe(table_immutable.mutability.immutable.absorbed);
            assert(table_immutable.value_context.sorted);
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.value_context.sorted);

            const values_count_limit = table_immutable.values.len;
            assert(values_count_limit == values_count_limit);
            assert(table_immutable.count() <= values_count_limit);
            assert(table_mutable.count() <= values_count_limit);
            assert(table_immutable.count() + table_mutable.count() <= values_count_limit);

            stdx.copy_disjoint(
                .inexact,
                Value,
                table_immutable.values[table_immutable.count()..],
                table_mutable.values[0..table_mutable.count()],
            );

            const tables_combined_count = table_immutable.count() + table_mutable.count();
            table_immutable.value_context.count =
                sort_suffix_from_offset(table_immutable.values[0..tables_combined_count], 0);
            assert(table_immutable.count() <= tables_combined_count);

            // TODO(TZ) revert
            @memcpy(table_immutable.values_shadow[0..table_immutable.count()], table_immutable.values_used());
            table_immutable.sorted_runs.reset();
            table_immutable.sorted_runs.add(table_immutable.values_shadow[0..table_immutable.count()]);

            if (constants.verify) table_immutable.sorted_runs.invariants(table_immutable.values_shadow[0..table_immutable.count()]);

            for (table_immutable.values_used(), table_immutable.values_shadow[0..table_immutable.count()]) |a, b| {
                assert(key_from_value(&a) == key_from_value(&b));
            }

            table_mutable.reset();
            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.value_context.count == 0,
                .absorbed = true,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn sort(table: *TableMemory) void {
            assert(table.mutability == .mutable);

            if (!table.value_context.sorted) {
                _ = table.mutable_sort_suffix_from_offset(0);
                table.value_context.sorted = true;
            }

            // Set suffix sort here to not create another run that overlaps with this one.
            table.mutability = .{ .mutable = .{ .suffix_offset = table.count() } };
            // reset sorted runs here since there can only be one full run.
            // TODO(TZ): revert
            @memcpy(table.values_shadow[0..table.count()], table.values_used());
            table.sorted_runs.reset();
            table.sorted_runs.add(table.values_shadow[0..table.count()]);
            if (constants.verify) table.sorted_runs.invariants(table.values_shadow[0..table.count()]);
            for (table.values_used(), table.values_shadow[0..table.count()]) |a, b| {
                assert(key_from_value(&a) == key_from_value(&b));
            }

            // can be empty when there is no data.
            assert(table.sorted_runs.count() == 1 or table.sorted_runs.count() == 0);
        }

        pub fn sort_suffix(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            assert(table.mutability.mutable.suffix_offset <= table.count());

            // Sorted is a global property so we can form a single run.
            if (table.value_context.sorted) {
                // TODO(TZ): Fix again to normal values
                @memcpy(table.values_shadow[0..table.count()], table.values_used());
                table.sorted_runs.reset();
                table.sorted_runs.add(table.values_shadow[0..table.count()]);
                if (constants.verify) table.sorted_runs.invariants(table.values_shadow[0..table.count()]);
                for (table.values_used(), table.values_shadow[0..table.count()]) |a, b| {
                    assert(key_from_value(&a) == key_from_value(&b));
                }

                table.mutability = .{ .mutable = .{ .suffix_offset = table.count() } };
                return;
            }

            // TODO(TZ): Fix again to normal values
            const begin = table.mutability.mutable.suffix_offset;
            const sorted_run = table.mutable_sort_suffix_from_offset(table.mutability.mutable.suffix_offset);
            @memcpy(table.values_shadow[begin .. begin + sorted_run.len], sorted_run);

            table.sorted_runs.add(table.values_shadow[begin .. begin + sorted_run.len]);
            if (constants.verify) table.sorted_runs.invariants(table.values_shadow[0..table.count()]);

            for (table.values_used(), table.values_shadow[0..table.count()]) |a, b| {
                assert(key_from_value(&a) == key_from_value(&b));
            }

            assert(table.mutability.mutable.suffix_offset == table.count());
        }

        // Returns the sorted slice, e.g. the new sorted run.
        fn mutable_sort_suffix_from_offset(table: *TableMemory, offset: u32) []const Value {
            assert(table.mutability == .mutable);
            assert(offset == table.mutability.mutable.suffix_offset or offset == 0);
            assert(offset <= table.count());

            const target_count = sort_suffix_from_offset(table.values_used(), offset);
            table.value_context.count = target_count;
            table.mutability = .{ .mutable = .{ .suffix_offset = target_count } };
            return table.values_used()[offset..target_count];
        }

        /// Returns the new length of `values`. (Values are deduplicated after sorting, so the
        /// returned count may be less than or equal to the original `values.len`.)
        fn sort_suffix_from_offset(values: []Value, offset: u32) u32 {
            assert(offset <= values.len);

            std.mem.sort(Value, values[offset..], {}, sort_values_by_key_in_ascending_order);

            // Merge values with identical keys (last one wins) and collapse tombstones for
            // secondary indexes.
            const source_count: u32 = @intCast(values.len);
            var source_index: u32 = offset;
            var target_index: u32 = offset;
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
                if (offset < target_count) {
                    for (
                        values[offset .. target_count - 1],
                        values[offset + 1 .. target_count],
                    ) |*value, *value_next| {
                        assert(key_from_value(value) < key_from_value(value_next));
                    }
                }
            }
            return target_count;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return key_from_value(&a) < key_from_value(&b);
        }

        pub fn key_min(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);

            // TODO(TZ): Change
            const gold = key_from_value(&values[0]);
            assert(table.sorted_runs.key_min.? == gold);
            return table.sorted_runs.key_min.?;
        }

        pub fn key_max(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);
            // TODO(TZ): Change

            const gold = key_from_value(&values[values.len - 1]);
            assert(table.sorted_runs.key_max.? == gold);
            return table.sorted_runs.key_max.?;
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
