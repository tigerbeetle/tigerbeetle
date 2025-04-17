const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;

// BUG: the sorted flag previously started sorted already.
//       it was set to false if the values were not insert in sorted order
//       Need to figure out a good way to do this.
pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: u32 = 0,
            // When true, `values` is strictly ascending-ordered (no duplicates).
            //sorted: bool = true,
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

        // `sort_suffix` breaks the `values` array into “sorted runs” (sorted sub‑arrays).
        // Each SortedRun captures the start (min) and end (max) indices of one sorted run.
        // We can exploit this knowledge to check if the full array is sorted in O(k) and avoid
        // the final sort, which reduces tail latency.
        // The number of calls to `sort_suffix` are determined by `constants.lsm_compaction_ops`.
        const SortedRun = struct {
            min: u32, // inclusive
            max: u32, // exclusive
        };
        // Allow one extra sorted run beyond lsm_compaction_ops:
        // Scans perform an ad‑hoc full-table sort, producing a full run.
        // Then there could be still number lsm_compaction_ops producing the remaining sorted runs.
        const sorted_runs_max = constants.lsm_compaction_ops + 1;
        sorted_runs: [sorted_runs_max]SortedRun,
        sorted_runs_count: u16,

        values: []Value,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

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
                .sorted_runs = .{.{
                    .min = std.math.maxInt(u32),
                    .max = std.math.maxInt(u32),
                }} ** sorted_runs_max,
                .sorted_runs_count = 0,
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
                .mutable => .{ .mutable = .{} },
            };

            table.* = .{
                .sorted_runs = .{.{
                    .min = std.math.maxInt(u32),
                    .max = std.math.maxInt(u32),
                }} ** sorted_runs_max,
                .sorted_runs_count = 0,
                .values = table.values,
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
            assert(table.value_context.count < table.values.len);
            // NOTE: this used to cover the case when we only append so what do we do now?
            if (table.sorted()) {
                assert(table.sorted_runs_count == 1);
                assert(table.count() > 0);
                // We expand the sorted run if the new key is strictly larger then the old max.
                const max_key = key_from_value(&table.values[table.sorted_runs[0].max - 1]);
                table.sorted_runs[0].max += if (max_key < key_from_value(value)) 1 else 0;
            }

            table.values[table.value_context.count] = value.*;
            table.value_context.count += 1;
        }

        /// This must be called on sorted tables.
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            assert(table.value_context.count <= table.values.len);
            assert(table.sorted());

            return binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                table.values_used(),
                key,
                .{ .mode = .upper_bound },
            );
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count <= table.values.len);
            defer assert(table.sorted());

            table.sort();

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
            assert(table.sorted());

            table.* = .{
                .sorted_runs = .{.{
                    .min = std.math.maxInt(u32),
                    .max = std.math.maxInt(u32),
                }} ** sorted_runs_max,
                .sorted_runs_count = 0,
                .values = table.values,
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
            assert(table_immutable.sorted());
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.sorted());

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

            table_mutable.reset();
            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.value_context.count == 0,
                .absorbed = true,
                .snapshot_min = snapshot_min,
            } };
        }

        // The table is sorted if we have exactly one sorted run.
        pub fn sorted(table: *const TableMemory) bool {
            return table.sorted_runs_count == 1;
        }

        pub fn sort(table: *TableMemory) void {
            assert(table.mutability == .mutable);

            if (!table.sorted()) {
                table.mutable_sort_suffix_from_offset(0);
            }
        }

        pub fn sort_suffix(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            assert(table.mutability.mutable.suffix_offset <= table.count());

            table.mutable_sort_suffix_from_offset(table.mutability.mutable.suffix_offset);

            assert(table.mutability.mutable.suffix_offset == table.count());
        }

        fn mutable_sort_suffix_from_offset(table: *TableMemory, offset: u32) void {
            assert(table.mutability == .mutable);
            assert(offset == table.mutability.mutable.suffix_offset or offset == 0);
            assert(offset <= table.count());

            const run_min = table.mutability.mutable.suffix_offset;
            const target_count = sort_suffix_from_offset(table.values_used(), offset);
            const run_max = table.mutability.mutable.suffix_offset;

            table.value_context.count = target_count;
            table.mutability = .{ .mutable = .{ .suffix_offset = target_count } };

            // New sorted run was created.
            if (run_min < run_max) {
                const run_new: SortedRun = .{ .min = run_min, .max = run_max };

                // First run, no chance to coalesce, so just append it.
                if (table.sorted_runs_count == 0) {
                    table.sorted_runs[table.sorted_runs_count] = run_new;
                    table.sorted_runs_count += 1;
                    return;
                }

                const run_old = &table.sorted_runs[table.sorted_runs_count - 1];

                const key_max_old = key_from_value(&table.values[run_new.max - 1]);
                const key_min_new = key_from_value(&table.values[run_old.min]);

                // Try to coalesce the new run with the old run.
                // Ensure adjacent runs neither overlap nor contain duplicates:
                // If the max key of run a is < the min key of run b, their union is
                // strictly increasing and we do not have to deduplicate and we coalesce them.
                if (key_max_old < key_min_new) {
                    run_old.max = run_new.max;

                    if (constants.verify) {
                        for (
                            table.values[0 .. table.count() - 1],
                            table.values[0 + 1 .. table.count()],
                        ) |*value, *value_next| {
                            assert(key_from_value(value) < key_from_value(value_next));
                        }
                    }
                } else {
                    table.sorted_runs[table.sorted_runs_count] = run_new;
                    table.sorted_runs_count += 1;
                }
            }
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
                if (source_index != target_index) {
                    values[target_index] = values[source_index];
                }

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

            return key_from_value(&values[0]);
        }

        pub fn key_max(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);

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
