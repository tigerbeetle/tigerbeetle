const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;
const binary_search = @import("binary_search.zig");

const snapshot_latest = @import("tree.zig").snapshot_latest;

pub fn ImmutableTableType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;

    return struct {
        const ImmutableTable = @This();

        value_count_max: u32,
        values: []Value,
        snapshot_min: u64,
        iterator_index: u32,
        free: bool,

        pub fn init(allocator: mem.Allocator, commit_count_max: u32) !ImmutableTable {
            // The in-memory immutable table is the same size as the mutable table:
            const value_count_max = commit_count_max * config.lsm_mutable_table_batch_multiple;
            assert(div_ceil(value_count_max, data.value_count_max) <= Table.data_block_count_max);

            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            return ImmutableTable{
                .value_count_max = value_count_max,
                .snapshot_min = undefined,
                .values = values,
                .iterator_index = values.len,
                .free = true,
            };
        }

        pub inline fn values_max(table: *const ImmutableTable) []Value {
            return table.values.ptr[0..table.value_count_max];
        }

        pub fn deinit(table: *ImmutableTable, allocator: mem.Allocator) void {
            allocator.free(table.values_max());
        }

        pub fn reset_with_sorted_values(
            table: *ImmutableTable,
            snapshot_min: u64,
            sorted_values: []const Value,
        ) void {
            assert(table.free);
            assert(table.iterator_index == table.values.len);

            assert(snapshot_min > 0);
            assert(snapshot_min < snapshot_latest);

            assert(sorted_values.ptr == table.values.ptr);
            assert(sorted_values.len > 0);
            assert(sorted_values.len <= table.value_count_max);
            assert(sorted_values.len <= data.value_count_max * data_block_count_max);

            if (config.verify) {
                var i: usize = 1;
                while (i < sorted_values.len) : (i += 1) {
                    assert(i > 0);
                    const left_key = key_from_value(sorted_values[i - 1]);
                    const right_key = key_from_value(sorted_values[i]);
                    assert(compare_keys(left_key, right_key) != .gt);
                }
            }

            table.* = .{
                .value_count_max = table.value_count_max,
                .values = sorted_values,
                .snapshot_min = snapshot_min,
                .iterator_index = 0,
                .free = false,
            };
        }

        // TODO(ifreund) This would be great to unit test.
        pub fn get(table: *const ImmutableTable, key: Key) ?*const Value {
            assert(!table.free);

            if (table.values.len > 0) {
                const result = binary_search.binary_search_values(
                    Key,
                    Value,
                    Table.key_from_value,
                    Table.compare_keys,
                    table.values,
                    key,
                );
                if (result.exact) {
                    const value = &table.values[result.index];
                    if (config.verify) assert(compare_keys(key, key_from_value(value.*)) == .eq);
                    return value;
                }
            }

            return null;
        }

        pub fn tick(_: *const ImmutableTable) bool {
            return false; // No I/O is performed as it's all in memory.
        }

        pub fn buffered_all_values(_: *const ImmutableTable) bool {
            return true; // All values are "buffered" in memory.
        }

        pub fn peek(table: *const ImmutableTable) ?Key {
            if (table.iterator_index == table.values.len) return null;
            return key_from_value(table.values[table.iterator_index]);
        }

        pub fn pop(table: *ImmutableTable) Value {
            defer table.iterator_index += 1;
            return table.values[table.iterator_index];
        }
    };
}
