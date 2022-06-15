const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;

/// Range queries are not supported on the TableMutable, it must first be made immutable.
pub fn TableMutableType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const tombstone_from_key = Table.tombstone_from_key;

    return struct {
        const TableMutable = @This();

        const load_factor = 50;
        const Values = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, load_factor);

        value_count_max: u32,
        values: Values = .{},

        pub fn init(allocator: mem.Allocator, commit_count_max: u32) !TableMutable {
            comptime assert(config.lsm_mutable_table_batch_multiple > 0);
            assert(commit_count_max > 0);

            const value_count_max = commit_count_max * config.lsm_mutable_table_batch_multiple;
            const data_block_count = div_ceil(value_count_max, Table.data.value_count_max);
            assert(data_block_count <= Table.data_block_count_max);

            var values: Values = .{};
            try values.ensureTotalCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            return TableMutable{
                .value_count_max = value_count_max,
                .values = values,
            };
        }

        pub fn deinit(table: *TableMutable, allocator: mem.Allocator) void {
            table.values.deinit(allocator);
        }

        pub fn get(table: *TableMutable, key: Key) ?*const Value {
            return table.values.getKeyPtr(tombstone_from_key(key));
        }

        pub fn put(table: *TableMutable, value: Value) void {
            table.values.putAssumeCapacity(value, {});
            // The hash map's load factor may allow for more capacity because of rounding:
            assert(table.values.count() <= table.value_count_max);
        }

        pub fn remove(table: *TableMutable, key: Key) void {
            table.values.putAssumeCapacity(tombstone_from_key(key), {});
            assert(table.values.count() <= table.value_count_max);
        }

        pub fn cannot_commit_batch(table: *TableMutable, batch_count: u32) bool {
            assert(batch_count <= table.value_count_max);
            return table.count() + batch_count > table.value_count_max;
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.values.count() > 0);
            table.values.clearRetainingCapacity();
            assert(table.values.count() == 0);
        }

        pub fn count(table: *TableMutable) u32 {
            const value = @intCast(u32, table.values.count());
            assert(value <= table.value_count_max);
            return value;
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_values_and_clear(
            table: *TableMutable,
            values_max: []Value,
        ) []const Value {
            assert(table.count() > 0);
            assert(table.count() <= table.value_count_max);
            assert(table.count() <= values_max.len);
            assert(values_max.len == table.value_count_max);

            var i: usize = 0;
            var it = table.values.keyIterator();
            while (it.next()) |value| : (i += 1) {
                values_max[i] = value.*;
            }

            const values = values_max[0..i];
            assert(values.len == table.count());
            std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

            table.clear();
            assert(table.count() == 0);

            return values;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(a), key_from_value(b)) == .lt;
        }
    };
}
