const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;
const TableValuesType = @import("table_values.zig").TableValuesType;

pub fn TableImmutableType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableImmutable = @This();

        const Values = TableValuesType(Table);

        values: Values,
        snapshot_min: u64,
        free: bool,

        pub fn init(allocator: mem.Allocator) !TableImmutable {
            var values = try Values.init(allocator);
            errdefer values.deinit(allocator);

            return TableImmutable{
                .snapshot_min = undefined,
                .values = values,
                .free = true,
            };
        }

        pub fn deinit(table: *TableImmutable, allocator: mem.Allocator) void {
            table.values.deinit(allocator);
        }

        pub inline fn key_min(table: *const TableImmutable) Key {
            assert(!table.free);
            const inserted = table.values.inserted();
            assert(inserted.len > 0);
            return key_from_value(&inserted[0]);
        }

        pub inline fn key_max(table: *const TableImmutable) Key {
            assert(!table.free);
            const inserted = table.values.inserted();
            assert(inserted.len > 0);
            return key_from_value(&inserted[inserted.len - 1]);
        }

        pub fn clear(table: *TableImmutable) void {
            table.values.clear();
            table.snapshot_min = undefined;
            table.free = true;
        }

        pub fn reset_with_sorted_values(table: *TableImmutable, snapshot_min: u64) void {
            assert(table.free);
            assert(snapshot_min > 0);
            assert(snapshot_min < snapshot_latest);

            const sorted_values = table.values.inserted();
            assert(sorted_values.len > 0);
            assert(sorted_values.len <= value_count_max);
            assert(sorted_values.len <= Table.data.block_value_count_max * Table.data_block_count_max);

            if (constants.verify) {
                var i: usize = 1;
                while (i < sorted_values.len) : (i += 1) {
                    assert(i > 0);
                    const left_key = key_from_value(&sorted_values[i - 1]);
                    const right_key = key_from_value(&sorted_values[i]);
                    assert(compare_keys(left_key, right_key) == .lt);
                }
            }

            table.snapshot_min = snapshot_min;
            table.free = false;
        }

        // TODO(ifreund) This would be great to unit test.
        pub fn get(table: *const TableImmutable, key: Key) ?*const Value {
            assert(!table.free);
            return table.values.find(key);
        }
    };
}
