const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: usize = 0,
            sorted: bool = true,
        };

        const Mutability = union(enum) {
            mutable,
            immutable: struct {
                /// An empty table has nothing to flush
                flushed: bool = true,
                snapshot_min: u64 = 0,
            },
        };

        values: []Value,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        pub fn init(
            allocator: mem.Allocator,
            mutability: Mutability,
            name: []const u8,
        ) !TableMemory {
            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            return TableMemory{
                .values = values,
                .value_context = .{},
                .mutability = mutability,
                .name = name,
            };
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            table.values = table.values.ptr[0..value_count_max];
            allocator.free(table.values);
        }

        pub fn reset(table: *TableMemory) void {
            var values_max = table.values.ptr[0..value_count_max];
            var mutability: Mutability = if (table.mutability == .immutable) .{ .immutable = .{
                .flushed = true,
            } } else .mutable;

            table.* = .{
                .values = values_max,
                .value_context = .{},
                .mutability = mutability,
                .name = table.name,
            };
        }

        pub fn count(table: *TableMemory) usize {
            return table.value_context.count;
        }

        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.values.len == value_count_max);
            assert(table.value_context.count < value_count_max);

            const put_order = if (table.value_context.count == 0)
                .lt
            else
                compare_keys(
                    key_from_value(&table.values[table.value_context.count - 1]),
                    key_from_value(value),
                );
            table.values[table.value_context.count] = value.*;
            table.value_context.count += 1;

            table.value_context.sorted = table.value_context.sorted and
                (put_order == .eq or put_order == .lt);
        }

        /// This function is intended to never be called by regular code. It only
        /// exists for fuzzing, due to the performance overhead it carries. Real
        /// code must rely on the Groove cache for lookups.
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            assert(constants.verify);

            // Just sort all the keys here, for simplicity.
            if (!table.value_context.sorted) {
                std.mem.sort(
                    Value,
                    table.values[0..table.value_context.count],
                    {},
                    sort_values_by_key_in_ascending_order,
                );
                table.value_context.sorted = true;
            }

            const result = binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                compare_keys,
                table.values[0..table.value_context.count],
                key,
                .{ .mode = .upper_bound },
            );
            if (result.exact) {
                const value = &table.values[result.index];
                assert(compare_keys(key, key_from_value(value)) == .eq);
                return value;
            }

            return null;
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            assert(table.mutability == .mutable);

            table.values = table.values[0..table.value_context.count];

            // Sort all the values. In future, this will be done incrementally, and use
            // k_way_merge, but for now the performance regression was too bad.
            if (!table.value_context.sorted) {
                std.mem.sort(Value, table.values, {}, sort_values_by_key_in_ascending_order);
                table.value_context.sorted = true;
            }

            // If we have no values, then we can consider ourselves flushed right away.
            table.mutability = .{ .immutable = .{
                .flushed = table.value_context.count == 0,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn make_mutable(table: *TableMemory) void {
            assert(table.mutability == .immutable);
            assert(table.mutability.immutable.flushed == true);

            var values_max = table.values.ptr[0..value_count_max];
            assert(values_max.len == value_count_max);

            table.* = .{
                .values = values_max,
                .value_context = .{},
                .mutability = .mutable,
                .name = table.name,
            };
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }

        pub inline fn key_min(table: *const TableMemory) Key {
            assert(table.values.len > 0);
            assert(table.mutability == .immutable);

            return key_from_value(&table.values[0]);
        }

        pub inline fn key_max(table: *const TableMemory) Key {
            assert(table.values.len > 0);
            assert(table.mutability == .immutable);

            return key_from_value(&table.values[table.values.len - 1]);
        }
    };
}

const TestTable = struct {
    const Key = u32;
    const Value = struct { key: Key, value: u32, tombstone: bool };
    const value_count_max = 16;

    inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }

    inline fn compare_keys(a: Key, b: Key) math.Order {
        return math.order(a, b);
    }

    inline fn tombstone_from_key(a: Key) Value {
        return Value{ .key = a, .value = 0, .tombstone = true };
    }
};

test "table_memory: unit" {
    const testing = std.testing;
    const TableMemory = TableMemoryType(TestTable);

    const allocator = testing.allocator;
    var table_memory = try TableMemory.init(allocator, .mutable, "test");
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
