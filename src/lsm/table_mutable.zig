const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const TableValuesType = @import("table_values.zig").TableValuesType;
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

/// Range queries are not supported on the TableMutable, it must first be made immutable.
pub fn TableMutableType(comptime Table: type, comptime tree_name: [:0]const u8) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const tombstone_from_key = Table.tombstone_from_key;
    const tombstone = Table.tombstone;
    const value_count_max = Table.value_count_max;
    const usage = Table.usage;

    return struct {
        const TableMutable = @This();

        const Values = TableValuesType(Table);

        pub const ValuesCache = SetAssociativeCache(
            Key,
            Value,
            Table.key_from_value,
            struct {
                inline fn hash(key: Key) u64 {
                    return stdx.fast_hash(&key);
                }
            }.hash,
            struct {
                inline fn equal(a: Key, b: Key) bool {
                    return compare_keys(a, b) == .eq;
                }
            }.equal,
            .{},
            tree_name,
        );

        values: Values,

        /// Rather than using values.count(), we count how many values we could have had if every
        /// operation had been on a different key. This means that mistakes in calculating
        /// value_count_max are much easier to catch when fuzzing, rather than requiring very
        /// specific workloads.
        /// Invariant: value_count_worst_case <= value_count_max
        value_count_worst_case: u32 = 0,

        /// This is used to accelerate point lookups and is not used for range queries.
        /// Secondary index trees used only for range queries can therefore set this to null.
        ///
        /// The values cache is only used for the latest snapshot for simplicity.
        /// Earlier snapshots will still be able to utilize the block cache.
        ///
        /// The values cache is updated (in bulk) when the mutable table is sorted and frozen,
        /// rather than updating on every `put()`/`remove()`.
        /// This amortizes cache inserts for hot keys in the mutable table, and avoids redundantly
        /// storing duplicate values in both the mutable table and values cache.
        // TODO Share cache between trees of different grooves:
        // "A set associative cache of values shared by trees with the same key/value sizes.
        // The value type will be []u8 and this will be shared by trees with the same value size."
        values_cache: ?*ValuesCache,

        pub fn init(allocator: mem.Allocator, values_cache: ?*ValuesCache) !TableMutable {
            var values = try Values.init(allocator);
            errdefer values.deinit(allocator);

            return TableMutable{
                .values = values,
                .values_cache = values_cache,
            };
        }

        pub fn deinit(table: *TableMutable, allocator: mem.Allocator) void {
            table.values.deinit(allocator);
        }

        pub fn get(table: *const TableMutable, key: Key) ?*const Value {
            if (table.values.find(key)) |value| {
                return value;
            }
            if (table.values_cache) |cache| {
                // Check the cache after the mutable table (see `values_cache` for explanation).
                if (cache.get(key)) |value| return value;
            }
            return null;
        }

        fn upsert(table: *TableMutable, value: *const Value, delete: bool) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;

            const key = key_from_value(value);
            const entry = table.values.upsert(key);

            if (usage == .secondary_index and entry.found_existing) {
                assert(tombstone(entry.value) == !delete);
            } else {
                entry.value.* = if (delete) tombstone_from_key(key) else value.*;
            }
        }

        pub inline fn put(table: *TableMutable, value: *const Value) void {
            return table.upsert(value, false);
        }

        pub inline fn remove(table: *TableMutable, value: *const Value) void {
            return table.upsert(value, true);
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.count() > 0);
            table.value_count_worst_case = 0;
            table.values.clear();
            assert(table.count() == 0);
        }

        pub fn count(table: *const TableMutable) u32 {
            const value_count = @intCast(u32, table.values.inserted().len);
            assert(value_count <= value_count_max);
            return value_count;
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_and_clear(table: *TableMutable, values: *Values) void {
            const inserted = table.values.inserted();
            assert(inserted.len > 0);
            assert(inserted.len <= value_count_max);
            assert(values.inserted().len == 0);

            // Sort the inserted value array directly. This invalidates all subsequent uses of
            // table.values.find/upsert/insert_no_clobber but that's fine as it will be cleared.
            std.sort.sort(Value, inserted, {}, sort_values_by_key_in_ascending_order);

            // Iterate the sorted values and insert them into the passed in TableValues.
            // Also insert them into the cache if any.
            for (inserted) |*value| {
                values.insert_no_clobber(value);

                if (table.values_cache) |cache| {
                    if (tombstone(value)) {
                        cache.remove(key_from_value(value));
                    } else {
                        cache.insert(value);
                    }
                }
            }

            // Finally, clear our table.
            table.clear();
            assert(table.count() == 0);
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }
    };
}
