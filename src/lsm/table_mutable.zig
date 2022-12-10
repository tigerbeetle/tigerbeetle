const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const div_ceil = @import("../util.zig").div_ceil;
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

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

        pub const ValuesCache = SetAssociativeCache(
            Key,
            Value,
            Table.key_from_value,
            struct {
                inline fn hash(key: Key) u64 {
                    return std.hash.Wyhash.hash(0, mem.asBytes(&key));
                }
            }.hash,
            struct {
                inline fn equal(a: Key, b: Key) bool {
                    return compare_keys(a, b) == .eq;
                }
            }.equal,
            .{},
        );

        value_count_max: u32,
        values: Values = .{},

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

        /// `commit_entries_max` is the maximum number of Values that can be inserted by a single commit.
        pub fn init(
            allocator: mem.Allocator,
            values_cache: ?*ValuesCache,
            commit_entries_max: u32,
        ) !TableMutable {
            comptime assert(constants.lsm_batch_multiple > 0);
            assert(commit_entries_max > 0);

            const value_count_max = commit_entries_max * constants.lsm_batch_multiple;
            const data_block_count = div_ceil(value_count_max, Table.data.value_count_max);
            assert(data_block_count <= Table.data_block_count_max);

            var values: Values = .{};
            try values.ensureTotalCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            return TableMutable{
                .value_count_max = value_count_max,
                .values = values,
                .values_cache = values_cache,
            };
        }

        pub fn deinit(table: *TableMutable, allocator: mem.Allocator) void {
            table.values.deinit(allocator);
        }

        pub fn get(table: *const TableMutable, key: Key) ?*const Value {
            if (table.values.getKeyPtr(tombstone_from_key(key))) |value| {
                return value;
            }
            if (table.values_cache) |cache| {
                // Check the cache after the mutable table (see `values_cache` for explanation).
                if (cache.get(key)) |value| return value;
            }
            return null;
        }

        pub fn put(table: *TableMutable, value: *const Value) void {
            // If the key is already present in the hash map, the old key will not be overwritten
            // by the new one if using e.g. putAssumeCapacity(). Instead we must use the lower
            // level getOrPut() API and manually overwrite the old key.
            //const upsert = table.values.getOrPutAssumeCapacity(value.*);
            //upsert.key_ptr.* = value.*;
            const upsert = table.values.getOrPutAssumeCapacity(value.*);
            if (upsert.found_existing) {
                assert(Table.tombstone(upsert.key_ptr));
                _ = table.values.remove(value.*);
            } else {
                upsert.key_ptr.* = value.*;
            }

            // The hash map's load factor may allow for more capacity because of rounding:
            assert(table.values.count() <= table.value_count_max);

            if (table.values_cache) |cache| {
                cache.insert(key_from_value(value)).* = value.*;
            }
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            // If the key is already present in the hash map, the old key will not be overwritten
            // by the new one if using e.g. putAssumeCapacity(). Instead we must use the lower
            // level getOrPut() API and manually overwrite the old key.
            //const upsert = table.values.getOrPutAssumeCapacity(value.*);
            //upsert.key_ptr.* = tombstone_from_key(key_from_value(value));
            const existing = table.values.fetchRemove(value.*);
            if (existing != null) {
                // Put and remove cancel each other out.
                assert(!Table.tombstone(&existing.?.key));
            } else {
                const upsert = table.values.getOrPutAssumeCapacity(value.*);
                upsert.key_ptr.* = tombstone_from_key(key_from_value(value));
            }

            assert(table.values.count() <= table.value_count_max);
        }

        /// This may return `false` even when committing would succeed — it pessimistically
        /// assumes that none of the batch's keys are already in `table.values`.
        pub fn can_commit_batch(table: *TableMutable, batch_count: u32) bool {
            assert(batch_count <= table.value_count_max);
            return (table.count() + batch_count) <= table.value_count_max;
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.values.count() > 0);
            table.values.clearRetainingCapacity();
            assert(table.values.count() == 0);
        }

        pub fn count(table: *const TableMutable) u32 {
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

                if (table.values_cache) |cache| cache.insert(key_from_value(value)).* = value.*;
            }

            const values = values_max[0..i];
            assert(values.len == table.count());
            std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

            if (constants.verify and i > 0) {
                var a = values[0];
                for (values[1..]) |b| {
                    assert(compare_keys(key_from_value(&a), key_from_value(&b)) == .lt);
                    a = b;
                }
            }

            table.clear();
            assert(table.count() == 0);

            return values;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }
    };
}
