const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;
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
            tree_name,
        );

        const ValuesMap = struct {
            const Values = struct {
                active: [value_count_max / 8]u8,
                items: [value_count_max]Value,
            };

            const load_factor = 50;
            const IndexMap = std.HashMapUnmanaged(u32, void, IndexMapContext, load_factor);

            const IndexMapContext = struct {
                values: *const Values,
                search_key: Key,
                search_index: u32 = math.maxInt(u32),

                inline fn key_from_index(ctx: *const IndexMapContext, index: u32) Key {
                    if (index == ctx.search_index) return ctx.search_key;
                    return key_from_value(&ctx.values.items[index]);
                }

                pub inline fn eql(ctx: IndexMapContext, a: u32, b: u32) bool {
                    return compare_keys(ctx.key_from_index(a), ctx.key_from_index(b)) == .eq;
                }

                pub inline fn hash(ctx: IndexMapContext, index: u32) u64 {
                    const key = ctx.key_from_index(index);
                    return std.hash.Wyhash.hash(0, mem.asBytes(&key));
                }
            };

            index_map: IndexMap,
            values: *Values,
            active: u32,

            pub fn init(allocator: mem.Allocator) !ValuesMap {
                var index_map = IndexMap{};
                try index_map.ensureTotalCapacityContext(allocator, value_count_max, undefined);
                errdefer index_map.deinit(allocator);

                const values = try allocator.create(Values);
                errdefer allocator.destroy(values);

                return ValuesMap{
                    .index_map = index_map,
                    .values = values,
                    .active = 0,
                };
            }

            pub fn deinit(map: *ValuesMap, allocator: mem.Allocator) void {
                allocator.destroy(map.values);
                map.index_map.deinit(allocator);
            }

            pub inline fn count(map: *const ValuesMap) u32 {
                return map.index_map.count();
            }

            pub inline fn clear(map: *ValuesMap) void {
                map.index_map.clearRetainingCapacity();
                map.active = 0;
            }

            pub fn find(map: *const ValuesMap, key: Key) ?*const Value {
                const ctx = IndexMapContext{ .values = map.values, .search_key = key };
                const index = map.index_map.getKeyAdapted(ctx.search_index, ctx) orelse return null;
                return &map.values.items[index];
            }

            pub fn upsert(map: *ValuesMap, value: *const Value) *Value {
                const ctx = IndexMapContext{ .values = map.values, .search_key = key_from_value(value) };
                const result = map.index_map.getOrPutAssumeCapacityContext(ctx.search_index, ctx);
                if (result.found_existing) {
                    return &map.values.items[result.key_ptr.*];
                }

                const index = @intCast(u32, map.active);
                map.active += 1;

                result.key_ptr.* = index;
                map.values.active[index / 8] |= @as(u8, 1) << @intCast(u3, index % 8);
                return &map.values.items[index];
            }

            pub fn fetch_remove(map: *ValuesMap, value: *const Value) ?*Value {
                const ctx = IndexMapContext{ .values = map.values, .search_key = key_from_value(value) };
                const kv = map.index_map.fetchRemoveContext(ctx.search_index, ctx) orelse return null;
                const index = kv.key;

                map.values.active[index / 8] &= ~(@as(u8, 1) << @intCast(u3, index % 8));
                return &map.values.items[index];
            }

            pub fn insert_no_clobber(map: *ValuesMap, value: Value) void {
                const index = @intCast(u32, map.active);
                map.active += 1;

                const ctx = IndexMapContext{
                    .values = map.values,
                    .search_key = key_from_value(&value),
                    .search_index = index,
                };

                map.index_map.putAssumeCapacityNoClobberContext(ctx.search_index, {}, ctx);
                map.values.active[index / 8] |= @as(u8, 1) << @intCast(u3, index % 8);
                map.values.items[index] = value;
            }

            pub inline fn iterator(map: *const ValuesMap) Iterator {
                return .{
                    .index = 0,
                    .active = map.active,
                    .values = map.values,
                };
            }

            pub const Iterator = struct {
                index: u32,
                active: u32,
                values: *const Values,

                pub fn next(it: *Iterator) ?*const Value {
                    while (true) {
                        it.active = math.sub(u32, it.active, 1) catch return null;
                        const index = it.index;
                        it.index += 1;

                        const mask = @as(u8, 1) << @intCast(u3, index % 8);
                        if (it.values.active[index / 8] & mask != 0) {
                            return &it.values.items[index];
                        }
                    }
                }
            };
        };

        values: ValuesMap,

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

        pub fn init(
            allocator: mem.Allocator,
            values_cache: ?*ValuesCache,
        ) !TableMutable {
            var values = try ValuesMap.init(allocator);
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

        pub fn put(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;
            switch (usage) {
                .secondary_index => {
                    if (table.values.fetch_remove(value)) |existing| {
                        // If there was a previous operation on this key then it must have been a remove.
                        // The put and remove cancel out.
                        assert(tombstone(existing));
                    } else {
                        table.values.insert_no_clobber(value.*);
                    }
                },
                .general => {
                    // Overwrite the old key and value if any.
                    table.values.upsert(value).* = value.*;
                },
            }

            // The hash map's load factor may allow for more capacity because of rounding:
            assert(table.values.count() <= value_count_max);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;
            switch (usage) {
                .secondary_index => {
                    if (table.values.fetch_remove(value)) |existing| {
                        // The previous operation on this key then it must have been a put.
                        // The put and remove cancel out.
                        assert(!tombstone(existing));
                    } else {
                        // If the put is already on-disk, then we need to follow it with a tombstone.
                        // The put and the tombstone may cancel each other out later during compaction.
                        table.values.insert_no_clobber(tombstone_from_key(key_from_value(value)));
                    }
                },
                .general => {
                    // Overwrite the old key and value if any.
                    table.values.upsert(value).* = tombstone_from_key(key_from_value(value));
                },
            }

            assert(table.values.count() <= value_count_max);
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.values.count() > 0);
            table.value_count_worst_case = 0;
            table.values.clear();
            assert(table.values.count() == 0);
        }

        pub fn count(table: *const TableMutable) u32 {
            const value = @intCast(u32, table.values.count());
            assert(value <= value_count_max);
            return value;
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_values_and_clear(
            table: *TableMutable,
            values_max: []Value,
        ) []const Value {
            assert(table.count() > 0);
            assert(table.count() <= value_count_max);
            assert(table.count() <= values_max.len);
            assert(values_max.len == value_count_max);

            var i: usize = 0;
            var sorted = true;
            var it = table.values.iterator();
            while (it.next()) |value| : (i += 1) {
                values_max[i] = value.*;

                if (i > 0 and sorted) {
                    const prev_key = key_from_value(&values_max[i - 1]);
                    sorted = compare_keys(prev_key, key_from_value(value)) != .gt;
                }

                if (table.values_cache) |cache| {
                    if (tombstone(value)) {
                        cache.remove(key_from_value(value));
                    } else {
                        cache.insert(value);
                    }
                }
            }

            const values = values_max[0..i];
            assert(values.len == table.count());
            if (!sorted) std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

            table.clear();
            assert(table.count() == 0);

            return values;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }
    };
}
