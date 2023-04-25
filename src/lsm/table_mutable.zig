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
            const load_factor = 50;
            const capacity = math.ceilPowerOfTwo(u32, value_count_max * 100 / load_factor) catch unreachable;

            const Slot = u24;
            const slot_bytes = @bitSizeOf(Slot) / 8;
            comptime {
                assert(value_count_max <= std.math.maxInt(Slot));
            }

            const Data = struct {
                tags: [capacity]u8,
                slots: [capacity * slot_bytes]u8,
                active: [value_count_max / 8]u8,
                values: [value_count_max]Value,
            };

            len: u32 = 0,
            used: u32 = 0,
            data: *Data,

            pub fn init(allocator: mem.Allocator) !ValuesMap {
                var map = ValuesMap{ .data = try allocator.create(Data) };
                map.clear();
                return map;
            }

            pub fn deinit(map: *ValuesMap, allocator: mem.Allocator) void {
                allocator.destroy(map.data);
            }

            pub inline fn count(map: *const ValuesMap) u32 {
                return map.len;
            }

            pub fn clear(map: *ValuesMap) void {
                map.len = 0;
                map.used = 0;
                @memset(&map.data.tags, 0, @sizeOf(@TypeOf(map.data.tags)));
                @memset(&map.data.active, 0, @sizeOf(@TypeOf(map.data.active)));
            }

            pub const Search = struct {
                pos: u32,
                tag: u8,
                key: Key,

                pub fn create(key: Key) Search {
                    const hash = std.hash.Wyhash.hash(0, mem.asBytes(&key));
                    return .{
                        .pos = @truncate(u32, hash) % capacity,
                        .tag = @truncate(u8, hash >> (64 - 8)) | 0x80,
                        .key = key,
                    };
                }
            };

            pub fn find(map: *const ValuesMap, search: *Search, intent: enum { get, put }) ?*Value {
                const data = map.data;
                const probe = (2 * @as(u32, search.tag)) + 1; // Double hashing / quadratic probing.

                while (true) : (search.pos = (search.pos +% probe) % capacity) {
                    const slot_tag = &data.tags[search.pos];

                    // Check the tag before comparing the value. This acts like a bloom filter.
                    if (slot_tag.* == search.tag) {
                        const slot = @ptrCast(*align(1) Slot, &data.slots[search.pos * slot_bytes]).*;
                        assert(slot < value_count_max);
                        assert(slot < map.used);

                        const value = &data.values[slot];
                        if (compare_keys(search.key, key_from_value(value)) == .eq) {
                            return value;
                        }

                        // Unlikely, but two values had the same tag.
                        // Skip checking for empty slots below and just check the next one.
                        continue;
                    }

                    switch (intent) {
                        // When looking for a matching value, only stop when an empty slot is found.
                        // Keep scanning if it's removed (no 0x80) as that's a tombstone.
                        .get => if (slot_tag.* == 0) return null,
                        // When inserting, stop at their empty slot or removed slot to replace it.
                        .put => if (slot_tag.* & 0x80 == 0) return null,
                    }
                }
            }

            pub fn insert(map: *ValuesMap, search: *const Search) *Value {
                // Increment the map count.
                const data = map.data;
                assert(map.len < value_count_max);
                map.len += 1;

                // Mark the slot as used with its tag for future find()s.
                const slot_tag = &data.tags[search.pos];
                assert(slot_tag.* & 0x80 == 0);
                slot_tag.* = search.tag;

                // Reserve a slot index for data.values.
                const slot = @intCast(Slot, map.used);
                assert(slot < value_count_max);
                map.used += 1;

                // Mark the slot index as active for iteration.
                const mask = @as(u8, 1) << @intCast(u3, slot % 8);
                assert(data.active[slot / 8] & mask == 0);
                data.active[slot / 8] |= mask;

                // Commit the slot index and return the value.
                @ptrCast(*align(1) Slot, &data.slots[search.pos * slot_bytes]).* = slot;
                return &data.values[slot];
            }

            pub fn remove(map: *ValuesMap, search: *const Search) void {
                // Decrement the map count.
                const data = map.data;
                assert(map.len <= value_count_max);
                assert(map.len > 0);
                map.len -= 1;

                // Mark the slot as deleted (no 0x80, but still not 0 for empty).
                const slot_tag = &data.tags[search.pos];
                assert(slot_tag.* == search.tag);
                slot_tag.* = 0x1;

                // Get the previously inserted slot index.
                const slot = @ptrCast(*align(1) Slot, &data.slots[search.pos * slot_bytes]).*;
                assert(slot < value_count_max);
                assert(slot < map.used);

                // Mark the slot index as inactive for iteration.
                const mask = @as(u8, 1) << @intCast(u3, slot % 8);
                assert(data.active[slot / 8] & mask != 0);
                data.active[slot / 8] &= ~mask;
            }

            pub inline fn iterator(map: *const ValuesMap) Iterator {
                return .{
                    .slot = 0,
                    .used = map.used,
                    .data = map.data,
                };
            }

            pub const Iterator = struct {
                slot: Slot,
                used: u32,
                data: *const Data,

                pub fn next(it: *Iterator) ?*const Value {
                    while (true) {
                        it.used = math.sub(u32, it.used, 1) catch return null;

                        const slot = it.slot;
                        it.slot += 1;

                        // Only return values that we're marked active.
                        const mask = @as(u8, 1) << @intCast(u3, slot % 8);
                        if (it.data.active[slot / 8] & mask != 0) {
                            return &it.data.values[slot];
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
            if (table.values.count() > 0) {
                var search = ValuesMap.Search.create(key);
                if (table.values.find(&search, .get)) |value| {
                    return value;
                }
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

            var search = ValuesMap.Search.create(key_from_value(value));
            const found_existing = table.values.find(&search, .put);

            switch (usage) {
                .secondary_index => {
                    if (found_existing) |existing| {
                        // If there was a previous operation on this key then it must have been a remove.
                        // The put and remove cancel out.
                        assert(tombstone(existing));
                        table.values.remove(&search);
                    } else {
                        table.values.insert(&search).* = value.*;
                    }
                },
                .general => {
                    // Either overwrite the existing value with the new one, or insert the new one.
                    const value_ptr = found_existing orelse table.values.insert(&search);
                    value_ptr.* = value.*;
                },
            }

            // The hash map's load factor may allow for more capacity because of rounding:
            assert(table.values.count() <= value_count_max);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;

            var search = ValuesMap.Search.create(key_from_value(value));
            const found_existing = table.values.find(&search, .put);

            switch (usage) {
                .secondary_index => {
                    if (found_existing) |existing| {
                        // The previous operation on this key then it must have been a put.
                        // The put and remove cancel out.
                        assert(!tombstone(existing));
                        table.values.remove(&search);
                    } else {
                        // If the put is already on-disk, then we need to follow it with a tombstone.
                        // The put and the tombstone may cancel each other out later during compaction.
                        table.values.insert(&search).* = tombstone_from_key(search.key);
                    }
                },
                .general => {
                    // Either overwrite the existing value with tombstone, or insert a new tombstone.
                    const value_ptr = found_existing orelse table.values.insert(&search);
                    value_ptr.* = tombstone_from_key(search.key);
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
