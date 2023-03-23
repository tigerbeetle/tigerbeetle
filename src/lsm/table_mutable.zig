const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;

fn ValuesCacheType(comptime Table: type, comptime tree_name: [:0]const u8) type {
    return @import("set_associative_cache.zig").SetAssociativeCache(
        Table.Key,
        Table.Value,
        Table.key_from_value,
        struct {
            inline fn hash(key: Table.Key) u64 {
                return std.hash.Wyhash.hash(0, mem.asBytes(&key));
            }
        }.hash,
        struct {
            inline fn equal(a: Table.Key, b: Table.Key) bool {
                return Table.compare_keys(a, b) == .eq;
            }
        }.equal,
        .{},
        tree_name,
    );
}

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

        const load_factor = 50;
        const Values = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, load_factor);

        pub const ValuesCache = ValuesCacheType(Table, tree_name);

        values: Values = .{},

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
            var values: Values = .{};
            try values.ensureTotalCapacity(allocator, value_count_max);
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
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;
            switch (usage) {
                .secondary_index => {
                    const existing = table.values.fetchRemove(value.*);
                    if (existing) |kv| {
                        // If there was a previous operation on this key then it must have been a remove.
                        // The put and remove cancel out.
                        assert(tombstone(&kv.key));
                    } else {
                        table.values.putAssumeCapacityNoClobber(value.*, {});
                    }
                },
                .general => {
                    // If the key is already present in the hash map, the old key will not be overwritten
                    // by the new one if using e.g. putAssumeCapacity(). Instead we must use the lower
                    // level getOrPut() API and manually overwrite the old key.
                    const upsert = table.values.getOrPutAssumeCapacity(value.*);
                    upsert.key_ptr.* = value.*;
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
                    const existing = table.values.fetchRemove(value.*);
                    if (existing) |kv| {
                        // The previous operation on this key then it must have been a put.
                        // The put and remove cancel out.
                        assert(!tombstone(&kv.key));
                    } else {
                        // If the put is already on-disk, then we need to follow it with a tombstone.
                        // The put and the tombstone may cancel each other out later during compaction.
                        table.values.putAssumeCapacityNoClobber(tombstone_from_key(key_from_value(value)), {});
                    }
                },
                .general => {
                    // If the key is already present in the hash map, the old key will not be overwritten
                    // by the new one if using e.g. putAssumeCapacity(). Instead we must use the lower
                    // level getOrPut() API and manually overwrite the old key.
                    const upsert = table.values.getOrPutAssumeCapacity(value.*);
                    upsert.key_ptr.* = tombstone_from_key(key_from_value(value));
                },
            }

            assert(table.values.count() <= value_count_max);
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.values.count() > 0);
            table.value_count_worst_case = 0;
            table.values.clearRetainingCapacity();
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
            var it = table.values.keyIterator();
            while (it.next()) |value| : (i += 1) {
                values_max[i] = value.*;

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
            std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

            table.clear();
            assert(table.count() == 0);

            return values;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }
    };
}

pub fn TableMutableTreeType(
    comptime Table: type,
    comptime tree_name: [:0]const u8,
    comptime TreeType: anytype,
) type {
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
        const Tree = TreeType(Key, Value, key_from_value, compare_keys);

        pub const ValuesCache = ValuesCacheType(Table, tree_name);

        value_count_worst_case: u32 = 0,
        values_cache: ?*ValuesCache,
        values_tree: Tree,

        pub fn init(allocator: mem.Allocator, values_cache: ?*ValuesCache) !TableMutable {
            var values_tree = try Tree.init(allocator, @intCast(u32, value_count_max));
            errdefer values_tree.deinit(allocator);

            return TableMutable{
                .values_cache = values_cache,
                .values_tree = values_tree,
            };
        }

        pub fn deinit(table: *TableMutable, allocator: mem.Allocator) void {
            table.values_tree.deinit(allocator);
        }

        pub fn get(table: *const TableMutable, key: Key) ?*const Value {
            if (table.values_tree.get(key)) |value| {
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

            const key = key_from_value(value);
            const entry = table.values_tree.get_or_put(key);
            switch (usage) {
                .secondary_index => {
                    if (entry.exists) {
                        // If there was a previous operation on this key then it must have been a
                        // remove. The put and remove cancel out.
                        assert(tombstone(entry.value));
                    } else {
                        entry.value.* = value.*;
                    }
                },
                .general => {
                    // Make sure to overwrite the old value if it exists.
                    entry.value.* = value.*;
                },
            }

            assert(table.values_tree.count() <= value_count_max);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;

            const key = key_from_value(value);
            const entry = table.values_tree.get_or_put(key);
            switch (usage) {
                .secondary_index => {
                    if (entry.exists) {
                        // The previous operation on this key then it must have been a put.
                        // The put and remove cancel out.
                        assert(!tombstone(entry.value));
                    } else {
                        // If the put is already on-disk, we need to follow it with a tombstone.
                        // The put and tombstone may cancel each other out later during compaction.
                        entry.value.* = tombstone_from_key(key);
                    }
                },
                .general => {
                    // Make sure to overwrite the old value if it exists.
                    entry.value.* = tombstone_from_key(key);
                },
            }

            assert(table.values_tree.count() <= value_count_max);
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.count() > 0);
            table.value_count_worst_case = 0;
            table.values_tree.clear();
            assert(table.values_tree.count() == 0);
        }

        pub fn count(table: *const TableMutable) u32 {
            const value_count = table.values_tree.count();
            assert(value_count <= value_count_max);
            return value_count;
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

            const values = table.values_tree.sort_into_and_clear(values_max);
            assert(table.count() == 0);

            table.value_count_worst_case = 0;
            return values;
        }
    };
}

pub fn AATreeType(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (value: *const Value) callconv(.Inline) Key,
    comptime compare_keys: fn (key: Key, key: Key) callconv(.Inline) math.Order,
) type {
    const AAList = std.MultiArrayList(struct {
        value: Value,
        links: [2]u32,
        stack: u32,
        level: u8,
    });

    return struct {
        const AATree = @This();

        list: AAList,
        root: u32 = 0,

        pub fn init(allocator: mem.Allocator, max_entries: u32) !AATree {
            var list = AAList{};
            try list.ensureTotalCapacity(allocator, max_entries);
            errdefer list.deinit(allocator);

            return AATree{ .list = list };
        }

        pub fn deinit(tree: *AATree, allocator: mem.Allocator) void {
            tree.list.deinit(allocator);
        }

        pub fn count(tree: *const AATree) u32 {
            return @intCast(u32, tree.list.len);
        }

        pub fn clear(tree: *AATree) void {
            tree.list.shrinkRetainingCapacity(0);
            tree.root = 0;
        }

        pub fn get(tree: *const AATree, key: Key) ?*const Value {
            var index = tree.root;
            const slice = tree.list.slice();
            while (true) {
                const slot = std.math.sub(u32, index, 1) catch return null;
                const value = &slice.items(.value)[slot];
                const cmp = compare_keys(key, key_from_value(value));
                if (cmp == .eq) return value;
                index = slice.items(.links)[slot][@boolToInt(cmp == .gt)];
            }
        }

        pub const AAEntry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn get_or_put(tree: *AATree, key: Key) AAEntry {
            var entry: AAEntry = undefined;
            tree.root = tree.insert(tree.root, &key, &entry);
            return entry;
        }

        fn insert(tree: *AATree, index: u32, key: *const Key, entry: *AAEntry) u32 {
            const slot = std.math.sub(u32, index, 1) catch {
                const new_slot = @intCast(u32, tree.list.addOneAssumeCapacity());
                const slice = tree.list.slice();

                slice.items(.links)[new_slot] = .{0, 0};
                slice.items(.level)[new_slot] = 1;

                entry.value = &slice.items(.value)[new_slot];
                entry.exists = false;
                return new_slot + 1;
            };

            const slice = tree.list.slice();
            entry.value = &slice.items(.value)[slot];

            const cmp = compare_keys(key.*, key_from_value(entry.value));
            entry.exists = cmp == .eq;
            if (entry.exists) return index;

            const link = &slice.items(.links)[slot][@boolToInt(cmp == .gt)];
            link.* = tree.insert(link.*, key, entry);
            return tree.split(tree.skew(index));
        }

        fn skew(tree: *const AATree, index: u32) u32 {
            const slot = std.math.sub(u32, index, 1) catch unreachable;
            const slice = tree.list.slice();

            const left_link = &slice.items(.links)[slot][0];
            const left_index = left_link.*;
            const left_slot = std.math.sub(u32, left_index, 1) catch return index;
            if (slice.items(.level)[left_slot] != slice.items(.level)[slot]) return index;

            left_link.* = index;
            mem.swap(u32, left_link, &slice.items(.links)[left_slot][1]);
            return left_index;
        }

        fn split(tree: *const AATree, index: u32) u32 {
            const slot = std.math.sub(u32, index, 1) catch unreachable;
            const slice = tree.list.slice();

            const right_link = &slice.items(.links)[slot][1];
            const right_index = right_link.*;
            const right_slot = std.math.sub(u32, right_index, 1) catch return index;

            const rr_index = slice.items(.links)[right_slot][1];
            const rr_slot = std.math.sub(u32, rr_index, 1) catch return index;
            if (slice.items(.level)[rr_slot] != slice.items(.level)[slot]) return index;

            right_link.* = index;
            mem.swap(u32, right_link, &slice.items(.links)[right_slot][0]);
            slice.items(.level)[right_slot] += 1;
            return right_index;
        }

        pub fn sort_into_and_clear(tree: *AATree, values: []Value) []const Value {
            const slice = tree.list.slice();
            assert(slice.len <= values.len);

            var top: u32 = 0;
            var index: u32 = 0;
            var current = tree.root;

            while (true) {
                while (std.math.sub(u32, current, 1) catch null) |slot| {
                    slice.items(.stack)[top] = slot;
                    top += 1;
                    current = slice.items(.links)[slot][0];
                }

                top = std.math.sub(u32, top, 1) catch break;
                const slot = slice.items(.stack)[top];
                current = slice.items(.links)[slot][1];

                const value = &slice.items(.value)[slot];
                values[index] = value.*;
                defer index += 1;

                if (std.math.sub(u32, index, 1) catch null) |prev_index| {
                    const prev_key = key_from_value(&values[prev_index]);
                    assert(compare_keys(prev_key, key_from_value(value)) != .gt);
                }
            }

            assert(index == tree.count());
            tree.clear();
            return values[0..index];
        }
    };
}
