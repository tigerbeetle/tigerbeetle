const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

/// Range queries are not supported on the TableMutable, it must first be made immutable.
pub fn TableMutableType(comptime Table: type, comptime tree_name: [:0]const u8) type {
    return TableMutableTreeType(Table, tree_name, HashMapTreeType);
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
    const Tree = TreeType(Table);

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

        values_tree: Tree,

        /// Rather than using tree.count(), we count how many values we could have had if every
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
            var tree = try Tree.init(allocator);
            errdefer tree.deinit(allocator);

            return TableMutable{
                .values_tree = tree,
                .values_cache = values_cache,
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
            const entry = table.values_tree.upsert(key);
            switch (usage) {
                .secondary_index => {
                    if (entry.exists) {
                        // If there was a previous operation on this key, it must have been remove.
                        // The put and remove cancel out.
                        assert(tombstone(entry.value));
                    } else {
                        entry.value.* = value.*;
                    }
                },
                .general => {
                    // Overwrite the existing key and value.
                    entry.value.* = value.*;
                },
            }

            assert(table.count() <= value_count_max);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;

            const key = key_from_value(value);
            const entry = table.values_tree.upsert(key);
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
                    // Overwrite the existing key and value with a tombstone.
                    entry.value.* = tombstone_from_key(key);
                },
            }

            assert(table.count() <= value_count_max);
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.values_tree.count() > 0);
            table.value_count_worst_case = 0;
            table.values_tree.clear();
            assert(table.values_tree.count() == 0);
        }

        pub fn count(table: *const TableMutable) u32 {
            const value = @intCast(u32, table.values_tree.count());
            assert(value <= value_count_max);
            return value;
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_values_and_clear(
            table: *TableMutable,
            values_max: []Value,
        ) []const Value {
            const table_count = table.count();
            assert(table_count > 0);
            assert(table_count <= value_count_max);
            assert(table_count <= values_max.len);
            assert(values_max.len == value_count_max);

            var i: u32 = 0;
            var it = table.values_tree.iterate_sort_clear(values_max);
            while (it.next()) |value| : (i += 1) {
                if (table.values_cache) |cache| {
                    if (tombstone(value)) {
                        cache.remove(key_from_value(value));
                    } else {
                        cache.insert(value);
                    }
                }
            }

            const values = values_max[0..i];
            assert(values.len == table_count);
            assert(table.count() == 0);

            table.value_count_worst_case = 0;
            return values;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }
    };
}

pub fn HashMapTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;
    const tombstone_from_key = Table.tombstone_from_key;

    return struct {
        const Tree = @This();

        const load_factor = 50;
        const Map = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, load_factor);

        map: Map,

        pub fn init(allocator: mem.Allocator) !Tree {
            var map: Map = .{};
            try map.ensureTotalCapacity(allocator, value_count_max);
            errdefer map.deinit(allocator);

            return Tree{ .map = map };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.map.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return tree.map.count();
        }

        pub fn clear(tree: *Tree) void {
            tree.map.clearRetainingCapacity();
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            return tree.map.getKeyPtr(tombstone_from_key(key));
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            const result = tree.map.getOrPutAssumeCapacity(tombstone_from_key(key));
            return .{ .value = result.key_ptr, .exists = result.found_existing };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);
            return .{ .tree = tree, .keys = tree.map.keyIterator(), .values_max = values_max };
        }

        pub const Iterator = struct {
            tree: *Tree,
            keys: Map.KeyIterator,
            add: u32 = 0,
            values_max: []Value,

            fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
                return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
            }

            pub fn next(it: *Iterator) ?*const Value {
                if (it.keys.next()) |value| {
                    it.values_max[it.add] = value.*;
                    it.add += 1;
                    return value;
                }

                const values = it.values_max[0..it.add];
                assert(values.len == it.tree.count());
                std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

                it.tree.clear();
                assert(it.tree.count() == 0);
                return null;
            }
        };
    };
}

pub fn HashMapSlotSortTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;
    const tombstone_from_key = Table.tombstone_from_key;

    return struct {
        const Tree = @This();

        const load_factor = 50;
        const Map = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, load_factor);
        const Slots = std.ArrayListUnmanaged(u32);

        map: Map,
        slots: Slots,

        pub fn init(allocator: mem.Allocator) !Tree {
            var map: Map = .{};
            try map.ensureTotalCapacity(allocator, value_count_max);
            errdefer map.deinit(allocator);

            var slots = try Slots.initCapacity(allocator, value_count_max);
            errdefer slots.deinit(allocator);

            return Tree{ .map = map, .slots = slots };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.slots.deinit(allocator);
            tree.map.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return tree.map.count();
        }

        pub fn clear(tree: *Tree) void {
            tree.map.clearRetainingCapacity();
            tree.slots.clearRetainingCapacity();
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            return tree.map.getKeyPtr(tombstone_from_key(key));
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            const result = tree.map.getOrPutAssumeCapacity(tombstone_from_key(key));

            if (!result.found_existing) {
                const slot = tree.slots.addOneAssumeCapacity();
                const offset = @ptrToInt(result.key_ptr) - @ptrToInt(tree.map.keyIterator().items);
                slot.* = @intCast(u32, offset / @sizeOf(Value));
            }

            return .{ .value = result.key_ptr, .exists = result.found_existing };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);
            return .{ .tree = tree, .keys = tree.map.keyIterator(), .values_max = values_max };
        }

        pub const Iterator = struct {
            tree: *Tree,
            keys: Map.KeyIterator,
            add: u32 = 0,
            values_max: []Value,

            fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
                return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
            }

            pub fn next(it: *Iterator) ?*const Value {
                if (it.keys.next()) |value| {
                    it.add += 1;
                    return value;
                }

                const SortContext = struct {
                    values: [*]const Value,

                    fn less_than(context: @This(), a_slot: u32, b_slot: u32) bool {
                        const a_key = key_from_value(&context.values[a_slot]);
                        const b_key = key_from_value(&context.values[b_slot]);
                        return compare_keys(a_key, b_key) == .lt;
                    }
                };

                assert(it.add == it.tree.count());
                it.keys = it.tree.map.keyIterator();

                const slots = it.tree.slots.items;
                const context = SortContext{ .values = it.keys.items };
                std.sort.sort(u32, slots, context, SortContext.less_than);
                for (slots) |slot, i| it.values_max[i] = context.values[slot];

                it.tree.clear();
                assert(it.tree.count() == 0);
                return null;
            }
        };
    };
}

pub fn SlotMapTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;
    const tombstone_from_key = Table.tombstone_from_key;

    return struct {
        const Tree = @This();

        const Values = std.ArrayListUnmanaged(Value);

        const lookup_slot = @intCast(u32, value_count_max + 1);
        const SlotMapContext = struct {
            values: *const Values,
            lookup: *const Value,

            inline fn value_ptr(context: SlotMapContext, slot: u32) *const Value {
                if (slot == lookup_slot) return context.lookup;
                return &context.values.items[slot];
            }

            pub fn eql(context: SlotMapContext, a_slot: u32, b_slot: u32) bool {
                const a_value = context.value_ptr(a_slot).*;
                const b_value = context.value_ptr(b_slot).*;
                return (Table.HashMapContextValue{}).eql(a_value, b_value);
            }

            pub fn hash(context: SlotMapContext, hash_slot: u32) u64 {
                const value = context.value_ptr(hash_slot).*;
                return (Table.HashMapContextValue{}).hash(value);
            }
        };

        const load_factor = 50;
        const SlotMap = std.HashMapUnmanaged(u32, void, SlotMapContext, load_factor);

        slots: SlotMap,
        values: Values,

        pub fn init(allocator: mem.Allocator) !Tree {
            var slots = SlotMap{};
            try slots.ensureTotalCapacityContext(allocator, value_count_max, undefined);
            errdefer slots.deinit(allocator);

            var values = try Values.initCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            return Tree{ .slots = slots, .values = values };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.values.deinit(allocator);
            tree.slots.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.values.items.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.slots.clearRetainingCapacity();
            assert(tree.slots.count() == 0);

            tree.values.clearRetainingCapacity();
            assert(tree.count() == 0);
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            const lookup = tombstone_from_key(key);
            const context = SlotMapContext{ .values = &tree.values, .lookup = &lookup };
            const slot = tree.slots.getKeyAdapted(lookup_slot, context) orelse return null;
            return &tree.values.items[slot];
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            const lookup = tombstone_from_key(key);
            const context = SlotMapContext{ .values = &tree.values, .lookup = &lookup };
            const result = tree.slots.getOrPutAssumeCapacityContext(lookup_slot, context);

            if (result.found_existing) {
                const value = &tree.values.items[result.key_ptr.*];
                return .{ .value = value, .exists = true };
            }

            const slot = @intCast(u32, tree.values.items.len);
            result.key_ptr.* = slot;

            const value = tree.values.addOneAssumeCapacity();
            return .{ .value = value, .exists = false };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);

            // Values are allocated in tree.values contiguously and tree.slots will be cleared.
            // Reuse tree.slots memory as indexes to sort tree.values.
            const slots = tree.slots.keyIterator().items[0..tree.count()];
            for (slots) |*s, i| s.* = @intCast(u32, i);

            const SortContext = struct {
                values: *const Values,

                fn less_than(context: @This(), a_slot: u32, b_slot: u32) bool {
                    const a_key = key_from_value(&context.values.items[a_slot]);
                    const b_key = key_from_value(&context.values.items[b_slot]);
                    return compare_keys(a_key, b_key) == .lt;
                }
            };

            const context = SortContext{ .values = &tree.values };
            std.sort.sort(u32, slots, context, SortContext.less_than);
            return .{ .tree = tree, .slots = slots, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            tree: *Tree,
            index: u32 = 0,
            slots: []const u32,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                if (it.index >= it.slots.len) {
                    it.tree.clear();
                    return null;
                }

                const slot = it.slots[it.index];
                it.index += 1;

                const value = &it.tree.values.items[slot];
                it.values_max[0] = value.*;
                it.values_max += 1;
                return value;
            }
        };
    };
}

pub fn F14TreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const Tree = @This();

        comptime {
            assert(value_count_max <= std.math.maxInt(u24));
        }

        const Values = std.ArrayListUnmanaged(Value);
        const List = std.MultiArrayList(struct {
            tag: struct { data: [16]u8 align(@alignOf(u32)) },
            slot: struct { data: [(24 * 15) / 8]u8 },
        });

        const capacity = @intCast(u32, value_count_max);
        const item_capacity = math.ceilPowerOfTwo(u32, capacity * 2) catch unreachable;
        const list_capacity = item_capacity / 16;

        values: Values,
        list: List,

        pub fn init(allocator: mem.Allocator) !Tree {
            var values = try Values.initCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            var list = List{};
            try list.ensureTotalCapacity(allocator, list_capacity);
            list.len = list_capacity; // We want access to all items (uninitialized) immediately.
            errdefer list.deinit(allocator);

            var tree = Tree{ .values = values, .list = list };
            tree.clear(); // Importantly, zeroes out list.items(.tag).
            return tree;
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.list.deinit(allocator);
            tree.values.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.values.items.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.values.clearRetainingCapacity();
            const tags = tree.list.items(.tag).ptr;
            @memset(@ptrCast([*]u8, tags), 0, item_capacity); // Constant size memset() for perf.
        }

        inline fn search(tree: *const Tree, key: Key, comptime reserve: bool) ?*Value {
            const slice = tree.list.slice();
            const tags = slice.items(.tag);
            const slots = slice.items(.slot);

            const hash = std.hash_map.getAutoHashFn(Key, Table.HashMapContextValue)(.{}, key);
            const tag = @truncate(u8, hash >> (64 - 8)) | 0x80; // Set high bit for `inserted`.
            const probe = (@as(u32, tag) << 1) + 1; // Probe is logarithmic: (2 * i) + 1.

            var index = hash;
            var tries: u32 = @ctz(u32, list_capacity) - 1;
            while (true) : (index +%= probe) {
                const pos = index % list_capacity;
                const tag_ptr = &tags[pos].data;
                const overflow_ptr = &tag_ptr[16 - 1];

                // Scan the entire chunk using SIMD.
                const mask = (~@as(u16, 0)) >> 1;
                const chunk = @as(meta.Vector(16, u8), tag_ptr.*);

                // Check values for slots which match our tag.
                var match = @ptrCast(*const u16, &(chunk == @splat(16, tag))).* & mask;
                while (match != 0) : (match &= match - 1) {
                    const offset: u8 = @ctz(u16, match);
                    const slot = @ptrCast(*align(1) u24, &slots[pos].data[offset * 3]).*;
                    const value = &tree.values.items[slot];
                    if (compare_keys(key, key_from_value(value)) == .eq) return value;
                }

                if (!reserve) {
                    // No keys overflowed to other chunks, so search is over.
                    if (overflow_ptr.* == 0) return null;
                    // Stop searching when the probe would start cycling over.
                    tries = math.sub(u32, tries, 1) catch return null;
                    continue;
                }

                // Check if we can reserve into an empty slot here.
                const empty = @ptrCast(*const u16, &(chunk == @splat(16, @as(u8, 0)))).* & mask;
                if (empty == 0) {
                    overflow_ptr.* +|= 1;
                    continue;
                }

                const offset: u8 = @ctz(u16, empty);
                const slot = @intCast(u24, tree.count());

                // Store the tag and (soon to be) allocated value slot.
                @ptrCast(*align(1) u24, &slots[pos].data[offset * 3]).* = slot;
                tag_ptr[offset] = tag;
                return null;
            }
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            return tree.search(key, false);
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            const maybe_value = tree.search(key, true);
            return .{
                .value = maybe_value orelse tree.values.addOneAssumeCapacity(),
                .exists = maybe_value != null,
            };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);

            // Values are allocated in tree.values contiguously and tree.slots will be cleared.
            // Reuse tree.slots memory as indexes to sort tree.values.
            const slots = @intToPtr([*]u32, @ptrToInt(tree.list.bytes))[0..tree.count()];
            for (slots) |*s, i| s.* = @intCast(u32, i);

            const SortContext = struct {
                values: *const Values,

                fn less_than(context: @This(), a_slot: u32, b_slot: u32) bool {
                    const a_key = key_from_value(&context.values.items[a_slot]);
                    const b_key = key_from_value(&context.values.items[b_slot]);
                    return compare_keys(a_key, b_key) == .lt;
                }
            };

            const context = SortContext{ .values = &tree.values };
            std.sort.sort(u32, slots, context, SortContext.less_than);
            return .{ .tree = tree, .slots = slots, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            tree: *Tree,
            index: u32 = 0,
            slots: []const u32,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                if (it.index >= it.slots.len) {
                    it.tree.clear();
                    return null;
                }

                const slot = it.slots[it.index];
                it.index += 1;

                const value = &it.tree.values.items[slot];
                it.values_max[0] = value.*;
                it.values_max += 1;
                return value;
            }
        };
    };
}

pub fn RobinHoodTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const Tree = @This();

        comptime {
            assert(value_count_max < std.math.maxInt(u24));
        }

        const Values = std.ArrayListUnmanaged(Value);
        const Slot = packed struct {
            probe: u8 align(@alignOf(u32)) = 0,
            ref: u24 = 0,
        };

        const capacity = @intCast(u32, value_count_max);
        const slot_capacity = math.ceilPowerOfTwo(u32, capacity * 2) catch unreachable;

        values: Values,
        slots: []Slot,

        pub fn init(allocator: mem.Allocator) !Tree {
            var values = try Values.initCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            const slots = try allocator.alloc(Slot, slot_capacity);
            errdefer allocator.free(slots);

            var tree = Tree{ .values = values, .slots = slots };
            tree.clear();
            return tree;
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            allocator.free(tree.slots);
            tree.values.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.values.items.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.values.clearRetainingCapacity();
            mem.set(Slot, tree.slots, Slot{});
        }

        inline fn reduce(comptime Int: type, hash: u64, comptime range: ?comptime_int) Int {
            const r = range orelse return @truncate(Int, hash >> (64 - @bitSizeOf(Int)));
            const v = hash >> (64 - @bitSizeOf(math.IntFittingRange(0, r)));
            return @truncate(Int, v % r);
        }

        inline fn search(tree: *const Tree, key: Key, comptime reserve: bool) ?*Value {
            const hash = std.hash_map.getAutoHashFn(Key, Table.HashMapContextValue)(.{}, key);
            const new_ref = @intCast(u24, tree.values.items.len) + 1;
            var new_slot = Slot{ .probe = 0, .ref = new_ref };
            var index = reduce(u32, hash, slot_capacity);

            while (true) {
                const slot = tree.slots[index];
                const ref = math.sub(u32, slot.ref, 1) catch {
                    if (reserve) tree.slots[index] = new_slot;
                    return null;
                };

                if (new_slot.ref == new_ref) {
                    const value = &tree.values.items[ref];
                    if (compare_keys(key, key_from_value(value)) == .eq) return value;
                }

                if (slot.probe < new_slot.probe) {
                    if (!reserve) return null;
                    tree.slots[index] = new_slot;
                    new_slot = slot;
                }

                index = (index + 1) % slot_capacity;
                new_slot.probe += 1;
            }
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            return tree.search(key, false);
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            const maybe_value = tree.search(key, true);
            return .{
                .value = maybe_value orelse tree.values.addOneAssumeCapacity(),
                .exists = maybe_value != null,
            };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);

            // Values are allocated in tree.values contiguously and tree.slots will be cleared.
            // Reuse tree.slots memory as indexes to sort tree.values.
            const slots = @ptrCast([*]u32, tree.slots.ptr)[0..tree.count()];
            for (slots) |*s, i| s.* = @intCast(u32, i);

            const SortContext = struct {
                values: *const Values,

                fn less_than(context: @This(), a_slot: u32, b_slot: u32) bool {
                    const a_key = key_from_value(&context.values.items[a_slot]);
                    const b_key = key_from_value(&context.values.items[b_slot]);
                    return compare_keys(a_key, b_key) == .lt;
                }
            };

            const context = SortContext{ .values = &tree.values };
            std.sort.sort(u32, slots, context, SortContext.less_than);
            return .{ .tree = tree, .slots = slots, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            tree: *Tree,
            index: u32 = 0,
            slots: []const u32,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                if (it.index >= it.slots.len) {
                    it.tree.clear();
                    return null;
                }

                const slot = it.slots[it.index];
                it.index += 1;

                const value = &it.tree.values.items[slot];
                it.values_max[0] = value.*;
                it.values_max += 1;
                return value;
            }
        };
    };
}

pub fn SwissMapTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const Tree = @This();

        const Tag = packed struct {
            exists: bool = false,
            fingerprint: u7 = 0,
        };

        const Values = std.ArrayListUnmanaged(Value);
        const List = std.MultiArrayList(struct {
            slot: u32,
            tag: Tag,
        });

        const capacity = @intCast(u32, value_count_max);
        const slot_capacity = capacity * 2;

        values: Values,
        list: List,

        pub fn init(allocator: mem.Allocator) !Tree {
            var values = try Values.initCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            var list = List{};
            try list.ensureTotalCapacity(allocator, slot_capacity);
            errdefer list.deinit(allocator);

            var tree = Tree{ .values = values, .list = list };
            tree.clear();
            return tree;
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.list.deinit(allocator);
            tree.values.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.values.items.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.values.clearRetainingCapacity();
            mem.set(Tag, tree.list.items(.tag).ptr[0..slot_capacity], Tag{});
        }

        inline fn reduce(comptime Int: type, hash: u64, comptime range: ?comptime_int) Int {
            const r = range orelse return @truncate(Int, hash >> (64 - @bitSizeOf(Int)));
            const v = hash >> (64 - @bitSizeOf(math.IntFittingRange(0, r)));
            return @truncate(Int, v % r);
        }

        fn search(tree: *const Tree, key: Key, reserve: bool) ?*Value {
            const slice = tree.list.slice();
            const tags = slice.items(.tag).ptr[0..slot_capacity];
            const slots = slice.items(.slot).ptr[0..slot_capacity];

            const hash = std.hash_map.getAutoHashFn(Key, Table.HashMapContextValue)(.{}, key);
            const fingerprint = reduce(u7, hash, null);
            var index = reduce(u32, hash, slot_capacity);

            const tag_match = @bitCast(u8, Tag{ .exists = true, .fingerprint = fingerprint });
            const tag_empty = @bitCast(u8, Tag{});

            while (true) {
                const scan_lanes = 16;
                const ScanVector = meta.Vector(scan_lanes, u8);
                const ScanMask = meta.Int(.unsigned, scan_lanes);

                var next_index = index + scan_lanes;
                defer index = next_index % slot_capacity;

                // Scan linearly until there's enough contiguous memory to scan with Vectors.
                if (next_index > slot_capacity) {
                    const tag = tags[index];

                    // Check if there's an empty slot to insert into.
                    if (@bitCast(u8, tag) == tag_empty) {
                        if (reserve) {
                            tags[index] = @bitCast(Tag, tag_match);
                            slots[index] = tree.count();
                        }
                        return null;
                    }

                    // Check if has a matching tag and value.
                    if (@bitCast(u8, tag) == tag_match) {
                        const value = &tree.values.items[slots[index]];
                        if (compare_keys(key, key_from_value(value)) == .eq) {
                            return value;
                        }
                    }

                    // Check the next entry.
                    next_index = index + 1;
                    continue;
                }

                // Read a bunch of tags into a Vector (SIMD).
                const lane: ScanVector = @ptrCast(*[scan_lanes]u8, tags[index..][0..scan_lanes]).*;

                // Check if there's any matching tags with matching values.
                var match = @ptrCast(*const ScanMask, &(lane == @splat(scan_lanes, tag_match))).*;
                while (match != 0) : (match &= match - 1) {
                    const match_index = index + @ctz(ScanMask, match);
                    const value = &tree.values.items[slots[match_index]];
                    if (compare_keys(key, key_from_value(value)) == .eq) {
                        return value;
                    }
                }

                // Check if there's any empty tags for us to insert into.
                const empty = @ptrCast(*const ScanMask, &(lane == @splat(scan_lanes, tag_empty))).*;
                if (empty != 0) {
                    const empty_index = index + @ctz(ScanMask, empty);
                    if (reserve) {
                        tags[empty_index] = @bitCast(Tag, tag_match);
                        slots[empty_index] = tree.count();
                    }
                    return null;
                }
            }
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            return tree.search(key, false);
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            const maybe_value = tree.search(key, true);
            return .{
                .value = maybe_value orelse tree.values.addOneAssumeCapacity(),
                .exists = maybe_value != null,
            };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);

            // Values are allocated in tree.values contiguously and tree.slots will be cleared.
            // Reuse tree.slots memory as indexes to sort tree.values.
            const slots = tree.list.items(.slot).ptr[0..tree.count()];
            for (slots) |*s, i| s.* = @intCast(u32, i);

            const SortContext = struct {
                values: *const Values,

                fn less_than(context: @This(), a_slot: u32, b_slot: u32) bool {
                    const a_key = key_from_value(&context.values.items[a_slot]);
                    const b_key = key_from_value(&context.values.items[b_slot]);
                    return compare_keys(a_key, b_key) == .lt;
                }
            };

            const context = SortContext{ .values = &tree.values };
            std.sort.sort(u32, slots, context, SortContext.less_than);
            return .{ .tree = tree, .slots = slots, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            tree: *Tree,
            index: u32 = 0,
            slots: []const u32,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                if (it.index >= it.slots.len) {
                    it.tree.clear();
                    return null;
                }

                const slot = it.slots[it.index];
                it.index += 1;

                const value = &it.tree.values.items[slot];
                it.values_max[0] = value.*;
                it.values_max += 1;
                return value;
            }
        };
    };
}

pub fn AATreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const Tree = @This();

        const List = std.MultiArrayList(struct {
            value: Value,
            links: [2]u32,
            stack: u32,
            level: u8,
        });

        list: List,
        root: u32 = 0,

        pub fn init(allocator: mem.Allocator) !Tree {
            var list = List{};
            try list.ensureTotalCapacity(allocator, value_count_max);
            errdefer list.deinit(allocator);

            return Tree{ .list = list };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.list.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.list.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.list.shrinkRetainingCapacity(0);
            tree.root = 0;
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            var index = tree.root;
            const slice = tree.list.slice();
            while (true) {
                const slot = math.sub(u32, index, 1) catch return null;
                const value = &slice.items(.value)[slot];
                const cmp = compare_keys(key, key_from_value(value));
                if (cmp == .eq) return value;
                index = slice.items(.links)[slot][@boolToInt(cmp == .gt)];
            }
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            var entry: Entry = undefined;
            tree.root = tree.insert(tree.root, &key, &entry);
            return entry;
        }

        fn insert(tree: *Tree, index: u32, key: *const Key, entry: *Entry) u32 {
            const slot = math.sub(u32, index, 1) catch {
                const new_slot = @intCast(u32, tree.list.addOneAssumeCapacity());
                const slice = tree.list.slice();

                slice.items(.links)[new_slot] = .{ 0, 0 };
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

        fn skew(tree: *const Tree, index: u32) u32 {
            const slot = math.sub(u32, index, 1) catch unreachable;
            const slice = tree.list.slice();

            const left_link = &slice.items(.links)[slot][0];
            const left_index = left_link.*;
            const left_slot = math.sub(u32, left_index, 1) catch return index;
            if (slice.items(.level)[left_slot] != slice.items(.level)[slot]) return index;

            left_link.* = index;
            mem.swap(u32, left_link, &slice.items(.links)[left_slot][1]);
            return left_index;
        }

        fn split(tree: *const Tree, index: u32) u32 {
            const slot = math.sub(u32, index, 1) catch unreachable;
            const slice = tree.list.slice();

            const right_link = &slice.items(.links)[slot][1];
            const right_index = right_link.*;
            const right_slot = math.sub(u32, right_index, 1) catch return index;

            const rr_index = slice.items(.links)[right_slot][1];
            const rr_slot = math.sub(u32, rr_index, 1) catch return index;
            if (slice.items(.level)[rr_slot] != slice.items(.level)[slot]) return index;

            right_link.* = index;
            mem.swap(u32, right_link, &slice.items(.links)[right_slot][0]);
            slice.items(.level)[right_slot] += 1;
            return right_index;
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);
            return .{ .current = tree.root, .tree = tree, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            top: u32 = 0,
            current: u32,
            tree: *Tree,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                const slice = it.tree.list.slice();
                while (math.sub(u32, it.current, 1) catch null) |slot| {
                    slice.items(.stack)[it.top] = slot;
                    it.top += 1;
                    it.current = slice.items(.links)[slot][0];
                }

                it.top = math.sub(u32, it.top, 1) catch {
                    it.tree.clear();
                    return null;
                };

                const slot = slice.items(.stack)[it.top];
                it.current = slice.items(.links)[slot][1];
                const value = &slice.items(.value)[slot];

                it.values_max[0] = value.*;
                it.values_max += 1;
                return value;
            }
        };
    };
}

pub fn RBTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const Tree = @This();

        const Color = enum(u1) { red, black };
        const Node = struct {
            parent_color: u32,
            links: [2]u32,

            inline fn get_parent(node: *const Node) u32 {
                return node.parent_color >> 1;
            }

            inline fn get_color(node: *const Node) Color {
                return @intToEnum(Color, @truncate(u1, node.parent_color));
            }

            inline fn set_parent(node: *Node, parent: u32) void {
                node.parent_color = (parent << 1) | (node.parent_color & 1);
            }

            inline fn set_color(node: *Node, color: Color) void {
                node.parent_color = (node.parent_color & ~@as(u32, 1)) | @enumToInt(color);
            }
        };

        const List = std.MultiArrayList(struct {
            value: Value,
            stack: u32,
            node: Node,
        });

        list: List,
        root: u32 = 0,

        pub fn init(allocator: mem.Allocator) !Tree {
            var list = List{};
            try list.ensureTotalCapacity(allocator, value_count_max);
            errdefer list.deinit(allocator);

            return Tree{ .list = list };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.list.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.list.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.list.shrinkRetainingCapacity(0);
            tree.root = 0;
        }

        const Context = struct {
            right: bool = false,
            parent: u32 = 0,
        };

        inline fn lookup(tree: *const Tree, context: ?*Context, key: Key) ?*Value {
            var index = tree.root;
            const slice = tree.list.slice();
            while (true) {
                const slot = math.sub(u32, index, 1) catch return null;
                const value = &slice.items(.value)[slot];
                const cmp = compare_keys(key, key_from_value(value));
                if (cmp == .eq) return value;

                const right = cmp == .gt;
                if (context) |ctx| ctx.* = .{ .parent = index, .right = right };
                index = slice.items(.node)[slot].links[@boolToInt(right)];
            }
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            return tree.lookup(null, key);
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            var ctx = Context{};
            if (tree.lookup(&ctx, key)) |value| {
                return .{ .value = value, .exists = true };
            }

            // Reserve a new slot and index for the node.
            const slot = @intCast(u32, tree.list.addOneAssumeCapacity());
            var index = slot + 1;

            const slice = tree.list.slice();
            const nodes = slice.items(.node);
            const entry = Entry{ .value = &slice.items(.value)[slot], .exists = false };

            const node = &nodes[slot];
            node.set_parent(ctx.parent);
            node.set_color(.red);
            node.links = [_]u32{ 0, 0 };

            // Link the node to the parent.
            const parent_link = blk: {
                const parent_slot = math.sub(u32, ctx.parent, 1) catch break :blk &tree.root;
                break :blk &nodes[parent_slot].links[@boolToInt(ctx.right)];
            };
            assert(parent_link.* == 0);
            parent_link.* = index;

            // Fixup color property after insert.
            while (true) {
                var parent_index = nodes[index - 1].get_parent();
                var parent = &nodes[math.sub(u32, parent_index, 1) catch break];
                if (parent.get_color() == .black) break;

                var grand_parent_index = parent.get_parent();
                var grand_parent = &nodes[math.sub(u32, grand_parent_index, 1) catch break];
                const right = parent_index == grand_parent.links[1];

                const uncle_index = grand_parent.links[@boolToInt(!right)];
                if (math.sub(u32, uncle_index, 1) catch null) |uncle_slot| {
                    const uncle = &nodes[uncle_slot];
                    if (uncle.get_color() == .black) break;

                    uncle.set_color(.black);
                    parent.set_color(.black);
                    grand_parent.set_color(.red);
                    index = parent_index;
                    continue;
                }

                if (index == parent.links[@boolToInt(!right)]) {
                    index = parent_index;
                    tree.rotate(nodes, index, right);

                    parent_index = nodes[index - 1].get_parent();
                    parent = &nodes[parent_index - 1];

                    grand_parent_index = parent.get_parent();
                    grand_parent = &nodes[grand_parent_index - 1];
                }

                parent.set_color(.black);
                grand_parent.set_color(.red);
                tree.rotate(nodes, grand_parent_index, !right);
            }

            // Color the root black and return the entry.
            nodes[tree.root - 1].set_color(.black);
            return entry;
        }

        fn rotate(tree: *Tree, nodes: []Node, index: u32, right: bool) void {
            const node = &nodes[index - 1];

            const target_link = &node.links[@boolToInt(!right)];
            const target_index = target_link.*;
            const target = &nodes[target_index - 1];

            const sibling_link = &target.links[@boolToInt(right)];
            const sibling_index = sibling_link.*;
            const maybe_sibling = blk: {
                const sibling_slot = math.sub(u32, sibling_index, 1) catch break :blk null;
                break :blk &nodes[sibling_slot];
            };

            const parent_index = node.get_parent();
            const parent_link = blk: {
                const parent_slot = math.sub(u32, parent_index, 1) catch break :blk &tree.root;
                const parent = &nodes[parent_slot];
                break :blk &parent.links[@boolToInt(parent.links[1] == index)];
            };

            assert(parent_link.* == index);
            parent_link.* = target_index;
            target.set_parent(parent_index);
            node.set_parent(target_index);

            target_link.* = sibling_index;
            if (maybe_sibling) |sibling| sibling.set_parent(index);
            sibling_link.* = index;
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);
            return .{ .current = tree.root, .tree = tree, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            top: u32 = 0,
            current: u32,
            tree: *Tree,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                const slice = it.tree.list.slice();
                while (math.sub(u32, it.current, 1) catch null) |slot| {
                    slice.items(.stack)[it.top] = slot;
                    it.top += 1;
                    it.current = slice.items(.node)[slot].links[0];
                }

                it.top = math.sub(u32, it.top, 1) catch {
                    it.tree.clear();
                    return null;
                };

                const slot = slice.items(.stack)[it.top];
                it.current = slice.items(.node)[slot].links[1];
                const value = &slice.items(.value)[slot];

                it.values_max[0] = value.*;
                it.values_max += 1;
                return value;
            }
        };
    };
}

pub fn SkipListTreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const Tree = @This();

        const capacity = @intCast(u32, value_count_max) + 1;
        const levels_max = blk: {
            var level: u32 = 0;
            while (true) : (level += 1) {
                const n = @intToFloat(f64, level);
                const limit = math.pow(f64, math.e, n);
                if (@floatToInt(u32, limit) > capacity) break;
            }
            break :blk level;
        };

        const probability_table = blk: {
            var table: [levels_max]f64 = undefined;
            for (table) |*probability, i| {
                const default = @floatCast(f64, 1 / math.e);
                probability.* = math.pow(f64, default, @intToFloat(f64, i));
            }
            break :blk table;
        };

        const Values = std.ArrayListUnmanaged(Value);
        const Links = std.ArrayListUnmanaged(u32);

        values: Values,
        links: Links,
        prng: std.rand.DefaultPrng = std.rand.DefaultPrng.init(42),

        pub fn init(allocator: mem.Allocator) !Tree {
            var values = try Values.initCapacity(allocator, value_count_max);
            errdefer values.deinit(allocator);

            var links = try Links.initCapacity(allocator, capacity * (1 + levels_max));
            errdefer links.deinit(allocator);

            var tree = Tree{
                .values = values,
                .links = links,
            };
            tree.clear();
            return tree;
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.links.deinit(allocator);
            tree.values.deinit(allocator);
        }

        pub fn count(tree: *const Tree) u32 {
            return @intCast(u32, tree.values.items.len);
        }

        pub fn clear(tree: *Tree) void {
            tree.values.clearRetainingCapacity();
            tree.links.clearRetainingCapacity();

            const head = @intCast(u32, tree.links.items.len);
            tree.links.appendNTimesAssumeCapacity(0, 1 + levels_max);
            mem.set(u32, tree.links.items[head + 1 ..][0..levels_max], 0);
        }

        pub fn get(tree: *const Tree, key: Key) ?*const Value {
            var prev: u32 = 0;
            var next_link: u32 = 0;
            var level: u32 = levels_max - 1;

            while (true) {
                level = math.sub(u32, level, 1) catch break;
                next_link = tree.links.items[prev + 1 + level];

                while (true) {
                    const next = math.sub(u32, next_link, 1) catch break;
                    const slot = tree.links.items[next];
                    const value = &tree.values.items[slot];
                    const cmp = compare_keys(key, key_from_value(value));
                    if (cmp == .eq) return value;
                    if (cmp != .gt) break;
                    prev = next;
                    next_link = tree.links.items[next + 1 + level];
                }
            }

            const next = math.sub(u32, next_link, 1) catch return null;
            const slot = tree.links.items[next];
            const value = &tree.values.items[slot];
            if (compare_keys(key, key_from_value(value)) == .eq) return value;
            return null;
        }

        pub const Entry = struct {
            value: *Value,
            exists: bool,
        };

        pub fn upsert(tree: *Tree, key: Key) Entry {
            var prev: u32 = 0;
            var next_link: u32 = 0;
            var level: u32 = levels_max - 1;
            var update = mem.zeroes([levels_max]u32);

            while (true) {
                level = math.sub(u32, level, 1) catch break;
                next_link = tree.links.items[prev + 1 + level];

                while (true) {
                    const next = math.sub(u32, next_link, 1) catch break;
                    const slot = tree.links.items[next];
                    const value = &tree.values.items[slot];
                    const cmp = compare_keys(key, key_from_value(value));
                    if (cmp == .eq) return .{ .value = value, .exists = true };

                    if (cmp != .gt) break;
                    prev = next;
                    next_link = tree.links.items[next + 1 + level];
                }

                update[level] = prev;
            }

            if (math.sub(u32, next_link, 1) catch null) |next| {
                const slot = tree.links.items[next];
                const value = &tree.values.items[slot];
                if (compare_keys(key, key_from_value(value)) == .eq) {
                    return .{ .value = value, .exists = true };
                }
            }

            const rand_level = blk: {
                var r = tree.prng.random().float(f64);
                level = 1;
                while (level < levels_max and r < probability_table[level]) level += 1;
                break :blk level;
            };

            const node = @intCast(u32, tree.links.items.len);
            tree.links.appendNTimesAssumeCapacity(0, 1 + rand_level);

            const slot = @intCast(u32, tree.values.items.len);
            const value = tree.values.addOneAssumeCapacity();
            tree.links.items[node] = slot;

            for (tree.links.items[node + 1 ..][0..rand_level]) |*link, i| {
                const update_node = update[i];
                const node_link = &tree.links.items[update_node + 1 + i];
                link.* = node_link.*;
                node_link.* = node + 1;
            }

            return .{ .value = value, .exists = false };
        }

        pub fn iterate_sort_clear(tree: *Tree, values_max: []Value) Iterator {
            assert(tree.count() <= values_max.len);
            const node_link = tree.links.items[0 + 1];
            return .{ .tree = tree, .node_link = node_link, .values_max = values_max.ptr };
        }

        pub const Iterator = struct {
            tree: *Tree,
            node_link: u32,
            values_max: [*]Value,

            pub fn next(it: *Iterator) ?*const Value {
                const node = math.sub(u32, it.node_link, 1) catch {
                    it.tree.clear();
                    return null;
                };

                it.node_link = it.tree.links.items[node + 1];
                const slot = it.tree.links.items[node];
                return &it.tree.values.items[slot];
            }
        };
    };
}
