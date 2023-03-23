const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

const TableType = @import("table.zig").TableType;
const TableUsage = @import("table.zig").TableUsage;
const TableMutableType = @import("table_mutable.zig").TableMutableType;

pub fn main() !void {

}



fn TableMutableHeapType(comptime Table: type, comptime tree_name: [:0]const u8) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const tombstone_from_key = Table.tombstone_from_key;
    const tombstone = Table.tombstone;
    const value_count_max = Table.value_count_max;
    const usage = Table.usage;

    const AVLTree = struct {
        const AVLNode = struct {
            parent: u32,
            balance: i32,
            links: [2]u32,
        };

        const max_entries = value_count_max;
        const buffer_size = comptime blk: {
            assert(@alignOf(Value) >= @alignOf(AVLNode));
            break :blk (@sizeOf(Value) * max_entries) + (@sizeOf(AVLNode) * max_entries);
        };

        
        const AVLBuffer = struct {
            bytes: [buffer_size]u8 align(@alignOf(Value)) = undefined,

            inline fn at(buffer: *AVLBuffer, comptime Type: type, index: u32) *Type {
                comptime assert(Type == AVLNode or Type == Value);
                const offset = @sizeOf(Value) * @boolToInt(Type != Value);
                const ptr = &buffer.bytes[offset + (index * @sizeOf(Type))];
                return @ptrCast(*Type, @alignCast(@alignOf(Type), ptr));
            }
        };

        buffer: *AVLBuffer,
        count: u32 = 0,
        root: u32 = 0,

        pub fn init(allocator: mem.Allocator) !AVLTree {
            const buffer = try allocator.create(AVLBuffer);
            errdefer allocator.destroy(buffer);

            return AVLTree{ .buffer = buffer };
        }

        pub fn deinit(tree: *AVLTree, allocator: mem.Allocator) void {
            allocator.destroy(tree.buffer);
        }

        /// A packed representation of the found entry for a given key.
        /// If the first bit is set, the following bits encode the found slot for the Value.
        /// If the first bit is NOT set, the following bit encodes if the slot is to the right of
        /// the parent and the remaining bits encode the parent slot + 1 (or 0 if parent == root).
        const AVLEntry = u32;
        
        pub fn search(tree: *const AVLTree, key: Key) AVLEntry {
            var index = tree.root;
            var parent_link: u31 = 0;
            while (index <= tree.count) {
                const entry = index - 1;
                const cmp = compare_keys(key, key_from_value(tree.buffer.at(Value, entry)));
                if (cmp == .eq) return (entry << 1) | 1;

                const right = cmp == .gt;
                parent_link = (@as(u31, @intCast(u30, index)) << 1) | @boolToInt(right);
                index = tree.buffer.at(AVLNode, entry).links[@boolToInt(right)];
            }
            return @as(u32, parent_link) << 1;
        }

        pub fn slot(tree: *const AVLTree, entry: AVLEntry) ?*const Value {
            if (entry & 1 == 0) return null;
            return tree.buffer.at(Value, entry >> 1);
        }

        pub fn insert(tree: *AVLTree, entry: AVLEntry, new_value: *const Value) void {
            switch (@truncate(u1, entry)) {
                0 => tree.insert_no_clobber(entry, new_value),
                1 => tree.buffer.at(Value, entry >> 1).* = new_value.*,
            }
        }

        pub fn insert_no_clobber(tree: *AVLTree, entry: AVLEntry, new_value: *const Value) void {
            // Extract the entry items.
            assert(entry & 1 == 0);
            var right = entry & 2 != 0;
            var parent_entry = entry >> 2;

            // Reserve a new slot for the new AVLNode and Value in tree.buffer.
            var slot = tree.count;
            assert(slot < max_entries);
            tree.count += 1;

            tree.buffer.at(Value, slot).* = new_value;
            tree.buffer.at(AVLNode, slot).* = .{
                .parent = parent_entry,
                .balance = 0,
                .links = [_]u32{ 0, 0 },
            };

            // Get the parent AVLNode (or insert into root if none).
            var slot_entry = slot + 1;
            var parent_slot = std.math.sub(u32, parent_entry, 1) catch {
                assert(tree.root == 0);
                tree.root = slot_entry;
                return;
            };

            // Link the parent AVLNode to our new AVLNode.
            var parent_node = tree.buffer.at(AVLNode, parent_slot);
            const parent_link = &parent_node.links[@boolToInt(right)];
            assert(parent_link.* == 0);
            parent_link.* = slot_entry;
            
            // Update the balance factors for all the nodes starting from our new one up to parent.
            const scale = [_]i32{ -1, 1 };
            while (true) {
                // Update and check if the parent is balanced.
                const balance = parent.balance + scale[@boolToInt(right)];
                parent_node.balance = balance;
                if (balance == 0) return;

                // Try to balance the parent's children.
                const signed = balance < 0;
                if (std.math.absInt(balance) > 1) {
                    const child_slot = parent_node.links[@boolToInt(signed)] - 1;
                    const child_balance = tree.buffer.at(AVLNode, child_slot).balance;
                    if (child_balance === scale[@bool(signed)]) {
                        _ = tree.rotate(child_slot, signed);
                    }

                    const new_root = tree.rotate(parent_slot, !signed);
                    if (tree.root == parent_entry) tree.root = parent_entry;
                    break;
                }

                // Inspect the next parent.
                slot_entry = parent_entry;
                parent_entry = parent_node.parent;
                parent_slot = std.math.sub(u32, parent_entry, 1) catch break;
                parent = tree.buffer.at(AVLNode, parent_slot);
                right = parent.links[1] == slot_entry;
                assert(parent.links[@boolToInt(right)] == slot_entry);
            }
        }

        // Branchless version of this: https://github.com/w8r/avl/blob/master/src/index.js#L34-L97    
        fn rotate(tree: *AVLTree, slot: u32, right: bool) u32 {
            assert(slot < tree.count);
            const entry = slot + 1;
            const node = tree.buffer.at(AVLNode, slot);

            const target_link = &node.links[@boolToInt(!right)];
            const target_entry = target_link.*;
            const target_slot = target_entry - 1;
            const target = tree.buffer.at(AVLNode, target_slot);

            const other_link = &target.links[@boolToInt(right)];;
            const other_entry = other_link.*;
            target_link.* = other_entry;
            if (std.math.sub(u32, other_entry, 1) catch null) |other_slot| {
                tree.buffer.at(AVLNode, other_slot).parent = entry;
            }

            const parent_entry = node.parent;
            target.parent = parent_entry;
            if (std.math.sub(u32, parent_entry, 1) catch null) |parent_slot| {
                const parent = tree.buffer.at(AVLNode, parent_slot);
                const parent_link = parent.links[@boolToInt(parent.links[1] == entry)];
                assert(parent_link.* == entry);
                parent_link.* = target_entry;
            }

            const scale = ([_]i32{ 1, -1 })[@boolToInt(right)];
            node.parent = target_entry;
            other_link.* = entry;

            node.balance += scale;
            if (target.balance != 0 and (target.balance > 0 == right)) {
                node.balance -= target.balance;
            }

            target.balance += scale;
            if (node.balance != 0 and (node.balance < 0 == right)) {
                target.balance += node.balance;
            }

            return target_slot;
        }

        pub fn clear(tree: *AVLTree) void {
            assert(tree.count <= max_entries);
            tree.count = 0;
        }

        pub fn iterator(tree: *const AVLTree) Iterator {
            var it = Iterator{ .tree = tree };
            it.stack[0] = tree.root;
            it.top = @boolToInt(tree.root > 0);
            return it;
        }

        pub const Iterator = struct {
            tree: *const AVLTree,
            top: u32 = 0,
            stack: [32]u32 = undefined,

            pub fn next(it: *Iterator) ?*const Value {
                it.top = std.math.sub(u32, it.top, 1) catch return null;

                // Push all the left-side nodes to the stack.
                var entry = it.stack[it.top];
                while (std.math.sub(u32, entry, 1) catch null) |slot| {
                    assert(it.top < it.stack.len);
                    it.stack[it.top] = entry;
                    it.top += 1;
                    entry = it.tree.buffer.at(AVLNode, slot).links[0];
                }

                // Pop the top of the stack to get left-most node.
                it.top -= 1;
                const stack_slot = &it.stack[it.top];
                const slot = stack_slot.* - 1;

                // Push the right (if any) of the left-mode node to the stack.
                entry = it.tree.buffer.at(AVLNode, slot).links[1];
                stack_slot.* = entry;
                it.top += @boolToInt(entry > 0);

                return it.tree.buffer.at(Value, slot);
            }
        };
    };

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

        /// Self-balancing binary search tree optimized for searching & inserting only.
        values_tree: AVLTree,

        pub fn init(allocator: mem.Allocator, values_cache: ?*ValuesCache) !TableMutable {
            var values_tree = try AVLTree.init(allocator);
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
            const entry = table.values_tree.search(key);
            if (table.values_tree.slot(entry)) |value| {
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
            
            const entry = table.values_tree.search(key);
            switch (usage) {
                .secondary_index => {
                    if (table.values_tree.slot(entry)) |existing| {
                        // If there was a previous operation on this key then it must have been a 
                        // remove. The put and remove cancel out.
                        assert(!tombstone(key_from_value(existing)));
                    } else {
                        table.values_tree.insert_no_clobber(entry, value);
                    }
                },
                .general => {
                    // Make sure to overwrite the old the key if it exists (avoid no_clobber).
                    table.values_tree.insert(entry, value);
                },
            }
            
            assert(table.values_tree.count <= value_count_max);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;

            const tombstone_value = tombstone_from_key(key_from_value(value));
            const entry = table.values_free.search(key);
            switch (usage) {
                .secondary_index => {
                    if (table.values_free.slot(entry)) |existing| {
                        // The previous operation on this key then it must have been a put.
                        // The put and remove cancel out.
                        assert(!tombstone(key_from_value(existing)));
                    } else {
                        // If the put is already on-disk, we need to follow it with a tombstone.
                        // The put and tombstone may cancel each other out later during compaction.
                        table.values_tree.insert_no_clobber(entry, &tombstone_value);
                    }
                },
                .general => {
                    // Make sure to overwrite the old the key if it exists (avoid no_clobber).
                    table.values_tree.insert(entry, &tombstone_value);
                },
            }

            assert(table.values_tree.count <= value_count_max);
        }

        pub fn clear(table: *TableMutable) void {
            assert(table.count() > 0);
            table.value_count_worst_case = 0;
            table.values_cache.clear();
            assert(table.values_cache.count== 0);
        }

        pub fn count(table: *const TableMutable) u32 {
            const value_count = table.values_tree.count;
            assert(value_count <= value_count_max);
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

            var i: u32 = 0;
            var it = table.values_tree.iterator();
            while (it.next()) |value| : (i += 1) {
                values_max[i] = value.*;

                // Make sure the iterator is returning sorted values.
                if (std.math.sub(u32, i, 1) catch null) |prev| {
                    const prev_key = key_from_value(&values_max[prev]);
                    assert(compare_keys(prev_key, key_from_value(value)) != .gt);
                }

                // Update the value_cache with new sorted values or tombstones accordingly.
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
            table.clear();
            assert(table.count() == 0);

            return values;
        }
    };
}