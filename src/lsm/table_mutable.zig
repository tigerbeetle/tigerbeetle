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

        const ValuesMap = struct {
            const levels_max = constants.lsm_levels;
            const segment_max = 16;

            const MemTableIndex = blk: {
                if (segment_max < math.maxInt(u8)) break :blk u8;
                if (segment_max < math.maxInt(u16)) break :blk u16;
                assert(segment_max < math.maxInt(u32));
                break :blk u32;
            };

            const Filter = [32]u8;
            const Segment = [segment_max]Value;

            const Data = struct {
                mem_table: struct {
                    links: [segment_max][2]MemTableID,
                    stack: [segment_max]MemTableIndex,
                    levels: [segment_max]u8,
                },
                index: [levels_max]Index,
                filters: [levels_max]Filter,
                active: [@divExact(value_count_max, 8)]u8,
                segments: [@divExact(value_count_max, segment_max)]Segment,
            };

            mem_table: struct {
                root: u32 = 0,
                size: u32 = 0,
                segment: u32 = 0,
            },

            fn mem_table_find(data: *Data, root: u32, segment: u32, key: Key) ?*const Value {
                var index = root;
                const values = &data.segments[segment];
                while (true) {
                    const slot = math.sub(u32, index, 1) catch return null;
                    const value = &values[slot];
                    switch (compare_keys(key, key_from_value(value))) {
                        .eq => return if (mem_table_active(data, segment, slot)) value else null,
                        else => |cmp| index = data.mem_table.links[slot][@boolToInt(cmp == .gt)],
                    }
                }
            }

            const Entry = struct {
                value: *Value,
                exists: bool,
                removed: bool,
            };

            fn mem_table_entry(data: *Data, segment: u32, index: u32, new_slot: u32, key: Key, entry: *Entry) u32 {
                const values = &data.segments[segment];
                const slot = math.sub(u32, index, 1) catch {
                    data.mem_table.links[new_slot] = .{0, 0};
                    data.mem_table.levels[new_slot] = 1;

                    entry.value = &values[new_slot];
                    entry.exists = false;
                    entry.removed = false;
                    return new_slot + 1;
                };

                entry.value = &values[slot];
                const cmp = compare_keys(key, key_from_value(entry.value));
                entry.exists = cmp == .eq;
                if (entry.exists) {
                    entry.removed = !mem_table_active(data, segment, slot);
                    return index;
                }

                const link = &data.mem_table.links[slot][@boolToInt(cmp == .gt)];
                link.* = mem_table_upsert(data, segment, link.*, new_slot, key, entry);
                return mem_table_split(data, mem_table_skew(data, index));
            }

            inline fn mem_table_skew(data: *const Data, index: u32) u32 {
                const slot = math.sub(u32, index, 1) catch unreachable;

                const left_link = &data.mem_table.links[slot][0];
                const left_index = left_link.*;
                const left_slot = math.sub(u32, left_index, 1) catch return index;
                if (data.mem_table.levels[left_slot] != data.mem_table.levels[slot]) return index;

                left_link.* = index;
                mem.swap(u32, left_link, &data.mem_table.links[left_slot][1]);
                return left_index;
            }

            inline fn mem_table_split(data: *const Data, index: u32) u32 {
                const slot = math.sub(u32, index, 1) catch unreachable;

                const right_link = &data.mem_table.links[slot][1];
                const right_index = right_link.*;
                const right_slot = math.sub(u32, right_index, 1) catch return index;

                const rr_index = data.mem_table.links[right_slot][1];
                const rr_slot = math.sub(u32, rr_index, 1) catch return index;
                if (data.mem_table.levels[rr_slot] != data.mem_table.levels[slot]) return index;

                right_link.* = index;
                mem.swap(u32, right_link, &data.mem_table.links[right_slot][0]);
                data.mem_table.levels[right_slot] += 1;
                return right_index;
            }

            fn mem_table_update(data: *Data, segment: u32, value: *Value, inserted: bool) void {
                const offset = @ptrToInt(value) - @ptrToInt(&data.segments[segment]);
                const slot = @intCast(u32, @divExact(offset, @sizeOf(Value)));
                assert(slot < segment_max);

                const mask = @as(u8, 1) << @intCast(u3, slot % 8);
                const byte = &data.active[((segment * segment_max) + slot) / 8];
                byte.* = if (inserted) (byte | mask) else (byte & ~mask);
            }

            inline fn mem_table_active(data: *Data, segment: u32, slot: MemTableIndex) bool {
                const mask = @as(u8, 1) << @intCast(u3, slot % 8);
                const byte = &data.active[((segment * segment_max) + slot) / 8];
                return (byte.* & mask != 0);
            }

            fn mem_table_sort_into(data: *const MemTable, segment: u32, root: u32, values: []Value) u32 {
                var top: u32 = 0;
                var size: u32 = 0;
                var current = root;
                const values = &data.segments[segment];
                
                while (true) {
                    while (math.sub(u32, current, 1) catch null) |slot| {
                        data.mem_table.stack[top] = slot;
                        top += 1;
                        current = data.mem_table.links[slot][0];
                    }

                    top = math.sub(u32, top, 1) catch return size;
                    const slot = data.mem_table.stack[top];
                    current = data.mem_table.links[slot][1];

                    if (!mem_table_active(data, segment, slot)) continue;
                    values_max[len] = values[slot];
                    size += 1;
                }
            }
        };

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
