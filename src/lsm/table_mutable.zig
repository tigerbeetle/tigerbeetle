const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
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
                    return stdx.hash_inline(key);
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

        const ValuesSet = struct {
            const Index = math.IntFittingRange(0, value_count_max);
            const Tag = std.meta.Int(.unsigned, 32 - @bitSizeOf(Index));
            const Slot = packed struct {
                tag: Tag,
                index: Index,
            };

            const can_delete = usage == .secondary_index;
            const deleted_init = if (can_delete) @as(u32, 0) else {};
            const slot_count_max = math.ceilPowerOfTwoAssert(u32, value_count_max * 2);

            const Data = struct {
                inserted: [slot_count_max]Slot,
                deleted: if (can_delete) std.bit_set.ArrayBitSet(u32, value_count_max) else void,
                values: [value_count_max]Value,
            };

            data: *Data,
            inserted: u32 = 0,
            deleted: @TypeOf(deleted_init) = deleted_init,
            sorted: bool = true,
            sorted_key_max: Key = undefined,

            pub fn init(allocator: mem.Allocator) !ValuesSet {
                const data = try allocator.create(Data);
                errdefer allocator.destroy(data);

                var set = ValuesSet{ .data = data };
                set.clear();
                return set;
            }

            pub fn deinit(set: *ValuesSet, allocator: mem.Allocator) void {
                allocator.destroy(set.data);
            }

            fn clear(set: *ValuesSet) void {
                const data = set.data;
                set.* = .{ .data = data };

                data.inserted = mem.zeroes(@TypeOf(data.inserted));
                if (can_delete and constants.verify) data.deleted = undefined;
            }

            pub inline fn count(set: *const ValuesSet) u32 {
                return set.inserted - (if (can_delete) set.deleted else 0);
            }

            inline fn lookup(op: enum { find, upsert }, data: *Data, key: Key, inserted: u32) ?u32 {
                assert(inserted <= value_count_max);

                const hash: u64 = stdx.hash_inline(key);
                const tag = @truncate(Tag, hash >> (64 - @bitSizeOf(Tag)));
                var pos = @truncate(u32, hash % slot_count_max);

                // We use linear probing so hint at sequential access in memory.
                @prefetch(&data.inserted[pos], .{});
                while (true) : (pos = (pos + 1) % slot_count_max) {
                    const slot = data.inserted[pos];

                    // On empty slots, return null and maybe insert depending on `op`.
                    if (@bitCast(u32, slot) == 0) {
                        if (op == .upsert) {
                            const index = @intCast(Index, inserted + 1); // non-zero index.
                            data.inserted[pos] = .{ .tag = tag, .index = index };
                        }
                        return null;
                    }

                    // On occupied slots, check the tag like a bloom filter before comparing keys.
                    if (slot.tag == tag) {
                        const index = @as(u32, slot.index) - 1;
                        assert(index < inserted);
                        if (compare_keys(key, key_from_value(&data.values[index])) == .eq) {
                            return index;
                        }
                    }
                }
            }

            pub fn find(set: *const ValuesSet, key: Key) ?*const Value {
                // Quick null check to avoid lookup.
                const inserted = set.inserted;
                if (inserted == 0) return null;

                // Before this, we could check against sorted_key_max, but that's slower than lookup.
                const data = set.data;
                const index = lookup(.find, data, key, inserted) orelse return null;

                // Make sure we're not returning deleted values.
                if (can_delete and data.deleted.isSet(index)) return null;
                return &data.values[index];
            }

            pub fn upsert(set: *ValuesSet, value: *const Value) void {
                const key = key_from_value(value);
                const inserted = set.inserted;
                const data = set.data;

                if (lookup(.upsert, data, key, inserted)) |index| {
                    assert(index < inserted);
                    const existing = &data.values[index];

                    if (can_delete) del: {
                        // If value index was deleted, un-delete it for upsert.
                        if (data.deleted.isSet(index)) {
                            data.deleted.unset(index);
                            set.deleted -= 1;
                            break :del;
                        }

                        // If value index exists and we're a secondary_index, it's either a put() or
                        // remove() that cancel each other out before hitting compaction.
                        assert(tombstone(value) != tombstone(existing));
                        data.deleted.set(index);
                        set.deleted += 1;
                        return;
                    }

                    existing.* = value.*;
                    return;
                }

                assert(inserted < value_count_max);
                set.inserted += 1;

                if (inserted == 0) {
                    set.sorted_key_max = key;
                } else if (set.sorted) {
                    set.sorted = compare_keys(set.sorted_key_max, key) != .gt;
                    if (set.sorted) set.sorted_key_max = key;
                }

                if (can_delete) data.deleted.setValue(inserted, false);
                data.values[inserted] = value.*;
            }

            pub fn sort_into_values_and_clear(set: *ValuesSet, values_max: []Value) []const Value {
                assert(values_max.len == value_count_max);
                assert(set.inserted <= value_count_max);
                assert(set.count() <= set.inserted);

                defer {
                    set.clear();
                    assert(set.count() == 0);
                }

                const data = set.data;
                const inserted = data.values[0..set.inserted];
                const values = blk: {
                    // There are "holes" in `inserted` that are deleted. Skip them when copying.
                    if (can_delete and set.deleted > 0) {
                        var i: u32 = 0;
                        for (inserted) |*value, index| {
                            if (data.deleted.isSet(index)) continue;
                            values_max[i] = value.*;
                            i += 1;
                        }
                        break :blk values_max[0..i];
                    }

                    // All values in `inserted` are unique. Copy them freely.
                    const values = values_max[0..inserted.len];
                    stdx.copy_disjoint(.exact, Value, values, inserted);
                    break :blk values;
                };

                // Make sure the values are sorted before returning them.
                if (!set.sorted) {
                    stdx.pdqContext(0, values.len, struct {
                        items: []Value,

                        pub inline fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                            return compare_keys(key_from_value(&ctx.items[a]), key_from_value(&ctx.items[b])) == .lt;
                        }

                        pub inline fn swap(ctx: @This(), a: usize, b: usize) void {
                            return mem.swap(Value, &ctx.items[a], &ctx.items[b]);
                        }
                    }{ .items = values });
                }

                assert(values.len == set.count());
                return values;
            }
        };

        values_set: ValuesSet,

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
            var values_set = try ValuesSet.init(allocator);
            errdefer values_set.deinit(allocator);

            return TableMutable{
                .values_set = values_set,
                .values_cache = values_cache,
            };
        }

        pub fn deinit(table: *TableMutable, allocator: mem.Allocator) void {
            table.values_set.deinit(allocator);
        }

        pub fn get(table: *const TableMutable, key: Key) ?*const Value {
            if (table.values_set.find(key)) |value| {
                return value;
            }
            if (table.values_cache) |cache| {
                // Check the cache after the mutable table (see `values_cache` for explanation).
                if (cache.get(key)) |value| return value;
            }
            return null;
        }

        pub fn put(table: *TableMutable, value: *const Value) void {
            assert(!tombstone(value));
            table.upsert(value);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            const new_value = tombstone_from_key(key_from_value(value));
            table.upsert(&new_value);
        }

        fn upsert(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;
            table.values_set.upsert(value);
            assert(table.count() <= table.value_count_worst_case);
        }

        pub fn count(table: *const TableMutable) u32 {
            const value_count = table.values_set.count();
            assert(value_count <= value_count_max);
            return value_count;
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_values_and_clear(
            table: *TableMutable,
            values_max: []Value,
        ) []const Value {
            const value_count = table.count();
            assert(value_count > 0);
            assert(value_count <= value_count_max);
            assert(values_max.len == value_count_max);

            const values = table.values_set.sort_into_values_and_clear(values_max);
            assert(values.ptr == values_max.ptr);
            assert(values.len == value_count);
            assert(table.count() == 0);

            table.value_count_worst_case = 0;
            return values;
        }
    };
}
