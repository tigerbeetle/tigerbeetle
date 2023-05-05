const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
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

        const ValuesTree = struct {
            const levels_max = 4; // constants.lsm_levels;
            const mem_table_size = 16;

            const KeyRange = struct {
                min_max: ?[2]Key = null,

                fn empty(range: *const KeyRange) bool {
                    return range.min_max == null;
                }

                fn may_contain(range: *const KeyRange, key: Key) bool {
                    const kr = range.min_max orelse return false;
                    if (compare_keys(key, kr[0]) == .lt) return false;
                    if (compare_keys(key, kr[1]) == .gt) return false;
                    return true;
                }

                fn add_key(range: *KeyRange, key: Key) void {
                    const min_max: ?[2]Key = [2]Key{ key, key };
                    const other = KeyRange{ .min_max = min_max };
                    range.add_range(&other);
                }

                fn add_range(range: *KeyRange, other: *const KeyRange) void {
                    const other_kr = other.min_max orelse return;
                    if (range.min_max == null) {
                        range.min_max = other_kr;
                        return;
                    }

                    const kr = &range.min_max.?;
                    kr[0] = if (compare_keys(kr[0], other_kr[0]) == .lt) kr[0] else other_kr[0];
                    kr[1] = if (compare_keys(kr[1], other_kr[1]) == .gt) kr[1] else other_kr[1];
                }
            };

            const ValueRange = struct {
                keys: KeyRange = .{},
                buf: []Value,
                len: u32 = 0,
            };

            const MemTable = struct {
                keys: KeyRange = .{},
                buf: []Value,
                bottom: u32 = mem_table_size,
            };

            level_count: u32 = 0,
            bloom_filter: KeyRange = .{},
            mem_table: MemTable,
            compaction: ValueRange,
            merge_buf: []Value,
            levels: [levels_max]ValueRange,

            pub fn init(allocator: mem.Allocator) !ValuesTree {
                const mem_table = MemTable{ .buf = try allocator.alloc(Value, mem_table_size) };
                errdefer allocator.free(mem_table.buf);

                const compaction = ValueRange{ .buf = try allocator.alloc(Value, value_count_max) };
                errdefer allocator.free(compaction.buf);

                const merge_buf = try allocator.alloc(Value, value_count_max);
                errdefer allocator.free(merge_buf);

                var levels: [levels_max]ValueRange = undefined;
                for (levels) |*level, i| {
                    // Each level is half the size of the next.
                    // The last level size is value_count_max.
                    const level_size = value_count_max >> @intCast(u6, levels_max - 1 - i);

                    errdefer for (levels[0..i]) |*l| allocator.free(l.buf);
                    level.* = ValueRange{ .buf = try allocator.alloc(Value, level_size) };
                }
                errdefer for (levels) |*l| allocator.free(l.buf);

                return ValuesTree{
                    .mem_table = mem_table,
                    .compaction = compaction,
                    .merge_buf = merge_buf,
                    .levels = levels,
                };
            }

            pub fn deinit(tree: *ValuesTree, allocator: mem.Allocator) void {
                for (tree.levels) |*l| allocator.free(l.buf);
                allocator.free(tree.merge_buf);
                allocator.free(tree.compaction.buf);
                allocator.free(tree.mem_table.buf);
            }

            pub inline fn empty(tree: *const ValuesTree) bool {
                return tree.bloom_filter.empty();
            }

            pub fn clear(tree: *ValuesTree) void {
                for (tree.levels) |*l| l.* = .{ .buf = l.buf };
                tree.* = .{
                    .mem_table = .{ .buf = tree.mem_table.buf },
                    .compaction = .{ .buf = tree.compaction.buf },
                    .merge_buf = tree.merge_buf,
                    .levels = tree.levels,
                };
            }

            pub fn find(tree: *const ValuesTree, key: Key) ?*Value {
                // Check bloom filter first before searching below.
                if (!tree.bloom_filter.may_contain(key)) return null;

                // Check the mem table before the levels with a linear scan.
                if (tree.mem_table.keys.may_contain(key)) {
                    for (tree.mem_table.buf[tree.mem_table.bottom..]) |*value| {
                        if (compare_keys(key, key_from_value(value)) == .eq) {
                            return value;
                        }
                    }
                }

                // Check the other levels with a binary search.
                var level: u32 = 0;
                while (level < tree.level_count) : (level += 1) {
                    const vr = &tree.levels[level];
                    if (!vr.keys.may_contain(key)) continue;

                    const values = vr.buf[0..vr.len];
                    const result = binary_search.binary_search_values(
                        Key,
                        Value,
                        key_from_value,
                        compare_keys,
                        values,
                        key,
                        .{},
                    );

                    if (result.exact) {
                        const value = &values[result.index];
                        if (constants.verify) assert(compare_keys(key, key_from_value(value)) == .eq);
                        return value;
                    }
                }

                return null;
            }

            pub fn append(tree: *ValuesTree, op: enum { insert, remove }, value: *const Value) void {
                // Reserve an index in the mem_table.
                // This happens in reverse to detect newer values while iterating in flush_into().
                tree.mem_table.bottom = math.sub(u32, tree.mem_table.bottom, 1) catch blk: {
                    tree.flush_into(.first_fit_level);
                    assert(tree.mem_table.bottom > 0);
                    break :blk tree.mem_table.bottom - 1;
                };

                // Add the key to the mem_table KeyRange and the bloom filter for all KeyRanges.
                const key = key_from_value(value);
                tree.mem_table.keys.add_key(key);
                tree.bloom_filter.add_key(key);

                // Append the Value to the mem_table.
                tree.mem_table.buf[tree.mem_table.bottom] = switch (op) {
                    .insert => value.*,
                    .remove => tombstone_from_key(key),
                };
            }

            fn flush_into(tree: *ValuesTree, target: enum { first_fit_level, compaction_buf }) void {
                assert(tree.compaction.len == 0);
                assert(tree.compaction.keys.empty());

                // Flush mem_table into compaction buffer, getting rid of duplicates along the way.
                mem_table: for (tree.mem_table.buf[tree.mem_table.bottom..]) |*current| {
                    for (tree.compaction.buf[0..tree.compaction.len]) |*existing| {
                        switch (compare_keys(key_from_value(current), key_from_value(existing))) {
                            .eq => continue :mem_table, // Duplicates in mem_table_values. Skip it.
                            .gt => continue, // Search the next value.
                            .lt => mem.swap(Value, current, existing), // Insertion sort.
                        }
                    }
                    tree.compaction.buf[tree.compaction.len] = current.*;
                    tree.compaction.len += 1;
                }

                // Mark mem_table as consumed into compaction buffer.
                tree.compaction.keys.add_range(&tree.mem_table.keys);
                tree.mem_table = .{ .buf = tree.mem_table.buf };

                // Then merge it with other levels.
                var vr_out_level: u32 = 0;
                while (true) : (vr_out_level += 1) {
                    assert(vr_out_level < levels_max);

                    // Bump level_count for find() depending on how deep we compact.
                    tree.level_count = @maximum(tree.level_count, vr_out_level + 1);
                    assert(tree.level_count <= levels_max);

                    // Consume and merge vr_in and vr_out into tree.compaction buffer.
                    const vr_out = &tree.levels[vr_out_level];
                    tree.compact(vr_out);

                    switch (target) {
                        // Compact the next level if compaction buffer overflows vr_out.
                        .first_fit_level => if (vr_out.buf.len < tree.compaction.len) continue,
                        // Compact all the levels until we reach the end, then return
                        .compaction_buf => if (vr_out_level < tree.level_count - 1) continue else return,
                    }

                    // Move/copy the compaction buffer into vr_out as its final destination.
                    return {
                        const sorted_values = tree.compaction.buf[0..tree.compaction.len];
                        stdx.copy_disjoint(.inexact, Value, vr_out.buf, sorted_values);

                        mem.swap(KeyRange, &vr_out.keys, &tree.compaction.keys);
                        assert(tree.compaction.keys.empty());
                        assert(!vr_out.keys.empty());

                        mem.swap(u32, &vr_out.len, &tree.compaction.len);
                        assert(tree.compaction.len == 0);
                        assert(vr_out.len > 0);
                    };
                }
            }

            fn compact(tree: *ValuesTree, vr: *ValueRange) void {
                // Bail if there's nothing to merge.
                if (vr.len == 0) {
                    assert(vr.keys.empty());
                    return;
                }

                const values_a = tree.merge_buf[0..tree.compaction.len];
                const values_b = vr.buf[0..vr.len];
                const values_out = tree.compaction.buf;

                // Copy existing compaction buffer values into merge_buf as we'll overwrite below.
                const values_in = tree.compaction.buf[0..tree.compaction.len];
                stdx.copy_disjoint(.exact, Value, values_a, values_in);

                var index_a: u32 = 0;
                var index_b: u32 = 0;
                var index_out: u32 = 0;

                const values_copy = while (true) {
                    // Keep merging until we can just memcpy one of the values arrays.
                    if (index_a == values_a.len) break values_b[index_b..];
                    if (index_b == values_b.len) break values_a[index_a..];

                    const value_merge = blk: {
                        const value_a = &values_a[index_a];
                        const value_b = &values_b[index_b];
                        switch (compare_keys(key_from_value(value_a), key_from_value(value_b))) {
                            .lt => {
                                index_a += 1;
                                break :blk value_a;
                            },
                            .gt => {
                                index_b += 1;
                                break :blk value_b;
                            },
                            .eq => {
                                index_a += 1;
                                index_b += 1;
                                if (usage == .secondary_index) {
                                    // A new remove() and previous put() cancel out as a delete.
                                    if (tombstone(value_a) and !tombstone(value_b)) continue;
                                }
                                break :blk value_a; // Always prefer the newer (value_a) Value.
                            },
                        }
                    };

                    assert(index_out < values_out.len);
                    values_out[index_out] = value_merge.*;
                    index_out += 1;
                } else unreachable;

                // Copy Values from the remaining values array if any.
                stdx.copy_disjoint(.inexact, Value, values_out[index_out..], values_copy);
                index_out += @intCast(u32, values_copy.len);
                assert(index_out <= values_out.len);

                // Consume the merged values and the passed in *ValueRange into tree.compaction.
                tree.compaction.keys.add_range(&vr.keys);
                tree.compaction.len = index_out;
                vr.* = .{ .buf = vr.buf };
            }

            pub fn sort_into_and_clear(tree: *ValuesTree, values_max: []Value) []const Value {
                tree.flush_into(.compaction_buf);

                const sorted_values = tree.compaction.buf[0..tree.compaction.len];
                stdx.copy_disjoint(.inexact, Value, values_max, sorted_values);

                tree.clear();
                return values_max[0..sorted_values.len];
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

        values: ValuesTree,

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
            var values = try ValuesTree.init(allocator);
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
            table.values.append(.insert, value);
        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            assert(table.value_count_worst_case < value_count_max);
            table.value_count_worst_case += 1;
            table.values.append(.remove, value);
        }

        pub fn empty(table: *const TableMutable) bool {
            return table.values.empty();
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_values_and_clear(
            table: *TableMutable,
            values_max: []Value,
        ) []const Value {
            assert(!table.empty());
            assert(values_max.len == value_count_max);

            const values = table.values.sort_into_and_clear(values_max);
            assert(table.empty());

            if (table.values_cache) |cache| {
                for (values) |*value| {
                    if (tombstone(value)) {
                        cache.remove(key_from_value(value));
                    } else {
                        cache.insert(value);
                    }
                }
            }

            table.value_count_worst_case = 0;
            return values;
        }
    };
}
