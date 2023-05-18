const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

pub fn TableSetType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const usage = Table.usage;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const tombstone = Table.tombstone;
    const value_count_max = Table.value_count_max;

    return struct {
        const TableSet = @This();

        const can_delete = usage == .secondary_index;
        const slot_count_max = math.ceilPowerOfTwoAssert(u32, value_count_max * 2);
        const buffer_count_max = @minimum(value_count_max, (64 * 1024) / @sizeOf(Value));
        const heap_count_max = stdx.div_ceil(value_count_max, buffer_count_max);

        const BufferRange = struct {
            pos: u32,
            end: u32,
        };

        const Data = struct {
            deleted: if (can_delete) std.bit_set.ArrayBitSet(u32, value_count_max) else void,
            inserted: std.bit_set.ArrayBitSet(u32, slot_count_max),
            slots: [slot_count_max]u32,
            buffer: [value_count_max]u32,
            heap: [heap_count_max]BufferRange,
            values: [value_count_max]Value,
        };

        data: *Data,
        len: u32 = 0,
        values: u32 = 0,
        range: [2]u32 = undefined,
        heap: struct {
            len: u32 = 0,
            last: u32 = 0,
            consuming: bool = false,
        } = .{},
        buffer: struct {
            pos: u32 = 0,
            len: u32 = 0,
            sorted: bool = true,
        } = .{},

        pub fn init(allocator: mem.Allocator) !TableSet {
            const data = try allocator.create(Data);
            errdefer allocator.destroy(data);

            var set = TableSet{ .data = data };
            set.clear();
            return set;
        }

        pub fn deinit(set: *TableSet, allocator: mem.Allocator) void {
            allocator.destroy(set.data);
        }

        pub fn clear(set: *TableSet) void {
            set.* = .{ .data = set.data };
            set.data.inserted = @TypeOf(set.data.inserted).initEmpty();
        }

        pub inline fn count(set: *const TableSet) u32 {
            assert(set.values <= value_count_max);
            assert(set.len <= set.values);
            return set.len;
        }

        pub const KeyRange = struct {
            key_min: Key,
            key_max: Key,
        };

        /// Rought estimate of the keys inserted into the TableSet.
        /// TODO: account for deleted keys when `can_delete`.
        pub inline fn key_range(set: *const TableSet) ?KeyRange {
            if (set.count() == 0) return null;
            return set.key_range_get();
        }

        fn key_range_get(set: *const TableSet) KeyRange {
            const min_slot = set.range[0];
            assert(min_slot < set.values);

            const max_slot = set.range[1];
            assert(max_slot < set.values);

            const data = set.data;
            return KeyRange{
                .key_min = key_from_value(&data.values[min_slot]),
                .key_max = key_from_value(&data.values[max_slot]),
            };
        }

        fn key_range_insert(set: *TableSet, key: Key, slot: u32) void {
            assert(slot < set.values);

            if (slot > 0) {
                const kr = set.key_range_get();
                if (compare_keys(key, kr.key_min) == .lt) set.range[0] = slot;
                if (compare_keys(key, kr.key_max) == .gt) set.range[1] = slot;
            } else {
                set.range = .{ slot, slot };
            }
        }

        pub fn find(set: *const TableSet, key: Key) ?*const Value {
            // Quick empty check to avoid lookup.
            const data = set.data;
            if (set.len == 0) return null;

            const slot = set.lookup(key, false) orelse return null;
            assert(slot < set.values);

            // Make sure to not return deleted Values.
            if (can_delete and data.deleted.isSet(slot)) return null;
            return &data.values[slot];
        }

        fn lookup(set: *const TableSet, key: Key, insert: bool) ?u32 {
            const hash = std.hash.Wyhash.hash(0, mem.asBytes(&key));
            const data = set.data;
            assert(set.values <= value_count_max);

            var pos = @truncate(u32, hash % slot_count_max);
            while (true) : (pos = (pos + 1) % slot_count_max) {
                // Check for an existing entry
                if (data.inserted.isSet(pos)) {
                    const pos_slot = data.slots[pos];
                    assert(pos_slot < set.values);

                    const pos_key = key_from_value(&data.values[pos_slot]);
                    if (compare_keys(pos_key, key) == .eq) return pos_slot;
                    continue;
                }

                // Empty position in hashmap. Return null and maybe insert current slot.
                if (insert) {
                    data.inserted.set(pos);
                    data.slots[pos] = set.values;
                }
                return null;
            }
        }

        pub fn upsert(set: *TableSet, value: *const Value) void {
            assert(!set.heap.consuming);
            assert(set.values <= value_count_max);
            assert(set.len <= set.values);

            const data = set.data;
            const key = key_from_value(value);

            if (set.lookup(key, true)) |slot| {
                assert(slot < set.values);
                const existing = &data.values[slot];

                if (can_delete) blk: {
                    // Unmark it as deleted to insert.
                    if (data.deleted.isSet(slot)) {
                        data.deleted.unset(slot);
                        set.len += 1;
                        break :blk;
                    }

                    // For secondary_indexes, updates to existing values cancel each other out as
                    // it's ether a new put() with an old remove() or vice versa.
                    assert(tombstone(value) != tombstone(existing));
                    data.deleted.set(slot);
                    set.len -= 1;
                    return;
                }

                existing.* = value.*;
                return;
            }

            const slot = set.values;
            assert(slot < value_count_max);
            set.values += 1;

            if (can_delete) data.deleted.setValue(slot, false);
            set.len += 1;

            data.values[slot] = value.*;
            set.buffer_add(key, slot);
            set.key_range_insert(key, slot);
        }

        fn buffer_add(set: *TableSet, key: Key, slot: u32) void {
            assert(set.values <= value_count_max);
            assert(slot < set.values);

            const data = set.data;
            const pos = set.buffer.pos + set.buffer.len;
            assert(pos < value_count_max);

            if (set.buffer.sorted and set.buffer.len > 0) {
                const last_slot = data.buffer[pos - 1];
                assert(last_slot < slot);

                const last_key = key_from_value(&data.values[last_slot]);
                set.buffer.sorted = compare_keys(last_key, key) == .lt;
            }

            data.buffer[pos] = slot;
            set.buffer.len += 1;
            if (set.buffer.len == buffer_count_max) set.buffer_flush();
        }

        fn buffer_flush(set: *TableSet) void {
            assert(set.buffer.len > 0);
            assert(set.buffer.pos + set.buffer.len <= value_count_max);
            assert(set.heap.len < heap_count_max);
            assert(!set.heap.consuming);

            // Consume the flushed buffer.
            const flushed = set.buffer;
            set.buffer = .{
                .pos = flushed.pos + flushed.len,
                .len = 0,
                .sorted = true,
            };

            // Make sure the flushed buffer is sorted.
            const data = set.data;
            if (!flushed.sorted) {
                const slots = data.buffer[flushed.pos..][0..flushed.len];
                std.sort.sort(u32, slots, data.values[0..set.values], struct {
                    fn less(values: []const Value, a_slot: u32, b_slot: u32) bool {
                        const a_key = key_from_value(&values[a_slot]);
                        const b_key = key_from_value(&values[b_slot]);
                        return compare_keys(a_key, b_key) == .lt;
                    }
                }.less);
            }

            // Try to merge the flushed buffer with the last one pushed to heap if they're disjoint.
            if (set.heap.len > 0) {
                const heap_slot = set.heap.last;
                assert(heap_slot < set.heap.len);

                const last_pos = data.heap[heap_slot].end;
                assert(last_pos == flushed.pos);

                const last_slot = data.buffer[last_pos - 1];
                assert(last_slot < set.values);

                const min_slot = data.buffer[flushed.pos];
                assert(min_slot < set.values);

                const last_key = key_from_value(&data.values[last_slot]);
                const min_key = key_from_value(&data.values[min_slot]);
                if (compare_keys(last_key, min_key) == .lt) {
                    data.heap[heap_slot].end += flushed.len;
                    return;
                }
            }

            // Push the flushed buffer to the min-heap.
            set.heap_push(.{
                .pos = flushed.pos,
                .end = flushed.pos + flushed.len,
            });
        }

        fn heap_push(set: *TableSet, buffer_range: BufferRange) void {
            assert(!set.heap.consuming);
            assert(set.heap.len < heap_count_max);
            assert(buffer_range.end <= value_count_max);
            assert(buffer_range.pos < buffer_range.end);

            // Push the BufferRange to the end of the heap.
            const data = set.data;
            data.heap[set.heap.len] = buffer_range;
            set.heap.len += 1;

            const current_key = blk: {
                const slot = data.buffer[buffer_range.pos];
                assert(slot < set.values);
                break :blk key_from_value(&data.values[slot]);
            };

            var current = set.heap.len - 1;
            defer set.heap.last = current;

            // Sift-up the BufferRange to maintain the min-heap property.
            while (current > 0) {
                const parent = (current - 1) / 2;
                assert(parent < set.heap.len);

                const parent_pos = data.heap[parent].pos;
                assert(parent_pos < value_count_max);

                const parent_slot = data.buffer[parent_pos];
                assert(parent_slot < set.values);

                const parent_key = key_from_value(&data.values[parent_slot]);
                if (compare_keys(current_key, parent_key) != .lt) break;

                mem.swap(BufferRange, &data.heap[current], &data.heap[parent]);
                current = parent;
            }
        }

        fn heap_pop(set: *TableSet) ?u32 {
            const data = set.data;
            assert(set.heap.consuming);
            if (set.heap.len == 0) return null;

            var buffer_range = &data.heap[0];
            assert(buffer_range.end <= value_count_max);
            assert(buffer_range.pos < buffer_range.end);

            const slot = data.buffer[buffer_range.pos];
            assert(slot < set.values);

            // Dequeue the buffer slot from the top-most BufferRange.
            // When the BufferRange becomes empty, swapRemove it with the last one in the heap.
            buffer_range.pos += 1;
            if (buffer_range.pos == buffer_range.end) {
                set.heap.len -= 1;
                if (set.heap.len == 0) return slot;

                mem.swap(BufferRange, &data.heap[0], &data.heap[set.heap.len]);
                assert(buffer_range.end <= value_count_max);
                assert(buffer_range.pos < buffer_range.end);
            }

            const current_key = blk: {
                const current_slot = data.buffer[buffer_range.pos];
                assert(current_slot < set.values);
                break :blk key_from_value(&data.values[current_slot]);
            };

            // Sift-down the swapRemoved BufferRange if needed to maintain the min-heap property.
            var current: u32 = 0;
            while (true) {
                var smallest = current;
                var smallest_key = current_key;

                for ([_]u32{ 1, 2 }) |offset| {
                    const child = (2 * current) + offset;
                    if (child >= set.heap.len) continue;

                    const child_pos = data.heap[child].pos;
                    assert(child_pos < value_count_max);

                    const child_slot = data.buffer[child_pos];
                    assert(child_slot < set.values);

                    const child_key = key_from_value(&data.values[child_slot]);
                    if (compare_keys(child_key, smallest_key) == .lt) {
                        smallest = child;
                        smallest_key = child_key;
                    }
                }

                if (current == smallest) return slot;
                mem.swap(BufferRange, &data.heap[current], &data.heap[smallest]);
                current = smallest;
            }
        }

        pub fn iterate_values(set: *const TableSet) ValueIterator {
            return .{ .set = set };
        }

        pub const ValueIterator = struct {
            slot: u32 = 0,
            set: *const TableSet,

            pub fn next(it: *ValueIterator) ?*const Value {
                const set = it.set;
                const data = set.data;

                while (it.slot < set.values) {
                    defer it.slot += 1;
                    if (can_delete and data.deleted.isSet(it.slot)) continue;
                    return &data.values[it.slot];
                }

                return null;
            }
        };

        pub fn consume_sorted(set: *TableSet) SortedIterator {
            assert(set.count() > 0);
            assert(!set.heap.consuming);

            if (set.buffer.len > 0) set.buffer_flush();
            assert(set.buffer.len == 0);

            set.heap.consuming = true;
            return .{ .set = set };
        }

        pub const SortedIterator = struct {
            set: ?*TableSet,
            last_slot: ?u32 = null,

            pub fn next(it: *SortedIterator) ?*const Value {
                const set = it.set orelse return null;
                const data = set.data;

                while (set.heap_pop()) |slot| {
                    assert(slot < set.values);
                    const value = &data.values[slot];

                    // Assert that we're returning sorted Values.
                    if (constants.verify) {
                        if (it.last_slot) |last_slot| {
                            assert(last_slot < set.values);
                            const last_key = key_from_value(&data.values[last_slot]);
                            assert(compare_keys(last_key, key_from_value(value)) == .lt);
                        }
                        it.last_slot = slot;
                    }

                    // Make sure not to return deleted Values.
                    if (can_delete and data.deleted.isSet(slot)) continue;
                    return value;
                }

                set.heap.consuming = false;
                it.set = null;
                return null;
            }
        };
    };
}
