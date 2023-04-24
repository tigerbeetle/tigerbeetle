const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;
const stdx = @import("../stdx.zig");

pub fn TableValuesType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const value_count_max = Table.value_count_max;

    return struct {
        const ValuesMap = @This();

        // Ensure we can use compressed u24 indexes into the values array.
        comptime {
            assert(value_count_max <= math.maxInt(u24));
        }

        const capacity = math.ceilPowerOfTwo(u32, value_count_max * 14 / 12) catch unreachable;
        const Data = struct {
            tags: [capacity]u8,
            slots: [capacity * 3]u8,
            values: [value_count_max]Value,
        };

        len: u32 = 0,
        data: *Data,

        pub fn init(allocator: mem.Allocator) !ValuesMap {
            var map = ValuesMap{ .data = try allocator.create(Data) };
            map.clear();
            return map;
        }

        pub fn deinit(map: *ValuesMap, allocator: mem.Allocator) void {
            allocator.destroy(map.data);
        }

        pub inline fn inserted(map: *const ValuesMap) []Value {
            return map.data.values[0..map.len];
        }

        pub fn clear(map: *ValuesMap) void {
            map.len = 0;
            for (map.data.tags) |*tag| tag.* = 0;
        }

        pub fn find(map: *const ValuesMap, key: Key) ?*const Value {
            if (map.len == 0) return null;

            var search = Search.start(map.data, key);
            while (true) : (search.next()) {
                if (search.match()) |value| return value;
                if (search.empty()) return null;
            }
        }

        pub const Entry = struct {
            value: *Value,
            found_existing: bool,
        };

        pub fn upsert(map: *ValuesMap, key: Key) Entry {
            @setRuntimeSafety(false);

            var search = Search.start(map.data, key);
            while (true) : (search.next()) {
                if (search.match()) |value| {
                    return .{ .value = value, .found_existing = true };
                }
                if (search.empty()) {
                    return .{ .value = map.insert(&search), .found_existing = false };
                }
            }
        }

        pub fn insert_no_clobber(map: *ValuesMap, value: *const Value) void {
            @setRuntimeSafety(false);

            var search = Search.start(map.data, key_from_value(value));
            while (!search.empty()) search.next();

            const new_value = map.insert(&search);
            new_value.* = value.*;
        }

        inline fn insert(map: *ValuesMap, search: *const Search) *Value {
            const index = @intCast(u24, map.len);
            map.len += 1;
            return search.insert(index);
        }

        const Search = struct {
            pos: u32,
            tag: u8,
            data: *Data,
            key: Key,

            inline fn start(data: *Data, key: Key) Search {
                const hash = stdx.fast_hash(&key);
                return .{
                    .pos = @truncate(u32, hash) % capacity,
                    .tag = @truncate(u8, hash >> (64 - 8)) | 0x80,
                    .data = data,
                    .key = key,
                };
            }

            inline fn next(self: *Search) void {
                const probe = (2 * @as(u32, self.tag)) + 1;
                self.pos = (self.pos + probe) % capacity;
            }

            inline fn match(self: *const Search) ?*Value {
                if (self.data.tags[self.pos] != self.tag) return null;
                const index_ptr = &self.data.slots[self.pos * 3];
                const index = @ptrCast(*align(1) u24, index_ptr).*;
                const value = &self.data.values[index];
                if (compare_keys(self.key, key_from_value(value)) == .eq) return value;
                return null;
            }

            inline fn empty(self: *const Search) bool {
                return self.data.tags[self.pos] == 0;
            }

            inline fn insert(self: *const Search, index: u24) *Value {
                self.data.tags[self.pos] = self.tag;
                const index_ptr = &self.data.slots[self.pos * 3];
                @ptrCast(*align(1) u24, index_ptr).* = index;
                return &self.data.values[index];
            }
        };
    };
}
