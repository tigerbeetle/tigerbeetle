const std = @import("std");
const assert = std.debug.assert;

pub fn find_first_unset(self: std.DynamicBitSetUnmanaged) ?u64 {
    var offset: u64 = 0;
    var mask = self.masks;
    while (offset < self.bit_length) {
        if (~mask[0] != 0) break;
        mask += 1;
        offset += @bitSizeOf(std.DynamicBitSetUnmanaged.MaskInt);
    } else return null;
    const first_unset = offset + @ctz(~mask[0]);
    return if (first_unset < self.bit_length) first_unset else null;
}

pub fn PoolType(comptime T: type) type {
    return struct {
        const Pool = @This();

        items: []T,
        busy: std.DynamicBitSetUnmanaged,

        pub fn init(gpa: std.mem.Allocator, capacity: u64) !Pool {
            const items = try gpa.alloc(T, capacity);
            errdefer gpa.free(items);
            const busy = try std.DynamicBitSetUnmanaged.initEmpty(gpa, capacity);
            errdefer busy.deinit(gpa);
            return Pool{ .items = items, .busy = busy };
        }

        pub fn deinit(self: *Pool, gpa: std.mem.Allocator) void {
            self.busy.deinit(gpa);
            gpa.free(self.items);
        }

        pub fn acquire(self: *Pool) ?*T {
            const i = find_first_unset(self.busy) orelse return null;
            self.busy.set(i);
            return &self.items[i];
        }

        pub fn release(self: *Pool, item: *T) void {
            const i = self.index(item);
            assert(self.busy.isSet(i));
            self.busy.unset(i);
        }

        pub fn index(self: *Pool, item: *T) u64 {
            const i = @divExact(
                (@intFromPtr(item) - @intFromPtr(self.items.ptr)),
                @sizeOf(T),
            );
            assert(i < self.items.len);
            return i;
        }

        /// Returns the count of elements available.
        pub fn available(self: *const Pool) u64 {
            return self.busy.capacity() - self.busy.count();
        }

        pub inline fn total(self: *const Pool) u64 {
            return self.items.len;
        }

        /// Returns the count of elements in use.
        pub fn in_use(self: *const Pool) u64 {
            return self.busy.count();
        }

        pub const Iterator = struct {
            pool: *Pool,
            bitset_iterator: std.DynamicBitSetUnmanaged.Iterator(.{}),

            pub fn next(iterator: *@This()) ?*T {
                const i = iterator.bitset_iterator.next() orelse return null;
                return &iterator.pool.items[i];
            }
        };

        pub fn iterate(self: *Pool) Iterator {
            return .{
                .pool = self,
                .bitset_iterator = self.busy.iterator(.{}),
            };
        }
    };
}

test find_first_unset {
    const alloc = std.testing.allocator;
    const capacity = 4;
    var bitset = try std.DynamicBitSetUnmanaged.initEmpty(alloc, capacity);
    defer bitset.deinit(alloc);
    for (0..capacity) |index| {
        bitset.set(index);
        if (index + 1 == capacity) {
            try std.testing.expectEqual(null, find_first_unset(bitset));
        } else {
            try std.testing.expectEqual(index + 1, find_first_unset(bitset));
        }
    }
    try std.testing.expectEqual(capacity, bitset.count());
    for (1..capacity + 1) |index| {
        const reverse_index = capacity - index;
        bitset.unset(reverse_index);
        try std.testing.expectEqual(find_first_unset(bitset), reverse_index);
    }
    try std.testing.expectEqual(0, bitset.count());
}

test PoolType {
    const testing = std.testing;
    var pool = try PoolType(u32).init(testing.allocator, 4);
    defer pool.deinit(testing.allocator);

    try testing.expectEqual(@as(u64, 4), pool.available());
    try testing.expectEqual(@as(u64, 0), pool.in_use());

    var one = pool.acquire().?;

    try testing.expectEqual(@as(u64, 3), pool.available());
    try testing.expectEqual(@as(u64, 1), pool.in_use());

    var two = pool.acquire().?;
    var three = pool.acquire().?;

    try testing.expectEqual(@as(u64, 1), pool.available());
    try testing.expectEqual(@as(u64, 3), pool.in_use());

    var four = pool.acquire().?;
    try testing.expectEqual(@as(?*u32, null), pool.acquire());

    try testing.expectEqual(@as(u64, 0), pool.available());
    try testing.expectEqual(@as(u64, 4), pool.in_use());

    pool.release(two);

    try testing.expectEqual(@as(u64, 1), pool.available());
    try testing.expectEqual(@as(u64, 3), pool.in_use());

    // there is only one slot free, so we will get the same pointer back.
    try testing.expectEqual(@as(?*u32, two), pool.acquire());

    pool.release(four);
    pool.release(two);
    pool.release(one);
    pool.release(three);

    try testing.expectEqual(@as(u64, 4), pool.available());
    try testing.expectEqual(@as(u64, 0), pool.in_use());

    one = pool.acquire().?;
    two = pool.acquire().?;
    three = pool.acquire().?;
    four = pool.acquire().?;
    try testing.expectEqual(@as(?*u32, null), pool.acquire());
    pool.release(one);
    pool.release(two);
    pool.release(three);
    pool.release(four);
}
