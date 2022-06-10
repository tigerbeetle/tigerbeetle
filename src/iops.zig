const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

/// Take a u6 to limit to 64 items max (2^6 = 64)
pub fn IOPS(comptime T: type, comptime size: u6) type {
    const Map = std.meta.Int(.unsigned, size);
    const MapLog2 = math.Log2Int(Map);
    return struct {
        const Self = @This();

        items: [size]T = undefined,
        /// 1 bits are free items
        free: Map = math.maxInt(Map),

        pub fn acquire(self: *Self) ?*T {
            const i = @ctz(Map, self.free);
            assert(i <= @bitSizeOf(Map));
            if (i == @bitSizeOf(Map)) return null;
            self.free &= ~(@as(Map, 1) << @intCast(MapLog2, i));
            return &self.items[i];
        }

        pub fn release(self: *Self, item: *T) void {
            item.* = undefined;
            const i = (@ptrToInt(item) - @ptrToInt(&self.items)) / @sizeOf(T);
            assert(self.free & (@as(Map, 1) << @intCast(MapLog2, i)) == 0);
            self.free |= (@as(Map, 1) << @intCast(MapLog2, i));
        }

        /// Returns true if there is at least one IOP available
        pub fn available(self: *const Self) math.Log2IntCeil(Map) {
            return @popCount(Map, self.free);
        }

        /// Returns true if there is at least one IOP in use
        pub fn executing(self: *const Self) math.Log2IntCeil(Map) {
            return @popCount(Map, math.maxInt(Map)) - @popCount(Map, self.free);
        }

        pub const Iterator = struct {
            iops: *Self,
            /// On iteration start this is a copy of the free map, but
            /// inverted so we can use @ctz() to find occupied instead of free slots.
            unseen: Map,

            pub fn next(iterator: *Iterator) ?*T {
                const i = @ctz(Map, iterator.unseen);
                assert(i <= @bitSizeOf(Map));
                if (i == @bitSizeOf(Map)) return null;
                // Set this bit of unseen to 1 to indicate this slot has been seen.
                iterator.unseen &= ~(@as(Map, 1) << @intCast(MapLog2, i));
                return &iterator.iops.items[i];
            }
        };

        pub fn iterate(self: *Self) Iterator {
            return .{ .iops = self, .unseen = ~self.free };
        }
    };
}

test {
    const testing = std.testing;
    var iops = IOPS(u32, 4){};

    try testing.expectEqual(@as(u4, 4), iops.available());
    try testing.expectEqual(@as(u4, 0), iops.executing());

    var one = iops.acquire().?;

    try testing.expectEqual(@as(u4, 3), iops.available());
    try testing.expectEqual(@as(u4, 1), iops.executing());

    var two = iops.acquire().?;
    var three = iops.acquire().?;

    try testing.expectEqual(@as(u4, 1), iops.available());
    try testing.expectEqual(@as(u4, 3), iops.executing());

    var four = iops.acquire().?;
    try testing.expectEqual(@as(?*u32, null), iops.acquire());

    try testing.expectEqual(@as(u4, 0), iops.available());
    try testing.expectEqual(@as(u4, 4), iops.executing());

    iops.release(two);

    try testing.expectEqual(@as(u4, 1), iops.available());
    try testing.expectEqual(@as(u4, 3), iops.executing());

    // there is only one slot free, so we will get the same pointer back.
    try testing.expectEqual(@as(?*u32, two), iops.acquire());

    iops.release(four);
    iops.release(two);
    iops.release(one);
    iops.release(three);

    try testing.expectEqual(@as(u4, 4), iops.available());
    try testing.expectEqual(@as(u4, 0), iops.executing());

    one = iops.acquire().?;
    two = iops.acquire().?;
    three = iops.acquire().?;
    four = iops.acquire().?;
    try testing.expectEqual(@as(?*u32, null), iops.acquire());
}
