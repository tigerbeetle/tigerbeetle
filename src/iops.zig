const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

/// Take a u8 to limit to 255 items max (maxInt(u8) == 255).
pub fn IOPSType(comptime T: type, comptime size: u8) type {
    const Map = stdx.BitSetType(size);

    return struct {
        const IOPS = @This();

        items: [size]T = undefined,
        busy: Map = .{},

        pub fn acquire(self: *IOPS) ?*T {
            const i = self.busy.first_unset() orelse return null;
            self.busy.set(i);
            return &self.items[i];
        }

        pub fn release(self: *IOPS, item: *T) void {
            item.* = undefined;
            const i = self.index(item);
            assert(self.busy.is_set(i));
            self.busy.unset(i);
        }

        pub fn index(self: *IOPS, item: *T) usize {
            const i = @divExact(
                (@intFromPtr(item) - @intFromPtr(&self.items)),
                @sizeOf(T),
            );
            assert(i < size);
            return i;
        }

        /// Returns the count of IOPs available.
        pub fn available(self: *const IOPS) usize {
            return self.busy.capacity() - self.busy.count();
        }

        pub inline fn total(_: *const IOPS) usize {
            return size;
        }

        /// Returns the count of IOPs in use.
        pub fn executing(self: *const IOPS) usize {
            return self.busy.count();
        }

        pub const Iterator = struct {
            iops: *IOPS,
            bitset_iterator: Map.Iterator,

            pub fn next(iterator: *@This()) ?*T {
                const i = iterator.bitset_iterator.next() orelse return null;
                return &iterator.iops.items[i];
            }
        };

        pub const IteratorConst = struct {
            iops: *const IOPS,
            bitset_iterator: Map.Iterator,

            pub fn next(iterator: *@This()) ?*const T {
                const i = iterator.bitset_iterator.next() orelse return null;
                return &iterator.iops.items[i];
            }
        };

        /// Iterates over all currently executing IOPs.
        pub fn iterate(self: *IOPS) Iterator {
            return .{
                .iops = self,
                .bitset_iterator = self.busy.iterate(),
            };
        }

        /// Iterates over all currently executing IOPs.
        pub fn iterate_const(self: *const IOPS) IteratorConst {
            return .{
                .iops = self,
                .bitset_iterator = self.busy.iterate(),
            };
        }
    };
}

test "IOPS" {
    const testing = std.testing;
    var iops = IOPSType(u32, 4){};

    try testing.expectEqual(@as(usize, 4), iops.available());
    try testing.expectEqual(@as(usize, 0), iops.executing());

    var one = iops.acquire().?;

    try testing.expectEqual(@as(usize, 3), iops.available());
    try testing.expectEqual(@as(usize, 1), iops.executing());

    var two = iops.acquire().?;
    var three = iops.acquire().?;

    try testing.expectEqual(@as(usize, 1), iops.available());
    try testing.expectEqual(@as(usize, 3), iops.executing());

    var four = iops.acquire().?;
    try testing.expectEqual(@as(?*u32, null), iops.acquire());

    try testing.expectEqual(@as(usize, 0), iops.available());
    try testing.expectEqual(@as(usize, 4), iops.executing());

    iops.release(two);

    try testing.expectEqual(@as(usize, 1), iops.available());
    try testing.expectEqual(@as(usize, 3), iops.executing());

    // there is only one slot free, so we will get the same pointer back.
    try testing.expectEqual(@as(?*u32, two), iops.acquire());

    iops.release(four);
    iops.release(two);
    iops.release(one);
    iops.release(three);

    try testing.expectEqual(@as(usize, 4), iops.available());
    try testing.expectEqual(@as(usize, 0), iops.executing());

    one = iops.acquire().?;
    two = iops.acquire().?;
    three = iops.acquire().?;
    four = iops.acquire().?;
    try testing.expectEqual(@as(?*u32, null), iops.acquire());
}
