const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

/// The 0 address is reserved for usage as a sentinel and will never be returned
/// by acquire().
/// TODO add encode/decode function for run-length encoding to/from SuperBlock.
pub const BlockFreeSet = struct {
    /// Bits set indicate free blocks
    free: std.bit_set.DynamicBitSetUnmanaged,

    pub fn init(allocator: *mem.Allocator, count: usize) !BlockFreeSet {
        return BlockFreeSet{
            .free = try std.bit_set.DynamicBitSetUnmanaged.initFull(count, allocator),
        };
    }

    pub fn deinit(set: *BlockFreeSet, allocator: *mem.Allocator) void {
        set.free.deinit(allocator);
    }

    // TODO consider "caching" the first set bit to speed up subsequent acquire() calls
    pub fn acquire(set: *BlockFreeSet) u64 {
        // TODO: To ensure this "unreachable" is never reached, the leader must reject
        // new requests when storage space is too low to fulfill them.
        const bit = set.free.findFirstSet() orelse unreachable;
        set.free.unset(bit);
        const address = bit + 1;
        return @intCast(u64, address);
    }

    pub fn release(set: *BlockFreeSet, address: u64) void {
        const bit = address - 1;
        assert(!set.free.isSet(bit));
        set.free.set(bit);
    }
};
