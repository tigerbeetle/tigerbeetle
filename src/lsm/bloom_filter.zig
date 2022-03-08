//! Implementation of Split block Bloom filters: https://arxiv.org/pdf/2101.01719v4.pdf

const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const meta = std.meta;

pub const Fingerprint = struct {
    /// Hash value used to map key to block.
    hash: u32,
    /// Mask of bits set in the block for the key.
    mask: meta.Vector(8, u32),

    pub fn create(key: []const u8) Fingerprint {
        const hash = std.hash.Wyhash.hash(0, key);
        const hash_lower = @truncate(u32, hash);
        const hash_upper = @intCast(u32, hash >> 32);

        // TODO These constants are from the paper and we understand them to be arbitrary odd
        // integers. Experimentally compare the performance of these with other randomly chosen
        // odd integers to verify/improve our understanding.
        const odd_integers: meta.Vector(8, u32) = [8]u32{
            0x47b6137b,
            0x44974d91,
            0x8824ad5b,
            0xa2b7289d,
            0x705495c7,
            0x2df1424b,
            0x9efc4947,
            0x5c6bfb31,
        };

        // Multiply-shift hashing. This produces 8 values in the range 0 to 31 (2^5 - 1).
        const bit_indexes = (odd_integers * @splat(8, hash_lower)) >> @splat(8, @as(u5, 32 - 5));

        return .{
            .hash = hash_upper,
            .mask = @splat(8, @as(u32, 1)) << @intCast(meta.Vector(8, u5), bit_indexes),
        };
    }
};

/// Add the key with the given fingerprint to the filter.
/// filter.len must be a multiple of 32.
pub fn add(fingerprint: Fingerprint, filter: []u8) void {
    comptime assert(@sizeOf(meta.Vector(8, u32)) == 32);

    assert(filter.len > 0);
    assert(filter.len % @sizeOf(meta.Vector(8, u32)) == 0);

    const blocks = mem.bytesAsSlice([8]u32, filter);
    const index = block_index(fingerprint.hash, filter.len);

    const current: meta.Vector(8, u32) = blocks[index];
    blocks[index] = current | fingerprint.mask;
}

/// Check if the key with the given fingerprint may have been added to the filter.
/// filter.len must be a multiple of 32.
pub fn may_contain(fingerprint: Fingerprint, filter: []const u8) bool {
    comptime assert(@sizeOf(meta.Vector(8, u32)) == 32);

    assert(filter.len > 0);
    assert(filter.len % @sizeOf(meta.Vector(8, u32)) == 0);

    const blocks = mem.bytesAsSlice([8]u32, filter);
    const index = block_index(fingerprint.hash, filter.len);

    const current: meta.Vector(8, u32) = blocks[index];
    return @reduce(.Or, ~current & fingerprint.mask) == 0;
}

inline fn block_index(hash: u32, size: usize) u32 {
    assert(size > 0);

    const block_count = @divExact(size, @sizeOf(meta.Vector(8, u32)));
    return @intCast(u32, (@as(u64, hash) * block_count) >> 32);
}

test {
    _ = std.testing.refAllDecls(@This());
}
