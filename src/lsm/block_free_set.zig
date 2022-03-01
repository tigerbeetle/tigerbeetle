const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const MaskInt = DynamicBitSetUnmanaged.MaskInt;
const config = @import("../config.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer(
    usize,
    config.cache_line_size / @sizeOf(usize), // 1 cache line of recently-freed blocks
    .array,
);
const ewah = @import("../ewah.zig").ewah(usize);
const div_ceil = @import("../utils.zig").div_ceil;

/// The 0 address is reserved for usage as a sentinel and will never be returned by acquire().
///
/// Set bits indicate free blocks, unset bits are allocated.
pub const BlockFreeSet = struct {
    // Each bit of `index` is the OR of `shard_size` bits of `blocks`.
    // That is, if a shard has any free blocks, the corresponding index bit is set.
    index: DynamicBitSetUnmanaged,
    blocks: DynamicBitSetUnmanaged,
    // A fast cache of the 0-indexed bits (not 1-indexed addresses) of recently freed blocks.
    recent: RingBuffer,

    // Each shard is 8 cache lines because the CPU line fill buffer can fetch 10 lines in parallel.
    // And 8 is fast for division when computing the shard of a block.
    // Since the shard is scanned sequentially, the prefetching amortizes the cost of the single
    // cache miss. It also reduces the size of the index.
    //
    // e.g. 10TiB disk ÷ 64KiB/block ÷ 512*8 blocks/shard ÷ 8 shards/byte = 5120B index
    const shard_cache_lines = 8;
    const shard_size = shard_cache_lines * config.cache_line_size * @bitSizeOf(u8);
    comptime {
        assert(shard_size == 4096);
        assert(@bitSizeOf(MaskInt) == 64);
        // Ensure there are no wasted padding bits at the end of the index.
        assert(shard_size % @bitSizeOf(MaskInt) == 0);
    }

    pub fn init(allocator: mem.Allocator, blocks_count: usize) !BlockFreeSet {
        assert(shard_size <= blocks_count);
        assert(blocks_count % shard_size == 0);
        assert(blocks_count % @bitSizeOf(usize) == 0);

        // Every block bit is covered by exactly one index bit.
        const shards_count = @divExact(blocks_count, shard_size);
        var index = try DynamicBitSetUnmanaged.initFull(allocator, shards_count);
        errdefer index.deinit(allocator);

        var blocks = try DynamicBitSetUnmanaged.initFull(allocator, blocks_count);
        errdefer blocks.deinit(allocator);

        return BlockFreeSet{
            .index = index,
            .blocks = blocks,
            .recent = .{},
        };
    }

    pub fn deinit(set: *BlockFreeSet, allocator: mem.Allocator) void {
        set.index.deinit(allocator);
        set.blocks.deinit(allocator);
    }

    /// Marks a free block as allocated, and returns the address. Panics if no blocks are available.
    pub fn acquire(set: *BlockFreeSet) ?u64 {
        const block = blk: {
            if (set.recent.pop()) |block| {
                break :blk block;
            } else if (set.index.findFirstSet()) |shard| {
                break :blk set.find_free_block_in_shard(shard) orelse unreachable;
            } else return null;
        };
        const shard = block / shard_size;
        assert(set.index.isSet(shard));
        assert(set.blocks.isSet(block));

        set.blocks.unset(block);
        // Update the index when every block in the shard is allocated.
        if (set.find_free_block_in_shard(shard) == null) set.index.unset(shard);

        const address = block + 1;
        return @intCast(u64, address);
    }

    fn find_free_block_in_shard(set: *BlockFreeSet, shard: usize) ?usize {
        const shard_start = shard * shard_size;
        const shard_end = shard_start + shard_size;
        assert(shard_start < set.blocks.bit_length);

        return find_first_set_bit(set.blocks, shard_start, shard_end);
    }

    fn is_free(set: *BlockFreeSet, address: u64) bool {
        const block = address - 1;
        return set.blocks.isSet(block);
    }

    /// Given the address, marks an allocated block as free.
    pub fn release(set: *BlockFreeSet, address: u64) void {
        const block = address - 1;
        assert(!set.blocks.isSet(block));

        set.index.set(block / shard_size);
        set.blocks.set(block);
        set.recent.push(block) catch {}; // Ignore error.NoSpaceLeft if full.
    }

    /// Decodes the compressed bitset in `source` into `set`. Panics if `source`'s encoding is invalid.
    pub fn decode(set: *BlockFreeSet, source: []align(@alignOf(usize)) const u8) void {
        // Verify that this BlockFreeSet is entirely unallocated.
        assert(set.index.count() == set.index.bit_length);

        const words_decoded = ewah.decode(source, bitset_masks(set.blocks));
        assert(words_decoded * @bitSizeOf(MaskInt) <= set.blocks.bit_length);

        var shard: usize = 0;
        while (shard < set.index.bit_length) : (shard += 1) {
            if (set.find_free_block_in_shard(shard) == null) set.index.unset(shard);
        }
    }

    /// Returns the maximum number of bytes that a `BlockFreeSet` with `blocks_count` blocks needs
    /// to encode to.
    pub fn encode_size_max(blocks_count: usize) usize {
        assert(shard_size <= blocks_count);
        assert(blocks_count % shard_size == 0);
        assert(blocks_count % @bitSizeOf(usize) == 0);

        return ewah.encode_size_max(@divExact(blocks_count, @bitSizeOf(usize)));
    }

    /// Returns the number of bytes written to `target`.
    pub fn encode(set: BlockFreeSet, target: []align(@alignOf(usize)) u8) usize {
        assert(target.len == BlockFreeSet.encode_size_max(set.blocks.bit_length));

        return ewah.encode(bitset_masks(set.blocks), target);
    }
};

fn bitset_masks(bitset: DynamicBitSetUnmanaged) []usize {
    const len = div_ceil(bitset.bit_length, @bitSizeOf(MaskInt));
    return bitset.masks[0..len];
}

test "BlockFreeSet acquire/release" {
    const block_size = config.lsm_table_block_size;
    const blocks_in_tb = (1 << 40) / block_size;
    try test_block_shards_count(5120 * 8, 10 * blocks_in_tb);
    try test_block_shards_count(5120 * 8 - 1, 10 * blocks_in_tb - BlockFreeSet.shard_size);
    try test_block_shards_count(1, BlockFreeSet.shard_size); // At least one index bit is required.

    try test_acquire_release(BlockFreeSet.shard_size);
    try test_acquire_release(2 * BlockFreeSet.shard_size);
    try test_acquire_release(63 * BlockFreeSet.shard_size);
    try test_acquire_release(64 * BlockFreeSet.shard_size);
    try test_acquire_release(65 * BlockFreeSet.shard_size);
}

fn test_acquire_release(blocks_count: usize) !void {
    const expectEqual = std.testing.expectEqual;
    // Acquire everything, then release, then acquire again.
    var set = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    var empty = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer empty.deinit(std.testing.allocator);

    var i: usize = 0;
    while (i < blocks_count) : (i += 1) try expectEqual(@as(?u64, i + 1), set.acquire());
    try expectEqual(@as(?u64, null), set.acquire());

    i = 0;
    while (i < blocks_count) : (i += 1) set.release(@as(u64, i + 1));
    try expect_block_free_set_equal(empty, set);

    i = 0;
    while (i < blocks_count) : (i += 1) try expectEqual(@as(?u64, i + 1), set.acquire());
    try expectEqual(@as(?u64, null), set.acquire());

    // Exercise the RingBuffer-index sync by de/re-allocating the last bit of a shard.
    set.release(i);
    try expectEqual(@as(usize, 1), set.recent.count);
    try expectEqual(@as(?u64, i), set.acquire());
    try expectEqual(@as(usize, 0), set.recent.count);
    try expectEqual(@as(?u64, null), set.acquire());
}

fn test_block_shards_count(expect_shards_count: usize, blocks_count: usize) !void {
    var set = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    try std.testing.expectEqual(expect_shards_count, set.index.bit_length);
}

test "BlockFreeSet encode, decode, encode" {
    const shard_size = BlockFreeSet.shard_size / @bitSizeOf(usize);
    // Uniform.
    try test_encode(&.{.{ .fill = .uniform_ones, .words = shard_size }});
    try test_encode(&.{.{ .fill = .uniform_zeros, .words = shard_size }});
    try test_encode(&.{.{ .fill = .literal, .words = shard_size }});
    try test_encode(&.{.{ .fill = .uniform_ones, .words = std.math.maxInt(u16) + 1 }});

    // Mixed.
    try test_encode(&.{
        .{ .fill = .uniform_ones, .words = shard_size / 4 },
        .{ .fill = .uniform_zeros, .words = shard_size / 4 },
        .{ .fill = .literal, .words = shard_size / 4 },
        .{ .fill = .uniform_ones, .words = shard_size / 4 },
    });

    // Random.
    var seed: u64 = undefined;
    try std.os.getrandom(mem.asBytes(&seed));

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const fills = [_]TestPatternFill{ .uniform_ones, .uniform_zeros, .literal };
    var t: usize = 0;
    while (t < 10) : (t += 1) {
        var patterns = std.ArrayList(TestPattern).init(std.testing.allocator);
        defer patterns.deinit();

        var i: usize = 0;
        while (i < shard_size) : (i += 1) {
            try patterns.append(.{
                .fill = fills[random.uintLessThan(usize, fills.len)],
                .words = 1,
            });
        }
        try test_encode(patterns.items);
    }
}

const TestPattern = struct {
    fill: TestPatternFill,
    words: usize,
};

const TestPatternFill = enum { uniform_ones, uniform_zeros, literal };

fn test_encode(patterns: []const TestPattern) !void {
    var seed: u64 = undefined;
    try std.os.getrandom(mem.asBytes(&seed));

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    var blocks_count: usize = 0;
    for (patterns) |pattern| blocks_count += pattern.words * @bitSizeOf(usize);

    var decoded_expect = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer decoded_expect.deinit(std.testing.allocator);

    {
        // The `index` will start out zero-filled. Every non-zero pattern will update the
        // corresponding index bit with a one (probably many times) to ensure it ends up synced
        // with `blocks`.
        std.mem.set(usize, bitset_masks(decoded_expect.index), 0);

        // Fill the bitset according to the patterns.
        var blocks = bitset_masks(decoded_expect.blocks);
        var blocks_offset: usize = 0;
        for (patterns) |pattern| {
            var i: usize = 0;
            while (i < pattern.words) : (i += 1) {
                blocks[blocks_offset] = switch (pattern.fill) {
                    .uniform_ones => ~@as(usize, 0),
                    .uniform_zeros => 0,
                    .literal => random.intRangeLessThan(usize, 1, std.math.maxInt(usize)),
                };
                const index_bit = blocks_offset * @bitSizeOf(usize) / BlockFreeSet.shard_size;
                if (pattern.fill != .uniform_zeros) decoded_expect.index.set(index_bit);
                blocks_offset += 1;
            }
        }
        assert(blocks_offset == blocks.len);
    }

    var encoded = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(usize),
        BlockFreeSet.encode_size_max(decoded_expect.blocks.bit_length),
    );
    defer std.testing.allocator.free(encoded);

    try std.testing.expectEqual(encoded.len % 8, 0);
    const encoded_length = decoded_expect.encode(encoded);
    var decoded_actual = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer decoded_actual.deinit(std.testing.allocator);

    decoded_actual.decode(encoded[0..encoded_length]);
    try expect_block_free_set_equal(decoded_expect, decoded_actual);
}

fn expect_block_free_set_equal(a: BlockFreeSet, b: BlockFreeSet) !void {
    try expect_bitset_equal(a.blocks, b.blocks);
    try expect_bitset_equal(a.index, b.index);
}

fn expect_bitset_equal(a: DynamicBitSetUnmanaged, b: DynamicBitSetUnmanaged) !void {
    try std.testing.expectEqual(a.bit_length, b.bit_length);
    const a_masks = bitset_masks(a);
    const b_masks = bitset_masks(b);
    for (a_masks) |aw, i| try std.testing.expectEqual(aw, b_masks[i]);
}

test "BlockFreeSet decode small bitset into large bitset" {
    const shard_size = BlockFreeSet.shard_size;
    var small_set = try BlockFreeSet.init(std.testing.allocator, shard_size);
    defer small_set.deinit(std.testing.allocator);

    // Set up a small bitset (with blocks_count==shard_size) with no free blocks.
    var i: usize = 0;
    while (i < small_set.blocks.bit_length) : (i += 1) _ = small_set.acquire();
    var small_buffer = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(usize),
        BlockFreeSet.encode_size_max(small_set.blocks.bit_length),
    );
    defer std.testing.allocator.free(small_buffer);

    const small_buffer_written = small_set.encode(small_buffer);
    // Decode the serialized small bitset into a larger bitset (with blocks_count==2*shard_size).
    var big_set = try BlockFreeSet.init(std.testing.allocator, 2 * shard_size);
    defer big_set.deinit(std.testing.allocator);

    big_set.decode(small_buffer[0..small_buffer_written]);

    var block: usize = 0;
    while (block < 2 * shard_size) : (block += 1) {
        const address = block + 1;
        try std.testing.expectEqual(shard_size <= block, big_set.is_free(address));
    }
}

test "BlockFreeSet encode/decode manual" {
    const encoded_expect = mem.sliceAsBytes(&[_]usize{
        // Mask 1: run of 2 words of 0s, then 3 literals
        0 | (2 << 1) | (3 << 32),
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 1
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101, // literal 2
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 3
        // Mask 2: run of 59 words of 1s, then 0 literals
        //
        // 59 is chosen so that because the blocks_count must be a multiple of the shard size:
        // shard_size = 4096 bits = 64 words × 64 bits/word = (2+3+59)*64
        1 | ((64 - 5) << 1),
    });
    const decoded_expect = [_]usize{
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000, // run 1
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 1
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101, // literal 2
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 3
    } ++ ([1]usize{~@as(usize, 0)} ** (64 - 5));
    const blocks_count = decoded_expect.len * @bitSizeOf(usize);

    // Test decode.
    var decoded_actual = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer decoded_actual.deinit(std.testing.allocator);

    decoded_actual.decode(encoded_expect);
    try std.testing.expectEqual(decoded_expect.len, bitset_masks(decoded_actual.blocks).len);
    try std.testing.expectEqualSlices(usize, &decoded_expect, bitset_masks(decoded_actual.blocks));

    // Test encode.
    var encoded_actual = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(usize),
        BlockFreeSet.encode_size_max(decoded_actual.blocks.bit_length),
    );
    defer std.testing.allocator.free(encoded_actual);

    const encoded_actual_length = decoded_actual.encode(encoded_actual);
    try std.testing.expectEqual(encoded_expect.len, encoded_actual_length);
}

// Returns the index of a set bit (relative to the start of the bitset)
// within start…end (inclusive…exclusive).
fn find_first_set_bit(bitset: DynamicBitSetUnmanaged, start: usize, end: usize) ?usize {
    assert(end <= bitset.bit_length);
    const word_start = start / @bitSizeOf(MaskInt);
    const word_offset = @mod(start, @bitSizeOf(MaskInt));
    const word_end = div_ceil(end, @bitSizeOf(MaskInt));
    assert(word_start < word_end);

    // Only iterate over the subset of bits that were requested.
    var iter = bitset.iterator(.{});
    iter.words_remain = bitset.masks[word_start + 1 .. word_end];
    const mask = ~@as(MaskInt, 0);
    iter.bits_remain = bitset.masks[word_start] & std.math.shl(MaskInt, mask, word_offset);

    const b = start - word_offset + (iter.next() orelse return null);
    return if (b < end) b else null;
}

test "find_first_set_bit" {
    const BitSet = DynamicBitSetUnmanaged;
    const window = 8;

    // Verify that only bits within the specified range are returned.
    var size: usize = @bitSizeOf(BitSet.MaskInt);
    while (size <= @bitSizeOf(BitSet.MaskInt) * 2) : (size += 1) {
        var set = try BitSet.initEmpty(std.testing.allocator, size);
        defer set.deinit(std.testing.allocator);

        var s: usize = 0;
        while (s < size - window) : (s += 1) {
            var b: usize = 0;
            while (b < size) : (b += 1) {
                set.set(b);
                const expect = if (s <= b and b < s + window) b else null;
                try std.testing.expectEqual(expect, find_first_set_bit(set, s, s + window));
                set.unset(b);
            }
        }
    }

    {
        // Make sure the first bit is returned.
        var set = try BitSet.initEmpty(std.testing.allocator, 16);
        defer set.deinit(std.testing.allocator);
        set.set(2);
        set.set(5);
        try std.testing.expectEqual(@as(?usize, 2), find_first_set_bit(set, 1, 9));
    }

    {
        // Don't return a bit outside of the bitset's interval, even with `initFull`.
        var set = try BitSet.initFull(std.testing.allocator, 56);
        defer set.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(?usize, null), find_first_set_bit(set, 56, 56));
    }
}
