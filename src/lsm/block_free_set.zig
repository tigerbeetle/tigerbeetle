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

const MarkerLiterals = u31;
const MarkerRunLength = u32;
const marker_run_length_max = std.math.maxInt(MarkerRunLength);
const marker_literals_max = std.math.maxInt(MarkerLiterals);

comptime {
    assert(std.Target.current.cpu.arch.endian() == std.builtin.Endian.Little);
    assert(@bitSizeOf(MaskInt) == 64);
    assert(@bitSizeOf(usize) == 64);
}

/// The 0 address is reserved for usage as a sentinel and will never be returned by acquire().
///
/// Set bits indicate free blocks, unset bits are allocated.
///
/// `encode`/`decode` compress the bitset using EWAH.
///
/// "Histogram-Aware Sorting for Enhanced Word-Aligned Compression in Bitmap Indexes":
///
/// > [EWAH] also uses only two types of words, where the first type is a 64-bit verbatim word.
/// > The second type of word is a marker word: the first bit indicates which clean word will follow,
/// > half the bits (32 bits) are used to store the number of clean words, and the rest of the bits
/// > (31 bits) are used to store the number of dirty words following the clean words. EWAH bitmaps
/// > begin with a marker word.
///
/// A 'marker' looks like:
///
///     [run_bit:u1][run_length:u31(LE)][literals_count:u32(LE)]
///
/// and is immediately followed by `literals_count` 64-bit literals.
/// This encoding requires that the architecture is little-endian with 64-bit words.
pub const BlockFreeSet = struct {
    // Each bit of `index` is the OR of `shard_size` bits of `blocks`.
    // That is, if a shard has any free blocks, the corresponding index bit is set.
    index: DynamicBitSetUnmanaged,
    blocks: DynamicBitSetUnmanaged,
    // A fast cache of the 0-indexed bits (not 1-indexed addresses) of recently freed blocks.
    recent: RingBuffer = .{},

    // Fixing the shard size to a constant rather than varying the shard size (but
    // guaranteeing the index always a multiple of 64B) means that the top-level index
    // may have some unused bits. But the shards themselves are always a multiple of
    // the word size. In practice the tail end of the index will be accessed less
    // frequently than the head/middle anyway.
    //
    // Each shard is 8 cache lines because the CPU line fill buffer can fetch 10 lines in parallel. And 8 is fast for division when computing the shard of a block.
    // Since the shard is scanned sequentially, the prefetching amortizes the cost of the single
    // cache miss. It also reduces the size of the index.
    //
    // e.g. 10TiB disk ÷ 64KiB/block ÷ 512*8 blocks/shard ÷ 8 shards/byte = 5120B index
    const shard_cache_lines = 8;
    const shard_size = shard_cache_lines * config.cache_line_size * @bitSizeOf(u8);
    comptime {
        assert(shard_size == 4096);
    }

    pub fn init(allocator: *mem.Allocator, blocks_count: usize) !BlockFreeSet {
        assert(shard_size <= blocks_count);
        assert(blocks_count % shard_size == 0);

        // Round up to ensure that every block bit is covered by the index.
        const shards_count = div_ceil(usize, blocks_count, shard_size);
        var index = try DynamicBitSetUnmanaged.initFull(shards_count, allocator);
        errdefer index.deinit(allocator);

        var blocks = try DynamicBitSetUnmanaged.initFull(blocks_count, allocator);
        errdefer blocks.deinit(allocator);

        return BlockFreeSet{
            .index = index,
            .blocks = blocks,
        };
    }

    pub fn deinit(set: *BlockFreeSet, allocator: *mem.Allocator) void {
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
        assert(set.blocks.isSet(block));
        assert(set.index.isSet(shard));

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

    /// Marks the specified block as free.
    pub fn release(set: *BlockFreeSet, address: u64) void {
        const block = address - 1;
        assert(!set.blocks.isSet(block));

        set.index.set(block / shard_size);
        set.blocks.set(block);
        set.recent.push(block) catch {};
    }

    /// Decodes the compressed bitset in `source` into `set`. Panics if `source`'s encoding is invalid.
    pub fn decode(set: *BlockFreeSet, source: []const u8) void {
        // Verify that this BlockFreeSet is entirely unallocated.
        assert(set.index.count() == set.index.bit_length);

        const source_words = @alignCast(@alignOf(usize), mem.bytesAsSlice(usize, source));
        var source_index: usize = 0;
        const blocks = bitset_masks(set.blocks);
        var block_index: usize = 0;
        while (source_index < source_words.len) {
            const source_word = source_words[source_index];
            const run_length = source_word >> 32;
            const run_word: usize = if (source_word & 1 == 1) ~@as(u64, 0) else 0;
            const literals_count = (source_word & marker_literals_max) >> 1;
            std.mem.set(usize, blocks[block_index..][0..run_length], run_word);
            std.mem.copy(usize, blocks[block_index + run_length..][0..literals_count],
                source_words[source_index + 1..][0..literals_count]);
            block_index += run_length + literals_count;
            source_index += 1 + literals_count;
        }
        assert(block_index <= blocks.len);
        assert(source_index == source_words.len);

        var shard: usize = 0;
        while (shard < set.index.bit_length) : (shard += 1) {
            if (set.find_free_block_in_shard(shard) == null) set.index.unset(shard);
        }
    }


    // Returns the maximum number of bytes that the `BlockFreeSet` needs to encode to.
    pub fn encode_size_max(set: BlockFreeSet) usize {
        assert(set.blocks.bit_length % @bitSizeOf(usize) == 0);
        // Assume (pessimistically) that every word will be encoded as a literal.
        const literals_count = bitset_masks(set.blocks).len;
        const markers = div_ceil(usize, literals_count, marker_literals_max);
        return (literals_count + markers) * @sizeOf(usize);
    }

    // Returns the number of bytes written to `target`.
    pub fn encode(set: BlockFreeSet, target: []u8) usize {
        assert(target.len == set.encode_size_max());

        const target_words = @alignCast(@alignOf(usize), mem.bytesAsSlice(usize, target));
        std.mem.set(usize, target_words, 0);
        var target_index: usize = 0;
        const blocks = bitset_masks(set.blocks);
        var blocks_index: usize = 0;
        while (blocks_index < blocks.len) {
            const word_next = blocks[blocks_index];
            const run_length = rl: {
                if (is_literal(word_next)) break :rl 0;
                // Measure run length.
                const run_max = std.math.min(blocks.len, blocks_index + marker_run_length_max);
                var run_end: usize = blocks_index + 1;
                while (run_end < run_max and blocks[run_end] == word_next) : (run_end += 1) {}
                const run = run_end - blocks_index;
                blocks_index += run;
                break :rl run;
            };
            // Count sequential literals that immediately follow the run.
            const literals_max = std.math.min(blocks.len - blocks_index, marker_literals_max);
            const literal_count = for (blocks[blocks_index..][0..literals_max]) |word, i| {
                if (!is_literal(word)) break i;
            } else literals_max;

            assert(run_length <= marker_run_length_max);
            assert(literal_count <= marker_literals_max);
            target_words[target_index] = (word_next & 1)
                | (literal_count << 1)
                | (run_length << 32);

            std.mem.copy(usize, target_words[target_index + 1..][0..literal_count],
                blocks[blocks_index..][0..literal_count]);
            target_index += 1 + literal_count; // +1 for the marker word
            blocks_index += literal_count;
        }
        assert(blocks_index == blocks.len);

        return target_index * @sizeOf(usize);
    }

    inline fn is_literal(word: u64) bool {
        return word != 0 and word != ~@as(u64, 0);
    }
};

fn bitset_masks(bitset: DynamicBitSetUnmanaged) []usize {
    const len = div_ceil(MaskInt, bitset.bit_length, @bitSizeOf(MaskInt));
    return bitset.masks[0..len];
}

fn div_ceil(comptime T: type, a: T, b: T) T {
    return (a + b - 1) / b;
}

test "div_ceil" {
    try std.testing.expectEqual(div_ceil(usize, 1, 8), 1);
    try std.testing.expectEqual(div_ceil(usize, 8, 8), 1);
    try std.testing.expectEqual(div_ceil(usize, 9, 8), 2);
}

// Returns the index of a set bit (relative to the start of the bitset) within start…end (inclusive…exclusive).
fn find_first_set_bit(bitset: DynamicBitSetUnmanaged, start: usize, end: usize) ?usize {
    assert(end <= bitset.bit_length);
    const word_start = start / @bitSizeOf(MaskInt);
    const word_offset = @mod(start, @bitSizeOf(MaskInt));
    const word_end = div_ceil(usize, end, @bitSizeOf(MaskInt));
    assert(word_start < word_end);

    // Only iterate over the subset of bits that were requested.
    var iter = bitset.iterator(.{});
    iter.words_remain = bitset.masks[word_start+1..word_end];
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
        var set = try BitSet.initEmpty(size, std.testing.allocator);
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
        var set = try BitSet.initEmpty(16, std.testing.allocator);
        defer set.deinit(std.testing.allocator);
        set.set(2);
        set.set(5);
        try std.testing.expectEqual(@as(?usize, 2), find_first_set_bit(set, 1, 9));
    }

    {
        // Don't return a bit outside of the bitset's interval, even with `initFull`.
        var set = try BitSet.initFull(56, std.testing.allocator);
        defer set.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(?usize, null), find_first_set_bit(set, 56, 56));
    }
}

test "BlockFreeSet acquire/release" {
    const block_size = config.lsm_table_block_size;
    const blocks_in_tb = (1 << 40) / block_size;
    try test_block_shards_count(5120 * 8, 10 * blocks_in_tb);
    try test_block_shards_count(5120 * 8 - 1, 10 * blocks_in_tb - BlockFreeSet.shard_size);
    try test_block_shards_count(1, BlockFreeSet.shard_size); // At least one index bit is required.
    // Block counts are not necessarily a multiple of the word size.
    try test_block_free_set(BlockFreeSet.shard_size);
    try test_block_free_set(2 * BlockFreeSet.shard_size);
    try test_block_free_set(63 * BlockFreeSet.shard_size);
    try test_block_free_set(64 * BlockFreeSet.shard_size);
    try test_block_free_set(65 * BlockFreeSet.shard_size);
}

fn test_block_free_set(blocks_count: usize) !void {
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

test "BlockFreeSet encode/decode" {
    try test_block_free_set_encode(BlockFreeSet.shard_size, 0); // fully allocated
    try test_block_free_set_encode(BlockFreeSet.shard_size, BlockFreeSet.shard_size); // fully free

    // TODO update old comment
    // 256 (max run length) * 65 runs/word (to ensure fast path) * 64 blocks/word
    const big = 256 * 65 * 64;
    try test_block_free_set_encode(big, 0);
    try test_block_free_set_encode(big, big);
    try test_block_free_set_encode(big, big / 16); // mostly free
    try test_block_free_set_encode(big, big / 16 * 15); // mostly full
}

fn test_block_free_set_encode(blocks_count: usize, unset_bits: usize) !void {
    assert(unset_bits <= blocks_count);
    var seed: u64 = undefined;
    try std.os.getrandom(mem.asBytes(&seed));
    var prng = std.rand.DefaultPrng.init(seed);

    var set = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    // To set up a BlockFreeSet for testing, first allocate everything, then free selected blocks.
    // This ensures that the index and block bitsets are synced.
    if (unset_bits < blocks_count) {
        var i: usize = 0;
        while (i < blocks_count) : (i += 1) {
            const address = i + 1;
            try std.testing.expectEqual(@as(?u64, address), set.acquire());
        }

        var j: usize = 0;
        while (j < unset_bits) : (j += 1) {
            const address = prng.random.uintLessThan(usize, blocks_count) + 1;
            if (!set.is_free(address)) set.release(address);
        }
    }

    var buffer = try std.testing.allocator.alloc(u8, set.encode_size_max());
    defer std.testing.allocator.free(buffer);

    try std.testing.expectEqual(buffer.len % 8, 0);
    const buffer_wrote = set.encode(buffer);
    var set2 = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer set2.deinit(std.testing.allocator);

    set2.decode(buffer[0..buffer_wrote]);
    try expect_block_free_set_equal(set, set2);
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

test "BlockFreeSet decode small bitset into large biset" {
    const shard_size = BlockFreeSet.shard_size;
    var small_set = try BlockFreeSet.init(std.testing.allocator, shard_size);
    defer small_set.deinit(std.testing.allocator);

    // Set up a small bitset (with blocks_count==shard_size) with no free blocks.
    var i: usize = 0;
    while (i < small_set.blocks.bit_length) : (i += 1) _ = small_set.acquire();
    var small_buffer = try std.testing.allocator.alloc(u8, small_set.encode_size_max());
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
        0 | (2 << 32) | (3 << 1),
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 1
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101, // literal 2
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 3
        // Mask 2: run of 59 words of 1s, then 0 literals
        //
        // 59 is chosen so that because the blocks_count must be a multiple of the shard size:
        // shard_size = 4096 bits = 64 words × 64 bits/word = (2+3+59)*64
        1 | ((64 - 5) << 32),
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
    var encoded_actual = try std.testing.allocator.alloc(u8, decoded_actual.encode_size_max());
    defer std.testing.allocator.free(encoded_actual);

    const encoded_actual_length = decoded_actual.encode(encoded_actual);
    try std.testing.expectEqual(encoded_expect.len, encoded_actual_length);
}
