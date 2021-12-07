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

/// The 0 address is reserved for usage as a sentinel and will never be returned
/// by acquire().
///
/// Set bits indicate free blocks, unset bits are allocated.
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
    // Each shard is 10 cache lines because the CPU line fill buffer can fetch 10 lines in parallel.
    // Since the shard is scanned sequentially, the prefetching amortizes the cost of the single
    // cache miss. It also reduces the size of the index.
    //
    // e.g. 10TiB disk / 64KiB block size / 640B shard size = 4096B index
    const shard_size = 10 * (config.cache_line_size * 8); // 10 cache lines per shard

    pub fn init(allocator: *mem.Allocator, blocks_count: usize) !BlockFreeSet {
        assert(shard_size <= blocks_count);
        assert(blocks_count % shard_size == 0);
        // Round up to ensure that every block bit is covered by the index.
        const shards_count = div_ceil(usize, blocks_count, shard_size);
        var index = try DynamicBitSetUnmanaged.initFull(shards_count, allocator);
        errdefer index.deinit(allocator);

        const blocks = try DynamicBitSetUnmanaged.initFull(blocks_count, allocator);
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
    // TODO consider "caching" the first set bit to speed up subsequent acquire() calls
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
        const bit = address - 1;
        assert(!set.blocks.isSet(bit));

        set.index.set(bit / shard_size);
        set.blocks.set(bit);
        set.recent.push(bit) catch {};
    }

    pub fn decode(set: *BlockFreeSet, data: []const u8) !void {
        assert(set.index.count() == set.index.bit_length);

        var blocks = try BitsetEncoder(.immutable).decode(data, &set.blocks);
        errdefer { for (set.blocks) |*blocks| blocks.* = ~0; }

        var shard: usize = 0;
        while (shard < set.index.bit_length) : (shard += 1) {
            if (set.find_free_block_in_shard(shard) == null) set.index.unset(shard);
        }
    }

    // Returns the number of bytes that the `BlockFreeSet` needs to encode to.
    pub fn encode_size(set: BlockFreeSet) usize {
        return set.make_encoder(.immutable).offsets.end;
    }

    pub fn encode(set: BlockFreeSet, target: []u8) void {
        set.make_encoder(.mutable).encode(target, set.blocks);
    }

    fn make_encoder(set: BlockFreeSet, comptime M: Mutable) BitsetEncoder(M) {
        const words = bitset_masks_length(set.blocks);
        var runs: usize = 0;
        var literals: usize = 0;
        var i: usize = 0;
        while (i < words) {
            const word = set.blocks.masks[i];
            if (word == 0 or word == ~@as(usize, 0)) {
                runs += 1;
                i += 1 + bitset_run_length(set.blocks, i + 1, words, word);
            } else {
                literals += 1;
                i += 1;
            }
        }
        return BitsetEncoder(M).init(.{
            .runs = runs,
            .literals = literals,
        });
    }
};

const Mutable = enum { mutable, immutable };
/// RLE compression of the `BlockFreeSet`.
///
/// This encoding requires that the architecture is little-endian with 64-bit words.
///
/// Encoding:
/// - `blocks_count`: `u32`(LE): equivalent to blocks_count/`BlockFreeSet.blocks.bit_length`
/// - `run_or_literal_count`: `u32`(LE): runs + literals
/// - `run_or_literal`: `[run_or_literal_count]u1`: 0=literal, 1=run (padded to word size)
/// - `run_of_zeroes_or_ones`: `[runs]u1`: specify run type (padded to word size)
/// - `run_lengths`: `[runs]u8`: one less than the run length, in words (padded to word size)
/// - `literals`: `[number of literals]u64`: 64-bit literals
fn BitsetEncoder(comptime mut: Mutable) type {
    const Bytes = if (mut == .mutable) []u8 else []const u8;
    const U32 = if (mut == .mutable) *u32 else *const u32;
    const U64s = if (mut == .mutable) []u64 else []const u64;

    const BlocksCount = u32;
    const blocks_count_size = @sizeOf(BlocksCount);
    const blocks_count_offset = 0;

    const RunOrLiteralCount = u32;
    const run_or_literal_count_size = @sizeOf(RunOrLiteralCount);
    const run_or_literal_count_offset = blocks_count_offset + blocks_count_size;
    const run_or_literal_offset = run_or_literal_count_offset + run_or_literal_count_size;

    comptime assert(std.Target.current.cpu.arch.endian() == std.builtin.Endian.Little);
    comptime assert(@bitSizeOf(MaskInt) == 64);

    return struct {
        const Self = @This();

        runs: usize,
        literals: usize,
        // Byte offsets for the start of the specified field.
        offsets: struct {
            run_or_literal: usize,
            run_of_zeroes_or_ones: usize,
            run_lengths: usize,
            literals: usize,
            end: usize, // the total byte size of the compressed bitset
        },

        fn init(params: struct {
            runs: usize,
            literals: usize,
        }) Self {
            const rol_offset = @sizeOf(u64);
            const rozoo_offset = rol_offset + div_ceil(usize, params.runs + params.literals, 64) * 8;
            const rl_offset = rozoo_offset + div_ceil(usize, params.runs, 64) * 8;
            const lit_offset = rl_offset + div_ceil(usize, params.runs, 8) * 8;
            const end_offset = lit_offset + params.literals * 8;
            assert(end_offset % 8 == 0); // each section is a multiple of word size

            return .{
                .runs = params.runs,
                .literals = params.literals,
                .offsets = .{
                    .run_or_literal = rol_offset,
                    .run_of_zeroes_or_ones = rozoo_offset,
                    .run_lengths = rl_offset,
                    .literals = lit_offset,
                    .end = end_offset,
                },
            };
        }

        fn parse(self: Self, data: Bytes) struct {
            blocks_count: U32,
            run_or_literal_count: U32,
            run_or_literal: Bytes,
            run_of_zeroes_or_ones: Bytes,
            run_lengths: Bytes,
            literals: U64s,
        } {
            const offsets = &self.offsets;
            assert(data.len == offsets.end);

            return .{
                .blocks_count = @ptrCast(U32, @alignCast(@alignOf(BlocksCount),
                    data[blocks_count_offset..][0..blocks_count_size])),
                .run_or_literal_count = @ptrCast(U32, @alignCast(@alignOf(RunOrLiteralCount),
                    data[run_or_literal_count_offset..][0..run_or_literal_count_size])),
                // `run_or_literal` and `run_of_zeroes_or_ones` slices include padding.
                .run_or_literal = data[offsets.run_or_literal..offsets.run_of_zeroes_or_ones],
                .run_of_zeroes_or_ones = data[offsets.run_of_zeroes_or_ones..offsets.run_lengths],
                // `run_lengths`'s slice excludes the padding.
                .run_lengths = data[offsets.run_lengths..(offsets.run_lengths + self.runs)],
                .literals = @alignCast(@alignOf(u64),
                    mem.bytesAsSlice(u64, data[offsets.literals..offsets.end])),
            };
        }

        fn encode(self: Self, target: []u8, bitset: DynamicBitSetUnmanaged) void {
            for (target) |*b| b.* = 0;

            const slices = self.parse(target);
            slices.blocks_count.* = @intCast(BlocksCount, bitset.bit_length);
            slices.run_or_literal_count.* = @intCast(RunOrLiteralCount, self.runs + self.literals);
            const word_count = bitset_masks_length(bitset);

            var run_or_literal_bit: usize = 0;
            var run_index: usize = 0;
            var literals_index: usize = 0;
            var word_index: usize = 0;
            while (word_index < word_count) {
                const word = bitset.masks[word_index];
                if (word == 0 or word == ~@as(usize, 0)) {
                    const run_length = 1 + bitset_run_length(bitset, word_index + 1, word_count, word);
                    slices.run_lengths[run_index] = @intCast(u8, run_length - 1);
                    if (word != 0) set_bit(slices.run_of_zeroes_or_ones, run_index);
                    set_bit(slices.run_or_literal, run_or_literal_bit);
                    run_index += 1;
                    word_index += run_length;
                } else {
                    slices.literals[literals_index] = word;
                    literals_index += 1;
                    word_index += 1;
                }
                run_or_literal_bit += 1;
            }

            assert(word_index == word_count);
            assert(slices.run_or_literal_count.* == run_or_literal_bit);
            assert(slices.run_lengths.len == run_index);
            assert(slices.literals.len == literals_index);
        }

        fn decode(data: []const u8, bitset: *DynamicBitSetUnmanaged) !void {
            const run_or_literal_count_bytes = data[run_or_literal_count_offset..][0..run_or_literal_count_size];
            const run_or_literal_count = mem.readIntSliceLittle(u32, run_or_literal_count_bytes);
            const run_or_literal_words = div_ceil(usize, run_or_literal_count, @bitSizeOf(usize));
            const run_or_literal = mem.bytesAsSlice(u64, data[run_or_literal_offset..][0..@sizeOf(usize) * run_or_literal_words]);
            var runs: usize = 0;
            for (run_or_literal) |rol| runs += @popCount(u64, rol);
            const decoder = BitsetEncoder(.immutable).init(.{
                .runs = runs,
                .literals = run_or_literal_count - runs,
            });
            const slices = decoder.parse(data);
            assert(slices.run_or_literal_count.* == run_or_literal_count);
            assert(bitset.bit_length >= slices.blocks_count.*);

            var run_index: usize = 0;
            var literals_index: usize = 0;
            var blocks_index: usize = 0;
            for (mem.bytesAsSlice(u64, slices.run_or_literal)) |rol_word, rol_i| {
                const last = rol_i == run_or_literal_words - 1;
                if (!last and rol_word == ~@as(u64, 0)) {
                    // Fast path: all runs.
                    var bit: usize = 0;
                    while (bit < 64) : (bit += 1) {
                        const run_length = @as(usize, slices.run_lengths[run_index]) + 1;
                        if (!get_bit(slices.run_of_zeroes_or_ones, run_index)) {
                            std.mem.set(usize, bitset.masks[blocks_index .. blocks_index + run_length], 0);
                        }
                        blocks_index += run_length;
                        run_index += 1;
                    }
                } else if (!last and rol_word == 0) {
                    // Fast path: all literals.
                    // The bitset is filled with 1s by default, so just skip over the word.
                    mem.copy(MaskInt, bitset.masks[blocks_index .. blocks_index + 64], slices.literals[literals_index..][0..64]);
                    blocks_index += 64;
                    literals_index += 64;
                } else {
                    // Mixed runs and literals.
                    const max_bit = if (last) (run_or_literal_count - 1) % 64 + 1 else 64;
                    var bit: usize = 0;
                    while (bit < max_bit) : (bit += 1) {
                        const is_run = rol_word & (@as(u64, 1) << @intCast(u6, bit)) != 0;
                        if (is_run) {
                            const run_length = @as(usize, slices.run_lengths[run_index]) + 1;
                            if (!get_bit(slices.run_of_zeroes_or_ones, run_index)) {
                                std.mem.set(usize, bitset.masks[blocks_index .. blocks_index + run_length], 0);
                            }
                            blocks_index += run_length;
                            run_index += 1;
                        } else {
                            bitset.masks[blocks_index] = slices.literals[literals_index];
                            blocks_index += 1;
                            literals_index += 1;
                        }
                    }
                }
            }
            assert(blocks_index * @bitSizeOf(MaskInt) == slices.blocks_count.*);
            assert(literals_index == decoder.literals);
            assert(run_index == decoder.runs);
        }
    };
}

fn bitset_masks_length(bitset: DynamicBitSetUnmanaged) usize {
    return div_ceil(MaskInt, bitset.bit_length, @bitSizeOf(MaskInt));
}

fn bitset_run_length(bitset: DynamicBitSetUnmanaged, offset: usize, len: usize, word: u64) usize {
    const max_run_length = std.math.maxInt(u8);
    const max = std.math.min(len, offset + max_run_length);
    var end: usize = offset;
    while (end < max and bitset.masks[end] == word) : (end += 1) {}
    return end - offset;
}

inline fn get_bit(b: []const u8, i: usize) bool {
    const mask = @as(u8, 1) << @intCast(u3, i % @bitSizeOf(u8));
    return (b[i / @bitSizeOf(u8)] & mask) != 0;
}

test "get_bit" {
    try std.testing.expect(!get_bit(&[_]u8{0b0000_0010}, 0));
    try std.testing.expect(get_bit(&[_]u8{0b0000_0010}, 1));
    try std.testing.expect(get_bit(&[_]u8{0, 0b0000_0010}, 8 + 1));
}

inline fn set_bit(b: []u8, i: usize) void {
    b[i / @bitSizeOf(u8)] |= @as(u8, 1) << @intCast(u3, i % @bitSizeOf(u8));
}

test "set_bit" {
    var b = [_]u8{0, 0};
    try std.testing.expect(!get_bit(&b, 9));
    set_bit(&b, 9);
    try std.testing.expect(get_bit(&b, 9));
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

test "BlockFreeSet" {
    const block_bytes = 64 * 1024;
    const blocks_in_tb = (1 << 40) / block_bytes;
    try test_block_index_size(4096 * 8, 10 * blocks_in_tb);
    try test_block_index_size(4096 * 8 - 1, 10 * blocks_in_tb - BlockFreeSet.shard_size);
    try test_block_index_size(1, BlockFreeSet.shard_size); // At least one index bit is required.
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

    // Exercise the try_acquire_recent index sync by de/re-allocating the last bit of a shard.
    set.release(i);
    try expectEqual(@as(usize, 1), set.recent.count);
    try expectEqual(@as(?u64, i), set.acquire());
    try expectEqual(@as(usize, 0), set.recent.count);
    try expectEqual(@as(?u64, null), set.acquire());
}

fn test_block_index_size(expect_shards_count: usize, blocks_count: usize) !void {
    var set = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    try std.testing.expectEqual(expect_shards_count, set.index.bit_length);
}

test "BlockFreeSet encode/decode" {
    try test_block_free_set_encode(BlockFreeSet.shard_size, 0); // fully free
    try test_block_free_set_encode(BlockFreeSet.shard_size, BlockFreeSet.shard_size); // fully allocated

    // 256 (max run length) * 65 runs/word (to ensure fast path) * 64 blocks/word
    const big = 256 * 65 * 64;
    try test_block_free_set_encode(big, 0);
    try test_block_free_set_encode(big, big);
    try test_block_free_set_encode(big, big / 16); // mostly free
    try test_block_free_set_encode(big, big / 16 * 15); // mostly full

    // Decode a small bitset into a larger buffer.
    {
        const shard_size = BlockFreeSet.shard_size;

        var small_set = try BlockFreeSet.init(std.testing.allocator, shard_size);
        defer small_set.deinit(std.testing.allocator);
        var i: usize = 0;
        while (i < small_set.blocks.bit_length) : (i += 1) _ = small_set.acquire();

        var small_buffer = try std.testing.allocator.alloc(u8, small_set.encode_size());
        defer std.testing.allocator.free(small_buffer);

        try std.testing.expectEqual(small_buffer.len % 8, 0);
        small_set.encode(small_buffer);

        var big_set = try BlockFreeSet.init(std.testing.allocator, 2 * shard_size);
        defer big_set.deinit(std.testing.allocator);

        try big_set.decode(small_buffer);

        var block: usize = 0;
        while (block < 2 * shard_size) : (block += 1) {
            const address = block + 1;
            try std.testing.expectEqual(shard_size <= block, big_set.is_free(address));
        }
    }
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
            try std.testing.expectEqual(@as(?u64, i + 1), set.acquire());
        }

        var j: usize = 0;
        while (j < unset_bits) : (j += 1) {
            const address = prng.random.uintLessThan(usize, blocks_count) + 1;
            if (!set.is_free(address)) set.release(address);
        }
    }

    var buffer = try std.testing.allocator.alloc(u8, set.encode_size());
    defer std.testing.allocator.free(buffer);

    try std.testing.expectEqual(buffer.len % 8, 0);
    set.encode(buffer);
    var set2 = try BlockFreeSet.init(std.testing.allocator, blocks_count);
    defer set2.deinit(std.testing.allocator);

    try set2.decode(buffer);
    try expect_block_free_set_equal(set, set2);
}

fn expect_block_free_set_equal(a: BlockFreeSet, b: BlockFreeSet) !void {
    try expect_bitset_equal(a.blocks, b.blocks);
    try expect_bitset_equal(a.index, b.index);
}

fn expect_bitset_equal(a: DynamicBitSetUnmanaged, b: DynamicBitSetUnmanaged) !void {
    try std.testing.expectEqual(a.bit_length, b.bit_length);
    var i: usize = 0;
    while (i < bitset_masks_length(a)) : (i += 1) try std.testing.expectEqual(a.masks[i], b.masks[i]);
}

test "BitsetEncoder" {
    const literals = 2;
    const runs = 2;
    const run_lengths = [_]usize{5, 3};
    const blocks_count = 64 * (literals + run_lengths[0] + run_lengths[1]);
    const encoded_expect = mem.sliceAsBytes(&[_]u64{
        blocks_count |
        (runs + literals) << 32,
        // run, literal, run, literal
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000101,
        // run of ones, run of zeroes
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001,
        // run of 5 words, run of 3 words
        (run_lengths[0] - 1) |
        (run_lengths[1] - 1) << 8,
        // literals:
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010,
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101,
    });

    const decoded_expect = [_]u64{
        0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111, // run 1
        0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
        0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
        0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
        0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 1
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000, // run 2
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101, // literal 2
    };

    var decoded_actual = try DynamicBitSetUnmanaged.initFull(blocks_count, std.testing.allocator);
    defer decoded_actual.deinit(std.testing.allocator);
    try BitsetEncoder(.immutable).decode(encoded_expect, &decoded_actual);

    try std.testing.expectEqual(blocks_count, decoded_actual.bit_length);
    try std.testing.expectEqual(decoded_expect.len, bitset_masks_length(decoded_actual));
    try std.testing.expectEqualSlices(u64, &decoded_expect, decoded_actual.masks[0..decoded_expect.len]);
}
