const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;

/// The 0 address is reserved for usage as a sentinel and will never be returned
/// by acquire().
///
/// Set bits indicate free blocks, unset bits are allocated.
pub const BlockFreeSet = struct {
    // Each bit of `index` is the OR of `shard_size` bits of `blocks`.
    index: DynamicBitSetUnmanaged,
    blocks: DynamicBitSetUnmanaged,

    // Fixing the shard size to a constant rather than varying the shard size (but
    // guaranteeing the index always a multiple of 64B) means that the top-level index
    // may have some unused bits. But the shards themselves are always a multiple of
    // the word size. In practice the tail end of the index will be accessed less
    // frequently than the head/middle anyway.
    //
    // 10TiB disk / 64KiB block size / 640B shard size = 4096B index
    const shard_size = 10 * (64 * 8); // 10 cache lines per shard

    pub fn init(allocator: *mem.Allocator, total_blocks: usize) !BlockFreeSet {
        // Round up to ensure that every block bit is covered by the index.
        const index_size = divCeil(usize, total_blocks, shard_size);
        var index = try DynamicBitSetUnmanaged.initFull(index_size, allocator);
        errdefer index.deinit(allocator);

        const blocks = try DynamicBitSetUnmanaged.initFull(total_blocks, allocator);
        return BlockFreeSet{
            .index = index,
            .blocks = blocks,
        };
    }

    pub fn deinit(set: *BlockFreeSet, allocator: *mem.Allocator) void {
        set.index.deinit(allocator);
        set.blocks.deinit(allocator);
    }

    // Marks a free block as allocated, and return the address. Panic if no blocks are available.
    // TODO consider "caching" the first set bit to speed up subsequent acquire() calls
    pub fn acquire(set: *BlockFreeSet) u64 {
        // TODO: To ensure this "unreachable" is never reached, the leader must reject
        // new requests when storage space is too low to fulfill them.
        return set.try_acquire() orelse unreachable;
    }

    fn try_acquire(set: *BlockFreeSet) ?u64 {
        const index_bit = set.index.findFirstSet() orelse return null;
        const shard_start = index_bit * shard_size;
        const shard_end = std.math.min(shard_start + shard_size, set.blocks.bit_length);
        assert(shard_start < set.blocks.bit_length);

        const bit = findFirstSetBit(set.blocks, shard_start, shard_end) orelse return null;
        assert(set.blocks.isSet(bit));
        set.blocks.unset(bit);

        // Update the index when every block in the shard is allocated.
        if (findFirstSetBit(set.blocks, shard_start, shard_end) == null) {
            set.index.unset(index_bit);
        }

        const address = bit + 1;
        return @intCast(u64, address);
    }

    fn isFree(set: *BlockFreeSet, address: u64) bool {
        return set.blocks.isSet(address - 1);
    }

    // Mark the specified block as free.
    pub fn release(set: *BlockFreeSet, address: u64) void {
        const bit = address - 1;
        assert(!set.blocks.isSet(bit));
        set.blocks.set(bit);
        set.index.set(@divTrunc(bit, shard_size));
    }

    pub fn decode(data: []const u8, allocator: *mem.Allocator) !BlockFreeSet {
        var blocks = try BitsetEncoder(.immutable).decode(data, allocator);
        errdefer blocks.deinit(allocator);

        const index_size = divCeil(usize, blocks.bit_length, shard_size);
        var index = try DynamicBitSetUnmanaged.initFull(index_size, allocator);
        errdefer index.deinit(allocator);

        var i: usize = 0;
        while (i < index_size) : (i += 1) {
            const shard_start = i * shard_size;
            const shard_end = std.math.min(shard_start + shard_size, blocks.bit_length);
            if (findFirstSetBit(blocks, shard_start, shard_end) == null) index.unset(i);
        }

        return BlockFreeSet{
            .index = index,
            .blocks = blocks,
        };
    }

    // Returns the number of bytes that the BlockFreeSet needs to encode to.
    pub fn encodeSize(set: BlockFreeSet) usize {
        return set.makeEncoder(.immutable).offsets.end;
    }

    pub fn encode(set: BlockFreeSet, dst: []u8) void {
        set.makeEncoder(.mutable).encode(dst, set.blocks);
    }

    fn makeEncoder(set: BlockFreeSet, comptime M: Mutable) BitsetEncoder(M) {
        const words = bitsetMasksLen(set.blocks);
        var runs: usize = 0;
        var literals: usize = 0;
        var i: usize = 0;
        while (i < words) {
            const word = set.blocks.masks[i];
            if (word == 0 or word == ~@as(usize, 0)) {
                runs += 1;
                i += 1 + bitsetRunLength(set.blocks, i + 1, words, word);
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

fn getBit(b: []const u8, i: usize) bool {
    return b[@divTrunc(i, 8)] & (@as(u8, 1) << @truncate(u3, i % 8)) != 0;
}

fn setBit(b: []u8, i: usize) void {
    assert(b[@divTrunc(i, 8)] & (@as(u8, 1) << @truncate(u3, i % 8)) == 0); // TODO remove this, it's just for testing!
    b[@divTrunc(i, 8)] |= @as(u8, 1) << @truncate(u3, i % 8);
}

const Mutable = enum { mutable, immutable };
// The encoding assumes that the architecture is little-endian.
fn BitsetEncoder(comptime mut: Mutable) type {
    const Bytes = if (mut == .mutable) []u8 else []const u8;
    const U32 = if (mut == .mutable) *u32 else *const u32;
    const U64s = if (mut == .mutable) []u64 else []const u64;

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
            const rozoo_offset = rol_offset + divCeil(usize, params.runs + params.literals, 64) * 8;
            const rl_offset = rozoo_offset + divCeil(usize, params.runs, 64) * 8;
            const lit_offset = rl_offset + divCeil(usize, params.runs, 8) * 8;
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

        fn parse(self: Self, buf: Bytes) struct {
            total_blocks: U32,
            run_or_literal_count: U32,
            run_or_literal: Bytes,
            run_of_zeroes_or_ones: Bytes,
            run_lengths: Bytes,
            literals: U64s,
        } {
            const offsets = &self.offsets;
            assert(buf.len == offsets.end);
            return .{
                .total_blocks = @ptrCast(U32, @alignCast(4, buf[0..4])),
                .run_or_literal_count = @ptrCast(U32, @alignCast(4, buf[4..8])),
                // `run_or_literal` and `run_of_zeroes_or_ones` slices include padding.
                .run_or_literal = buf[offsets.run_or_literal..offsets.run_of_zeroes_or_ones],
                .run_of_zeroes_or_ones = buf[offsets.run_of_zeroes_or_ones..offsets.run_lengths],
                // `run_lengths`'s slice excludes the padding.
                .run_lengths = buf[offsets.run_lengths..(offsets.run_lengths + self.runs)],
                .literals = @alignCast(8,
                    mem.bytesAsSlice(u64, buf[offsets.literals..offsets.end])),
            };
        }

        // Encoding:
        // - u32 count of runs + count of literals
        // - u32 (unused)
        // - [run_or_literal_count]u1 run_of_zeroes_or_ones (padded to word size)
        // - [number of runs]u1 run_of_zeroes_or_ones (padded to word size)
        // - [number of literals]u64 literals
        fn encode(self: Self, dst: []u8, bitset: DynamicBitSetUnmanaged) void {
            for (dst) |*b| b.* = 0;

            const slices = self.parse(dst);
            var run_or_literal_bit: usize = 0;
            var run_index: usize = 0;
            var literals_index: usize = 0;

            var word_index: usize = 0;
            const word_count = bitsetMasksLen(bitset);
            while (word_index < word_count) {
                const word = bitset.masks[word_index];
                if (word == 0 or word == ~@as(usize, 0)) {
                    const run_len = 1 + bitsetRunLength(bitset, word_index + 1, word_count, word);
                    slices.run_lengths[run_index] = @intCast(u8, run_len - 1);
                    if (word != 0) setBit(slices.run_of_zeroes_or_ones, run_index);
                    setBit(slices.run_or_literal, run_or_literal_bit);
                    run_index += 1;
                    word_index += run_len;
                } else {
                    slices.literals[literals_index] = word;
                    literals_index += 1;
                    word_index += 1;
                }
                run_or_literal_bit += 1;
            }
            slices.total_blocks.* = @intCast(u32, bitset.bit_length);
            slices.run_or_literal_count.* = @intCast(u32, self.runs + self.literals);

            assert(word_index == word_count);
            assert(slices.run_or_literal_count.* == run_or_literal_bit);
            assert(slices.run_lengths.len == run_index);
            assert(slices.literals.len == literals_index);
        }

        fn decode(data: []const u8, allocator: *mem.Allocator) !DynamicBitSetUnmanaged {
            assert(data.len % 8 == 0);
            // TODO what if the size of the decoded block set disagrees with the configured total_blocks? maybe allow it to grow but not shrink?
            const run_or_literal_count = mem.readIntSliceLittle(u32, data[4..8]);
            const run_or_literal_words = divCeil(usize, run_or_literal_count, 64);
            const run_or_literal = mem.bytesAsSlice(u64, data[8 .. 8 + 8 * run_or_literal_words]);
            var runs: usize = 0;
            for (run_or_literal) |rol| runs += @popCount(u64, rol);
            const decoder = BitsetEncoder(.immutable).init(.{
                .runs = runs,
                .literals = run_or_literal_count - runs,
            });
            const slices = decoder.parse(data);
            assert(slices.run_or_literal_count.* == run_or_literal_count);

            var bitset = try DynamicBitSetUnmanaged.initFull(slices.total_blocks.*, allocator);
            errdefer bitset.deinit(allocator);

            var run_index: usize = 0;
            var literals_index: usize = 0;
            var blocks_index: usize = 0;
            for (mem.bytesAsSlice(u64, slices.run_or_literal)) |rol_word, rol_i| {
                const last = rol_i == run_or_literal_words - 1;
                // TODO fix fast paths
                //if (!last && rol_word == ~@as(u64, 0)) { // Fast path: all runs.
                //    //var word = std.bitset.IntegerBitSet(64).initFull();
                //    var run: usize = 0;
                //    while (run < 64) : (run += 1) {
                //        const run_ones = getBit(slices.run_of_zeroes_or_ones, run_index);
                //        const run_len = slices.run_lengths[run_index] + 1;
                //        if (run_ones) {
                //            // The bitset is all 1s by default, so just skip over the run.
                //            //bit += run_len;
                //            blocks_index += run_len;
                //        } else {
                //            var w: usize = 0;
                //            while (w < run_len) : (w += 1) {
                //                set.blocks.masks[blocks_index] = 0;
                //                blocks_index += 1;
                //            }
                //            //const max = bit + run_len;
                //            //while (bit < max) : (bit += 1) set.blocks.unset(bit);
                //        }
                //        run_index += 1;
                //    }
                //} else if (!last && rol_word == 0) {
                //    // Fast path: all literals.
                //    // The bitset is filled with 1s by default, so just skip over the word.
                //    var wb: usize = 0;
                //    while (wb < 64) : (wb += 1) {
                //        set.blocks.masks[blocks_index] = slices.literals[literals_index];
                //        blocks_index += 1;
                //        //var lb: usize = 0;
                //        //while (lb < 64) : (lb += 1) {
                //        //    getBit(slices.literals[literals_index]
                //        //    set.blocks.
                //        //}
                //        literals_index += 1;
                //    }
                //} else { // Mixed runs and literals.
                    const max_bit = if (last) (run_or_literal_count - 1) % 64 + 1 else 64;
                    var bit: usize = 0;
                    while (bit < max_bit) : (bit += 1) {
                        const is_run = rol_word & (@as(u64, 1) << @truncate(u6, bit)) != 0;
                        if (is_run) {
                            const run_len = @as(usize, slices.run_lengths[run_index]) + 1;
                            if (!getBit(slices.run_of_zeroes_or_ones, run_index)) {
                                var w: usize = 0;
                                while (w < run_len) : (w += 1) bitset.masks[blocks_index + w] = 0;
                            }
                            blocks_index += run_len;
                            run_index += 1;
                        } else {
                            bitset.masks[blocks_index] = slices.literals[literals_index];
                            blocks_index += 1;
                            literals_index += 1;
                        }
                    }
                //}
            }
            assert(blocks_index == bitsetMasksLen(bitset));
            assert(literals_index == decoder.literals);
            assert(run_index == decoder.runs);
            return bitset;
        }
    };
}

fn divCeil(comptime T: type, a: T, b: T) T {
    return @divTrunc(a + b - 1, b);
}

test "divCeil" {
    try std.testing.expectEqual(divCeil(usize, 1, 8), 1);
    try std.testing.expectEqual(divCeil(usize, 8, 8), 1);
    try std.testing.expectEqual(divCeil(usize, 9, 8), 2);
}

// Returns the index of a set bit (relative to the start of the bitset) within start…end (inclusive…exclusive).
fn findFirstSetBit(bitset: DynamicBitSetUnmanaged, start: usize, end: usize) ?usize {
    const MaskInt = DynamicBitSetUnmanaged.MaskInt;
    comptime assert(@bitSizeOf(MaskInt) == 64);

    assert(end <= bitset.bit_length);
    const word_start = @divTrunc(start, @bitSizeOf(MaskInt));
    const word_offset = @mod(start, @bitSizeOf(MaskInt));
    const word_end = divCeil(usize, end, @bitSizeOf(MaskInt));
    assert(word_start < word_end);

    // Only iterate over the subset of bits that were requested.
    var iter = bitset.iterator(.{});
    iter.words_remain = bitset.masks[word_start+1..word_end];
    const mask = ~@as(MaskInt, 0);
    iter.bits_remain = bitset.masks[word_start] & std.math.shl(MaskInt, mask, word_offset);

    const b = start - word_offset + (iter.next() orelse return null);
    return if (b < end) b else null;
}

fn bitsetMasksLen(bitset: DynamicBitSetUnmanaged) usize {
    return divCeil(usize, bitset.bit_length, 64);
}

fn bitsetRunLength(bitset: DynamicBitSetUnmanaged, offset: usize, len: usize, word: u64) usize {
    const max = std.math.min(len, offset + 255);
    var end: usize = offset;
    while (end < max and bitset.masks[end] == word) : (end += 1) {}
    return end - offset;
}

test "findFirstSetBit" {
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
                try std.testing.expectEqual(expect, findFirstSetBit(set, s, s + window));
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
        try std.testing.expectEqual(@as(?usize, 2), findFirstSetBit(set, 1, 9));
    }

    {
        // Don't return a bit outside of the bitset's interval, even with initFull.
        var set = try BitSet.initFull(56, std.testing.allocator);
        defer set.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(?usize, null), findFirstSetBit(set, 56, 56));
    }
}

test "BlockFreeSet" {
    const block_bytes = 64 * 1024;
    const blocks_in_tb = (1 << 40) / block_bytes;
    try testBlockIndexSize(4096 * 8, 10 * blocks_in_tb);
    try testBlockIndexSize(1, 1); // At least one index bit is required.
    // Block counts are not necessarily a multiple of the word size.
    try testBlockFreeSet(64 * 64);
    var i: usize = 1;
    while (i < 128) : (i += 1) try testBlockFreeSet(64 * 8 + i);
}

fn testBlockFreeSet(total_blocks: usize) !void {
    const expectEqual = std.testing.expectEqual;
    // Acquire everything, then release, then acquire again.
    var set = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer set.deinit(std.testing.allocator);

    var empty = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer empty.deinit(std.testing.allocator);

    var i: usize = 0;
    while (i < total_blocks) : (i += 1) try expectEqual(@as(?u64, i + 1), set.try_acquire());
    try expectEqual(@as(?u64, null), set.try_acquire());

    i = 0;
    while (i < total_blocks) : (i += 1) set.release(@as(u64, i + 1));
    try expectBlockFreeSetEqual(empty, set);

    i = 0;
    while (i < total_blocks) : (i += 1) try expectEqual(@as(?u64, i + 1), set.try_acquire());
    try expectEqual(@as(?u64, null), set.try_acquire());
}

fn testBlockIndexSize(expect_index_size: usize, total_blocks: usize) !void {
    var set = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer set.deinit(std.testing.allocator);

    try std.testing.expectEqual(expect_index_size, set.index.bit_length);
}

test "BlockFreeSet encode/decode" {
    try testBlockFreeSetEncode(64 * 64, 0); // fully free
    try testBlockFreeSetEncode(64 * 64, 64 * 64); // fully allocated

    const big = 64 * 64 * 64;
    try testBlockFreeSetEncode(big, 0);
    try testBlockFreeSetEncode(big, big);
    try testBlockFreeSetEncode(big, big / 16); // mostly free
    try testBlockFreeSetEncode(big, big / 16 * 15); // mostly full
    // Bitsets that aren't a multiple of the word size.
    try testBlockFreeSetEncode(big + 1, big / 16);
    try testBlockFreeSetEncode(big - 1, big / 16);
}

fn testBlockFreeSetEncode(total_blocks: usize, unset_bits: usize) !void {
    assert(unset_bits <= total_blocks);
    var seed: u64 = undefined;
    try std.os.getrandom(mem.asBytes(&seed));
    var prng = std.rand.DefaultPrng.init(seed);

    var set = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer set.deinit(std.testing.allocator);

    // To set up a BlockFreeSet for testing, first allocate everything, then free selected blocks.
    // This ensures that the index and block bitsets are synced.
    if (unset_bits < total_blocks) {
        var i: usize = 0;
        while (i < total_blocks) : (i += 1) {
            try std.testing.expectEqual(@as(?u64, i + 1), set.try_acquire());
        }

        var j: usize = 0;
        while (j < unset_bits) : (j += 1) {
            const block = prng.random.uintLessThan(usize, total_blocks) + 1;
            if (!set.isFree(block)) set.release(block);
        }
    }

    var buf = try std.testing.allocator.alloc(u8, set.encodeSize());
    defer std.testing.allocator.free(buf);

    set.encode(buf);
    var set2 = try BlockFreeSet.decode(buf, std.testing.allocator);
    defer set2.deinit(std.testing.allocator);

    try expectBlockFreeSetEqual(set, set2);
}

fn expectBlockFreeSetEqual(a: BlockFreeSet, b: BlockFreeSet) !void {
    try expectBitsetEqual(a.blocks, b.blocks);
    try expectBitsetEqual(a.index, b.index);
}

fn expectBitsetEqual(a: DynamicBitSetUnmanaged, b: DynamicBitSetUnmanaged) !void {
    try std.testing.expectEqual(a.bit_length, b.bit_length);
    var i: usize = 0;
    while (i < bitsetMasksLen(a)) : (i += 1) try std.testing.expectEqual(a.masks[i], b.masks[i]);
}

test "BitsetEncoder" {
    const literals = 2;
    const runs = 2;
    const run_lengths = [_]usize{5, 3};
    const total_blocks = 64 * (literals + run_lengths[0] + run_lengths[1]);
    const encoded = mem.sliceAsBytes(&[_]u64{
        total_blocks |
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

    const decoded = [_]u64{
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

    var got_decoded = try BitsetEncoder(.immutable).decode(encoded, std.testing.allocator);
    defer got_decoded.deinit(std.testing.allocator);

    try std.testing.expectEqual(total_blocks, got_decoded.bit_length);
    try std.testing.expectEqual(decoded.len, bitsetMasksLen(got_decoded));
    try std.testing.expectEqualSlices(u64, &decoded, got_decoded.masks[0..decoded.len]);
}
