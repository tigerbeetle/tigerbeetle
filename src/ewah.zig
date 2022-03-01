const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const div_ceil = @import("utils.zig").div_ceil;

/// Encode or decode a bitset using Daniel Lemire's EWAH codec.
/// ("Histogram-Aware Sorting for Enhanced Word-Aligned Compression in Bitmap Indexes")
///
/// EWAH uses only two types of words, where the first type is a 64-bit verbatim ("literal") word.
/// The second type of word is a marker word:
/// * The first bit indicates which uniform word will follow.
/// * The next 31 bits are used to store the number of uniform words.
/// * The last 32 bits are used to store the number of literal words following the uniform words.
/// EWAH bitmaps begin with a marker word. A 'marker' looks like (assuming a 64-bit word):
///
///     [uniform_bit:u1][uniform_word_count:u31(LE)][literal_word_count:u32(LE)]
///
/// and is immediately followed by `literal_word_count` 64-bit literals.
/// When decoding a marker, the uniform words precede the literal words.
///
/// This encoding requires that the architecture is little-endian with 64-bit words.
pub fn ewah(comptime Word: type) type {
    const word_bits = @bitSizeOf(Word);

    return struct {
        const Self = @This();

        const marker_uniform_word_count_max = (1 << ((word_bits / 2) - 1)) - 1;
        const marker_literal_word_count_max = (1 << (word_bits / 2)) - 1;

        pub const MarkerUniformCount = math.IntFittingRange(0, marker_uniform_word_count_max); // Word=usize → u31
        pub const MarkerLiteralCount = math.IntFittingRange(0, marker_literal_word_count_max); // Word=usize → u32

        const Marker = packed struct {
            uniform_bit: u1, // Whether the uniform word is all 0s or all 1s.
            uniform_word_count: MarkerUniformCount, // 31-bit number of uniform words following the marker.
            literal_word_count: MarkerLiteralCount, // 32-bit number of literal words following the uniform words.
        };

        comptime {
            assert(@import("builtin").target.cpu.arch.endian() == std.builtin.Endian.Little);
            assert(@typeInfo(Word).Int.signedness == .unsigned);
            assert(word_bits % 8 == 0); // A multiple of a byte, so that words can be cast to bytes.
            assert(@bitSizeOf(Marker) == word_bits);
            assert(@sizeOf(Marker) == @sizeOf(Word));

            assert(@bitSizeOf(MarkerUniformCount) % 2 == 1);
            assert(math.maxInt(MarkerUniformCount) == marker_uniform_word_count_max);

            assert(@bitSizeOf(MarkerLiteralCount) % 2 == 0);
            assert(math.maxInt(MarkerLiteralCount) == marker_literal_word_count_max);
        }

        inline fn marker_word(mark: Marker) Word {
            return @bitCast(Word, mark);
        }

        /// Decodes the compressed bitset in `source` into `target_words`.
        /// Returns the number of *words* written to `target_words`.
        pub fn decode(source: []align(@alignOf(Word)) const u8, target_words: []Word) usize {
            assert(source.len % @sizeOf(Word) == 0);
            assert(source.len >= @sizeOf(Marker));
            assert(is_disjoint(u8, Word, source, target_words));

            const source_words = mem.bytesAsSlice(Word, source);
            var source_index: usize = 0;
            var target_index: usize = 0;
            while (source_index < source_words.len) {
                const marker = @ptrCast(*const Marker, &source_words[source_index]);
                source_index += 1;
                std.mem.set(
                    Word,
                    target_words[target_index..][0..marker.uniform_word_count],
                    if (marker.uniform_bit == 1) ~@as(Word, 0) else 0,
                );
                target_index += marker.uniform_word_count;
                std.mem.copy(
                    Word,
                    target_words[target_index..][0..marker.literal_word_count],
                    source_words[source_index..][0..marker.literal_word_count],
                );
                source_index += marker.literal_word_count;
                target_index += marker.literal_word_count;
            }
            assert(source_index == source_words.len);
            assert(target_index <= target_words.len);
            return target_index;
        }

        // Returns the number of bytes written to `target`.
        pub fn encode(source_words: []const Word, target: []align(@alignOf(Word)) u8) usize {
            assert(target.len >= @sizeOf(Marker));
            assert(target.len == encode_size_max(source_words.len));
            assert(is_disjoint(Word, u8, source_words, target));

            const target_words = mem.bytesAsSlice(Word, target);
            std.mem.set(Word, target_words, 0);

            var target_index: usize = 0;
            var source_index: usize = 0;
            while (source_index < source_words.len) {
                const word = source_words[source_index];

                const uniform_word_count = count: {
                    if (is_literal(word)) break :count 0;
                    // Measure run length.
                    const uniform_max = math.min(source_words.len - source_index, marker_uniform_word_count_max);
                    var uniform: usize = 1;
                    while (uniform < uniform_max and source_words[source_index + uniform] == word) uniform += 1;
                    break :count uniform;
                };
                source_index += uniform_word_count;
                // For consistent encoding, set the run/uniform bit to 0 when there is no run.
                const uniform_bit = if (uniform_word_count == 0) 0 else @intCast(u1, word & 1);

                // Count sequential literals that immediately follow the run.
                const literals_max = math.min(source_words.len - source_index, marker_literal_word_count_max);
                const literal_word_count = for (source_words[source_index..][0..literals_max]) |w, i| {
                    if (!is_literal(w)) break i;
                } else literals_max;

                target_words[target_index] = marker_word(.{
                    .uniform_bit = uniform_bit,
                    .uniform_word_count = @intCast(MarkerUniformCount, uniform_word_count),
                    .literal_word_count = @intCast(MarkerLiteralCount, literal_word_count),
                });
                target_index += 1;
                std.mem.copy(
                    Word,
                    target_words[target_index..][0..literal_word_count],
                    source_words[source_index..][0..literal_word_count],
                );
                source_index += literal_word_count;
                target_index += literal_word_count;
            }
            assert(source_index == source_words.len);

            return target_index * @sizeOf(Word);
        }

        /// Returns the maximum number of bytes required to encode `word_count` words.
        /// Assumes (pessimistically) that every word will be encoded as a literal.
        pub fn encode_size_max(word_count: usize) usize {
            const marker_count = div_ceil(word_count, marker_literal_word_count_max);
            assert(marker_count != 0);

            return marker_count * @sizeOf(Marker) + word_count * @sizeOf(Word);
        }

        inline fn is_literal(word: Word) bool {
            return word != 0 and word != ~@as(Word, 0);
        }
    };
}

fn is_disjoint(comptime A: type, comptime B: type, a: []const A, b: []const B) bool {
    return @ptrToInt(a.ptr) + a.len * @sizeOf(A) <= @ptrToInt(b.ptr) or
        @ptrToInt(b.ptr) + b.len * @sizeOf(B) <= @ptrToInt(a.ptr);
}

test "is_disjoint" {
    const a = try std.testing.allocator.alignedAlloc(u8, @sizeOf(u32), 8 * @sizeOf(u32));
    defer std.testing.allocator.free(a);

    const b = try std.testing.allocator.alloc(u32, 8);
    defer std.testing.allocator.free(b);

    try std.testing.expectEqual(true, is_disjoint(u8, u32, a, b));
    try std.testing.expectEqual(true, is_disjoint(u32, u8, b, a));

    try std.testing.expectEqual(true, is_disjoint(u8, u8, a, a[0..0]));
    try std.testing.expectEqual(true, is_disjoint(u32, u32, b, b[0..0]));

    try std.testing.expectEqual(false, is_disjoint(u8, u8, a, a[0..1]));
    try std.testing.expectEqual(false, is_disjoint(u8, u8, a, a[a.len - 1 .. a.len]));

    try std.testing.expectEqual(false, is_disjoint(u32, u32, b, b[0..1]));
    try std.testing.expectEqual(false, is_disjoint(u32, u32, b, b[b.len - 1 .. b.len]));

    try std.testing.expectEqual(false, is_disjoint(u8, u32, a, std.mem.bytesAsSlice(u32, a)));
    try std.testing.expectEqual(false, is_disjoint(u32, u8, b, std.mem.sliceAsBytes(b)));
}

test "ewah Word=u8 decode→encode→decode" {
    try test_decode_with_word(u8);

    const codec = ewah(u8);
    var uniform_word_count: usize = 0;
    while (uniform_word_count <= math.maxInt(codec.MarkerUniformCount)) : (uniform_word_count += 1) {
        try test_decode(u8, &.{
            codec.marker_word(.{
                .uniform_bit = 0,
                .uniform_word_count = @intCast(codec.MarkerUniformCount, uniform_word_count),
                .literal_word_count = 3,
            }),
            12,
            34,
            56,
        });
    }
}

test "ewah Word=u8 encode→decode→encode" {
    const codec = ewah(u8);

    var seed: u64 = undefined;
    try std.os.getrandom(mem.asBytes(&seed));

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    var decoded_expect: [4096]u8 = undefined;
    var decoded_actual: [4096]u8 = undefined;

    const encoded_actual = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(u8),
        codec.encode_size_max(decoded_expect.len),
    );
    defer std.testing.allocator.free(encoded_actual);

    var t: usize = 0;
    while (t < 100) : (t += 1) {
        random.bytes(decoded_expect[0..]);
        _ = codec.encode(decoded_expect[0..], encoded_actual);
        const decoded_actual_length = codec.decode(encoded_actual[0..], decoded_actual[0..]);
        try std.testing.expectEqual(decoded_expect.len, decoded_actual_length);
        try std.testing.expectEqual(decoded_expect, decoded_actual);
    }
}

test "ewah Word=u16" {
    try test_decode_with_word(u16);
}

// decode → encode → decode
fn test_decode_with_word(comptime Word: type) !void {
    const codec = ewah(Word);

    // Alternating runs, no literals.
    try test_decode(Word, &.{
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 0 }),
        codec.marker_word(.{ .uniform_bit = 1, .uniform_word_count = 3, .literal_word_count = 0 }),
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 4, .literal_word_count = 0 }),
    });
    // Alternating runs, with literals.
    try test_decode(Word, &.{
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 1 }),
        12,
        codec.marker_word(.{ .uniform_bit = 1, .uniform_word_count = 3, .literal_word_count = 1 }),
        34,
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 4, .literal_word_count = 1 }),
        56,
    });
    // Consecutive run marker overflow.
    try test_decode(Word, &.{
        codec.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = math.maxInt(codec.MarkerUniformCount),
            .literal_word_count = 0,
        }),
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 0 }),
    });

    var encoding = std.ArrayList(Word).init(std.testing.allocator);
    defer encoding.deinit();

    {
        // Consecutive literal marker overflow.
        try encoding.append(codec.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = 0,
            .literal_word_count = math.maxInt(codec.MarkerLiteralCount),
        }));
        var i: Word = 0;
        while (i < math.maxInt(codec.MarkerLiteralCount)) : (i += 1) try encoding.append(i + 1);
        try encoding.append(codec.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = 0,
            .literal_word_count = 2,
        }));
        try encoding.append(i + 2);
        try encoding.append(i + 3);
        try test_decode(Word, encoding.items);
        encoding.items.len = 0;
    }
}

fn test_decode(comptime Word: type, encoded_expect_words: []Word) !void {
    const encoded_expect = mem.sliceAsBytes(encoded_expect_words);
    const codec = ewah(Word);
    const decoded_expect_data = try std.testing.allocator.alloc(Word, 4 * math.maxInt(Word));
    defer std.testing.allocator.free(decoded_expect_data);

    const decoded_expect_length = codec.decode(encoded_expect, decoded_expect_data);
    const decoded_expect = decoded_expect_data[0..decoded_expect_length];
    const encoded_actual = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(Word),
        codec.encode_size_max(decoded_expect.len),
    );
    defer std.testing.allocator.free(encoded_actual);

    const encoded_actual_length = codec.encode(decoded_expect, encoded_actual);
    try std.testing.expectEqual(encoded_expect.len, encoded_actual_length);
    try std.testing.expectEqualSlices(u8, encoded_expect, encoded_actual[0..encoded_actual_length]);

    const encoded_size_max = codec.encode_size_max(decoded_expect.len);
    try std.testing.expect(encoded_expect.len <= encoded_size_max);

    const decoded_actual = try std.testing.allocator.alloc(Word, decoded_expect.len);
    defer std.testing.allocator.free(decoded_actual);

    const decoded_actual_length = codec.decode(encoded_actual, decoded_actual);
    try std.testing.expectEqual(decoded_expect.len, decoded_actual_length);
    try std.testing.expectEqualSlices(Word, decoded_expect, decoded_actual);
}
