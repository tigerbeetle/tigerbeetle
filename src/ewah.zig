const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

/// Encode or decode a bitset using Daniel Lemire's EWAH codec.
/// ("Histogram-Aware Sorting for Enhanced Word-Aligned Compression in Bitmap Indexes")
///
/// EWAH uses only two types of words, where the first type is a 64-bit verbatim ("literal") word.
/// The second type of word is a marker word:
/// * The first bit indicates which uniform word will follow.
/// * The next 31 bits are used to store the number of uniform words.
/// * The last 32 bits are used to store the number of literal words following the uniform words.
/// EWAH bitmaps begin with a marker word. A 'marker' looks like:
///
///     [uniform_bit:u1][uniform_word_count:u31(LE)][literal_word_count:u32(LE)]
///
/// and is immediately followed by `literal_word_count` 64-bit literals.
/// When decoding a marker, the uniform words precede the literal words.
///
/// This encoding requires that the architecture is little-endian with 64-bit words.
pub fn EWAH(comptime Word: type) type {
    const word_bits = @bitSizeOf(Word);

    const marker_uniform_word_count_max = (1 << ((word_bits / 2) - 1)) - 1;
    const marker_literal_word_count_max = (1 << (word_bits / 2)) - 1;

    const MarkerUniformCount = std.math.IntFittingRange(0, marker_uniform_word_count_max); // Word=usize → u31
    const MarkerLiteralCount = std.math.IntFittingRange(0, marker_literal_word_count_max); // Word=usize → u32

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
        assert(std.math.maxInt(MarkerUniformCount) == marker_uniform_word_count_max);

        assert(@bitSizeOf(MarkerLiteralCount) % 2 == 0);
        assert(std.math.maxInt(MarkerLiteralCount) == marker_literal_word_count_max);
    }

    return struct {
        const Self = @This();

        inline fn marker_word(mark: Marker) Word {
            return @bitCast(Word, mark);
        }

        /// Decodes the compressed bitset in `source` into `set`. Panics if `source`'s encoding is invalid.
        /// Returns the number of *words* written to `target_words`.
        pub fn decode(source: []align(@alignOf(Word)) const u8, target_words: []Word) usize {
            assert(source.len % @sizeOf(Word) == 0);
            const source_words = mem.bytesAsSlice(Word, source);
            var source_index: usize = 0;
            var target_index: usize = 0;
            while (source_index < source_words.len) {
                const marker = @bitCast(Marker, source_words[source_index]);
                const uniform_word_count: usize = marker.uniform_word_count;
                const uniform_word: Word = if (marker.uniform_bit == 1) ~@as(Word, 0) else 0;
                const literal_word_count: usize = marker.literal_word_count;
                std.mem.set(Word, target_words[target_index..][0..uniform_word_count], uniform_word);
                std.mem.copy(Word, target_words[target_index + uniform_word_count..][0..literal_word_count],
                    source_words[source_index + 1..][0..literal_word_count]);
                target_index += uniform_word_count + literal_word_count;
                source_index += 1 + literal_word_count;
            }
            assert(source_index == source_words.len);
            assert(target_index <= target_words.len);
            return target_index;
        }

        // Returns the number of bytes written to `target`.
        pub fn encode(source_words: []const Word, target: []align(@alignOf(Word)) u8) usize {
            assert(target.len >= @sizeOf(Marker));
            assert(target.len == encode_size_max(source_words));

            const target_words = mem.bytesAsSlice(Word, target);
            std.mem.set(Word, target_words, 0);

            var target_index: usize = 0;
            var source_index: usize = 0;
            while (source_index < source_words.len) {
                const word = source_words[source_index];
                const uniform_word_count = count: {
                    if (is_literal(word)) break :count 0;
                    // Measure run length.
                    const uniform_max = std.math.min(source_words.len - source_index, marker_uniform_word_count_max);
                    var uniform: usize = 1;
                    while (uniform < uniform_max and source_words[source_index + uniform] == word) uniform += 1;
                    break :count uniform;
                };
                source_index += uniform_word_count;
                // For consistent encoding, set the run bit to 0 when there is no run.
                const uniform_bit = if (uniform_word_count == 0) 0 else @intCast(u1, word & 1);
                // Count sequential literals that immediately follow the run.
                const literals_max = std.math.min(source_words.len - source_index, marker_literal_word_count_max);
                const literal_word_count = for (source_words[source_index..][0..literals_max]) |w, i| {
                    if (!is_literal(w)) break i;
                } else literals_max;

                target_words[target_index] = marker_word(.{
                    .uniform_bit = uniform_bit,
                    .uniform_word_count = @intCast(MarkerUniformCount, uniform_word_count),
                    .literal_word_count = @intCast(MarkerLiteralCount, literal_word_count),
                });
                std.mem.copy(Word, target_words[target_index + 1..][0..literal_word_count],
                    source_words[source_index..][0..literal_word_count]);
                target_index += 1 + literal_word_count; // +1 for the marker word
                source_index += literal_word_count;
            }
            assert(source_index == source_words.len);

            return target_index * @sizeOf(Word);
        }

        // Returns the maximum number of bytes that `source` needs to encode to.
        pub fn encode_size_max(source: []const Word) usize {
            // Assume (pessimistically) that every word will be encoded as a literal.
            const literal_word_count = source.len;
            const marker_count = div_ceil(literal_word_count, marker_literal_word_count_max);
            assert(marker_count != 0);
            return marker_count * @sizeOf(Marker) + literal_word_count * @sizeOf(Word);
        }

        inline fn is_literal(word: Word) bool {
            return word != 0 and word != ~@as(Word, 0);
        }
    };
}

inline fn div_ceil(a: anytype, b: @TypeOf(a)) @TypeOf(a) {
    return (a + b - 1) / b;
}

test "div_ceil" {
    try std.testing.expectEqual(div_ceil(1, 8), 1);
    try std.testing.expectEqual(div_ceil(8, 8), 1);
    try std.testing.expectEqual(div_ceil(9, 8), 2);
}

test "EWAH Word=u8" {
    try test_decode_with_word(u8);

    const Encoder = EWAH(u8);
    const maxInt = std.math.maxInt;
    var uniform_word_count: usize = 0;
    while (uniform_word_count <= maxInt(u3)) : (uniform_word_count += 1) {
        try test_decode(u8, &.{
            Encoder.marker_word(.{
                .uniform_bit = 0,
                .uniform_word_count = @intCast(u3, uniform_word_count),
                .literal_word_count = 3,
            }),
            12, 34, 56,
        });
    }
}

test "EWAH Word=u16" {
    try test_decode_with_word(u16);
}

fn test_decode_with_word(comptime Word: type) !void {
    const Encoder = EWAH(Word);
    const maxInt = std.math.maxInt;
    const max_uniform_words = maxInt(Word) >> ((@bitSizeOf(Word) / 2) + 1);
    const max_literal_words = maxInt(Word) >> (@bitSizeOf(Word) / 2);

    // Alternating runs, no literals.
    try test_decode(Word, &.{
        Encoder.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 0 }),
        Encoder.marker_word(.{ .uniform_bit = 1, .uniform_word_count = 3, .literal_word_count = 0 }),
        Encoder.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 4, .literal_word_count = 0 }),
    });
    // Alternating runs, with literals.
    try test_decode(Word, &.{
        Encoder.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 1 }),
        12,
        Encoder.marker_word(.{ .uniform_bit = 1, .uniform_word_count = 3, .literal_word_count = 1 }),
        34,
        Encoder.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 4, .literal_word_count = 1 }),
        56,
    });
    // Consecutive run marker overflow.
    try test_decode(Word, &.{
        Encoder.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = max_uniform_words,
            .literal_word_count = 0,
        }),
        Encoder.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 0 }),
    });

    var encoding = std.ArrayList(Word).init(std.testing.allocator);
    defer encoding.deinit();

    {
        // Consecutive literal marker overflow.
        try encoding.append(Encoder.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = 0,
            .literal_word_count = max_literal_words,
        }));
        var i: Word = 0;
        while (i < max_literal_words) : (i += 1) try encoding.append(i + 1);
        try encoding.append(Encoder.marker_word(.{
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
    const Encoder = EWAH(Word);
    const decoded_expect_data = try std.testing.allocator.alloc(Word, 4 * std.math.maxInt(Word));
    defer std.testing.allocator.free(decoded_expect_data);

    const decoded_expect_length = Encoder.decode(encoded_expect, decoded_expect_data);
    const decoded_expect = decoded_expect_data[0..decoded_expect_length];
    const encoded_actual = try std.testing.allocator.alignedAlloc(u8, @alignOf(Word),
        Encoder.encode_size_max(decoded_expect));
    defer std.testing.allocator.free(encoded_actual);

    const encoded_actual_length = Encoder.encode(decoded_expect, encoded_actual);
    try std.testing.expectEqual(encoded_expect.len, encoded_actual_length);
    try std.testing.expectEqualSlices(u8, encoded_expect, encoded_actual[0..encoded_actual_length]);

    const encoded_size_max = Encoder.encode_size_max(decoded_expect);
    try std.testing.expect(encoded_expect.len <= encoded_size_max);

    const decoded_actual = try std.testing.allocator.alloc(Word, decoded_expect.len);
    defer std.testing.allocator.free(decoded_actual);

    const decoded_actual_length = Encoder.decode(encoded_actual, decoded_actual);
    try std.testing.expectEqual(decoded_expect.len, decoded_actual_length);
    try std.testing.expectEqualSlices(Word, decoded_expect, decoded_actual);
}
