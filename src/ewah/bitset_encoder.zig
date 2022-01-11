const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

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
/// When decoding a marker, the run precedes the literals.
///
/// This encoding requires that the architecture is little-endian with 64-bit words.
pub fn BitSetEncoder(comptime Word: type) type {
    const word_bits = @bitSizeOf(Word);

    const marker_run_length_max = (1 << (word_bits / 2)) - 1;
    const marker_literals_max = (1 << (word_bits / 2 - 1)) - 1;

    const MarkerRunLength = std.math.IntFittingRange(0, marker_run_length_max); // Word=usize → u32
    const MarkerLiterals = std.math.IntFittingRange(0, marker_literals_max); // Word=usize → u31

    const Marker = packed struct {
        run_bit: u1, // run of 0s or 1s
        literals: MarkerLiterals, // number of literal words immediately following marker
        run_length: MarkerRunLength, // length of run (in words)
    };

    comptime {
        assert(std.Target.current.cpu.arch.endian() == std.builtin.Endian.Little);
        assert(@typeInfo(Word).Int.signedness == .unsigned);
        assert(word_bits % 8 == 0); // byte multiple, so bytes can be cast to words
        assert(@bitSizeOf(Marker) == word_bits);

        assert(@bitSizeOf(MarkerRunLength) % 2 == 0);
        assert(std.math.maxInt(MarkerRunLength) == marker_run_length_max);

        assert(@bitSizeOf(MarkerLiterals) % 2 == 1);
        assert(std.math.maxInt(MarkerLiterals) == marker_literals_max);
    }

    return struct {
        const Self = @This();

        inline fn marker(mark: Marker) Word {
            return @bitCast(Word, mark);
        }

        /// Decodes the compressed bitset in `source` into `set`. Panics if `source`'s encoding is invalid.
        /// Returns the number of *words* written to `target`.
        pub fn decode(source: []const u8, target: []Word) usize {
            const source_words = @alignCast(@alignOf(Word), mem.bytesAsSlice(Word, source));
            var source_index: usize = 0;
            var target_index: usize = 0;
            while (source_index < source_words.len) {
                const source_marker = @bitCast(Marker, source_words[source_index]);
                const run_length: usize = source_marker.run_length;
                const run_word: Word = if (source_marker.run_bit == 1) ~@as(Word, 0) else 0;
                const literals_count: usize = source_marker.literals;
                std.mem.set(Word, target[target_index..][0..run_length], run_word);
                std.mem.copy(Word, target[target_index + run_length..][0..literals_count],
                    source_words[source_index + 1..][0..literals_count]);
                target_index += run_length + literals_count;
                source_index += 1 + literals_count;
            }
            assert(target_index <= target.len);
            assert(source_index == source_words.len);
            return target_index;
        }

        // Returns the maximum number of bytes that `source` needs to encode to.
        pub fn encode_size_max(source: []const Word) usize {
            // Assume (pessimistically) that every word will be encoded as a literal.
            const literals_count = source.len;
            const markers = div_ceil(literals_count, marker_literals_max);
            return (literals_count + markers) * @sizeOf(Word);
        }

        // Returns the number of bytes written to `target`.
        pub fn encode(source: []const Word, target: []u8) usize {
            assert(target.len == encode_size_max(source));

            const target_words = @alignCast(@alignOf(Word), mem.bytesAsSlice(Word, target));
            std.mem.set(Word, target_words, 0);
            var target_index: usize = 0;
            var source_index: usize = 0;
            while (source_index < source.len) {
                const word_next = source[source_index];
                const run_length = rl: {
                    if (is_literal(word_next)) break :rl 0;
                    // Measure run length.
                    const run_max = std.math.min(source.len, source_index + marker_run_length_max);
                    var run_end: usize = source_index + 1;
                    while (run_end < run_max and source[run_end] == word_next) : (run_end += 1) {}
                    const run = run_end - source_index;
                    source_index += run;
                    break :rl run;
                };
                // For consistent encoding, set the run bit to 0 when there is no run.
                const run_bit = if (run_length == 0) 0 else word_next & 1;
                // Count sequential literals that immediately follow the run.
                const literals_max = std.math.min(source.len - source_index, marker_literals_max);
                const literal_count = for (source[source_index..][0..literals_max]) |word, i| {
                    if (!is_literal(word)) break i;
                } else literals_max;

                target_words[target_index] = marker(.{
                    .run_bit = @intCast(u1, run_bit),
                    .run_length = @intCast(MarkerRunLength, run_length),
                    .literals = @intCast(MarkerLiterals, literal_count),
                });
                std.mem.copy(Word, target_words[target_index + 1..][0..literal_count],
                    source[source_index..][0..literal_count]);
                target_index += 1 + literal_count; // +1 for the marker word
                source_index += literal_count;
            }
            assert(source_index == source.len);

            return target_index * @sizeOf(Word);
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

test "BitSetEncoder Word=u8" {
    try test_decode_with_word(u8);

    const Encoder = BitSetEncoder(u8);
    const maxInt = std.math.maxInt;
    var run_length: usize = 0;
    while (run_length <= maxInt(u4)) : (run_length += 1) {
        try test_decode(u8, &.{
            Encoder.marker(.{
                .run_bit = 0,
                .run_length = @intCast(u4, run_length),
                .literals = 3,
            }),
            12, 34, 56,
        });
    }
}

test "BitSetEncoder Word=u16" {
    try test_decode_with_word(u16);
}

fn test_decode_with_word(comptime Word: type) !void {
    const Encoder = BitSetEncoder(Word);
    const maxInt = std.math.maxInt;
    const max_run = maxInt(Word) >> (@bitSizeOf(Word) / 2);
    const max_literals = maxInt(Word) >> (@bitSizeOf(Word) / 2 + 1);

    // Alternating runs, no literals.
    try test_decode(Word, &.{
        Encoder.marker(.{ .run_bit = 0, .run_length = 2, .literals = 0 }),
        Encoder.marker(.{ .run_bit = 1, .run_length = 3, .literals = 0 }),
        Encoder.marker(.{ .run_bit = 0, .run_length = 4, .literals = 0 }),
    });
    // Alternating runs, with literals.
    try test_decode(Word, &.{
        Encoder.marker(.{ .run_bit = 0, .run_length = 2, .literals = 1 }), 12,
        Encoder.marker(.{ .run_bit = 1, .run_length = 3, .literals = 1 }), 34,
        Encoder.marker(.{ .run_bit = 0, .run_length = 4, .literals = 1 }), 56,
    });
    // Consecutive run marker overflow.
    try test_decode(Word, &.{
        Encoder.marker(.{ .run_bit = 0, .run_length = max_run, .literals = 0 }),
        Encoder.marker(.{ .run_bit = 0, .run_length = 2, .literals = 0 }),
    });

    var encoding = std.ArrayList(Word).init(std.testing.allocator);
    defer encoding.deinit();

    {
        // Consecutive literal marker overflow.
        try encoding.append(Encoder.marker(.{ .run_bit = 0, .run_length = 0, .literals = max_literals }));
        var i: Word = 0;
        while (i < max_literals) : (i += 1) try encoding.append(i + 1);
        try encoding.append(Encoder.marker(.{ .run_bit = 0, .run_length = 0, .literals = 2 }));
        try encoding.append(i + 2);
        try encoding.append(i + 3);
        try test_decode(Word, encoding.items);
        encoding.items.len = 0;
    }
}

fn test_decode(comptime Word: type, encoded_expect_words: []Word) !void {
    const encoded_expect = mem.sliceAsBytes(encoded_expect_words);
    const Encoder = BitSetEncoder(Word);
    const decoded_expect_data = try std.testing.allocator.alloc(Word, 4 * std.math.maxInt(Word));
    defer std.testing.allocator.free(decoded_expect_data);

    const decoded_expect_length = Encoder.decode(encoded_expect, decoded_expect_data);
    const decoded_expect = decoded_expect_data[0..decoded_expect_length];
    const encoded_actual = try std.testing.allocator.alloc(u8,
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
