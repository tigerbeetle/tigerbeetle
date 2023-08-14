const std = @import("std");
const assert = std.debug.assert;
const ewah = @import("ewah.zig").ewah(usize);

const BitSetConfig = struct {
    words: usize,
    run_length_e: usize,
    literals_length_e: usize,
};

const samples = 100;
const repeats: usize = 100_000;

// Explanation of fields:
// - "n": Number of randomly generate bitsets to test.
// - "words": The length of the decoded bitset, in u64s.
// - "run_length_e": The expected length of a run, ignoring truncation due to reaching the end of
//   the bitset.
// - "literals_length_e": Expected length of a sequence of literals.
const configs = [_]BitSetConfig{
    // primarily runs
    .{ .words = 640, .run_length_e = 10, .literals_length_e = 10 },
    .{ .words = 640, .run_length_e = 100, .literals_length_e = 10 },
    .{ .words = 640, .run_length_e = 200, .literals_length_e = 10 },
    // primarily literals
    .{ .words = 640, .run_length_e = 1, .literals_length_e = 100 },
};

var prng = std.rand.DefaultPrng.init(42);

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    for (configs) |config| {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();

        const allocator = arena.allocator();
        var i: usize = 0;
        var bitsets: [samples][]usize = undefined;
        var bitsets_encoded: [samples][]align(@alignOf(usize)) u8 = undefined;
        var bitsets_decoded: [samples][]usize = undefined;
        var bitset_lengths: [samples]usize = undefined;
        while (i < samples) : (i += 1) {
            bitsets[i] = try make_bitset(allocator, config);
            bitsets_encoded[i] = try allocator.alignedAlloc(u8, @alignOf(usize), ewah.encode_size_max(bitsets[0].len));
            bitsets_decoded[i] = try allocator.alloc(usize, config.words);
        }

        // Benchmark encoding.
        var encode_timer = try std.time.Timer.start();
        i = 0;
        while (i < samples) : (i += 1) {
            var j: usize = 0;
            var size: usize = undefined;
            while (j < repeats) : (j += 1) {
                size = ewah.encode(bitsets[i], bitsets_encoded[i]);
            }
            bitset_lengths[i] = size;
        }
        const encode_time = encode_timer.read() / samples / repeats;

        var decode_timer = try std.time.Timer.start();
        // Benchmark decoding.
        i = 0;
        while (i < samples) : (i += 1) {
            const bitset_encoded = bitsets_encoded[i][0..bitset_lengths[i]];
            var j: usize = 0;
            while (j < repeats) : (j += 1) {
                _ = ewah.decode(bitset_encoded, bitsets_decoded[i]);
            }
        }
        const decode_time = decode_timer.read() / samples / repeats;

        i = 0;
        while (i < samples) : (i += 1) {
            assert(std.mem.eql(usize, bitsets[i], bitsets_decoded[i]));
        }

        // Compute compression ratio.
        var total_uncompressed: f64 = 0.0;
        var total_compressed: f64 = 0.0;
        i = 0;
        while (i < samples) : (i += 1) {
            total_uncompressed += @as(f64, @floatFromInt(bitsets[i].len * @sizeOf(usize)));
            total_compressed += @as(f64, @floatFromInt(bitset_lengths[i]));
        }

        try stdout.print("Words={:_>3} E(Run)={:_>3} E(Literal)={:_>3} EncTime={:_>6}ns DecTime={:_>6}ns Ratio={d:_>6.2}\n", .{
            config.words,
            config.run_length_e,
            config.literals_length_e,
            encode_time,
            decode_time,
            total_uncompressed / total_compressed,
        });
    }
}

fn make_bitset(allocator: std.mem.Allocator, config: BitSetConfig) ![]usize {
    var words = try allocator.alloc(usize, config.words);
    var w: usize = 0;
    var literal: usize = 1;
    while (w < words.len) : (w += 1) {
        const run_length = prng.random().uintLessThan(usize, 2 * config.run_length_e);
        const literals_length = prng.random().uintLessThan(usize, 2 * config.literals_length_e);
        const run_bit = prng.random().boolean();

        const run_end = @min(w + run_length, words.len);
        while (w < run_end) : (w += 1) {
            words[w] = if (run_bit) std.math.maxInt(usize) else 0;
        }
        const literals_end = @min(w + literals_length, words.len);
        while (w < literals_end) : (w += 1) {
            words[w] = literal;
            literal += 1;
        }
    }
    return words;
}
