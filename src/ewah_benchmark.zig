const std = @import("std");
const assert = std.debug.assert;
const ewah = @import("ewah.zig").ewah(usize);
const stdx = @import("stdx");

const Bench = stdx.Bench;

const repetitions: usize = 32;

const BitSetConfig = struct {
    // Expected lengths in words, before truncation at the end of the bitset.
    run_length_e: usize,
    literals_length_e: usize,
};

const configs = [_]BitSetConfig{
    .{ .run_length_e = 10, .literals_length_e = 10 },
    .{ .run_length_e = 100, .literals_length_e = 10 },
    .{ .run_length_e = 200, .literals_length_e = 10 },
    .{ .run_length_e = 1, .literals_length_e = 100 },
};

test "benchmark: ewah" {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_instance.deinit();

    const arena = arena_instance.allocator();

    var bench: Bench = .init(arena);
    defer bench.deinit(arena);

    const words_count: usize = @intCast(bench.parameter("words_count", 640, 1 << 18));
    assert(words_count > 0);

    var prng = stdx.PRNG.from_seed(bench.seed);
    var checksum: u64 = 0;
    for (configs) |config| {
        checksum +%= try run(&bench, config, words_count, arena, &prng);
    }
    bench.report("checksum={}", .{checksum});
}

fn run(
    bench: *Bench,
    config: BitSetConfig,
    words_count: usize,
    gpa: std.mem.Allocator,
    prng: *stdx.PRNG,
) !u64 {
    const words = try gpa.alloc(usize, words_count);
    defer gpa.free(words);

    const encoded = try gpa.alignedAlloc(
        u8,
        .fromByteUnits(@alignOf(usize)),
        ewah.encode_size_max(words_count),
    );
    const decoded = try gpa.alloc(usize, words_count);
    defer gpa.free(decoded);

    make_bitset(words, config, prng);

    var encode_samples: [repetitions]stdx.Duration = undefined;
    var decode_samples: [repetitions]stdx.Duration = undefined;
    var encoded_size: usize = 0;
    var checksum: u64 = 0;
    for (&encode_samples, &decode_samples) |*encode_duration, *decode_duration| {
        bench.start();
        encoded_size = ewah.encode_all(words, encoded);
        encode_duration.* = bench.stop();

        bench.start();
        const decoded_size = ewah.decode_all(encoded[0..encoded_size], decoded);
        decode_duration.* = bench.stop();

        assert(decoded_size == words.len);
        assert(std.mem.eql(usize, words, decoded));
        checksum +%= std.hash.Wyhash.hash(0, encoded[0..encoded_size]);
        checksum +%= std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(decoded));
    }

    const encode_duration = bench.estimate(&encode_samples);
    const decode_duration = bench.estimate(&decode_samples);
    const uncompressed_bytes: f64 = @floatFromInt(words.len * @sizeOf(usize));
    const compressed_bytes: f64 = @floatFromInt(encoded_size);
    bench.report("E(Run)={:_>3} E(Literal)={:_>3}: EncTime={f} DecTime={f} Ratio={d:_>6.2}", .{
        config.run_length_e,
        config.literals_length_e,
        encode_duration,
        decode_duration,
        uncompressed_bytes / compressed_bytes,
    });
    return checksum;
}

fn make_bitset(words: []usize, config: BitSetConfig, prng: *stdx.PRNG) void {
    assert(config.run_length_e > 0);
    assert(config.literals_length_e > 0);

    var w: usize = 0;
    var literal: usize = 1;
    while (w < words.len) {
        // Uniform lengths in [1, 2E - 1] have mean E and always make progress.
        const run_length = 1 + prng.int_inclusive(usize, 2 * (config.run_length_e - 1));
        const literals_length = 1 + prng.int_inclusive(usize, 2 * (config.literals_length_e - 1));
        const run_bit = prng.boolean();

        const run_end = w + @min(run_length, words.len - w);
        while (w < run_end) : (w += 1) {
            words[w] = if (run_bit) std.math.maxInt(usize) else 0;
        }
        const literals_end = w + @min(literals_length, words.len - w);
        while (w < literals_end) : (w += 1) {
            words[w] = literal;
            literal += 1;
        }
    }
}
