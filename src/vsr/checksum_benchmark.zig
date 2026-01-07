const std = @import("std");

const cache_line_size = @import("../constants.zig").cache_line_size;
const checksum = @import("checksum.zig").checksum;

const stdx = @import("stdx");

const KiB = stdx.KiB;
const MiB = stdx.MiB;

const Bench = @import("../testing/bench.zig");

const repetitions = 35;

test "benchmark: checksum" {
    var bench: Bench = .init();
    defer bench.deinit();

    const blob_size = bench.parameter("blob_size", KiB, MiB);

    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_instance.deinit();

    const arena = arena_instance.allocator();
    var prng = stdx.PRNG.from_seed(bench.seed);
    const blob = try arena.alignedAlloc(u8, cache_line_size, blob_size);
    prng.fill(blob);

    var duration_samples: [repetitions]stdx.Duration = undefined;
    var checksum_counter: u128 = 0;

    for (&duration_samples) |*duration| {
        bench.start();
        checksum_counter +%= checksum(blob);
        duration.* = bench.stop();
    }

    const result = bench.estimate(&duration_samples);

    // See "benchmark: API tutorial" to understand why we print out the "hash" of this run.
    bench.report("checksum {x:0>32}", .{checksum_counter});
    bench.report("{} for whole blob", .{result});
}
