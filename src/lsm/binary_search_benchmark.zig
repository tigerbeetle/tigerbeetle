const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const binary_search_keys_upsert_index = @import("./binary_search.zig").binary_search_keys_upsert_index;
const binary_search_values_upsert_index = @import("./binary_search.zig").binary_search_values_upsert_index;

const GiB = 1 << 30;
const searches = 500_000;

const kv_types = .{
    .{ .key_size = @sizeOf(u64), .value_size = 128 },
    .{ .key_size = @sizeOf(u64), .value_size = 64 },
    .{ .key_size = @sizeOf(u128), .value_size = 16 },
    .{ .key_size = @sizeOf(u256), .value_size = 32 },
};

const values_per_page = .{ 128, 256, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024 };
const body_fmt = "K={:_>2}B V={:_>3}B N={:_>4} {s}{s}: WT={:_>6}ns UT={:_>6}ns" ++
    " CY={:_>6} IN={:_>6} CR={:_>5} CM={:_>5} BM={}\n";

pub fn main() !void {
    std.log.info("Samples: {}", .{searches});
    std.log.info("WT: Wall time/search", .{});
    std.log.info("UT: utime time/search", .{});
    std.log.info("CY: CPU cycles/search", .{});
    std.log.info("IN: instructions/search", .{});
    std.log.info("CR: cache references/search", .{});
    std.log.info("CM: cache misses/search", .{});
    std.log.info("BM: branch misses/search", .{});

    var seed: u64 = undefined;
    try std.os.getrandom(std.mem.asBytes(&seed));
    var prng = std.rand.DefaultPrng.init(seed);

    // Allocate on the heap just once.
    // All page allocations reuse this buffer to speed up the run time.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const blob_size = GiB;
    var blob = try arena.allocator().alloc(u8, blob_size);

    inline for (kv_types) |kv| {
        inline for (values_per_page) |values_count| {
            try run_benchmark(.{
                .blob_size = blob_size,
                .key_size = kv.key_size,
                .value_size = kv.value_size,
                .values_count = values_count,
                .searches = searches,
            }, blob, prng.random());
        }
    }
}

fn run_benchmark(comptime layout: Layout, blob: []u8, random: std.rand.Random) !void {
    assert(blob.len == layout.blob_size);
    const V = Value(layout);
    const K = V.Key;
    const Page = struct {
        values: [layout.values_count]V,
    };
    const page_count = layout.blob_size / @sizeOf(Page);

    // Search pages and keys in random order.
    var page_picker = shuffled_index(page_count, random);
    var value_picker = shuffled_index(layout.values_count, random);

    // Generate 1GiB worth of 24KiB pages.
    var blob_alloc = std.heap.FixedBufferAllocator.init(blob);
    var pages = try blob_alloc.allocator().alloc(Page, page_count);
    random.bytes(std.mem.sliceAsBytes(pages));
    for (pages) |*page| {
        for (&page.values, 0..) |*value, i| value.key = i;
    }

    const stdout = std.io.getStdOut().writer();
    inline for (&.{ true, false }) |prefetch| {
        var benchmark = try Benchmark.begin();
        var i: usize = 0;
        var v: usize = 0;
        while (i < layout.searches) : (i += 1) {
            const target = value_picker[v % value_picker.len];
            const page = &pages[page_picker[i % page_picker.len]];
            const hit = page.values[
                binary_search_values_upsert_index(
                    K,
                    V,
                    V.key_from_value,
                    page.values[0..],
                    target,
                    .{ .prefetch = prefetch },
                )
            ];

            assert(hit.key == target);
            if (i % pages.len == 0) v += 1;
        }
        const result = try benchmark.end(layout.searches);
        try stdout.print(body_fmt, .{
            layout.key_size,
            layout.value_size,
            layout.values_count,
            if (prefetch) "P" else "_",
            "B",
            result.wall_time,
            result.utime,
            result.cpu_cycles,
            result.instructions,
            result.cache_references,
            result.cache_misses,
            result.branch_misses,
        });
    }
}

const Layout = struct {
    blob_size: usize, // bytes allocated for all pages
    key_size: usize, // bytes per key
    value_size: usize, // bytes per value
    values_count: usize, // values per page
    searches: usize,
};

fn Value(comptime layout: Layout) type {
    return struct {
        pub const max_key = 1 << (8 * layout.key_size) - 1;
        pub const Key = math.IntFittingRange(0, max_key);
        const Self = @This();
        key: Key,
        body: [layout.value_size - layout.key_size]u8,

        comptime {
            assert(@sizeOf(Key) == layout.key_size);
            assert(@sizeOf(Self) == layout.value_size);
        }

        inline fn key_from_value(self: *const Self) Key {
            return self.key;
        }
    };
}

const BenchmarkResult = struct {
    wall_time: u64, // nanoseconds
    utime: u64, // nanoseconds
    cpu_cycles: usize,
    instructions: usize,
    cache_references: usize,
    cache_misses: usize,
    branch_misses: usize,
};

const PERF = std.os.linux.PERF;
const perf_counters = [_]PERF.COUNT.HW{
    PERF.COUNT.HW.CPU_CYCLES,
    PERF.COUNT.HW.INSTRUCTIONS,
    PERF.COUNT.HW.CACHE_REFERENCES,
    PERF.COUNT.HW.CACHE_MISSES,
    PERF.COUNT.HW.BRANCH_MISSES,
};

const Benchmark = struct {
    timer: std.time.Timer,
    rusage: std.os.rusage,
    perf_fds: [perf_counters.len]std.os.fd_t,

    fn begin() !Benchmark {
        var perf_fds = [1]std.os.fd_t{-1} ** perf_counters.len;
        for (perf_counters, 0..) |counter, i| {
            var attr: std.os.linux.perf_event_attr = .{
                .type = PERF.TYPE.HARDWARE,
                .config = @intFromEnum(counter),
                .flags = .{
                    .disabled = true,
                    .exclude_kernel = true,
                    .exclude_hv = true,
                },
            };
            perf_fds[i] = try std.os.perf_event_open(&attr, 0, -1, perf_fds[0], PERF.FLAG.FD_CLOEXEC);
        }
        const err = std.os.linux.ioctl(perf_fds[0], PERF.EVENT_IOC.ENABLE, PERF.IOC_FLAG_GROUP);
        if (err == -1) return error.Unexpected;

        // Start the wall clock after perf, since setup is slow.
        const timer = try std.time.Timer.start();
        return Benchmark{
            .timer = timer,
            // TODO pass std.os.linux.rusage.SELF once Zig is upgraded
            .rusage = std.os.getrusage(0),
            .perf_fds = perf_fds,
        };
    }

    fn end(self: *Benchmark, samples: usize) !BenchmarkResult {
        defer {
            for (perf_counters, 0..) |_, i| {
                std.os.close(self.perf_fds[i]);
                self.perf_fds[i] = -1;
            }
        }

        const rusage = std.os.getrusage(0);
        const err = std.os.linux.ioctl(self.perf_fds[0], PERF.EVENT_IOC.DISABLE, PERF.IOC_FLAG_GROUP);
        if (err == -1) return error.Unexpected;
        return BenchmarkResult{
            .wall_time = self.timer.read() / samples,
            .utime = (timeval_to_ns(rusage.utime) - timeval_to_ns(self.rusage.utime)) / samples,
            .cpu_cycles = (try readPerfFd(self.perf_fds[0])) / samples,
            .instructions = (try readPerfFd(self.perf_fds[1])) / samples,
            .cache_references = (try readPerfFd(self.perf_fds[2])) / samples,
            .cache_misses = (try readPerfFd(self.perf_fds[3])) / samples,
            .branch_misses = (try readPerfFd(self.perf_fds[4])) / samples,
        };
    }
};

// shuffle([0,1,…,n-1])
fn shuffled_index(comptime n: usize, rand: std.rand.Random) [n]usize {
    var indices: [n]usize = undefined;
    for (&indices, 0..) |*i, j| i.* = j;
    rand.shuffle(usize, indices[0..]);
    return indices;
}

fn timeval_to_ns(tv: std.os.timeval) u64 {
    const ns_per_us = std.time.ns_per_s / std.time.us_per_s;
    return @as(u64, @bitCast(tv.tv_sec)) * std.time.ns_per_s +
        @as(u64, @bitCast(tv.tv_usec)) * ns_per_us;
}

fn readPerfFd(fd: std.os.fd_t) !usize {
    var result: usize = 0;
    const n = try std.os.read(fd, std.mem.asBytes(&result));
    assert(n == @sizeOf(usize));

    return result;
}
