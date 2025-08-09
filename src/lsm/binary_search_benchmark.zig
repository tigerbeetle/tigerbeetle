const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const builtin = @import("builtin");

const stdx = @import("stdx");

const binary_search_keys_upsert_index =
    @import("./binary_search.zig").binary_search_keys_upsert_index;
const binary_search_values_upsert_index =
    @import("./binary_search.zig").binary_search_values_upsert_index;

const log = std.log;

const GiB = 1 << 30;

// Bump these up if you want to use this as a real benchmark rather than as a test.
const blob_size = @divExact(GiB, 1024);
const searches = 5_000;

const kv_types = .{
    .{ .key_size = @sizeOf(u64), .value_size = 128 },
    .{ .key_size = @sizeOf(u64), .value_size = 64 },
    .{ .key_size = @sizeOf(u128), .value_size = 16 },
    .{ .key_size = @sizeOf(u256), .value_size = 32 },
};

const values_per_page = .{ 128, 256, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024 };
const body_fmt = "K={:_>2}B V={:_>3}B N={:_>4} {s}{s}: WT={:_>6}ns UT={:_>6}ns";

test "benchmark: binary search" {
    log.info("Samples: {}", .{searches});
    log.info("WT: Wall time/search", .{});
    log.info("UT: utime time/search", .{});

    const seed = std.crypto.random.int(u64);
    var prng = stdx.PRNG.from_seed(seed);

    // Allocate on the heap just once.
    // All page allocations reuse this buffer to speed up the run time.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const blob = try arena.allocator().alloc(u8, blob_size);

    inline for (kv_types) |kv| {
        inline for (values_per_page) |values_count| {
            try run_benchmark(.{
                .blob_size = blob_size,
                .key_size = kv.key_size,
                .value_size = kv.value_size,
                .values_count = values_count,
                .searches = searches,
            }, blob, &prng);
        }
    }
}

fn run_benchmark(comptime layout: Layout, blob: []u8, prng: *stdx.PRNG) !void {
    assert(blob.len == layout.blob_size);
    const V = ValueType(layout);
    const K = V.Key;
    const Page = struct {
        values: [layout.values_count]V,
    };
    const page_count = layout.blob_size / @sizeOf(Page);

    // Search pages and keys in random order.
    const page_picker = shuffled_index(page_count, prng);
    const value_picker = shuffled_index(layout.values_count, prng);

    // Generate 1GiB worth of 24KiB pages.
    var blob_alloc = std.heap.FixedBufferAllocator.init(blob);
    const pages = try blob_alloc.allocator().alloc(Page, page_count);
    prng.fill(std.mem.sliceAsBytes(pages));
    for (pages) |*page| {
        for (&page.values, 0..) |*value, i| value.key = i;
    }

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
        log.info(body_fmt, .{
            layout.key_size,
            layout.value_size,
            layout.values_count,
            if (prefetch) "P" else "_",
            "B",
            result.wall_time,
            result.utime,
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

fn ValueType(comptime layout: Layout) type {
    return struct {
        pub const max_key = 1 << (8 * layout.key_size) - 1;
        pub const Key = math.IntFittingRange(0, max_key);
        const Value = @This();
        key: Key,
        body: [layout.value_size - layout.key_size]u8,

        comptime {
            assert(@sizeOf(Key) == layout.key_size);
            assert(@sizeOf(Value) == layout.value_size);
        }

        inline fn key_from_value(self: *const Value) Key {
            return self.key;
        }
    };
}

const BenchmarkResult = struct {
    wall_time: u64, // nanoseconds
    utime: u64, // nanoseconds
};

const Benchmark = struct {
    timer: std.time.Timer,
    utime_ns: u128,

    fn begin() !Benchmark {
        const timer = try std.time.Timer.start();
        return Benchmark{
            .timer = timer,
            .utime_ns = utime_nanos(),
        };
    }

    fn end(self: *Benchmark, samples: usize) !BenchmarkResult {
        const utime_now = utime_nanos();
        return BenchmarkResult{
            .wall_time = self.timer.read() / samples,
            .utime = @intCast((utime_now - self.utime_ns) / samples),
        };
    }

    fn utime_nanos() u128 {
        if (builtin.os.tag == .windows) {
            var creation_time: std.os.windows.FILETIME = undefined;
            var exit_time: std.os.windows.FILETIME = undefined;
            var kernel_time: std.os.windows.FILETIME = undefined;
            var user_time: std.os.windows.FILETIME = undefined;

            const get_process_times = @extern(
                *const fn (
                    in_hProcess: std.os.windows.HANDLE,
                    out_lpCreationTime: *std.os.windows.FILETIME,
                    out_lpExitTime: *std.os.windows.FILETIME,
                    out_lpKernelTime: *std.os.windows.FILETIME,
                    out_lpUserTime: *std.os.windows.FILETIME,
                ) callconv(std.os.windows.WINAPI) std.os.windows.BOOL,
                .{
                    .library_name = "kernel32",
                    .name = "GetProcessTimes",
                },
            );

            if (get_process_times(
                std.os.windows.kernel32.GetCurrentProcess(),
                &creation_time,
                &exit_time,
                &kernel_time,
                &user_time,
            ) == std.os.windows.FALSE) {
                std.debug.panic("GetProcessTimes(): {}", .{std.os.windows.kernel32.GetLastError()});
            }

            const utime100ns = (@as(u64, user_time.dwHighDateTime) << 32) | user_time.dwLowDateTime;
            return utime100ns * 100;
        }

        const utime_tv = std.posix.getrusage(std.posix.rusage.SELF).utime;
        return (@as(u128, @intCast(utime_tv.sec)) * std.time.ns_per_s) +
            (@as(u32, @intCast(utime_tv.usec)) * std.time.ns_per_us);
    }
};

// shuffle([0,1,…,n-1])
fn shuffled_index(comptime n: usize, prng: *stdx.PRNG) [n]usize {
    var indices: [n]usize = undefined;
    for (&indices, 0..) |*i, j| i.* = j;
    prng.shuffle(usize, indices[0..]);
    return indices;
}
