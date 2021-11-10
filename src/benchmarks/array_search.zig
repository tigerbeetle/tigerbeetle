const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const binary_search = @import("../binary_search.zig").binary_search;
const eytzinger = @import("../eytzinger.zig").eytzinger;

const KiB = 1 << 10;
const GiB = 1 << 30;

// TODO Test secondary index (24:24) as well.
const kv_types = .{
    .{.Key = u64, .value_size = 128},
    .{.Key = u64, .value_size = 64},
};

// keys_per_summary = values_per_page / summary_fraction
const summary_fractions = .{4, 8, 16, 32};
const values_per_page = .{128, 256, 512, 1024, 2048, 4096, 8192};

pub fn main() !void {
    const searches = 10000;
    std.log.info("Samples: {}", .{searches});
    std.log.info("│ {s:7} │ {s:7} │ {s:4} │ {s:6} │ {s:8} │ {s:9} │", .{
        "KeySize", "ValSize", "Keys", "Values", "μ Search", "Algorithm",
    });
    // TODO why do these need (besides kv_types) need to be `inline for`?
    inline for (summary_fractions) |summary_fraction| {
        inline for (values_per_page) |values_count| {
            inline for (kv_types) |kv| {
                // Generate 1GiB worth of 24KiB pages.
                const layout = .{
                    .Key = kv.Key,
                    .algorithm = Algorithm.eytzinger,
                    .value_size = kv.value_size,
                    .keys_count = values_count / summary_fraction,
                    .values_count = values_count,
                    .searches = searches,
                };

                const eytz_result = try benchmark_eytzinger(layout);
                std.log.info("│ {:7} │ {:7} │ {:4} │ {:6} │ {d:6}ns │ {s:9} │", .{
                    @sizeOf(kv.Key),
                    kv.value_size,
                    layout.keys_count,
                    layout.values_count,
                    eytz_result.wall_time.mean(),
                    "Eytzinger",
                });

                const bins_result = try benchmark_binary_search(layout);
                std.log.info("│ {:7} │ {:7} │ {:4} │ {:6} │ {d:6}ns │ {s:9} │", .{
                    @sizeOf(kv.Key),
                    kv.value_size,
                    layout.keys_count,
                    layout.values_count,
                    bins_result.wall_time.mean(),
                    "BinSearch",
                });
            }
        }
    }
}

const Algorithm = enum { eytzinger, binary_search };
const Layout = struct {
    Key: type,
    algorithm: Algorithm,
    value_size: usize, // bytes per value
    keys_count: usize, // keys per page (in the summary)
    values_count: usize, // values per page
    searches: usize,
};

const BenchmarkResult = struct {
    wall_time: Histogram, // nanoseconds
};

fn Value(comptime layout: Layout) type {
    return struct {
        const Self = @This();
        key: layout.Key,
        body: [layout.value_size - @sizeOf(layout.Key)]u8,

        fn key_from_value(self: Self) layout.Key {
            return self.key;
        }

        fn key_compare(a: layout.Key, b: layout.Key) math.Order {
            return math.order(a, b);
        }
    };
}

// TODO track percentiles. Also, record measurements as floats (or maybe be generic)?
const Histogram = struct {
    sum: u64 = 0,
    min: u64 = math.maxInt(u64),
    max: u64 = 0,
    count: usize = 0,

    fn mean(self: Histogram) u64 {
        return self.sum / self.count;
    }

    fn sample(self: *Histogram, value: u64) void {
        self.sum += value;
        self.count += 1;
        if (value < self.min) self.min = value;
        if (value > self.max) self.max = value;
    }
};

const Benchmark = struct {
    arena: std.heap.ArenaAllocator,
    prng: std.rand.DefaultPrng,
    timer: std.time.Timer,

    wall_time: Histogram,
    // TODO track utime/stime with os.rusage()
    // TODO use linux perf to track cache/branch misses

    fn init() !Benchmark {
        var seed: u64 = undefined;
        try std.os.getrandom(std.mem.asBytes(&seed));
        var prng = std.rand.DefaultPrng.init(seed);
        return Benchmark{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .prng = prng,
            .timer = try std.time.Timer.start(),
            .wall_time = Histogram{},
        };
    }

    fn deinit(self: *Benchmark) void {
        self.arena.deinit();
    }

    fn begin(self: *Benchmark) void {
        self.timer.reset();
    }

    fn end(self: *Benchmark) void {
        self.wall_time.sample(self.timer.read());
    }

    fn result(self: Benchmark) BenchmarkResult {
        return BenchmarkResult{
            .wall_time = self.wall_time,
        };
    }
};

fn benchmark_eytzinger(comptime layout: Layout) !BenchmarkResult {
    var benchmark = try Benchmark.init();
    defer benchmark.deinit();

    const Eytzinger = eytzinger(layout.keys_count, layout.values_count);

    const Val = Value(layout);
    const Page = struct {
        summary: [layout.keys_count]layout.Key,
        values: [layout.values_count]Val,
    };
    const page_count = GiB / @sizeOf(Page); //(24 * KiB);

    var pages = try benchmark.arena.allocator.alloc(Page, page_count);
    for (pages) |*page| {
        benchmark.prng.random.bytes(std.mem.sliceAsBytes(&page.values));
        //std.sort.sort(Value, page.values[0..], {}, Value.key_lt);
        // TODO should the keys be randomized too?
        for (page.values) |*value, i| value.key = i;
        Eytzinger.layout(layout.Key, Val, Val.key_from_value, &page.summary, &page.values);
    }

    // Search pages and keys in random order.
    var page_picker = shuffled_index(page_count, &benchmark.prng.random);
    var value_picker = shuffled_index(layout.values_count, &benchmark.prng.random);

    var i: usize = 0;
    var v: usize = 0;
    while (i < layout.searches) : (i += 1) {
        const target = value_picker[v % value_picker.len];
        const page_index = page_picker[i % page_picker.len];

        benchmark.begin();
        const page = pages[page_index];
        const bounds = Eytzinger.search(layout.Key, Val, Val.key_from_value, Val.key_compare, &page.summary, &page.values, target);
        const hit = if (bounds.len == 0) unreachable
            else if (bounds.len == 1) bounds[0]
            else bounds[binary_search(layout.Key, Val, Val.key_from_value, Val.key_compare, bounds, target)];
        benchmark.end();

        assert(hit.key == target);
        if (i % pages.len == 0) v += 1;
    }

    return benchmark.result();
}

fn benchmark_binary_search(comptime layout: Layout) !BenchmarkResult {
    var benchmark = try Benchmark.init();
    defer benchmark.deinit();
    const Val = Value(layout);
    const Page = [layout.values_count]Val;
    const page_count = GiB / @sizeOf(Page); //(24 * KiB);

    var pages = try benchmark.arena.allocator.alloc(Page, page_count);
    for (pages) |*page| {
        benchmark.prng.random.bytes(std.mem.sliceAsBytes(page[0..]));
        //std.sort.sort(Value, page.values[0..], {}, Value.key_lt);
        // TODO should the keys be randomized too?
        for (page) |*value, i| value.key = i;
    }

    // Search pages and keys in random order.
    var page_picker = shuffled_index(page_count, &benchmark.prng.random);
    var value_picker = shuffled_index(layout.values_count, &benchmark.prng.random);

    var i: usize = 0;
    var v: usize = 0;
    while (i < layout.searches) : (i += 1) {
        const target = value_picker[v % value_picker.len];
        const p = page_picker[i % page_picker.len];

        benchmark.begin();
        const page = pages[p];
        const hit = page[binary_search(layout.Key, Val, Val.key_from_value, Val.key_compare, page[0..], target)];
        benchmark.end();

        assert(hit.key == target);
        if (i % pages.len == 0) v += 1;
    }

    return benchmark.result();
}

// shuffle([0,1,…,n-1])
fn shuffled_index(comptime n: usize, rand: *std.rand.Random) [n]usize {
    var indices: [n]usize = undefined;
    for (indices) |*i, j| i.* = j;
    rand.shuffle(usize, indices[0..]);
    return indices;
}
