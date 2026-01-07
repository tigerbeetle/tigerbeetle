const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("stdx");
const KiB = stdx.KiB;
const MiB = stdx.MiB;
const GiB = stdx.GiB;

const Bench = @import("../testing/bench.zig");

const binary_search_values_upsert_index =
    @import("./binary_search.zig").binary_search_values_upsert_index;

const kv_types = .{
    .{ .key_size = @sizeOf(u64), .value_size = 128 },
    .{ .key_size = @sizeOf(u256), .value_size = 32 },
};

const Scenario = struct {
    name: []const u8,
    values_per_page: u32,
    page_buffer_size: usize,
};

const scenarios = [_]Scenario{
    .{
        .name = "in-cache",
        .values_per_page = 64,
        .page_buffer_size = 256 * KiB,
    },
    .{
        .name = "out-of-cache",
        .values_per_page = 4_096,
        .page_buffer_size = 1 * GiB,
    },
};

const body_fmt = "{s} K={:_>2}B V={:_>3}B N={:_>5}: WT={}";
const repetitions: usize = 32;

test "benchmark: binary search" {
    var bench: Bench = .init();
    defer bench.deinit();

    bench.report("WT: Wall time/search", .{});

    const blob_size = bench.parameter("blob_size", MiB, GiB);
    const searches = bench.parameter("searches", 500, 20_000);

    var prng = stdx.PRNG.from_seed(bench.seed);
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const blob = try arena.allocator().alignedAlloc(u8, 64, blob_size);
    var checksum: u64 = 0;

    inline for (kv_types) |kv| {
        inline for (scenarios) |scenario| {
            // Clamp for `smoke` mode.
            const page_buffer_size = @min(blob_size, scenario.page_buffer_size);
            checksum +%= try run_benchmark(
                &bench,
                scenario.name,
                .{
                    .key_size = kv.key_size,
                    .value_size = kv.value_size,
                    .values_count = scenario.values_per_page,
                },
                searches,
                blob[0..page_buffer_size],
                arena.allocator(),
                &prng,
            );
        }
    }
    bench.report("checksum {}", .{checksum});
}

fn run_benchmark(
    bench: *Bench,
    scenario_name: []const u8,
    comptime layout: Layout,
    search_count: u64,
    page_buffer: []u8,
    arena: std.mem.Allocator,
    prng: *stdx.PRNG,
) !u64 {
    const V = ValueType(layout);
    const K = V.Key;
    const Page = struct {
        values: [layout.values_count]V,
    };

    const page_count = @divFloor(page_buffer.len, @sizeOf(Page));
    assert(page_count > 0);
    if (page_count > 1024 * 1024) @panic("page_count too large");

    const page_picker = try arena.alloc(usize, page_count);
    shuffled_index(prng, page_picker);

    const value_picker = try arena.alloc(usize, layout.values_count);
    shuffled_index(prng, value_picker);

    var page_alloc = std.heap.FixedBufferAllocator.init(page_buffer);
    const pages = try page_alloc.allocator().alloc(Page, page_count);
    prng.fill(std.mem.sliceAsBytes(pages));
    for (pages) |*page| {
        for (&page.values, 0..) |*value, i| value.key = i;
    }

    var duration_samples: [repetitions]stdx.Duration = undefined;
    var checksum: u64 = 0;
    for (&duration_samples) |*duration| {
        bench.start();
        for (0..search_count) |i| {
            const target = value_picker[i % value_picker.len];
            const page = &pages[page_picker[i % page_picker.len]];
            const hit = page.values[
                binary_search_values_upsert_index(
                    K,
                    V,
                    V.key_from_value,
                    page.values[0..],
                    target,
                    .{},
                )
            ];

            assert(hit.key == target);
            checksum +%= @truncate(hit.key);
        }
        duration.* = bench.stop();
        duration.ns /= search_count;
    }

    const result = bench.estimate(&duration_samples);

    bench.report(body_fmt, .{
        scenario_name,
        layout.key_size,
        layout.value_size,
        layout.values_count,
        result,
    });

    return checksum;
}

const Layout = struct {
    key_size: usize, // bytes per key
    value_size: usize, // bytes per value
    values_count: usize, // values per page
};

fn ValueType(comptime layout: Layout) type {
    return struct {
        pub const max_key = (1 << (8 * layout.key_size)) - 1;
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

// shuffle([0,1,…,n-1])
fn shuffled_index(prng: *stdx.PRNG, indices: []usize) void {
    for (indices, 0..) |*i, j| i.* = j;
    prng.shuffle(usize, indices);
}
