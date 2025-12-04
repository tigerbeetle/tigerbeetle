const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("stdx");
const MiB = stdx.MiB;
const GiB = stdx.GiB;

const Bench = @import("../testing/bench.zig");

const binary_search_keys_upsert_index =
    @import("./binary_search.zig").binary_search_keys_upsert_index;
const binary_search_values_upsert_index =
    @import("./binary_search.zig").binary_search_values_upsert_index;

const kv_types = .{
    .{ .key_size = @sizeOf(u64), .value_size = 128 },
    .{ .key_size = @sizeOf(u64), .value_size = 64 },
    .{ .key_size = @sizeOf(u128), .value_size = 16 },
    .{ .key_size = @sizeOf(u256), .value_size = 32 },
};

const values_per_page = .{ 128, 256, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024 };
const body_fmt = "K={:_>2}B V={:_>3}B N={:_>4} {s}{s}: WT={}";

test "benchmark: binary search" {
    var bench: Bench = .init();
    defer bench.deinit();

    const blob_size = bench.parameter("blob_size", MiB, GiB);
    const searches = bench.parameter("searches", 5_000, 500_000);

    bench.report("WT: Wall time/search", .{});

    const seed = std.crypto.random.int(u64);
    var prng = stdx.PRNG.from_seed(seed);

    // Allocate on the heap just once.
    // All page allocations reuse this buffer to speed up the run time.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const blob = try arena.allocator().alloc(u8, blob_size);

    inline for (kv_types) |kv| {
        inline for (values_per_page) |values_count| {
            try run_benchmark(&bench, .{
                .key_size = kv.key_size,
                .value_size = kv.value_size,
                .values_count = values_count,
            }, searches, blob, &prng);
        }
    }
}

fn run_benchmark(
    bench: *Bench,
    comptime layout: Layout,
    search_count: u64,
    blob: []u8,
    prng: *stdx.PRNG,
) !void {
    const V = ValueType(layout);
    const K = V.Key;
    const Page = struct {
        values: [layout.values_count]V,
    };
    const page_count = @divFloor(blob.len, @sizeOf(Page));
    assert(page_count > 0);
    const page_count_max = 1024 * 1024;
    if (page_count > page_count_max) @panic("page_count too large");

    // Search pages and keys in random order.
    var page_picker_buffer: [page_count_max]usize = undefined;
    const page_picker = page_picker_buffer[0..page_count];
    shuffled_index(prng, page_picker);

    var value_picker: [layout.values_count]usize = undefined;
    shuffled_index(prng, value_picker[0..]);

    // Generate 1GiB worth of 24KiB pages.
    var blob_alloc = std.heap.FixedBufferAllocator.init(blob);
    const pages = try blob_alloc.allocator().alloc(Page, page_count);
    prng.fill(std.mem.sliceAsBytes(pages));
    for (pages) |*page| {
        for (&page.values, 0..) |*value, i| value.key = i;
    }

    inline for (&.{ true, false }) |prefetch| {
        bench.start();
        var v: usize = 0;
        for (0..search_count) |i| {
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
        var result_per_search = bench.stop();
        result_per_search.ns /= search_count;
        bench.report(body_fmt, .{
            layout.key_size,
            layout.value_size,
            layout.values_count,
            if (prefetch) "P" else "_",
            "B",
            result_per_search,
        });
    }
}

const Layout = struct {
    key_size: usize, // bytes per key
    value_size: usize, // bytes per value
    values_count: usize, // values per page
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

// shuffle([0,1,…,n-1])
fn shuffled_index(prng: *stdx.PRNG, indices: []usize) void {
    for (indices, 0..) |*i, j| i.* = j;
    prng.shuffle(usize, indices);
}
