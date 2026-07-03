const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");
const Bench = stdx.Bench;
const radix = @import("radix.zig");

const repetitions: usize = 32;
const layouts = .{
    .{ .Key = u64, .value_bytes = 16 },
    .{ .Key = u128, .value_bytes = 32 },
    .{ .Key = u256, .value_bytes = 128 },
    .{ .Key = u64, .value_bytes = 128 },
};

test "benchmark: radix sort" {
    var bench: Bench = .init();
    defer bench.deinit();

    const values_count: usize = @intCast(bench.parameter("values_count", 64, 1 << 20));

    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_instance.deinit();
    const arena = arena_instance.allocator();

    var prng = stdx.PRNG.from_seed(bench.seed);
    var checksum: u64 = 0;
    inline for (layouts) |layout| {
        checksum +%= try run(
            &bench,
            layout.Key,
            layout.value_bytes,
            values_count,
            arena,
            &prng,
        );
    }
    bench.report("checksum={}", .{checksum});
}

fn run(
    bench: *Bench,
    comptime Key: type,
    comptime value_bytes: usize,
    values_count: usize,
    arena: std.mem.Allocator,
    prng: *stdx.PRNG,
) !u64 {
    const Value = ValueType(Key, value_bytes);
    const values_original = try arena.alignedAlloc(Value, 64, values_count);
    const values = try arena.alignedAlloc(Value, 64, values_count);
    const values_scratch = try arena.alignedAlloc(Value, 64, values_count);

    for (values_original) |*value| value.key = prng.int(Key);

    var duration_samples: [repetitions]stdx.Duration = undefined;
    var checksum: u64 = 0;
    for (&duration_samples) |*duration| {
        stdx.copy_disjoint(.exact, Value, values, values_original);

        bench.start();
        radix.sort(Key, Value, Value.key_from_value, values, values_scratch);
        duration.* = bench.stop();

        assert(std.sort.isSorted(Value, values, {}, Value.less_than));
        checksum +%= @truncate(values[values.len / 2].key);
    }

    const duration_overall = bench.estimate(&duration_samples);
    const duration_element: stdx.Duration = .{ .ns = duration_overall.ns / values_count };
    const duration_key_byte: stdx.Duration = .{ .ns = duration_element.ns / @sizeOf(Key) };
    bench.report("K={:_>2}B V={:_>3}B: {d:.0}, {} per element, {} per key byte", .{
        @sizeOf(Key),
        @sizeOf(Value),
        duration_overall,
        duration_element,
        duration_key_byte,
    });
    return checksum;
}

fn ValueType(comptime Key: type, comptime value_bytes: usize) type {
    return struct {
        const Value = @This();

        key: Key,
        body: [value_bytes - @sizeOf(Key)]u8 = @splat(0),

        comptime {
            assert(@sizeOf(Value) == value_bytes);
        }

        inline fn key_from_value(value: *const Value) Key {
            return value.key;
        }

        fn less_than(_: void, a: Value, b: Value) bool {
            return a.key < b.key;
        }
    };
}
