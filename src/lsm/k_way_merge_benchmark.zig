const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");

const Bench = @import("../testing/bench.zig");
const Pending = error{Pending};
const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;

const streams_count_max = 32;
const repetitions: usize = 32;

// Those Values are close to the real-world use case.
const Values = .{
    ValueType(u64, 128),
    ValueType(u256, 32),
    ValueType(u256, 32),
    ValueType(u256, 32),
    ValueType(u128, 16),
    ValueType(u64, 8),
    ValueType(u128, 16),
};

test "benchmark: k-way-merge" {
    var bench: Bench = .init();
    defer bench.deinit();

    const streams_count: usize = @intCast(bench.parameter("streams_count", 4, 32));
    const stream_length: usize = @intCast(bench.parameter("stream_length", 128, 8192));
    assert(streams_count <= streams_count_max);

    var prng = stdx.PRNG.from_seed(bench.seed);

    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_instance.deinit();

    const arena = arena_instance.allocator();

    const streams = .{
        try prepare_streams(Values[0], &prng, arena, streams_count, stream_length),
        try prepare_streams(Values[1], &prng, arena, streams_count, stream_length),
        try prepare_streams(Values[2], &prng, arena, streams_count, stream_length),
        try prepare_streams(Values[3], &prng, arena, streams_count, stream_length),
        try prepare_streams(Values[4], &prng, arena, streams_count, stream_length),
        try prepare_streams(Values[5], &prng, arena, streams_count, stream_length),
        try prepare_streams(Values[6], &prng, arena, streams_count, stream_length),
    };
    comptime assert(streams.len == Values.len);

    var duration_samples: [repetitions]stdx.Duration = undefined;

    for (&duration_samples) |*duration| {
        bench.start();
        inline for (streams) |pair| {
            var context, const output = pair;
            context.merge(output);
        }
        duration.* = bench.stop();

        inline for (streams) |pair| {
            _, const output = pair;
            const Value = @TypeOf(output[0]);
            assert(std.sort.isSorted(Value, output, {}, Value.sort.asc));
        }
    }

    const duration_streams = bench.estimate(&duration_samples);
    var duration_element = duration_streams;
    duration_element.ns /= (streams_count * stream_length * streams.len);

    bench.report("{} total", .{
        duration_streams,
    });
    bench.report("{} per element", .{
        duration_element,
    });
}

pub fn prepare_streams(
    comptime Value: type,
    prng: *stdx.PRNG,
    arena: std.mem.Allocator,
    streams_count: usize,
    stream_length: usize,
) !struct { KWayMergeContextType(Value), []Value } {
    var streams = try arena.alignedAlloc(Value, 64, streams_count * stream_length);
    var context: KWayMergeContextType(Value) = .{
        .streams = undefined,
        .streams_count = @intCast(streams_count),
    };
    const output = try arena.alignedAlloc(Value, 64, streams_count * stream_length);

    for (0..streams_count) |stream_id| {
        const stream_begin = stream_id * stream_length;
        const stream_end = stream_begin + stream_length;
        const stream = streams[stream_begin..stream_end];

        for (stream) |*value| {
            value.key = prng.int_inclusive(Value.Key, 2_000_000);
        }

        std.mem.sort(Value, stream, {}, Value.sort.asc);

        context.streams[stream_id] = stream;
    }

    return .{ context, output };
}

fn KWayMergeContextType(comptime Value: type) type {
    return struct {
        const Context = @This();

        streams: [streams_count_max][]const Value,
        streams_count: u16,

        fn stream_peek(context: *const Context, stream_index: u32) Pending!?Value.Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return null;
            return stream[0].key;
        }

        fn stream_pop(context: *Context, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
        }

        fn merge(context: *Context, output: []Value) void {
            const KWayIterator = KWayMergeIteratorType(Context, Value.Key, Value, .{
                .streams_max = streams_count_max,
                .deduplicate = false,
            }, Value.key_from_value, stream_peek, stream_pop);

            var k_way_iterator = KWayIterator.init(context, context.streams_count, .ascending);

            for (output) |*slot| {
                slot.* = (k_way_iterator.pop() catch unreachable).?;
            }
        }
    };
}

fn ValueType(comptime KeyType: type, comptime value_size: u32) type {
    return struct {
        key: Key,
        body: [value_size - @sizeOf(Key)]u8,

        const Key = KeyType;
        const Value = @This();

        comptime {
            assert(@sizeOf(Value) == value_size);
        }

        inline fn key_from_value(self: *const Value) Key {
            return self.key;
        }

        pub const sort = struct {
            pub fn asc(ctx: void, lhs: Value, rhs: Value) bool {
                return std.sort.asc(Key)(ctx, lhs.key, rhs.key);
            }
        };
    };
}
