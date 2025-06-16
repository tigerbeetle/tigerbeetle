const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const math = std.math;
const mem = std.mem;

const Direction = @import("../direction.zig").Direction;

pub fn KWayMergeIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime streams_max: u32,
    /// Peek the next key in the stream identified by stream_index.
    /// For example, peek(stream_index=2) returns user_streams[2][0].
    /// Returns Drained if the stream was consumed and
    /// must be refilled before calling peek() again.
    /// Returns Empty if the stream was fully consumed and reached the end.
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) error{ Empty, Drained }!Key,
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
    /// Returns true if stream A has higher precedence than stream B.
    /// This is used to deduplicate values across streams.
    comptime stream_precedence: fn (context: *const Context, a: u32, b: u32) bool,
) type {
    return struct {
        const KWayMergeIterator = @This();

        context: *Context,
        streams_count: u32,
        direction: Direction,
        state: enum { loading, iterating },

        /// Array of keys, with each key representing the next key in each stream.
        ///
        /// `keys` is *almost* structured as a binary heap — to become a heap, streams[0] must be
        /// peeked and sifted (see pop_internal()).
        ///
        /// * When `direction=ascending`, keys are ordered low-to-high.
        /// * When `direction=descending`, keys are ordered high-to-low.
        /// * Equivalent keys are ordered from high precedence to low.
        keys: [streams_max]Key = undefined,

        /// For each key in keys above, the corresponding index of the stream containing that key.
        /// This decouples the order and storage of streams, the user being responsible for storage.
        /// The user's streams array is never reordered while keys are swapped, only this mapping.
        streams: [streams_max]u32 = undefined,

        /// The number of streams remaining in the iterator.
        k: u32 = 0,

        key_popped: ?Key = null,

        pub fn init(
            context: *Context,
            streams_count: u32,
            direction: Direction,
        ) KWayMergeIterator {
            assert(streams_count <= streams_max);
            // Streams ZERO can be used to represent empty sets.
            maybe(streams_count == 0);

            return .{
                .context = context,
                .streams_count = streams_count,
                .direction = direction,
                .state = .loading,
            };
        }

        pub fn empty(it: *const KWayMergeIterator) bool {
            assert(it.state == .iterating);
            return it.k == 0;
        }

        pub fn reset(it: *KWayMergeIterator) void {
            it.* = .{
                .context = it.context,
                .streams_count = it.streams_count,
                .direction = it.direction,
                .state = .loading,
                .key_popped = it.key_popped,
            };
        }

        fn load(it: *KWayMergeIterator) error{Drained}!void {
            assert(it.state == .loading);
            assert(it.k == 0);

            errdefer it.reset();

            // We must loop on stream_index but assign at it.k, as k may be less than stream_index
            // when there are empty streams.
            // TODO Do we have test coverage for this edge case?
            var stream_index: u32 = 0;
            while (stream_index < it.streams_count) : (stream_index += 1) {
                it.keys[it.k] = stream_peek(it.context, stream_index) catch |err| switch (err) {
                    error.Drained => return error.Drained,
                    error.Empty => continue,
                };
                it.streams[it.k] = stream_index;
                it.up_heap(it.k);
                it.k += 1;
            }
            it.state = .iterating;
        }

        pub fn pop(it: *KWayMergeIterator) error{Drained}!?Value {
            if (it.state == .loading) try it.load();
            assert(it.state == .iterating);

            while (try it.pop_heap()) |value| {
                const key = key_from_value(&value);
                if (it.key_popped) |previous| {
                    switch (std.math.order(previous, key)) {
                        .lt => assert(it.direction == .ascending),
                        // Discard this value and pop the next one.
                        .eq => continue,
                        .gt => assert(it.direction == .descending),
                    }
                }
                it.key_popped = key;
                return value;
            }

            return null;
        }

        fn pop_heap(it: *KWayMergeIterator) error{Drained}!?Value {
            assert(it.state == .iterating);
            if (it.k == 0) return null;

            // We update the heap prior to removing the value from the stream. If we updated after
            // stream_pop() instead, when stream_peek() returns Drained we would be unable to order
            // the heap, and when the stream does buffer data it would be out of position.
            if (stream_peek(it.context, it.streams[0])) |key| {
                it.keys[0] = key;
                it.down_heap();
            } else |err| switch (err) {
                error.Drained => return error.Drained,
                error.Empty => {
                    it.swap(0, it.k - 1);
                    it.k -= 1;
                    it.down_heap();
                },
            }
            if (it.k == 0) return null;

            const root = it.streams[0];
            const value = stream_pop(it.context, root);

            return value;
        }

        fn up_heap(it: *KWayMergeIterator, start: u32) void {
            var i = start;
            while (parent(i)) |p| : (i = p) {
                if (it.ordered(p, i)) break;
                it.swap(p, i);
            }
        }

        // Start at the root node.
        // Compare the current node with its children, if the order is correct stop.
        // If the order is incorrect, swap the current node with the appropriate child.
        fn down_heap(it: *KWayMergeIterator) void {
            if (it.k == 0) return;
            var i: u32 = 0;
            // A maximum of height iterations are required. After height iterations we are
            // guaranteed to have reached a leaf node, in which case we are always done.
            var safety_count: u32 = 0;
            const binary_tree_height = math.log2_int(u32, it.k) + 1;
            while (safety_count < binary_tree_height) : (safety_count += 1) {
                const left = left_child(i, it.k);
                const right = right_child(i, it.k);

                if (it.ordered(i, left)) {
                    if (it.ordered(i, right)) {
                        break;
                    } else {
                        it.swap(i, right.?);
                        i = right.?;
                    }
                } else if (it.ordered(i, right)) {
                    it.swap(i, left.?);
                    i = left.?;
                } else if (it.ordered(left.?, right.?)) {
                    it.swap(i, left.?);
                    i = left.?;
                } else {
                    it.swap(i, right.?);
                    i = right.?;
                }
            }
            assert(safety_count < binary_tree_height);
        }

        fn parent(node: u32) ?u32 {
            if (node == 0) return null;
            return (node - 1) / 2;
        }

        fn left_child(node: u32, k: u32) ?u32 {
            const child = 2 * node + 1;
            return if (child < k) child else null;
        }

        fn right_child(node: u32, k: u32) ?u32 {
            const child = 2 * node + 2;
            return if (child < k) child else null;
        }

        fn swap(it: *KWayMergeIterator, a: u32, b: u32) void {
            mem.swap(Key, &it.keys[a], &it.keys[b]);
            mem.swap(u32, &it.streams[a], &it.streams[b]);
        }

        inline fn ordered(it: *const KWayMergeIterator, a: u32, b_maybe: ?u32) bool {
            const b = b_maybe orelse return true;
            return if (it.keys[a] == it.keys[b])
                stream_precedence(it.context, it.streams[a], it.streams[b])
            else if (it.keys[a] < it.keys[b])
                it.direction == .ascending
            else
                it.direction == .descending;
        }
    };
}

fn TestContextType(comptime streams_max: u32) type {
    const testing = std.testing;

    return struct {
        const TestContext = @This();

        const log = false;

        const Value = struct {
            key: u32,
            version: u32,

            inline fn to_key(v: *const Value) u32 {
                return v.key;
            }
        };

        streams: [streams_max][]const Value,

        fn stream_peek(
            context: *const TestContext,
            stream_index: u32,
        ) error{ Empty, Drained }!u32 {
            // TODO: test for Drained somehow as well.
            const stream = context.streams[stream_index];
            if (stream.len == 0) return error.Empty;
            return stream[0].key;
        }

        fn stream_pop(context: *TestContext, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
        }

        fn stream_precedence(context: *const TestContext, a: u32, b: u32) bool {
            _ = context;

            // Higher streams have higher precedence.
            return a > b;
        }

        fn merge(
            direction: Direction,
            streams_keys: []const []const u32,
            expect: []const Value,
        ) !void {
            const KWay = KWayMergeIteratorType(
                TestContext,
                u32,
                Value,
                Value.to_key,
                streams_max,
                stream_peek,
                stream_pop,
                stream_precedence,
            );
            var actual = std.ArrayList(Value).init(testing.allocator);
            defer actual.deinit();

            var streams: [streams_max][]Value = undefined;

            for (streams_keys, 0..) |stream_keys, i| {
                errdefer for (streams[0..i]) |s| testing.allocator.free(s);
                streams[i] = try testing.allocator.alloc(Value, stream_keys.len);
                for (stream_keys, 0..) |key, j| {
                    streams[i][j] = .{
                        .key = key,
                        .version = @intCast(i),
                    };
                }
            }
            defer for (streams[0..streams_keys.len]) |s| testing.allocator.free(s);

            var context: TestContext = .{ .streams = streams };
            var kway = KWay.init(&context, @intCast(streams_keys.len), direction);

            while (try kway.pop()) |value| {
                try actual.append(value);
            }

            try testing.expectEqualSlices(Value, expect, actual.items);
        }

        fn fuzz(prng: *stdx.PRNG, stream_key_count_max: u32) !void {
            if (log) std.debug.print("\n", .{});
            const allocator = testing.allocator;

            var streams: [streams_max][]u32 = undefined;

            const streams_buffer = try allocator.alloc(u32, streams_max * stream_key_count_max);
            defer allocator.free(streams_buffer);

            const expect_buffer = try allocator.alloc(Value, streams_max * stream_key_count_max);
            defer allocator.free(expect_buffer);

            var k: u32 = 0;
            while (k < streams_max) : (k += 1) {
                if (log) std.debug.print("k = {}\n", .{k});
                {
                    var i: u32 = 0;
                    while (i < k) : (i += 1) {
                        const len = fuzz_stream_len(prng, stream_key_count_max);
                        streams[i] = streams_buffer[i * stream_key_count_max ..][0..len];
                        fuzz_stream_keys(prng, streams[i]);

                        if (log) {
                            std.debug.print("stream {} = ", .{i});
                            for (streams[i]) |key| std.debug.print("{},", .{key});
                            std.debug.print("\n", .{});
                        }
                    }
                }

                var expect_buffer_len: usize = 0;
                for (streams[0..k], 0..) |stream, version| {
                    for (stream) |key| {
                        expect_buffer[expect_buffer_len] = .{
                            .key = key,
                            .version = @intCast(version),
                        };
                        expect_buffer_len += 1;
                    }
                }
                const expect_with_duplicates = expect_buffer[0..expect_buffer_len];
                std.mem.sort(Value, expect_with_duplicates, {}, value_less_than);

                var target: usize = 0;
                var previous_key: ?u32 = null;
                for (expect_with_duplicates) |value| {
                    if (previous_key) |p| {
                        if (value.key == p) continue;
                    }
                    previous_key = value.key;
                    expect_with_duplicates[target] = value;
                    target += 1;
                }
                const expect = expect_with_duplicates[0..target];

                if (log) {
                    std.debug.print("expect = ", .{});
                    for (expect) |value| std.debug.print("({},{}),", .{ value.key, value.version });
                    std.debug.print("\n", .{});
                }

                try merge(.ascending, streams[0..k], expect);

                for (streams[0..k]) |stream| mem.reverse(u32, stream);
                mem.reverse(Value, expect);

                try merge(.descending, streams[0..k], expect);

                if (log) std.debug.print("\n", .{});
            }
        }

        fn fuzz_stream_len(prng: *stdx.PRNG, stream_key_count_max: u32) u32 {
            const Len = enum { zero, max, random };
            return switch (prng.enum_weighted(Len, .{ .zero = 5, .max = 5, .random = 90 })) {
                .zero => 0,
                .max => stream_key_count_max,
                .random => prng.int_inclusive(u32, stream_key_count_max),
            };
        }

        fn fuzz_stream_keys(prng: *stdx.PRNG, stream: []u32) void {
            const key_max = prng.range_inclusive(u32, 512, 1023);
            const Key = enum { all_same, random };
            switch (prng.enum_weighted(Key, .{ .all_same = 5, .random = 95 })) {
                .all_same => {
                    @memset(stream, prng.int(u32));
                },
                .random => {
                    prng.fill(mem.sliceAsBytes(stream));
                },
            }
            for (stream) |*key| key.* = key.* % key_max;
            std.mem.sort(u32, stream, {}, key_less_than);
        }

        fn key_less_than(_: void, a: u32, b: u32) bool {
            return a < b;
        }

        fn value_less_than(_: void, a: Value, b: Value) bool {
            return switch (math.order(a.key, b.key)) {
                .lt => true,
                .eq => a.version > b.version,
                .gt => false,
            };
        }
    };
}

test "k_way_merge: unit" {
    try TestContextType(1).merge(
        .ascending,
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8 },
        },
        &[_]TestContextType(1).Value{
            .{ .key = 0, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
        },
    );
    try TestContextType(1).merge(
        .descending,
        &[_][]const u32{
            &[_]u32{ 8, 4, 3, 0 },
        },
        &[_]TestContextType(1).Value{
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 0, .version = 0 },
        },
    );
    try TestContextType(3).merge(
        .ascending,
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8, 11 },
            &[_]u32{ 2, 11, 12, 13, 15 },
            &[_]u32{ 1, 2, 11 },
        },
        &[_]TestContextType(3).Value{
            .{ .key = 0, .version = 0 },
            .{ .key = 1, .version = 2 },
            .{ .key = 2, .version = 2 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 11, .version = 2 },
            .{ .key = 12, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 15, .version = 1 },
        },
    );
    try TestContextType(3).merge(
        .descending,
        &[_][]const u32{
            &[_]u32{ 11, 8, 4, 3, 0 },
            &[_]u32{ 15, 13, 12, 11, 2 },
            &[_]u32{ 11, 2, 1 },
        },
        &[_]TestContextType(3).Value{
            .{ .key = 15, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 12, .version = 1 },
            .{ .key = 11, .version = 2 },
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 2, .version = 2 },
            .{ .key = 1, .version = 2 },
            .{ .key = 0, .version = 0 },
        },
    );
}

test "k_way_merge: fuzz" {
    const seed = std.crypto.random.int(u64);
    errdefer std.debug.print("\nTEST FAILED: seed = {}\n", .{seed});

    var prng = stdx.PRNG.from_seed(seed);
    try TestContextType(32).fuzz(&prng, 256);
}

// ---------------------------------------------------------------------------
//  D R A I N   C O N T E X T   (random quotas)
// ---------------------------------------------------------------------------
fn DrainedContextType(comptime streams_max: u32) type {
    return struct {
        const Self = @This();

        pub const Value = struct {
            key: u32,
            version: u32,
            inline fn to_key(v: *const Value) u32 {
                return v.key;
            }
        };

        // ----------------------------------------------------------------
        //  run‑time state
        // ----------------------------------------------------------------
        streams: [streams_max][]Value,
        pos: [streams_max]usize, // next index per stream
        quota: [streams_max]usize, // items remaining before Drained

        pub fn init(
            streams_in: [streams_max][]Value,
            // every stream starts with a *non‑zero* quota
            start_quota: usize,
        ) Self {
            assert(start_quota > 0);
            return .{
                .streams = streams_in,
                .pos = [_]usize{0} ** streams_max,
                .quota = [_]usize{start_quota} ** streams_max,
            };
        }

        // --------------------------------------------------------------
        //  Loser‑tree callbacks
        // --------------------------------------------------------------
        pub fn stream_peek(self: *const Self, idx: u32) error{ Empty, Drained }!u32 {
            const i: usize = @intCast(idx);

            if (self.pos[i] >= self.streams[i].len)
                return error.Empty;

            if (self.quota[i] == 0)
                return error.Drained;

            return self.streams[i][self.pos[i]].key;
        }

        pub fn stream_pop(self: *Self, idx: u32) Value {
            const i: usize = @intCast(idx);
            assert(self.quota[i] > 0); // invariant
            const v = self.streams[i][self.pos[i]];
            self.pos[i] += 1;
            self.quota[i] -= 1;
            return v;
        }

        pub fn stream_precedence(_: *const Self, a: u32, b: u32) bool {
            return a > b; // “higher index wins”
        }

        /// Re‑fill any exhausted quotas with **1–5** new tickets.
        pub fn refill_all(self: *Self, prng: *stdx.PRNG) void {
            inline for (0..streams_max) |i| {
                if (self.quota[i] == 0 and self.pos[i] < self.streams[i].len)
                    self.quota[i] =
                        prng.range_inclusive(usize, 1, 5);
            }
        }
    };
}

// ---------------------------------------------------------------------------
//  U N I T   T E S T   –   S I N G L E   R A N D O M   D R A I N
// ---------------------------------------------------------------------------
test "k_way_merge: drained basic (random quota)" {
    const N = 2;
    const Ctx = DrainedContextType(N);
    const Value = Ctx.Value;

    // Streams: [1,3,5] and [2,4,6]
    var s0 = [_]Value{ .{ .key = 1, .version = 0 }, .{ .key = 3, .version = 0 }, .{ .key = 5, .version = 0 } };
    var s1 = [_]Value{ .{ .key = 2, .version = 1 }, .{ .key = 4, .version = 1 }, .{ .key = 6, .version = 1 } };
    const streams = .{ &s0, &s1 };

    var dummy_prng = stdx.PRNG.from_seed(0xdead_beef);
    var ctx = Ctx.init(streams, 2); // start with quota = 2

    const KWay = KWayMergeIteratorType(
        Ctx,
        u32,
        Value,
        Value.to_key,
        N,
        Ctx.stream_peek,
        Ctx.stream_pop,
        Ctx.stream_precedence,
    );

    var it = KWay.init(&ctx, N, .ascending);

    var actual = std.ArrayList(Value).init(std.testing.allocator);
    defer actual.deinit();

    while (true) {
        const maybe_val = it.pop() catch |err| switch (err) {
            error.Drained => {
                ctx.refill_all(&dummy_prng);
                continue;
            },
        };
        if (maybe_val) |v| try actual.append(v) else break;
    }

    const expect = [_]Value{
        .{ .key = 1, .version = 0 },
        .{ .key = 2, .version = 1 },
        .{ .key = 3, .version = 0 },
        .{ .key = 4, .version = 1 },
        .{ .key = 5, .version = 0 },
        .{ .key = 6, .version = 1 },
    };
    try std.testing.expectEqualSlices(Value, &expect, actual.items);
}

// ---------------------------------------------------------------------------
//  F U Z Z   T E S T   –   R A N D O M   Q U O T A S
// ---------------------------------------------------------------------------
test "k_way_merge: drained fuzz (random quotas)" {
    const STREAMS_MAX = 32;
    const KEYS_PER_STREAM_MAX = 256;
    const Ctx = DrainedContextType(STREAMS_MAX);
    const Value = Ctx.Value;

    const seed = std.crypto.random.int(u64);
    errdefer std.debug.print("\nDRAIN‑FUZZ seed = {}\n", .{seed});

    var prng = stdx.PRNG.from_seed(seed);
    const alloc = std.testing.allocator;

    const streams_buf = try alloc.alloc(Value, STREAMS_MAX * KEYS_PER_STREAM_MAX);
    defer alloc.free(streams_buf);

    const expect_buf = try alloc.alloc(Value, STREAMS_MAX * KEYS_PER_STREAM_MAX);
    defer alloc.free(expect_buf);

    var k: u16 = 1;
    while (k <= STREAMS_MAX) : (k += 1) {
        var expect_len: usize = 0;

        var streams: [STREAMS_MAX][]Value = undefined;

        // ---------------- Build k random streams ------------------
        for (0..k) |s| {
            const len = prng.int_inclusive(u32, KEYS_PER_STREAM_MAX);
            const slice = streams_buf[s * KEYS_PER_STREAM_MAX ..][0..len];

            prng.fill(mem.sliceAsBytes(slice));
            const key_cap = prng.range_inclusive(u32, 256, 2047);

            for (slice) |*v| v.* = .{
                .key = v.key % key_cap,
                .version = @intCast(s),
            };

            mem.sort(Value, slice, {}, struct {
                fn less(_: void, a: Value, b: Value) bool {
                    return math.order(a.key, b.key) == .lt or
                        (a.key == b.key and a.version > b.version);
                }
            }.less);

            streams[s] = slice;

            // accumulate reference data
            for (slice) |v| {
                expect_buf[expect_len] = v;
                expect_len += 1;
            }
        }

        // --------------- Expected global order --------------------
        var expect_all = expect_buf[0..expect_len];
        mem.sort(Value, expect_all, {}, struct {
            fn less(_: void, a: Value, b: Value) bool {
                return math.order(a.key, b.key) == .lt or
                    (a.key == b.key and a.version < b.version);
            }
        }.less);

        var write_i: usize = 0;

        for (expect_buf[0..expect_len]) |item| {
            if (write_i != 0 and item.key == expect_buf[write_i - 1].key) {
                // same key as the one we just emitted → overwrite it
                expect_buf[write_i - 1] = item; // last value wins
            } else {
                // new key → append
                expect_buf[write_i] = item;
                write_i += 1;
            }
        }

        // new logical length of the slice
        expect_len = write_i;
        expect_all = expect_buf[0..expect_len];

        // --------------- Run the iterator -------------------------
        var ctx = Ctx.init(streams, 1); // quota starts at 1

        const KWay = KWayMergeIteratorType(
            Ctx,
            u32,
            Value,
            Value.to_key,
            STREAMS_MAX,
            Ctx.stream_peek,
            Ctx.stream_pop,
            Ctx.stream_precedence,
        );
        var it = KWay.init(&ctx, k, .ascending);

        var actual = std.ArrayList(Value).init(alloc);
        defer actual.deinit();

        while (true) {
            const maybe_val = it.pop() catch |err| switch (err) {
                error.Drained => {
                    ctx.refill_all(&prng);
                    continue;
                },
            };
            if (maybe_val) |v| try actual.append(v) else break;
        }
        try std.testing.expectEqualSlices(Value, expect_all, actual.items);
    }
}
