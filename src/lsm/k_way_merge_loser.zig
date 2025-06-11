const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const math = std.math;
const mem = std.mem;

const Direction = @import("../direction.zig").Direction;

pub fn LoserTreeTypeIterator(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime sorted_runs_max: u32,
    comptime allow_duplicates: bool,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) error{ Empty, Drained }!Key,
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
    // TODO: Precedence
    comptime stream_precedence: fn (context: *const Context, a: u32, b: u32) bool,
) type {
    // Tree of losers is binary tree, so requires K to be power two.
    comptime assert(sorted_runs_max >= 2);

    const stream_max: u32 = std.math.ceilPowerOfTwoAssert(u32, sorted_runs_max);
    //comptime assert(std.math.isPowerOfTwo(sorted_runs_max));

    return struct {

        // TODO: benchmark in which cases it is better, e.g., if there is implicit padding?
        pub inline fn swap_struct(comptime T: type, a: *T, b: *T) void {
            inline for (std.meta.fields(T)) |f| {
                {
                    const tmp_field = @field(a, f.name);
                    @field(a, f.name) = @field(b, f.name);
                    @field(b, f.name) = tmp_field;
                }
            }
        }

        const Self = @This();

        const Node = struct {
            key: Key,
            sorted_run_id: u32,
            sentinel: bool = false,
        };

        const node_count_max = 2 * (stream_max / 2) - 1;

        context: *Context,
        height: u16,
        node_count: u16,
        active_streams: u16,
        challenger: Node,
        nodes_loser: [node_count_max]Node = undefined,

        direction: Direction,
        value_popped: ?Value,
        key_popped: ?Key,

        // TODO Think about this.
        // Use `maxInt(Key)` + maxInt(run_id) to mark empty runs without affecting merge.
        // what if direction is smaller shit that depends on the precedence the sentinel does not work?!
        // TODO: THink about sentinel values more with two directiosn.

        // in the ideal world sentinel would be
        // an unreachable key that compares correctly.

        //const sentinel = .{
        //.key = std.math.maxInt(Key),
        //.sorted_run_id = std.math.maxInt(u8),
        //};

        sentinel: Node,

        fn init(
            context: *Context,
            stream_count: u16,
            direction: Direction,
        ) Self {
            const sentinel: Node = switch (direction) {
                .ascending => .{
                    .key = std.math.maxInt(Key),
                    .sorted_run_id = std.math.maxInt(u8),
                    .sentinel = true,
                },
                .descending => .{
                    .key = std.math.minInt(Key),
                    .sorted_run_id = std.math.maxInt(u8),
                    .sentinel = true,
                },
            };
            // 1. Collect the non‑empty batches as initial “competitors”.
            var competitors: [stream_max]Node = undefined;
            var competitors_count: u16 = 0;
            for (0..stream_count) |id| {
                const key = stream_peek(context, @intCast(id)) catch |err| switch (err) {
                    error.Drained => @panic("not expected debug"),
                    error.Empty => {
                        competitors[id] = sentinel;
                        continue;
                    },
                };
                competitors[id] = .{
                    .key = key,
                    .sorted_run_id = @intCast(id),
                };
                competitors_count += 1;
            }
            if (competitors_count == 0) {
                return .{
                    .context = context,
                    .height = 0,
                    .node_count = 0,
                    .challenger = undefined,
                    .nodes_loser = undefined,
                    .value_popped = null,
                    .key_popped = null,
                    .direction = direction,
                    .active_streams = 0,
                    .sentinel = sentinel,
                };
            }

            // 2. Round up to the next power‑of‑two so the loser tree is perfectly balanced.
            // TODO: BUG FIX here if some streams are empty we do not correctly drain them with this logc!
            const leaf_capacity = std.math.ceilPowerOfTwo(u16, stream_count) catch unreachable;
            const height = std.math.log2(leaf_capacity);
            const node_count: u16 = leaf_capacity - 1; // k leaves ⇒ k‑1 internal nodes.

            // 3. Pad unused leaf slots with the sentinel so comparisons stay trivial.
            for (stream_count..stream_max) |i| competitors[i] = sentinel;

            const ascending = if (direction == .ascending) true else false;
            // 4. Build the tree of losers bottom‑up.
            var nodes_loser: [node_count_max]Node = undefined;
            for (1..height + 1) |level| {
                const level_min = (stream_max / (@as(usize, 1) << @as(u6, @intCast(level)))) - 1;
                const level_max = (stream_max / (@as(usize, 1) << @as(u6, @intCast(level - 1)))) - 1;

                for (level_min..level_max, 0..) |loser_index, competitor_index| {
                    const competitor_a = competitors[competitor_index * 2];
                    const competitor_b = competitors[competitor_index * 2 + 1];
                    // TODO: do ordering here.
                    // TODO: check sentinel
                    //if (ordered(ascending, context, &competitor_a, &competitor_b)) {
                    if (ordered(ascending, context, &competitor_b, &competitor_a)) {
                        competitors[competitor_index] = competitor_a;
                        nodes_loser[loser_index] = competitor_b;
                    } else {
                        competitors[competitor_index] = competitor_b;
                        nodes_loser[loser_index] = competitor_a;
                    }
                }
            }

            const value_winner = stream_pop(context, competitors[0].sorted_run_id);

            return .{
                .context = context,
                .height = height,
                .node_count = node_count,
                .challenger = competitors[0],
                .nodes_loser = nodes_loser,
                .value_popped = value_winner,
                .key_popped = null,
                .direction = direction,
                .active_streams = competitors_count,
                .sentinel = sentinel,
            };
        }

        // TODO: Test:  How to handle 0 set special case it in the build phase

        // TODO: can we handle deduplication here.
        //       Think about this tomorrow.
        fn next_challenger(self: *Self, run_id: u32) error{Drained}!Node {
            assert(run_id < sorted_runs_max);
            if (stream_peek(self.context, run_id)) |key| {
                // since this we have precedence it is fine.
                return .{
                    .key = key,
                    .sorted_run_id = run_id,
                };
            } else |err| switch (err) {
                error.Drained => {
                    return error.Drained;
                },
                error.Empty => {
                    self.active_streams -= 1;
                    return self.sentinel;
                },
            }
            return self.sentinel;
        }

        fn pop(self: *Self) error{Drained}!?Value {
            // handle duplicates here.
            while (self.active_streams > 0) {
                if (try self.next()) |value| {
                    if (allow_duplicates) {} else {
                        if (key_from_value(&value) == self.key_popped) {
                            continue;
                        }
                    }
                    self.key_popped = key_from_value(&value);
                    return value;
                } else return null;
            }
            return null;
        }

        inline fn sentinel_check(
            node_a: *const Node,
            node_b: *const Node,
        ) bool {
            return node_a.sentinel or !node_b.sentinel;
        }
        // important to inline
        // very hot loop
        inline fn ordered(
            ascending: bool,
            context: *const Context,
            node_a: *const Node,
            node_b: *const Node,
        ) bool {
            const smaller = if (ascending) node_b.key < node_a.key else node_b.key > node_a.key;

            // When is it stabler
            const stabler =
                node_a.key == node_b.key and
                // this should allow to have a special sorted run.
                // we should make sure that we do not have run count of max int.
                (node_a.sorted_run_id == std.math.maxInt(u8) or
                stream_precedence(
                context,
                node_b.sorted_run_id,
                node_a.sorted_run_id,
            ));
            return smaller or stabler;
        }

        fn next(self: *Self) error{Drained}!?Value {
            // TODO: what if I load here the first value and return the winner directly then we do not need to save it right?
            // Batch id of the previous winner, from here we need to get the next challenger.
            if (self.value_popped == null) return null;

            const run_id = self.challenger.sorted_run_id;

            self.challenger = try self.next_challenger(run_id);
            const previous_winner = self.value_popped orelse return null;

            const ascending = if (self.direction == .ascending) true else false;

            // Start at our own id and find the first challenger in the loop.
            var opponent_id = (self.nodes_loser.len + run_id);

            // Leaf to root traversal to find the champion.
            // TODO: build turth table
            for (0..self.height) |_| {
                opponent_id = (opponent_id - 1) / 2;
                const opponent = &self.nodes_loser[opponent_id];

                const new_challenger = ordered(ascending, self.context, &self.challenger, opponent);
                const sentinel = sentinel_check(&self.challenger, opponent);

                // Code structured in a way so that `cmov` generation is likely.
                const winner: *Node = if (sentinel and new_challenger) opponent else &self.challenger;
                swap_struct(Node, winner, &self.challenger);
            }

            // Todo: we could use active streams
            if (self.challenger.sentinel) {
                self.value_popped = null;
            } else {
                self.value_popped = stream_pop(self.context, self.challenger.sorted_run_id);
            }
            return previous_winner;
        }
    };
}

fn TestContextType(comptime streams_max: u32) type {
    const testing = std.testing;

    return struct {
        const TestContext = @This();

        const log = true;

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
            const KWay = LoserTreeTypeIterator(
                TestContext,
                u32,
                Value,
                streams_max,
                false,
                Value.to_key,
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

            var k: u32 = 1;
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

                std.debug.print("ascending \n", .{});
                try merge(.ascending, streams[0..k], expect);

                for (streams[0..k]) |stream| mem.reverse(u32, stream);
                mem.reverse(Value, expect);

                std.debug.print("descending\n", .{});
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
    //try TestContextType(1).merge(
    //.ascending,
    //&[_][]const u32{
    //&[_]u32{ 0, 3, 4, 8 },
    //},
    //&[_]TestContextType(1).Value{
    //.{ .key = 0, .version = 0 },
    //.{ .key = 3, .version = 0 },
    //.{ .key = 4, .version = 0 },
    //.{ .key = 8, .version = 0 },
    //},
    //);
    //try TestContextType(1).merge(
    //.descending,
    //&[_][]const u32{
    //&[_]u32{ 8, 4, 3, 0 },
    //},
    //&[_]TestContextType(1).Value{
    //.{ .key = 8, .version = 0 },
    //.{ .key = 4, .version = 0 },
    //.{ .key = 3, .version = 0 },
    //.{ .key = 0, .version = 0 },
    //},
    //);
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

/// ---------------------------------------------------------------------------
///  DR A I N   S U P P O R T   T E S T S
/// ---------------------------------------------------------------------------
/// A minimal test‑only context that can make any stream raise `error.Drained`
/// at pre‑defined item indices (the *drain markers*).
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

        // --------------------------------------------------------------------
        //  runtime state
        // --------------------------------------------------------------------
        streams: [streams_max][]Value, // full data per stream
        pos: [streams_max]usize, // next element to read
        drains: [streams_max][]const usize, // sorted drain markers
        drain_idx: [streams_max]usize, // current drain marker per stream

        // Convenience constructor used by the tests below.
        pub fn init(
            streams_in: [streams_max][]Value,
            drains_in: [streams_max][]const usize,
        ) Self {
            return .{
                .streams = streams_in,
                .pos = [_]usize{0} ** streams_max,
                .drains = drains_in,
                .drain_idx = [_]usize{0} ** streams_max,
            };
        }

        // --------------------------------------------------------------------
        //  Loser‑tree callbacks
        // --------------------------------------------------------------------
        pub fn stream_peek(self: *const Self, idx: u32) error{ Empty, Drained }!u32 {
            const i = idx;

            // 1. No more data at all.
            if (self.pos[i] >= self.streams[i].len)
                return error.Empty;

            // 2. Need refill at the current drain marker.
            if (self.drain_idx[i] < self.drains[i].len and
                self.pos[i] == self.drains[i][self.drain_idx[i]])
                return error.Drained;

            // 3. Normal case – return next key.
            return self.streams[i][self.pos[i]].key;
        }

        pub fn stream_pop(self: *Self, idx: u32) Value {
            const i = idx;
            const v = self.streams[i][self.pos[i]];
            self.pos[i] += 1;
            return v;
        }

        /// Policy: higher stream index ⇒ higher precedence when keys are equal.
        pub fn stream_precedence(_: *const Self, a: u32, b: u32) bool {
            return a > b;
        }

        /// Advance ‟the hose” on a single stream after we handled `Drained`.
        pub fn resume_stream(self: *Self, idx: u32) void {
            const i = idx;
            if (self.drain_idx[i] < self.drains[i].len and
                self.pos[i] >= self.drains[i][self.drain_idx[i]])
                self.drain_idx[i] += 1;
        }

        /// Convenience: resume *all* streams.
        pub fn resume_all(self: *Self) void {
            inline for (0..streams_max) |s|
                self.resume_stream(s);
        }
    };
}

// ---------------------------------------------------------------------------
//  U N I T   T E S T   –   S I N G L E   D R A I N
// ---------------------------------------------------------------------------
test "k_way_merge: drained basic" {
    const testing = std.testing;
    const N = 2; // streams
    const Ctx = DrainedContextType(N);
    const Value = Ctx.Value;
    const allocator = testing.allocator;

    // --------------------------------------------------------------------
    // Stream 0   1,3 ▒drain▒ 5
    // Stream 1   2,4,6        (never drains)
    // --------------------------------------------------------------------
    //const s0 = try allocator.alloc(Value, 3);
    //const s1 = try allocator.alloc(Value, 3);
    //defer {
    //allocator.free(s0);
    //allocator.free(s1);
    //}

    const s0 = [_]Value{ .{ .key = 1, .version = 0 }, .{ .key = 3, .version = 0 }, .{ .key = 5, .version = 0 } };
    const s1 = [_]Value{ .{ .key = 2, .version = 1 }, .{ .key = 4, .version = 1 }, .{ .key = 6, .version = 1 } };

    const streams: [N][]Value = .{ &s0, &s1 };
    const drains: [N][]const usize = .{ &[_]usize{2}, &[_]usize{} }; // drain after 2 items in stream‑0

    var ctx = Ctx.init(streams, drains);

    const KWay = LoserTreeTypeIterator(
        Ctx,
        u32,
        Value,
        N,
        false,
        Value.to_key,
        Ctx.stream_peek,
        Ctx.stream_pop,
        Ctx.stream_precedence,
    );
    var kway = KWay.init(&ctx, N, .ascending);

    var actual = std.ArrayList(Value).init(allocator);
    defer actual.deinit();

    while (true) {
        const maybe_value = kway.pop() catch |err| switch (err) {
            error.Drained => { // refill stage
                ctx.resume_all();
                continue;
            },
        };
        if (maybe_value) |v|
            try actual.append(v)
        else
            break;
    }

    const expect = [_]Value{
        .{ .key = 1, .version = 0 },
        .{ .key = 2, .version = 1 },
        .{ .key = 3, .version = 0 },
        .{ .key = 4, .version = 1 },
        .{ .key = 5, .version = 0 },
        .{ .key = 6, .version = 1 },
    };
    try testing.expectEqualSlices(Value, &expect, actual.items);
}

// ---------------------------------------------------------------------------
//  F U Z Z E R   –   R A N D O M   D R A I N   P O I N T S
// ---------------------------------------------------------------------------
test "k_way_merge: drained fuzz" {
    const STREAMS_MAX = 32;
    const KEYS_PER_STREAM_MAX = 256;
    const Ctx = DrainedContextType(STREAMS_MAX);
    const Value = Ctx.Value;

    const seed = std.crypto.random.int(u64);
    errdefer std.debug.print("\nDRAIN‑FUZZ seed = {}\n", .{seed});

    var prng = stdx.PRNG.from_seed(seed);
    const alloc = std.testing.allocator;

    var streams: [STREAMS_MAX][]Value = undefined;
    var drains: [STREAMS_MAX][]usize = undefined;

    // scratch buffers
    const streams_buf = try alloc.alloc(Value, STREAMS_MAX * KEYS_PER_STREAM_MAX);
    defer alloc.free(streams_buf);

    const drains_buf = try alloc.alloc(usize, STREAMS_MAX * 8);
    defer alloc.free(drains_buf);

    const expect_buf = try alloc.alloc(Value, STREAMS_MAX * KEYS_PER_STREAM_MAX);
    defer alloc.free(expect_buf);

    var k: u32 = 1;
    while (k <= STREAMS_MAX) : (k += 1) {
        // ------------------------------------------------------------
        // Build *k* random streams with random drain markers.
        // ------------------------------------------------------------
        var drains_cursor: usize = 0;
        var expect_len: usize = 0;

        for (0..k) |s| {
            const len = prng.int_inclusive(u32, KEYS_PER_STREAM_MAX);
            const slice = streams_buf[s * KEYS_PER_STREAM_MAX ..][0..len];

            // Fill with random keys, then sort ascending.
            prng.fill(mem.sliceAsBytes(slice));
            const key_cap = prng.range_inclusive(u32, 256, 2047);
            for (slice) |*v| v.* = .{
                .key = v.key % key_cap,
                .version = @intCast(s),
            };
            mem.sort(Value, slice, {}, struct {
                fn less(_: void, a: Value, b: Value) bool {
                    return math.order(a.key, b.key) == .lt or
                        (a.key == b.key and a.version > b.version); // precedence
                }
            }.less);

            streams[s] = slice;

            // Randomly decide how many drains for this stream (0–3) and positions.
            const drains_for_stream = prng.int_inclusive(u32, 3);
            const drains_slice = drains_buf[drains_cursor..][0..drains_for_stream];
            drains_cursor += drains_for_stream;
            for (drains_slice, 0..) |*pos, d| {
                // choose unique, increasing positions (never the final element)
                pos.* = @min(len - 1, prng.range_inclusive(usize, d, len - 1));
            }
            mem.sort(usize, drains_slice, {}, struct {
                fn less(_: void, a: usize, b: usize) bool {
                    return a < b;
                }
            }.less);
            drains[s] = drains_slice;

            // Add to reference buffer.
            for (slice) |v| {
                expect_buf[expect_len] = v;
                expect_len += 1;
            }
        }

        // ------------------------------------------------------------
        // Reference result (ascending, duplicates kept).
        // ------------------------------------------------------------
        const expect_all = expect_buf[0..expect_len];
        mem.sort(Value, expect_all, {}, struct {
            fn less(_: void, a: Value, b: Value) bool {
                return math.order(a.key, b.key) == .lt or
                    (a.key == b.key and a.version > b.version);
            }
        }.less);

        // ------------------------------------------------------------
        // Run the iterator with drains enabled.
        // ------------------------------------------------------------
        var ctx = Ctx.init(streams, drains);

        const KWay = LoserTreeTypeIterator(
            Ctx,
            u32,
            Value,
            STREAMS_MAX,
            true, // duplicates allowed
            Value.to_key,
            Ctx.stream_peek,
            Ctx.stream_pop,
            Ctx.stream_precedence,
        );
        var it = KWay.init(&ctx, k, .ascending);

        var actual = std.ArrayList(Value).init(alloc);
        defer actual.deinit();

        while (true) {
            const maybe_value = it.pop() catch |err| switch (err) {
                error.Drained => {
                    ctx.resume_all();
                    continue;
                },
            };
            if (maybe_value) |v|
                try actual.append(v)
            else
                break;
        }
        try std.testing.expectEqualSlices(Value, expect_all, actual.items);
    }
}
