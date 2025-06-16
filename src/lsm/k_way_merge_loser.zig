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
        streams_count: u16,
        active_streams: u16,
        challenger: Node,
        nodes_loser: [node_count_max]Node = undefined,
        state: enum { loading, iterating },

        direction: Direction,
        key_popped: ?Key,

        // NOTE:
        // - we could programatically find out the sentinel and choose values that are not usable.
        // - this would allow to make it work.
        // - note
        // create sentinel

        sentinel: Node,

        pub fn init(
            context: *Context,
            streams_count: u16,
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
                    .sorted_run_id = std.math.maxInt(u8), // NOTE: what should we do here?
                    .sentinel = true,
                },
            };

            return .{
                .context = context,
                .height = 0,
                .node_count = 0,
                .challenger = sentinel,
                .nodes_loser = undefined,
                .key_popped = null,
                .direction = direction,
                .active_streams = undefined,
                .sentinel = sentinel,
                .streams_count = streams_count,
                .state = .loading,
            };
        }
        pub fn empty(self: *Self) bool {
            return self.active_streams == 0;
        }

        pub fn reset(self: *Self) void {
            self.* = .{
                .context = self.context,
                .height = 0,
                .node_count = 0,
                .challenger = self.sentinel,
                .nodes_loser = undefined,
                .direction = self.direction,
                .active_streams = self.active_streams,
                .sentinel = self.sentinel,
                .streams_count = self.streams_count,
                .state = .loading,
                .key_popped = self.key_popped,
            };
        }

        fn loading(
            self: *Self,
        ) error{Drained}!void {
            errdefer self.reset();

            // 1. Collect the non‑empty batches as initial “competitors”.
            var competitors: [stream_max]Node = undefined;
            var competitors_count: u16 = 0;
            for (0..self.streams_count) |id| {
                const key = stream_peek(self.context, @intCast(id)) catch |err| switch (err) {
                    error.Drained => {
                        return error.Drained;
                    },
                    error.Empty => {
                        competitors[id] = self.sentinel;
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
                self.state = .iterating;
                return;
            }

            // 2. Round up to the next power‑of‑two so the loser tree is perfectly balanced.
            // TODO: BUG FIX here if some streams are empty we do not correctly drain them with this logc!
            const leaf_capacity = std.math.ceilPowerOfTwo(u16, self.streams_count) catch unreachable;
            const height = std.math.log2(leaf_capacity);
            const node_count: u16 = leaf_capacity - 1; // k leaves ⇒ k‑1 internal nodes.

            // 3. Pad unused leaf slots with the sentinel so comparisons stay trivial.
            for (self.streams_count..stream_max) |i| competitors[i] = self.sentinel;
            for (0..node_count_max) |i| self.nodes_loser[i] = self.sentinel;

            const ascending = if (self.direction == .ascending) true else false;
            // 4. Build the tree of losers bottom‑up.
            for (1..height + 1) |level| {
                const level_min = (stream_max / (@as(usize, 1) << @as(u6, @intCast(level)))) - 1;
                const level_max = (stream_max / (@as(usize, 1) << @as(u6, @intCast(level - 1)))) - 1;

                for (level_min..level_max, 0..) |loser_index, competitor_index| {
                    const competitor_a = competitors[competitor_index * 2];
                    const competitor_b = competitors[competitor_index * 2 + 1];
                    // TODO: do ordering here.
                    // TODO: check sentinel
                    //if (ordered(ascending, context, &competitor_a, &competitor_b)) {
                    if (ordered(ascending, self.context, &competitor_b, &competitor_a)) {
                        competitors[competitor_index] = competitor_a;
                        self.nodes_loser[loser_index] = competitor_b;
                    } else {
                        competitors[competitor_index] = competitor_b;
                        self.nodes_loser[loser_index] = competitor_a;
                    }
                }
            }

            self.challenger = competitors[0];
            self.node_count = node_count;
            self.active_streams = competitors_count;
            self.height = height;
            self.key_popped = self.key_popped;
            self.state = .iterating;
        }

        // TODO: can we handle deduplication here.
        //       Think about this tomorrow.
        fn next_challenger(self: *Self, run_id: u32) error{Drained}!Node {
            if (run_id == std.math.maxInt(u8)) {
                return self.sentinel;
            }
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

        pub fn pop(self: *Self) error{Drained}!?Value {
            if (self.state == .loading) try self.loading();
            assert(self.state == .iterating);
            // handle duplicates here.
            while (self.active_streams > 0) {
                if (try self.next()) |value| {
                    if (allow_duplicates) {} else {
                        if (self.key_popped) |previous| {
                            if (key_from_value(&value) == previous) {
                                continue;
                            }
                        }
                    }
                    self.key_popped = key_from_value(&value);
                    return value;
                } else return null;
            }
            return null;
        }

        inline fn ordered(ascending: bool, ctx: *const Context, a: *const Node, b: *const Node) bool {
            // a sentinel always loses
            if (a.sentinel) return true;
            if (b.sentinel) return false;

            //const smaller = ascending ? b.key < a.key : b.key > a.key;
            const smaller = if (ascending) b.key < a.key else b.key > a.key;
            const stabler = (a.key == b.key) and
                stream_precedence(ctx, b.sorted_run_id, a.sorted_run_id);
            return smaller or stabler; // “true”  means  a  loses
        }

        fn choose(has_new_winner: bool, parent_ptr: *Node, winner: *Node) *Node {
            // value-level `if` – no side effects inside the branches
            return if (has_new_winner) parent_ptr else winner;
        }
        fn next(self: *Self) error{Drained}!?Value {
            if (self.challenger.sentinel) { // just became dry
                return null;
            }

            const run_id = self.challenger.sorted_run_id;
            assert(!self.challenger.sentinel);
            assert(run_id < std.math.maxInt(u8));

            self.challenger = try self.next_challenger(run_id);

            const ascending = if (self.direction == .ascending) true else false;

            // Start at our own id and find the first challenger in the loop.
            var opponent_id = (self.nodes_loser.len + run_id);

            // Leaf to root traversal to find the champion.
            // TODO: build turth table
            for (0..self.height) |_| {
                opponent_id = (opponent_id - 1) / 2;
                const opponent = &self.nodes_loser[opponent_id];
                const new_challenger = ordered(ascending, self.context, &self.challenger, opponent);
                const winner: *Node = choose(new_challenger, opponent, &self.challenger);
                swap_struct(Node, winner, &self.challenger);
            }

            if (self.challenger.sentinel) {
                return null;
            }

            return stream_pop(self.context, self.challenger.sorted_run_id);
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
    //if (true) return error.SkipZigTest;
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
    //if (true) return error.SkipZigTest;
    const N = 2;
    const Ctx = DrainedContextType(N);
    const Value = Ctx.Value;

    // Streams: [1,3,5] and [2,4,6]
    var s0 = [_]Value{ .{ .key = 1, .version = 0 }, .{ .key = 3, .version = 0 }, .{ .key = 5, .version = 0 } };
    var s1 = [_]Value{ .{ .key = 2, .version = 1 }, .{ .key = 4, .version = 1 }, .{ .key = 6, .version = 1 } };
    const streams = .{ &s0, &s1 };

    var dummy_prng = stdx.PRNG.from_seed(0xdead_beef);
    var ctx = Ctx.init(streams, 2); // start with quota = 2

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
    //if (true) return error.SkipZigTest;
    const STREAMS_MAX = 32;
    const KEYS_PER_STREAM_MAX = 256;
    const Ctx = DrainedContextType(STREAMS_MAX);
    const Value = Ctx.Value;

    const seed = std.crypto.random.int(u64);
    std.debug.print("darined fuzzer {} \n", .{seed});
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
            if (len == 0) {
                std.debug.print("k {} len {}", .{ k, len });
            }
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

        const KWay = LoserTreeTypeIterator(
            Ctx,
            u32,
            Value,
            STREAMS_MAX,
            false,
            Value.to_key,
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
test "k_way_merge: drained fuzz (random quotas, descending)" {
    //if (true) return error.SkipZigTest;
    const STREAMS_MAX = 9;
    const KEYS_PER_STREAM_MAX = 10;
    const Ctx = DrainedContextType(STREAMS_MAX);
    const Value = Ctx.Value;

    const seed = std.crypto.random.int(u64);
    std.debug.print("darined fuzzer {} \n", .{seed});
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
            if (len == 0) {
                std.debug.print("s {} len {}\n", .{ s, len });
            }
            const slice = streams_buf[s * KEYS_PER_STREAM_MAX ..][0..len];

            prng.fill(mem.sliceAsBytes(slice));
            const key_cap = prng.range_inclusive(u32, 256, 2047);

            for (slice) |*v| v.* = .{
                .key = v.key % key_cap,
                .version = @intCast(s),
            };

            mem.sort(Value, slice, {}, struct {
                fn greater(_: void, a: Value, b: Value) bool {
                    return math.order(a.key, b.key) == .gt or
                        (a.key == b.key and a.version > b.version);
                }
            }.greater);

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
            fn greater(_: void, a: Value, b: Value) bool {
                return math.order(a.key, b.key) == .gt or
                    (a.key == b.key and a.version < b.version);
            }
        }.greater);

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

        const KWay = LoserTreeTypeIterator(
            Ctx,
            u32,
            Value,
            STREAMS_MAX,
            false,
            Value.to_key,
            Ctx.stream_peek,
            Ctx.stream_pop,
            Ctx.stream_precedence,
        );
        var it = KWay.init(&ctx, k, .descending);

        var actual = std.ArrayList(Value).init(alloc);
        defer actual.deinit();

        while (true) {
            // sometimes call reset
            it.reset();
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
