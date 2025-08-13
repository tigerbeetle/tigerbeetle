const std = @import("std");
const stdx = @import("stdx");
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
    comptime sorted_runs_max: u32,
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) error{ Empty, Drained }!Key,
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
    comptime stream_precedence: fn (context: *const Context, a: u32, b: u32) bool,
    comptime allow_duplicates: bool,
) type {
    // Tree of losers is binary tree, so requires K to be power two.
    comptime assert(sorted_runs_max >= 2);

    const stream_max: u32 = std.math.ceilPowerOfTwoAssert(u32, sorted_runs_max);
    //comptime assert(std.math.isPowerOfTwo(sorted_runs_max));

    return struct {

        // TODO: benchmark in which cases it is better, e.g., if there is implicit padding?
        pub fn swap_struct(comptime T: type, a: *T, b: *T) void {
            inline for (std.meta.fields(T)) |f| {
                {
                    const tmp_field = @field(a, f.name);
                    @field(a, f.name) = @field(b, f.name);
                    @field(b, f.name) = tmp_field;
                }
            }
        }

        const Self = @This();

        // TODO: optimize sentinel encoding and handling with invalid sorted_run_id.
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

            // TODO: handle drained
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

        inline fn choose(has_new_winner: bool, parent_ptr: *Node, winner: *Node) *Node {
            // Note: The layout of the code coaxes the compiler to generate branchless code.
            //       Best do not change it without verifying the generated code.
            var p = winner;
            if (has_new_winner) {
                @branchHint(.unpredictable); // attaches to this branch
                p = parent_ptr;
            }
            return p;
        }

        fn next(self: *Self) error{Drained}!?Value {
            if (self.challenger.sentinel) { // just became dry
                return null;
            }

            const run_id = self.challenger.sorted_run_id;
            assert(!self.challenger.sentinel);
            assert(run_id < std.math.maxInt(u8));
            // TODO: think about encoding here for run_id

            self.challenger = try self.next_challenger(run_id);

            const ascending = if (self.direction == .ascending) true else false;

            // TODO: implement and test fast path

            // Start at our own id and find the first challenger in the loop.
            var opponent_id = (self.nodes_loser.len + run_id);
            for (0..self.height) |_| {
                opponent_id = (opponent_id - 1) >> 1;
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
            const KWay = KWayMergeIteratorType(
                TestContext,
                u32,
                Value,
                Value.to_key,
                streams_max,
                stream_peek,
                stream_pop,
                stream_precedence,
                false,
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
