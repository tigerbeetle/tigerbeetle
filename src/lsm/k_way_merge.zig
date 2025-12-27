//! K-way merge via a loser tree algorithm (Knuth Volume 3 p. 253).
//! Merges k sorted streams using a tournament (loser) tree.
//! The current global winner lives in `contender`. Internal nodes in `losers`
//! store the losers of the last comparisons along the root-to-leaf paths.
//!     0 (winner, contender)
//!
//!     1
//!    / \
//!   2   3
//!  / \ / \
//! 4  5 6  7
//! -------------
//! K input streams
//!
//! The internal nodes are organized in a flat Eytzinger layout.
//! That is the tree above is stored as [1][2][3][4][5][6][7].
//! Empty streams are represented with a sentinel node that always loses against real nodes.
//!
//! There are also a few optimizations that seem to be helpful, but did not work, such as:
//! - Only store stream_id in the inner nodes, and have a heads array for the first keys
//!   of the streams. This reduces space (densely packed) and the bytes in swap.
//!   Unfortunately, it made the code much slower.
//!
const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const math = std.math;
const mem = std.mem;

const Direction = @import("../direction.zig").Direction;
const Pending = error{Pending};

const Options = struct {
    streams_max: u32,
    deduplicate: bool,
};

pub fn KWayMergeIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime options: Options,
    comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
    /// Peek the next key in the stream identified by stream_index.
    /// For example, peek(stream_index=2) returns user_streams[2][0].
    /// Returns Pending if the stream was consumed and
    /// must be refilled before calling peek() again.
    /// Returns null if the stream was fully consumed and reached the end.
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) Pending!?Key,
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
) type {
    comptime assert(options.streams_max >= 1);
    comptime assert(options.streams_max < std.math.maxInt(@TypeOf(options.streams_max)));

    return struct {
        const KWayMergeIterator = @This();

        const node_max: u32 = std.math.ceilPowerOfTwoAssert(u32, options.streams_max);
        const stream_id_invalid = std.math.maxInt(@TypeOf(options.streams_max));
        const sentinel: Node = .{
            // The key itself is not used to determine whether a Node is a sentinel.
            // This ensures that valid Keys with the maximum value are not treated as sentinels.
            .key = std.math.maxInt(Key),
            .stream_id = stream_id_invalid,
            .sentinel = true,
        };

        state: enum { loading, iterating },
        tree_height: u16,
        nodes_count: u16,
        streams_count: u16,
        streams_active: u16,
        context: *Context,
        contender: Node,
        losers: [node_max]Node,
        direction: Direction,
        key_popped: ?Key,

        const Node = struct {
            key: Key,
            stream_id: u32,
            sentinel: bool,

            // Returns true iff `a` wins over `b` under `direction`.
            // If keys are equal, smaller stream_id wins.
            inline fn beats(
                a: *const Node,
                b: *const Node,
                direction: Direction,
            ) bool {
                // A sentinel always loses.
                if (b.sentinel) return true;
                if (a.sentinel) return false;

                const ordered = if (direction == .ascending) a.key < b.key else a.key > b.key;
                const stabler = (a.key == b.key) and (a.stream_id < b.stream_id);
                return ordered or stabler; // “true”  means  a wins.
            }
        };

        pub fn init(
            context: *Context,
            streams_count: u16,
            direction: Direction,
        ) KWayMergeIterator {
            assert(streams_count <= options.streams_max);
            // Streams ZERO can be used to represent empty sets.
            maybe(streams_count == 0);

            return .{
                .context = context,
                .tree_height = 0,
                .nodes_count = 0,
                .contender = sentinel,
                .losers = @splat(sentinel),
                .key_popped = null,
                .direction = direction,
                .streams_active = 0,
                .streams_count = streams_count,
                .state = .loading,
            };
        }

        pub fn reset(self: *KWayMergeIterator) void {
            self.* = .{
                .context = self.context,
                .tree_height = 0,
                .nodes_count = 0,
                .contender = sentinel,
                .losers = @splat(sentinel),
                .direction = self.direction,
                .streams_active = self.streams_active,
                .streams_count = self.streams_count,
                .state = .loading,
                .key_popped = self.key_popped,
            };
        }

        fn load(self: *KWayMergeIterator) Pending!void {
            assert(self.state == .loading);
            assert(self.nodes_count == 0);
            assert(self.tree_height == 0);
            errdefer self.reset();

            // Collect the non‑empty batches as initial “contestants”.
            var contestants: [node_max]Node = @splat(sentinel);
            var contestants_count: u16 = 0;
            for (0..self.streams_count) |id| {
                const key = try stream_peek(self.context, @intCast(id)) orelse continue;
                contestants[id] = .{ .key = key, .stream_id = @intCast(id), .sentinel = false };
                contestants_count += 1;
            }

            if (contestants_count == 0) {
                self.streams_active = 0;
                self.contender = sentinel;
                self.state = .iterating;
                return;
            }

            // Calculate the shape of the binary tree.
            const leafs_count = std.math.ceilPowerOfTwo(u16, self.streams_count) catch unreachable;
            const tree_height = std.math.log2(leafs_count);
            const nodes_count: u16 = leafs_count - 1;

            // Construct the binary tree bottom up.
            for (0..tree_height) |level| {
                const level_min = (leafs_count >> @as(u4, @intCast(level + 1))) - 1;
                const level_max = (leafs_count >> @as(u4, @intCast(level))) - 1;

                for (level_min..level_max, 0..) |loser_index, competitor_index| {
                    const competitor_a = contestants[competitor_index * 2];
                    const competitor_b = contestants[competitor_index * 2 + 1];

                    if (competitor_a.beats(&competitor_b, self.direction)) {
                        contestants[competitor_index] = competitor_a;
                        self.losers[loser_index] = competitor_b;
                    } else {
                        contestants[competitor_index] = competitor_b;
                        self.losers[loser_index] = competitor_a;
                    }
                }
            }

            // The final winner of the first competition is now in `contestants[0]`
            self.contender = contestants[0];
            self.nodes_count = nodes_count;
            self.streams_active = contestants_count;
            self.tree_height = tree_height;
            self.key_popped = self.key_popped;
            self.state = .iterating;
        }

        pub fn pop(self: *KWayMergeIterator) Pending!?Value {
            if (self.state == .loading) try self.load();
            assert(self.state == .iterating);

            while (self.streams_active > 0) {
                const value = try self.next() orelse return null;
                if (options.deduplicate) {
                    const key_next = key_from_value(&value);
                    if (self.key_popped) |key_prev| if (key_next == key_prev) continue;
                    self.key_popped = key_next;
                }
                return value;
            }
            return null;
        }

        fn next(self: *KWayMergeIterator) Pending!?Value {
            const direction = self.direction;
            const stream_id = self.contender.stream_id;
            assert(!self.contender.sentinel);

            self.contender = try self.next_contender(stream_id);

            var opponent_id: usize = self.nodes_count + stream_id;
            for (0..self.tree_height) |_| {
                opponent_id = (opponent_id - 1) >> 1;

                const opponent = &self.losers[opponent_id];
                const winner = determine_winner(&self.contender, opponent, direction);
                swap_nodes(winner, &self.contender);
            }

            if (self.contender.sentinel) {
                assert(self.streams_active == 0);
                return null;
            }

            return stream_pop(self.context, self.contender.stream_id);
        }

        fn next_contender(self: *KWayMergeIterator, stream_id: u32) Pending!Node {
            assert(stream_id < self.streams_count);
            const next_key = try stream_peek(self.context, stream_id) orelse {
                self.streams_active -= 1;
                return sentinel;
            };
            return .{ .key = next_key, .stream_id = stream_id, .sentinel = false };
        }

        inline fn select(choose_a: bool, a: *Node, b: *Node) *Node {
            // Note: The code layout coaxes the compiler to generate branchless code.
            //       Best do not change it without verifying the generated code.
            var p = b;
            if (choose_a) {
                @branchHint(.unpredictable); // attaches to this branch
                p = a;
            }
            return p;
        }

        /// Return a pointer to the winner without branching.
        inline fn determine_winner(
            contender: *Node,
            challenger: *Node,
            direction: Direction,
        ) *Node {
            const challenger_wins: bool = challenger.beats(contender, direction);
            const winner: *Node = select(challenger_wins, challenger, contender);
            return winner;
        }

        // This custom swap is faster than `std.mem.swap` for our Node struct.
        inline fn swap_nodes(a: *Node, b: *Node) void {
            inline for (std.meta.fields(Node)) |f| {
                const tmp_field = @field(a, f.name);
                @field(a, f.name) = @field(b, f.name);
                @field(b, f.name) = tmp_field;
            }
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
        ) Pending!?u32 {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return null;
            return stream[0].key;
        }

        fn stream_pop(context: *TestContext, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
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
                .{
                    .streams_max = streams_max,
                    .deduplicate = true,
                },
                Value.to_key,
                stream_peek,
                stream_pop,
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
    };
}

test "k_way_merge: unit" {
    // Empty stream.
    try TestContextType(1).merge(
        .ascending,
        &[_][]const u32{},
        &[_]TestContextType(1).Value{},
    );

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
            .{ .key = 2, .version = 1 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 11, .version = 0 },
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
            .{ .key = 11, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 2, .version = 1 },
            .{ .key = 1, .version = 2 },
            .{ .key = 0, .version = 0 },
        },
    );

    try TestContextType(32).merge(
        .ascending,
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8 },
        },
        &[_]TestContextType(32).Value{
            .{ .key = 0, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
        },
    );

    try TestContextType(32).merge(
        .descending,
        &[_][]const u32{
            &[_]u32{ 11, 8, 4, 3, 0 },
            &[_]u32{ 15, 13, 12, 11, 2 },
            &[_]u32{ 11, 2, 1 },
        },
        &[_]TestContextType(32).Value{
            .{ .key = 15, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 12, .version = 1 },
            .{ .key = 11, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 2, .version = 1 },
            .{ .key = 1, .version = 2 },
            .{ .key = 0, .version = 0 },
        },
    );
}

fn FuzzTestContextType(comptime streams_max: u32) type {
    const testing = std.testing;
    const ratio = stdx.PRNG.ratio;

    return struct {
        const FuzzTestContext = @This();

        const TestContext = TestContextType(streams_max);
        const Value = TestContext.Value;

        const log = false;

        prng: *stdx.PRNG,
        inner: TestContext,

        fn fuzz_stream_peek(
            context: *const FuzzTestContext,
            stream_index: u32,
        ) Pending!?u32 {
            if (context.prng.chance(ratio(5, 100))) {
                return error.Pending;
            }
            return context.inner.stream_peek(stream_index);
        }

        fn stream_pop(context: *FuzzTestContext, stream_index: u32) Value {
            return context.inner.stream_pop(stream_index);
        }

        fn merge(
            direction: Direction,
            streams_keys: []const []const u32,
            expect: []const Value,
            prng: *stdx.PRNG,
        ) !void {
            const fuzz_helper = @import("../testing/fuzz.zig");
            const KWay = KWayMergeIteratorType(
                FuzzTestContext,
                u32,
                Value,
                .{
                    .streams_max = streams_max,
                    .deduplicate = true,
                },
                Value.to_key,
                fuzz_stream_peek,
                stream_pop,
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

            var context: FuzzTestContext = .{ .inner = .{ .streams = streams }, .prng = prng };
            var kway = KWay.init(&context, @intCast(streams_keys.len), direction);

            const Declarations = fuzz_helper.DeclEnumExcludingType(
                KWay,
                &.{.init},
            );

            var values_popped: u32 = 0;
            while (values_popped < expect.len) {
                switch (prng.enum_weighted(Declarations, .{ .pop = 98, .reset = 2 })) {
                    .pop => {
                        const maybe_value = kway.pop() catch continue;
                        const value = maybe_value orelse break;
                        try actual.append(value);
                        values_popped += 1;
                    },
                    .reset => {
                        kway.reset();
                    },
                }
            }

            try testing.expectEqualSlices(Value, expect, actual.items);
        }

        fn fuzz(prng: *stdx.PRNG, stream_key_count_max: u32) !void {
            if (log) std.debug.print("\n", .{});
            const gpa = testing.allocator;

            var streams: [streams_max][]u32 = undefined;

            const streams_buffer = try gpa.alloc(u32, streams_max * stream_key_count_max);
            defer gpa.free(streams_buffer);

            const expect_buffer = try gpa.alloc(Value, streams_max * stream_key_count_max);
            defer gpa.free(expect_buffer);

            for (0..streams_max) |k| {
                if (log) std.debug.print("k = {}\n", .{k});
                {
                    for (0..k) |i| {
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

                try merge(.ascending, streams[0..k], expect, prng);

                for (streams[0..k]) |stream| mem.reverse(u32, stream);
                mem.reverse(Value, expect);

                try merge(.descending, streams[0..k], expect, prng);

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
                .eq => a.version < b.version,
                .gt => false,
            };
        }
    };
}

test "k_way_merge: fuzz" {
    var prng = stdx.PRNG.from_seed_testing();
    try FuzzTestContextType(32).fuzz(&prng, 1024);
}
