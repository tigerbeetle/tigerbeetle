//! K-way merge via a loser tree algorithm (Knuth Volume 3 p. 253).
//! Merges k sorted streams using a tournament (loser) tree.
//! The current global winner lives in `win_key`/`win_id`. Internal nodes store
//! the losers of the last comparisons along the root-to-leaf paths in a
//! struct-of-arrays layout (`loser_keys`/`loser_ids`).
//!
//!     0 (winner)
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
const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const math = std.math;
const mem = std.mem;

const Direction = @import("../direction.zig").Direction;
const Pending = error{Pending};

pub fn TournamentTreeType(comptime Key: type, contestants_max: comptime_int) type {
    return struct {
        loser_keys: [node_count_max]Key align(64),
        loser_ids: [node_count_max]u32 align(64),
        win_key: Key,
        win_id: u32,
        contestants_left: u16,
        height: u8,
        direction: Direction,

        pub const node_count_max: u32 = std.math.ceilPowerOfTwoAssert(u32, contestants_max);
        const height_max = std.math.log2_int(u32, node_count_max);
        const sentinel_key = std.math.maxInt(Key);

        const Node = struct {
            key: Key,
            id: u32,

            const id_sentinel = std.math.maxInt(u32);
            const sentinel: Node = .{
                .key = sentinel_key,
                .id = id_sentinel,
            };
        };

        const TournamentTree = @This();

        pub fn init(
            direction: Direction,
            contestants: *[node_count_max]Node,
            contestant_count: u16,
        ) TournamentTree {
            assert(contestant_count <= contestants_max);
            maybe(contestant_count == 0);

            var contestant_previous: ?*const Node = null;
            var contestants_left: u16 = 0;
            for (contestants[0..contestant_count]) |*contestant| {
                if (contestant.id == Node.id_sentinel) {
                    // Stream is empty to begin with.
                } else {
                    contestants_left += 1;
                    if (contestant_previous) |previous| assert(previous.id < contestant.id);
                    contestant_previous = contestant;
                }
            }
            for (contestants[contestant_count..]) |*contestant| {
                assert(contestant.id == Node.id_sentinel);
            }

            var tree: TournamentTree = .{
                .win_key = sentinel_key,
                .win_id = Node.id_sentinel,
                .loser_keys = @splat(sentinel_key),
                .loser_ids = @splat(Node.id_sentinel),
                .direction = direction,
                .contestants_left = contestants_left,
                .height = 0,
            };

            if (contestants_left == 0) return tree;

            // Compute effective tree size: only as large as needed for contestant_count.
            const node_count: u32 = std.math.ceilPowerOfTwoAssert(u32, contestant_count);
            tree.height = @intCast(std.math.log2_int(u32, node_count));

            for (0..tree.height) |level| {
                const shift_min: u5 = @intCast(level + 1);
                const shift_max: u5 = @intCast(level);
                const level_min: usize = (node_count >> shift_min) - 1;
                const level_max: usize = (node_count >> shift_max) - 1;

                for (level_min..level_max, 0..) |loser_index, competitor_index| {
                    const a = contestants[competitor_index * 2];
                    const b = contestants[competitor_index * 2 + 1];
                    const a_wins = beats(a.key, a.id, b.key, b.id, direction);

                    contestants[competitor_index] = .{
                        .key = stdx.branchless_select(Key, a_wins, a.key, b.key),
                        .id = stdx.branchless_select(u32, a_wins, a.id, b.id),
                    };
                    // We select the loser here thus a, b are swapped.
                    tree.loser_keys[loser_index] = stdx.branchless_select(
                        Key,
                        a_wins,
                        b.key,
                        a.key,
                    );
                    tree.loser_ids[loser_index] = stdx.branchless_select(u32, a_wins, b.id, a.id);
                }
            }

            tree.win_key = contestants[0].key;
            tree.win_id = contestants[0].id;

            return tree;
        }

        pub fn pop_winner(tree: *TournamentTree, entrant: ?Key) void {
            switch (tree.direction) {
                inline else => |direction| {
                    switch (tree.height) {
                        inline 0...height_max => |height| pop_winner_impl(
                            tree,
                            entrant,
                            direction,
                            height,
                        ),
                        else => unreachable,
                    }
                },
            }
        }

        inline fn pop_winner_impl(
            tree: *TournamentTree,
            entrant: ?Key,
            comptime direction: Direction,
            comptime height: u32,
        ) void {
            const node_count = @as(u32, 1) << @as(u5, @intCast(height));
            const winner_id = tree.win_id;

            assert(tree.win_id < node_count);
            if (entrant == null) tree.contestants_left -= 1;

            var new_key: Key = if (entrant) |key| key else sentinel_key;
            var new_id: u32 = if (entrant != null) winner_id else Node.id_sentinel;

            var idx: usize = (node_count - 1) + winner_id;
            inline for (0..height) |_| {
                idx = (idx - 1) >> 1;

                const opp_key = tree.loser_keys[idx];
                const opp_id = tree.loser_ids[idx];
                const new_wins = beats(new_key, new_id, opp_key, opp_id, direction);

                tree.loser_keys[idx] = stdx.branchless_select(Key, new_wins, opp_key, new_key);
                tree.loser_ids[idx] = stdx.branchless_select(u32, new_wins, opp_id, new_id);
                new_key = stdx.branchless_select(Key, new_wins, new_key, opp_key);
                new_id = stdx.branchless_select(u32, new_wins, new_id, opp_id);
            }

            tree.win_key = new_key;
            tree.win_id = new_id;

            if (tree.win_id == Node.id_sentinel) assert(tree.contestants_left == 0);
        }

        /// Returns true if (a_key, a_id) wins over (b_key, b_id).
        /// Sentinels (id_sentinel) always lose. Equal keys broken by id for stability.
        /// In ascending mode, sentinel_key (maxInt) naturally loses on `<` so no
        /// explicit sentinel checks are needed. In descending mode, maxInt would
        /// incorrectly "win" on `>`, so explicit sentinel checks are required.
        inline fn beats(a_key: Key, a_id: u32, b_key: Key, b_id: u32, direction: Direction) bool {
            const id_lt: u1 = @intFromBool(a_id < b_id);
            const keys_eq: u1 = @intFromBool(a_key == b_key);
            const eq_and_id_wins: u1 = keys_eq & id_lt;

            if (direction == .ascending) {
                const key_lt: u1 = @intFromBool(a_key < b_key);
                return (key_lt | eq_and_id_wins) == 1;
            } else {
                const key_gt: u1 = @intFromBool(a_key > b_key);
                const b_is_sentinel: u1 = @intFromBool(b_id == Node.id_sentinel);
                const a_is_sentinel: u1 = @intFromBool(a_id == Node.id_sentinel);
                const key_wins: u1 = key_gt | eq_and_id_wins;
                return (b_is_sentinel | ((1 - a_is_sentinel) & key_wins)) == 1;
            }
        }
    };
}

pub const KWayMergeOptions = struct {
    streams_max: comptime_int,
    deduplicate: bool = false,
};

pub fn KWayMergeIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime options: KWayMergeOptions,
    comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
    comptime stream_peek: fn (context: *Context, stream_index: u32) Pending!?Key,
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
) type {
    comptime assert(options.streams_max >= 1);
    comptime assert(options.streams_max <= 1024); // Reasonable upper bound.

    return struct {
        context: *Context,
        streams_count: u16,
        direction: Direction,
        key_popped: ?Key,
        tree: ?Tree,

        const Tree = TournamentTreeType(Key, options.streams_max);
        const KWayMergeIterator = @This();

        pub fn init(
            context: *Context,
            streams_count: u16,
            direction: Direction,
        ) KWayMergeIterator {
            assert(streams_count <= options.streams_max);
            maybe(streams_count == 0);

            return .{
                .context = context,
                .key_popped = null,
                .direction = direction,
                .streams_count = streams_count,
                .tree = null,
            };
        }

        pub fn reset(self: *KWayMergeIterator) void {
            self.* = .{
                .context = self.context,
                .direction = self.direction,
                .streams_count = self.streams_count,
                .key_popped = self.key_popped,
                .tree = null,
            };
        }

        fn load(self: *KWayMergeIterator) Pending!void {
            assert(self.tree == null);
            errdefer self.reset();

            var contestants: [Tree.node_count_max]Tree.Node = @splat(.sentinel);
            for (0..self.streams_count) |id_usize| {
                const id: u32 = @intCast(id_usize);
                const key = try stream_peek(self.context, id) orelse continue;
                contestants[id_usize] = .{ .key = key, .id = id };
            }

            self.tree = Tree.init(self.direction, &contestants, self.streams_count);
        }

        pub fn pop(self: *KWayMergeIterator) Pending!?Value {
            if (self.tree == null) try self.load();
            const tree = &self.tree.?;

            while (tree.contestants_left > 0) {
                const key = try stream_peek(self.context, tree.win_id);
                tree.pop_winner(key);
                if (tree.contestants_left == 0) return null;
                const value = stream_pop(self.context, tree.win_id);
                if (options.deduplicate) {
                    const key_next = key_from_value(&value);
                    if (self.key_popped) |key_prev| if (key_next == key_prev) continue;
                    self.key_popped = key_next;
                }
                return value;
            }
            return null;
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

            fn less_than(direction: Direction, a: Value, b: Value) bool {
                var order = math.order(a.key, b.key);
                if (direction == .descending) order = order.invert();
                return switch (order) {
                    .lt => true,
                    .eq => a.version < b.version,
                    .gt => false,
                };
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
            expect: ?[]const Value,
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

            const gpa = std.testing.allocator;

            var actual = std.ArrayList(Value).init(gpa);
            defer actual.deinit();

            var streams: [streams_max][]Value = undefined;

            for (streams_keys, 0..) |stream_keys, i| {
                errdefer for (streams[0..i]) |s| gpa.free(s);
                streams[i] = try gpa.alloc(Value, stream_keys.len);
                for (stream_keys, 0..) |key, j| {
                    streams[i][j] = .{
                        .key = key,
                        .version = @intCast(i),
                    };
                }
            }
            defer for (streams[0..streams_keys.len]) |s| gpa.free(s);

            const expect_naive_buffer = try gpa.alloc(Value, key_count: {
                var total_count: u32 = 0;
                for (streams_keys) |stream| total_count += @intCast(stream.len);
                break :key_count total_count;
            });
            defer gpa.free(expect_naive_buffer);

            const expect_naive = merge_naive(streams_keys, direction, expect_naive_buffer);

            if (expect) |expect_explicit| {
                try testing.expectEqualSlices(Value, expect_naive, expect_explicit);
            }

            var context: TestContext = .{ .streams = streams };
            var kway = KWay.init(&context, @intCast(streams_keys.len), direction);

            while (try kway.pop()) |value| {
                try actual.append(value);
            }

            try testing.expectEqualSlices(Value, expect_naive, actual.items);
        }

        fn merge_naive(
            streams: []const []const u32,
            direction: Direction,
            result: []Value,
        ) []Value {
            var count: u32 = 0;
            for (streams, 0..) |stream, stream_index| {
                for (stream) |key| {
                    result[count] = .{
                        .key = key,
                        .version = @intCast(stream_index),
                    };
                    count += 1;
                }
            }
            assert(result.len >= count);

            std.mem.sort(Value, result[0..count], direction, Value.less_than);

            const count_duplicates = count;
            count = 0;

            var previous_key: ?u32 = null;
            for (result[0..count_duplicates]) |value| {
                if (previous_key) |p| {
                    if (value.key == p) continue;
                }
                previous_key = value.key;
                result[count] = value;
                count += 1;
            }
            return result[0..count];
        }
    };
}

test "k_way_merge: unit" {
    // Empty stream.
    try TestContextType(1).merge(
        .ascending,
        &.{},
        &.{},
    );

    try TestContextType(1).merge(
        .ascending,
        &.{
            &.{ 0, 3, 4, 8 },
        },
        &.{
            .{ .key = 0, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
        },
    );
    try TestContextType(1).merge(
        .descending,
        &.{
            &.{ 8, 4, 3, 0 },
        },
        &.{
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 0, .version = 0 },
        },
    );
    try TestContextType(3).merge(
        .ascending,
        &.{
            &.{ 0, 3, 4, 8, 11 },
            &.{ 2, 11, 12, 13, 15 },
            &.{ 1, 2, 11 },
        },
        &.{
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
        &.{
            &.{ 11, 8, 4, 3, 0 },
            &.{ 15, 13, 12, 11, 2 },
            &.{ 11, 2, 1 },
        },
        &.{
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
        &.{
            &.{ 0, 3, 4, 8 },
        },
        &.{
            .{ .key = 0, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
        },
    );

    try TestContextType(32).merge(
        .descending,
        &.{
            &.{ 11, 8, 4, 3, 0 },
            &.{ 15, 13, 12, 11, 2 },
            &.{ 11, 2, 1 },
        },
        &.{
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

test "k_way_merge: exhaustigen" {
    const Gen = @import("../testing/exhaustigen.zig");
    const N = 3;
    const M = 2;

    var streams_buffer: [N][M]u32 = @splat(@splat(0));
    var streams: [N][]u32 = @splat(&.{});
    var g: Gen = .{};
    while (!g.done()) {
        const direction = g.enum_value(Direction);
        for (0..N) |stream_index| {
            const key_count = g.int_inclusive(u32, M);
            for (0..key_count) |key_index| {
                streams_buffer[stream_index][key_index] = g.int_inclusive(u32, M);
            }
            streams[stream_index] = streams_buffer[stream_index][0..key_count];
            std.mem.sort(u32, streams[stream_index], {}, std.sort.asc(u32));
            if (direction == .descending) {
                std.mem.reverse(u32, streams[stream_index]);
            }
        }

        try TestContextType(N).merge(
            direction,
            &streams,
            null,
        );
    }
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
            gpa: std.mem.Allocator,
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
            var actual = std.ArrayList(Value).init(gpa);
            defer actual.deinit();

            var streams: [streams_max][]Value = undefined;

            for (streams_keys, 0..) |stream_keys, i| {
                errdefer for (streams[0..i]) |s| gpa.free(s);
                streams[i] = try gpa.alloc(Value, stream_keys.len);
                for (stream_keys, 0..) |key, j| {
                    streams[i][j] = .{
                        .key = key,
                        .version = @intCast(i),
                    };
                }
            }
            defer for (streams[0..streams_keys.len]) |s| gpa.free(s);

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

                const expect = TestContext.merge_naive(streams[0..k], .ascending, expect_buffer);

                if (log) {
                    std.debug.print("expect = ", .{});
                    for (expect) |value| std.debug.print("({},{}),", .{ value.key, value.version });
                    std.debug.print("\n", .{});
                }

                try merge(gpa, .ascending, streams[0..k], expect, prng);

                for (streams[0..k]) |stream| mem.reverse(u32, stream);
                mem.reverse(Value, expect);

                try merge(gpa, .descending, streams[0..k], expect, prng);

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
            std.mem.sort(u32, stream, {}, std.sort.asc(u32));
        }
    };
}

test "k_way_merge: fuzz" {
    var prng = stdx.PRNG.from_seed_testing();
    try FuzzTestContextType(32).fuzz(&prng, 1024);
}
