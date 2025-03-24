//! Utils functions for writing fuzzers.

const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const PRNG = stdx.PRNG;
const ratio = stdx.PRNG.ratio;

/// Returns an integer of type `T` with an exponential distribution of rate `avg`.
/// Note: If you specify a very high rate then `std.math.maxInt(T)` may be over-represented.
pub fn random_int_exponential(prng: *stdx.PRNG, comptime T: type, avg: T) T {
    comptime {
        const info = @typeInfo(T);
        assert(info == .Int);
        assert(info.Int.signedness == .unsigned);
    }
    // Note: we use floats and rely on std implementaion. Ideally, we should do neither, but I
    // wasn't able to find a quick way to generate geometrically distributed integers using only
    // integer arithmetic.
    const random = std.Random.init(prng, stdx.PRNG.fill);
    const exp = random.floatExp(f64) * @as(f64, @floatFromInt(avg));
    return std.math.lossyCast(T, exp);
}

/// Return a distribution for use with `random_enum`.
///
/// This is swarm testing: sm   ome variants are disabled completely,
/// and the rest have wildly different probabilites.
pub fn random_enum_weights(
    prng: *stdx.PRNG,
    comptime Enum: type,
) stdx.PRNG.EnumWeightsType(Enum) {
    const fields = std.meta.fieldNames(Enum);

    var combination = stdx.PRNG.Combination.init(.{
        .total_count = fields.len,
        .sample_count = prng.range_inclusive(u32, 1, fields.len),
    });
    defer assert(combination.done());

    var weights: PRNG.EnumWeightsType(Enum) = undefined;
    inline for (fields) |field| {
        @field(weights, field) = if (combination.take(prng))
            prng.int_inclusive(u64, 100)
        else
            0;
    }

    return weights;
}

pub const FuzzArgs = struct {
    seed: u64,
    events_max: ?usize,
};

pub fn parse_seed(bytes: []const u8) u64 {
    if (bytes.len == 40) {
        // Normally, a seed is specified as a base-10 integer. However, as a special case, we allow
        // using a Git hash (a hex string 40 character long). This is used by our CI, which passes
        // current commit hash as a seed --- that way, we run simulator on CI, we run it with
        // different, "random" seeds, but the failures remain reproducible just from the commit
        // hash!
        const commit_hash = std.fmt.parseUnsigned(u160, bytes, 16) catch |err| switch (err) {
            error.Overflow => unreachable,
            error.InvalidCharacter => @panic("commit hash seed contains an invalid character"),
        };
        return @truncate(commit_hash);
    }

    return std.fmt.parseUnsigned(u64, bytes, 10) catch |err| switch (err) {
        error.Overflow => @panic("seed exceeds a 64-bit unsigned integer"),
        error.InvalidCharacter => @panic("seed contains an invalid character"),
    };
}

/// A queue for tracking work to be executed at a particular tick.
pub fn ReadyQueueType(T: type) type {
    return struct {
        queue: Queue,

        const Queue = std.PriorityQueue(T, void, order_by_ready_at);
        fn order_by_ready_at(_: void, lhs: T, rhs: T) std.math.Order {
            return std.math.order(lhs.ready_at_tick, rhs.ready_at_tick);
        }

        const ReadyQueue = @This();

        pub fn init(gpa_allocator: std.mem.Allocator, capacity: usize) !ReadyQueue {
            var result: ReadyQueue = .{
                .queue = Queue.init(gpa_allocator, {}),
            };
            try result.queue.ensureTotalCapacity(capacity);
            return result;
        }

        pub fn deinit(ready: *ReadyQueue, _: std.mem.Allocator) void {
            ready.queue.deinit();
            ready.* = undefined;
        }

        pub fn reset(ready: *ReadyQueue) void {
            ready.queue.items.len = 0;
        }

        pub fn add(ready: *ReadyQueue, item: T) void {
            assert(ready.queue.count() < ready.queue.capacity());
            ready.queue.add(item) catch |err| switch (err) {
                error.OutOfMemory => unreachable,
            };
        }

        pub fn remove_ready(ready: *ReadyQueue, prng: *stdx.PRNG, tick: u64) ?T {
            const top = ready.queue.peek() orelse return null;
            if (top.ready_at_tick > tick) return null;

            const root = ready.pick_random_ready(prng, tick, 0);
            assert(root.count > 0);
            assert(root.pick < ready.queue.items.len);

            const result = ready.queue.removeIndex(root.pick);
            assert(result.ready_at_tick <= tick);
            return result;
        }

        pub fn remove_random(ready: *ReadyQueue, prng: *stdx.PRNG) ?T {
            if (ready.count() == 0) return null;
            const index = prng.index(ready.queue.items);
            return ready.queue.removeIndex(index);
        }

        // All items in the queue, in an unspecified order.
        pub fn slice(ready: *ReadyQueue) []T {
            return ready.queue.items;
        }

        // All items in the queue, in an unspecified order.
        pub fn const_slice(ready: *const ReadyQueue) []const T {
            return ready.queue.items;
        }

        pub fn count(ready: *ReadyQueue) usize {
            return ready.queue.items.len;
        }

        // For a subtree rooted at index, return the total number of items in the subtree as
        // well as an index of a randomly uniformly drawn ready item.
        //
        // The implementation relies on a particular layout of std.PriorityQueue, and might require
        // vendoring it in the future.
        const SubtreePick = struct { pick: usize, count: usize };
        fn pick_random_ready(
            ready: *const ReadyQueue,
            prng: *stdx.PRNG,
            tick: u64,
            index: usize,
        ) SubtreePick {
            if (index >= ready.queue.items.len or ready.queue.items[index].ready_at_tick > tick) {
                return .{
                    .pick = undefined,
                    .count = 0,
                };
            }
            assert(ready.queue.items[index].ready_at_tick <= tick);

            // Reservoir-ish sampling: replace our pick from the one from subtree, with probability
            // proportional to subtree's size.
            var result: SubtreePick = .{ .pick = index, .count = 1 };
            inline for (.{ index * 2 + 1, index * 2 + 2 }) |index_child| {
                const subtree = ready.pick_random_ready(prng, tick, index_child);
                if (prng.chance(ratio(subtree.count, result.count + subtree.count))) {
                    result.pick = subtree.pick;
                }
                result.count += subtree.count;
            }

            return result;
        }
    };
}
