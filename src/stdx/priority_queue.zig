//! A statically bound PriorityQueue.
//! We follow the API of the ArrayList in that we them items live in
//! items[] that is items.len is how many items are actaully in the PriorityQueue.
const assert = std.debug.assert;
const std = @import("std");
const stdx = @import("stdx.zig");

pub fn PriorityQueueType(
    comptime T: type,
    comptime compare_fn: fn (T, T) std.math.Order,
) type {
    return struct {
        items: []T,
        capacity: usize,

        const PriorityQueue = @This();

        pub fn init(gpa: std.mem.Allocator, capacity: usize) !PriorityQueue {
            const items = try gpa.alloc(T, capacity);
            return .{
                .items = items[0..0],
                .capacity = capacity,
            };
        }

        pub fn deinit(priority_queue: *PriorityQueue, gpa: std.mem.Allocator) void {
            priority_queue.items.len = priority_queue.capacity;
            gpa.free(priority_queue.items);
        }

        pub fn push(priority_queue: *PriorityQueue, value: T) void {
            assert(priority_queue.items.len < priority_queue.capacity);
            const index = priority_queue.items.len;
            priority_queue.items.len += 1;

            priority_queue.sift_up(index, value);
        }

        pub fn peek(priority_queue: *const PriorityQueue) ?T {
            if (priority_queue.items.len == 0) return null;
            return priority_queue.items[0];
        }

        pub fn pop(priority_queue: *PriorityQueue) ?T {
            if (priority_queue.items.len == 0) return null;

            const root = priority_queue.items[0];
            const last = priority_queue.items[priority_queue.items.len - 1];
            priority_queue.items.len -= 1;

            if (priority_queue.items.len == 0) return root;

            priority_queue.sift_down(0, last);
            return root;
        }

        pub fn pop_index(priority_queue: *PriorityQueue, index: usize) ?T {
            assert(index < priority_queue.items.len);

            const value = priority_queue.items[index];
            const last = priority_queue.items[priority_queue.items.len - 1];
            priority_queue.items.len -= 1;
            if (index == priority_queue.items.len) return value;

            if (index > 0 and compare_fn(last, priority_queue.items[(index - 1) / 2]) == .lt) {
                priority_queue.sift_up(index, last);
            } else {
                priority_queue.sift_down(index, last);
            }
            return value;
        }

        fn sift_up(priority_queue: *PriorityQueue, index: usize, value: T) void {
            assert(index < priority_queue.items.len);
            assert(index < priority_queue.capacity);

            var hole = index;
            while (hole > 0) {
                const parent = @divFloor(hole - 1, 2);
                if (compare_fn(value, priority_queue.items[parent]) != .lt) break;
                priority_queue.items[hole] = priority_queue.items[parent];
                hole = parent;
            }
            priority_queue.items[hole] = value;
        }

        fn sift_down(priority_queue: *PriorityQueue, index: usize, value: T) void {
            assert(index < priority_queue.items.len);

            var hole = index;
            while (hole < @divFloor(priority_queue.items.len, 2)) {
                var left_child = 2 * hole + 1;
                const right_child = left_child + 1;

                if (right_child < priority_queue.items.len and
                    compare_fn(
                        priority_queue.items[right_child],
                        priority_queue.items[left_child],
                    ) == .lt)
                {
                    left_child = right_child;
                }

                if (compare_fn(priority_queue.items[left_child], value) != .lt) break;

                priority_queue.items[hole] = priority_queue.items[left_child];
                hole = left_child;
            }
            priority_queue.items[hole] = value;
        }
    };
}

const TestContext = struct {
    const MinHeap = PriorityQueueType(u64, ascending);
    const MaxHeap = PriorityQueueType(u64, descending);

    fn ascending(a: u64, b: u64) std.math.Order {
        return std.math.order(a, b);
    }

    fn descending(a: u64, b: u64) std.math.Order {
        return std.math.order(b, a);
    }

    const ModelMinHeap = std.PriorityQueue(u64, void, std_ascending);
    const ModelMaxHeap = std.PriorityQueue(u64, void, std_descending);

    fn std_ascending(_: void, a: u64, b: u64) std.math.Order {
        return ascending(a, b);
    }

    fn std_descending(_: void, a: u64, b: u64) std.math.Order {
        return descending(a, b);
    }
};

test "PriorityQueue: MinHeap" {
    var min_heap = try TestContext.MinHeap.init(std.testing.allocator, 8);
    defer min_heap.deinit(std.testing.allocator);

    const numbers = [_]u64{ 4, 1, 3, 2, 5, 0, 7, 6 };
    for (numbers) |number| {
        min_heap.push(number);
    }
    for (0..8) |expected| {
        try std.testing.expectEqual(expected, min_heap.peek().?);
        const actual = min_heap.pop().?;
        try std.testing.expectEqual(actual, expected);
    }
    try std.testing.expectEqual(min_heap.peek(), null);
}

test "PriorityQueue: MaxHeap" {
    var max_heap = try TestContext.MaxHeap.init(std.testing.allocator, 8);
    defer max_heap.deinit(std.testing.allocator);

    const numbers = [_]u64{ 4, 1, 3, 2, 5, 0, 7, 6 };
    for (numbers) |number| {
        max_heap.push(number);
    }

    var expected = numbers.len;
    while (expected > 0) {
        expected -= 1;
        try std.testing.expectEqual(expected, max_heap.peek().?);
        const actual = max_heap.pop().?;
        try std.testing.expectEqual(actual, expected);
    }
    try std.testing.expectEqual(max_heap.peek(), null);
}

test "PriorityQueue: items" {
    var queue = try TestContext.MinHeap.init(std.testing.allocator, 8);
    defer queue.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), queue.items.len);

    for ([_]u64{ 4, 1, 3, 2 }) |value| queue.push(value);
    _ = queue.pop();

    const queue_const: *const TestContext.MinHeap = &queue;
    var seen: [5]bool = @splat(false);
    var count: usize = 0;
    for (queue_const.items) |value| {
        try std.testing.expect(value >= 2 and value <= 4);
        try std.testing.expect(!seen[value]);
        seen[value] = true;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(usize, 3), queue.items.len);
}

test "PriorityQueue: fuzz" {
    const fuzz = stdx.fuzz;

    const gpa = std.testing.allocator;
    var prng = stdx.PRNG.from_seed_testing();

    inline for (
        .{
            .{ TestContext.MinHeap, TestContext.ModelMinHeap },
            .{ TestContext.MaxHeap, TestContext.ModelMaxHeap },
        },
    ) |pair| {
        const PQ, const Model = pair;

        const capacity = prng.int_inclusive(u64, 1000);

        var priority_queue = try PQ.init(gpa, capacity);
        defer priority_queue.deinit(gpa);

        var model: Model = .empty;
        defer model.deinit(gpa);

        try model.ensureTotalCapacity(gpa, capacity);

        const Actions = fuzz.DeclEnumExcludingType(
            PQ,
            &.{ .init, .deinit },
        );
        const action_weights = prng.enum_weights(Actions);

        var iterations: stdx.PRNG.FuzzIterations = .{};

        while (iterations.more()) {
            switch (prng.enum_weighted(Actions, action_weights)) {
                .pop => {
                    const actual = priority_queue.pop();
                    const expected = model.pop();
                    try std.testing.expectEqual(expected, actual);
                },
                .pop_index => {
                    if (priority_queue.items.len == 0) {
                        try std.testing.expectEqual(priority_queue.items.len, model.count());
                        continue;
                    }
                    const index = prng.index(priority_queue.items);
                    const actual = priority_queue.pop_index(index);
                    const expected = model.popIndex(index);
                    try std.testing.expectEqual(expected, actual);
                },
                .push => {
                    if (priority_queue.items.len == capacity) {
                        try std.testing.expectEqual(priority_queue.items.len, model.count());
                        continue;
                    }
                    const number = prng.int_inclusive(u64, 100);
                    try model.push(.failing, number);
                    priority_queue.push(number);
                },
                .peek => {
                    try std.testing.expectEqual(model.peek(), priority_queue.peek());
                },
            }
            try std.testing.expectEqual(model.count(), priority_queue.items.len);
            var iterator_expected = model.iterator();
            for (priority_queue.items) |actual| {
                try std.testing.expectEqual(iterator_expected.next().?, actual);
            }
            try std.testing.expectEqual(null, iterator_expected.next());
        }

        try std.testing.expectEqual(priority_queue.items.len, model.count());
        while (priority_queue.items.len > 0) {
            try std.testing.expectEqual(model.pop(), priority_queue.pop());
        }
    }
}
