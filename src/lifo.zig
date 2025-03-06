const std = @import("std");
const assert = std.debug.assert;

const constants = @import("./constants.zig");

/// An intrusive last in/first out (LIFO, stack) linked list.
/// The element type T must have a field called "next" of type ?*T.
pub fn LIFOType(comptime T: type) type {
    return struct {
        const LIFO = @This();

        head: ?*T = null,

        count: u64 = 0,
        count_max: u64,

        // This should only be null if you're sure we'll never want to monitor `count`.
        name: ?[]const u8,

        // If the number of elements is large, the constants.verify check in push() can be too
        // expensive. Allow the user to gate it.
        verify_push: bool,

        pub fn init(options: struct {
            capacity: u64,
            verify_push: bool,
            name: ?[]const u8,
        }) LIFO {
            return LIFO{
                .name = options.name,
                .count_max = options.capacity,
                .verify_push = options.verify_push,
            };
        }

        /// Pushes a new node to the first position of the LIFO.
        pub fn push(self: *LIFO, node: *T) void {
            if (constants.verify and self.verify_push) assert(!self.contains(node));

            assert((self.count == 0) == (self.head == null));
            assert(node.next == null);
            assert(self.count < self.count_max);

            // Insert the new element at the head.
            node.next = self.head;
            self.head = node;
            self.count += 1;
        }

        /// Returns the first element of the LIFO list, and removes it.
        pub fn pop(self: *LIFO) ?*T {
            assert((self.count == 0) == (self.head == null));

            const node = self.head orelse return null;
            self.head = node.next;
            node.next = null;
            self.count -= 1;
            return node;
        }

        /// Returns the first element of the LIFO list, but does not remove it.
        pub fn peek(self: LIFO) ?*T {
            return self.head;
        }

        /// Checks if the LIFO is empty.
        pub fn empty(self: LIFO) bool {
            assert((self.count == 0) == (self.head == null));
            return self.head == null;
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        fn contains(self: *const LIFO, needle: *const T) bool {
            assert(self.count <= self.count_max);
            var next = self.head;
            for (0..self.count + 1) |_| {
                const node = next orelse return false;
                if (node == needle) return true;
                next = node.next;
            } else unreachable;
        }

        /// Resets the state.
        pub fn reset(self: *LIFO) void {
            self.* = .{
                .name = self.name,
                .count_max = self.count_max,
                .verify_push = self.verify_push,
            };
        }
    };
}

test "LIFO: fuzz" {
    // Fuzzy test to compare behavior of LIFO against std.ArrayList (reference model).
    comptime assert(constants.verify);

    const fuzz = @import("testing/fuzz.zig");
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const Node = struct {
        id: u32,
        next: ?*@This() = null,
    };
    const LIFO = LIFOType(Node);

    const node_count_max = 1024;
    const events_max = 1 << 10;

    const Event = enum { push, pop };
    const event_distribution = fuzz.DistributionType(Event){
        .push = 2,
        .pop = 1,
    };

    // Allocate a pool of nodes.
    var nodes = try allocator.alloc(Node, node_count_max);
    defer allocator.free(nodes);
    for (nodes, 0..) |*node, i| {
        node.* = Node{ .id = @intCast(i), .next = null };
    }

    // A bit set that tracks which nodes are available.
    var nodes_free = try std.DynamicBitSetUnmanaged.initFull(allocator, node_count_max);
    defer nodes_free.deinit(allocator);

    var lifo = LIFO.init(.{
        .capacity = node_count_max,
        .name = "fuzz",
        .verify_push = true,
    });

    // Reference model: a dynamic array of node IDs in LIFO order (last is the top).
    var model = try std.ArrayList(u32).initCapacity(allocator, node_count_max);
    defer model.deinit();

    // Run a sequence of randomized events.
    for (0..events_max) |_| {
        assert(model.items.len <= node_count_max);
        assert(model.items.len == lifo.count);
        assert(model.items.len == 0 or !lifo.empty());

        const event = fuzz.random_enum(random, Event, event_distribution);
        switch (event) {
            .push => {
                // Only push if a free node is available.
                const free_index = nodes_free.findFirstSet() orelse continue;
                const node = &nodes[free_index];
                lifo.push(node);
                try model.append(node.id);
                nodes_free.unset(node.id);
            },
            .pop => {
                if (lifo.pop()) |node| {
                    // The reference model should have the same node at the top.
                    const id = node.id;
                    const expected = model.pop();
                    assert(id == expected);
                    nodes_free.set(id);
                } else {
                    assert(model.items.len == 0);
                    assert(lifo.empty());
                    assert(lifo.count == 0);
                    assert(lifo.peek() == null);
                }
            },
        }
        // Verify that peek() returns the same as the last element in our model.
        if (model.items.len > 0) {
            const top = lifo.peek() orelse unreachable;
            const top_ref = model.pop();
            assert(top.id == top_ref);
            try model.append(top_ref);
        } else {
            assert(lifo.empty());
            assert(lifo.count == 0);
            assert(lifo.peek() == null);
        }
    }

    // Finally, empty the LIFO and ensure our reference model agrees.
    while (lifo.pop()) |node| {
        const id = node.id;
        const expected = model.pop();
        assert(id == expected);
        nodes_free.set(id);
    }
    assert(model.items.len == 0);
    assert(lifo.empty());
    assert(lifo.count == 0);
    assert(lifo.peek() == null);
}

test "LIFO: push/pop/peek/empty" {
    const testing = @import("std").testing;
    const Foo = struct { next: ?*@This() = null };

    var one: Foo = .{};
    var two: Foo = .{};
    var three: Foo = .{};

    var lifo: LIFOType(Foo) = LIFOType(Foo).init(.{
        .capacity = 3,
        .name = "fuzz",
        .verify_push = true,
    });

    try testing.expect(lifo.empty());

    // Push one element and verify
    lifo.push(&one);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), lifo.peek());
    try testing.expect(lifo.contains(&one));
    try testing.expect(!lifo.contains(&two));
    try testing.expect(!lifo.contains(&three));

    // Push two more elements
    lifo.push(&two);
    lifo.push(&three);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &three), lifo.peek());
    try testing.expect(lifo.contains(&one));
    try testing.expect(lifo.contains(&two));
    try testing.expect(lifo.contains(&three));

    // Pop elements and check LIFO order
    try testing.expectEqual(@as(?*Foo, &three), lifo.pop());
    try testing.expectEqual(@as(?*Foo, &two), lifo.pop());
    try testing.expectEqual(@as(?*Foo, &one), lifo.pop());
    try testing.expect(lifo.empty());
    try testing.expectEqual(@as(?*Foo, null), lifo.pop());
}
