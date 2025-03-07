const std = @import("std");
const assert = std.debug.assert;

const constants = @import("./constants.zig");

/// An intrusive last in/first out linked list (Stack).
/// The element type T must have a field called "next" of type ?*T.
pub fn StackType(comptime T: type) type {
    return struct {
        const Stack = @This();

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
        }) Stack {
            return Stack{
                .name = options.name,
                .count_max = options.capacity,
                .verify_push = options.verify_push,
            };
        }

        /// Pushes a new node to the first position of the Stack.
        pub fn push(self: *Stack, node: *T) void {
            if (constants.verify and self.verify_push) assert(!self.contains(node));

            assert((self.count == 0) == (self.head == null));
            assert(node.next == null);
            assert(self.count < self.count_max);

            // Insert the new element at the head.
            node.next = self.head;
            self.head = node;
            self.count += 1;
        }

        /// Returns the first element of the Stack list, and removes it.
        pub fn pop(self: *Stack) ?*T {
            assert((self.count == 0) == (self.head == null));

            const node = self.head orelse return null;
            self.head = node.next;
            node.next = null;
            self.count -= 1;
            return node;
        }

        /// Returns the first element of the Stack list, but does not remove it.
        pub fn peek(self: Stack) ?*T {
            return self.head;
        }

        /// Checks if the Stack is empty.
        pub fn empty(self: Stack) bool {
            assert((self.count == 0) == (self.head == null));
            return self.head == null;
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        fn contains(self: *const Stack, needle: *const T) bool {
            assert(self.count <= self.count_max);
            var next = self.head;
            for (0..self.count + 1) |_| {
                const node = next orelse return false;
                if (node == needle) return true;
                next = node.next;
            } else unreachable;
        }

        /// Resets the state.
        pub fn reset(self: *Stack) void {
            self.* = .{
                .name = self.name,
                .count_max = self.count_max,
                .verify_push = self.verify_push,
            };
        }
    };
}

test "Stack: fuzz" {
    // Fuzzy test to compare behavior of Stack against std.ArrayList (reference model).
    comptime assert(constants.verify);

    const fuzz = @import("testing/fuzz.zig");
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const Node = struct {
        id: u32,
        next: ?*@This() = null,
    };
    const Stack = StackType(Node);

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

    var stack = Stack.init(.{
        .capacity = node_count_max,
        .name = "fuzz",
        .verify_push = true,
    });

    // Reference model: a dynamic array of node IDs in Stack order (last is the top).
    var model = try std.ArrayList(u32).initCapacity(allocator, node_count_max);
    defer model.deinit();

    // Run a sequence of randomized events.
    for (0..events_max) |_| {
        assert(model.items.len <= node_count_max);
        assert(model.items.len == stack.count);
        assert(model.items.len == 0 or !stack.empty());

        const event = fuzz.random_enum(random, Event, event_distribution);
        switch (event) {
            .push => {
                // Only push if a free node is available.
                const free_index = nodes_free.findFirstSet() orelse continue;
                const node = &nodes[free_index];
                stack.push(node);
                try model.append(node.id);
                nodes_free.unset(node.id);
            },
            .pop => {
                if (stack.pop()) |node| {
                    // The reference model should have the same node at the top.
                    const id = node.id;
                    const expected = model.pop();
                    assert(id == expected);
                    nodes_free.set(id);
                } else {
                    assert(model.items.len == 0);
                    assert(stack.empty());
                    assert(stack.count == 0);
                    assert(stack.peek() == null);
                }
            },
        }
        // Verify that peek() returns the same as the last element in our model.
        if (model.items.len > 0) {
            const top = stack.peek() orelse unreachable;
            const top_ref = model.pop();
            assert(top.id == top_ref);
            try model.append(top_ref);
        } else {
            assert(stack.empty());
            assert(stack.count == 0);
            assert(stack.peek() == null);
        }
    }

    // Finally, empty the Stack and ensure our reference model agrees.
    while (stack.pop()) |node| {
        const id = node.id;
        const expected = model.pop();
        assert(id == expected);
        nodes_free.set(id);
    }
    assert(model.items.len == 0);
    assert(stack.empty());
    assert(stack.count == 0);
    assert(stack.peek() == null);
}

test "Stack: push/pop/peek/empty" {
    const testing = @import("std").testing;
    const Foo = struct { next: ?*@This() = null };

    var one: Foo = .{};
    var two: Foo = .{};
    var three: Foo = .{};

    var stack: StackType(Foo) = StackType(Foo).init(.{
        .capacity = 3,
        .name = "fuzz",
        .verify_push = true,
    });

    try testing.expect(stack.empty());

    // Push one element and verify
    stack.push(&one);
    try testing.expect(!stack.empty());
    try testing.expectEqual(@as(?*Foo, &one), stack.peek());
    try testing.expect(stack.contains(&one));
    try testing.expect(!stack.contains(&two));
    try testing.expect(!stack.contains(&three));

    // Push two more elements
    stack.push(&two);
    stack.push(&three);
    try testing.expect(!stack.empty());
    try testing.expectEqual(@as(?*Foo, &three), stack.peek());
    try testing.expect(stack.contains(&one));
    try testing.expect(stack.contains(&two));
    try testing.expect(stack.contains(&three));

    // Pop elements and check Stack order
    try testing.expectEqual(@as(?*Foo, &three), stack.pop());
    try testing.expectEqual(@as(?*Foo, &two), stack.pop());
    try testing.expectEqual(@as(?*Foo, &one), stack.pop());
    try testing.expect(stack.empty());
    try testing.expectEqual(@as(?*Foo, null), stack.pop());
}
