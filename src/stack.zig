const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

const constants = @import("./constants.zig");

pub const StackLink = extern struct {
    next: ?*StackLink = null,
};

/// An intrusive last in/first out linked list (LIFO).
/// The element type T must have a field called "next" of type StackType(T).Link.
pub fn StackType(comptime T: type) type {
    return struct {
        any: StackAny,

        pub const Link = StackLink;
        const Stack = @This();

        pub inline fn init(options: struct {
            capacity: u32,
            verify_push: bool,
        }) Stack {
            return .{ .any = .{
                .capacity = options.capacity,
                .verify_push = options.verify_push,
            } };
        }

        pub inline fn count(self: *Stack) u32 {
            return self.any.count;
        }

        pub inline fn capacity(self: *Stack) u32 {
            return self.any.capacity;
        }

        /// Pushes a new node to the first position of the Stack.
        pub inline fn push(self: *Stack, node: *T) void {
            self.any.push(&node.link);
        }

        /// Returns the first element of the Stack list, and removes it.
        pub inline fn pop(self: *Stack) ?*T {
            const link = self.any.pop() orelse return null;
            return @fieldParentPtr("link", link);
        }

        /// Returns the first element of the Stack list, but does not remove it.
        pub inline fn peek(self: *const Stack) ?*T {
            const link = self.any.peek() orelse return null;
            return @fieldParentPtr("link", link);
        }

        /// Checks if the Stack is empty.
        pub inline fn empty(self: *const Stack) bool {
            return self.any.empty();
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        inline fn contains(self: *const Stack, needle: *const T) bool {
            return self.any.contains(&needle.link);
        }
    };
}

// Non-generic implementation for smaller binary and faster compile times.
const StackAny = struct {
    head: ?*StackLink = null,

    count: u32 = 0,
    capacity: u32,

    // If the number of elements is large, the constants.verify check in push() can be too
    // expensive. Allow the user to gate it.
    verify_push: bool,

    fn push(self: *StackAny, link: *StackLink) void {
        if (constants.verify and self.verify_push) assert(!self.contains(link));

        assert((self.count == 0) == (self.head == null));
        assert(link.next == null);
        assert(self.count < self.capacity);

        // Insert the new element at the head.
        link.next = self.head;
        self.head = link;
        self.count += 1;
    }

    fn pop(self: *StackAny) ?*StackLink {
        assert((self.count == 0) == (self.head == null));

        const link = self.head orelse return null;
        self.head = link.next;
        link.next = null;
        self.count -= 1;
        return link;
    }

    fn peek(self: *const StackAny) ?*StackLink {
        return self.head;
    }

    fn empty(self: *const StackAny) bool {
        assert((self.count == 0) == (self.head == null));
        return self.head == null;
    }

    fn contains(self: *const StackAny, needle: *const StackLink) bool {
        assert(self.count <= self.capacity);
        var next = self.head;
        for (0..self.count + 1) |_| {
            const link = next orelse return false;
            if (link == needle) return true;
            next = link.next;
        } else unreachable;
    }
};

test "Stack: fuzz" {
    // Fuzzy test to compare behavior of Stack against std.ArrayList (reference model).
    comptime assert(constants.verify);

    const allocator = std.testing.allocator;

    var prng = stdx.PRNG.from_seed_testing();

    const Item = struct {
        id: u32,
        link: StackType(@This()).Link,
    };
    const Stack = StackType(Item);

    const item_count_max = 1024;
    const events_max = 1 << 10;

    const Event = enum { push, pop };
    const event_weights = stdx.PRNG.EnumWeightsType(Event){
        .push = 2,
        .pop = 1,
    };

    // Allocate a pool of nodes.
    var items = try allocator.alloc(Item, item_count_max);
    defer allocator.free(items);
    for (items, 0..) |*item, i| {
        item.* = Item{ .id = @intCast(i), .link = .{} };
    }

    // A bit set that tracks which nodes are available.
    var items_free = try std.DynamicBitSetUnmanaged.initFull(allocator, item_count_max);
    defer items_free.deinit(allocator);

    var stack = Stack.init(.{
        .capacity = item_count_max,
        .verify_push = true,
    });

    // Reference model: a dynamic array of node IDs in Stack order (last is the top).
    var model = try std.ArrayList(u32).initCapacity(allocator, item_count_max);
    defer model.deinit();

    // Run a sequence of randomized events.
    for (0..events_max) |_| {
        assert(model.items.len <= item_count_max);
        assert(model.items.len == stack.count());
        assert(model.items.len == 0 or !stack.empty());

        const event = prng.enum_weighted(Event, event_weights);
        switch (event) {
            .push => {
                // Only push if a free node is available.
                const free_index = items_free.findFirstSet() orelse continue;
                const item = &items[free_index];
                stack.push(item);
                try model.append(item.id);
                items_free.unset(item.id);
            },
            .pop => {
                if (stack.pop()) |item| {
                    // The reference model should have the same node at the top.
                    const id = item.id;
                    const expected = model.pop();
                    assert(id == expected);
                    items_free.set(id);
                } else {
                    assert(model.items.len == 0);
                    assert(stack.empty());
                    assert(stack.count() == 0);
                    assert(stack.peek() == null);
                }
            },
        }
        // Verify that peek() returns the same as the last element in our model.
        if (model.items.len > 0) {
            const top = stack.peek() orelse unreachable;
            const top_ref = model.pop().?;
            assert(top.id == top_ref);
            try model.append(top_ref);
        } else {
            assert(stack.empty());
            assert(stack.count() == 0);
            assert(stack.peek() == null);
        }
    }

    // Finally, empty the Stack and ensure our reference model agrees.
    while (stack.pop()) |item| {
        const id = item.id;
        const expected = model.pop();
        assert(id == expected);
        items_free.set(id);
    }
    assert(model.items.len == 0);
    assert(stack.empty());
    assert(stack.count() == 0);
    assert(stack.peek() == null);
}

test "Stack: push/pop/peek/empty" {
    const testing = @import("std").testing;
    const Item = struct { link: StackLink = .{} };

    var one: Item = .{};
    var two: Item = .{};
    var three: Item = .{};

    var stack: StackType(Item) = StackType(Item).init(.{
        .capacity = 3,
        .verify_push = true,
    });

    try testing.expect(stack.empty());

    // Push one element and verify
    stack.push(&one);
    try testing.expect(!stack.empty());
    try testing.expectEqual(@as(?*Item, &one), stack.peek());
    try testing.expect(stack.contains(&one));
    try testing.expect(!stack.contains(&two));
    try testing.expect(!stack.contains(&three));

    // Push two more elements
    stack.push(&two);
    stack.push(&three);
    try testing.expect(!stack.empty());
    try testing.expectEqual(@as(?*Item, &three), stack.peek());
    try testing.expect(stack.contains(&one));
    try testing.expect(stack.contains(&two));
    try testing.expect(stack.contains(&three));

    // Pop elements and check Stack order
    try testing.expectEqual(@as(?*Item, &three), stack.pop());
    try testing.expectEqual(@as(?*Item, &two), stack.pop());
    try testing.expectEqual(@as(?*Item, &one), stack.pop());
    try testing.expect(stack.empty());
    try testing.expectEqual(@as(?*Item, null), stack.pop());
}
