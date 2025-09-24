const std = @import("std");
const assert = std.debug.assert;

const constants = @import("./constants.zig");

const QueueLink = extern struct {
    next: ?*QueueLink = null,
};

/// An intrusive first in/first out linked list.
/// The element type T must have a field called "link" of type QueueType(T).Link.
pub fn QueueType(comptime T: type) type {
    return struct {
        any: QueueAny,

        pub const Link = QueueLink;
        const Queue = @This();

        pub inline fn init(options: struct {
            name: ?[]const u8,
            verify_push: bool = true,
        }) Queue {
            return .{ .any = .{
                .name = options.name,
                .verify_push = options.verify_push,
            } };
        }

        pub inline fn push(self: *Queue, link: *T) void {
            self.any.push(&link.link);
        }

        pub inline fn pop(self: *Queue) ?*T {
            const link = self.any.pop() orelse return null;
            return @alignCast(@fieldParentPtr("link", link));
        }

        pub inline fn peek_last(self: *const Queue) ?*T {
            const link = self.any.peek_last() orelse return null;
            return @alignCast(@fieldParentPtr("link", link));
        }

        pub inline fn peek(self: *const Queue) ?*T {
            const link = self.any.peek() orelse return null;
            return @alignCast(@fieldParentPtr("link", link));
        }

        pub fn count(self: *const Queue) u64 {
            return self.any.count;
        }

        pub inline fn empty(self: *const Queue) bool {
            return self.any.empty();
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        pub inline fn contains(self: *const Queue, elem_needle: *const T) bool {
            return self.any.contains(&elem_needle.link);
        }

        /// Remove an element from the Queue. Asserts that the element is
        /// in the Queue. This operation is O(N), if this is done often you
        /// probably want a different data structure.
        pub inline fn remove(self: *Queue, to_remove: *T) void {
            self.any.remove(&to_remove.link);
        }

        pub inline fn reset(self: *Queue) void {
            self.any.reset();
        }

        pub inline fn iterate(self: *const Queue) Iterator {
            return .{ .any = self.any.iterate() };
        }

        pub const Iterator = struct {
            any: QueueAny.Iterator,

            pub inline fn next(iterator: *@This()) ?*T {
                const link = iterator.any.next() orelse return null;
                return @alignCast(@fieldParentPtr("link", link));
            }
        };
    };
}

// Non-generic implementation for smaller binary and faster compile times.
const QueueAny = struct {
    in: ?*QueueLink = null,
    out: ?*QueueLink = null,
    count: u64 = 0,

    // This should only be null if you're sure we'll never want to monitor `count`.
    name: ?[]const u8,

    // If the number of elements is large, the constants.verify check in push() can be too
    // expensive. Allow the user to gate it. Could also be a comptime param?
    verify_push: bool = true,

    pub fn push(self: *QueueAny, link: *QueueLink) void {
        if (constants.verify and self.verify_push) assert(!self.contains(link));

        assert(link.next == null);
        if (self.in) |in| {
            in.next = link;
            self.in = link;
        } else {
            assert(self.out == null);
            self.in = link;
            self.out = link;
        }
        self.count += 1;
    }

    pub fn pop(self: *QueueAny) ?*QueueLink {
        const result = self.out orelse return null;
        self.out = result.next;
        result.next = null;
        if (self.in == result) self.in = null;
        self.count -= 1;
        return result;
    }

    pub fn peek_last(self: *const QueueAny) ?*QueueLink {
        return self.in;
    }

    pub fn peek(self: *const QueueAny) ?*QueueLink {
        return self.out;
    }

    pub fn empty(self: *const QueueAny) bool {
        return self.peek() == null;
    }

    pub fn contains(self: *const QueueAny, needle: *const QueueLink) bool {
        var iterator = self.peek();
        while (iterator) |link| : (iterator = link.next) {
            if (link == needle) return true;
        }
        return false;
    }

    pub fn remove(self: *QueueAny, to_remove: *QueueLink) void {
        if (to_remove == self.out) {
            _ = self.pop();
            return;
        }
        var it = self.out;
        while (it) |link| : (it = link.next) {
            if (to_remove == link.next) {
                if (to_remove == self.in) self.in = link;
                link.next = to_remove.next;
                to_remove.next = null;
                self.count -= 1;
                break;
            }
        } else unreachable;
    }

    pub fn reset(self: *QueueAny) void {
        self.* = .{
            .name = self.name,
            .verify_push = self.verify_push,
        };
    }

    pub fn iterate(self: *const QueueAny) Iterator {
        return .{
            .head = self.out,
        };
    }

    const Iterator = struct {
        head: ?*QueueLink,

        fn next(iterator: *Iterator) ?*QueueLink {
            const head = iterator.head orelse return null;
            iterator.head = head.next;
            return head;
        }
    };
};

test "Queue: push/pop/peek/remove/empty" {
    const testing = @import("std").testing;

    const Item = struct { link: QueueType(@This()).Link = .{} };

    var one: Item = .{};
    var two: Item = .{};
    var three: Item = .{};

    var fifo = QueueType(Item).init(.{
        .name = null,
        .verify_push = true,
    });
    try testing.expect(fifo.empty());

    fifo.push(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Item, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&two);
    fifo.push(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Item, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(fifo.contains(&two));
    try testing.expect(fifo.contains(&three));

    fifo.remove(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Item, &two), fifo.pop());
    try testing.expectEqual(@as(?*Item, &three), fifo.pop());
    try testing.expectEqual(@as(?*Item, null), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expect(!fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&two);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Item, &one), fifo.pop());
    try testing.expectEqual(@as(?*Item, &three), fifo.pop());
    try testing.expectEqual(@as(?*Item, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Item, &one), fifo.pop());
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Item, &two), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expectEqual(@as(?*Item, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.remove(&two);
    fifo.push(&three);
    try testing.expectEqual(@as(?*Item, &one), fifo.pop());
    try testing.expectEqual(@as(?*Item, &three), fifo.pop());
    try testing.expectEqual(@as(?*Item, null), fifo.pop());
    try testing.expect(fifo.empty());
}

test "Queue: fuzz" {
    const stdx = @import("stdx");
    const fuzz = @import("./testing/fuzz.zig");

    const Item = struct {
        value: u64,
        link: QueueType(@This()).Link,
    };
    const Queue = QueueType(Item);
    const Model = stdx.RingBufferType(u64, .slice);

    const gpa = std.testing.allocator;
    var prng = stdx.PRNG.from_seed_testing();

    for (0..100) |_| {
        const N = 1000;

        var queue = Queue.init(.{
            .name = "fuzz",
        });
        var model = try Model.init(gpa, N);
        defer model.deinit(gpa);

        // Implement "remove" by copy.
        var model_scratch = try Model.init(gpa, N);
        defer model_scratch.deinit(gpa);

        var items = try gpa.alloc(Item, N);
        defer gpa.free(items);
        for (items, 0..) |*item, value| {
            item.* = .{ .value = value, .link = .{} };
        }

        var free: std.ArrayListUnmanaged(usize) = .{};
        defer free.deinit(gpa);

        try free.ensureTotalCapacity(gpa, N);
        for (items, 0..) |_, index| {
            free.appendAssumeCapacity(index);
        }

        const Declarations = fuzz.DeclEnumExcludingType(
            Queue,
            &.{ .Link, .Iterator, .init, .count, .empty },
        );
        const weights = fuzz.random_enum_weights(&prng, Declarations);

        for (0..N) |_| {
            switch (prng.enum_weighted(Declarations, weights)) {
                .push => {
                    if (free.items.len > 0) {
                        const index = free.swapRemove(prng.index(free.items));
                        model.push_assume_capacity(items[index].value);
                        queue.push(&items[index]);
                    }
                },
                .pop => {
                    if (model.empty()) {
                        assert(queue.pop() == null);
                    } else {
                        const item = queue.pop().?;
                        assert(item.value == model.pop().?);

                        const index = @divExact(
                            (@intFromPtr(item) - @intFromPtr(items.ptr)),
                            @sizeOf(Item),
                        );
                        free.appendAssumeCapacity(index);
                    }
                },
                .peek => {
                    if (model.head()) |head| {
                        assert(queue.peek().?.value == head);
                    } else {
                        assert(queue.peek() == null);
                    }
                },
                .peek_last => {
                    if (model.tail()) |tail| {
                        assert(queue.peek_last().?.value == tail);
                    } else {
                        assert(queue.peek_last() == null);
                    }
                },
                .contains => {
                    const item = &items[prng.index(items)];

                    const model_contains = model_contains: {
                        var it = model.iterator();
                        while (it.next()) |v| {
                            if (v == item.value) {
                                break :model_contains true;
                            }
                        }
                        break :model_contains false;
                    };
                    assert(model_contains == queue.contains(item));
                },
                .remove => {
                    const item = &items[prng.index(items)];
                    if (queue.contains(item)) {
                        queue.remove(item);

                        assert(model_scratch.empty());
                        while (model.pop()) |value| {
                            if (value != item.value) {
                                model_scratch.push_assume_capacity(value);
                            }
                        }
                        std.mem.swap(Model, &model, &model_scratch);
                    }
                },
                .reset => {
                    model.clear();
                    queue.reset();
                },
                .iterate => {
                    var queue_iterator = queue.iterate();
                    var model_iterator = model.iterator();

                    while (queue_iterator.next()) |queue_item| {
                        const model_item = model_iterator.next().?;
                        assert(queue_item.value == model_item);
                    }
                    assert(model_iterator.next() == null);
                    assert(queue_iterator.next() == null);
                },
            }
            assert(queue.count() == model.count);
            assert(queue.empty() == model.empty());
        }
    }
}
