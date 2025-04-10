const std = @import("std");
const assert = std.debug.assert;

const constants = @import("./constants.zig");

/// An intrusive first in/first out linked list.
/// The element type T must have a field called "next" of type ?*T
pub fn QueueType(comptime T: type) type {
    return struct {
        const Queue = @This();

        in: ?*T = null,
        out: ?*T = null,
        count: u64 = 0,

        // This should only be null if you're sure we'll never want to monitor `count`.
        name: ?[]const u8,

        // If the number of elements is large, the constants.verify check in push() can be too
        // expensive. Allow the user to gate it. Could also be a comptime param?
        verify_push: bool = true,

        pub fn push(self: *Queue, elem: *T) void {
            if (constants.verify and self.verify_push) assert(!self.contains(elem));

            assert(elem.next == null);
            if (self.in) |in| {
                in.next = elem;
                self.in = elem;
            } else {
                assert(self.out == null);
                self.in = elem;
                self.out = elem;
            }
            self.count += 1;
        }

        pub fn pop(self: *Queue) ?*T {
            const ret = self.out orelse return null;
            self.out = ret.next;
            ret.next = null;
            if (self.in == ret) self.in = null;
            self.count -= 1;
            return ret;
        }

        pub fn peek_last(self: Queue) ?*T {
            return self.in;
        }

        pub fn peek(self: Queue) ?*T {
            return self.out;
        }

        pub fn empty(self: Queue) bool {
            return self.peek() == null;
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        pub fn contains(self: *const Queue, elem_needle: *const T) bool {
            var iterator = self.peek();
            while (iterator) |elem| : (iterator = elem.next) {
                if (elem == elem_needle) return true;
            }
            return false;
        }

        /// Remove an element from the Queue. Asserts that the element is
        /// in the Queue. This operation is O(N), if this is done often you
        /// probably want a different data structure.
        pub fn remove(self: *Queue, to_remove: *T) void {
            if (to_remove == self.out) {
                _ = self.pop();
                return;
            }
            var it = self.out;
            while (it) |elem| : (it = elem.next) {
                if (to_remove == elem.next) {
                    if (to_remove == self.in) self.in = elem;
                    elem.next = to_remove.next;
                    to_remove.next = null;
                    self.count -= 1;
                    break;
                }
            } else unreachable;
        }

        pub fn reset(self: *Queue) void {
            self.* = .{ .name = self.name };
        }
    };
}

test "Queue: push/pop/peek/remove/empty" {
    const testing = @import("std").testing;

    const Foo = struct { next: ?*@This() = null };

    var one: Foo = .{};
    var two: Foo = .{};
    var three: Foo = .{};

    var fifo: QueueType(Foo) = .{ .name = null };
    try testing.expect(fifo.empty());

    fifo.push(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&two);
    fifo.push(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(fifo.contains(&two));
    try testing.expect(fifo.contains(&three));

    fifo.remove(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expect(!fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&two);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.remove(&two);
    fifo.push(&three);
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());
}

test "Queue: fuzz" {
    const stdx = @import("./stdx.zig");
    const fuzz = @import("./testing/fuzz.zig");

    const Item = struct {
        value: u64,
        next: ?*@This(),
    };
    const Queue = QueueType(Item);
    const Model = stdx.RingBufferType(u64, .slice);

    const gpa = std.testing.allocator;
    var prng = stdx.PRNG.from_seed(92);

    for (0..100) |_| {
        const N = 1000;

        var queue: Queue = .{
            .name = "fuzz",
        };
        var model = try Model.init(gpa, N);
        defer model.deinit(gpa);

        // Implement "remove" by copy.
        var model_scratch = try Model.init(gpa, N);
        defer model_scratch.deinit(gpa);

        var items = try gpa.alloc(Item, N);
        defer gpa.free(items);
        for (items) |*item| {
            item.* = .{ .value = prng.int(u64), .next = null };
        }

        var free: std.ArrayListUnmanaged(usize) = .{};
        defer free.deinit(gpa);

        try free.ensureTotalCapacity(gpa, N);
        for (items, 0..) |_, index| {
            free.appendAssumeCapacity(index);
        }

        const weights = fuzz.random_enum_weights(&prng, std.meta.DeclEnum(Queue));

        for (0..N) |_| {
            switch (prng.enum_weighted(std.meta.DeclEnum(Queue), weights)) {
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
                .empty => {
                    assert(model.empty() == queue.empty());
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
            }
            assert(queue.count == model.count);
        }
    }
}
