const std = @import("std");
const assert = std.debug.assert;

const constants = @import("constants.zig");
const stdx = @import("stdx.zig");

/// An intrusive doubly-linked list.
/// Currently it is LIFO for simplicity because its consumer (IO.awaiting) doesn't care about order.
pub fn DoublyLinkedListType(
    comptime Node: type,
    comptime field_back_enum: std.meta.FieldEnum(Node),
    comptime field_next_enum: std.meta.FieldEnum(Node),
) type {
    assert(@typeInfo(Node) == .Struct);
    assert(field_back_enum != field_next_enum);
    assert(std.meta.FieldType(Node, field_back_enum) == ?*Node);
    assert(std.meta.FieldType(Node, field_next_enum) == ?*Node);

    const field_back = @tagName(field_back_enum);
    const field_next = @tagName(field_next_enum);

    return struct {
        const DoublyLinkedList = @This();

        tail: ?*Node = null,
        count: u32 = 0,

        pub fn verify(list: *const DoublyLinkedList) void {
            assert((list.count == 0) == (list.tail == null));

            var count: u32 = 0;
            var iterator = list.tail;

            if (iterator) |node| {
                assert(@field(node, field_next) == null);
            }

            while (iterator) |node| {
                const back = @field(node, field_back);
                if (back) |back_node| {
                    assert(back_node != node); // There are no cycles.
                    assert(@field(back_node, field_next) == node);
                }
                count += 1;
                iterator = back;
            }
            assert(count == list.count);
        }

        fn contains(list: *const DoublyLinkedList, target: *const Node) bool {
            var count: u32 = 0;

            var iterator = list.tail;
            while (iterator) |node| {
                if (node == target) return true;
                iterator = @field(node, field_back);
                count += 1;
            }

            assert(count == list.count);
            return false;
        }

        pub fn empty(list: *const DoublyLinkedList) bool {
            assert((list.count == 0) == (list.tail == null));
            return list.count == 0;
        }

        pub fn push(list: *DoublyLinkedList, node: *Node) void {
            if (constants.verify) list.verify();
            if (constants.verify) assert(!list.contains(node));
            assert(@field(node, field_back) == null);
            assert(@field(node, field_next) == null);

            if (list.tail) |tail| {
                assert(list.count > 0);
                assert(@field(tail, field_next) == null);

                @field(node, field_back) = tail;
                @field(tail, field_next) = node;
            } else {
                assert(list.count == 0);
            }

            list.tail = node;
            list.count += 1;
        }

        pub fn pop(list: *DoublyLinkedList) ?*Node {
            if (constants.verify) list.verify();

            if (list.tail) |tail_old| {
                assert(list.count > 0);
                assert(@field(tail_old, field_next) == null);

                list.tail = @field(tail_old, field_back);
                list.count -= 1;

                if (list.tail) |tail_new| {
                    assert(@field(tail_new, field_next) == tail_old);
                    @field(tail_new, field_next) = null;
                }

                @field(tail_old, field_back) = null;
                return tail_old;
            } else {
                assert(list.count == 0);
                return null;
            }
        }

        pub fn remove(list: *DoublyLinkedList, node: *Node) void {
            if (constants.verify) list.verify();
            if (constants.verify) assert(list.contains(node));
            assert(list.count > 0);
            assert(list.tail != null);

            const tail = list.tail.?;

            if (node == tail) {
                // Pop the last element of the list.
                assert(@field(node, field_next) == null);
                list.tail = @field(node, field_back);
            }
            if (@field(node, field_back)) |node_back| {
                assert(@field(node_back, field_next).? == node);
                @field(node_back, field_next) = @field(node, field_next);
            }
            if (@field(node, field_next)) |node_next| {
                assert(@field(node_next, field_back).? == node);
                @field(node_next, field_back) = @field(node, field_back);
            }
            @field(node, field_back) = null;
            @field(node, field_next) = null;
            list.count -= 1;

            if (constants.verify) list.verify();
            assert((list.count == 0) == (list.tail == null));
        }
    };
}

test "DoublyLinkedList LIFO" {
    const Node = struct { id: u32, back: ?*@This() = null, next: ?*@This() = null };
    const List = DoublyLinkedListType(Node, .back, .next);

    var nodes: [3]Node = undefined;
    for (&nodes, 0..) |*node, i| node.* = .{ .id = @intCast(i) };

    var list = List{};
    list.push(&nodes[0]);
    list.push(&nodes[1]);
    list.push(&nodes[2]);

    try std.testing.expectEqual(list.pop().?, &nodes[2]);
    try std.testing.expectEqual(list.pop().?, &nodes[1]);
    try std.testing.expectEqual(list.pop().?, &nodes[0]);
    try std.testing.expectEqual(list.pop(), null);
}

test "DoublyLinkedList fuzz" {
    // Ensure that The DoublyLinkedList's extra-verification is enabled.
    comptime assert(constants.verify);

    const fuzz = @import("testing/fuzz.zig");
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const Node = struct { id: u32, back: ?*@This() = null, next: ?*@This() = null };
    const List = DoublyLinkedListType(Node, .back, .next);

    const nodes_count = 1024;
    const events_max = 1 << 15;

    const Event = enum { push, pop, remove };
    const event_distribution = fuzz.DistributionType(Event){ .push = 10, .pop = 1, .remove = 8 };

    const nodes = try allocator.alloc(Node, nodes_count);
    defer allocator.free(nodes);
    for (nodes, 0..) |*node, i| node.* = .{ .id = @intCast(i) };

    var nodes_free = try std.DynamicBitSetUnmanaged.initFull(allocator, nodes_count);
    defer nodes_free.deinit(allocator);

    var list = List{};
    var list_model = stdx.BoundedArrayType(u32, nodes_count){};

    for (0..events_max) |_| {
        assert(list_model.count() <= nodes_count);
        assert(list_model.count() == list.count);
        assert(list_model.empty() == list.empty());

        const event = fuzz.random_enum(random, Event, event_distribution);
        switch (event) {
            .push => {
                const node_free = nodes_free.findFirstSet() orelse continue;
                const node = &nodes[node_free];

                list.push(node);
                list_model.append_assume_capacity(node.id);
                nodes_free.unset(node.id);
            },
            .pop => {
                if (list.pop()) |node| {
                    assert(node.back == null);
                    assert(node.next == null);

                    const node_id = list_model.pop();
                    assert(node_id == node.id);
                    assert(nodes_free.count() < nodes_count);
                    assert(!nodes_free.isSet(node_id));

                    nodes_free.set(node_id);
                } else {
                    assert(nodes_free.count() == nodes_count);
                    assert(list_model.empty());
                }
            },
            .remove => {
                if (list_model.count() == 0) continue;

                const list_index = random.uintLessThan(u32, list_model.count_as(u32));
                const node_id = list_model.get(list_index);
                assert(node_id == list_model.ordered_remove(list_index));
                assert(!nodes_free.isSet(node_id));

                list.remove(&nodes[node_id]);
                assert(nodes[node_id].back == null);
                assert(nodes[node_id].next == null);

                nodes_free.set(node_id);
            },
        }
    }
}
