const std = @import("std");
const assert = std.debug.assert;

const constants = @import("constants.zig");
const stdx = @import("stdx");

/// An intrusive doubly-linked list.
pub fn DoublyLinkedListType(
    comptime Node: type,
    comptime field_back_enum: std.meta.FieldEnum(Node),
    comptime field_next_enum: std.meta.FieldEnum(Node),
) type {
    assert(@typeInfo(Node) == .@"struct");
    assert(field_back_enum != field_next_enum);

    const field_back = @tagName(field_back_enum);
    const field_next = @tagName(field_next_enum);
    assert(@FieldType(Node, field_back) == ?*Node);
    assert(@FieldType(Node, field_next) == ?*Node);

    return struct {
        const DoublyLinkedList = @This();

        head: ?*Node = null,
        tail: ?*Node = null,
        count: u32 = 0,

        pub fn verify(list: *const DoublyLinkedList) void {
            assert((list.count == 0) == (list.head == null));
            assert((list.count == 0) == (list.tail == null));

            if (list.head) |node| assert(@field(node, field_back) == null);
            if (list.tail) |node| assert(@field(node, field_next) == null);

            var count: u32 = 0;
            var iterator = list.head;
            while (iterator) |node| {
                const next = @field(node, field_next);
                if (next) |next_node| {
                    assert(next_node != node); // There are no cycles.
                    assert(@field(next_node, field_back) == node);
                } else {
                    assert(list.tail == node);
                }
                count += 1;
                iterator = next;
            }
            assert(count == list.count);
        }

        fn contains(list: *const DoublyLinkedList, target: *const Node) bool {
            var count: u32 = 0;

            var iterator = list.head;
            while (iterator) |node| {
                if (node == target) return true;
                iterator = @field(node, field_next);
                count += 1;
            }

            assert(count == list.count);
            return false;
        }

        pub fn empty(list: *const DoublyLinkedList) bool {
            assert((list.count == 0) == (list.head == null));
            assert((list.count == 0) == (list.tail == null));
            return list.count == 0;
        }

        pub fn push_tail(list: *DoublyLinkedList, node: *Node) void {
            if (constants.verify) assert(!list.contains(node));
            if (constants.verify) list.verify();
            assert(@field(node, field_back) == null);
            assert(@field(node, field_next) == null);

            if (list.tail) |tail| {
                assert(list.count > 0);
                assert(@field(tail, field_next) == null);

                @field(node, field_back) = tail;
                @field(tail, field_next) = node;
            } else {
                assert(list.count == 0);
                assert(list.head == null);

                list.head = node;
            }

            list.tail = node;
            list.count += 1;
        }

        pub fn pop_head(list: *DoublyLinkedList) ?*Node {
            if (constants.verify) list.verify();
            assert((list.head == null) == (list.tail == null));

            if (list.head) |head_old| {
                assert(list.count > 0);
                assert(@field(head_old, field_back) == null);

                list.head = @field(head_old, field_next);
                list.count -= 1;

                if (list.head) |head_new| {
                    assert(@field(head_new, field_back) == head_old);
                    @field(head_new, field_back) = null;
                }

                if (list.count == 0) {
                    assert(list.tail == head_old);
                    list.tail = null;
                }

                @field(head_old, field_next) = null;
                return head_old;
            } else {
                assert(list.count == 0);
                return null;
            }
        }

        /// Insert node_b into the list immediately following node_a.
        pub fn insert_before(list: *DoublyLinkedList, node_target: *Node, node_insert: *Node) void {
            assert(@field(node_insert, field_back) == null);
            assert(@field(node_insert, field_next) == null);
            assert(node_target != node_insert);

            if (constants.verify) {
                assert(list.contains(node_target));
                assert(!list.contains(node_insert));
                list.verify();
            }

            if (@field(node_target, field_back)) |node_bumped| {
                @field(node_insert, field_back) = node_bumped;
                @field(node_bumped, field_next) = node_insert;
            } else {
                assert(list.head == node_target);
                list.head = node_insert;
            }

            @field(node_insert, field_next) = node_target;
            @field(node_target, field_back) = node_insert;
            list.count += 1;
        }

        pub fn remove(list: *DoublyLinkedList, node: *Node) void {
            if (constants.verify) assert(list.contains(node));
            if (constants.verify) list.verify();
            assert(list.count > 0);
            assert(list.head != null);
            assert(list.tail != null);

            if (@field(node, field_back)) |node_back| {
                assert(@field(node_back, field_next).? == node);
                @field(node_back, field_next) = @field(node, field_next);
            } else {
                // Pop the first element of the list.
                assert(node == list.head.?);
                assert(@field(node, field_back) == null);

                list.head = @field(node, field_next);
            }

            if (@field(node, field_next)) |node_next| {
                assert(@field(node_next, field_back).? == node);
                @field(node_next, field_back) = @field(node, field_back);
            } else {
                // Pop the last element of the list.
                assert(node == list.tail.?);
                assert(@field(node, field_next) == null);

                list.tail = @field(node, field_back);
            }

            @field(node, field_back) = null;
            @field(node, field_next) = null;
            list.count -= 1;

            if (constants.verify) list.verify();
            assert((list.count == 0) == (list.head == null));
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
    list.push_tail(&nodes[0]);
    list.push_tail(&nodes[1]);
    list.push_tail(&nodes[2]);

    try std.testing.expectEqual(&nodes[0], list.pop_head().?);
    try std.testing.expectEqual(&nodes[1], list.pop_head().?);
    try std.testing.expectEqual(&nodes[2], list.pop_head().?);
    try std.testing.expectEqual(null, list.pop_head());
}

test "DoublyLinkedList fuzz" {
    // Ensure that The DoublyLinkedList's extra-verification is enabled.
    comptime assert(constants.verify);

    const allocator = std.testing.allocator;

    var prng = stdx.PRNG.from_seed_testing();

    const Node = struct { id: u32, back: ?*@This() = null, next: ?*@This() = null };
    const List = DoublyLinkedListType(Node, .back, .next);

    const nodes_count = 1024;
    const events_max = 1 << 15;

    const Event = enum { push, pop, insert_before, remove };
    const event_weights: stdx.PRNG.EnumWeightsType(Event) = .{
        .push = 10,
        .pop = 1,
        .insert_before = 5,
        .remove = 8,
    };

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

        const event = prng.enum_weighted(Event, event_weights);
        switch (event) {
            .push => {
                const node_free = nodes_free.findFirstSet() orelse continue;
                const node = &nodes[node_free];

                list.push_tail(node);
                list_model.push(node.id);
                nodes_free.unset(node.id);
            },
            .pop => {
                if (list.pop_head()) |node| {
                    assert(node.back == null);
                    assert(node.next == null);

                    const node_id = list_model.ordered_remove(0);
                    assert(node_id == node.id);
                    assert(nodes_free.count() < nodes_count);
                    assert(!nodes_free.isSet(node_id));

                    nodes_free.set(node_id);
                } else {
                    assert(nodes_free.count() == nodes_count);
                    assert(list_model.empty());
                }
            },
            .insert_before => {
                if (list_model.count() == 0) continue;

                const node_free = nodes_free.findFirstSet() orelse continue;
                const node = &nodes[node_free];

                const list_index = prng.index(list_model.slice());
                const target_id = list_model.get(list_index);
                list_model.insert_at(list_index, node.id);
                list.insert_before(&nodes[target_id], node);

                assert(nodes_free.isSet(node.id));
                nodes_free.unset(node.id);
            },
            .remove => {
                if (list_model.count() == 0) continue;

                const list_index = prng.index(list_model.slice());
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
