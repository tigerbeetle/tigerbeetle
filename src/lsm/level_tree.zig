const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");
const lsm = @import("tree.zig");

const Direction = @import("tree.zig").Direction;

fn div_ceil(numerator: anytype, denominator: anytype) @TypeOf(numerator, denominator) {
    const T = @TypeOf(numerator, denominator);
    return math.divCeil(T, numerator, denominator) catch unreachable;
}

pub fn ManifestLevel(
    comptime Key: type,
    comptime TableInfo: type,
    comptime compare_keys: fn (Key, Key) math.Order,
) type {
    const node_size = config.lsm_manifest_node_size;

    return struct {
        const Self = @This();

        const RootNode = struct {
            const count_max = blk: {
                // Data nodes are stored in a segmented list where if a node
                // fills up it is divided into two new nodes. Therefore,
                // the worst possible space overhead is when all data nodes
                // are half full.
                const min_table_info_per_data_node = DataNode.count_max / 2;
                const min_table_info_per_index_node = IndexNode.count_max * min_table_info_per_data_node;
                break :blk lsm.table_count_max / min_table_info_per_index_node;
            };

            const ChildNode = union {
                index_node: ?*IndexNode,
                data_node: ?*DataNode,
            };

            child_node_pointer: [count_max]ChildNode,
            child_node_key_min: [count_max]Key,

            count: u32,
        };

        const IndexNode = extern struct {
            const count_max = (node_size - @sizeOf(u32)) / (@sizeOf(?*DataNode) + @sizeOf(Key));

            data_node_pointer: [count_max]?*DataNode,
            data_node_key_min: [count_max]Key,

            count: u32,

            comptime {
                assert(@sizeOf(IndexNode) <= node_size);
            }
        };

        const DataNode = extern struct {
            const count_max = blk: {
                const data_size = node_size - (2 * @sizeOf(?*DataNode) + @sizeOf(u32));
                const table_info_size = @sizeOf(u128) + 2 * @sizeOf(Key) + 3 * @sizeOf(u64);
                break :blk data_size / table_info_size;
            };

            prev: ?*DataNode,
            next: ?*DataNode,

            // TableInfo in struct-of-arrays layout:
            checksum: [count_max]u128,
            key_min: [count_max]Key,
            key_max: [count_max]Key,
            address: [count_max]u64,
            timestamp_created: [count_max]u64,
            timestamp_deleted: [count_max]u64,

            count: u32,

            comptime {
                assert(@sizeOf(DataNode) <= node_size);
            }
        };

        index_node_count: u32 = 0,
        index_node_pointers: *[index_node_count_max]*IndexNode,
        index_node_elements: *[index_node_count_max]u32,

        fn init(allocator: *mem.Allocator, level: u8) !Self {}

        fn get(level: *Self, index: usize) TableInfo {
            var i: usize = 0;

            var prev: usize = 0;
            var link: usize = @ptrToInt(level.head);
            while (link != 0) {
                const node = @intToPtr(*IndexNode, link);

                if (index < i + node.elements_total) {
                    for (node.data_node_elements) |elements, j| {
                        if (index < i + elements) {
                            const data_node_pointer = node.data_node_pointers[j];
                            // This line isn't strictly necessary, but gives us bounds checking.
                            const data_node = data_node_pointer[0..data_node_element_count_max];
                            return data_node[index - i];
                        }
                        i += elements;
                    }
                    unreachable;
                }
                i += node.elements_total;

                link = prev ^ node.link;
                prev = @ptrToInt(node);
            }
            unreachable;
        }

        const Iterator = struct {};

        pub fn get_tables(
            level: *Self,
            /// May pass math.maxInt(u64) if there is no snapshot.
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) Iterator {
            return .{
                .level = level,
                .snapshot = snapshot,
                .key_min = key_min,
                .key_max = key_max,
                .direction = direction,
            };
        }
    };
}
