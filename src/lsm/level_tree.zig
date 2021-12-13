const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const config = @import("../config.zig");

const Direction = @import("tree.zig").Direction;

// TODO choose a good number
const node_size = 4096;

pub fn LevelManifest(
    comptime Key: type,
    comptime TableInfo: type,
    comptime compare_keys: fn (Key, Key) math.Order,
) type {
    const data_node_element_count_max = node_size / @sizeOf(TableInfo);

    return struct {
        const Self = @This();

        const IndexNode = extern struct {
            const data_node_count_max = blk: {
                const ptr_size = @sizeOf(usize);
                const remaining = node_size - (ptr_size + @sizeOf(u32));
                const elements_size = remaining * @sizeOf(u32) / (@sizeOf(u32) + ptr_size);
                break :blk elements_size / @sizeOf(u32);
            };

            comptime {
                assert(@sizeOf(IndexNode) == node_size);
            }

            /// xor'd prev/next pointers
            link: usize,
            elements_total: u32,
            data_node_count: u32,
            data_node_pointers: [data_node_count_max][*]TableInfo,
            data_node_elements: [data_node_count_max]u32,

            _padding: if (config.is_32_bit) u32 else void = if (config.is_32_bit) 0 else {},
        };

        const index_node_count_max = blk: {
            const table_info_per_index_node = IndexNode.data_node_count_max *
                data_node_element_count_max;
            // TODO divCeil()
            const x = config.lsm_table_count_max / table_info_per_index_node;
        };

        head: *IndexNode,
        tail: *IndexNode,

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
