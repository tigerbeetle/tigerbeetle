const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");
const lsm = @import("tree.zig");
const binary_search = @import("binary_search").binary_search;

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

        const key_node_capacity = node_size / @sizeOf(Key);
        const key_nodes_max = blk: {
            // Key nodes are a segmented array where if a node
            // fills up it is divided into two new nodes. Therefore,
            // the worst possible space overhead is when all key nodes
            // are half full.
            const min_keys_per_node = key_node_capacity / 2;
            // TODO Can we get rid of this +1?
            break :blk div_ceil(lsm.table_count_max, min_keys_per_node) + 1;
        };

        const table_node_capacity = node_size / @sizeOf(TableInfo);
        const table_nodes_max = blk: {
            // Info nodes are a segmented array where if a node
            // fills up it is divided into two new nodes. Therefore,
            // the worst possible space overhead is when all table nodes
            // are half full.
            const min_tables_per_node = table_node_capacity / 2;
            // TODO Can we get rid of this +1?
            break :blk div_ceil(lsm.table_count_max, min_tables_per_node) + 1;
        };

        key_node_count: u32,
        key_node_key_min: *[key_nodes_max]Key,
        key_node_pointer: *[key_nodes_max]?*[key_node_capacity]Key,
        // TODO Get rid of this as it is redundant with key_node_start_index
        key_node_counts: *[key_nodes_max]u32,
        key_node_start_index: *[key_nodes_max]u32,
        key_node_key_min_table_node: *[key_nodes_max]u32,

        table_node_count: u32,
        table_node_pointer: *[table_nodes_max]?*[table_node_capacity]TableInfo,
        // TODO Get rid of this as it is redundant with table_node_start_index
        table_node_counts: *[table_nodes_max]u32,
        table_node_start_index: *[table_nodes_max]u32,

        fn init(allocator: *mem.Allocator, level: u8) !Self {}

        pub const Iterator = struct {
            level: *Self,

            node: u32,
            /// The index inside the current table node.
            index: u32,

            /// May pass math.maxInt(u64)-1 if there is no snapshot.
            snapshot: u64,

            key_min: Key,
            key_max: Key,
            direction: Direction,

            pub fn next(it: *Iterator) ?*TableInfo {
                {
                    assert(direction == .ascending);
                    if (it.node >= it.level.table_node_count) return null;
                }

                const tables_len = it.level.table_node_count[it.node];
                const tables = it.level.table_node_pointer[it.node][0..tables_len];

                const table_info = &tables[it.index];

                switch (direction) {
                    .ascending => {
                        assert(compare_keys(table_info.key_min, it.key_min) != .lt);
                        if (compare_keys(table_info.key_min, it.key_max) == .gt) {
                            // Set this to ensure that next() continues to return null if called again.
                            it.node = it.level.table_node_count;
                            return null;
                        }
                    },
                    .descending => {
                        assert(compare_keys(table_info.key_max, it.key_max) != .gt);
                        if (compare_keys(table_info.key_max, it.key_min) == .lt) {
                            // Set this to ensure that next() continues to return null if called again.
                            it.node = 0;
                            return null;
                        }
                    },
                }

                it.index += 1;
                if (it.index >= tables.len) {
                    assert(direction == .ascending);
                    it.index = 0;
                    it.node += 1;
                }

                return table_info;
            }
        };

        pub fn iterate(
            level: *Self,
            /// May pass math.maxInt(u64) if there is no snapshot.
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) Iterator {
            // TODO handle descending direction
            assert(direction == .ascending);

            const key_node = binary_search(level.key_node_key_min, key_min);
            // TODO think through out of bounds/negative lookup/etc.
            const keys = level.key_node_pointer[key_node][0..level.key_node_counts[key_node]];
            const index = level.key_node_start_index[key_node] + binary_search(keys, key_min);

            var table_node = level.key_node_key_min_table_node[key_node];
            while (table_node + 1 < level.table_node_count and
                level.table_node_start_index[table_node + 1] <= index)
            {
                table_node += 1;
            }
            const relative_index = index - level.table_node_start_index[table_node];

            return .{
                .level = level,

                .table_node = table_node,
                .relative_index = relative_index,

                .snapshot = snapshot,
                .key_min = key_min,
                .key_max = key_max,
                .direction = direction,
            };
        }

        fn binary_search(keys: []const Key, key: Key) usize {
            assert(keys.len > 0);

            var offset: usize = 0;
            var length: usize = keys.len;
            while (length > 1) {
                const half = length / 2;
                const mid = offset + half;

                // This trick seems to be what's needed to get llvm to emit branchless code for this,
                // a ternay-style if expression was generated as a jump here for whatever reason.
                const next_offsets = [_]usize{ offset, mid };
                offset = next_offsets[@boolToInt(compare_keys(keys[mid], key) == .lt)];

                length -= half;
            }

            return offset + @boolToInt(compare_keys(keys[offset], key) == .lt);
        }
    };
}
