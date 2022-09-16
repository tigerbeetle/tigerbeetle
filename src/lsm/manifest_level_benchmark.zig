const std = @import("std");
const assert = std.debug.assert;

const config = @import("../config.zig");
const NodePoolType = @import("../lsm/node_pool.zig").NodePool;
const ManifestLevelType = @import("../lsm/manifest_level.zig").ManifestLevelType;
const TableType = @import("../lsm/table.zig").TableType;
const TableInfoType = @import("../lsm/manifest.zig").TableInfoType;
const CompositeKey = @import("../lsm/composite_key.zig").CompositeKey;
const table_count_max_for_level = @import("../lsm/tree.zig").table_count_max_for_level;
const table_count_max_for_tree = @import("../lsm/tree.zig").table_count_max_for_tree;

const samples = 5_000_000;

const Options = struct {
    Field: type,
    node_size: u32,
    table_count: u32,
};

const configs = [_]Options{
    Options{ .Field = u64, .node_size = 256, .table_count = 33 },
    Options{ .Field = u64, .node_size = 256, .table_count = 34 },
    Options{ .Field = u64, .node_size = 256, .table_count = 1024 },
    Options{ .Field = u64, .node_size = 512, .table_count = 1024 },

    Options{
        .Field = u64,
        .node_size = config.lsm_manifest_node_size,
        .table_count = table_count_max_for_level(config.lsm_growth_factor, 1),
    },
    Options{
        .Field = u64,
        .node_size = config.lsm_manifest_node_size,
        .table_count = table_count_max_for_level(config.lsm_growth_factor, 2),
    },
    Options{
        .Field = u64,
        .node_size = config.lsm_manifest_node_size,
        .table_count = table_count_max_for_level(config.lsm_growth_factor, 3),
    },
    Options{
        .Field = u64,
        .node_size = config.lsm_manifest_node_size,
        .table_count = table_count_max_for_level(config.lsm_growth_factor, 4),
    },
    Options{
        .Field = u64,
        .node_size = config.lsm_manifest_node_size,
        .table_count = table_count_max_for_level(config.lsm_growth_factor, 5),
    },
    Options{
        .Field = u64,
        .node_size = config.lsm_manifest_node_size,
        .table_count = table_count_max_for_level(config.lsm_growth_factor, 6),
    },
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var prng = std.rand.DefaultPrng.init(42);

    inline for (configs) |options| {
        const Key = CompositeKey(options.Field);
        const Table = TableType(
            Key,
            Key.Value,
            Key.compare_keys,
            Key.key_from_value,
            Key.sentinel_key,
            Key.tombstone,
            Key.tombstone_from_key,
        );
        const TableInfo = TableInfoType(Table);

        const NodePool = NodePoolType(options.node_size, @alignOf(TableInfo));
        const ManifestLevel = ManifestLevelType(
            NodePool,
            Key,
            TableInfo,
            Table.compare_keys,
            // Must be max of both to avoid hitting SegmentedArray's assertion:
            //   assert(element_count_max > node_capacity);
            comptime std.math.max(
                options.table_count,
                @divFloor(options.node_size, @sizeOf(Key)) + 1,
            ),
        );

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var node_pool = try NodePool.init(
            allocator,
            ManifestLevel.Keys.node_count_max + ManifestLevel.Tables.node_count_max,
        );
        defer node_pool.deinit(allocator);

        var manifest_level = try ManifestLevel.init(allocator);
        defer manifest_level.deinit(allocator, &node_pool);

        var key: options.Field = 1;
        while (key <= options.table_count) : (key += 1) {
            const tables = [_]TableInfo{.{
                .checksum = 0,
                .address = 1,
                .flags = 0,
                .snapshot_min = 0,
                .snapshot_max = 100,
                .key_min = .{ .field = key, .timestamp = 0 },
                .key_max = .{ .field = key, .timestamp = 0 },
            }};
            manifest_level.insert_tables(&node_pool, &tables);
        }

        const queries = try alloc_shuffled_index(allocator, options.table_count, prng.random());
        defer allocator.free(queries);

        const timer = try std.time.Timer.start();

        const repetitions = std.math.max(1, @divFloor(samples, queries.len));
        // Sum the results, just to make sure the compiler doesn't optimize all of this out.
        var sum: options.Field = 0;
        var i: usize = 0;
        while (i < repetitions) : (i += 1) {
            for (queries) |query| {
                //sum += manifest_level.absolute_index_for_insert(.{
                //    .field = query,
                //    .timestamp = 0,
                //});
                sum += manifest_level.keys.absolute_index_for_cursor(manifest_level.keys.search(Key, struct {
                    inline fn key_from_value(k: *const Key) Key {
                        return k.*;
                    }
                }.key_from_value, Table.compare_keys, .{
                    .field = query,
                    .timestamp = 0,
                }));
            }
        }
        assert(sum > 0);

        const time = timer.read() / repetitions / queries.len;
        try stdout.print("FieldType={} NodeSize={:_>6} TableCount={:_>7} LookupTime={:_>6}ns\n", .{
            options.Field,
            options.node_size,
            options.table_count,
            time,
        });
    }
}

// shuffle([0,1,…,n-1])
fn alloc_shuffled_index(allocator: std.mem.Allocator, n: usize, rand: std.rand.Random) ![]usize {
    // Allocate on the heap; the array may be too large to fit on the stack.
    var indices = try allocator.alloc(usize, n);
    for (indices) |*i, j| i.* = j;
    rand.shuffle(usize, indices[0..]);
    return indices;
}
