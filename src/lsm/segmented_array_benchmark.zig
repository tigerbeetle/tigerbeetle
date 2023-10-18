const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const NodePoolType = @import("node_pool.zig").NodePool;
const table_count_max_for_level = @import("tree.zig").table_count_max_for_level;
const table_count_max_for_tree = @import("tree.zig").table_count_max_for_tree;
const SortedSegmentedArray = @import("segmented_array.zig").SortedSegmentedArray;

const samples = 5_000_000;

const Options = struct {
    Key: type,
    value_size: u32,
    value_count: u32,
    node_size: u32,
};

// Benchmark 112B values to match `@sizeOf(TableInfo)`, which is either 112B or 80B depending on
// the Key type.
const configs = [_]Options{
    Options{ .Key = u64, .value_size = 112, .value_count = 33, .node_size = 256 },
    Options{ .Key = u64, .value_size = 112, .value_count = 34, .node_size = 256 },
    Options{ .Key = u64, .value_size = 112, .value_count = 1024, .node_size = 256 },
    Options{ .Key = u64, .value_size = 112, .value_count = 1024, .node_size = 512 },

    Options{
        .Key = u64,
        .value_size = 112,
        .value_count = table_count_max_for_level(constants.lsm_growth_factor, 1),
        .node_size = constants.lsm_manifest_node_size,
    },
    Options{
        .Key = u64,
        .value_size = 112,
        .value_count = table_count_max_for_level(constants.lsm_growth_factor, 2),
        .node_size = constants.lsm_manifest_node_size,
    },
    Options{
        .Key = u64,
        .value_size = 112,
        .value_count = table_count_max_for_level(constants.lsm_growth_factor, 3),
        .node_size = constants.lsm_manifest_node_size,
    },
    Options{
        .Key = u64,
        .value_size = 112,
        .value_count = table_count_max_for_level(constants.lsm_growth_factor, 4),
        .node_size = constants.lsm_manifest_node_size,
    },
    Options{
        .Key = u64,
        .value_size = 112,
        .value_count = table_count_max_for_level(constants.lsm_growth_factor, 5),
        .node_size = constants.lsm_manifest_node_size,
    },
    Options{
        .Key = u64,
        .value_size = 112,
        .value_count = table_count_max_for_level(constants.lsm_growth_factor, 6),
        .node_size = constants.lsm_manifest_node_size,
    },
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var prng = std.rand.DefaultPrng.init(42);

    inline for (configs) |options| {
        const Key = options.Key;
        const Value = struct {
            key: Key,
            padding: [options.value_size - @sizeOf(Key)]u8,
        };

        const NodePool = NodePoolType(options.node_size, @alignOf(Value));
        const SegmentedArray = SortedSegmentedArray(
            Value,
            NodePool,
            // Must be max of both to avoid hitting SegmentedArray's assertion:
            //   assert(element_count_max > node_capacity);
            comptime @max(
                options.value_count,
                @divFloor(options.node_size, @sizeOf(Key)) + 1,
            ),
            Key,
            struct {
                inline fn key_from_value(value: *const Value) Key {
                    return value.key;
                }
            }.key_from_value,
            .{ .verify = false },
        );

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var node_pool = try NodePool.init(allocator, SegmentedArray.node_count_max);
        defer node_pool.deinit(allocator);

        var array = try SegmentedArray.init(allocator);
        defer array.deinit(allocator, &node_pool);

        var i: usize = 0;
        while (i < options.value_count) : (i += 1) {
            _ = array.insert_element(&node_pool, .{
                .key = prng.random().uintLessThanBiased(u64, options.value_count),
                .padding = [_]u8{0} ** (options.value_size - @sizeOf(Key)),
            });
        }

        const queries = try alloc_shuffled_index(allocator, options.value_count, prng.random());
        defer allocator.free(queries);

        var timer = try std.time.Timer.start();
        const repetitions = @max(1, @divFloor(samples, queries.len));
        var j: usize = 0;
        while (j < repetitions) : (j += 1) {
            for (queries) |query| {
                std.mem.doNotOptimizeAway(array.absolute_index_for_cursor(array.search(query)));
            }
        }
        const time = timer.read() / repetitions / queries.len;

        try stdout.print("KeyType={} ValueCount={:_>7} ValueSize={:_>2}B NodeSize={:_>6}B LookupTime={:_>6}ns\n", .{
            options.Key,
            options.value_count,
            options.value_size,
            options.node_size,
            time,
        });
    }
}

// shuffle([0,1,…,n-1])
fn alloc_shuffled_index(allocator: std.mem.Allocator, n: usize, rand: std.rand.Random) ![]usize {
    // Allocate on the heap; the array may be too large to fit on the stack.
    var indices = try allocator.alloc(usize, n);
    for (indices, 0..) |*i, j| i.* = j;
    rand.shuffle(usize, indices[0..]);
    return indices;
}
