const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

const Key = packed struct {
    id: u64 align(@alignOf(u64)),

    const Value = packed struct {
        id: u64,
        value: u63,
        tombstone: u1 = 0,
    };

    inline fn compare_keys(a: Key, b: Key) std.math.Order {
        return std.math.order(a.id, b.id);
    }

    inline fn key_from_value(value: *const Key.Value) Key {
        return Key{ .id = value.id };
    }

    const sentinel_key = Key{
        .id = std.math.maxInt(u64),
    };

    inline fn tombstone(value: *const Key.Value) bool {
        return value.tombstone != 0;
    }

    inline fn tombstone_from_key(key: Key) Key.Value {
        return Key.Value{
            .id = key.id,
            .value = 0,
            .tombstone = 1,
        };
    }
};

const batch_size_max = constants.message_size_max - @sizeOf(vsr.Header);
const commit_entries_max = @divFloor(batch_size_max, @sizeOf(Key.Value));
const value_count_max = constants.lsm_batch_multiple * commit_entries_max;

const Table = @import("table.zig").TableType(
    Key,
    Key.Value,
    Key.compare_keys,
    Key.key_from_value,
    Key.sentinel_key,
    Key.tombstone,
    Key.tombstone_from_key,
    value_count_max,
    .general,
);

pub fn main() !void {
    var prng = std.rand.DefaultPrng.init(42);
    const random = prng.random();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const sorted_values = try allocator.alloc(Key.Value, value_count_max);
    defer allocator.free(sorted_values);

    const values = try allocator.alloc(Key.Value, value_count_max);
    defer allocator.free(values);

    // Generate values with .ids in a random order
    var random_id = random.uintLessThanBiased(u64, values.len);
    for (values) |*value| {
        assert(random_id < values.len);
        value.* = .{
            .id = random_id,
            .value = random.int(u63),
        };

        random_id += values.len - 1;
        if (random_id >= values.len) random_id -= values.len;
    }

    var timer = try std.time.Timer.start();
    const rng_seed = random.int(u64);

    const tm = @import("table_mutable.zig");
    inline for (.{
        .{ "std.HashMap", tm.TableMutableType(Table, "") },
        .{ "MinHeap", tm.TableMutableTreeType(Table, "", tm.HeapTreeType) },
        .{ "AA-Tree", tm.TableMutableTreeType(Table, "", tm.AATreeType) },
        .{ "RB-Tree", tm.TableMutableTreeType(Table, "", tm.RBTreeType) },
    }) |setup| {
        try bench(setup[1], setup[0], allocator, values, sorted_values, &timer, rng_seed);
    }
}

fn bench(
    comptime TableMutable: type,
    comptime table_mutable_name: []const u8,
    allocator: mem.Allocator,
    values: []const Key.Value,
    sorted_values: []Key.Value,
    timer: *std.time.Timer,
    rng_seed: u64,
) !void {
    const stdout = std.io.getStdOut().writer();
    const separator = "-" ** 54;

    try stdout.print("{s}\n{s:^53}|\n" ++ ("{s:>7} |" ** 6) ++ "\n{s}\n", .{
        separator,
        "TableMutable(" ++ table_mutable_name ++ ")",
        "size",
        "put",
        "get",
        "miss",
        "sort",
        "total",
        separator,
    });

    var table_mutable = try TableMutable.init(allocator, null);
    defer table_mutable.deinit(allocator);

    var prng = std.rand.DefaultPrng.init(rng_seed);
    const random = prng.random();

    var count_max = std.math.min(1024, value_count_max);
    while (count_max <= value_count_max) {
        var put_elapsed: u64 = 0;
        var get_elapsed: u64 = 0;
        var miss_elapsed: u64 = 0;
        var sort_elapsed: u64 = 0;

        assert(table_mutable.count() == 0);
        for (values[0..count_max]) |*value, index| {
            // Insert the value into TableMutable.
            const put_start = timer.read();
            table_mutable.put(value);
            put_elapsed += timer.read() - put_start;

            // Fetch a random, previously inserted, value from TableMutable.
            const get_value = &values[random.uintAtMostBiased(usize, index)];
            const get_start = timer.read();
            const get_result = table_mutable.get(.{ .id = get_value.id }).?;
            get_elapsed += timer.read() - get_start;
            assert(mem.eql(u8, mem.asBytes(get_result), mem.asBytes(get_value)));

            // Fetch a random value that we know will not be in TableMutable.
            const miss_id = random.uintAtMostBiased(usize, value_count_max) + value_count_max;
            const miss_start = timer.read();
            const miss_result = table_mutable.get(.{ .id = miss_id });
            miss_elapsed += timer.read() - miss_start;
            assert(miss_result == null);
        }

        const sort_start = timer.read();
        const sorted = table_mutable.sort_into_values_and_clear(sorted_values);
        sort_elapsed = timer.read() - sort_start;

        for (sorted) |*value, i| {
            const prev = std.math.sub(usize, i, 1) catch continue;
            const prev_key = Table.key_from_value(&sorted[prev]);
            const key = Table.key_from_value(value);
            assert(Table.compare_keys(prev_key, key) != .gt);
        }

        try stdout.print("{d:>7} |", .{count_max});
        for ([_]u64{
            put_elapsed,
            get_elapsed,
            miss_elapsed,
            sort_elapsed,
            put_elapsed + get_elapsed + miss_elapsed + sort_elapsed,
        }) |ns| {
            if (ns < std.time.ns_per_us) {
                try stdout.print("{d:>5}ns |", .{ns});
            } else if (ns < std.time.ns_per_ms) {
                try stdout.print("{d:>5}us |", .{ns / std.time.ns_per_us});
            } else if (ns < std.time.ns_per_s) {
                try stdout.print("{d:>5}ms |", .{ns / std.time.ns_per_ms});
            } else {
                try stdout.print("{d:>6.2}s |", .{@intToFloat(f64, ns) / std.time.ns_per_s});
            }
        }
        try stdout.print("\n", .{});

        if (count_max == value_count_max) break;
        count_max = std.math.min(count_max * 2, value_count_max);
    }

    try stdout.print("\n", .{});
}
