const std = @import("std");
const lsm_levels_max: usize = 8;

const Allocator = std.mem.Allocator;
const Table = struct {
    tree_id: u16,
    level: u8,
    key_max: u64,
    snapshot_min: u64,
};

const Defaults = struct {
    pub const tables: usize = 100_000;
    pub const trees: usize = 4;
    pub const levels: usize = 6;
    pub const seed: u64 = 0xdead_beef;
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const raw_args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, raw_args);

    var cfg = try parse_args(raw_args);
    cfg.raw_args = raw_args;

    cfg.tables = @max(cfg.tables, 1);
    cfg.trees = @max(cfg.trees, 1);
    cfg.levels = @max(cfg.levels, 1);
    cfg.levels = @min(cfg.levels, lsm_levels_max);

    const data = try generate_tables(allocator, cfg);
    defer allocator.free(data);

    const ns_to_ms = 1_000_000.0;
    const stdout = std.io.getStdOut().writer();

    const new_stats = try bench_batch_sort(allocator, data, cfg);
    const new_ns_per = @as(f64, @floatFromInt(new_stats.ns)) / @as(f64, @floatFromInt(cfg.tables));

    try stdout.print("new_path: {d:.2} ms total, {d:.2} ns/table checksum={d}\n", .{
        @as(f64, @floatFromInt(new_stats.ns)) / ns_to_ms,
        new_ns_per,
        new_stats.checksum,
    });

    const old_stats = try bench_incremental(allocator, data, cfg);
    const old_ns_per = @as(f64, @floatFromInt(old_stats.ns)) / @as(f64, @floatFromInt(cfg.tables));

    const speedup =
        @as(f64, @floatFromInt(old_stats.ns)) / @as(f64, @floatFromInt(new_stats.ns));

    try stdout.print(
        "tables={d} trees={d} levels={d} seed={x}\n" ++
            "old_path: {d:.2} ms total, {d:.2} ns/table checksum={d}\n" ++
            "speedup: {d:.2}x\n",
        .{
            cfg.tables,
            cfg.trees,
            cfg.levels,
            cfg.seed,
            @as(f64, @floatFromInt(old_stats.ns)) / ns_to_ms,
            old_ns_per,
            old_stats.checksum,
            speedup,
        },
    );
}

const Config = struct {
    tables: usize = Defaults.tables,
    trees: usize = Defaults.trees,
    levels: usize = Defaults.levels,
    seed: u64 = Defaults.seed,
    raw_args: [][:0]u8,
};

fn parse_args(args: [][:0]u8) !Config {
    var cfg = Config{ .raw_args = args };
    // Skip program name.
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        const expect_tables = std.mem.eql(u8, arg, "--tables");
        const expect_trees = std.mem.eql(u8, arg, "--trees");
        const expect_levels = std.mem.eql(u8, arg, "--levels");
        const expect_seed = std.mem.eql(u8, arg, "--seed");
        if (!expect_tables and !expect_trees and !expect_levels and !expect_seed) {
            return error.InvalidArgument;
        }
        if (i + 1 >= args.len) return error.InvalidArgument;
        i += 1;
        const value = args[i];
        if (expect_tables) cfg.tables = try std.fmt.parseInt(usize, value, 10) else if (expect_trees) cfg.trees = try std.fmt.parseInt(usize, value, 10) else if (expect_levels) cfg.levels = try std.fmt.parseInt(usize, value, 10) else if (expect_seed) cfg.seed = try std.fmt.parseInt(u64, value, 0);
    }
    return cfg;
}

fn generate_tables(allocator: Allocator, cfg: Config) ![]Table {
    var prng = std.Random.DefaultPrng.init(cfg.seed);
    const random = prng.random();

    const tables = try allocator.alloc(Table, cfg.tables);
    for (tables, 0..) |*t, idx| {
        t.tree_id = @intCast((random.int(u16) % @as(u16, @intCast(cfg.trees))) + 1);
        t.level = @intCast(random.int(u8) % @as(u8, @intCast(cfg.levels)));
        // Spread key_max so sorts do work but allow duplicates.
        const base: u64 = random.int(u64) % 1_000_000;
        t.key_max = base * 16 + @as(u64, @intCast(idx % 16));
        t.snapshot_min = random.int(u64) % 10_000;
    }
    return tables;
}

const BenchResult = struct {
    ns: u128,
    checksum: u64,
};

fn bench_incremental(allocator: Allocator, data: []const Table, cfg: Config) !BenchResult {
    // Emulates the old “insert into sorted segmented array” behaviour with ArrayLists.
    var buckets = try allocator.alloc(std.ArrayList(Table), cfg.trees * cfg.levels);
    defer {
        for (buckets) |*b| b.deinit();
        allocator.free(buckets);
    }
    for (buckets) |*b| b.* = std.ArrayList(Table).init(allocator);

    const start = std.time.nanoTimestamp();
    for (data) |t| {
        const index = bucket_index(cfg, t.tree_id, t.level);
        var list = &buckets[index];
        const insert_at = binary_search(list.items, t);
        try list.insert(insert_at, t);
    }
    const end = std.time.nanoTimestamp();

    var checksum: u64 = 0;
    for (buckets) |list| {
        for (list.items) |t| checksum ^= t.key_max +% t.snapshot_min;
    }

    return .{ .ns = @intCast(end - start), .checksum = checksum };
}

fn bench_batch_sort(allocator: Allocator, data: []const Table, cfg: Config) !BenchResult {
    var buckets = try allocator.alloc(std.ArrayList(Table), cfg.trees * cfg.levels);
    defer {
        for (buckets) |*b| b.deinit();
        allocator.free(buckets);
    }
    for (buckets) |*b| b.* = std.ArrayList(Table).init(allocator);

    const start = std.time.nanoTimestamp();
    for (data) |t| {
        const index = bucket_index(cfg, t.tree_id, t.level);
        try buckets[index].append(t);
    }

    for (buckets) |*bucket| {
        if (bucket.items.len <= 1) continue;
        std.sort.block(Table, bucket.items, {}, less_table);
    }
    const end = std.time.nanoTimestamp();

    var checksum: u64 = 0;
    for (buckets) |bucket| {
        for (bucket.items) |t| checksum ^= t.key_max +% t.snapshot_min;
    }

    return .{ .ns = @intCast(end - start), .checksum = checksum };
}

fn bucket_index(cfg: Config, tree_id: u16, level: u8) usize {
    const tree_index: usize = @intCast(tree_id - 1);
    const level_index: usize = level;
    return tree_index * cfg.levels + level_index;
}

fn less_table(_: void, a: Table, b: Table) bool {
    if (a.key_max != b.key_max) return a.key_max < b.key_max;
    return a.snapshot_min < b.snapshot_min;
}

fn binary_search(items: []const Table, target: Table) usize {
    var left: usize = 0;
    var right: usize = items.len;
    while (left < right) {
        const mid = left + (right - left) / 2;
        const cmp = less_cmp(target, items[mid]);
        if (cmp) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    return left;
}

fn less_cmp(a: Table, b: Table) bool {
    if (a.key_max != b.key_max) return a.key_max < b.key_max;
    return a.snapshot_min < b.snapshot_min;
}
