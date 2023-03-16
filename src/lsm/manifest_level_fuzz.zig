//! Fuzz ManifestLevel insert_table()/set_snapshot_max()/remove_table*()/iterator().
//!
//! Invariants checked:
//! 
//! - Inserted tables become visible (and invisible, given the snapshot) from the iterator.
//! - Tables with updated snapshot_max become invisible to future snapshots from the iterator.
//! - Tables visible to the current/future snapshot can be removed.
//! - Tables invisible to the current/future/past snapshots can be removed.
//! - Tables can be inserted, updated, and removed in any order.
//!
const std = @import("std");
const assert = std.debug.assert;
const allocator = std.testing.allocator;

const log = std.log.scoped(.lsm_manifest_level_fuzz);
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const binary_search = @import("binary_search.zig");
const lsm = @import("tree.zig");

const Key = u64;
const Value = struct {
    key: Key,
    tombstone: bool,
};

inline fn compare_keys(a: Key, b: Key) std.math.Order {
    return std.math.order(a, b);
}

inline fn key_from_value(value: *const Value) Key {
    return value.key;
}

inline fn tombstone_from_key(key: Key) Value {
    return .{ .key = key, .tombstone = true };
}

inline fn tombstone(value: *const Value) bool {
    return value.tombstone;
}

const TableInfo = @import("manifest.zig").TableInfoType(Table);
const Table = @import("table.zig").TableType(
    Key,
    Value,
    compare_keys,
    key_from_value,
    std.math.maxInt(Key),
    tombstone,
    tombstone_from_key,
    1, // Doesn't matter for this test.
    .general,
);

pub const tigerbeetle_config = @import("../config.zig").configs.test_min;

pub fn main() !void {
    const args = try fuzz.parse_fuzz_args(allocator);

    var prng = std.rand.DefaultPrng.init(args.seed);
    const random = prng.random();

    const fuzz_op_count = std.math.min(
        args.events_max orelse @as(usize, 2e5),
        fuzz.random_int_exponential(random, usize, 1e4),
    );

    const fuzz_ops = try generate_fuzz_ops(random, fuzz_op_count);
    defer allocator.free(fuzz_ops);

    const table_count_max = 1024;
    const node_size = 1024;
    try EnvironmentType(table_count_max, node_size).run_fuzz_ops(random, fuzz_ops);
}

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    insert_tables: usize,
    update_tables: usize,
    take_snapshot,
    remove_visible: usize,
    remove_invisible: usize,
};

const max_tables_per_insert = 10;

fn generate_fuzz_ops(random: std.rand.Random, fuzz_op_count: usize) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        .insert_tables = 10,
        .update_tables = 5,
        .take_snapshot = 3,
        .remove_visible = 3,
        .remove_invisible = 3,
    };
    log.info("fuzz_op_distribution = {d:.2}", .{fuzz_op_distribution});

    var ctx = GenerateContext{ .random = random };
    for (fuzz_ops) |*fuzz_op| {
        fuzz_op.* = ctx.next(fuzz.random_enum(random, FuzzOpTag, fuzz_op_distribution));
    }

    return fuzz_ops;
}

const GenerateContext = struct {
    inserted: usize = 0,
    invisible: usize = 0,
    random: std.rand.Random,

    fn next(ctx: *GenerateContext, fuzz_op_tag: FuzzOpTag) FuzzOp {
        switch (fuzz_op_tag) {
            .insert_tables => {
                const amount = ctx.random.intRangeAtMostBiased(usize, 1, max_tables_per_insert);
                ctx.inserted += amount;
                return FuzzOp{ .insert_tables = amount };
            },
            .update_tables => {
                // TODO: Try generating another op instead.
                // For now, insert more tables to be updated in the future:
                const visible = ctx.inserted - ctx.invisible;
                if (visible == 0) return ctx.next(.insert_tables);

                const amount = ctx.random.intRangeAtMostBiased(usize, 1, visible);
                ctx.invisible += amount;
                return FuzzOp{ .update_tables = amount };
            },
            .take_snapshot => {
                ctx.invisible = ctx.inserted;
                return FuzzOp.take_snapshot;
            },
            .remove_invisible => {
                // TODO: Try generating another op instead.
                // For now, update more tables to be removed as invisible in the future:
                const invisible = ctx.invisible;
                if (invisible == 0) return ctx.next(.update_tables);

                // Decide if all invisible tables should be removed.
                var amount = ctx.random.intRangeAtMostBiased(usize, 1, invisible);
                if (ctx.random.boolean()) amount = invisible;

                ctx.inserted -= amount;
                ctx.invisible -= amount;
                return FuzzOp{ .remove_invisible = amount };
            },
            .remove_visible => {
                // TODO: Try generating another op instead.
                // For now, insert more tables to be removed as visible in the future:
                const visible = ctx.inserted - ctx.invisible;
                if (visible == 0) return ctx.next(.insert_tables);

                // Decide if all visible tables should be removed.
                var amount = ctx.random.intRangeAtMostBiased(usize, 1, visible);
                if (ctx.random.boolean()) amount = visible;

                ctx.inserted -= visible;
                assert(ctx.invisible <= ctx.inserted);
                return FuzzOp{ .remove_visible = amount };
            },
        }
    }
};

fn EnvironmentType(comptime table_count_max: u32, comptime node_size: u32) type {
    return struct {
        const Environment = @This();

        const TableBuffer = std.ArrayList(TableInfo);
        const NodePool = @import("node_pool.zig").NodePool(node_size, @alignOf(TableInfo));
        const ManifestLevel = @import("manifest_level.zig").ManifestLevelType(
            NodePool,
            Key,
            TableInfo,
            compare_keys,
            table_count_max,
        );

        pool: NodePool,
        level: ManifestLevel,
        buffer: TableBuffer,
        tables: TableBuffer,
        random: std.rand.Random,
        snapshot: u64,

        pub fn run_fuzz_ops(random: std.rand.Random, fuzz_ops: []const FuzzOp) !void {
            var env: Environment = undefined;

            const node_pool_size = ManifestLevel.Keys.node_count_max +
                ManifestLevel.Tables.node_count_max;
            env.pool = try NodePool.init(allocator, node_pool_size);
            defer env.pool.deinit(allocator);

            env.level = try ManifestLevel.init(allocator);
            defer env.level.deinit(allocator, &env.pool);

            env.buffer = TableBuffer.init(allocator);
            defer env.buffer.deinit();

            env.tables = TableBuffer.init(allocator);
            defer env.tables.deinit();

            env.random = random;
            env.snapshot = 1; // the first snapshot is reserved.

            for (fuzz_ops) |fuzz_op, op_index| {
                log.debug("Running fuzz_ops[{}/{}] == {}", .{ op_index, fuzz_ops.len, fuzz_op });
                switch (fuzz_op) {
                    .insert_tables => |amount| try env.insert_tables(amount),
                    .update_tables => |amount| try env.update_tables(amount),
                    .take_snapshot => try env.take_snapshot(),
                    .remove_invisible => |amount| try env.remove_invisible(amount),
                    .remove_visible => |amount| try env.remove_visible(amount),
                }
            }
        }

        fn insert_tables(env: *Environment, amount: usize) !void {
            assert(env.buffer.items.len == 0);

            // Generate random, non-overlapping TableInfo's into env.buffer:
            {
                var insert_count: usize = 0;
                const insert_max = @minimum(amount, table_count_max - env.level.keys.len());
                var key = env.random.uintAtMostBiased(Key, table_count_max * 64);

                while (insert_count < insert_max) : (insert_count += 1) {
                    const table = env.generate_non_overlapping_table(key);
                    try env.buffer.append(table);
                    key = table.key_max;
                }
            }

            const tables = env.buffer.items;
            defer env.buffer.clearRetainingCapacity();

            // Insert the generated tables into the ManifestLevel:
            for (tables) |*table| {
                assert(table.visible(env.snapshot));
                assert(table.visible(lsm.snapshot_latest));
                env.level.insert_table(&env.pool, table);
            }

            // Insert the generated tables into the Environment for reference:
            for (tables) |*table| {
                const index = binary_search.binary_search_values_raw(
                    Key,
                    TableInfo,
                    key_min_from_table,
                    compare_keys,
                    env.tables.items,
                    table.key_max,
                    .{},
                );

                // Can't be equal as the tables may not overlap
                if (index < env.tables.items.len) {
                    assert(env.tables.items[index].key_min > table.key_max);
                }

                try env.tables.insert(index, table.*);
            }
        }

        fn generate_non_overlapping_table(env: *Environment, key: Key) TableInfo {
            var new_key_min = key + env.random.uintLessThanBiased(Key, 31) + 1;
            assert(compare_keys(new_key_min, key) == .gt);

            var i = binary_search.binary_search_values_raw(
                Key,
                TableInfo,
                key_min_from_table,
                compare_keys,
                env.tables.items,
                new_key_min,
                .{},
            );

            if (i > 0) {
                if (compare_keys(new_key_min, env.tables.items[i - 1].key_max) != .gt) {
                    new_key_min = env.tables.items[i - 1].key_max + 1;
                }
            }

            const next_key_min = for (env.tables.items[i..]) |table| {
                switch (compare_keys(new_key_min, table.key_min)) {
                    .lt => break table.key_min,
                    .eq => new_key_min = table.key_max + 1,
                    .gt => unreachable,
                }
            } else std.math.maxInt(Key);

            const max_delta = @minimum(32, next_key_min - 1 - new_key_min);
            const new_key_max = new_key_min + env.random.uintAtMostBiased(Key, max_delta);

            return .{
                .checksum = env.random.int(u128),
                // zero addresses are used to indicate the table being removed.
                .address = env.random.intRangeAtMostBiased(u64, 1, std.math.maxInt(u64)),
                .snapshot_min = env.snapshot,
                .key_min = new_key_min,
                .key_max = new_key_max,
            };
        }

        inline fn key_min_from_table(table: *const TableInfo) Key {
            return table.key_min;
        }

        fn update_tables(env: *Environment, amount: usize) !void {
            var update_amount = amount;
            assert(amount > 0);

            // Check if there's any tables to update:
            const iter_range = @intCast(u32, env.tables.items.len);
            if (iter_range == 0) return;

            // TODO: use level.iterator() + binary_search(env.tables) instead.
            // Iterate tables in a random order (until enough tables have been updated):
            // https://lemire.me/blog/2017/09/18/visiting-all-values-in-an-array-exactly-once-in-random-order/
            var index = env.random.uintLessThanBiased(u32, iter_range);
            var iter_count = iter_range;
            while (iter_count > 0 and update_amount > 0) : (iter_count -= 1) {
                assert(index < iter_range);
                defer {
                    index += iter_range - 1;
                    if (index >= iter_range) index -= iter_range;
                }

                // Update the snapshot_max of tables which are visible:
                const table = &env.tables.items[index];
                if (table.visible(lsm.snapshot_latest)) {
                    assert(table.visible(env.snapshot));

                    const snapshot_max = env.snapshot;
                    env.level.set_snapshot_max(snapshot_max, table);

                    assert(table.snapshot_max == snapshot_max);
                    assert(!table.visible(lsm.snapshot_latest));
                    update_amount -= 1;
                }
            }
        }

        fn take_snapshot(env: *Environment) !void {
            env.snapshot += 1;
            assert(env.snapshot < lsm.snapshot_latest);
        }

        fn remove_invisible(env: *Environment, amount: usize) !void {
            var remove_amount = amount;
            assert(amount > 0);

            // Decide whether updated tables in the current snapshot should be removed.
            var snapshots = [_]u64{lsm.snapshot_latest};
            if (env.random.boolean()) snapshots[0] = env.snapshot;

            // Remove invisible tables from ManifestLevel and mark them as removed in env.tables:
            var it = env.level.iterator(.invisible, &snapshots, .descending, null);
            while (it.next()) |level_table| {
                env.mark_removed_table(level_table);

                assert(level_table.invisible(&snapshots));
                env.level.remove_table_invisible(&env.pool, &snapshots, level_table);

                remove_amount -= 1;
                if (remove_amount == 0) break;
            }

            try env.purge_removed_tables();
        }

        fn remove_visible(env: *Environment, amount: usize) !void {
            var remove_amount = amount;
            assert(amount > 0);

            // Remove visible tables from ManifestLevel and mark them as removed in env.tables:
            const snapshots = @as(*const [1]u64, &lsm.snapshot_latest);
            var it = env.level.iterator(.visible, snapshots, .descending, null);
            while (it.next()) |level_table| {
                env.mark_removed_table(level_table);

                assert(level_table.visible(lsm.snapshot_latest));
                const removed = env.level.remove_table_visible(&env.pool, level_table);
                assert(removed.equal(level_table));

                remove_amount -= 1;
                if (remove_amount == 0) break;
            }

            try env.purge_removed_tables();
        }

        /// Mark the matching table as removed in env.tables.
        /// It will be removed from env.tables from a later call to env.purge_removed_tables().
        fn mark_removed_table(env: *Environment, level_table: *const TableInfo) void {
            // TODO: use i = binary_search(tables, key_min); binary_search(tables[i..], key_max)
            const env_table = for (env.tables.items) |*env_table| {
                if (level_table.equal(env_table)) break env_table;
            } else unreachable;

            // Zero address means the table is removed.
            assert(level_table.equal(env_table));
            assert(env_table.address != 0);
            env_table.address = 0;
        }

        /// Filter out all env.tables removed with env.mark_removed_table() by copying all
        /// non-removed tables into env.buffer and flipping it with env.tables.
        /// TODO: This clears removed tables in O(n). Use a better way.
        fn purge_removed_tables(env: *Environment) !void {
            assert(env.buffer.items.len == 0);
            for (env.tables.items) |*table| {
                if (table.address == 0) continue;
                try env.buffer.append(table.*);
            }

            std.mem.swap(TableBuffer, &env.buffer, &env.tables);
            env.buffer.clearRetainingCapacity();
        }
    };
}
