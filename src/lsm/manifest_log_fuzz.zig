//! Fuzz ManifestLog open()/insert()/update()/remove()/compact()/checkpoint().
//!
//! Invariants checked:
//!
//! - Checkpoint flushes all buffered log blocks (including partial blocks).
//! - The state of the ManifestLog immediately after recovery matches
//!   the state of the ManifestLog immediately after the latest checkpoint.
//! - ManifestLog.open() only returns the latest version of each table.
//! - The ManifestLog performs enough compaction to not "fall behind" (i.e. run out of blocks).
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_lsm_manifest_log);
const maybe = stdx.maybe;

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const SuperBlock = @import("../vsr/superblock.zig").SuperBlockType(Storage);
const Storage = @import("../testing/storage.zig").Storage;
const Grid = @import("../vsr/grid.zig").GridType(Storage);
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);
const ManifestLogOptions = @import("manifest_log.zig").Options;
const fuzz = @import("../testing/fuzz.zig");
const schema = @import("./schema.zig");
const tree = @import("./tree.zig");
const TableInfo = schema.ManifestNode.TableInfo;

pub const tigerbeetle_config = @import("../config.zig").configs.fuzz_min;

const manifest_log_options = ManifestLogOptions{
    .tree_id_min = 1,
    // Use many trees so that we fill manifest blocks quickly.
    // (This makes it easier to hit "worst case" scenarios in manifest compaction pacing.)
    .tree_id_max = 20,
    // Use a artificially low table-count-max so that we can easily fill the manifest log and verify
    // that pacing is correct.
    .forest_table_count_max = schema.ManifestNode.entry_count_max * 100,
};

const pace = @import("manifest_log.zig").Pace.init(.{
    .tree_count = manifest_log_options.forest_tree_count(),
    .tables_max = manifest_log_options.forest_table_count_max,
    .half_bar_compact_blocks_extra = constants.lsm_manifest_compact_blocks_extra,
});

pub fn main() !void {
    const allocator = fuzz.allocator;
    const args = try fuzz.parse_fuzz_args(allocator);

    var prng = std.rand.DefaultPrng.init(args.seed);

    const events_count = @min(
        args.events_max orelse @as(usize, 1e7),
        fuzz.random_int_exponential(prng.random(), usize, 1e6),
    );

    const events = try generate_events(allocator, prng.random(), events_count);
    defer allocator.free(events);

    try run_fuzz(allocator, prng.random(), events);
    log.info("Passed!", .{});
}

fn run_fuzz(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    events: []const ManifestEvent,
) !void {
    const storage_options = .{
        .seed = random.int(u64),
        .read_latency_min = 1,
        .read_latency_mean = 1 + random.uintLessThan(u64, 40),
        .write_latency_min = 1,
        .write_latency_mean = 1 + random.uintLessThan(u64, 40),
    };

    var storage = try Storage.init(allocator, constants.storage_size_max, storage_options);
    defer storage.deinit(allocator);

    var storage_verify = try Storage.init(allocator, constants.storage_size_max, storage_options);
    defer storage_verify.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = constants.storage_size_max,
    });
    defer superblock.deinit(allocator);

    var superblock_verify = try SuperBlock.init(allocator, .{
        .storage = &storage_verify,
        .storage_size_limit = constants.storage_size_max,
    });
    defer superblock_verify.deinit(allocator);

    var grid = try Grid.init(allocator, .{
        .superblock = &superblock,
        .missing_blocks_max = 0,
        .missing_tables_max = 0,
    });
    defer grid.deinit(allocator);

    var grid_verify = try Grid.init(allocator, .{
        .superblock = &superblock_verify,
        .missing_blocks_max = 0,
        .missing_tables_max = 0,
    });
    defer grid_verify.deinit(allocator);

    var env = try Environment.init(allocator, .{
        .grid = &grid,
        .grid_verify = &grid_verify,
    });
    defer env.deinit(allocator);

    {
        env.format_superblock();
        env.wait(&env.manifest_log);

        env.open_superblock();
        env.wait(&env.manifest_log);

        env.open();
        env.wait(&env.manifest_log);
    }

    // The manifest doesn't compact during the first bar.
    for (0..2) |_| {
        try env.half_bar_commence();
        try env.half_bar_complete();
    }

    try env.half_bar_commence();

    for (events) |event| {
        log.debug("event={}", .{event});
        switch (event) {
            .insert => |e| try env.insert(e.level, &e.table),
            .update => |e| try env.update(e.level, &e.table),
            .remove => |e| try env.remove(e.level, &e.table),
            .compact => {
                try env.half_bar_complete();
                try env.half_bar_commence();
            },
            .checkpoint => {
                // Checkpoint always immediately follows compaction.
                try env.half_bar_complete();
                try env.checkpoint();
                try env.half_bar_commence();
            },
            .noop => {},
        }
    }

    try env.half_bar_complete();
}

const ManifestEvent = union(enum) {
    insert: struct { level: u6, table: TableInfo },
    update: struct { level: u6, table: TableInfo },
    remove: struct { level: u6, table: TableInfo },
    compact,
    checkpoint,
    /// The random EventType could not be generated — this simplifies event generation.
    noop,
};

fn generate_events(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    events_count: usize,
) ![]const ManifestEvent {
    var events = std.ArrayList(ManifestEvent).init(allocator);
    errdefer events.deinit();

    var tables = std.ArrayList(struct { level: u6, table: TableInfo }).init(allocator);
    defer tables.deinit();

    // The maximum number of (live) tables that the manifest has at any point in time.
    var tables_max: usize = 0;

    // Dummy table address for Table Infos.
    var table_address: u64 = 1;

    const compacts_per_checkpoint = fuzz.random_int_exponential(random, usize, 16);
    log.info("compacts_per_checkpoint = {d}", .{compacts_per_checkpoint});

    // When true, create as many entries as possible.
    // This tries to test the manifest upper-bound calculation.
    const fill_always = random.uintLessThan(usize, 4) == 0;

    // The maximum number of snapshot-max updates per half-bar.
    const updates_max = tree.compactions_max * tree.compaction_tables_input_max;

    while (events.items.len < events_count) {
        const fill = fill_always or random.boolean();
        // All of the trees we are inserting/modifying have the same id (for simplicity), but we
        // want to perform more updates if there are more trees, to better simulate a real state
        // machine.
        for (manifest_log_options.tree_id_min..manifest_log_options.tree_id_max + 1) |_| {
            const operations: struct {
                update_levels: usize,
                update_snapshots: usize,
                inserts: usize,
            } = operations: {
                const move = !fill and random.uintLessThan(usize, 10) == 0;
                if (move) {
                    break :operations .{
                        .update_levels = 1,
                        .update_snapshots = 0,
                        .inserts = 0,
                    };
                } else {
                    const updates =
                        if (fill) updates_max else random.uintAtMost(usize, updates_max);
                    break :operations .{
                        .update_levels = 0,
                        .update_snapshots = updates,
                        .inserts = updates,
                    };
                }
            };

            for (0..operations.inserts) |_| {
                if (tables.items.len == manifest_log_options.forest_table_count_max) break;

                const level = random.uintLessThan(u6, constants.lsm_levels);
                const table = TableInfo{
                    .checksum = 0,
                    .address = table_address,
                    .snapshot_min = 1,
                    .snapshot_max = std.math.maxInt(u64),
                    .key_min = .{0} ** 16,
                    .key_max = .{0} ** 16,
                    .tree_id = 1,
                };

                table_address += 1;
                try tables.append(.{ .level = level, .table = table });
                try events.append(.{ .insert = .{ .level = level, .table = table } });
            }
            tables_max = @max(tables_max, tables.items.len);

            for (0..operations.update_levels) |_| {
                if (tables.items.len == 0) break;

                const table = &tables.items[random.uintLessThan(usize, tables.items.len)];
                if (table.level < constants.lsm_levels - 1) table.level += 1;
                try events.append(.{ .update = .{ .level = table.level, .table = table.table } });
            }

            for (0..operations.update_snapshots) |_| {
                if (tables.items.len == 0) break;

                const table = &tables.items[random.uintLessThan(usize, tables.items.len)];
                // Only update a table snapshot_max once (like real compaction).
                if (table.table.snapshot_max == 2) continue;
                table.table.snapshot_max = 2;
                try events.append(.{ .update = .{ .level = table.level, .table = table.table } });
            }
        }

        // We apply removes only after all inserts/updates (rather than mixing them together) to
        // mimic how compaction is followed by remove_invisible_tables().
        var i: usize = 0;
        while (i < tables.items.len) {
            if (tables.items[i].table.snapshot_max == 2) {
                const table = tables.swapRemove(i);
                try events.append(.{ .remove = .{ .level = table.level, .table = table.table } });
            } else {
                i += 1;
            }
        }

        if (random.uintAtMost(usize, compacts_per_checkpoint) == 0) {
            try events.append(.compact);
        } else {
            try events.append(.checkpoint);
        }
    }
    log.info("event_count = {d}", .{events.items.len});
    log.info("tables_max = {d}/{d}", .{ tables_max, manifest_log_options.forest_table_count_max });

    return events.toOwnedSlice();
}

const Environment = struct {
    allocator: std.mem.Allocator,
    superblock_context: SuperBlock.Context = undefined,
    manifest_log: ManifestLog,
    manifest_log_verify: ManifestLog,
    manifest_log_model: ManifestLogModel,
    manifest_log_opening: ?ManifestLogModel.TableMap = null,
    pending: usize = 0,

    fn init(
        allocator: std.mem.Allocator,
        options: struct {
            grid: *Grid,
            grid_verify: *Grid,
        },
    ) !Environment {
        var manifest_log_model = try ManifestLogModel.init(allocator);
        errdefer manifest_log_model.deinit();

        var manifest_log = try ManifestLog.init(allocator, options.grid, manifest_log_options);
        errdefer manifest_log.deinit(allocator);

        var manifest_log_verify =
            try ManifestLog.init(allocator, options.grid_verify, manifest_log_options);
        errdefer manifest_log_verify.deinit(allocator);

        return Environment{
            .allocator = allocator,
            .manifest_log = manifest_log,
            .manifest_log_verify = manifest_log_verify,
            .manifest_log_model = manifest_log_model,
        };
    }

    fn deinit(env: *Environment, allocator: std.mem.Allocator) void {
        env.manifest_log.deinit(allocator);
        env.manifest_log_verify.deinit(env.allocator);
        env.manifest_log_model.deinit();
        assert(env.manifest_log_opening == null);
    }

    fn wait(env: *Environment, manifest_log: *ManifestLog) void {
        while (env.pending > 0) {
            manifest_log.superblock.storage.tick();
        }
    }

    fn format_superblock(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.manifest_log.superblock.format(format_superblock_callback, &env.superblock_context, .{
            .cluster = 0,
            .replica = 0,
            .replica_count = 6,
        });
    }

    fn format_superblock_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "superblock_context", context);
        env.pending -= 1;
    }

    fn open_superblock(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.manifest_log.superblock.open(open_superblock_callback, &env.superblock_context);
    }

    fn open_superblock_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "superblock_context", context);
        env.pending -= 1;
    }

    fn open(env: *Environment) void {
        assert(env.pending == 0);

        env.pending += 1;
        env.manifest_log.open(open_event, open_callback);
    }

    fn open_event(manifest_log: *ManifestLog, level: u6, table: *const TableInfo) void {
        _ = manifest_log;
        _ = level;
        _ = table;

        // This ManifestLog is only opened during setup, when it has no blocks.
        unreachable;
    }

    fn open_callback(manifest_log: *ManifestLog) void {
        const env = @fieldParentPtr(Environment, "manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn insert(env: *Environment, level: u6, table: *const TableInfo) !void {
        try env.manifest_log_model.insert(level, table);
        env.manifest_log.insert(level, table);
    }

    fn update(env: *Environment, level: u6, table: *const TableInfo) !void {
        try env.manifest_log_model.update(level, table);
        env.manifest_log.update(level, table);
    }

    fn remove(env: *Environment, level: u6, table: *const TableInfo) !void {
        try env.manifest_log_model.remove(level, table);
        env.manifest_log.remove(level, table);
    }

    fn half_bar_commence(env: *Environment) !void {
        env.pending += 1;
        env.manifest_log.compact(
            manifest_log_compact_callback,
            vsr.Checkpoint.checkpoint_after(
                env.manifest_log.superblock.working.vsr_state.checkpoint.commit_min,
            ) + 1,
        );
        env.wait(&env.manifest_log);
    }

    fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
        const env = @fieldParentPtr(Environment, "manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn half_bar_complete(env: *Environment) !void {
        env.manifest_log.compact_end();
    }

    fn checkpoint(env: *Environment) !void {
        assert(env.manifest_log.grid_reservation == null);

        try env.manifest_log_model.checkpoint();

        env.pending += 1;
        env.manifest_log.checkpoint(checkpoint_callback);
        env.wait(&env.manifest_log);

        const vsr_state = &env.manifest_log.superblock.working.vsr_state;

        {
            // VSRState.monotonic() asserts that the previous_checkpoint id changes.
            // In a normal replica this is guaranteed – even if the LSM is idle and no blocks
            // are acquired or released, the client sessions are necessarily mutated.
            var reply = std.mem.zeroInit(vsr.Header, .{
                .cluster = 0,
                .command = .reply,
                .op = vsr_state.checkpoint.commit_min + 1,
                .commit = vsr_state.checkpoint.commit_min + 1,
            });
            reply.set_checksum_body(&.{});
            reply.set_checksum();

            _ = env.manifest_log.superblock.client_sessions.put(1, &reply);
        }

        env.pending += 1;
        env.manifest_log.superblock.checkpoint(
            checkpoint_superblock_callback,
            &env.superblock_context,
            .{
                .manifest_references = env.manifest_log.checkpoint_references(),
                .commit_min_checksum = vsr_state.checkpoint.commit_min_checksum + 1,
                .commit_min = vsr.Checkpoint.checkpoint_after(vsr_state.checkpoint.commit_min),
                .commit_max = vsr.Checkpoint.checkpoint_after(vsr_state.commit_max),
                .sync_op_min = 0,
                .sync_op_max = 0,
            },
        );
        env.wait(&env.manifest_log);

        try env.verify();
    }

    fn checkpoint_callback(manifest_log: *ManifestLog) void {
        const env = @fieldParentPtr(Environment, "manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn checkpoint_superblock_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "superblock_context", context);
        env.pending -= 1;
    }

    /// Verify that the state of a ManifestLog restored from checkpoint matches the state
    /// immediately after the checkpoint was created.
    fn verify(env: *Environment) !void {
        const test_superblock = env.manifest_log_verify.superblock;
        const test_storage = test_superblock.storage;
        const test_grid = env.manifest_log_verify.grid;
        const test_manifest_log = &env.manifest_log_verify;

        {
            test_storage.copy(env.manifest_log.superblock.storage);
            test_storage.reset();

            // Reset the state so that the manifest log (and dependencies) can be reused.
            // Do not "defer deinit()" because these are cleaned up by Env.deinit().
            test_superblock.deinit(env.allocator);
            test_superblock.* = try SuperBlock.init(
                env.allocator,
                .{
                    .storage = test_storage,
                    .storage_size_limit = constants.storage_size_max,
                },
            );

            test_grid.deinit(env.allocator);
            test_grid.* = try Grid.init(env.allocator, .{
                .superblock = test_superblock,
                .missing_blocks_max = 0,
                .missing_tables_max = 0,
            });

            test_manifest_log.deinit(env.allocator);
            test_manifest_log.* =
                try ManifestLog.init(env.allocator, test_grid, manifest_log_options);
        }

        env.pending += 1;
        test_superblock.open(verify_superblock_open_callback, &env.superblock_context);
        env.wait(test_manifest_log);

        assert(env.manifest_log_opening == null);
        env.manifest_log_opening = try env.manifest_log_model.tables.clone();
        defer {
            assert(env.manifest_log_opening.?.count() == 0);
            env.manifest_log_opening.?.deinit();
            env.manifest_log_opening = null;
        }

        env.pending += 1;
        test_manifest_log.open(verify_manifest_open_event, verify_manifest_open_callback);
        env.wait(test_manifest_log);

        try std.testing.expect(hash_map_equals(
            u64,
            ManifestLog.TableExtent,
            &env.manifest_log.table_extents,
            &test_manifest_log.table_extents,
        ));
    }

    fn verify_superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "superblock_context", superblock_context);
        env.pending -= 1;
    }

    fn verify_manifest_open_event(
        manifest_log_verify: *ManifestLog,
        level: u6,
        table: *const TableInfo,
    ) void {
        const env = @fieldParentPtr(Environment, "manifest_log_verify", manifest_log_verify);
        assert(env.pending > 0);

        const expect = env.manifest_log_opening.?.get(table.address).?;
        assert(expect.level == level);
        assert(std.meta.eql(expect.table, table.*));
        assert(env.manifest_log_opening.?.remove(table.address));
    }

    fn verify_manifest_open_callback(manifest_log_verify: *ManifestLog) void {
        const env = @fieldParentPtr(Environment, "manifest_log_verify", manifest_log_verify);
        env.pending -= 1;
    }
};

const ManifestLogModel = struct {
    /// Stores the latest checkpointed version of every table.
    /// Indexed by table address.
    const TableMap = std.AutoHashMap(u64, TableEntry);

    const TableEntry = struct {
        level: u6,
        table: TableInfo,
    };

    /// Stores table updates that are not yet checkpointed.
    const AppendList = std.ArrayList(struct {
        event: enum { insert, update, remove },
        level: u6,
        table: TableInfo,
    });

    tables: TableMap,
    appends: AppendList,

    fn init(allocator: std.mem.Allocator) !ManifestLogModel {
        const tables = TableMap.init(allocator);
        errdefer tables.deinit(allocator);

        const appends = AppendList.init(allocator);
        errdefer appends.deinit(allocator);

        return ManifestLogModel{
            .tables = tables,
            .appends = appends,
        };
    }

    fn deinit(model: *ManifestLogModel) void {
        model.tables.deinit();
        model.appends.deinit();
    }

    fn current(model: ManifestLogModel, table_address: u64) ?TableEntry {
        assert(model.appends.items.len == 0);

        return model.tables.get(table_address);
    }

    fn insert(model: *ManifestLogModel, level: u6, table: *const TableInfo) !void {
        try model.appends.append(.{
            .event = .insert,
            .level = level,
            .table = table.*,
        });
    }

    fn update(model: *ManifestLogModel, level: u6, table: *const TableInfo) !void {
        try model.appends.append(.{
            .event = .update,
            .level = level,
            .table = table.*,
        });
    }

    fn remove(model: *ManifestLogModel, level: u6, table: *const TableInfo) !void {
        try model.appends.append(.{
            .event = .remove,
            .level = level,
            .table = table.*,
        });
    }

    fn checkpoint(model: *ManifestLogModel) !void {
        for (model.appends.items) |append| {
            switch (append.event) {
                .insert,
                .update,
                => {
                    try model.tables.put(append.table.address, .{
                        .level = append.level,
                        .table = append.table,
                    });
                },
                .remove => {
                    const removed = model.tables.get(append.table.address).?;
                    assert(removed.level == append.level);
                    assert(std.meta.eql(removed.table, append.table));
                    assert(model.tables.remove(append.table.address));
                },
            }
        }
        model.appends.clearRetainingCapacity();
    }
};

fn hash_map_equals(
    comptime K: type,
    comptime V: type,
    a: *const std.AutoHashMapUnmanaged(K, V),
    b: *const std.AutoHashMapUnmanaged(K, V),
) bool {
    if (a.count() != b.count()) return false;

    var a_iterator = a.iterator();
    while (a_iterator.next()) |a_entry| {
        const a_value = a_entry.value_ptr.*;
        const b_value = b.get(a_entry.key_ptr.*) orelse return false;
        if (!std.meta.eql(a_value, b_value)) return false;
    }
    return true;
}
