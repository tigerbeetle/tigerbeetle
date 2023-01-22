//! Fuzz ManifestLog open()/insert()/remove()/compact()/checkpoint().
//!
//! Invariants checked:
//!
//! - Checkpoint flushes all buffered log blocks (including partial blocks).
//! - The state of the ManifestLog/SuperBlock.Manifest immediately after recovery matches
//!   the state of the ManifestLog/SuperBlock.Manifest immediately after the latest checkpoint.
//! - SuperBlock.Manifest.open() only returns the latest version of each table.
//! - SuperBlock.Manifest's compaction queue contains any blocks which:
//!   - contain fewer than entry_count_max entries, or
//!   - contain a "remove" entry, or
//!   - contain an overridden entry.
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_lsm_manifest_log);

const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const MessagePool = @import("../message_pool.zig").MessagePool;
const SuperBlock = @import("../vsr/superblock.zig").SuperBlockType(Storage);
const data_file_size_min = @import("../vsr/superblock.zig").data_file_size_min;
const TableExtent = @import("../vsr/superblock_manifest.zig").Manifest.TableExtent;
const Storage = @import("../test/storage.zig").Storage;
const Grid = @import("grid.zig").GridType(Storage);
const BlockType = @import("grid.zig").BlockType;
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage, TableInfo);
const fuzz = @import("../test/fuzz.zig");

pub const tigerbeetle_config = @import("../config.zig").configs.test_min;

const entries_max_block = ManifestLog.Block.entry_count_max;
const entries_max_buffered = entries_max_block *
    std.meta.fieldInfo(ManifestLog, .blocks).field_type.count_max;

pub fn main() !void {
    const allocator = std.testing.allocator;
    const args = try fuzz.parse_fuzz_args(allocator);

    var prng = std.rand.DefaultPrng.init(args.seed);

    const events_count = std.math.min(
        args.events_max orelse @as(usize, 2e5),
        fuzz.random_int_exponential(prng.random(), usize, 1e4),
    );

    const events = try generate_events(allocator, prng.random(), events_count);
    defer allocator.free(events);

    try run_fuzz(allocator, prng.random(), events);
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

    // The MessagePool is shared by both superblocks because they will not use it.
    var message_pool = try MessagePool.init(allocator, .replica);
    defer message_pool.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = constants.storage_size_max,
        .message_pool = &message_pool,
    });
    defer superblock.deinit(allocator);

    var superblock_verify = try SuperBlock.init(allocator, .{
        .storage = &storage_verify,
        .storage_size_limit = constants.storage_size_max,
        .message_pool = &message_pool,
    });
    defer superblock_verify.deinit(allocator);

    var grid = try Grid.init(allocator, &superblock);
    defer grid.deinit(allocator);

    var grid_verify = try Grid.init(allocator, &superblock_verify);
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

    for (events) |event| {
        log.debug("event={}", .{event});
        switch (event) {
            .insert => |e| try env.insert(e.level, &e.table),
            .remove => |e| try env.remove(e.level, &e.table),
            .compact => try env.compact(),
            .checkpoint => try env.checkpoint(),
            .noop => {},
        }
    }
}

const ManifestEvent = union(enum) {
    insert: struct { level: u7, table: TableInfo },
    remove: struct { level: u7, table: TableInfo },
    compact: void,
    checkpoint: void,
    /// The random EventType could not be generated — this simplifies event generation.
    noop: void,
};

fn generate_events(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    events_count: usize,
) ![]const ManifestEvent {
    const EventType = enum {
        insert_new,
        insert_change_level,
        insert_change_snapshot,
        remove,
        compact,
        checkpoint,
    };

    const events = try allocator.alloc(ManifestEvent, events_count);
    errdefer allocator.free(events);

    var event_distribution = fuzz.random_enum_distribution(random, EventType);
    // Don't remove too often, so that there are plenty of tables accumulating.
    event_distribution.remove /= @intToFloat(f64, constants.lsm_levels);
    // Don't compact or checkpoint too often, to approximate a real workload.
    // Additionally, checkpoint is slow because of the verification, so run it less
    // frequently.
    event_distribution.compact /= @intToFloat(
        f64,
        constants.lsm_levels * constants.lsm_batch_multiple,
    );
    event_distribution.checkpoint /= @intToFloat(
        f64,
        constants.lsm_levels * constants.journal_slot_count,
    );

    log.info("event_distribution = {d:.2}", .{event_distribution});
    log.info("event_count = {d}", .{events.len});

    var tables = std.ArrayList(struct {
        level: u7,
        table: TableInfo,
    }).init(allocator);
    defer tables.deinit();

    // The number of appends since the last flush (compact or checkpoint).
    var append_count: usize = 0;
    for (events) |*event, i| {
        const event_type = blk: {
            if (append_count == ManifestLog.compaction_appends_max) {
                // We must compact or checkpoint periodically to avoid overfilling the ManifestLog.
                break :blk if (random.boolean()) EventType.compact else EventType.checkpoint;
            }

            const event_type_random = fuzz.random_enum(random, EventType, event_distribution);
            if (tables.items.len == 0) {
                if (event_type_random == .insert_change_level or
                    event_type_random == .insert_change_snapshot or
                    event_type_random == .remove)
                {
                    break :blk .insert_new;
                }
            }

            break :blk event_type_random;
        };

        event.* = switch (event_type) {
            .insert_new => insert: {
                const level = random.uintLessThan(u7, constants.lsm_levels);
                const table = TableInfo{
                    .checksum = 0,
                    .address = i + 1,
                    .snapshot_min = 1,
                    .snapshot_max = 2,
                    .key_min = 0,
                    .key_max = 0,
                };
                try tables.append(.{
                    .level = level,
                    .table = table,
                });
                const insert = ManifestEvent{ .insert = .{
                    .level = level,
                    .table = table,
                } };
                break :insert insert;
            },

            .insert_change_level => insert: {
                const table = &tables.items[random.uintLessThan(usize, tables.items.len)];
                if (table.level == constants.lsm_levels - 1) {
                    break :insert ManifestEvent{ .noop = {} };
                }

                table.level += 1;
                const insert = ManifestEvent{ .insert = .{
                    .level = table.level,
                    .table = table.table,
                } };
                break :insert insert;
            },

            .insert_change_snapshot => insert: {
                const table = &tables.items[random.uintLessThan(usize, tables.items.len)];
                table.table.snapshot_max += 1;
                const insert = ManifestEvent{ .insert = .{
                    .level = table.level,
                    .table = table.table,
                } };
                break :insert insert;
            },

            .remove => remove: {
                const table = tables.swapRemove(random.uintLessThan(usize, tables.items.len));
                const remove = ManifestEvent{ .remove = .{
                    .level = table.level,
                    .table = table.table,
                } };
                break :remove remove;
            },

            .compact => ManifestEvent{ .compact = {} },
            .checkpoint => ManifestEvent{ .checkpoint = {} },
        };

        switch (event.*) {
            .compact, .checkpoint => append_count = 0,
            .noop => {},
            else => append_count += 1,
        }
    }
    return events;
}

const TableInfo = extern struct {
    checksum: u128,
    address: u64,
    flags: u64 = 0,
    snapshot_min: u64,
    snapshot_max: u64 = std.math.maxInt(u64),
    key_min: u128,
    key_max: u128,

    comptime {
        assert(@sizeOf(TableInfo) == 48 + 16 * 2);
        assert(@alignOf(TableInfo) == 16);
        assert(@bitSizeOf(TableInfo) == @sizeOf(TableInfo) * 8);
    }
};

const Environment = struct {
    allocator: std.mem.Allocator,
    superblock_context: SuperBlock.Context = undefined,
    manifest_log: ManifestLog,
    manifest_log_verify: ManifestLog,
    manifest_log_model: ManifestLogModel,
    manifest_log_opening: ?ManifestLogModel.TableMap = null,
    manifest_log_reserved: bool = false,
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

        const tree_hash = std.math.maxInt(u128);
        var manifest_log = try ManifestLog.init(allocator, options.grid, tree_hash);
        errdefer manifest_log.deinit(allocator);

        var manifest_log_verify = try ManifestLog.init(allocator, options.grid_verify, tree_hash);
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
            // manifest_log.grid.tick();
            manifest_log.superblock.storage.tick();
        }
    }

    fn format_superblock(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.manifest_log.superblock.format(format_superblock_callback, &env.superblock_context, .{
            .cluster = 0,
            .replica = 0,
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
        assert(!env.manifest_log_reserved);

        env.pending += 1;
        env.manifest_log.open(open_event, open_callback);
        env.manifest_log.reserve();
        env.manifest_log_reserved = true;
    }

    fn open_event(manifest_log: *ManifestLog, level: u7, table: *const TableInfo) void {
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

    fn insert(env: *Environment, level: u7, table: *const TableInfo) !void {
        if (!env.manifest_log_reserved) env.manifest_log.reserve();
        env.manifest_log_reserved = true;

        try env.manifest_log_model.insert(level, table);
        env.manifest_log.insert(level, table);
    }

    fn remove(env: *Environment, level: u7, table: *const TableInfo) !void {
        if (!env.manifest_log_reserved) env.manifest_log.reserve();
        env.manifest_log_reserved = true;

        try env.manifest_log_model.remove(level, table);
        env.manifest_log.remove(level, table);
    }

    fn compact(env: *Environment) !void {
        if (!env.manifest_log_reserved) env.manifest_log.reserve();
        env.manifest_log_reserved = true;

        env.pending += 1;
        env.manifest_log.compact(compact_callback);
        env.wait(&env.manifest_log);

        env.manifest_log_reserved = false;
    }

    fn compact_callback(manifest_log: *ManifestLog) void {
        const env = @fieldParentPtr(Environment, "manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn checkpoint(env: *Environment) !void {
        // Checkpoint always follows compaction.
        try env.compact();

        try env.manifest_log_model.checkpoint();

        env.pending += 1;
        env.manifest_log.checkpoint(checkpoint_callback);
        env.wait(&env.manifest_log);

        const vsr_state = &env.manifest_log.superblock.working.vsr_state;

        env.pending += 1;
        env.manifest_log.superblock.checkpoint(
            checkpoint_superblock_callback,
            &env.superblock_context,
            .{
                .commit_min_checksum = vsr_state.commit_min_checksum + 1,
                .commit_min = vsr_state.commit_min + 1,
                .commit_max = vsr_state.commit_max + 1,
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
                    .message_pool = env.manifest_log.superblock.client_table.message_pool,
                },
            );

            test_grid.deinit(env.allocator);
            test_grid.* = try Grid.init(env.allocator, test_superblock);

            test_manifest_log.deinit(env.allocator);
            test_manifest_log.* = try ManifestLog.init(
                env.allocator,
                test_grid,
                env.manifest_log.tree_hash,
            );
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

        try verify_manifest(&test_superblock.manifest, &env.manifest_log.superblock.manifest);
        try verify_manifest_compaction_set(test_superblock, &env.manifest_log_model);
    }

    fn verify_superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "superblock_context", superblock_context);
        env.pending -= 1;
    }

    fn verify_manifest_open_event(
        manifest_log_verify: *ManifestLog,
        level: u7,
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
        level: u7,
        table: TableInfo,
    };

    /// Stores table updates that are not yet checkpointed.
    const AppendList = std.ArrayList(struct {
        event: enum { insert, remove },
        level: u7,
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

    fn insert(model: *ManifestLogModel, level: u7, table: *const TableInfo) !void {
        try model.appends.append(.{
            .event = .insert,
            .level = level,
            .table = table.*,
        });
    }

    fn remove(model: *ManifestLogModel, level: u7, table: *const TableInfo) !void {
        try model.appends.append(.{
            .event = .remove,
            .level = level,
            .table = table.*,
        });
    }

    fn checkpoint(model: *ManifestLogModel) !void {
        for (model.appends.items) |append| {
            switch (append.event) {
                .insert => {
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

fn verify_manifest(
    expect: *const SuperBlock.Manifest,
    actual: *const SuperBlock.Manifest,
) !void {
    try std.testing.expectEqual(expect.count, actual.count);
    try std.testing.expectEqual(expect.count_max, actual.count_max);

    const c = expect.count;
    try std.testing.expect(std.mem.eql(u128, expect.trees[0..c], actual.trees[0..c]));
    try std.testing.expect(std.mem.eql(u128, expect.checksums[0..c], actual.checksums[0..c]));
    try std.testing.expect(std.mem.eql(u64, expect.addresses[0..c], actual.addresses[0..c]));

    try std.testing.expect(hash_map_equals(
        SuperBlock.Manifest.TableExtentKey,
        SuperBlock.Manifest.TableExtent,
        &expect.tables,
        &actual.tables,
    ));
    try std.testing.expect(hash_map_equals(
        u64,
        void,
        &expect.compaction_set,
        &actual.compaction_set,
    ));
}

fn verify_manifest_compaction_set(
    superblock: *const SuperBlock,
    manifest_log_model: *const ManifestLogModel,
) !void {
    var compact_blocks_checked: u32 = 0;

    // This test doesn't include any actual table blocks, so all blocks are manifest blocks.
    var blocks = superblock.free_set.blocks.iterator(.{ .kind = .set });
    while (blocks.next()) |block_index| {
        const block_address = block_index + 1;
        const block = superblock.storage.grid_block(block_address);
        const block_header = std.mem.bytesToValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
        try std.testing.expectEqual(BlockType.manifest.operation(), block_header.operation);

        const entry_count = ManifestLog.Block.entry_count(block);
        var compact_soon: bool = entry_count < ManifestLog.Block.entry_count_max;
        for (ManifestLog.Block.labels_const(block)[0..entry_count]) |label, i| {
            const table = &ManifestLog.Block.tables_const(block)[i];
            compact_soon = compact_soon or switch (label.event) {
                .remove => true,
                .insert => blk: {
                    const table_current = manifest_log_model.current(table.address);
                    break :blk table_current == null or
                        table_current.?.level != label.level or
                        table_current.?.table.snapshot_min != table.snapshot_min or
                        table_current.?.table.snapshot_max != table.snapshot_max;
                },
            };
        }
        try std.testing.expectEqual(
            compact_soon,
            superblock.manifest.compaction_set.contains(block_address),
        );
        compact_blocks_checked += @boolToInt(compact_soon);
    }

    // There are no blocks queued for compaction which were not allocated in the FreeSet.
    try std.testing.expectEqual(superblock.manifest.compaction_set.count(), compact_blocks_checked);
}

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
