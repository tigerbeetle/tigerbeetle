//! Fuzz ManifestLog open()/insert()/remove()/compact()/checkpoint().
//!
//! Invariants checked:
//!
//! - Checkpoint flushes all buffered log blocks (including partial blocks).
//! - Recovering from a checkpoint restores the state of the ManifestLog immediately after the
//!   checkpoint was created.
//! - SuperBlock.Manifest.open() only returns the latest version of each table.
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_lsm_manifest_log);

const config = @import("../config.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const MessagePool = @import("../message_pool.zig").MessagePool;
const SuperBlock = @import("../vsr/superblock.zig").SuperBlockType(Storage);
const data_file_size_min = @import("../vsr/superblock.zig").data_file_size_min;
const TableExtent = @import("../vsr/superblock_manifest.zig").Manifest.TableExtent;
const Storage = @import("../test/storage.zig").Storage;
const Grid = @import("grid.zig").GridType(Storage);
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage, TableInfo);
const fuzz = @import("../test/fuzz.zig");

const storage_size_max = data_file_size_min + config.block_size * 1024;

const entries_max_per_block = ManifestLog.entry_count_max;
const entries_max_buffered = entries_max_per_block *
    std.meta.fieldInfo(ManifestLog, .blocks).field_type.count_max;

pub fn main() !void {
    const allocator = std.testing.allocator;
    var args = std.process.args();

    // Discard executable name.
    allocator.free(try args.next(allocator).?);

    var seed: u64 = undefined;
    if (args.next(allocator)) |arg_or_error| {
        const arg = try arg_or_error;
        defer allocator.free(arg);

        seed = std.fmt.parseInt(u64, arg, 10) catch |err| {
            std.debug.panic("Invalid seed: '{}'; err: {}", .{
                std.zig.fmtEscapes(arg),
                err,
            });
        };
    } else {
        try std.os.getrandom(std.mem.asBytes(&seed));
    }

    log.info("seed={}", .{ seed });

    var prng = std.rand.DefaultPrng.init(seed);

    const events = try generate_events(allocator, prng.random());
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

        .read_fault_probability = 0, // TODO Remove when defaults are available.
        .write_fault_probability = 0, // TODO Remove when defaults are available.
    };

    var storage = try Storage.init(allocator, storage_size_max, storage_options, 0, undefined);
    defer storage.deinit(allocator);

    var storage_verify = try Storage.init(allocator, storage_size_max, storage_options, 0, undefined);
    defer storage_verify.deinit(allocator);

    // The MessagePool is shared by both superblocks because they will not use it.
    var message_pool = try MessagePool.init(allocator, .replica);
    defer message_pool.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, &storage, &message_pool);
    defer superblock.deinit(allocator);

    var superblock_verify = try SuperBlock.init(allocator, &storage_verify, &message_pool);
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
        log.debug("event={}", .{ event });
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
) ![]const ManifestEvent {
    const EventType = enum {
        insert_new,
        insert_change_level,
        insert_change_snapshot,
        remove,
        compact,
        checkpoint,
    };

    const events = try allocator.alloc(ManifestEvent, std.math.min(
        @as(usize, 1E7),
        fuzz.random_int_exponential(random, usize, 1e5),
    ));
    errdefer allocator.free(events);

    var event_distribution = fuzz.random_enum_distribution(random, EventType);
    event_distribution[@enumToInt(EventType.insert_new)] *= 10.0;
    event_distribution[@enumToInt(EventType.insert_change_level)] *= 2.0;
    event_distribution[@enumToInt(EventType.insert_change_snapshot)] *= 2.0;
    log.info("event_distribution = {d:.2}", .{ event_distribution });
    log.info("event_count = {d}", .{ events.len });

    var tables = std.ArrayList(struct {
        level: u7,
        table: TableInfo,
    }).init(allocator);
    defer tables.deinit();

    var entry_count: usize = 0;
    for (events) |*event, i| {
        event.* = if (entry_count == entries_max_buffered)
            // We must compact or checkpoint periodically to avoid overfilling the ManifestLog.
            if (random.boolean()) ManifestEvent{ .compact = {} }
            else ManifestEvent{ .checkpoint = {} }
        else
        switch (fuzz.random_enum(random, EventType, event_distribution)) {
            .insert_new => insert: {
                const level = random.uintLessThan(u7, config.lsm_levels);
                const table = .{
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
                break :insert ManifestEvent{ .insert = .{
                    .level = level,
                    .table = table,
                } };
            },

            .insert_change_level => insert: {
                if (tables.items.len == 0) break :insert ManifestEvent{ .noop = {} };
                const table = &tables.items[random.uintLessThan(usize, tables.items.len)];
                if (table.level == config.lsm_levels - 1) break :insert ManifestEvent{ .noop = {} };

                table.level += 1;
                break :insert ManifestEvent{ .insert = .{
                    .level = table.level,
                    .table = table.table,
                } };
            },

            .insert_change_snapshot => insert: {
                if (tables.items.len == 0) break :insert ManifestEvent{ .noop = {} };

                const table = &tables.items[random.uintLessThan(usize, tables.items.len)];
                table.table.snapshot_max += 1;
                break :insert ManifestEvent{ .insert = .{
                    .level = table.level,
                    .table = table.table,
                } };
            },

            .remove => remove: {
                if (tables.items.len == 0) break :remove ManifestEvent{ .noop = {} };

                const table = tables.swapRemove(random.uintLessThan(usize, tables.items.len));
                break :remove ManifestEvent{ .remove = .{
                    .level = table.level,
                    .table = table.table,
                } };
            },

            .compact => ManifestEvent{ .compact = {} },
            .checkpoint => ManifestEvent{ .checkpoint = {} },
        };

        switch (event.*) {
            .compact => {
                if (entry_count >= entries_max_buffered) {
                    entry_count -= entries_max_per_block;
                }
            },
            .checkpoint => entry_count = 0,
            .noop => {},
            else => entry_count += 1,
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
            manifest_log.grid.tick();
            manifest_log.superblock.storage.tick();
        }
    }

    fn format_superblock(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.manifest_log.superblock.format(format_superblock_callback, &env.superblock_context, .{
            .cluster = 0,
            .replica = 0,
            .size_max = storage_size_max,
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
        try env.manifest_log_model.insert(level, table);
        env.manifest_log.insert(level, table);
    }

    fn remove(env: *Environment, level: u7, table: *const TableInfo) !void {
        try env.manifest_log_model.remove(level, table);
        env.manifest_log.remove(level, table);
    }

    fn compact(env: *Environment) !void {
        env.pending += 1;
        env.manifest_log.compact(compact_callback);
        env.wait(&env.manifest_log);
    }

    fn compact_callback(manifest_log: *ManifestLog) void {
        const env = @fieldParentPtr(Environment, "manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn checkpoint(env: *Environment) !void {
        try env.manifest_log_model.checkpoint();

        env.pending += 1;
        env.manifest_log.checkpoint(checkpoint_callback);
        env.wait(&env.manifest_log);

        env.pending += 1;
        env.manifest_log.superblock.checkpoint(checkpoint_superblock_callback, &env.superblock_context);
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
                test_storage,
                env.manifest_log.superblock.client_table.message_pool,
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
        env.wait(&env.manifest_log_verify);

        const test_manifest = &test_superblock.manifest;
        const manifest = &env.manifest_log.superblock.manifest;
        assert(test_manifest.count == manifest.count);
        assert(test_manifest.count_max == manifest.count_max);
        assert(hash_map_equals(
            u64,
            SuperBlock.Manifest.TableExtent,
            &test_manifest.tables,
            &manifest.tables,
        ));
        assert(hash_map_equals(u64, void, &test_manifest.compaction_set, &manifest.compaction_set));

        const c = test_manifest.count;
        assert(std.mem.eql(u128, test_manifest.trees[0..c], manifest.trees[0..c]));
        assert(std.mem.eql(u128, test_manifest.checksums[0..c], manifest.checksums[0..c]));
        assert(std.mem.eql(u64, test_manifest.addresses[0..c], manifest.addresses[0..c]));
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
    /// Stores the checkpointed version of every table.
    /// Indexed by table address.
    const TableMap = std.AutoHashMap(u64, struct {
        level: u7,
        table: TableInfo,
    });

    tables: TableMap,

    appends: RingBuffer(struct {
        event: enum { insert, remove },
        level: u7,
        table: TableInfo,
    }, entries_max_buffered, .array) = .{},

    fn init(allocator: std.mem.Allocator) !ManifestLogModel {
        const tables = TableMap.init(allocator);
        errdefer tables.deinit(allocator);

        return ManifestLogModel{ .tables = tables };
    }

    fn deinit(model: *ManifestLogModel) void {
        model.tables.deinit();
    }

    fn insert(model: *ManifestLogModel, level: u7, table: *const TableInfo) !void {
        model.appends.push_assume_capacity(.{
            .event = .insert,
            .level = level,
            .table = table.*,
        });
    }

    fn remove(model: *ManifestLogModel, level: u7, table: *const TableInfo) !void {
        model.appends.push_assume_capacity(.{
            .event = .remove,
            .level = level,
            .table = table.*,
        });
    }

    fn checkpoint(model: *ManifestLogModel) !void {
        while (model.appends.pop()) |append| {
            try model.tables.put(append.table.address, .{
                .level = append.level,
                .table = append.table,
            });
        }
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
