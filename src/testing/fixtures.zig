//! Convenient constructs for TigerBeetle components, for fuzzing and testing.
//!
//! Consider the Grid. In the actual database, there is only single call to Grid.init.
//! However, Grid is needed for most of our tests and fuzzers. If the init call is repeated
//! in every fuzzer, changing Storage creation flow becomes hard. To solve this, all fuzzers create
//! Grid through this file, such that we have one production and one test call to Grid.init.
//!
//! Design:
//!
//! - All functions take struct options as a last argument, even if it starts out as empty.
//!   All call-sites pass at least .{}, which makes adding new options cheap.
//! - Most options should have defaults. This is intentional deviation from TigerStyle, as, for
//!   tests, we gain a useful property: all options that are set are meaningful for a particular
//!   test.
//! - All dependent fixtures are passed in positionally because they can't be defaulted and their
//!   types are unique.
//! - It could be convenient to export types themselves, in addition to constructors, but we avoid
//!   introducing two ways to import something.
const std = @import("std");
const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const assert = std.debug.assert;

const Time = @import("../time.zig").Time;
const OffsetType = @import("./time.zig").OffsetType;
const Tracer = @import("../trace.zig").Tracer;
const Storage = @import("./storage.zig").Storage;
const SuperBlock = vsr.SuperBlockType(Storage);
const Grid = vsr.GridType(Storage);

const TimeSim = @import("./time.zig").TimeSim;

pub const cluster: u128 = 0;
pub const replica: u8 = 0;
pub const replica_count: u8 = 6;

pub fn init_time(options: struct {
    resolution: u64 = constants.tick_ms * std.time.ns_per_ms,
    offset_type: OffsetType = .linear,
    offset_coefficient_A: i64 = 0,
    offset_coefficient_B: i64 = 0,
    offset_coefficient_C: u32 = 0,
}) TimeSim {
    const result: TimeSim = .{
        .resolution = options.resolution,
        .offset_type = options.offset_type,
        .offset_coefficient_A = options.offset_coefficient_A,
        .offset_coefficient_B = options.offset_coefficient_B,
        .offset_coefficient_C = options.offset_coefficient_C,
    };
    return result;
}

pub fn init_tracer(gpa: std.mem.Allocator, init: Time, options: struct {
    writer: ?std.io.AnyWriter = null,
    process_id: Tracer.ProcessID = .{ .replica = .{ .cluster = cluster, .replica = replica } },
}) !Tracer {
    return Tracer.init(gpa, init, options.process_id, .{ .writer = options.writer });
}

pub fn init_storage(gpa: std.mem.Allocator, options: Storage.Options) !Storage {
    return try Storage.init(gpa, options);
}

pub fn storage_format(
    gpa: std.mem.Allocator,
    storage: *Storage,
    options: struct {
        cluster: u128 = cluster,
        replica: u8 = replica,
        replica_count: u8 = replica_count,
        release: vsr.Release = vsr.Release.minimum,
    },
) !void {
    assert(storage.reads.count() == 0);
    assert(storage.writes.count() == 0);

    var superblock = try init_superblock(gpa, storage, .{});
    defer superblock.deinit(gpa);

    const Context = struct {
        superblock_context: SuperBlock.Context = undefined,
        done: bool = false,
        fn callback(superblock_context: *SuperBlock.Context) void {
            const self: *@This() = @fieldParentPtr("superblock_context", superblock_context);
            assert(!self.done);
            self.done = true;
        }
    };
    var context: Context = .{};

    superblock.format(Context.callback, &context.superblock_context, .{
        .cluster = options.cluster,
        .replica = options.replica,
        .replica_count = options.replica_count,
        .release = options.release,
        .view = null,
    });
    for (0..10_000) |_| {
        if (context.done) break;
        storage.run();
    } else @panic("superblock format loop stuck");
    assert(storage.reads.count() == 0);
    assert(storage.writes.count() == 0);
}

pub fn init_superblock(gpa: std.mem.Allocator, storage: *Storage, options: struct {
    storage_size_limit: ?u64 = null,
}) !SuperBlock {
    return try SuperBlock.init(gpa, storage, .{
        .storage_size_limit = options.storage_size_limit orelse storage.size,
    });
}

pub fn init_grid(gpa: std.mem.Allocator, trace: *Tracer, superblock: *SuperBlock, options: struct {
    missing_blocks_max: u64 = 0,
    missing_tables_max: u64 = 0,
    blocks_released_prior_checkpoint_durability_max: u64 = 0,
}) !Grid {
    return try Grid.init(gpa, .{
        .superblock = superblock,
        .trace = trace,
        .missing_blocks_max = options.missing_blocks_max,
        .missing_tables_max = options.missing_tables_max,
        .blocks_released_prior_checkpoint_durability_max = //
        options.blocks_released_prior_checkpoint_durability_max,
    });
}

pub fn open_superblock(superblock: *SuperBlock) void {
    const storage: *Storage = superblock.storage;
    assert(storage.reads.count() == 0);
    assert(storage.writes.count() == 0);
    defer assert(storage.reads.count() == 0);
    defer assert(storage.writes.count() == 0);

    const Context = struct {
        superblock_context: SuperBlock.Context = undefined,
        pending: bool = false,

        fn callback(superblock_context: *SuperBlock.Context) void {
            const self: *@This() = @fieldParentPtr("superblock_context", superblock_context);
            assert(self.pending);
            self.pending = false;
        }
    };
    var context: Context = .{};

    context.pending = true;
    superblock.open(Context.callback, &context.superblock_context);
    for (0..10_000) |_| {
        storage.run();
        if (!context.pending) break;
    } else @panic("open superblock stuck");
}

pub fn open_grid(grid: *Grid) void {
    const storage: *Storage = grid.superblock.storage;
    assert(storage.reads.count() == 0);
    assert(storage.writes.count() == 0);
    defer assert(storage.reads.count() == 0);
    defer assert(storage.writes.count() == 0);

    const Context = struct {
        pending: bool = false,
        // NB: This lacks in elegance and robustness, but is good enough for testing.
        var global: @This() = .{};
        fn callback(_: *Grid) void {
            assert(@This().global.pending);
            @This().global.pending = false;
        }
    };

    assert(!Context.global.pending);
    Context.global.pending = true;
    grid.open(Context.callback);
    for (0..10_000) |_| {
        storage.run();
        if (!Context.global.pending) break;
    } else @panic("open grid stuck");
}
