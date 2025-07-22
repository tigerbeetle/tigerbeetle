//! Convenient constors for TigerBeetle components, for fuzzing and testing.
//!
//! Consider Storage. In the actual database, there is only single call to Storage.init.
//! However, Storage is needed for most of our tests and fuzzers. If the init call is repeated
//! in every fuzzer, changing Storage creation flow becomes hard. To solve this, all fuzzers create
//! Storage through this file, such that we have one production and one test call to Storage.init.
//!
//! Design:
//!
//! - All fuctions take struct options as a last argument, even if it starts out as empty.
//!   All call-sites pass at least .{}, which makes adding new options cheap.
//! - Options should be spelled-out in this file, rather re-using StorageOptions and the like, to
//!   help the reader see the full API at a glance.
//! - Most options should have defaults. This is intentional deviation from TigerStyle, as, for
//!   tests, we gain a useful property: all options that are set are meaningful for a particular
//!   test.
//! - All dependent fixtures are passed in positionally because they can't be defaulted and their
//!   types are unique.
//! - It could be convenient to export types themselves, in addition to constructors, but we avoid
//!   introducing two ways to import something.
const std = @import("std");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const assert = std.debug.assert;
const Ratio = stdx.PRNG.Ratio;
const Duration = stdx.Duration;

const Time = @import("../time.zig").Time;
const OffsetType = @import("./time.zig").OffsetType;
const Tracer = @import("../trace.zig").Tracer;
const Storage = @import("./storage.zig").Storage;
const ClusterFaultAtlas = @import("./storage.zig").ClusterFaultAtlas;
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

pub const StorageOptions = struct {
    size: u32,

    seed: u64 = 0,

    read_latency_min: Duration = .{ .ns = 0 },
    read_latency_mean: Duration = .{ .ns = 0 },

    write_latency_min: Duration = .{ .ns = 0 },
    write_latency_mean: Duration = .{ .ns = 0 },

    read_fault_probability: Ratio = .zero(),
    write_fault_probability: Ratio = .zero(),
    write_misdirect_probability: Ratio = .zero(),
    crash_fault_probability: Ratio = .zero(),

    fault_atlas: ?*const ClusterFaultAtlas = null,
};

pub fn init_storage(
    gpa: std.mem.Allocator,
    options: StorageOptions,
) !Storage {
    return try Storage.init(gpa, options.size, .{
        .seed = options.seed,
        .read_latency_min = options.read_latency_min,
        .read_latency_mean = options.read_latency_mean,
        .write_latency_min = options.write_latency_min,
        .write_latency_mean = options.write_latency_mean,
        // Faults makes sense only in the cluster.
        .read_fault_probability = .zero(),
        .write_fault_probability = .zero(),
        .write_misdirect_probability = .zero(),
        .crash_fault_probability = .zero(),
    });
}

pub fn storage_format(
    gpa: std.mem.Allocator,
    storage: *Storage,
    format_options: struct {
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
        .cluster = format_options.cluster,
        .replica = format_options.replica,
        .replica_count = format_options.replica_count,
        .release = format_options.release,
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
    return try SuperBlock.init(gpa, .{
        .storage = storage,
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
    assert(superblock.storage.reads.count() == 0);
    assert(superblock.storage.writes.count() == 0);
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

    superblock.open(Context.callback, &context.superblock_context);
    for (0..10_000) |_| {
        if (context.done) break;
        superblock.storage.run();
    } else @panic("superblock open loop stuck");

    assert(superblock.storage.reads.count() == 0);
    assert(superblock.storage.writes.count() == 0);
}
