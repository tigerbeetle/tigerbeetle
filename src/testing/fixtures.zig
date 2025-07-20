const std = @import("std");
const assert = std.debug.assert;
const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const Time = @import("../time.zig").Time;
const TimeSim = @import("./time.zig").TimeSim;
const Tracer = @import("../trace.zig").Tracer;
const Storage = @import("./storage.zig").Storage;
const SuperBlock = @import("../vsr/superblock.zig").SuperBlockType(Storage);
const data_file_size_min = @import("../vsr/superblock.zig").data_file_size_min;
const Grid = @import("../vsr/grid.zig").GridType(Storage);
const Ratio = stdx.PRNG.Ratio;

pub fn time(options: struct {}) TimeSim {
    _ = options;
    return .{
        .resolution = constants.tick_ms * std.time.ns_per_ms,
        .offset_type = .linear,
        .offset_coefficient_A = 0,
        .offset_coefficient_B = 0,
    };
}

pub fn tracer(gpa: std.mem.Allocator, t: Time, options: struct {
    cluster: u128 = 0,
    replica: u8 = 0,
}) !Tracer {
    return Tracer.init(
        gpa,
        t,
        .{ .replica = .{ .cluster = options.cluster, .replica = options.replica } },
        .{},
    );
}

pub fn storage(
    gpa: std.mem.Allocator,
    options: struct {
        // Default to small size to make sure that tests that can be fast are fast.
        size: u32 = 4096,

        seed: u64 = 0,

        read_latency_min: u64 = 0,
        read_latency_mean: u64 = 0,
        write_latency_min: u64 = 0,
        write_latency_mean: u64 = 0,
        read_fault_probability: Ratio = Ratio.zero(),
        write_fault_probability: Ratio = Ratio.zero(),
        write_misdirect_probability: Ratio = Ratio.zero(),
        crash_fault_probability: Ratio = Ratio.zero(),

        format: ?struct {
            cluster: u128 = 0,
            replica: u8 = 0,
            replica_count: u8 = constants.replicas_max,
            release: vsr.Release = vsr.Release.minimum,
        } = null,
    },
) !Storage {
    var result = try Storage.init(gpa, options.size, .{
        .seed = options.seed,
        .read_latency_min = options.read_latency_min,
        .read_latency_mean = options.read_latency_mean,
        .write_latency_min = options.write_latency_min,
        .write_latency_mean = options.write_latency_mean,
        .read_fault_probability = options.read_fault_probability,
        .write_fault_probability = options.write_fault_probability,
        .write_misdirect_probability = options.write_misdirect_probability,
        .crash_fault_probability = options.crash_fault_probability,
    });

    if (options.format) |format_options| {
        var formatter = try superblock(gpa, &result, .{});
        defer formatter.deinit(gpa);

        var format_context: struct {
            superblock_context: SuperBlock.Context = undefined,
            done: bool = false,
            fn callback(superblock_context: *SuperBlock.Context) void {
                const this: *@This() = @fieldParentPtr("superblock_context", superblock_context);
                assert(!this.done);
                this.done = true;
            }
        } = .{};

        formatter.format(@TypeOf(format_context).callback, &format_context.superblock_context, .{
            .cluster = format_options.cluster,
            .replica = format_options.replica,
            .replica_count = format_options.replica_count,
            .release = format_options.release,
            .view = null,
        });
        for (0..10_000) |_| {
            if (format_context.done) break;
            result.run();
        } else @panic("format loop stuck");
    }

    return result;
}

pub fn superblock(gpa: std.mem.Allocator, s: *Storage, options: struct {
    storage_size_limit: ?u64 = null,
}) !SuperBlock {
    return try SuperBlock.init(gpa, .{
        .storage = s,
        .storage_size_limit = options.storage_size_limit orelse s.size,
    });
}

pub fn grid(gpa: std.mem.Allocator, trace: *Tracer, s: *SuperBlock, options: struct {
    missing_blocks_max: u64 = 0,
    missing_tables_max: u64 = 0,
    blocks_released_prior_checkpoint_durability_max: u64 = 0,
}) !Grid {
    return try Grid.init(gpa, .{
        .superblock = s,
        .trace = trace,
        .missing_blocks_max = options.missing_blocks_max,
        .missing_tables_max = options.missing_tables_max,
        .blocks_released_prior_checkpoint_durability_max = //
        options.blocks_released_prior_checkpoint_durability_max,
    });
}
