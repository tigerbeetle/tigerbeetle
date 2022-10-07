//! Fuzz SuperBlock open()/checkpoint()/view_change().
//!
//! Invariants checked:
//!
//! - Crashing during a checkpoint() or view_change() is safe — open() will find a quorum.
//! - open()'s quorum never regresses.
//! - Calling checkpoint() and view_change() concurrently is safe.
//!   - VSRState does not leak.
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.superblock_fuzz);

const Storage = @import("../test/storage.zig").Storage;
const MessagePool = @import("../message_pool.zig").MessagePool;
const superblock_zone_size = @import("superblock.zig").superblock_zone_size;
const data_file_size_min = @import("superblock.zig").data_file_size_min;
const VSRState = @import("superblock.zig").SuperBlockSector.VSRState;
const SuperBlockType = @import("superblock.zig").SuperBlockType;
const SuperBlock = SuperBlockType(Storage);

// TODO Test invariants and transitions across TestRunner functions.
// TODO test corruption
// TODO test trailers

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
    try fuzz(allocator, seed);
}

pub fn fuzz(allocator: std.mem.Allocator, seed: u64) !void {
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    _ = random.boolean(); // TODO

    // SuperBlock's IO is all serial, so latencies never reorder reads/writes.
    const storage_options = .{
        .read_latency_min = 1,
        .read_latency_mean = 1,
        .write_latency_min = 1,
        .write_latency_mean = 1,
    };

    var storage = try Storage.init(allocator, superblock_zone_size, storage_options);
    defer storage.deinit(allocator);

    var storage_verify = try Storage.init(allocator, superblock_zone_size, storage_options);
    defer storage_verify.deinit(allocator);

    var message_pool = try MessagePool.init(allocator, .replica);
    defer message_pool.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, &storage, &message_pool);
    defer superblock.deinit(allocator);

    var superblock_verify = try SuperBlock.init(allocator, &storage_verify, &message_pool);
    defer superblock_verify.deinit(allocator);

    var vsr_states = std.AutoHashMap(u64, VSRState).init(allocator);
    defer vsr_states.deinit();

    var env = Environment{
        .superblock = &superblock,
        .superblock_verify = &superblock_verify,
        .vsr_states = vsr_states,
    };

    try env.format();
    try env.wait();

    env.open();
    try env.wait_and_verify();

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        // TODO what about calling this between IO tick()s? while IO is in progress...
        switch (random.uintLessThan(u8, 4)) {
            0 => try env.view_change(),
            1 => try env.checkpoint(),
            2 => {
                try env.checkpoint();
                try env.view_change();
            },
            3 => {
                try env.view_change();
                try env.checkpoint();
            },
            else => unreachable,
        }
        try env.wait_and_verify();
    }

    // TODO loop VC/checkpoint and vice versa
    // TODO also check only one at a time...
    // TODO inject faults?
}

// TODO Test invariants and transitions across Testenv functions.

const Environment = struct {
    superblock: *SuperBlock,
    superblock_verify: *SuperBlock,

    /// Verify that the working superblock after open() never regresses.
    latest_sequence: u64 = 1,
    latest_checksum: u128 = 0,
    latest_parent: u128 = 0,
    latest_vsr_state: VSRState = std.mem.zeroInit(VSRState, .{}),

    /// Track the expected VSRState for each superblock sequence.
    vsr_states: std.AutoHashMap(u64, VSRState),

    context_format: SuperBlock.Context = undefined,
    context_open: SuperBlock.Context = undefined,
    context_checkpoint: SuperBlock.Context = undefined,
    context_view_change: SuperBlock.Context = undefined,
    context_verify: SuperBlock.Context = undefined,

    pending: usize = 0,
    pending_verify: bool = false,

    const Task = enum { format, open, checkpoint, view_change };

    fn wait(env: *Environment) !void {
        while (env.pending > 0) {
            env.superblock.storage.tick();
        }
    }

    /// After every write to `superblock`'s storage, verify that the superblock can be opened,
    /// and the quorum never regresses.
    fn wait_and_verify(env: *Environment) !void {
        var tick: usize = 0;
        while (env.pending > 0) {
            env.superblock.storage.tick();
            assert(env.superblock.storage.reads.len + env.superblock.storage.writes.len <= 1);
            if (env.superblock.storage.writes.peek()) |write| {
                if (tick != write.done_at_tick) {
                    // Verify after every write.
                    tick = write.done_at_tick;
                    try env.verify();
                }
            }
        }
    }

    fn verify(env: *Environment) !void {
        const expectEqual = std.testing.expectEqual;
        // Duplicate the `superblock`'s storage, so that any repairs performed by
        // `superblock_verify.open()` do not pollute it.
        env.superblock_verify.storage.copy(env.superblock.storage);
        env.superblock_verify.opened = false; // Reuse the SuperBlock.
        env.superblock_verify.open(verify_callback, &env.context_verify);

        assert(!env.pending_verify);
        env.pending_verify = true;
        while (env.pending_verify) env.superblock_verify.storage.tick();

        assert(
            env.superblock_verify.working.checksum == env.superblock.working.checksum or
            env.superblock_verify.working.checksum == env.superblock.writing.checksum
        );

        // Verify the sequence we read from disk is monotonically increasing.
        if (env.latest_sequence < env.superblock_verify.working.sequence) {
            assert(
                env.latest_sequence + 1 == env.superblock_verify.working.sequence or
                env.latest_sequence + 2 == env.superblock_verify.working.sequence
            );

            if (env.latest_checksum != 0) {
                if (env.latest_sequence + 1 == env.superblock_verify.working.sequence) {
                    // After a checkpoint(), the parent points to the previous working sector.
                    try expectEqual(env.superblock_verify.working.parent, env.latest_checksum);
                }

                if (env.latest_sequence + 2 == env.superblock_verify.working.sequence) {
                    // After a view_change(), the parent is unchanged.
                    try expectEqual(env.superblock_verify.working.parent, env.latest_parent);
                }
            }

            try std.testing.expect(VSRState.monotonic(
                env.latest_vsr_state,
                env.superblock_verify.working.vsr_state,
            ));
            try std.testing.expectEqual(
                env.vsr_states.get(env.superblock_verify.working.sequence).?,
                env.superblock_verify.working.vsr_state,
            );

            env.latest_sequence = env.superblock_verify.working.sequence;
            env.latest_checksum = env.superblock_verify.working.checksum;
            env.latest_parent = env.superblock_verify.working.parent;
            env.latest_vsr_state = env.superblock_verify.working.vsr_state;
        } else {
            try expectEqual(env.latest_sequence, env.superblock_verify.working.sequence);
            try expectEqual(env.latest_checksum, env.superblock_verify.working.checksum);
            try expectEqual(env.latest_parent, env.superblock_verify.working.parent);
        }
    }

    fn verify_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_verify", context);
        env.pending_verify = false;
    }

    fn format(env: *Environment) !void {
        env.pending += 1;
        env.superblock.format(format_callback, &env.context_format, .{
            .cluster = 0,
            .replica = 0,
            .size_max = data_file_size_min,
        });

        try env.vsr_states.putNoClobber(1, std.mem.zeroInit(VSRState, .{}));
        try env.vsr_states.putNoClobber(2, std.mem.zeroInit(VSRState, .{}));
    }

    fn format_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_format", context);
        env.pending -= 1;
    }

    fn open(env: *Environment) void {
        env.pending += 1;
        env.superblock.open(open_callback, &env.context_open);
    }

    fn open_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_open", context);
        env.pending -= 1;

        assert(env.superblock.working.sequence == 2);
        assert(env.superblock.working.replica == 0);
        assert(env.superblock.working.cluster == 0);
    }

    fn view_change(env: *Environment) !void {
        const vsr_state = .{
            .commit_min_checksum = env.superblock.staging.vsr_state.commit_min_checksum,
            .commit_min = env.superblock.staging.vsr_state.commit_min,
            .commit_max = env.superblock.staging.vsr_state.commit_max + 3,
            .view_normal = env.superblock.staging.vsr_state.view_normal + 4,
            .view = env.superblock.staging.vsr_state.view + 5,
        };

        const sequence = env.superblock.writing.sequence + 2;
        try env.vsr_states.putNoClobber(sequence, vsr_state);

        env.pending += 1;
        env.superblock.view_change(view_change_callback, &env.context_view_change, vsr_state);
    }

    fn view_change_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_view_change", context);
        env.pending -= 1;
    }

    fn checkpoint(env: *Environment) !void {
        const vsr_state = .{
            .commit_min_checksum = env.superblock.staging.vsr_state.commit_min_checksum + 1,
            .commit_min = env.superblock.staging.vsr_state.commit_min + 1,
            .commit_max = env.superblock.staging.vsr_state.commit_max + 1,
            .view_normal = env.superblock.staging.vsr_state.view_normal,
            .view = env.superblock.staging.vsr_state.view,
        };

        const sequence = env.superblock.writing.sequence + 1;
        try env.vsr_states.putNoClobber(sequence, vsr_state);

        env.pending += 1;
        env.superblock.staging.vsr_state = vsr_state;
        env.superblock.checkpoint(checkpoint_callback, &env.context_checkpoint);
    }

    fn checkpoint_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_checkpoint", context);
        env.pending -= 1;
    }
};
