//! Fuzz SuperBlock open()/checkpoint()/view_change().
//!
//! Invariants checked:
//!
//! - Crashing during a checkpoint() or view_change().
//!   - open() finds a quorum, even with the interference of disk faults.
//!   - open()'s quorum never regresses.
//! - Calling checkpoint() and view_change() concurrently is safe.
//!   - VSRState will not leak before the corresponding checkpoint()/view_change().
//!   - Trailers will not leak before the corresponding checkpoint().
//! - view_change_in_progress() reports the correct state.
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_vsr_superblock);

const constants = @import("../constants.zig");
const util = @import("../util.zig");
const vsr = @import("../vsr.zig");
const Storage = @import("../test/storage.zig").Storage;
const MessagePool = @import("../message_pool.zig").MessagePool;
const superblock_zone_size = @import("superblock.zig").superblock_zone_size;
const data_file_size_min = @import("superblock.zig").data_file_size_min;
const VSRState = @import("superblock.zig").SuperBlockSector.VSRState;
const SuperBlockType = @import("superblock.zig").SuperBlockType;
const SuperBlock = SuperBlockType(Storage);
const fuzz = @import("../test/fuzz.zig");

pub const tigerbeetle_config = @import("../config.zig").configs.test_min;

const cluster = 0;

pub fn main() !void {
    const allocator = std.testing.allocator;
    const args = try fuzz.parse_fuzz_args(allocator);

    // Total calls to checkpoint() + view_change().
    const transitions_count_total = args.events_max orelse 10;

    try run_fuzz(allocator, args.seed, transitions_count_total);
}

fn run_fuzz(allocator: std.mem.Allocator, seed: u64, transitions_count_total: usize) !void {
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const storage_options = .{
        .seed = random.int(u64),
        // SuperBlock's IO is all serial, so latencies never reorder reads/writes.
        .read_latency_min = 1,
        .read_latency_mean = 1,
        .write_latency_min = 1,
        .write_latency_mean = 1,
        // Storage will never inject more faults than the superblock is able to recover from,
        // so a 100% fault probability is allowed.
        .read_fault_probability = 25 + random.uintLessThan(u8, 76),
        .write_fault_probability = 25 + random.uintLessThan(u8, 76),
        .crash_fault_probability = 50 + random.uintLessThan(u8, 51),
        .faulty_superblock = true,
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

    var sequence_states = Environment.SequenceStates.init(allocator);
    defer sequence_states.deinit();

    var env = Environment{
        .sequence_states = sequence_states,
        .superblock = &superblock,
        .superblock_verify = &superblock_verify,
    };

    try env.format();
    while (env.pending.count() > 0) env.superblock.storage.tick();

    env.open();
    while (env.pending.count() > 0) env.superblock.storage.tick();

    try env.verify();
    assert(env.pending.count() == 0);
    assert(env.latest_sequence == 1);

    var transitions: usize = 0;
    while (transitions < transitions_count_total or env.pending.count() > 0) {
        if (transitions < transitions_count_total) {
            // TODO bias the RNG
            if (env.pending.count() == 0) {
                transitions += 1;
                if (random.boolean()) {
                    try env.checkpoint();
                } else {
                    try env.view_change();
                }
            }

            if (env.pending.count() == 1 and random.uintLessThan(u8, 6) == 0) {
                transitions += 1;
                if (env.pending.contains(.view_change)) {
                    try env.checkpoint();
                } else {
                    try env.view_change();
                }
            }
        }

        assert(env.pending.count() > 0);
        assert(env.pending.count() <= 2);
        try env.tick();

        // Trailers are only updated on-disk by checkpoint(), never view_change().
        // Trailers must not be mutated while a checkpoint() is in progress.
        if (!env.pending.contains(.checkpoint) and random.boolean()) {
            const range = env.superblock.free_set.reserve(1).?;
            _ = env.superblock.free_set.acquire(range).?;
            env.superblock.free_set.forfeit(range);
        }
    }
}

const Environment = struct {
    /// Track the expected value of parameters at a particular sequence.
    /// Indexed by sequence.
    const SequenceStates = std.ArrayList(struct {
        vsr_state: VSRState,
        /// Track the expected `checksum(free_set)`.
        /// Note that this is a checksum of the decoded free set; it is not the same as
        /// `SuperBlockSector.free_set_checksum`.
        free_set: u128,
    });

    sequence_states: SequenceStates,

    superblock: *SuperBlock,
    superblock_verify: *SuperBlock,

    /// Verify that the working superblock after open() never regresses.
    latest_sequence: u64 = 0,
    latest_checksum: u128 = 0,
    latest_parent: u128 = 0,
    latest_vsr_state: VSRState = std.mem.zeroInit(VSRState, .{}),

    context_format: SuperBlock.Context = undefined,
    context_open: SuperBlock.Context = undefined,
    context_checkpoint: SuperBlock.Context = undefined,
    context_view_change: SuperBlock.Context = undefined,
    context_verify: SuperBlock.Context = undefined,

    // Set bits indicate pending operations.
    pending: std.enums.EnumSet(SuperBlock.Context.Caller) = .{},
    pending_verify: bool = false,

    /// After every write to `superblock`'s storage, verify that the superblock can be opened,
    /// and the quorum never regresses.
    fn tick(env: *Environment) !void {
        assert(env.pending.count() <= 2);
        assert(env.superblock.storage.reads.len + env.superblock.storage.writes.len <= 1);
        assert(!env.pending.contains(.format));
        assert(!env.pending.contains(.open));
        assert(!env.pending_verify);
        assert(env.pending.contains(.view_change) == env.superblock.view_change_in_progress());

        const write = env.superblock.storage.writes.peek();
        env.superblock.storage.tick();

        if (write) |w| {
            if (w.done_at_tick <= env.superblock.storage.ticks) try env.verify();
        }
    }

    /// Verify that the superblock will recover safely if the replica crashes immediately after
    /// the most recent write.
    fn verify(env: *Environment) !void {
        assert(!env.pending_verify);

        {
            // Reset `superblock_verify` so that it can be reused.
            env.superblock_verify.opened = false;
            var free_set_iterator = env.superblock_verify.free_set.blocks.iterator(.{
                .kind = .unset,
            });
            while (free_set_iterator.next()) |block_bit| {
                const block_address = block_bit + 1;
                env.superblock_verify.free_set.release(block_address);
            }
            env.superblock_verify.free_set.checkpoint();
        }

        // Duplicate the `superblock`'s storage so it is not modified by `superblock_verify`'s
        // repairs. Immediately reset() it to simulate a crash (potentially injecting additional
        // faults for pending writes) and clear the read/write queues.
        env.superblock_verify.storage.copy(env.superblock.storage);
        env.superblock_verify.storage.reset();
        env.superblock_verify.open(verify_callback, &env.context_verify);

        env.pending_verify = true;
        while (env.pending_verify) env.superblock_verify.storage.tick();

        assert(env.superblock_verify.working.checksum == env.superblock.working.checksum or
            env.superblock_verify.working.checksum == env.superblock.staging.checksum);

        // Verify the sequence we read from disk is monotonically increasing.
        if (env.latest_sequence < env.superblock_verify.working.sequence) {
            assert(env.latest_sequence + 1 == env.superblock_verify.working.sequence);

            if (env.latest_checksum != 0) {
                if (env.latest_sequence + 1 == env.superblock_verify.working.sequence) {
                    // After a checkpoint() or view_change(), the parent points to the previous
                    // working sector.
                    assert(env.superblock_verify.working.parent == env.latest_checksum);
                }
            }

            assert(env.latest_vsr_state.monotonic(env.superblock_verify.working.vsr_state));

            const expect = env.sequence_states.items[env.superblock_verify.working.sequence];
            assert(std.meta.eql(expect.vsr_state, env.superblock_verify.working.vsr_state));
            assert(expect.free_set == checksum_free_set(env.superblock_verify));

            env.latest_sequence = env.superblock_verify.working.sequence;
            env.latest_checksum = env.superblock_verify.working.checksum;
            env.latest_parent = env.superblock_verify.working.parent;
            env.latest_vsr_state = env.superblock_verify.working.vsr_state;
        } else {
            assert(env.latest_sequence == env.superblock_verify.working.sequence);
            assert(env.latest_checksum == env.superblock_verify.working.checksum);
            assert(env.latest_parent == env.superblock_verify.working.parent);
        }
    }

    fn verify_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_verify", context);
        assert(env.pending_verify);
        env.pending_verify = false;
    }

    fn format(env: *Environment) !void {
        assert(env.pending.count() == 0);
        env.pending.insert(.format);
        env.superblock.format(format_callback, &env.context_format, .{
            .cluster = cluster,
            .replica = 0,
            // Include extra space (beyond what Storage actually needs) because SuperBlock will
            // update the sector's size according to the highest FreeSet block allocated.
            .size_max = data_file_size_min + 1000 * constants.block_size,
        });

        assert(env.sequence_states.items.len == 0);
        try env.sequence_states.append(undefined); // skip sequence=0
        try env.sequence_states.append(.{
            .vsr_state = VSRState.root(cluster),
            .free_set = checksum_free_set(env.superblock),
        });
    }

    fn format_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_format", context);
        assert(env.pending.contains(.format));
        env.pending.remove(.format);
    }

    fn open(env: *Environment) void {
        assert(env.pending.count() == 0);
        env.pending.insert(.open);
        env.superblock.open(open_callback, &env.context_open);
    }

    fn open_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_open", context);
        assert(env.pending.contains(.open));
        env.pending.remove(.open);

        assert(env.superblock.working.sequence == 1);
        assert(env.superblock.working.replica == 0);
        assert(env.superblock.working.cluster == cluster);
    }

    fn view_change(env: *Environment) !void {
        assert(!env.pending.contains(.view_change));
        assert(env.pending.count() < 2);

        const vsr_state = .{
            .commit_min_checksum = env.superblock.staging.vsr_state.commit_min_checksum,
            .commit_min = env.superblock.staging.vsr_state.commit_min,
            .commit_max = env.superblock.staging.vsr_state.commit_max + 3,
            .view_normal = env.superblock.staging.vsr_state.view_normal + 4,
            .view = env.superblock.staging.vsr_state.view + 5,
        };

        assert(env.sequence_states.items.len == env.superblock.staging.sequence + 1);
        try env.sequence_states.append(.{
            .vsr_state = vsr_state,
            .free_set = env.sequence_states.items[env.sequence_states.items.len - 1].free_set,
        });

        env.pending.insert(.view_change);
        env.superblock.view_change(view_change_callback, &env.context_view_change, vsr_state);
    }

    fn view_change_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_view_change", context);
        assert(env.pending.contains(.view_change));
        env.pending.remove(.view_change);
    }

    fn checkpoint(env: *Environment) !void {
        assert(!env.pending.contains(.checkpoint));
        assert(env.pending.count() < 2);

        const vsr_state = .{
            .commit_min_checksum = env.superblock.staging.vsr_state.commit_min_checksum + 1,
            .commit_min = env.superblock.staging.vsr_state.commit_min + 1,
            .commit_max = env.superblock.staging.vsr_state.commit_max + 1,
            .view_normal = env.superblock.staging.vsr_state.view_normal + 1,
            .view = env.superblock.staging.vsr_state.view + 1,
        };

        assert(env.sequence_states.items.len == env.superblock.staging.sequence + 1);
        try env.sequence_states.append(.{
            .vsr_state = vsr_state,
            .free_set = checksum_free_set(env.superblock),
        });

        env.pending.insert(.checkpoint);
        env.superblock.checkpoint(checkpoint_callback, &env.context_checkpoint, vsr_state);
    }

    fn checkpoint_callback(context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(Environment, "context_checkpoint", context);
        assert(env.pending.contains(.checkpoint));
        env.pending.remove(.checkpoint);
    }
};

fn checksum_free_set(superblock: *const SuperBlock) u128 {
    const mask_bits = @bitSizeOf(std.DynamicBitSetUnmanaged.MaskInt);
    const count_bits = superblock.free_set.blocks.bit_length;
    const count_words = util.div_ceil(count_bits, mask_bits);
    return vsr.checksum(std.mem.sliceAsBytes(superblock.free_set.blocks.masks[0..count_words]));
}
