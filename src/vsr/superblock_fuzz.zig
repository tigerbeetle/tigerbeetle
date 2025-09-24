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
//! - updating() reports the correct state.
//!
const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Storage = @import("../testing/storage.zig").Storage;
const StorageFaultAtlas = @import("../testing/storage.zig").ClusterFaultAtlas;
const superblock_zone_size = @import("superblock.zig").superblock_zone_size;
const data_file_size_min = @import("superblock.zig").data_file_size_min;
const VSRState = @import("superblock.zig").SuperBlockHeader.VSRState;
const SuperBlockHeader = @import("superblock.zig").SuperBlockHeader;
const SuperBlockType = @import("superblock.zig").SuperBlockType;
const Caller = @import("superblock.zig").Caller;
const SuperBlock = SuperBlockType(Storage);
const fixtures = @import("../testing/fixtures.zig");
const fuzz = @import("../testing/fuzz.zig");
const stdx = @import("stdx");
const ratio = stdx.PRNG.ratio;

const cluster = fixtures.cluster;
const replica = fixtures.replica;
const replica_count = fixtures.replica_count;

pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    // Total calls to checkpoint() + view_change().
    const transitions_count_total = args.events_max orelse 10;

    try run_fuzz(gpa, args.seed, transitions_count_total);
}

fn run_fuzz(gpa: std.mem.Allocator, seed: u64, transitions_count_total: usize) !void {
    var prng = stdx.PRNG.from_seed(seed);

    var storage_fault_atlas = try StorageFaultAtlas.init(gpa, 1, &prng, .{
        .faulty_superblock = true,
        .faulty_wal_headers = false,
        .faulty_wal_prepares = false,
        .faulty_client_replies = false,
        .faulty_grid = false,
    });
    defer storage_fault_atlas.deinit(gpa);

    const storage_options: Storage.Options = .{
        .seed = prng.int(u64),
        .size = superblock_zone_size,
        // SuperBlock's IO is all serial, so latencies never reorder reads/writes.
        .read_latency_min = .{ .ns = 0 },
        .read_latency_mean = .{ .ns = 0 },
        .write_latency_min = .{ .ns = 0 },
        .write_latency_mean = .{ .ns = 0 },
        // Storage will never inject more faults than the superblock is able to recover from,
        // so a 100% fault probability is allowed.
        .read_fault_probability = ratio(
            prng.range_inclusive(u64, 25, 100),
            100,
        ),
        .write_fault_probability = ratio(
            prng.range_inclusive(u64, 25, 100),
            100,
        ),
        .crash_fault_probability = ratio(
            prng.range_inclusive(u64, 50, 100),
            100,
        ),
        .replica_index = fixtures.replica,
        .fault_atlas = &storage_fault_atlas,
    };

    var storage = try fixtures.init_storage(gpa, storage_options);
    defer storage.deinit(gpa);

    var storage_verify = try fixtures.init_storage(gpa, storage_options);
    defer storage_verify.deinit(gpa);

    var superblock = try fixtures.init_superblock(gpa, &storage, .{
        .storage_size_limit = constants.storage_size_limit_default,
    });
    defer superblock.deinit(gpa);

    var superblock_verify = try fixtures.init_superblock(gpa, &storage_verify, .{
        .storage_size_limit = constants.storage_size_limit_default,
    });
    defer superblock_verify.deinit(gpa);

    var sequence_states = Environment.SequenceStates.init(gpa);
    defer sequence_states.deinit();

    const members = vsr.root_members(cluster);
    var env = Environment{
        .members = members,
        .sequence_states = sequence_states,
        .superblock = &superblock,
        .superblock_verify = &superblock_verify,
        .latest_vsr_state = SuperBlockHeader.VSRState{
            .checkpoint = .{
                .header = std.mem.zeroes(vsr.Header.Prepare),
                .parent_checkpoint_id = 0,
                .grandparent_checkpoint_id = 0,
                .free_set_blocks_acquired_checksum = comptime vsr.checksum(&.{}),
                .free_set_blocks_released_checksum = comptime vsr.checksum(&.{}),
                .free_set_blocks_acquired_last_block_checksum = 0,
                .free_set_blocks_released_last_block_checksum = 0,
                .free_set_blocks_acquired_last_block_address = 0,
                .free_set_blocks_released_last_block_address = 0,
                .free_set_blocks_acquired_size = 0,
                .free_set_blocks_released_size = 0,
                .client_sessions_checksum = vsr.checksum(&.{}),
                .client_sessions_last_block_checksum = 0,
                .client_sessions_last_block_address = 0,
                .client_sessions_size = 0,
                .manifest_oldest_checksum = 0,
                .manifest_oldest_address = 0,
                .manifest_newest_checksum = 0,
                .manifest_newest_address = 0,
                .snapshots_block_checksum = 0,
                .snapshots_block_address = 0,
                .manifest_block_count = 0,
                .storage_size = data_file_size_min,
                .release = vsr.Release.minimum,
            },
            .commit_max = 0,
            .sync_op_min = 0,
            .sync_op_max = 0,
            .log_view = 0,
            .view = 0,
            .replica_id = members[replica],
            .members = members,
            .replica_count = replica_count,
        },
    };

    try env.format();
    while (env.pending.count() > 0) env.superblock.storage.run();

    env.open();

    try env.verify();
    assert(env.pending.count() == 0);
    assert(env.latest_sequence == 1);

    var transitions: usize = 0;
    while (transitions < transitions_count_total or env.pending.count() > 0) {
        if (transitions < transitions_count_total) {
            // TODO bias the RNG
            if (env.pending.count() == 0) {
                transitions += 1;
                if (prng.boolean()) {
                    try env.checkpoint();
                } else {
                    try env.view_change();
                }
            }

            if (env.pending.count() == 1 and prng.chance(ratio(1, 6))) {
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
    }
}

const Environment = struct {
    /// Track the expected value of parameters at a particular sequence.
    /// Indexed by sequence.
    const SequenceStates = std.ArrayList(struct {
        vsr_state: VSRState,
        view_headers: vsr.Headers.Array,
    });

    sequence_states: SequenceStates,

    members: vsr.Members,

    superblock: *SuperBlock,
    superblock_verify: *SuperBlock,

    /// Verify that the working superblock after open() never regresses.
    latest_sequence: u64 = 0,
    latest_checksum: u128 = 0,
    latest_parent: u128 = 0,
    latest_vsr_state: VSRState,

    context_format: SuperBlock.Context = undefined,
    context_open: SuperBlock.Context = undefined,
    context_checkpoint: SuperBlock.Context = undefined,
    context_view_change: SuperBlock.Context = undefined,
    context_verify: SuperBlock.Context = undefined,

    // Set bits indicate pending operations.
    pending: std.enums.EnumSet(Caller) = .{},
    pending_verify: bool = false,

    /// After every write to `superblock`'s storage, verify that the superblock can be opened,
    /// and the quorum never regresses.
    fn tick(env: *Environment) !void {
        assert(env.pending.count() <= 2);
        assert(env.superblock.storage.reads.count() + env.superblock.storage.writes.count() <= 1);
        assert(!env.pending.contains(.format));
        assert(!env.pending.contains(.open));
        assert(!env.pending_verify);
        assert(env.pending.contains(.view_change) == env.superblock.updating(.view_change));

        while (env.superblock.storage.step()) {
            try env.verify();
        }
        env.superblock.storage.tick();
    }

    /// Verify that the superblock will recover safely if the replica crashes immediately after
    /// the most recent write.
    fn verify(env: *Environment) !void {
        assert(!env.pending_verify);

        // Reset `superblock_verify` so that it can be reused.
        env.superblock_verify.opened = false;
        // Duplicate the `superblock`'s storage so it is not modified by `superblock_verify`'s
        // repairs. Immediately reset() it to simulate a crash (potentially injecting additional
        // faults for pending writes) and clear the read/write queues.
        env.superblock_verify.storage.copy(env.superblock.storage);
        env.superblock_verify.storage.reset();
        env.superblock_verify.open(verify_callback, &env.context_verify);

        env.pending_verify = true;
        while (env.pending_verify) env.superblock_verify.storage.run();

        assert(env.superblock_verify.working.checksum == env.superblock.working.checksum or
            env.superblock_verify.working.checksum == env.superblock.staging.checksum);

        // Verify the sequence we read from disk is monotonically increasing.
        if (env.latest_sequence < env.superblock_verify.working.sequence) {
            assert(env.latest_sequence + 1 == env.superblock_verify.working.sequence);

            if (env.latest_checksum != 0) {
                if (env.latest_sequence + 1 == env.superblock_verify.working.sequence) {
                    // After a checkpoint() or view_change(), the parent points to the previous
                    // working header.
                    assert(env.superblock_verify.working.parent == env.latest_checksum);
                }
            }

            assert(env.latest_vsr_state.monotonic(env.superblock_verify.working.vsr_state));

            const expect = env.sequence_states.items[env.superblock_verify.working.sequence];
            try std.testing.expectEqualDeep(
                expect.vsr_state,
                env.superblock_verify.working.vsr_state,
            );

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
        const env: *Environment = @fieldParentPtr("context_verify", context);
        assert(env.pending_verify);
        env.pending_verify = false;
    }

    fn format(env: *Environment) !void {
        assert(env.pending.count() == 0);
        env.pending.insert(.format);
        env.superblock.format(format_callback, &env.context_format, .{
            .cluster = cluster,
            .release = vsr.Release.minimum,
            .replica = replica,
            .replica_count = replica_count,
            .view = null,
        });

        var view_headers = vsr.Headers.Array{};
        view_headers.push(vsr.Header.Prepare.root(cluster));

        assert(env.sequence_states.items.len == 0);
        try env.sequence_states.append(undefined); // skip sequence=0
        try env.sequence_states.append(.{
            .vsr_state = VSRState.root(.{
                .cluster = cluster,
                .release = vsr.Release.minimum,
                .replica_id = env.members[replica],
                .members = env.members,
                .replica_count = replica_count,
                .view = 0,
            }),
            .view_headers = view_headers,
        });
    }

    fn format_callback(context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("context_format", context);
        assert(env.pending.contains(.format));
        env.pending.remove(.format);
    }

    fn open(env: *Environment) void {
        assert(env.pending.count() == 0);
        fixtures.open_superblock(env.superblock);
        assert(env.superblock.working.sequence == 1);
        assert(env.superblock.working.vsr_state.replica_id == env.members[replica]);
        assert(env.superblock.working.vsr_state.replica_count == replica_count);
        assert(env.superblock.working.cluster == cluster);
    }

    fn view_change(env: *Environment) !void {
        assert(!env.pending.contains(.view_change));
        assert(env.pending.count() < 2);

        const vsr_state = VSRState{
            .checkpoint = env.superblock.staging.vsr_state.checkpoint,
            .commit_max = env.superblock.staging.vsr_state.commit_max + 3,
            .sync_op_min = 0,
            .sync_op_max = 0,
            .log_view = env.superblock.staging.vsr_state.log_view + 4,
            .view = env.superblock.staging.vsr_state.view + 5,
            .replica_id = env.members[replica],
            .members = env.members,
            .replica_count = replica_count,
        };

        var view_headers = vsr.Headers.Array{};
        var vsr_head = std.mem.zeroInit(vsr.Header.Prepare, .{
            .client = 1,
            .request = 1,
            .command = .prepare,
            .release = vsr.Release.minimum,
            .operation = @as(vsr.Operation, @enumFromInt(constants.vsr_operations_reserved + 1)),
            .op = env.superblock.staging.vsr_state.checkpoint.header.op + 1,
            .timestamp = 1,
        });
        vsr_head.set_checksum_body(&.{});
        vsr_head.set_checksum();
        view_headers.push(vsr_head);

        assert(env.sequence_states.items.len == env.superblock.staging.sequence + 1);
        try env.sequence_states.append(.{
            .vsr_state = vsr_state,
            .view_headers = view_headers,
        });

        env.pending.insert(.view_change);
        env.superblock.view_change(view_change_callback, &env.context_view_change, .{
            .commit_max = vsr_state.commit_max,
            .log_view = vsr_state.log_view,
            .view = vsr_state.view,
            .headers = &.{
                .command = .do_view_change,
                .array = view_headers,
            },
            .sync_checkpoint = null,
        });
    }

    fn view_change_callback(context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("context_view_change", context);
        assert(env.pending.contains(.view_change));
        env.pending.remove(.view_change);
    }

    fn checkpoint(env: *Environment) !void {
        assert(!env.pending.contains(.checkpoint));
        assert(env.pending.count() < 2);

        const vsr_state_old = env.superblock.staging.vsr_state;
        const vsr_state = VSRState{
            .checkpoint = .{
                .header = header: {
                    var header = vsr.Header.Prepare.root(cluster);
                    header.op = vsr_state_old.checkpoint.header.op + 1;
                    header.set_checksum();
                    break :header header;
                },
                .parent_checkpoint_id = env.superblock.staging.checkpoint_id(),
                .grandparent_checkpoint_id = vsr_state_old.checkpoint.parent_checkpoint_id,
                .free_set_blocks_acquired_checksum = comptime vsr.checksum(&.{}),
                .free_set_blocks_released_checksum = comptime vsr.checksum(&.{}),
                .free_set_blocks_acquired_last_block_checksum = 0,
                .free_set_blocks_released_last_block_checksum = 0,
                .free_set_blocks_acquired_last_block_address = 0,
                .free_set_blocks_released_last_block_address = 0,
                .free_set_blocks_acquired_size = 0,
                .free_set_blocks_released_size = 0,
                .client_sessions_checksum = vsr.checksum(&.{}),
                .client_sessions_last_block_checksum = 0,
                .client_sessions_last_block_address = 0,
                .client_sessions_size = 0,
                .manifest_oldest_checksum = 0,
                .manifest_newest_checksum = 0,
                .manifest_oldest_address = 0,
                .manifest_newest_address = 0,
                .manifest_block_count = 0,
                .storage_size = data_file_size_min,
                .snapshots_block_checksum = 0,
                .snapshots_block_address = 0,
                .release = vsr.Release.minimum,
            },
            .commit_max = vsr_state_old.commit_max + 1,
            .sync_op_min = 0,
            .sync_op_max = 0,
            .log_view = vsr_state_old.log_view,
            .view = vsr_state_old.view,
            .replica_id = env.members[replica],
            .members = env.members,
            .replica_count = replica_count,
        };

        assert(env.sequence_states.items.len == env.superblock.staging.sequence + 1);
        try env.sequence_states.append(.{
            .vsr_state = vsr_state,
            .view_headers = vsr.Headers.Array.from_slice(
                env.superblock.staging.view_headers().slice,
            ) catch unreachable,
        });

        env.pending.insert(.checkpoint);
        env.superblock.checkpoint(checkpoint_callback, &env.context_checkpoint, .{
            .manifest_references = .{
                .oldest_checksum = 0,
                .newest_checksum = 0,
                .oldest_address = 0,
                .newest_address = 0,
                .block_count = 0,
            },
            .view_attributes = null,
            .free_set_references = .{
                .blocks_acquired = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
                .blocks_released = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
            },
            .client_sessions_reference = .{
                .last_block_checksum = 0,
                .last_block_address = 0,
                .trailer_size = 0,
                .checksum = vsr.checksum(&.{}),
            },
            .header = vsr_state.checkpoint.header,
            .commit_max = vsr_state.commit_max,
            .sync_op_min = 0,
            .sync_op_max = 0,
            .storage_size = data_file_size_min,
            .release = vsr.Release.minimum,
        });
    }

    fn checkpoint_callback(context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("context_checkpoint", context);
        assert(env.pending.contains(.checkpoint));
        env.pending.remove(.checkpoint);
    }
};
