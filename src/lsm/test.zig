const std = @import("std");
const testing = std.testing;
const allocator = testing.allocator;
const assert = std.debug.assert;
const os = std.os;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.lsm_forest_test);

const MessagePool = @import("../message_pool.zig").MessagePool;
const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../storage.zig").Storage;
const IO = @import("../io.zig").IO;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage);

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const Forest = StateMachine.Forest;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

const Environment = struct {
    const cluster = 32;
    const replica = 4;
    const size_max = vsr.Zone.superblock.size().? +
        vsr.Zone.wal_headers.size().? +
        vsr.Zone.wal_prepares.size().? +
        (512 + 64) * 1024 * 1024;

    const node_count = 1024;
    const cache_entries_max = 2 * 1024 * 1024;
    const forest_options = StateMachine.forest_options(.{
        // Ignored by StateMachine.forest_options().
        .lsm_forest_node_count = undefined,
        .cache_entries_accounts = cache_entries_max,
        .cache_entries_transfers = cache_entries_max,
        .cache_entries_posted = cache_entries_max,
        .message_body_size_max = config.message_size_max - @sizeOf(vsr.Header),
    });

    const State = enum {
        uninit,
        init,
        formatted,
        superblock_open,
        forest_open,
        forest_compacting,
        forest_checkpointing,
        superblock_checkpointing,
    };

    state: State,
    dir_fd: os.fd_t,
    fd: os.fd_t,
    io: IO,
    storage: Storage,
    message_pool: MessagePool,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context,
    grid: Grid,
    forest: Forest,
    // We need @fieldParentPtr() of forest, so we can't use an optional Forest.
    forest_exists: bool = false,

    fn init(env: *Environment, must_create: bool) !void {
        env.state = .uninit;

        const dir_path = ".";
        env.dir_fd = try IO.open_dir(dir_path);
        errdefer std.os.close(env.dir_fd);

        env.fd = try IO.open_file(env.dir_fd, "test_forest", size_max, must_create);
        errdefer std.os.close(env.fd);

        env.io = try IO.init(128, 0);
        errdefer env.io.deinit();

        env.storage = try Storage.init(&env.io, env.fd);
        errdefer env.storage.deinit();

        env.message_pool = try MessagePool.init(allocator, .replica);
        errdefer env.message_pool.deinit(allocator);

        env.superblock = try SuperBlock.init(allocator, &env.storage, &env.message_pool);
        env.superblock_context = undefined;
        errdefer env.superblock.deinit(allocator);

        env.grid = try Grid.init(allocator, &env.superblock);
        errdefer env.grid.deinit(allocator);

        // Forest must be initialized with an open superblock.
        env.forest = undefined;
        env.forest_exists = false;

        env.state = .init;
    }

    fn deinit(env: *Environment) void {
        assert(env.state != .uninit);
        defer env.state = .uninit;

        if (env.forest_exists) {
            env.forest.deinit(allocator);
            env.forest_exists = false;
        }
        env.grid.deinit(allocator);
        env.superblock.deinit(allocator);
        env.message_pool.deinit(allocator);
        env.storage.deinit();
        env.io.deinit();
        std.os.close(env.fd);
        std.os.close(env.dir_fd);
    }

    fn tick(env: *Environment) !void {
        env.grid.tick();
        try env.io.tick();
    }

    pub fn format() !void {
        var env: Environment = undefined;

        const must_create = true;
        try env.init(must_create);
        defer env.deinit();

        assert(env.state == .init);
        env.superblock.format(superblock_format_callback, &env.superblock_context, .{
            .cluster = cluster,
            .replica = replica,
            .size_max = size_max,
        });

        while (true) {
            switch (env.state) {
                .init => try env.tick(),
                .formatted => break,
                else => unreachable,
            }
        }
    }

    fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        assert(env.state == .init);
        env.state = .formatted;
    }

    pub fn open(env: *Environment) !void {
        assert(env.state == .init);
        env.superblock.open(superblock_open_callback, &env.superblock_context);

        while (true) {
            switch (env.state) {
                .init, .superblock_open => try env.tick(),
                .forest_open => break,
                else => unreachable,
            }
        }
    }

    fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        assert(env.state == .init);
        env.state = .superblock_open;

        env.forest = Forest.init(allocator, &env.grid, node_count, forest_options) catch unreachable;
        env.forest_exists = true;
        env.forest.open(forest_open_callback);
    }

    fn forest_open_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        assert(env.state == .superblock_open);
        env.state = .forest_open;
    }

    pub fn checkpoint(env: *Environment) !void {
        assert(env.state == .forest_open);
        env.state = .forest_checkpointing;
        env.forest.checkpoint(forest_checkpoint_callback);

        while (true) {
            switch (env.state) {
                .forest_checkpointing, .superblock_checkpointing => try env.tick(),
                .forest_open => break,
                else => unreachable,
            }
        }
    }

    fn forest_checkpoint_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        assert(env.state == .forest_checkpointing);

        log.debug("forest checkpointing completed!", .{});

        var vsr_state = env.superblock.staging.vsr_state;
        vsr_state.commit_min += 1;
        vsr_state.commit_min_checkpoint += 1;

        env.state = .superblock_checkpointing;
        env.superblock.checkpoint(
            superblock_checkpoint_callback,
            &env.superblock_context,
            vsr_state,
        );
    }

    fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        assert(env.state == .superblock_checkpointing);
        env.state = .forest_open;

        log.debug("superblock checkpointing completed!", .{});
    }

    pub fn compact(env: *Environment, op: u64) !void {
        assert(env.state == .forest_open);
        env.state = .forest_compacting;
        env.forest.compact(forest_compact_callback, op);

        while (true) {
            switch (env.state) {
                .forest_compacting => try env.tick(),
                .forest_open => break,
                else => unreachable,
            }
        }
    }

    fn forest_compact_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        assert(env.state == .forest_compacting);
        env.state = .forest_open;
    }

    const Visibility = enum {
        visible,
        invisible,
    };

    pub fn assert_visibility(
        env: *Environment,
        comptime visibility: Visibility,
        groove: anytype,
        objects: anytype,
        comptime commit_entries_max: u32,
    ) !void {
        const Groove = @TypeOf(groove.*);
        const Object = @TypeOf(objects[0]);

        const VisibilityAssertion = struct {
            prefetch_context: Groove.PrefetchContext = undefined,
            verify_count: usize = 0,
            objects: []const Object,
            groove: *Groove,

            fn verify(assertion: *@This()) void {
                assert(assertion.verify_count == 0);
                assertion.verify_count = std.math.min(commit_entries_max, assertion.objects.len);
                if (assertion.verify_count == 0) return;

                assertion.groove.prefetch_setup(null);
                for (assertion.objects[0..assertion.verify_count]) |*object| {
                    assertion.groove.prefetch_enqueue(object.id);
                }

                assertion.groove.prefetch(prefetch_callback, &assertion.prefetch_context);
            }

            fn prefetch_callback(prefetch_context: *Groove.PrefetchContext) void {
                const assertion = @fieldParentPtr(@This(), "prefetch_context", prefetch_context);
                assert(assertion.verify_count > 0);

                {
                    for (assertion.objects[0..assertion.verify_count]) |*object| {
                        log.debug("verifying {} for id={}", .{ visibility, object.id });
                        const result = assertion.groove.get(object.id);

                        switch (visibility) {
                            .invisible => assert(result == null),
                            .visible => {
                                assert(result != null);
                                assert(std.mem.eql(u8, std.mem.asBytes(result.?), std.mem.asBytes(object)));
                            },
                        }
                    }
                }

                assertion.objects = assertion.objects[assertion.verify_count..];
                assertion.verify_count = 0;
                assertion.verify();
            }
        };

        var assertion = VisibilityAssertion{ .objects = objects, .groove = groove };
        assertion.verify();
        while (assertion.verify_count > 0) try env.tick();
    }

    fn run() !void {
        var env: Environment = undefined;

        const must_create = false;
        try env.init(must_create);

        // We will be manually deinitializing during the test to simulate crashes and recovery.
        // If an error occurs during re-initialization, we don't want to trip this call to deinit().
        var crashing = false;
        defer if (!crashing) env.deinit();

        // Open the superblock then forest to start inserting accounts and transfers.
        try env.open();

        // Recording types for verification
        var inserted = std.ArrayList(Account).init(allocator);
        defer inserted.deinit();

        const accounts_to_insert_per_op = 1; // forest_options.accounts.commit_entries_max;
        const iterations = 4;

        var op: u64 = 1;
        var id: u128 = 1;
        var timestamp: u64 = 42;
        var crash_probability = std.rand.DefaultPrng.init(1337);

        var iter: usize = 0;
        while (iter < (accounts_to_insert_per_op * config.lsm_batch_multiple * iterations)) : (iter += 1) {
            // Insert a bunch of accounts

            var i: u32 = 0;
            while (i < accounts_to_insert_per_op) : (i += 1) {
                defer id += 1;
                defer timestamp += 1;
                const account = Account{
                    .id = id,
                    .timestamp = timestamp,
                    .user_data = 0,
                    .reserved = [_]u8{0} ** 48,
                    .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
                    .code = 1000, // A chart of accounts code to describe this as a clearing account.
                    .flags = .{ .debits_must_not_exceed_credits = true },
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = 42,
                };

                // Insert an account ...
                const groove = &env.forest.grooves.accounts;
                groove.put(&account);

                // ..and make sure it can be retrieved
                try env.assert_visibility(
                    .visible,
                    &env.forest.grooves.accounts,
                    @as([]const Account, &.{account}),
                    forest_options.accounts.tree_options_object.commit_entries_max,
                );

                // Record the successfull insertion.
                try inserted.append(account);
            }

            // compact the forest
            try env.compact(op);
            defer op += 1;

            // Checkpoint when the forest finishes compaction.
            // Don't repeat a checkpoint (commit_min must always advance).
            const checkpoint_op = op -| config.lsm_batch_multiple;
            if (checkpoint_op % config.lsm_batch_multiple == config.lsm_batch_multiple - 1 and
                checkpoint_op != env.superblock.staging.vsr_state.commit_min)
            {
                // Checkpoint the forest then superblock
                env.superblock.staging.vsr_state.commit_min = checkpoint_op;
                env.superblock.staging.vsr_state.commit_max = checkpoint_op;
                try env.checkpoint();

                const checkpointed = inserted.items[0 .. checkpoint_op * accounts_to_insert_per_op];
                const uncommitted = inserted.items[checkpointed.len..];
                log.debug("checkpointed={d} uncommitted={d}", .{ checkpointed.len, uncommitted.len });
                assert(uncommitted.len == config.lsm_batch_multiple * accounts_to_insert_per_op);

                // Randomly initiate a crash
                if (crash_probability.random().uintLessThanBiased(u32, 100) >= 50) {
                    // Simulate crashing and restoring.
                    log.debug("simulating crash", .{});
                    crashing = true;
                    {
                        env.deinit();
                        try env.init(must_create);

                        // Re-open the superblock and forest.
                        try env.open();

                        // Double check the forest DOES NOT contain the un-checkpointed values (negative space)
                        try env.assert_visibility(
                            .invisible,
                            &env.forest.grooves.accounts,
                            uncommitted,
                            forest_options.accounts.tree_options_object.commit_entries_max,
                        );

                        // Reset everything to after checkpoint
                        op = checkpoint_op;
                        inserted.items.len = checkpointed.len;
                        id = checkpointed[checkpointed.len - 1].id + 1;
                        timestamp = checkpointed[checkpointed.len - 1].timestamp + 1;
                    }
                    crashing = false;
                }

                // Double check the forest contains the checkpointed values (positive space)
                try env.assert_visibility(
                    .visible,
                    &env.forest.grooves.accounts,
                    checkpointed,
                    forest_options.accounts.tree_options_object.commit_entries_max,
                );
            }
        }
    }
};

pub fn main() !void {
    try Environment.format(); // NOTE: this can be commented out after first run to speed up testing.
    try Environment.run(); //try do_simple();
}
