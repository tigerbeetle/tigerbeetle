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

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const ForestType = @import("forest.zig").ForestType;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const Forest = ForestType(Storage, .{
    .accounts = GrooveType(
        Storage,
        Account,
        .{
            .ignored = &[_][]const u8{ "reserved", "flags" },
            .derived = .{},
        },
    ),
    .transfers = GrooveType(
        Storage,
        Transfer,
        .{
            .ignored = &[_][]const u8{ "reserved", "flags" },
            .derived = .{},
        },
    ),
});

fn GrooveRecordType(comptime Object: type, comptime commit_count_max: u32) type {
    return struct {
        const GrooveRecord = @This();
        const RecordList = std.ArrayList(Object);

        checkpointed: RecordList,
        recorded: RecordList,

        pub fn init() !GrooveRecord {
            return GrooveRecord{
                .checkpointed = RecordList.init(allocator),
                .recorded = RecordList.init(allocator),
            };
        }

        pub fn deinit(record: *GrooveRecord) void {
            record.checkpointed.deinit();
            record.recorded.deinit();
        }

        pub fn put(record: *GrooveRecord, object: *const Object) !void {
            try record.recorded.append(object.*);
        }

        pub fn checkpoint(record: *GrooveRecord) !void {
            try record.checkpointed.appendSlice(record.recorded.items);
            record.recorded.clearRetainingCapacity();
        }

        pub fn assert_checkpointed(record: *const GrooveRecord, groove: anytype, io: *IO) !void {
            const Groove = @TypeOf(groove.*);
            const CheckpointAssertion = struct {
                prefetch_context: Groove.PrefetchContext = undefined,
                checkpointed: []const Object,
                verify_count: u32 = 0,
                completed: bool = false,
                groove: *Groove,

                const Assertion = @This();

                fn verify(assertion: *Assertion) void {
                    assert(!assertion.completed);
                    assert(assertion.verify_count == 0);

                    // Select how many objects to prefetch from the groove for verification.
                    assertion.verify_count = std.math.min(
                        assertion.checkpointed.len, 
                        commit_count_max,
                    );

                    if (assertion.verify_count == 0) {
                        assertion.completed = true;
                        return;
                    }

                    const checkpointed = assertion.checkpointed[0..assertion.verify_count];
                    for (checkpointed) |*object| {
                        assertion.groove.prefetch_enqueue(object.id);
                    }

                    assertion.groove.prefetch(
                        groove_prefetch_callback, 
                        &assertion.prefetch_context,
                    );
                }

                fn groove_prefetch_callback(prefetch_context: *Groove.PrefetchContext) void {
                    const assertion = @fieldParentPtr(
                        Assertion,
                        "prefetch_context",
                        prefetch_context,
                    );
                    assert(!assertion.completed);
                    assert(assertion.verify_count > 0);
                    assert(assertion.verify_count <= commit_count_max);

                    const checkpointed = assertion.checkpointed[0..assertion.verify_count];
                    for (checkpointed) |*object| {
                        const groove_object = assertion.groove.get(object.id).?;
                        assert(std.mem.eql(
                            u8, 
                            std.mem.asBytes(object), 
                            std.mem.asBytes(groove_object),
                        ));
                    }

                    assertion.groove.prefetch_clear();
                    assertion.checkpointed = assertion.checkpointed[checkpointed.len..];
                    assertion.verify_count = 0;
                    assertion.verify();
                }
            };

            var checkpoint_assertion = CheckpointAssertion{
                .groove = groove,
                .checkpointed = record.checkpointed.items,
            };
            checkpoint_assertion.verify();

            while (!checkpoint_assertion.completed) {
                try io.tick();
            }
        }
    };
}

const Environment = struct {
    const cluster = 32;
    const replica = 4;
    const size_max = (512 + 64) * 1024 * 1024;

    const node_count = 1024;
    const cache_size = 2 * 1024 * 1024;
    const forest_config = .{
        .transfers = .{
            .cache_size = cache_size,
            .commit_count_max = 8191 * 2,
        },
        .accounts = .{
            .cache_size = cache_size,
            .commit_count_max = 8191,
        },
    };

    const GrooveRecordAccounts = GrooveRecordType(Account, forest_config.accounts.commit_count_max);
    const GrooveRecordTransfers = GrooveRecordType(Transfer, forest_config.transfers.commit_count_max);

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
    record_accounts: GrooveRecordAccounts,
    record_transfers: GrooveRecordTransfers,

    fn init(env: *Environment, must_create: bool) !void {
        try env.start(must_create);
        errdefer env.shutdown();

        env.record_accounts = try GrooveRecordAccounts.init();
        errdefer env.record_accounts.deinit();

        env.record_transfers = try GrooveRecordTransfers.init();
        errdefer env.record_transfers.deinit();
    }

    fn deinit(env: *Environment) void {
        env.record_transfers.deinit();
        env.record_accounts.deinit();
        env.shutdown();
    }

    /// Setup the environment without affecting the state that needs to persist across shutdown()s.
    fn start(env: *Environment, must_create: bool) !void {
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

        env.superblock = try SuperBlock.init(allocator, &env.storage, &env.message_pool);
        env.superblock_context = undefined;
        errdefer env.superblock.deinit(allocator);

        env.grid = try Grid.init(allocator, &env.superblock);
        errdefer env.grid.deinit(allocator);

        env.forest = try Forest.init(allocator, &env.grid, node_count, forest_config);
        errdefer env.forest.deinit(allocator);

        env.state = .init;
    }

    fn shutdown(env: *Environment) void {
        assert(env.state != .uninit);
        defer env.state = .uninit;
        
        env.forest.deinit(allocator);
        env.grid.deinit(allocator);
        env.superblock.deinit(allocator);
        // message_pool doesn't need to be deinit()
        env.storage.deinit();
        env.io.deinit();
        std.os.close(env.fd);
        std.os.close(env.dir_fd);
    }

    fn format() !void {
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
                .init => try env.io.tick(),
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

    fn open(env: *Environment) !void {
        assert(env.state == .init);
        env.superblock.open(superblock_open_callback, &env.superblock_context);

        while (true) {
            switch (env.state) {
                .init, .superblock_open => try env.io.tick(),
                .forest_open => break,
                else => unreachable,
            }
        }
    }

    fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        assert(env.state == .init);
        env.state = .superblock_open;
        env.forest.open(forest_open_callback);
    } 

    fn forest_open_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        assert(env.state == .superblock_open);
        env.state = .forest_open;
    }

    fn checkpoint(env: *Environment, op: u64) !void {
        assert(env.state == .forest_open);
        env.state = .forest_checkpointing;
        env.forest.checkpoint(forest_checkpoint_callback, op);

        while (true) {
            switch (env.state) {
                .forest_checkpointing, .superblock_checkpointing => try env.io.tick(),
                .forest_open => break,
                else => unreachable,
            }
        }
    }

    fn forest_checkpoint_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        assert(env.state == .forest_checkpointing);
        env.state = .superblock_checkpointing;
        env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context);
    }

    fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        assert(env.state == .superblock_checkpointing);
        env.state = .forest_open;
    }

    fn compact(env: *Environment, op: u64) !void {
        assert(env.state == .forest_open);
        env.state = .forest_compacting;
        env.forest.compact(forest_compact_callback, op);

        while (true) {
            switch (env.state) {
                .forest_compacting => try env.io.tick(),
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
        var next_id: u128 = 0;
        var prng = std.rand.DefaultPrng.init(0xdeadbeef);

        const num_accounts = 10_000;
        {
            var next_op: u64 = 0;
            var next_crash: u32 = 0;
            var next_checkpoint: u32 = 0;
            var next_compact: u32 = forest_config.accounts.commit_count_max;
            
            var i: usize = 0;
            while (i < num_accounts) : (i += 1) {
                defer next_id += 1;
                const account = Account{
                    .id = next_id,
                    .user_data = 0,
                    .reserved = [_]u8{0} ** 48,
                    .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
                    .code = 1000, // A chart of accounts code to describe this as a clearing account.
                    .flags = .{ .debits_must_not_exceed_credits = true },
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = prng.random().uintLessThanBiased(u64, 1000),
                };

                // Insert created account.
                log.debug("inserting account {d}/{d}", .{ i, num_accounts });
                try env.record_accounts.put(&account);
                env.forest.grooves.accounts.put(&account);

                // Compact the forest
                if (i + 1 == next_compact) {
                    log.debug("compacting forest {d}/{d}", .{ i, num_accounts });
                    next_compact += forest_config.accounts.commit_count_max;

                    const op = next_op;
                    next_op += 1;

                    try env.compact(op);
                }

                // Checkpoint everything.
                if (i == next_checkpoint) {
                    log.debug("checkpointing everything {d}/{d}", .{ i, num_accounts });
                    next_checkpoint += 1024;

                    const op = next_op;
                    try env.checkpoint(op);
                    try env.record_accounts.checkpoint();
                    try env.record_transfers.checkpoint();
                }

                // Simulate crashing and restoring.
                if (i == next_crash) {
                    log.debug("simulating crash {d}/{d}", .{ i, num_accounts });
                    next_crash += 1024;

                    crashing = true;
                    {
                        env.shutdown();
                        try env.start(must_create);
                        errdefer env.deinit();

                        try env.open();
                        try env.record_accounts.assert_checkpointed(&env.forest.grooves.accounts, &env.io);
                        try env.record_transfers.assert_checkpointed(&env.forest.grooves.transfers, &env.io);
                    }
                    crashing = false;
                }
            }
        }
    }
};

pub fn main() !void {
    try Environment.format();
    try Environment.run();
}

