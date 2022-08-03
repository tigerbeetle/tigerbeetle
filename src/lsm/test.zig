const std = @import("std");
const testing = std.testing;
const allocator = testing.allocator;
const assert = std.debug.assert;
const os = std.os;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

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

        pub fn insert(record: *GrooveRecord, object: *const Object) !void {
            try record.recorded.append(object.*);
        }

        pub fn checkpoint(record: *GrooveRecord) !void {
            try record.checkpointed.appendSlice(record.recorded.items);
            record.recorded.clearRetainingCapacity();
        }

        pub fn assert_checkpointed(record: *const GrooveRecord, io: *IO, groove: anytype) !void {
            // Checkpoint asserts should happen before we start updating the record. 
            assert(record.recorded.items.len == 0);

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
                        const groove_object = assertion.groove.get(object.id) catch unreachable;
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

        env.record_accounts = try GrooveRecordAccounts.init();
        errdefer env.record_accounts.deinit();

        env.record_transfers = try GrooveRecordTransfers.init();
        errdefer env.record_transfers.deinit();

        env.state = .init;
    }

    fn deinit(env: *Environment) void {
        assert(env.state != .uninit);

        env.record_transfers.deinit();
        env.record_accounts.deinit();
        env.forest.deinit(allocator);
        env.grid.deinit(allocator);
        env.superblock.deinit(allocator);
        // message_pool doesn't need to be deinit()
        env.storage.deinit();
        env.io.deinit();
        std.os.close(env.fd);
        std.os.close(env.dir_fd);

        env.* = undefined;
        env.state = .uninit;
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

    fn simulate_crash(env: *Environment) !void {
        env.deinit();

        const must_create = false;
        try env.init(must_create);
        errdefer env.deinit();

        try env.open();
    }

    fn run() !void {
        var env: Environment = undefined;

        const must_create = false;
        try env.init(must_create);

        // We will be manually deinitializing during the test to simulate crashes and recovery.
        // If an error occurs during re-initialization, we don't want to trip this call to deinit().
        var crashing = false;
        defer if (!crashing) env.deinit();

        try env.open();
    }
};

pub fn main() !void {
    try Environment.format();
    try Environment.run();
}

