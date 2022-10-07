const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const GridType = @import("grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);

pub fn ForestType(comptime Storage: type, comptime groove_config: anytype) type {
    var groove_fields: []const std.builtin.TypeInfo.StructField = &.{};
    var groove_options_fields: []const std.builtin.TypeInfo.StructField = &.{};

    for (std.meta.fields(@TypeOf(groove_config))) |field| {
        const Groove = @field(groove_config, field.name);
        groove_fields = groove_fields ++ [_]std.builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = Groove,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };

        groove_options_fields = groove_options_fields ++ [_]std.builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = Groove.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };
    }

    const Grooves = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = groove_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    const _GroovesOptions = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = groove_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    return struct {
        const Forest = @This();

        const Grid = GridType(Storage);

        const Callback = fn (*Forest) void;
        const JoinOp = enum {
            compacting,
            checkpoint,
            open,
        };

        pub const GroovesOptions = _GroovesOptions;

        join_op: ?JoinOp = null,
        join_pending: usize = 0,
        join_callback: ?Callback = null,

        grid: *Grid,
        grooves: Grooves,
        node_pool: *NodePool,

        pub fn init(
            allocator: mem.Allocator,
            grid: *Grid,
            node_count: u32,
            // (e.g.) .{ .transfers = .{ .cache_entries_max = 128, … }, .accounts = … }
            grooves_options: GroovesOptions,
        ) !Forest {
            // NodePool must be allocated to pass in a stable address for the Grooves.
            const node_pool = try allocator.create(NodePool);
            errdefer allocator.destroy(node_pool);

            // TODO: look into using lsm_table_size_max for the node_count.
            node_pool.* = try NodePool.init(allocator, node_count);
            errdefer node_pool.deinit(allocator);

            var grooves: Grooves = undefined;
            var grooves_initialized: usize = 0;

            errdefer inline for (std.meta.fields(Grooves)) |field, field_index| {
                if (grooves_initialized >= field_index + 1) {
                    @field(grooves, field.name).deinit(allocator);
                }
            };

            inline for (std.meta.fields(Grooves)) |groove_field| {
                const groove = &@field(grooves, groove_field.name);
                const Groove = @TypeOf(groove.*);
                const groove_options: Groove.Options = @field(grooves_options, groove_field.name);

                groove.* = try Groove.init(
                    allocator,
                    node_pool,
                    grid,
                    groove_options,
                );

                grooves_initialized += 1;
            }

            return Forest{
                .grid = grid,
                .grooves = grooves,
                .node_pool = node_pool,
            };
        }

        pub fn deinit(forest: *Forest, allocator: mem.Allocator) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).deinit(allocator);
            }

            forest.node_pool.deinit(allocator);
            allocator.destroy(forest.node_pool);
        }

        fn JoinType(comptime join_op: JoinOp) type {
            return struct {
                pub fn start(forest: *Forest, callback: Callback) void {
                    assert(forest.join_op == null);
                    assert(forest.join_pending == 0);
                    assert(forest.join_callback == null);

                    forest.join_op = join_op;
                    forest.join_pending = std.meta.fields(Grooves).len;
                    forest.join_callback = callback;
                }

                fn GrooveFor(comptime groove_field_name: []const u8) type {
                    return @TypeOf(@field(@as(Grooves, undefined), groove_field_name));
                }

                pub fn groove_callback(
                    comptime groove_field_name: []const u8,
                ) fn (*GrooveFor(groove_field_name)) void {
                    return struct {
                        fn groove_cb(groove: *GrooveFor(groove_field_name)) void {
                            const grooves = @fieldParentPtr(Grooves, groove_field_name, groove);
                            const forest = @fieldParentPtr(Forest, "grooves", grooves);

                            assert(forest.join_op == join_op);
                            assert(forest.join_callback != null);
                            assert(forest.join_pending <= std.meta.fields(Grooves).len);

                            forest.join_pending -= 1;
                            if (forest.join_pending > 0) return;

                            const callback = forest.join_callback.?;
                            forest.join_op = null;
                            forest.join_callback = null;
                            callback(forest);
                        }
                    }.groove_cb;
                }
            };
        }

        pub fn open(forest: *Forest, callback: Callback) void {
            const Join = JoinType(.open);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open(Join.groove_callback(field.name));
            }
        }

        pub fn compact(forest: *Forest, callback: Callback, op: u64) void {
            // Start a compacting join.
            const Join = JoinType(.compacting);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact(Join.groove_callback(field.name), op);
            }
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            const Join = JoinType(.checkpoint);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).checkpoint(Join.groove_callback(field.name));
            }
        }
    };
}

pub fn main() !void {
    try ForestTestType(struct {
        pub const Storage = @import("../storage.zig").Storage;

        const IO = @import("../io.zig").IO;
        const SuperBlock = vsr.SuperBlockType(Storage);
        const MessagePool = @import("../message_pool.zig").MessagePool;

        const StorageContext = struct {
            dir_fd: std.os.fd_t,
            fd: std.os.fd_t,
            io: IO,
            storage: Storage,
            initialized: bool,

            fn init(
                context: *StorageContext,
                comptime size_max: u32,
                comptime must_create: bool,
            ) !void {
                assert(!context.initialized);

                const dir_path = ".";
                context.dir_fd = try IO.open_dir(dir_path);
                errdefer std.os.close(context.dir_fd);

                context.fd = try IO.open_file(context.dir_fd, "test_forest", size_max, must_create);
                errdefer std.os.close(context.fd);

                context.io = try IO.init(128, 0);
                errdefer context.io.deinit();

                context.storage = try Storage.init(&context.io, context.fd);
                errdefer context.storage.deinit();

                context.initialized = true;
            }

            fn deinit(context: *StorageContext) void {
                if (!context.initialized) return;
                defer context.initialized = false;

                context.storage.deinit();
                context.io.deinit();
                std.os.close(context.fd);
                std.os.close(context.dir_fd);
            }
        };

        pub fn with_storage(
            allocator: mem.Allocator,
            comptime size_max: u32,
            comptime callback: fn (*Storage) anyerror!void,
        ) !void {
            var context: StorageContext = undefined;
            context.initialized = false;

            // Format the storage
            {
                const must_create = true;
                try context.init(size_max, must_create);
                defer context.deinit();

                var message_pool = try MessagePool.init(allocator, .replica);
                defer message_pool.deinit(allocator);

                var superblock = try SuperBlock.init(allocator, &context.storage, &message_pool);
                defer superblock.deinit(allocator);

                const cluster = 0;
                const replica = 0;
                try vsr.format(
                    Storage,
                    allocator,
                    cluster,
                    replica,
                    &context.storage,
                    &superblock,
                );
            }

            // Open and run the storage
            {
                const must_create = false;
                try context.init(size_max, must_create);
                defer context.deinit();

                try callback(&context.storagE);
            }
        }

        pub fn crash_and_restore(
            storage: *Storage,
            allocator: mem.Allocator,
            comptime size_max: u32,
        ) !void {
            _ = allocator;

            const context = @fieldParentPtr(StorageContext, "storage", storage);
            context.deinit();

            const must_create = false;
            try context.init(size_max, must_create);
        }
    }).run();
}

test "Forest" {
    // TODO panics
    // try test_forest_in_memory();

    _ = ForestTestType(struct {
        pub const Storage = @import("../storage.zig").Storage;
    });
}

fn test_forest_in_memory() !void {
    try ForestTestType(struct {
        pub const Storage = @import("../test/storage.zig").Storage;

        const SuperBlock = vsr.SuperBlockType(Storage);
        const MessagePool = @import("../message_pool.zig").MessagePool;

        pub fn with_storage(
            allocator: mem.Allocator,
            comptime size_max: u32,
            comptime callback: fn (*Storage) anyerror!void,
        ) !void {
            var storage = try Storage.init(
                allocator,
                size_max,
                Storage.Options{
                    .read_latency_min = 0,
                    .read_latency_mean = 0,
                    .write_latency_min = 0,
                    .write_latency_mean = 0,
                },
            );
            defer storage.deinit(allocator);

            // Format the superblock
            {
                var message_pool = try MessagePool.init(allocator, .replica);
                defer message_pool.deinit(allocator);

                var superblock = try SuperBlock.init(allocator, &storage, &message_pool);
                defer superblock.deinit(allocator);

                const cluster = 0;
                const replica = 0;
                try vsr.format(
                    Storage,
                    allocator,
                    cluster,
                    replica,
                    &storage,
                    &superblock,
                );
            }

            // Run the callback on the formatted storage
            try callback(&storage);
        }

        pub fn crash_and_restore(
            storage: *Storage,
            allocator: mem.Allocator,
            comptime size_max: u32,
        ) !void {
            // contents are in memory so we don't want to deinit() then init() again.
            _ = storage;
            _ = allocator;
            _ = size_max;
        }
    }).run();
}

fn ForestTestType(comptime StorageProvider: type) type {
    const tb = @import("../tigerbeetle.zig");
    const Account = tb.Account;
    const Transfer = tb.Transfer;

    const GrooveType = @import("groove.zig").GrooveType;
    const MessagePool = @import("../message_pool.zig").MessagePool;

    const Storage = StorageProvider.Storage;
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

    const allocator = std.testing.allocator;
    const size_max = (512 + 64) * 1024 * 1024;

    const node_count = 1024;
    const cache_entries_max = 2 * 1024 * 1024;
    const forest_config = .{
        .transfers = .{
            .cache_entries_max = cache_entries_max,
            .commit_entries_max = 8191 * 2,
        },
        .accounts = .{
            .cache_entries_max = cache_entries_max,
            .commit_entries_max = 8191,
        },
    };

    const max_run_ops = 10; // TODO bump this up

    return struct {
        const Self = @This();

        op: u64 = 0,
        timestamp: u64 = 0,
        checkpoint_op: ?u64 = null,
        accounts: Recorder(Account) = .{},
        transfers: Recorder(Transfer) = .{},
        prng: std.rand.DefaultPrng = std.rand.DefaultPrng.init(0xdeadbeef),

        fn Recorder(comptime T: type) type {
            return struct {
                inserted: std.ArrayList(T) = std.ArrayList(T).init(allocator),
                checkpointed: u32 = 0,
                next_checkpoint: u32 = 0,
            };
        }

        pub fn run() !void {
            try StorageProvider.with_storage(allocator, size_max, run_with_storage);
        }

        fn run_with_storage(storage: *Storage) !void {
            var self = Self{};
            defer self.accounts.inserted.deinit();
            defer self.transfers.inserted.deinit();

            while (true) {
                try self.run_until_crash(storage);
                if (self.op >= max_run_ops) break;
                try StorageProvider.crash_and_restore(storage, allocator, size_max);
            }
        }

        fn run_until_crash(self: *Self, storage: *Storage) !void {
            var message_pool = try MessagePool.init(allocator, .replica);
            defer message_pool.deinit(allocator);

            var superblock = try SuperBlock.init(allocator, storage, &message_pool);
            defer superblock.deinit(allocator);

            // Open the superblock
            {
                const S = struct {
                    var opened = false;
                    fn superblock_open_callback(_: *SuperBlock.Context) void {
                        opened = true;
                    }
                };

                S.opened = false;
                var context: SuperBlock.Context = undefined;
                superblock.open(S.superblock_open_callback, &context);
                while (!S.opened) storage.tick();
            }

            var grid = try Grid.init(allocator, &superblock);
            defer grid.deinit(allocator);

            var forest = try Forest.init(allocator, &grid, node_count, forest_config);
            defer forest.deinit(allocator);

            // Open the forest
            {
                const S = struct {
                    var opened = false;
                    fn forest_open_callback(_: *Forest) void {
                        opened = true;
                    }
                };

                S.opened = false;
                forest.open(S.forest_open_callback);
                while (!S.opened) {
                    grid.tick();
                    storage.tick();
                }
            }

            // TODO Use self.accounts/transfers to check positive and negative space here
            // TODO create transfers as well
            // TODO crash in random intervals

            while (self.op < max_run_ops) : (self.op += 1) {
                // StateMachine.create_accounts
                {
                    const ledger = self.prng.random().int(u32);
                    const code = self.prng.random().int(u16);

                    const id = self.prng.random().int(u128);
                    const timestamp = self.timestamp;
                    self.timestamp += 1;

                    const account = Account{
                        .id = id,
                        .timestamp = timestamp,
                        .user_data = self.prng.random().int(u128),
                        .reserved = [_]u8{0} ** 48,
                        .ledger = ledger,
                        .code = code,
                        .flags = .{ .debits_must_not_exceed_credits = true },
                        .debits_pending = 0,
                        .debits_posted = 0,
                        .credits_pending = 0,
                        .credits_posted = 42,
                    };

                    forest.grooves.accounts.put(&account);
                    try self.accounts.inserted.append(account);
                }

                // Compact the forest
                {
                    const S = struct {
                        var compacted = false;
                        fn forest_compact_callback(_: *Forest) void {
                            compacted = true;
                        }
                    };

                    S.compacted = false;
                    forest.compact(S.forest_compact_callback, self.op);
                    while (!S.compacted) {
                        grid.tick();
                        storage.tick();
                    }
                }

                // At the end of a compaction measure, record how many will be checkpointed next.
                if (self.op % config.lsm_batch_multiple == config.lsm_batch_multiple - 1) {
                    self.accounts.next_checkpoint = @intCast(u32, self.accounts.inserted.items.len);
                    self.transfers.next_checkpoint = @intCast(u32, self.transfers.inserted.items.len);
                }

                // Try to checkpoint the previous batch's compaction
                if (std.math.sub(u64, self.op, config.lsm_batch_multiple) catch null) |checkpoint_op| {
                    if (checkpoint_op % config.lsm_batch_multiple == config.lsm_batch_multiple - 1) {
                        // Checkpoint the forest
                        {
                            const S = struct {
                                var checkpointed = false;
                                fn forest_checkpoint_callback(_: *Forest) void {
                                    checkpointed = true;
                                }
                            };

                            S.checkpointed = false;
                            forest.checkpoint(S.forest_checkpoint_callback, self.op);
                            while (!S.checkpointed) {
                                grid.tick();
                                storage.tick();
                            }
                        }

                        // Checkpoint the superblock
                        {
                            const S = struct {
                                var checkpointed = false;
                                fn superblock_checkpoint_callback(_: *SuperBlock.Context) void {
                                    checkpointed = true;
                                }
                            };

                            S.checkpointed = false;
                            var context: SuperBlock.Context = undefined;
                            superblock.checkpoint(S.superblock_checkpoint_callback, &context);
                            while (!S.checkpointed) {
                                grid.tick();
                                storage.tick();
                            }
                        }

                        // Mark all state as checkpointed for crash recovery
                        self.checkpoint_op = checkpoint_op;
                        self.accounts.checkpointed = self.accounts.next_checkpoint;
                        self.transfers.checkpointed = self.transfers.next_checkpoint;
                    }
                }
            }
        }
    };
}
