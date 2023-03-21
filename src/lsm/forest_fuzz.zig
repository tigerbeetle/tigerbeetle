const std = @import("std");
const testing = std.testing;
const allocator = testing.allocator;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const vsr = @import("../vsr.zig");

const log = std.log.scoped(.lsm_forest_fuzz);
const tracer = @import("../tracer.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;
const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../testing/storage.zig").Storage;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = constants.message_body_size_max,
    .lsm_batch_multiple = constants.lsm_batch_multiple,
});

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const Forest = StateMachine.Forest;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

pub const tigerbeetle_config = @import("../config.zig").configs.test_min;

const FuzzOp = union(enum) {
    // TODO Test secondary index lookups and range queries.
    compact: struct {
        op: u64,
        checkpoint: bool,
    },
    put_account: struct {
        op: u64,
        account: Account,
    },
    get_account: u128,
    storage_reset: void,
};
const FuzzOpTag = std.meta.Tag(FuzzOp);

const Environment = struct {
    const cluster = 32;
    const replica = 4;
    const replica_count = 6;

    const node_count = 1024;
    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = 2048;
    const forest_options = StateMachine.forest_options(.{
        .lsm_forest_node_count = node_count,
        .cache_entries_accounts = cache_entries_max,
        .cache_entries_transfers = cache_entries_max,
        .cache_entries_posted = cache_entries_max,
    });

    // We must call compact after every 'batch'.
    // Every `lsm_batch_multiple` batches may put/remove `value_count_max` values per index.
    // Every `FuzzOp.put_account` issues one remove and one put per index.
    const puts_since_compact_max = @divTrunc(
        Forest.groove_config.accounts_mutable.ObjectTree.Table.value_count_max,
        2 * constants.lsm_batch_multiple,
    );

    const compacts_per_checkpoint = std.math.divCeil(
        usize,
        constants.journal_slot_count,
        constants.lsm_batch_multiple,
    ) catch unreachable;

    const State = enum {
        init,
        superblock_format,
        superblock_open,
        forest_init,
        forest_open,
        fuzzing,
        forest_compact,
        forest_checkpoint,
        superblock_checkpoint,
    };

    state: State,
    storage: *Storage,
    message_pool: MessagePool,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context,
    grid: Grid,
    forest: Forest,
    checkpoint_op: ?u64,

    fn init(env: *Environment, storage: *Storage) !void {
        env.storage = storage;
        env.message_pool = try MessagePool.init(allocator, .replica);

        env.superblock = try SuperBlock.init(allocator, .{
            .storage = env.storage,
            .storage_size_limit = constants.storage_size_max,
            .message_pool = &env.message_pool,
        });

        env.grid = try Grid.init(allocator, &env.superblock);

        env.forest = undefined;
        env.checkpoint_op = null;
    }

    fn deinit(env: *Environment) void {
        env.message_pool.deinit(allocator);
        env.superblock.deinit(allocator);
        env.grid.deinit(allocator);
    }

    pub fn run(storage: *Storage, fuzz_ops: []const FuzzOp) !void {
        var env: Environment = undefined;
        env.state = .init;
        try env.init(storage);
        defer env.deinit();

        env.change_state(.init, .superblock_format);
        env.superblock.format(superblock_format_callback, &env.superblock_context, .{
            .cluster = cluster,
            .replica = replica,
            .replica_count = replica_count,
        });
        env.tick_until_state_change(.superblock_format, .superblock_open);

        try env.open();
        defer env.close();

        try env.apply(fuzz_ops);
    }

    fn change_state(env: *Environment, current_state: State, next_state: State) void {
        assert(env.state == current_state);
        env.state = next_state;
    }

    fn tick_until_state_change(env: *Environment, current_state: State, next_state: State) void {
        // Sometimes operations complete synchronously so we might already be in next_state before ticking.
        //assert(env.state == current_state or env.state == next_state);
        while (env.state == current_state) env.storage.tick();
        assert(env.state == next_state);
    }

    fn open(env: *Environment) !void {
        env.superblock.open(superblock_open_callback, &env.superblock_context);
        env.tick_until_state_change(.superblock_open, .forest_init);

        env.forest = try Forest.init(allocator, &env.grid, node_count, forest_options);
        env.change_state(.forest_init, .forest_open);
        env.forest.open(forest_open_callback);

        env.tick_until_state_change(.forest_open, .fuzzing);
    }

    fn close(env: *Environment) void {
        env.forest.deinit(allocator);
    }

    fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.superblock_format, .superblock_open);
    }

    fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.superblock_open, .forest_init);
    }

    fn forest_open_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        env.change_state(.forest_open, .fuzzing);
    }

    pub fn compact(env: *Environment, op: u64) void {
        env.change_state(.fuzzing, .forest_compact);
        env.forest.compact(forest_compact_callback, op);
        env.tick_until_state_change(.forest_compact, .fuzzing);
    }

    fn forest_compact_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        env.change_state(.forest_compact, .fuzzing);
    }

    pub fn checkpoint(env: *Environment, op: u64) void {
        assert(env.checkpoint_op == null);
        env.checkpoint_op = op - constants.lsm_batch_multiple;

        env.change_state(.fuzzing, .forest_checkpoint);
        env.forest.checkpoint(forest_checkpoint_callback);
        env.tick_until_state_change(.forest_checkpoint, .superblock_checkpoint);
        env.tick_until_state_change(.superblock_checkpoint, .fuzzing);
    }

    fn forest_checkpoint_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        const op = env.checkpoint_op.?;
        env.checkpoint_op = null;

        env.change_state(.forest_checkpoint, .superblock_checkpoint);
        env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
            .commit_min_checksum = env.superblock.working.vsr_state.commit_min_checksum + 1,
            .commit_min = op,
            .commit_max = op + 1,
        });
    }

    fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.superblock_checkpoint, .fuzzing);
    }

    fn prefetch_account(env: *Environment, id: u128) void {
        const groove_immutable = &env.forest.grooves.accounts_immutable;
        const groove_mutable = &env.forest.grooves.accounts_mutable;

        const GrooveImmutable = @TypeOf(groove_immutable.*);
        const GrooveMutable = @TypeOf(groove_mutable.*);
        const Getter = struct {
            _id: u128,
            _groove_mutable: *GrooveMutable,
            _groove_immutable: *GrooveImmutable,

            finished: bool = false,
            prefetch_context_mutable: GrooveMutable.PrefetchContext = undefined,
            prefetch_context_immutable: GrooveImmutable.PrefetchContext = undefined,

            fn prefetch_start(getter: *@This()) void {
                const groove = getter._groove_immutable;
                groove.prefetch_setup(null);
                groove.prefetch_enqueue(getter._id);
                groove.prefetch(@This().prefetch_callback_immuttable, &getter.prefetch_context_immutable);
            }

            fn prefetch_callback_immuttable(prefetch_context: *GrooveImmutable.PrefetchContext) void {
                const getter = @fieldParentPtr(@This(), "prefetch_context_immutable", prefetch_context);
                const groove = getter._groove_mutable;
                groove.prefetch_setup(null);

                if (getter._groove_immutable.get(getter._id)) |immut| {
                    groove.prefetch_enqueue(immut.timestamp);
                }

                groove.prefetch(@This().prefetch_callback_mutable, &getter.prefetch_context_mutable);
            }

            fn prefetch_callback_mutable(prefetch_context: *GrooveMutable.PrefetchContext) void {
                const getter = @fieldParentPtr(@This(), "prefetch_context_mutable", prefetch_context);
                assert(!getter.finished);
                getter.finished = true;
            }
        };

        var getter = Getter{
            ._id = id,
            ._groove_mutable = groove_mutable,
            ._groove_immutable = groove_immutable,
        };
        getter.prefetch_start();
        while (!getter.finished) env.storage.tick();
    }

    fn put_account(env: *Environment, a: *const Account) void {
        env.forest.grooves.accounts_immutable.put(&StateMachine.AccountImmutable.from_account(a));
        env.forest.grooves.accounts_mutable.put(&StateMachine.AccountMutable.from_account(a));
    }

    fn get_account(env: *Environment, id: u128) ?Account {
        const immut = env.forest.grooves.accounts_immutable.get(id) orelse return null;
        const mut = env.forest.grooves.accounts_mutable.get(immut.timestamp).?;
        return StateMachine.into_account(immut, mut);
    }

    fn apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
        // The forest should behave like a simple key-value data-structure.
        const Model = struct {
            checkpointed: KVType, // represents persistent state
            log: LogType, // represents in-memory state

            const KVType = std.hash_map.AutoHashMap(u128, Account);
            const LogEntry = struct { op: u64, account: Account };
            const LogType = std.fifo.LinearFifo(LogEntry, .Dynamic);
            const Model = @This();

            pub fn init() Model {
                return .{
                    .checkpointed = KVType.init(allocator),
                    .log = LogType.init(allocator),
                };
            }

            pub fn deinit(model: *Model) void {
                model.checkpointed.deinit();
                model.log.deinit();
            }

            pub fn put_account(model: *Model, account: *const Account, op: u64) !void {
                try model.log.writeItem(.{ .op = op, .account = account.* });
            }

            pub fn get_account(model: *const Model, id: u128) ?Account {
                var latest_op: ?u64 = null;
                const log_size = model.log.readableLength();
                var log_left = log_size;
                while (log_left > 0) : (log_left -= 1) {
                    const entry = model.log.peekItem(log_left - 1); // most recent first
                    if (latest_op == null) {
                        latest_op = entry.op;
                    }

                    assert(latest_op.? >= entry.op);

                    if (entry.account.id == id) {
                        return entry.account;
                    }
                }
                return model.checkpointed.get(id);
            }

            pub fn checkpoint(model: *Model, op: u64) !void {
                const checkpointable = op - (op % constants.lsm_batch_multiple) -| 1;
                const log_size = model.log.readableLength();
                var log_index: usize = 0;
                while (log_index < log_size) : (log_index += 1) {
                    const entry = model.log.peekItem(log_index);
                    if (entry.op > checkpointable) {
                        break;
                    }

                    try model.checkpointed.put(entry.account.id, entry.account);
                }
                model.log.discard(log_index);
            }

            pub fn storage_reset(model: *Model) void {
                model.log.discard(model.log.readableLength());
            }
        };
        var model = Model.init();
        defer model.deinit();

        for (fuzz_ops) |fuzz_op, fuzz_op_index| {
            assert(env.state == .fuzzing);
            log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzz_op_index, fuzz_ops.len, fuzz_op });

            const storage_size_used = env.storage.size_used();
            log.debug("storage.size_used = {}/{}", .{ storage_size_used, env.storage.size });

            const model_size = (model.log.readableLength() + model.checkpointed.count()) * @sizeOf(Account);
            // NOTE: This isn't accurate anymore because the model can contain multiple copies of an account in the log
            log.debug("space_amplification ~= {d:.2}", .{
                @intToFloat(f64, storage_size_used) / @intToFloat(f64, model_size),
            });

            // Apply fuzz_op to the forest and the model.
            try env.apply_op(fuzz_op, &model);
        }
    }

    fn apply_op(env: *Environment, fuzz_op: FuzzOp, model: anytype) !void {
        switch (fuzz_op) {
            .compact => |compact| {
                env.compact(compact.op);
                if (compact.checkpoint) {
                    try model.checkpoint(compact.op);
                    env.checkpoint(compact.op);
                }
            },
            .put_account => |put| {
                // The forest requires prefetch before put.
                env.prefetch_account(put.account.id);
                env.put_account(&put.account);
                try model.put_account(&put.account, put.op);
            },
            .get_account => |id| {
                // Get account from lsm.
                env.prefetch_account(id);
                const lsm_account = env.get_account(id);

                // Compare result to model.
                const model_account = model.get_account(id);
                if (model_account == null) {
                    assert(lsm_account == null);
                } else {
                    assert(std.mem.eql(
                        u8,
                        std.mem.asBytes(&model_account.?),
                        std.mem.asBytes(&lsm_account.?),
                    ));
                }
            },
            .storage_reset => {
                env.storage.log_pending_io();
                env.close();
                env.deinit();
                env.storage.reset();

                env.change_state(.fuzzing, .init);
                try env.init(env.storage);

                env.change_state(.init, .superblock_open);
                try env.open();

                // TODO: currently this checks that everything added to the LSM after checkpoint
                // resets to the last checkpoint on crash by looking through what's been added
                // afterwards. This won't work if we add account removal to the fuzzer though.
                const log_size = model.log.readableLength();
                var log_index: usize = 0;
                while (log_index < log_size) : (log_index += 1) {
                    const entry = model.log.peekItem(log_index);
                    const id = entry.account.id;
                    if (model.checkpointed.get(id)) |checkpointed_account| {
                        env.prefetch_account(id);
                        if (env.get_account(id)) |lsm_account| {
                            assert(std.mem.eql(
                                u8,
                                std.mem.asBytes(&lsm_account),
                                std.mem.asBytes(&checkpointed_account),
                            ));
                        } else {
                            std.debug.panic("Account checkpointed but not in lsm after crash.\n {}\n", .{checkpointed_account});
                        }
                    }
                }
                model.storage_reset();
            },
        }
    }
};

pub fn run_fuzz_ops(storage_options: Storage.Options, fuzz_ops: []const FuzzOp) !void {
    // Init mocked storage.
    var storage = try Storage.init(allocator, constants.storage_size_max, storage_options);
    defer storage.deinit(allocator);

    try Environment.run(&storage, fuzz_ops);
}

fn random_id(random: std.rand.Random, comptime Int: type) Int {
    // We have two opposing desires for random ids:
    const avg_int: Int = if (random.boolean())
        // 1. We want to cause many collisions.
        8
    else
        // 2. We want to generate enough ids that the cache can't hold them all.
        Environment.cache_entries_max;
    return fuzz.random_int_exponential(random, Int, avg_int);
}

pub fn generate_fuzz_ops(random: std.rand.Random, fuzz_op_count: usize) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (random.boolean()) 0 else 1,
        // Always do puts.
        .put_account = constants.lsm_batch_multiple * 2,
        // Maybe do some gets.
        .get_account = if (random.boolean()) 0 else constants.lsm_batch_multiple,
        // Maybe crash and recover from the last checkpoint a few times per fuzzer run.
        .storage_reset = if (random.boolean()) 0 else 1E-4,
    };
    log.info("fuzz_op_distribution = {d:.2}", .{fuzz_op_distribution});

    log.info("puts_since_compact_max = {}", .{Environment.puts_since_compact_max});
    log.info("compacts_per_checkpoint = {}", .{Environment.compacts_per_checkpoint});

    var id_to_account = std.hash_map.AutoHashMap(u128, Account).init(allocator);
    defer id_to_account.deinit();

    var op: u64 = 1;
    var persisted_op: u64 = op;
    var puts_since_compact: usize = 0;
    for (fuzz_ops) |*fuzz_op, fuzz_op_index| {
        const fuzz_op_tag: FuzzOpTag = if (puts_since_compact >= Environment.puts_since_compact_max)
            // We have to compact before doing any other operations.
            .compact
        else
            // Otherwise pick a random FuzzOp.
            fuzz.random_enum(random, FuzzOpTag, fuzz_op_distribution);
        fuzz_op.* = switch (fuzz_op_tag) {
            .compact => compact: {
                const compact_op = op;
                op += 1;
                const checkpoint =
                    // Can only checkpoint on the last beat of the bar.
                    compact_op % constants.lsm_batch_multiple == constants.lsm_batch_multiple - 1 and
                    compact_op > constants.lsm_batch_multiple and
                    // Never checkpoint at the same op twice
                    compact_op > persisted_op + constants.lsm_batch_multiple and
                    // Checkpoint at roughly the same rate as log wraparound.
                    random.uintLessThan(usize, Environment.compacts_per_checkpoint) == 0;
                if (checkpoint) {
                    persisted_op = op - constants.lsm_batch_multiple;
                }
                break :compact FuzzOp{
                    .compact = .{
                        .op = compact_op,
                        .checkpoint = checkpoint,
                    },
                };
            },
            .put_account => put_account: {
                const id = random_id(random, u128);
                var account = id_to_account.get(id) orelse Account{
                    .id = id,
                    // `timestamp` must be unique.
                    .timestamp = fuzz_op_index,
                    .user_data = random_id(random, u128),
                    .reserved = [_]u8{0} ** 48,
                    .ledger = random_id(random, u32),
                    .code = random_id(random, u16),
                    .flags = .{
                        .debits_must_not_exceed_credits = random.boolean(),
                        .credits_must_not_exceed_debits = random.boolean(),
                    },
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = 0,
                };

                // These are the only fields we are allowed to change on existing accounts.
                account.debits_pending = random.int(u64);
                account.debits_posted = random.int(u64);
                account.credits_pending = random.int(u64);
                account.credits_posted = random.int(u64);

                try id_to_account.put(account.id, account);
                break :put_account FuzzOp{ .put_account = .{
                    .op = op,
                    .account = account,
                } };
            },
            .get_account => FuzzOp{ .get_account = random_id(random, u128) },
            .storage_reset => storage_reset: {
                op = persisted_op;
                break :storage_reset FuzzOp{ .storage_reset = {} };
            },
        };
        switch (fuzz_op.*) {
            .compact => puts_since_compact = 0,
            .put_account => puts_since_compact += 1,
            .get_account => {},
            .storage_reset => {},
        }
    }

    return fuzz_ops;
}

pub fn main() !void {
    try tracer.init(allocator);
    defer tracer.deinit(allocator);

    const fuzz_args = try fuzz.parse_fuzz_args(allocator);
    var rng = std.rand.DefaultPrng.init(fuzz_args.seed);
    const random = rng.random();

    const fuzz_op_count = @minimum(
        fuzz_args.events_max orelse @as(usize, 1E7),
        fuzz.random_int_exponential(random, usize, 1E6),
    );

    const fuzz_ops = try generate_fuzz_ops(random, fuzz_op_count);
    defer allocator.free(fuzz_ops);

    try run_fuzz_ops(Storage.Options{
        .seed = random.int(u64),
        .read_latency_min = 0,
        .read_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        .write_latency_min = 0,
        .write_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        // We can't actually recover from a crash in this fuzzer since we would need
        // to transfer state from a different replica to continue
        .crash_fault_probability = 0,
    }, fuzz_ops);

    log.info("Passed!", .{});
}
