const std = @import("std");
const testing = std.testing;
const allocator = testing.allocator;
const assert = std.debug.assert;

const config = @import("../config.zig");
const fuzz = @import("../test/fuzz.zig");
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.lsm_forest_fuzz);

const MessagePool = @import("../message_pool.zig").MessagePool;
const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../test/storage.zig").Storage;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = config.message_body_size_max,
});

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const Forest = StateMachine.Forest;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

const FuzzOp = union(enum) {
    // TODO Test secondary index lookups and range queries.
    compact: struct {
        op: u64,
        checkpoint: bool,
    },
    put_account: Account,
    get_account: u128,
};
const FuzzOpTag = std.meta.Tag(FuzzOp);

const Environment = struct {
    const cluster = 32;
    const replica = 4;
    // TODO Is this appropriate for the number of fuzz_ops we want to run?
    const size_max = vsr.Zone.superblock.size().? + vsr.Zone.wal.size().? + 1024 * 1024 * 1024;

    const node_count = 1024;
    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = 2048;
    const forest_options = StateMachine.forest_options(.{
        // Ignored by StateMachine.forest_options().
        .lsm_forest_node_count = undefined,
        .cache_entries_accounts = cache_entries_max,
        .cache_entries_transfers = cache_entries_max,
        .cache_entries_posted = cache_entries_max,
    });

    // Each account put can generate a put and a tombstone in each index.
    const puts_since_compact_max = @divTrunc(forest_options.accounts.tree_options_object.commit_entries_max, 2);

    const compacts_per_checkpoint = std.math.divCeil(
        usize,
        config.journal_slot_count,
        config.lsm_batch_multiple,
    ) catch unreachable;

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
    storage: *Storage,
    message_pool: MessagePool,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context = undefined,
    grid: Grid,
    forest: Forest,
    // We need @fieldParentPtr() of forest, so we can't use an optional Forest.
    forest_exists: bool,

    fn init(env: *Environment, storage: *Storage) !void {
        env.state = .uninit;

        env.storage = storage;
        errdefer env.storage.deinit(allocator);

        env.message_pool = try MessagePool.init(allocator, .replica);
        errdefer env.message_pool.deinit(allocator);

        env.superblock = try SuperBlock.init(allocator, env.storage, &env.message_pool);
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

        if (env.forest_exists) {
            env.forest.deinit(allocator);
            env.forest_exists = false;
        }
        env.grid.deinit(allocator);
        env.superblock.deinit(allocator);
        env.message_pool.deinit(allocator);

        env.state = .uninit;
    }

    fn tick(env: *Environment) void {
        env.grid.tick();
        env.storage.tick();
    }

    fn tick_until_state_change(env: *Environment, current_state: State, next_state: State) void {
        // Sometimes IO completes synchronously (eg if cached), so we might already be in next_state before ticking.
        assert(env.state == current_state or env.state == next_state);
        while (env.state == current_state) env.tick();
        assert(env.state == next_state);
    }

    fn change_state(env: *Environment, current_state: State, next_state: State) void {
        assert(env.state == current_state);
        env.state = next_state;
    }

    pub fn format(storage: *Storage) !void {
        var env: Environment = undefined;

        try env.init(storage);
        defer env.deinit();

        assert(env.state == .init);
        env.superblock.format(superblock_format_callback, &env.superblock_context, .{
            .cluster = cluster,
            .replica = replica,
            .size_max = size_max,
        });
        env.tick_until_state_change(.init, .formatted);
    }

    fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.init, .formatted);
    }

    pub fn open(env: *Environment) void {
        assert(env.state == .init);
        env.superblock.open(superblock_open_callback, &env.superblock_context);
        env.tick_until_state_change(.init, .forest_open);
    }

    fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.init, .superblock_open);
        env.forest = Forest.init(allocator, &env.grid, node_count, forest_options) catch unreachable;
        env.forest_exists = true;
        env.forest.open(forest_open_callback);
    }

    fn forest_open_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        env.change_state(.superblock_open, .forest_open);
    }

    pub fn compact(env: *Environment, op: u64) void {
        env.change_state(.forest_open, .forest_compacting);
        env.forest.compact(forest_compact_callback, op);
        env.tick_until_state_change(.forest_compacting, .forest_open);
    }

    fn forest_compact_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        env.change_state(.forest_compacting, .forest_open);
    }

    pub fn checkpoint(env: *Environment) void {
        env.change_state(.forest_open, .forest_checkpointing);
        env.forest.checkpoint(forest_checkpoint_callback);
        env.tick_until_state_change(.forest_checkpointing, .superblock_checkpointing);
        env.tick_until_state_change(.superblock_checkpointing, .forest_open);
    }

    fn forest_checkpoint_callback(forest: *Forest) void {
        const env = @fieldParentPtr(@This(), "forest", forest);
        env.change_state(.forest_checkpointing, .superblock_checkpointing);
        env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context);
    }

    fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.superblock_checkpointing, .forest_open);
    }

    fn prefetch_account(env: *Environment, id: u128) void {
        const groove = &env.forest.grooves.accounts;
        const Groove = @TypeOf(groove.*);
        const Getter = struct {
            finished: bool = false,
            prefetch_context: Groove.PrefetchContext = undefined,
            fn prefetch_callback(prefetch_context: *Groove.PrefetchContext) void {
                const getter = @fieldParentPtr(@This(), "prefetch_context", prefetch_context);
                getter.finished = true;
            }
        };
        var getter = Getter{};
        groove.prefetch_setup(null);
        groove.prefetch_enqueue(id);
        groove.prefetch(Getter.prefetch_callback, &getter.prefetch_context);
        while (!getter.finished) env.tick();
    }

    fn run(storage: *Storage, fuzz_ops: []const FuzzOp) !void {
        var env: Environment = undefined;

        try env.init(storage);
        defer env.deinit();

        // Open the superblock then forest.
        env.open();

        // The forest should behave like a simple key-value data-structure.
        // We'll compare it to a hash map.
        var model = std.hash_map.AutoHashMap(u128, Account).init(allocator);
        defer model.deinit();

        for (fuzz_ops) |fuzz_op, fuzz_op_index| {
            log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzz_op_index, fuzz_ops.len, fuzz_op });
            //TODO(@djg) Restore these when dj-vopr-workload merges.
            //const storage_size_used = storage.size_used();
            //log.debug("storage.size_used = {}/{}", .{ storage_size_used, storage.size });
            //const model_size = model.count() * @sizeOf(Account);
            //log.debug("space_amplification = {d:.2}", .{@intToFloat(f64, storage_size_used) / @intToFloat(f64, model_size)});
            // Apply fuzz_op to the forest and the model.
            switch (fuzz_op) {
                .compact => |compact| {
                    env.compact(compact.op);
                    if (compact.checkpoint)
                        env.checkpoint();
                },
                .put_account => |account| {
                    env.forest.grooves.accounts.put(&account);
                    try model.put(account.id, account);
                },
                .get_account => |id| {
                    // Get account from lsm.
                    env.prefetch_account(id);
                    const lsm_account = env.forest.grooves.accounts.get(id);

                    // Compare result to model.
                    const model_account = model.get(id);
                    if (model_account == null) {
                        assert(lsm_account == null);
                    } else {
                        assert(std.mem.eql(
                            u8,
                            std.mem.asBytes(&model_account.?),
                            std.mem.asBytes(lsm_account.?),
                        ));
                    }
                },
            }
        }
    }
};

pub fn run_fuzz_ops(fuzz_ops: []const FuzzOp) !void {
    // Init mocked storage.
    var storage = try Storage.init(
        allocator,
        Environment.size_max,
        Storage.Options{
            // We don't apply storage faults yet, so this seed doesn't matter.
            .seed = 0xdeadbeef,
            .read_latency_min = 0,
            .read_latency_mean = 0,
            .write_latency_min = 0,
            .write_latency_mean = 0,
            .read_fault_probability = 0,
            .write_fault_probability = 0,
        },
        0,
        .{
            .first_offset = 0,
            .period = 0,
        },
    );
    defer storage.deinit(allocator);

    try Environment.format(&storage);
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

pub fn generate_fuzz_ops(random: std.rand.Random) ![]const FuzzOp {
    const fuzz_op_count = @minimum(
        @as(usize, 1E7),
        fuzz.random_int_exponential(random, usize, 1E6),
    );
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (random.boolean()) 0 else 1,
        // Always do puts, and always more puts than removes.
        .put_account = config.lsm_batch_multiple * 2,
        // Maybe do some gets.
        .get_account = if (random.boolean()) 0 else config.lsm_batch_multiple,
    };
    log.info("fuzz_op_distribution = {d:.2}", .{fuzz_op_distribution});

    log.info("puts_since_compact_max = {}", .{Environment.puts_since_compact_max});
    log.info("compacts_per_checkpoint = {}", .{Environment.compacts_per_checkpoint});

    var id_to_timestamp = std.hash_map.AutoHashMap(u128, u64).init(allocator);
    defer id_to_timestamp.deinit();

    var op: u64 = 1;
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
                    compact_op % config.lsm_batch_multiple == config.lsm_batch_multiple - 1 and
                    // Checkpoint at roughly the same rate as log wraparound.
                    random.uintLessThan(usize, Environment.compacts_per_checkpoint) == 0;
                break :compact FuzzOp{
                    .compact = .{
                        .op = compact_op,
                        .checkpoint = checkpoint,
                    },
                };
            },
            .put_account => put_account: {
                const id = random_id(random, u128);
                // `timestamp` just needs to be unique, but we're not allowed to change the timestamp of an existing account.
                const timestamp = id_to_timestamp.get(id) orelse fuzz_op_index;
                try id_to_timestamp.put(id, timestamp);
                break :put_account FuzzOp{ .put_account = Account{
                    .id = id,
                    .timestamp = timestamp,
                    .user_data = random_id(random, u128),
                    .reserved = [_]u8{0} ** 48,
                    .ledger = random_id(random, u32),
                    .code = random_id(random, u16),
                    .flags = .{
                        .debits_must_not_exceed_credits = random.boolean(),
                        .credits_must_not_exceed_debits = random.boolean(),
                    },
                    .debits_pending = random.int(u64),
                    .debits_posted = random.int(u64),
                    .credits_pending = random.int(u64),
                    .credits_posted = random.int(u64),
                } };
            },
            .get_account => FuzzOp{
                .get_account = random_id(random, u128),
            },
        };
        switch (fuzz_op.*) {
            .compact => puts_since_compact = 0,
            .put_account => puts_since_compact += 1,
            .get_account => {},
        }
    }

    return fuzz_ops;
}

pub fn main() !void {
    const fuzz_args = try fuzz.parse_fuzz_args(allocator);
    var rng = std.rand.DefaultPrng.init(fuzz_args.seed);

    const fuzz_ops = try generate_fuzz_ops(rng.random());
    defer allocator.free(fuzz_ops);

    try run_fuzz_ops(fuzz_ops);

    log.info("Passed!", .{});
}
