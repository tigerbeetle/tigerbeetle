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
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage);

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const Forest = StateMachine.Forest;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

const FuzzOp = union(enum) {
    // TODO Test checkpointing, secondary index lookups and range queries.
    compact,
    put: Account,
    get_account: u128,
};

const Environment = struct {
    const cluster = 32;
    const replica = 4;
    // TODO Is this appropriate for the number of fuzz_ops we want to run?
    const size_max = vsr.Zone.superblock.size().? +
        vsr.Zone.wal_headers.size().? +
        vsr.Zone.wal_prepares.size().? +
        1024 * 1024 * 1024;

    const node_count = 1024;
    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = 2048;
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
    };

    state: State,
    storage: *Storage,
    message_pool: MessagePool,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context,
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

        // We need an op number for compact.
        var op: u64 = 1;

        for (fuzz_ops) |fuzz_op, fuzz_op_index| {
            log.debug("Running fuzz_ops[{}] == {}", .{ fuzz_op_index, fuzz_op });
            // Apply fuzz_op to the forest and the model.
            switch (fuzz_op) {
                .compact => {
                    env.compact(op);
                    op += 1;
                },
                .put => |account| {
                    env.forest.grooves.accounts.put(&account);
                    model.put(account.id, account) catch unreachable;
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

pub fn run_fuzz_ops(storage_options: Storage.Options, fuzz_ops: []const FuzzOp) !void {
    // Init mocked storage.
    var storage = try Storage.init(allocator, Environment.size_max, storage_options);
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
    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    const fuzz_op_distribution = fuzz.random_enum_distribution(random, std.meta.Tag(FuzzOp));
    log.info("fuzz_op_distribution = {d:.2}", .{fuzz_op_distribution});

    // We're not allowed to go more than Environment.cache_entries_max puts without compacting.
    var puts_since_compact: usize = 0;

    var id_to_timestamp = std.hash_map.AutoHashMap(u128, u64).init(allocator);
    defer id_to_timestamp.deinit();

    for (fuzz_ops) |*fuzz_op, fuzz_op_index| {
        fuzz_op.* = if (puts_since_compact >= Environment.cache_entries_max)
            // We have to compact before doing any other operations.
            FuzzOp{ .compact = {} }
        else
        // Otherwise pick a random FuzzOp.
        switch (fuzz.random_enum(random, std.meta.Tag(FuzzOp), fuzz_op_distribution)) {
            .compact => FuzzOp{
                .compact = {},
            },
            .put => put: {
                const id = random_id(random, u128);
                // `timestamp` just needs to be unique, but we're not allowed to change the timestamp of an existing account.
                const timestamp = id_to_timestamp.get(id) orelse fuzz_op_index;
                try id_to_timestamp.put(id, timestamp);
                break :put FuzzOp{ .put = Account{
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
            .put => puts_since_compact += 1,
            .get_account => {},
        }
    }

    return fuzz_ops;
}

pub fn main() !void {
    var seed: ?u64 = null;

    var args = std.process.args();

    // Discard executable name.
    allocator.free(try args.next(allocator).?);

    while (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;
        defer allocator.free(arg);

        if (std.mem.eql(u8, arg, "--seed")) {
            const seed_string_or_err = args.next(allocator) orelse
                std.debug.panic("Expected an argument to --seed", .{});
            const seed_string = try seed_string_or_err;
            defer allocator.free(seed_string);

            if (seed != null) {
                std.debug.panic("Received more than one \"--seed\"", .{});
            }
            seed = std.fmt.parseInt(u64, seed_string, 10) catch |err|
                std.debug.panic("Could not parse \"{}\" as an integer seed: {}", .{ std.zig.fmtEscapes(seed_string), err });
        } else {
            // When run with `--test-cmd`, `zig run` also passes the location of the zig binary as an extra arg.
            // I don't know how to turn this off, so we just skip such args.
            if (!std.mem.endsWith(u8, arg, "zig")) {
                std.debug.panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
            }
        }
    }

    if (seed == null) {
        // If no seed was given, use a random seed instead.
        var buffer: [@sizeOf(u64)]u8 = undefined;
        try std.os.getrandom(&buffer);
        seed = @bitCast(u64, buffer);
    }

    log.info("Fuzz seed = {}", .{seed.?});

    var rng = std.rand.DefaultPrng.init(seed.?);
    const random = rng.random();

    const fuzz_ops = try generate_fuzz_ops(random);
    defer allocator.free(fuzz_ops);

    const read_latency_min = fuzz.random_int_exponential(random, u64, 5);
    const write_latency_min = fuzz.random_int_exponential(random, u64, 5);

    try run_fuzz_ops(
        Storage.Options{
            .read_latency_min = read_latency_min,
            .read_latency_mean = read_latency_min + fuzz.random_int_exponential(random, u64, 20),
            .write_latency_min = write_latency_min,
            .write_latency_mean = write_latency_min + fuzz.random_int_exponential(random, u64, 20),
        },
        fuzz_ops
    );
}
