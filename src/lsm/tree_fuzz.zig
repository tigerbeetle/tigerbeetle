const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const vsr = @import("../vsr.zig");
const schema = @import("schema.zig");
const allocator = fuzz.allocator;

const log = std.log.scoped(.lsm_tree_fuzz);
const tracer = @import("../tracer.zig");

const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../testing/storage.zig").Storage;
const ClusterFaultAtlas = @import("../testing/storage.zig").ClusterFaultAtlas;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const GridType = @import("../vsr/grid.zig").GridType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const TableUsage = @import("table.zig").TableUsage;
const TableType = @import("table.zig").TableType;
const key_fingerprint = @import("tree.zig").key_fingerprint;
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

pub const tigerbeetle_config = @import("../config.zig").configs.fuzz_min;

// TODO Test grid corruption

const Key = extern struct {
    id: u64,

    const Value = packed struct(u128) {
        id: u64,
        value: u63,
        tombstone: u1 = 0,

        comptime {
            assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);
        }
    };

    inline fn compare_keys(a: Key, b: Key) std.math.Order {
        return std.math.order(a.id, b.id);
    }

    inline fn key_from_value(value: *const Key.Value) Key {
        return Key{ .id = value.id };
    }

    const sentinel_key = Key{
        .id = std.math.maxInt(u64),
    };

    inline fn tombstone(value: *const Key.Value) bool {
        return value.tombstone != 0;
    }

    inline fn tombstone_from_key(key: Key) Key.Value {
        return Key.Value{
            .id = key.id,
            .value = 0,
            .tombstone = 1,
        };
    }
};

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    // TODO Test range queries.
    compact: struct {
        op: u64,
        checkpoint: bool,
    },
    put: Key.Value,
    remove: Key.Value,
    get: Key,
};

const batch_size_max = constants.message_size_max - @sizeOf(vsr.Header);
const commit_entries_max = @divFloor(batch_size_max, @sizeOf(Key.Value));
const value_count_max = constants.lsm_batch_multiple * commit_entries_max;
const snapshot_latest = @import("tree.zig").snapshot_latest;

const cluster = 32;
const replica = 4;
const replica_count = 6;
const node_count = 1024;
const tree_options = .{
    // This is the smallest size that set_associative_cache will allow us.
    .cache_entries_max = 2048,
};

// We must call compact after every 'batch'.
// Every `lsm_batch_multiple` batches may put/remove `value_count_max` values.
// Every `FuzzOp.put` issues one remove and one put.
const puts_since_compact_max = @divTrunc(commit_entries_max, 2);
const compacts_per_checkpoint = std.math.divCeil(
    usize,
    constants.journal_slot_count,
    constants.lsm_batch_multiple,
) catch unreachable;

fn EnvironmentType(comptime table_usage: TableUsage) type {
    return struct {
        const Environment = @This();

        const Tree = @import("tree.zig").TreeType(Table, Storage);
        const Table = TableType(
            Key,
            Key.Value,
            Key.compare_keys,
            Key.key_from_value,
            Key.sentinel_key,
            Key.tombstone,
            Key.tombstone_from_key,
            value_count_max,
            table_usage,
        );

        const State = enum {
            init,
            superblock_format,
            superblock_open,
            tree_init,
            manifest_log_open,
            fuzzing,
            tree_compact,
            manifest_log_compact,
            manifest_log_checkpoint,
            superblock_checkpoint,
            tree_lookup,
        };

        const GridRepairQueue = std.ArrayList(struct {
            address: u64,
            checksum: u128,
        });

        state: State,
        storage: *Storage,
        superblock: SuperBlock,
        superblock_context: SuperBlock.Context,
        grid: Grid,
        grid_repair_write: Grid.Write,
        grid_repair_block: Grid.BlockPtr,
        grid_repair_queue: GridRepairQueue,
        manifest_log: ManifestLog,
        node_pool: NodePool,
        tree: Tree,
        lookup_context: Tree.LookupContext,
        lookup_value: ?*const Key.Value,

        pub fn run(storage: *Storage, fuzz_ops: []const FuzzOp) !void {
            var env: Environment = undefined;
            env.state = .init;
            env.storage = storage;

            env.superblock = try SuperBlock.init(allocator, .{
                .storage = env.storage,
                .storage_size_limit = constants.storage_size_max,
            });
            defer env.superblock.deinit(allocator);

            env.grid = try Grid.init(allocator, .{
                .superblock = &env.superblock,
                .on_read_fault = on_grid_read_fault,
            });
            defer env.grid.deinit(allocator);

            env.grid_repair_block = try allocate_block(allocator);
            defer allocator.free(env.grid_repair_block);

            env.grid_repair_queue = GridRepairQueue.init(allocator);
            defer env.grid_repair_queue.deinit();

            env.manifest_log =
                try ManifestLog.init(allocator, &env.grid, .{ .forest_tree_count = 1 });
            defer env.manifest_log.deinit(allocator);

            env.node_pool = try NodePool.init(allocator, node_count);
            defer env.node_pool.deinit(allocator);

            env.tree = undefined;
            env.lookup_value = null;
            //env.checkpoint_op = null;

            try env.open_then_apply(fuzz_ops);
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

        pub fn open_then_apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            env.change_state(.init, .superblock_format);
            env.superblock.format(superblock_format_callback, &env.superblock_context, .{
                .cluster = cluster,
                .replica = replica,
                .replica_count = replica_count,
            });

            env.tick_until_state_change(.superblock_format, .superblock_open);
            env.superblock.open(superblock_open_callback, &env.superblock_context);

            env.tick_until_state_change(.superblock_open, .tree_init);
            env.tree = try Tree.init(allocator, &env.node_pool, &env.grid, .{
                .id = 1,
                .name = "Key.Value",
            }, tree_options);
            defer env.tree.deinit(allocator);

            env.change_state(.tree_init, .manifest_log_open);

            env.tree.open_commence(&env.manifest_log);
            env.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);

            env.tick_until_state_change(.manifest_log_open, .fuzzing);
            env.tree.open_complete();

            try env.apply(fuzz_ops);
        }

        fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
            const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            env.change_state(.superblock_format, .superblock_open);
        }

        fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
            const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            env.change_state(.superblock_open, .tree_init);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            level: u7,
            table: *const schema.ManifestLog.TableInfo,
        ) void {
            _ = manifest_log;
            _ = level;
            _ = table;

            // This ManifestLog is only opened during setup, when it has no blocks.
            unreachable;
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const env = @fieldParentPtr(@This(), "manifest_log", manifest_log);
            env.change_state(.manifest_log_open, .fuzzing);
        }

        pub fn compact(env: *Environment, op: u64) void {
            env.change_state(.fuzzing, .tree_compact);
            env.tree.compact(tree_compact_callback, op);
            env.tick_until_state_change(.tree_compact, .fuzzing);

            if (op % @divExact(constants.lsm_batch_multiple, 2) == 0 and
                op >= constants.lsm_batch_multiple)
            {
                env.change_state(.fuzzing, .manifest_log_compact);
                env.manifest_log.compact(manifest_log_compact_callback);
                env.tick_until_state_change(.manifest_log_compact, .fuzzing);
            }

            env.tree.compact_end();

            if ((op + 1) % @divExact(constants.lsm_batch_multiple, 2) == 0 and
                op >= constants.lsm_batch_multiple)
            {
                env.manifest_log.compact_end();
            }
        }

        fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
            const env = @fieldParentPtr(@This(), "manifest_log", manifest_log);
            env.change_state(.manifest_log_compact, .fuzzing);
        }

        fn tree_compact_callback(tree: *Tree) void {
            const env = @fieldParentPtr(@This(), "tree", tree);
            env.change_state(.tree_compact, .fuzzing);
        }

        pub fn checkpoint(env: *Environment, op: u64) void {
            //assert(env.checkpoint_op == null);
            env.tree.assert_between_bars();

            {
                // VSRState.monotonic() asserts that the previous_checkpoint id changes.
                // In a normal replica this is guaranteed – even if the LSM is idle and no blocks
                // are acquired or released, the client sessions are necessarily mutated.
                var reply = std.mem.zeroInit(vsr.Header, .{
                    .cluster = cluster,
                    .command = .reply,
                    .op = op,
                    .commit = op,
                });
                reply.set_checksum_body(&.{});
                reply.set_checksum();

                _ = env.superblock.client_sessions.put(1, &reply);
            }

            const checkpoint_op = op - constants.lsm_batch_multiple;
            env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
                .commit_min_checksum = env.superblock.working.vsr_state.commit_min_checksum + 1,
                .commit_min = checkpoint_op,
                .commit_max = checkpoint_op + 1,
                .sync_op_min = 0,
                .sync_op_max = 0,
            });

            env.change_state(.fuzzing, .superblock_checkpoint);
            env.tick_until_state_change(.superblock_checkpoint, .fuzzing);
        }

        fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
            const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            env.change_state(.superblock_checkpoint, .fuzzing);
        }

        pub fn get(env: *Environment, key: Key) ?*const Key.Value {
            env.change_state(.fuzzing, .tree_lookup);
            env.lookup_value = null;

            const fingerprint = key_fingerprint(key);
            switch (env.tree.lookup_from_memory(snapshot_latest, key, fingerprint)) {
                .negative => {
                    get_callback(&env.lookup_context, null);
                },
                .positive => |value| {
                    get_callback(&env.lookup_context, Tree.unwrap_tombstone(value));
                },
                .possible => |level_min| {
                    env.tree.lookup_from_levels_storage(.{
                        .callback = get_callback,
                        .context = &env.lookup_context,
                        .snapshot = snapshot_latest,
                        .key = key,
                        .fingerprint = fingerprint,
                        .level_min = level_min,
                    });
                },
            }
            env.tick_until_state_change(.tree_lookup, .fuzzing);
            return env.lookup_value;
        }

        fn get_callback(lookup_context: *Tree.LookupContext, value: ?*const Key.Value) void {
            const env = @fieldParentPtr(Environment, "lookup_context", lookup_context);
            assert(env.lookup_value == null);
            env.lookup_value = value;
            env.change_state(.tree_lookup, .fuzzing);
        }

        pub fn apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            // The tree should behave like a simple key-value data-structure.
            // We'll compare it to a hash map.
            var model = std.hash_map.AutoHashMap(Key, Key.Value).init(allocator);
            defer model.deinit();

            for (fuzz_ops, 0..) |fuzz_op, fuzz_op_index| {
                assert(env.state == .fuzzing);
                log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzz_op_index, fuzz_ops.len, fuzz_op });

                const storage_size_used = env.storage.size_used();
                log.debug("storage.size_used = {}/{}", .{ storage_size_used, env.storage.size });

                const model_size = model.count() * @sizeOf(Key.Value);
                log.debug("space_amplification = {d:.2}", .{
                    @as(f64, @floatFromInt(storage_size_used)) / @as(f64, @floatFromInt(model_size)),
                });

                // Apply fuzz_op to the tree and the model.
                switch (fuzz_op) {
                    .compact => |c| {
                        env.compact(c.op);
                        if (c.checkpoint) env.checkpoint(c.op);
                    },
                    .put => |value| {
                        if (table_usage == .secondary_index) {
                            if (model.get(Key.key_from_value(&value))) |old_value| {
                                // Not allowed to put a present key without removing the old value first.
                                env.tree.remove(&old_value);
                            }
                        }
                        env.tree.put(&value);
                        try model.put(Key.key_from_value(&value), value);
                    },
                    .remove => |value| {
                        if (table_usage == .secondary_index and !model.contains((Key.key_from_value(&value)))) {
                            // Not allowed to remove non-present keys
                        } else {
                            env.tree.remove(&value);
                        }
                        _ = model.remove(Key.key_from_value(&value));
                    },
                    .get => |key| {
                        // Get account from lsm.
                        const tree_value = env.get(key);

                        // Compare result to model.
                        const model_value = model.get(key);
                        if (model_value == null) {
                            assert(tree_value == null);
                        } else {
                            switch (table_usage) {
                                .general => {
                                    assert(std.mem.eql(
                                        u8,
                                        std.mem.asBytes(&model_value.?),
                                        std.mem.asBytes(tree_value.?),
                                    ));
                                },
                                .secondary_index => {
                                    // secondary_index only preserves keys - may return old values
                                    assert(std.mem.eql(
                                        u8,
                                        std.mem.asBytes(&Key.key_from_value(&model_value.?)),
                                        std.mem.asBytes(&Key.key_from_value(tree_value.?)),
                                    ));
                                },
                            }
                        }
                    },
                }
            }
        }

        fn on_grid_read_fault(grid: *Grid, read: *const Grid.Read) void {
            const env = @fieldParentPtr(Environment, "grid", grid);
            const writing = grid.writing(read.address, null);
            // If the same block faulted more than once, we should only repair once.
            if (writing == .repair) return;
            assert(writing == .not_writing);

            env.grid_repair_queue.append(.{
                .address = read.address,
                .checksum = read.checksum,
            }) catch unreachable;

            if (env.grid_repair_queue.items.len == 1) env.repair_block();
        }

        fn on_block_write_repair(write: *Grid.Write) void {
            const env = @fieldParentPtr(Environment, "grid_repair_write", write);
            const wrote = env.grid_repair_queue.swapRemove(0);
            assert(wrote.address == write.address);

            if (env.grid_repair_queue.items.len > 0) env.repair_block();
        }

        fn repair_block(env: *Environment) void {
            const repair = env.grid_repair_queue.items[0];
            const block = &env.grid_repair_block;

            assert(repair.address > 0);
            assert(!env.grid.superblock.free_set.is_free(repair.address));

            const actual_block = env.grid.superblock.storage.grid_block(repair.address);
            stdx.copy_disjoint(.exact, u8, block.*, actual_block);

            const header = schema.header_from_block(block.*);
            assert(header.op == repair.address);
            assert(header.checksum == repair.checksum);

            env.grid.write_block_repair(
                on_block_write_repair,
                &env.grid_repair_write,
                block,
                repair.address,
            );
        }
    };
}

fn random_id(random: std.rand.Random, comptime Int: type) Int {
    // We have two opposing desires for random ids:
    const avg_int: Int = if (random.boolean())
        // 1. We want to cause many collisions.
        //8
        100 * constants.lsm_growth_factor * tree_options.cache_entries_max
    else
        // 2. We want to generate enough ids that the cache can't hold them all.
        constants.lsm_growth_factor * tree_options.cache_entries_max;
    return fuzz.random_int_exponential(random, Int, avg_int);
}

pub fn generate_fuzz_ops(random: std.rand.Random, fuzz_op_count: usize) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (random.boolean()) 0 else 1,
        // Always do puts, and always more puts than removes.
        .put = constants.lsm_batch_multiple * 2,
        // Maybe do some removes.
        .remove = if (random.boolean()) 0 else constants.lsm_batch_multiple,
        // Maybe do some gets.
        .get = if (random.boolean()) 0 else constants.lsm_batch_multiple,
    };
    log.info("fuzz_op_distribution = {:.2}", .{fuzz_op_distribution});

    log.info("puts_since_compact_max = {}", .{puts_since_compact_max});
    log.info("compacts_per_checkpoint = {}", .{compacts_per_checkpoint});

    var op: u64 = 1;
    var puts_since_compact: usize = 0;
    for (fuzz_ops) |*fuzz_op| {
        const fuzz_op_tag = if (puts_since_compact >= puts_since_compact_max)
            // We have to compact before doing any other operations.
            FuzzOpTag.compact
        else
            // Otherwise pick a random FuzzOp.
            fuzz.random_enum(random, FuzzOpTag, fuzz_op_distribution);
        fuzz_op.* = switch (fuzz_op_tag) {
            .compact => compact: {
                const compact_op = op;
                op += 1;
                const is_checkpoint =
                    // Can only checkpoint on the last beat of the bar.
                    compact_op % constants.lsm_batch_multiple == constants.lsm_batch_multiple - 1 and
                    compact_op > constants.lsm_batch_multiple and
                    // Checkpoint at roughly the same rate as log wraparound.
                    random.uintLessThan(usize, compacts_per_checkpoint) == 0;
                break :compact FuzzOp{
                    .compact = .{
                        .op = compact_op,
                        .checkpoint = is_checkpoint,
                    },
                };
            },
            .put => FuzzOp{ .put = .{
                .id = random_id(random, u64),
                .value = random.int(u63),
            } },
            .remove => FuzzOp{ .remove = .{
                .id = random_id(random, u64),
                .value = random.int(u63),
            } },
            .get => FuzzOp{ .get = .{
                .id = random_id(random, u64),
            } },
        };
        switch (fuzz_op.*) {
            .compact => puts_since_compact = 0,
            // Tree.remove() works by inserting a tombstone, so it counts as a put.
            .put, .remove => puts_since_compact += 1,
            .get => {},
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

    const table_usage = random.enumValue(TableUsage);
    log.info("table_usage={}", .{table_usage});

    const storage_fault_atlas = ClusterFaultAtlas.init(3, random, .{
        .faulty_superblock = false,
        .faulty_wal_headers = false,
        .faulty_wal_prepares = false,
        .faulty_client_replies = false,
        .faulty_grid = true,
    });

    const storage_options = .{
        .seed = random.int(u64),
        .replica_index = 0,
        .read_latency_min = 0,
        .read_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        .write_latency_min = 0,
        .write_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        .read_fault_probability = random.uintLessThan(u8, 100),
        .write_fault_probability = random.uintLessThan(u8, 100),
        .fault_atlas = &storage_fault_atlas,
    };

    const fuzz_op_count = @min(
        fuzz_args.events_max orelse @as(usize, 1E7),
        fuzz.random_int_exponential(random, usize, 1E6),
    );

    const fuzz_ops = try generate_fuzz_ops(random, fuzz_op_count);
    defer allocator.free(fuzz_ops);

    // Init mocked storage.
    var storage = try Storage.init(allocator, constants.storage_size_max, storage_options);
    defer storage.deinit(allocator);

    switch (table_usage) {
        inline else => |usage| {
            try EnvironmentType(usage).run(&storage, fuzz_ops);
        },
    }

    log.info("Passed!", .{});
}
