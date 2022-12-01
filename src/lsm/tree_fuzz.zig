const std = @import("std");
const testing = std.testing;
const allocator = testing.allocator;
const assert = std.debug.assert;

const config = @import("../config.zig");
const fuzz = @import("../test/fuzz.zig");
const vsr = @import("../vsr.zig");

const log = std.log.scoped(.lsm_tree_fuzz);
const tracer = @import("../tracer.zig");

const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;

const Storage = @import("../test/storage.zig").Storage;
const Grid = @import("grid.zig").GridType(Storage);

const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const Tree = @import("tree.zig").TreeType(Table, Storage, "Key.Value");
const Table = @import("table.zig").TableType(
    Key,
    Key.Value,
    Key.compare_keys,
    Key.key_from_value,
    Key.sentinel_key,
    Key.tombstone,
    Key.tombstone_from_key,
);
const Key = packed struct {
    id: u64 align(@alignOf(u64)),

    const Value = packed struct {
        id: u64,
        value: u63,
        tombstone: u1 = 0,
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

const FuzzRunner = @import("tree_fuzz_runner.zig").FuzzRunner(Tree, TreeContext);
const TreeContext = struct {
    const node_count = 1024;
    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = 2048;
    const batch_size_max = config.message_size_max - @sizeOf(vsr.Header);
    const commit_entries_max = @divFloor(batch_size_max, @sizeOf(Key.Value));
    const tree_options = Tree.Options{
        .commit_entries_max = commit_entries_max,
    };

    node_pool: NodePool,
    cache: Tree.TableMutable.ValuesCache,
    lookup_context: Tree.LookupContext = undefined,
    lookup_value: ?*const Key.Value = null,
    lookup_callback: ?fn (*TreeContext) void = null,

    pub fn init() !TreeContext {
        var node_pool = try NodePool.init(allocator, node_count);
        errdefer node_pool.deinit(allocator);

        var cache = try Tree.TableMutable.ValuesCache.init(allocator, cache_entries_max);
        errdefer cache.deinit(allocator);

        return TreeContext{
            .node_pool = node_pool,
            .cache = cache,
        };
    }

    pub fn deinit(context: *TreeContext) void {
        context.cache.deinit(allocator);
        context.node_pool.deinit(allocator);
        context.* = undefined;
    }

    pub fn create(context: *TreeContext, tree: *Tree, grid: *Grid) void {
        tree.* = Tree.init(
            allocator,
            &context.node_pool,
            grid,
            &context.cache,
            tree_options,
        ) catch unreachable;
    }

    pub fn access(context: *TreeContext, callback: fn (*TreeContext) void, tree: *Tree, key: Key) void {
        assert(context.lookup_callback == null);
        context.lookup_callback = callback;

        const value = tree.lookup_from_memory(tree.lookup_snapshot_max, key) orelse {
            tree.lookup_from_levels(get_callback, &context.lookup_context, tree.lookup_snapshot_max, key);
            return;
        };

        context.lookup_value = Tree.unwrap_tombstone(value);
        context.resolve();
    }

    fn get_callback(lookup_context: *Tree.LookupContext, value: ?*const Key.Value) void {
        const context = @fieldParentPtr(TreeContext, "lookup_context", lookup_context);
        context.lookup_value = value;
        context.resolve();
    }

    fn resolve(context: *@This()) void {
        const callback = context.lookup_callback.?;
        context.lookup_callback = null;
        callback(context);
    }
};

const Fuzzer = struct {
    ops: []const FuzzOp,
    op_index: usize,
    model: std.hash_map.AutoHashMap(Key, Key.Value),
    storage: Storage,
    runner: FuzzRunner,

    pub fn run(storage_options: Storage.Options, fuzz_ops: []const FuzzOp) !void {
        var fuzzer: Fuzzer = undefined;
        fuzzer.ops = fuzz_ops;
        fuzzer.op_index = 0;

        // The tree should behave like a simple key-value data-structure.
        // We'll compare it to a hash map.
        fuzzer.model = std.hash_map.AutoHashMap(Key, Key.Value).init(allocator);
        defer fuzzer.model.deinit();

        // Init mocked storage.
        fuzzer.storage = try Storage.init(allocator, FuzzRunner.size_max, storage_options);
        defer fuzzer.storage.deinit(allocator);

        // Run the fuzzer on all ops
        try fuzzer.runner.run(runner_start_callback, &fuzzer.storage);
    }

    fn runner_start_callback(runner: *FuzzRunner) void {
        const fuzzer = @fieldParentPtr(Fuzzer, "runner", runner);
        fuzzer.apply();
    }

    fn apply(fuzzer: *Fuzzer) void {
        if (fuzzer.op_index == fuzzer.ops.len) return fuzzer.runner.stop();

        const fuzz_op = fuzzer.ops[fuzzer.op_index];
        log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzzer.op_index, fuzzer.ops.len, fuzz_op });

        const storage_size_used = fuzzer.storage.size_used();
        log.debug("storage.size_used = {}/{}", .{ storage_size_used, fuzzer.storage.size });

        const model_size = fuzzer.model.count() * @sizeOf(Key.Value);
        log.debug("space_amplification = {d:.2}", .{
            @intToFloat(f64, storage_size_used) / @intToFloat(f64, model_size),
        });

        // Consume and apply fuzz_op to the tree and the model.
        switch (fuzz_op) {
            .put => |value| {
                fuzzer.runner.tree.put(&value);
                fuzzer.model.put(Key.key_from_value(&value), value) catch unreachable;
                fuzzer.tick();
            },
            .remove => |value| {
                fuzzer.runner.tree.remove(&value);
                _ = fuzzer.model.remove(Key.key_from_value(&value));
                fuzzer.tick();
            },
            .get => |key| {
                fuzzer.runner.access(runner_lookup_callback, key);
            },
            .compact => |compact| {
                fuzzer.runner.compact(runner_compact_callback, compact.op);
            },
        }
    }

    fn runner_lookup_callback(runner: *FuzzRunner) void {
        const fuzzer = @fieldParentPtr(Fuzzer, "runner", runner);
        const key = fuzzer.ops[fuzzer.op_index].get;

        const tree_value = fuzzer.runner.tree_context.lookup_value;
        const model_value = fuzzer.model.get(key);

        // Compare result to model.
        if (model_value == null) {
            assert(tree_value == null);
        } else {
            assert(std.mem.eql(
                u8,
                std.mem.asBytes(&model_value.?),
                std.mem.asBytes(tree_value.?),
            ));
        }

        fuzzer.tick();
    }

    fn runner_compact_callback(runner: *FuzzRunner) void {
        const fuzzer = @fieldParentPtr(Fuzzer, "runner", runner);
        const compact = fuzzer.ops[fuzzer.op_index].compact;

        if (compact.checkpoint) {
            fuzzer.runner.checkpoint(runner_checkpoint_callback, compact.op);
        } else {
            fuzzer.tick();
        }
    }

    fn runner_checkpoint_callback(runner: *FuzzRunner) void {
        const fuzzer = @fieldParentPtr(Fuzzer, "runner", runner);
        const compact = fuzzer.ops[fuzzer.op_index].compact;
        assert(compact.checkpoint);
        fuzzer.tick();
    }

    fn tick(fuzzer: *Fuzzer) void {
        assert(fuzzer.op_index < fuzzer.ops.len);
        fuzzer.op_index += 1;
        fuzzer.apply();
    }
};

fn random_id(random: std.rand.Random, comptime Int: type) Int {
    // We have two opposing desires for random ids:
    const avg_int: Int = if (random.boolean())
        // 1. We want to cause many collisions.
        8
    else
        // 2. We want to generate enough ids that the cache can't hold them all.
        TreeContext.cache_entries_max;
    return fuzz.random_int_exponential(random, Int, avg_int);
}

fn generate_fuzz_ops(random: std.rand.Random) ![]const FuzzOp {
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
        .put = config.lsm_batch_multiple * 2,
        // Maybe do some removes.
        .remove = if (random.boolean()) 0 else config.lsm_batch_multiple,
        // Maybe do some gets.
        .get = if (random.boolean()) 0 else config.lsm_batch_multiple,
    };
    log.info("fuzz_op_distribution = {d:.2}", .{fuzz_op_distribution});

    const puts_since_compact_max = TreeContext.commit_entries_max;
    log.info("puts_since_compact_max = {}", .{puts_since_compact_max});

    const compacts_per_checkpoint = std.math.divCeil(
        usize,
        config.journal_slot_count,
        config.lsm_batch_multiple,
    ) catch unreachable;
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
                const checkpoint =
                    // Can only checkpoint on the last beat of the bar.
                    compact_op % config.lsm_batch_multiple == config.lsm_batch_multiple - 1 and
                    compact_op > config.lsm_batch_multiple and
                    // Checkpoint at roughly the same rate as log wraparound.
                    random.uintLessThan(usize, compacts_per_checkpoint) == 0;
                break :compact FuzzOp{
                    .compact = .{
                        .op = compact_op,
                        .checkpoint = checkpoint,
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

    const fuzz_ops = try generate_fuzz_ops(random);
    defer allocator.free(fuzz_ops);

    try Fuzzer.run(Storage.Options{
        .seed = random.int(u64),
        .read_latency_min = 0,
        .read_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        .write_latency_min = 0,
        .write_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
    }, fuzz_ops);

    log.info("Passed!", .{});
}
