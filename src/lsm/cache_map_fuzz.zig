const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");

const TestTable = @import("cache_map.zig").TestTable;
const TestCacheMap = @import("cache_map.zig").TestCacheMap;

const log = std.log.scoped(.lsm_cache_map_fuzz);
const Key = TestTable.Key;
const Value = TestTable.Value;

const stash_value_count_max = 1024;
// Use a large scope (relative to stash_value_count_max) to increase the chances of
// (SetAssociativeCache) hash collisions.
const scope_value_count_max = stash_value_count_max;

const OpValue = struct {
    op: u32,
    value: Value,
};

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    compact,
    get: Key,
    upsert: Value,
    remove: Key,
    scope: enum { open, persist, discard },
};

const Environment = struct {
    cache_map: TestCacheMap,
    model: Model,

    pub fn init(gpa: std.mem.Allocator, options: TestCacheMap.Options) !Environment {
        var cache_map = try TestCacheMap.init(gpa, options);
        errdefer cache_map.deinit(gpa);

        var model = Model.init(gpa);
        errdefer model.deinit(gpa);

        return Environment{
            .cache_map = cache_map,
            .model = model,
        };
    }

    pub fn deinit(self: *Environment, gpa: std.mem.Allocator) void {
        self.model.deinit();
        self.cache_map.deinit(gpa);
    }

    pub fn apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
        // The cache_map should behave exactly like a hash map, with some exceptions:
        // * .compact() removes values added more than one .compact() ago.
        // * .scope_close(.discard) rolls back all operations done from the corresponding
        //   .scope_open()

        for (fuzz_ops, 0..) |fuzz_op, fuzz_op_index| {
            log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzz_op_index, fuzz_ops.len, fuzz_op });

            // Apply fuzz_op to the tree and the model.
            switch (fuzz_op) {
                .compact => {
                    env.cache_map.compact();
                    env.model.compact();
                },
                .upsert => |value| {
                    env.cache_map.upsert(&value);
                    try env.model.upsert(&value);
                },
                .remove => |key| {
                    env.cache_map.remove(key);
                    try env.model.remove(key);
                },
                .get => |key| {
                    // Get account from cache_map.
                    const cache_map_value = env.cache_map.get(key);

                    // Compare result to model.
                    const model_value = env.model.get(key);
                    if (model_value == null) {
                        assert(cache_map_value == null);
                    } else if (env.model.compacts > model_value.?.op) {
                        // .compact() support; if the entry has an op 1 or more compacts ago, it
                        // doesn't have to exist in the cache_map. It may still be served from the
                        // cache layer, however.
                        stdx.maybe(cache_map_value == null);
                        if (cache_map_value) |cache_map_value_unwrapped| {
                            assert(std.meta.eql(cache_map_value_unwrapped.*, model_value.?.value));
                        }
                    } else {
                        assert(std.meta.eql(model_value.?.value, cache_map_value.?.*));
                    }
                },
                .scope => |mode| switch (mode) {
                    .open => {
                        env.cache_map.scope_open();
                        env.model.scope_open();
                    },
                    .persist => {
                        env.cache_map.scope_close(.persist);
                        try env.model.scope_close(.persist);
                    },
                    .discard => {
                        env.cache_map.scope_close(.discard);
                        try env.model.scope_close(.discard);
                    },
                },
            }
        }
    }

    /// Verifies both the positive and negative spaces, as both are equally important. We verify
    /// the positive space by iterating over our model, and ensuring everything exists and is
    /// equal in the cache_map.
    ///
    /// We verify the negative space by iterating over the cache_map's cache and maps directly,
    /// ensuring that:
    /// 1. The values in the cache all exist and are equal in the model.
    /// 2. The values in stash either exists and are equal in the model, or there's the same key
    ///    in the cache.
    /// 3. The values in stash_2 either exists and are equal in the model, or there's the same key
    ///    in stash_1 or the cache.
    pub fn verify(env: *Environment) void {
        var checked: u32 = 0;
        var it = env.model.iterator();
        while (it.next()) |kv| {
            // Compare from cache_map, if found:
            const cache_map_value = env.cache_map.get(kv.key_ptr.*);
            stdx.maybe(cache_map_value != null);
            if (cache_map_value) |cache_map_value_unwrapped| {
                assert(std.meta.eql(kv.value_ptr.value, cache_map_value_unwrapped.*));
            } else {
                // .compact() support:
                assert(env.model.compacts > kv.value_ptr.op);
            }

            checked += 1;
        }

        log.info("Verified {} items from model exist and match in cache_map.", .{checked});

        // It's fine for the cache_map to have values older than .compact() in it; good, in fact,
        // but they _MUST NOT_ be stale.
        if (env.cache_map.cache) |*cache| {
            for (cache.values, 0..) |*cache_value, i| {
                // If the count for an index is 0, the value doesn't exist.
                if (cache.counts.get(i) == 0) {
                    continue;
                }

                const model_val = env.model.get(TestTable.key_from_value(cache_value));
                assert(std.meta.eql(cache_value.*, model_val.?.value));
            }
        }

        // The stash can have stale values, but in that case the real value _must_ exist
        // in the cache. It should be impossible for the stash to have a value that isn't in the
        // model, since cache_map.remove() removes from both the cache and stash.
        var stash_iterator = env.cache_map.stash.keyIterator();
        while (stash_iterator.next()) |stash_value| {
            // Get account from model.
            const model_value = env.model.get(TestTable.key_from_value(stash_value));

            // Even if the stash has stale values, the key must still exist in the model.
            assert(model_value != null);

            const stash_value_equal = std.meta.eql(stash_value.*, model_value.?.value);

            if (!stash_value_equal) {
                if (env.cache_map.cache) |*cache| {
                    // We verified all cache entries were equal and correct above, so if it exists,
                    // it must be right.
                    const cache_value = cache.get(
                        TestTable.key_from_value(stash_value),
                    );
                    assert(cache_value != null);
                }
            }
        }

        log.info(
            "Verified all items in the cache and stash exist and match the model.",
            .{},
        );
    }
};

const Model = struct {
    const Map = std.hash_map.AutoHashMap(Key, OpValue);
    const UndoLog = std.ArrayList(struct {
        key: Key,
        value: ?OpValue,
    });

    map: Map,
    undo_log: UndoLog,
    scope_active: bool = false,
    compacts: u32 = 0,

    fn init(gpa: std.mem.Allocator) Model {
        return .{
            .map = Map.init(gpa),
            .undo_log = UndoLog.init(gpa),
        };
    }

    fn deinit(model: *Model) void {
        model.undo_log.deinit();
        model.map.deinit();
        model.* = undefined;
    }

    fn get(model: *Model, key: Key) ?*OpValue {
        return model.map.getPtr(key);
    }

    fn iterator(model: *Model) Map.Iterator {
        return model.map.iterator();
    }

    fn upsert(model: *Model, value: *const Value) !void {
        const key = TestTable.key_from_value(value);
        const kv_old = try model.map.fetchPut(
            key,
            .{ .op = model.compacts, .value = value.* },
        );
        if (model.scope_active) {
            try model.undo_log.append(.{
                .key = key,
                .value = if (kv_old) |kv| kv.value else null,
            });
        }
    }

    fn remove(model: *Model, key: Key) !void {
        const kv_old = model.map.fetchRemove(key);
        if (model.scope_active) {
            try model.undo_log.append(.{
                .key = key,
                .value = if (kv_old) |kv| kv.value else null,
            });
        }
    }

    fn compact(model: *Model) void {
        assert(!model.scope_active);
        model.compacts += 1;
    }

    fn scope_open(model: *Model) void {
        assert(!model.scope_active);
        assert(model.undo_log.items.len == 0);
        model.scope_active = true;
    }

    fn scope_close(model: *Model, mode: enum { persist, discard }) !void {
        assert(model.scope_active);
        model.scope_active = false;
        defer assert(model.undo_log.items.len == 0);

        switch (mode) {
            .discard => while (model.undo_log.pop()) |undo_entry| {
                if (undo_entry.value) |value| {
                    try model.map.put(undo_entry.key, value);
                } else {
                    _ = model.map.remove(undo_entry.key);
                }
            },
            .persist => model.undo_log.clearRetainingCapacity(),
        }
    }
};

fn random_id(prng: *stdx.PRNG) u32 {
    return fuzz.random_id(prng, u32, .{
        .average_hot = 8,
        .average_cold = scope_value_count_max + stash_value_count_max +
            TestCacheMap.Cache.value_count_max_multiple,
    });
}

pub fn generate_fuzz_ops(
    gpa: std.mem.Allocator,
    prng: *stdx.PRNG,
    fuzz_op_count: usize,
) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try gpa.alloc(FuzzOp, fuzz_op_count);
    errdefer gpa.free(fuzz_ops);

    const fuzz_op_weights = stdx.PRNG.EnumWeightsType(FuzzOpTag){
        // Always do puts, and always more puts than removes.
        .upsert = constants.lsm_compaction_ops * 2,
        // Maybe do some removes.
        .remove = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
        // Maybe do some gets.
        .get = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
        // Maybe do some extra compacts.
        .compact = if (prng.boolean()) 0 else 2,
        // Maybe use scopes.
        .scope = if (prng.boolean()) 0 else @divExact(constants.lsm_compaction_ops, 4),
    };
    log.info("fuzz_op_weights = {:.2}", .{fuzz_op_weights});

    // TODO: Is there a point to making _max random (both here and in .init) and anything less than
    //       the maximum capacity...?
    var op: u64 = 0;
    var operations_since_scope_open: usize = 0;
    const operations_since_scope_open_max: usize = scope_value_count_max;
    var upserts_since_compact: usize = 0;
    const upserts_since_compact_max: usize = stash_value_count_max;
    var scope_is_open = false;
    for (fuzz_ops, 0..) |*fuzz_op, i| {
        var fuzz_op_tag: FuzzOpTag = undefined;
        if (upserts_since_compact >= upserts_since_compact_max) {
            // We have to compact before doing any other operations, but the scope must be closed.
            fuzz_op_tag = FuzzOpTag.compact;

            if (scope_is_open) {
                fuzz_op_tag = FuzzOpTag.scope;
            }
        } else if (operations_since_scope_open >= operations_since_scope_open_max) {
            // We have to close our scope before doing anything else.
            fuzz_op_tag = FuzzOpTag.scope;
        } else if (i == fuzz_ops.len - 1 and scope_is_open) {
            // Ensure we close scope before ending.
            fuzz_op_tag = FuzzOpTag.scope;
        } else if (scope_is_open) {
            fuzz_op_tag = prng.enum_weighted(FuzzOpTag, fuzz_op_weights);
            if (fuzz_op_tag == FuzzOpTag.compact) {
                // We can't compact while a scope is open.
                fuzz_op_tag = FuzzOpTag.scope;
            }
        } else {
            // Otherwise pick a random FuzzOp.
            fuzz_op_tag = prng.enum_weighted(FuzzOpTag, fuzz_op_weights);
            if (i == fuzz_ops.len - 1 and fuzz_op_tag == FuzzOpTag.scope) {
                // We can't let our final operation be a scope open.
                fuzz_op_tag = FuzzOpTag.get;
            }
        }

        fuzz_op.* = switch (fuzz_op_tag) {
            .upsert => blk: {
                upserts_since_compact += 1;

                if (scope_is_open) {
                    operations_since_scope_open += 1;
                }

                break :blk FuzzOp{ .upsert = .{
                    .key = random_id(prng),
                    .value = prng.int(u32),
                } };
            },
            .remove => blk: {
                if (scope_is_open) {
                    operations_since_scope_open += 1;
                }

                break :blk FuzzOp{ .remove = random_id(prng) };
            },
            .get => FuzzOp{ .get = random_id(prng) },
            .compact => blk: {
                upserts_since_compact = 0;
                op += 1;

                break :blk FuzzOp{ .compact = {} };
            },
            .scope => blk: {
                operations_since_scope_open = 0;
                defer scope_is_open = !scope_is_open;

                if (scope_is_open) {
                    break :blk FuzzOp{ .scope = if (prng.boolean()) .persist else .discard };
                } else {
                    break :blk FuzzOp{ .scope = .open };
                }
            },
        };
    }

    return fuzz_ops;
}

pub fn main(gpa: std.mem.Allocator, fuzz_args: fuzz.FuzzArgs) !void {
    var prng = stdx.PRNG.from_seed(fuzz_args.seed);

    const fuzz_op_count = @min(
        fuzz_args.events_max orelse @as(usize, 1E9),
        fuzz.random_int_exponential(&prng, usize, 1E8),
    );

    const fuzz_ops = try generate_fuzz_ops(gpa, &prng, fuzz_op_count);
    defer gpa.free(fuzz_ops);

    // Running the same fuzz with and without cache enabled.
    inline for (&.{ TestCacheMap.Cache.value_count_max_multiple, 0 }) |cache_value_count_max| {
        const options = TestCacheMap.Options{
            .cache_value_count_max = cache_value_count_max,
            .stash_value_count_max = stash_value_count_max,
            .scope_value_count_max = scope_value_count_max,
            .name = "fuzz map",
        };

        var env = try Environment.init(gpa, options);
        defer env.deinit(gpa);

        try env.apply(fuzz_ops);
        env.verify();

        log.info("Passed {any}!", .{options});
    }
}
