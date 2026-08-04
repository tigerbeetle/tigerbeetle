// TODO Test scope_open/scope_close.

const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const fixtures = @import("../testing/fixtures.zig");
const fuzz = @import("../testing/fuzz.zig");
const stdx = @import("stdx");
const vsr = @import("../vsr.zig");

const log = std.log.scoped(.lsm_forest_fuzz);
const tb = @import("../tigerbeetle.zig");

const TimeSim = stdx.TimeSim;
const Storage = @import("../testing/storage.zig").Storage;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage);
const Reservation = @import("../vsr/free_set.zig").Reservation;
const GridType = @import("../vsr/grid.zig").GridType;
const ScanRangeType = @import("../lsm/scan_range.zig").ScanRangeType;
const EvaluateNext = @import("../lsm/scan_range.zig").EvaluateNext;
const ScanLookupType = @import("../lsm/scan_lookup.zig").ScanLookupType;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;
const Direction = @import("../direction.zig").Direction;
const Forest = StateMachine.Forest;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

const FuzzOpAction = union(enum) {
    compact: struct {
        checkpoint: bool,
        checkpoint_durable: bool,
    },
    put: tb.Transfer,
    remove: u128,
    get_by_id: UniqueKey,
    get_by_timestamp: UniqueKey,
    scan: ScanParams,
};
const FuzzOpActionTag = std.meta.Tag(FuzzOpAction);

const FuzzOpModifier = union(enum) {
    normal,
    crash_after_ticks: usize,
};
const FuzzOpModifierTag = std.meta.Tag(FuzzOpModifier);

const FuzzOp = struct {
    op: u64,
    action: FuzzOpAction,
    modifier: FuzzOpModifier,
};

/// This fuzzer skips the StateMachine logic, so it inserts
/// `Transfers` without validating the existence of accounts
/// and pending transfers, which is useful for testing secondary
/// indexes and unique keys.
const GrooveTransfers = @FieldType(
    @FieldType(Forest, "grooves"),
    "transfers",
);
const UniqueKey = GrooveTransfers.UniqueKey;

const ScanParams = struct {
    index: std.meta.FieldEnum(GrooveTransfers.IndexTrees),
    min: u128, // Type-erased field min.
    max: u128, // Type-erased field max.
    direction: Direction,
};

const Environment = struct {
    const node_count = 1024;
    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = GrooveTransfers.ObjectsCache.Cache.value_count_max_multiple;
    const forest_options = StateMachine.forest_options(.{
        .batch_size_limit = constants.message_body_size_max,
        .lsm_forest_compaction_block_count = Forest.Options.compaction_block_count_min,
        .lsm_forest_node_count = node_count,
        .cache_entries_accounts = cache_entries_max,
        .cache_entries_transfers = cache_entries_max,
        .cache_entries_transfers_pending = cache_entries_max,
        .log_trace = true,
        .aof_recovery = false,
    });

    const free_set_fragments_max = 2048;
    const free_set_fragment_size = 67;

    // We must call compact after every 'batch'.
    // Every `lsm_compaction_ops` batches may put/remove `value_count_max` values per index.
    // Every `FuzzOp.put` issues one remove and one put per index.
    const puts_since_compact_max = @divTrunc(
        Forest.groove_config.transfers.ObjectTree.Table.value_count_max,
        2 * constants.lsm_compaction_ops,
    );

    const State = enum {
        init,
        forest_init,
        forest_open,
        fuzzing,
        forest_compact,
        grid_checkpoint,
        forest_checkpoint,
        superblock_checkpoint,
    };

    state: State,
    storage: *Storage,
    time_sim: TimeSim,
    trace: Storage.Tracer,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context,
    grid: Grid,
    forest: Forest,
    checkpoint_op: ?u64,
    ticks_remaining: usize,
    scan_lookup_buffer: []tb.Transfer,

    fn init(env: *Environment, gpa: std.mem.Allocator, storage: *Storage) !void {
        env.storage = storage;

        env.time_sim = fixtures.init_time(.{});
        env.trace = try fixtures.init_tracer(gpa, env.time_sim.interface(), .{});

        env.superblock = try fixtures.init_superblock(gpa, env.storage, .{});

        env.grid = try fixtures.init_grid(gpa, &env.trace, &env.superblock, .{
            .blocks_released_prior_checkpoint_durability_max = Forest
                .compaction_blocks_released_per_pipeline_max(),
        });

        env.scan_lookup_buffer = try gpa.alloc(
            tb.Transfer,
            StateMachine.batch_max.create_transfers,
        );

        env.forest = undefined;
        env.checkpoint_op = null;
        env.ticks_remaining = std.math.maxInt(usize);
    }

    fn deinit(env: *Environment, gpa: std.mem.Allocator) void {
        env.superblock.deinit(gpa);
        env.grid.deinit(gpa);
        env.trace.deinit(gpa);
        gpa.free(env.scan_lookup_buffer);
    }

    pub fn run(gpa: std.mem.Allocator, storage: *Storage, fuzz_ops: []const FuzzOp) !void {
        var env: Environment = undefined;
        env.state = .init;
        try env.init(gpa, storage);
        defer env.deinit(gpa);

        try env.open(gpa);
        defer env.close(gpa);

        try env.apply(gpa, fuzz_ops);
    }

    fn change_state(env: *Environment, current_state: State, next_state: State) void {
        assert(env.state == current_state);
        env.state = next_state;
    }

    fn tick_until_state_change(env: *Environment, current_state: State, next_state: State) !void {
        while (true) {
            if (env.state != current_state) break;

            if (env.ticks_remaining == 0) return error.OutOfTicks;
            env.ticks_remaining -= 1;
            env.storage.run();
        }
        assert(env.state == next_state);
    }

    fn open(env: *Environment, gpa: std.mem.Allocator) !void {
        fixtures.open_superblock(&env.superblock);
        fixtures.open_grid(&env.grid);

        env.change_state(.init, .forest_init);
        try env.forest.init(gpa, &env.grid, .{
            // TODO Test that the same sequence of events applied to forests with different
            // compaction_blocks result in identical grids.
            .compaction_block_count = Forest.Options.compaction_block_count_min,
            .node_count = node_count,
        }, forest_options);
        env.change_state(.forest_init, .forest_open);
        env.forest.open(forest_open_callback);

        try env.tick_until_state_change(.forest_open, .fuzzing);

        if (env.grid.free_set.count_acquired() == 0) {
            // Only run this once, to avoid acquiring an ever-increasing number of (never
            // to-be-released) blocks on every restart.
            env.fragmentate_free_set();
        }
    }

    /// Allocate a sparse subset of grid blocks to make sure that the encoded free set needs more
    /// than one block to exercise the block linked list logic from CheckpointTrailer.
    fn fragmentate_free_set(env: *Environment) void {
        assert(env.grid.free_set.count_acquired() == 0);
        assert(free_set_fragments_max * free_set_fragment_size <= env.grid.free_set.count_free());

        var reservations: [free_set_fragments_max]Reservation = undefined;
        for (&reservations) |*reservation| {
            reservation.* = env.grid.reserve(free_set_fragment_size);
        }
        for (reservations) |reservation| {
            _ = env.grid.free_set.acquire(reservation).?;
        }
        for (reservations) |reservation| {
            env.grid.free_set.forfeit(reservation);
        }
    }

    fn close(env: *Environment, gpa: std.mem.Allocator) void {
        env.forest.deinit(gpa);
    }

    fn forest_open_callback(forest: *Forest) void {
        const env: *Environment = @fieldParentPtr("forest", forest);
        env.change_state(.forest_open, .fuzzing);
    }

    pub fn compact(env: *Environment, op: u64) !void {
        env.change_state(.fuzzing, .forest_compact);
        env.forest.compact(forest_compact_callback, op);
        try env.tick_until_state_change(.forest_compact, .fuzzing);
    }

    fn forest_compact_callback(forest: *Forest) void {
        const env: *Environment = @fieldParentPtr("forest", forest);
        env.change_state(.forest_compact, .fuzzing);
    }

    pub fn checkpoint(env: *Environment, op: u64) !void {
        assert(env.checkpoint_op == null);
        env.checkpoint_op = op - constants.lsm_compaction_ops;

        env.change_state(.fuzzing, .forest_checkpoint);
        env.forest.checkpoint(forest_checkpoint_callback);
        try env.tick_until_state_change(.forest_checkpoint, .grid_checkpoint);

        env.grid.checkpoint(grid_checkpoint_callback);
        try env.tick_until_state_change(.grid_checkpoint, .superblock_checkpoint);

        env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
            .header = header: {
                var header = vsr.Header.Prepare.root(fixtures.cluster);
                header.op = env.checkpoint_op.?;
                header.set_checksum();
                break :header header;
            },
            .view_attributes = null,
            .manifest_references = env.forest.manifest_log.checkpoint_references(),
            .free_set_references = .{
                .blocks_acquired = env.grid
                    .free_set_checkpoint_blocks_acquired.checkpoint_reference(),
                .blocks_released = env.grid
                    .free_set_checkpoint_blocks_released.checkpoint_reference(),
            },
            .client_sessions_reference = .{
                .last_block_checksum = 0,
                .last_block_address = 0,
                .trailer_size = 0,
                .checksum = vsr.checksum(&.{}),
            },
            .commit_max = env.checkpoint_op.? + 1,
            .sync_op_min = 0,
            .sync_op_max = 0,
            .storage_size = vsr.superblock.data_file_size_min +
                (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
            .release = vsr.Release.minimum,
        });
        try env.tick_until_state_change(.superblock_checkpoint, .fuzzing);

        env.grid.mark_checkpoint_not_durable();
        env.checkpoint_op = null;
    }

    fn grid_checkpoint_callback(grid: *Grid) void {
        const env: *Environment = @alignCast(@fieldParentPtr("grid", grid));
        assert(env.checkpoint_op != null);
        env.change_state(.grid_checkpoint, .superblock_checkpoint);
    }

    fn forest_checkpoint_callback(forest: *Forest) void {
        const env: *Environment = @alignCast(@fieldParentPtr("forest", forest));
        assert(env.checkpoint_op != null);
        env.change_state(.forest_checkpoint, .grid_checkpoint);
    }

    fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
        const env: *Environment =
            @alignCast(@fieldParentPtr("superblock_context", superblock_context));
        env.change_state(.superblock_checkpoint, .fuzzing);
    }

    fn prefetch(env: *Environment, key: UniqueKey, snapshot: u64) !void {
        const Context = struct {
            _key: UniqueKey,
            _snapshot: u64,
            _groove: *GrooveTransfers,

            finished: bool = false,
            prefetch_context: GrooveTransfers.PrefetchContext = undefined,

            fn prefetch_start(getter: *@This()) void {
                const groove = getter._groove;
                groove.prefetch_setup(getter._snapshot);
                groove.prefetch_enqueue(getter._key);
                groove.prefetch(@This().prefetch_callback, &getter.prefetch_context);
            }

            fn prefetch_callback(prefetch_context: *GrooveTransfers.PrefetchContext) void {
                const context: *@This() = @fieldParentPtr("prefetch_context", prefetch_context);
                assert(!context.finished);
                context.finished = true;
            }
        };

        var context = Context{
            ._key = key,
            ._snapshot = snapshot,
            ._groove = &env.forest.grooves.transfers,
        };
        context.prefetch_start();
        while (!context.finished) {
            if (env.ticks_remaining == 0) return error.OutOfTicks;
            env.ticks_remaining -= 1;
            env.storage.run();
        }
    }

    fn put(env: *Environment, a: *const tb.Transfer, maybe_old: ?tb.Transfer) void {
        if (maybe_old) |*old| {
            env.forest.grooves.transfers.update(.{ .old = old, .new = a });
        } else {
            env.forest.grooves.transfers.insert(a);
        }
    }

    fn remove(env: *Environment, transfer: *const tb.Transfer) void {
        env.forest.grooves.transfers.remove(transfer.id);
    }

    fn get(env: *Environment, key: UniqueKey) ?tb.Transfer {
        return switch (key) {
            .id => |id| switch (env.forest.grooves.transfers.get(id)) {
                .found_object => |transfer| transfer,
                .found_orphaned => unreachable, // TODO: test orphaned IDs.
                .not_found => null,
            },
            else => env.forest.grooves.transfers.indirect_lookup(key),
        };
    }

    fn check_lookup(
        env: *Environment,
        key: UniqueKey,
        snapshot: u64,
        expected: ?tb.Transfer,
    ) !?tb.Transfer {
        try env.prefetch(key, snapshot);
        const actual = env.get(key);
        try std.testing.expectEqualDeep(expected, actual);
        return actual;
    }

    fn ScannerIndexType(comptime index: std.meta.FieldEnum(GrooveTransfers.IndexTrees)) type {
        const Tree = @FieldType(GrooveTransfers.IndexTrees, @tagName(index));
        const Value = Tree.Table.Value;
        const Index = GrooveTransfers.IndexHelperType(@tagName(index)).Type;

        const ScanRange = ScanRangeType(
            Tree,
            Storage,
            void,
            struct {
                inline fn value_next(_: void, _: *const Value) EvaluateNext {
                    return .include_and_continue;
                }
            }.value_next,
            struct {
                inline fn timestamp_from_value(_: void, value: *const Value) u64 {
                    return value.timestamp;
                }
            }.timestamp_from_value,
        );

        const ScanLookup = ScanLookupType(
            GrooveTransfers,
            ScanRange,
            Storage,
        );

        return struct {
            const ScannerIndex = @This();

            lookup: ScanLookup = undefined,
            result: ?[]const tb.Transfer = null,

            fn scan(
                self: *ScannerIndex,
                env: *Environment,
                params: ScanParams,
                snapshot: u64,
            ) ![]const tb.Transfer {
                const min: Index, const max: Index = switch (Index) {
                    void => range: {
                        assert(params.min == 0);
                        assert(params.max == 0);
                        break :range .{ {}, {} };
                    },
                    else => range: {
                        const min: Index = @intCast(params.min);
                        const max: Index = @intCast(params.max);
                        assert(min <= max);
                        break :range .{ min, max };
                    },
                };

                const scan_buffer_pool = &env.forest.scan_buffer_pool;
                const groove = &env.forest.grooves.transfers;
                defer scan_buffer_pool.reset();

                // It's not expected to exceed `lsm_scans_max` here.
                const scan_buffer = scan_buffer_pool.acquire() catch unreachable;

                var scan_range: ScanRange = undefined;
                scan_range.init(
                    {},
                    &@field(groove.indexes, @tagName(index)),
                    scan_buffer,
                    snapshot,
                    Value.key_from_value(&.{
                        .field = min,
                        .timestamp = TimestampRange.timestamp_min,
                    }),
                    Value.key_from_value(&.{
                        .field = max,
                        .timestamp = TimestampRange.timestamp_max,
                    }),
                    params.direction,
                );

                self.lookup = ScanLookup.init(groove, &scan_range);
                self.lookup.read(env.scan_lookup_buffer, &scan_lookup_callback);

                while (self.result == null) {
                    if (env.ticks_remaining == 0) return error.OutOfTicks;
                    env.ticks_remaining -= 1;
                    env.storage.run();
                }

                return self.result.?;
            }

            fn scan_lookup_callback(lookup: *ScanLookup, result: []const tb.Transfer) void {
                const self: *ScannerIndex = @fieldParentPtr("lookup", lookup);
                assert(self.result == null);
                self.result = result;
            }
        };
    }

    fn scan(env: *Environment, params: ScanParams, snapshot: u64) ![]const tb.Transfer {
        switch (params.index) {
            inline else => |index| {
                const Scanner = ScannerIndexType(index);
                var scanner = Scanner{};
                return try scanner.scan(env, params, snapshot);
            },
        }
    }

    // The forest should behave like a simple persistent key-value data structure.
    //
    // The model maintains two sets of transfers and a log:
    // 1. transfers_mutable: contains all transfers in their current state.
    // 2. transfers_stashed: contains transfers as of the latest checkpoint.
    // 3. log: records updates not yet covered by a checkpoint, since each checkpoint
    //    persists only a prefix of the log.
    //
    // Each transfer set has two indexes: transfers_by_id stores transfers keyed by ID,
    // and id_by_timestamp maps timestamp UniqueKeys to transfer IDs. On storage reset,
    // transfers_mutable is restored from transfers_stashed and the log is discarded.
    //
    // The goal is to keep the model independent of the LSM / forest implementation.
    const Model = struct {
        const ObjectMap = std.hash_map.AutoHashMap(u128, tb.Transfer);
        const UniqueKeysMap = std.hash_map.AutoHashMap(UniqueKey, u128);
        const Indexes = struct {
            transfers_by_id: ObjectMap,
            id_by_timestamp: UniqueKeysMap,

            pub fn init(gpa: std.mem.Allocator) Indexes {
                return .{
                    .transfers_by_id = ObjectMap.init(gpa),
                    .id_by_timestamp = UniqueKeysMap.init(gpa),
                };
            }
            pub fn deinit(indexes: *Indexes) void {
                indexes.transfers_by_id.deinit();
                indexes.id_by_timestamp.deinit();
            }

            pub fn clone(indexes: Indexes) !Indexes {
                var transfers_by_id = try indexes.transfers_by_id.clone();
                errdefer transfers_by_id.deinit();
                return .{
                    .transfers_by_id = transfers_by_id,
                    .id_by_timestamp = try indexes.id_by_timestamp.clone(),
                };
            }

            pub fn put(indexes: *Indexes, transfer: tb.Transfer) !void {
                try indexes.id_by_timestamp.put(.{ .timestamp = transfer.timestamp }, transfer.id);
                try indexes.transfers_by_id.put(transfer.id, transfer);
            }

            pub fn get(indexes: Indexes, key: UniqueKey) ?tb.Transfer {
                switch (key) {
                    .id => |id| return indexes.transfers_by_id.get(id),
                    .timestamp => {
                        const transfer_id = indexes.id_by_timestamp.get(key) orelse return null;
                        return indexes.transfers_by_id.get(transfer_id);
                    },
                }
            }

            pub fn remove(indexes: *Indexes, transfer_id: u128) void {
                const transfer = indexes.transfers_by_id.fetchRemove(transfer_id).?;
                assert(indexes.id_by_timestamp.remove(.{ .timestamp = transfer.value.timestamp }));
            }
        };

        const Operation = union(enum) { put: tb.Transfer, remove };
        const LogEntry = struct { op: u64, id: u128, operation: Operation };
        const Log = std.ArrayList(LogEntry);

        transfers_mutable: Indexes,
        transfers_stashed: Indexes,
        log: Log,

        gpa: std.mem.Allocator,

        pub fn init(gpa: std.mem.Allocator) Model {
            return .{
                .transfers_mutable = Indexes.init(gpa),
                .transfers_stashed = Indexes.init(gpa),
                .log = .empty,
                .gpa = gpa,
            };
        }

        pub fn deinit(model: *Model) void {
            model.transfers_mutable.deinit();
            model.transfers_stashed.deinit();
            model.log.deinit(model.gpa);
        }

        pub fn put(model: *Model, transfer: *const tb.Transfer, op: u64) !void {
            try model.mutate(.{ .op = op, .id = transfer.id, .operation = .{ .put = transfer.* } });
        }

        pub fn remove(model: *Model, id: u128, op: u64) !void {
            try model.mutate(.{ .op = op, .id = id, .operation = .remove });
        }

        fn mutate(model: *Model, entry: LogEntry) !void {
            const log_count = model.log.items.len;
            if (log_count > 0) assert(model.log.items[log_count - 1].op <= entry.op);

            try model.log.append(model.gpa, entry);
            try apply_entry(&model.transfers_mutable, entry);
        }

        fn apply_entry(indexes: *Indexes, entry: LogEntry) !void {
            switch (entry.operation) {
                .put => |transfer| {
                    assert(transfer.id == entry.id);
                    try indexes.put(transfer);
                },
                .remove => indexes.remove(entry.id),
            }
        }

        pub fn get(model: *const Model, key: UniqueKey) ?tb.Transfer {
            return model.transfers_mutable.get(key);
        }

        pub fn checkpoint(model: *Model, op: u64) !void {
            const checkpointable = op - (op % constants.lsm_compaction_ops) -| 1;

            var log_index: usize = 0;
            while (log_index < model.log.items.len) : (log_index += 1) {
                const entry = model.log.items[log_index];
                if (entry.op > checkpointable) {
                    break;
                }
                try apply_entry(&model.transfers_stashed, entry);
            }

            model.log.replaceRangeAssumeCapacity(0, log_index, &.{});
        }

        pub fn storage_reset(model: *Model) !void {
            model.transfers_mutable.deinit();
            model.transfers_mutable = try model.transfers_stashed.clone();
            model.log.clearRetainingCapacity();
        }

        pub fn scan(model: *const Model, params: ScanParams) ![]tb.Transfer {
            var matches: std.ArrayList(tb.Transfer) = .empty;
            errdefer matches.deinit(model.gpa);

            var iterator = model.transfers_mutable.transfers_by_id.valueIterator();
            while (iterator.next()) |transfer| {
                const key = scan_key(params.index, transfer) orelse continue;
                if (key >= params.min and key <= params.max) {
                    try matches.append(model.gpa, transfer.*);
                }
            }
            std.mem.sort(tb.Transfer, matches.items, params, struct {
                fn less_than(context: ScanParams, a: tb.Transfer, b: tb.Transfer) bool {
                    const key_a = scan_key(context.index, &a).?;
                    const key_b = scan_key(context.index, &b).?;
                    const order = if (key_a == key_b)
                        std.math.order(a.timestamp, b.timestamp)
                    else
                        std.math.order(key_a, key_b);
                    return order == switch (context.direction) {
                        .ascending => std.math.Order.lt,
                        .descending => std.math.Order.gt,
                    };
                }
            }.less_than);
            return matches.toOwnedSlice(model.gpa);
        }

        fn scan_key(index: @FieldType(ScanParams, "index"), object: *const tb.Transfer) ?u128 {
            return switch (index) {
                .expires_at => if (object.flags.pending and object.timeout > 0)
                    object.timestamp + object.timeout_ns()
                else
                    null,
                .imported => if (object.flags.imported) 0 else null,
                .closing => if (object.flags.closing_debit or object.flags.closing_credit)
                    0
                else
                    null,
                inline .pending_id, .user_data_128, .user_data_64, .user_data_32 => |field| key: {
                    const value = @field(object, @tagName(field));
                    break :key if (value == 0) null else value;
                },
                inline else => |field| @field(object, @tagName(field)),
            };
        }
    };

    fn apply(env: *Environment, gpa: std.mem.Allocator, fuzz_ops: []const FuzzOp) !void {
        var model = Model.init(gpa);
        defer model.deinit();

        for (fuzz_ops, 0..) |fuzz_op, fuzz_op_index| {
            assert(env.state == .fuzzing);
            log.debug("Running fuzz_ops[{}/{}] == {}", .{
                fuzz_op_index,
                fuzz_ops.len,
                fuzz_op.action,
            });

            const storage_size_used = env.storage.size_used();
            log.debug("storage.size_used = {}/{}", .{ storage_size_used, env.storage.size });

            const model_size = model.transfers_mutable.transfers_by_id.count() *
                @sizeOf(tb.Transfer);
            log.debug("space_amplification ~= {d:.2}", .{
                @as(f64, @floatFromInt(storage_size_used)) / @as(f64, @floatFromInt(model_size)),
            });

            try env.apply_op(gpa, fuzz_op, &model);
        }

        log.debug("Applied all ops", .{});
    }

    fn apply_op(env: *Environment, gpa: std.mem.Allocator, fuzz_op: FuzzOp, model: *Model) !void {
        switch (fuzz_op.modifier) {
            .normal => {
                env.ticks_remaining = std.math.maxInt(usize);
                env.apply_op_action(fuzz_op, model) catch |err| {
                    switch (err) {
                        error.OutOfTicks => unreachable,
                        else => return err,
                    }
                };
            },
            .crash_after_ticks => |ticks_remaining| {
                env.ticks_remaining = ticks_remaining;
                env.apply_op_action(fuzz_op, model) catch |err| {
                    switch (err) {
                        error.OutOfTicks => {},
                        else => return err,
                    }
                };
                env.ticks_remaining = std.math.maxInt(usize);

                env.storage.log_pending_io();
                env.close(gpa);
                env.deinit(gpa);
                env.storage.reset();

                env.state = .init;
                try env.init(gpa, env.storage);

                try env.open(gpa);

                // Every ID changed after the checkpoint must return to its checkpointed state,
                // including IDs that should no longer exist after recovery.
                const snapshot = blk: {
                    if (vsr.Checkpoint.trigger_for_checkpoint(
                        env.superblock.working.vsr_state.checkpoint.header.op,
                    )) |trigger| {
                        break :blk trigger + 1;
                    } else break :blk 0;
                };

                // Recovery checks can prefetch more objects than a single commit's stash.
                // So we need to call `compact` in the loops below.
                const groove_stash_value_count_max = env.forest.grooves
                    .transfers.objects_cache.options.stash_value_count_max;
                var index: u32 = 0;

                while (index < model.log.items.len) : (index += 1) {
                    const entry = model.log.items[index];
                    const id = entry.id;
                    _ = try env.check_lookup(
                        .{ .id = id },
                        snapshot,
                        model.transfers_stashed.get(.{ .id = id }),
                    );

                    if (index % groove_stash_value_count_max == 0) {
                        env.forest.grooves.transfers.objects_cache.compact();
                    }
                }
                // Here we check that we have not lost objects that should be in the checkpoint.
                var iterator = model.transfers_stashed.transfers_by_id.valueIterator();
                while (iterator.next()) |transfer| : (index += 1) {
                    _ = try env.check_lookup(.{ .id = transfer.id }, snapshot, transfer.*);
                    if (index % groove_stash_value_count_max == 0) {
                        env.forest.grooves.transfers.objects_cache.compact();
                    }
                }
                // This is required to reset the stash again otherwise it can overflow.
                env.forest.grooves.transfers.objects_cache.compact();
                try model.storage_reset();
            },
        }
    }

    fn apply_op_action(env: *Environment, fuzz_op: FuzzOp, model: *Model) !void {
        const snapshot = if (env.superblock.working.vsr_state.op_compacted(fuzz_op.op))
            vsr.Checkpoint.trigger_for_checkpoint(
                env.superblock.working.vsr_state.checkpoint.header.op,
            ).? + 1
        else
            fuzz_op.op;

        switch (fuzz_op.action) {
            .compact => |c| {

                // Checkpoint is marked durable *before* a replica compacts the (pipeline + 1)ᵗʰ
                // op. This is because `blocks_released_prior_checkpoint_durability` in FreeSet
                // is sized according to the maximum number of blocks released by a pipeline of ops
                // (see `blocks_released_prior_checkpoint_durability_max` in vsr.zig).
                if (c.checkpoint_durable) env.grid.free_set.mark_checkpoint_durable();

                try env.compact(fuzz_op.op);
                if (c.checkpoint) {
                    assert(!c.checkpoint_durable);
                    try model.checkpoint(fuzz_op.op);
                    try env.checkpoint(fuzz_op.op);
                }
            },
            .put => |object| {
                // The forest requires prefetch before put.
                assert(object.id != 0);

                const key: UniqueKey = .{ .id = object.id };
                const lsm_object = try env.check_lookup(key, snapshot, model.get(key));

                env.put(&object, lsm_object);
                try model.put(&object, fuzz_op.op);
            },
            .remove => |id| {
                const key: UniqueKey = .{ .id = id };
                if (try env.check_lookup(key, snapshot, model.get(key))) |object| {
                    env.remove(&object);
                    try model.remove(id, fuzz_op.op);
                }
            },
            inline .get_by_id,
            .get_by_timestamp,
            => |key, action| {
                switch (action) {
                    .get_by_id => {
                        assert(key == .id);
                        assert(key.id != 0);
                    },
                    .get_by_timestamp => {
                        assert(key == .timestamp);
                        assert(TimestampRange.valid(key.timestamp));
                    },
                    else => comptime unreachable,
                }
                _ = try env.check_lookup(key, snapshot, model.get(key));
            },
            .scan => |params| {
                // TODO: Test pagination.
                const results = try env.scan(params, snapshot);
                const expected = try model.scan(params);
                defer model.gpa.free(expected);

                const count = @min(expected.len, env.scan_lookup_buffer.len);
                assert((expected.len == 0) == (results.len == 0));

                try std.testing.expectEqualSlices(tb.Transfer, expected[0..count], results);
            },
        }
    }
};

fn random_id(prng: *stdx.PRNG, comptime Int: type) Int {
    return fuzz.random_id(prng, Int, .{
        .average_hot = 8,
        .average_cold = Environment.cache_entries_max,
    }) + 1; // Does not allow zero.
}

pub fn generate_fuzz_ops(
    gpa: std.mem.Allocator,
    prng: *stdx.PRNG,
    fuzz_op_count: usize,
) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try gpa.alloc(FuzzOp, fuzz_op_count);
    errdefer gpa.free(fuzz_ops);

    const action_weights = stdx.PRNG.EnumWeightsType(FuzzOpActionTag){
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (prng.boolean()) 0 else 1,
        // Always do puts.
        .put = constants.lsm_compaction_ops * 2,
        // Sometimes do many removes, so we can stress tombstones still in cache.
        // Sometimes do it rarely, so we can reach tombstones in deeper levels.
        .remove = if (prng.boolean()) 1 else constants.lsm_compaction_ops,
        // Maybe do some gets.
        .get_by_id = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
        .get_by_timestamp = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
        // Maybe do some scans.
        .scan = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
    };
    log.info("action_weights = {:.2}", .{action_weights});

    const modifier_weights = stdx.PRNG.EnumWeightsType(FuzzOpModifierTag){
        .normal = 100,
        // Maybe crash and recover from the last checkpoint a few times per fuzzer run.
        .crash_after_ticks = if (prng.boolean()) 0 else 1,
    };
    log.info("modifier_weights = {:.2}", .{modifier_weights});

    log.info("puts_since_compact_max = {}", .{Environment.puts_since_compact_max});

    var id_to_object = std.hash_map.AutoHashMap(u128, tb.Transfer).init(gpa);
    defer id_to_object.deinit();

    var op: u64 = 1;
    var persisted_op: u64 = 0;
    var puts_since_compact: usize = 0;
    for (fuzz_ops, 0..) |*fuzz_op, fuzz_op_index| {
        const too_many_puts = puts_since_compact >= Environment.puts_since_compact_max;
        const action_tag: FuzzOpActionTag = if (too_many_puts)
            // We have to compact before doing any other operations.
            .compact
        else
            // Otherwise pick a prng FuzzOp.
            prng.enum_weighted(FuzzOpActionTag, action_weights);
        const action = switch (action_tag) {
            .compact => action: {
                const action = generate_compact(.{ .op = op, .persisted_op = persisted_op });
                if (action.compact.checkpoint) {
                    assert(!action.compact.checkpoint_durable);
                    persisted_op = op - constants.lsm_compaction_ops;
                }
                break :action action;
            },
            .put => action: {
                const action = generate_put(
                    prng,
                    &id_to_object,
                    fuzz_op_index + 1, // Timestamp cannot be zero.
                );
                assert(action.put.id != 0);

                try id_to_object.put(action.put.id, action.put);
                break :action action;
            },
            .remove => FuzzOpAction{
                .remove = id: {
                    const it = id_to_object.keyIterator();
                    for (0..it.len) |_| {
                        const index = prng.int_inclusive(usize, it.len - 1);
                        if (!it.metadata[index].isUsed()) continue;

                        break :id it.items[index];
                    }
                    break :id 0;
                },
            },
            .get_by_id => FuzzOpAction{ .get_by_id = .{
                .id = random_id(prng, u128),
            } },
            .get_by_timestamp => FuzzOpAction{
                // Not all ops generate objects, so the timestamp may or may not be found.
                .get_by_timestamp = .{ .timestamp = prng.range_inclusive(
                    u64,
                    TimestampRange.timestamp_min,
                    fuzz_op_index + 1,
                ) },
            },
            .scan => blk: {
                @setEvalBranchQuota(10_000);
                const Index = std.meta.FieldEnum(GrooveTransfers.IndexTrees);
                const index = prng.enum_uniform(Index);
                break :blk switch (index) {
                    inline else => |field| {
                        const IndexHelper = GrooveTransfers.IndexHelperType(@tagName(field));
                        const min: u128, const max: u128 = switch (IndexHelper.Type) {
                            void => .{ 0, 0 },
                            else => range: {
                                var min = random_id(prng, IndexHelper.Type);
                                var max = if (prng.boolean()) min else random_id(
                                    prng,
                                    IndexHelper.Type,
                                );
                                if (min > max) std.mem.swap(IndexHelper.Type, &min, &max);
                                assert(min <= max);
                                break :range .{ min, max };
                            },
                        };

                        break :blk FuzzOpAction{
                            .scan = .{
                                .index = index,
                                .min = min,
                                .max = max,
                                .direction = prng.enum_uniform(Direction),
                            },
                        };
                    },
                };
            },
        };

        // TODO(jamii)
        // Currently, crashing is only interesting during a compact.
        // But once we have concurrent compaction, crashing at any point can be interesting.
        //
        // TODO(jamii)
        // If we crash during a checkpoint, on restart we should either:
        // * See the state from that checkpoint.
        // * See the state from the previous checkpoint.
        // But this is difficult to test, so for now we'll avoid it.
        const modifier_tag = if (action == .compact and !action.compact.checkpoint)
            prng.enum_weighted(FuzzOpModifierTag, modifier_weights)
        else
            FuzzOpModifierTag.normal;
        const modifier = switch (modifier_tag) {
            .normal => FuzzOpModifier{ .normal = {} },
            .crash_after_ticks => FuzzOpModifier{
                .crash_after_ticks = fuzz.random_int_exponential(
                    prng,
                    usize,
                    io_latency_mean_ticks,
                ),
            },
        };

        fuzz_op.* = .{
            .op = op,
            .action = action,
            .modifier = modifier,
        };

        switch (modifier) {
            .normal => {},
            .crash_after_ticks => op = persisted_op,
        }
        switch (action) {
            .compact => {
                op += 1;
                puts_since_compact = 0;
            },
            .put => puts_since_compact += 1,
            .remove => puts_since_compact += 1,
            .get_by_id => {},
            .get_by_timestamp => {},
            .scan => {},
        }
    }

    return fuzz_ops;
}

fn generate_compact(options: struct { op: u64, persisted_op: u64 }) FuzzOpAction {
    const checkpoint =
        // Can only checkpoint on the last beat of the bar.
        options.op % constants.lsm_compaction_ops == constants.lsm_compaction_ops - 1 and
        options.op > constants.lsm_compaction_ops and
        // Never checkpoint at the same op twice
        options.op > options.persisted_op + constants.lsm_compaction_ops and
        // Checkpoint at the normal rate.
        // TODO Make LSM (and this fuzzer) unaware of VSR's checkpoint schedule.
        options.op == vsr.Checkpoint.trigger_for_checkpoint(
            vsr.Checkpoint.checkpoint_after(options.persisted_op),
        );

    // Checkpoint is considered durable when a replica is committing/compacting the (pipeline + 1)ᵗʰ
    // prepare after checkpoint trigger. See `op_repair_min` in `replica.zig` for more context.
    const checkpoint_durable = checkpoint_durable: {
        if (vsr.Checkpoint.trigger_for_checkpoint(options.persisted_op)) |trigger| {
            if (options.op == trigger + constants.pipeline_prepare_queue_max + 1)
                break :checkpoint_durable true
            else
                break :checkpoint_durable false;
        } else {
            assert(options.persisted_op == 0);
            if (options.op == 1)
                break :checkpoint_durable true
            else
                break :checkpoint_durable false;
        }
    };

    return FuzzOpAction{ .compact = .{
        .checkpoint = checkpoint,
        .checkpoint_durable = checkpoint_durable,
    } };
}

fn generate_put(
    prng: *stdx.PRNG,
    id_to_object: *const std.AutoHashMap(u128, tb.Transfer),
    timestamp: u64,
) FuzzOpAction {
    const id = random_id(prng, u128);
    var object: tb.Transfer = id_to_object.get(id) orelse .{
        // `id` is the primary key.
        .id = id,
        // `timestamp` is a unique key.
        .timestamp = timestamp,
        // `pending_id` is a unique key, but optional.
        // Don't use `random_id` to avoid collision.
        .pending_id = if (prng.boolean() or id_to_object.count() == 0) 0 else prng.int(u128),

        .debit_account_id = 0,
        .credit_account_id = 0,
        .user_data_128 = 0,
        .user_data_64 = 0,
        .user_data_32 = 0,
        .ledger = 0,
        .code = 0,
        .flags = .{},
        .timeout = 0,
        .amount = 0,
    };

    // Although the StateMachine does not allow mutating a `Transfer`,
    // the LSM object and secondary indexes can still be updated,
    // so we do it for fuzzing purposes.
    object.debit_account_id = random_id(prng, u128);
    object.credit_account_id = random_id(prng, u128);
    object.user_data_128 = random_id(prng, u128);
    object.user_data_64 = random_id(prng, u64);
    object.user_data_32 = random_id(prng, u32);
    object.ledger = random_id(prng, u32);
    object.code = random_id(prng, u16);
    object.timeout = if (prng.boolean()) 0 else prng.int(u32);
    object.flags.imported = prng.boolean();
    object.flags.closing_debit = prng.boolean();
    object.flags.closing_credit = prng.boolean();

    // Increment this field as a version indicator.
    object.amount += 1;

    return FuzzOpAction{ .put = object };
}

const io_latency_mean_ticks = 20;
const io_latency_mean_ms: u64 = io_latency_mean_ticks * constants.tick_ms;

pub fn main(gpa: std.mem.Allocator, fuzz_args: fuzz.FuzzArgs) !void {
    var prng = stdx.PRNG.from_seed(fuzz_args.seed);

    const fuzz_op_count = @min(
        fuzz_args.events_max orelse @as(usize, 1E6),
        fuzz.random_int_exponential(&prng, usize, 1E5),
    );

    const fuzz_ops = try generate_fuzz_ops(gpa, &prng, fuzz_op_count);
    defer gpa.free(fuzz_ops);

    // Init mocked storage.
    var storage = try fixtures.init_storage(gpa, .{
        .seed = prng.int(u64),
        .size = constants.storage_size_limit_default,
        .read_latency_min = .{ .ns = 0 },
        .read_latency_mean = fuzz.range_inclusive_ms(&prng, 0, io_latency_mean_ms),
        .write_latency_min = .{ .ns = 0 },
        .write_latency_mean = fuzz.range_inclusive_ms(&prng, 0, io_latency_mean_ms),
    });
    defer storage.deinit(gpa);

    try fixtures.storage_format(gpa, &storage, .{});

    try Environment.run(gpa, &storage, fuzz_ops);

    log.info("Passed!", .{});
}
