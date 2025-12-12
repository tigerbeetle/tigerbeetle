const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const stdx = @import("stdx");
const constants = @import("../constants.zig");
const fixtures = @import("../testing/fixtures.zig");
const fuzz = @import("../testing/fuzz.zig");
const vsr = @import("../vsr.zig");
const schema = @import("schema.zig");
const ratio = stdx.PRNG.ratio;

const log = std.log.scoped(.lsm_tree_fuzz);

const ScratchMemory = @import("scratch_memory.zig").ScratchMemory;
const Direction = @import("../direction.zig").Direction;
const TimeSim = @import("../testing/time.zig").TimeSim;
const Storage = @import("../testing/storage.zig").Storage;
const GridType = @import("../vsr/grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePoolType(constants.lsm_manifest_node_size, 16);
const TableUsage = @import("table.zig").TableUsage;
const TableType = @import("table.zig").TableType;
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);
const ManifestLogPace = @import("manifest_log.zig").Pace;
const snapshot_min_for_table_output = @import("compaction.zig").snapshot_min_for_table_output;
const compaction_op_min = @import("compaction.zig").compaction_op_min;
const compaction_block_count_beat_min = @import("compaction.zig").compaction_block_count_beat_min;
const ResourcePool = @import("compaction.zig").ResourcePoolType(Grid);

const Grid = @import("../vsr/grid.zig").GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const TreeType = @import("tree.zig").TreeType;
const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const SortedSegmentedArrayType = @import("./segmented_array.zig").SortedSegmentedArrayType;

const Value = packed struct(u128) {
    id: u64,
    value: u63,
    tombstone_bit: u1 = 0,

    comptime {
        assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);
    }

    inline fn key_from_value(value: *const Value) u64 {
        return value.id;
    }

    const sentinel_key = std.math.maxInt(u64);

    inline fn tombstone(value: *const Value) bool {
        return value.tombstone_bit != 0;
    }

    inline fn tombstone_from_key(key: u64) Value {
        return Value{
            .id = key,
            .value = 0,
            .tombstone_bit = 1,
        };
    }
};

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    const Scan = struct {
        min: u64,
        max: u64,
        direction: Direction,
    };

    compact: struct {
        op: u64,
        beats_remaining: u64,
        checkpoint: bool,
    },
    put: Value,
    remove: Value,
    get: u64,
    scan: Scan,
};

const batch_size_max = constants.message_size_max - @sizeOf(vsr.Header);
const commit_entries_max = @divFloor(batch_size_max, @sizeOf(Value));
const value_count_max = constants.lsm_compaction_ops * commit_entries_max;
const snapshot_latest = @import("tree.zig").snapshot_latest;
const table_count_max = @import("tree.zig").table_count_max;

const node_count = 1024;
const scan_results_max = 4096;
const events_max = 10_000_000;

// We must call compact after every 'batch'.
// Every `lsm_compaction_ops` batches may put/remove `value_count_max` values.
// Every `FuzzOp.put` issues one remove and one put.
const puts_since_compact_max = @divTrunc(commit_entries_max, 2);

fn EnvironmentType(comptime table_usage: TableUsage) type {
    return struct {
        const Environment = @This();

        const Table = TableType(
            u64,
            Value,
            Value.key_from_value,
            Value.sentinel_key,
            Value.tombstone,
            Value.tombstone_from_key,
            value_count_max,
            table_usage,
        );
        const Tree = TreeType(Table, Storage);
        const ScanTree = ScanTreeType(*Environment, Tree, Storage);

        const State = enum {
            init,
            tree_init,
            manifest_log_open,
            fuzzing,
            tree_compact,
            manifest_log_compact,
            grid_checkpoint,
            superblock_checkpoint,
            grid_checkpoint_durable,
            tree_lookup,
            scan_tree,
        };

        state: State,
        storage: *Storage,
        time_sim: TimeSim,
        trace: Storage.Tracer,
        superblock: SuperBlock,
        superblock_context: SuperBlock.Context,
        grid: Grid,
        manifest_log: ManifestLog,
        node_pool: NodePool,
        tree: Tree,
        scan_tree: ScanTree,
        lookup_context: Tree.LookupContext,
        lookup_value: ?Value,
        scan_buffer: ScanBuffer,
        scan_results: []Value,
        scan_results_count: u32,
        scan_results_model: []Value,
        compaction_exhausted: bool = false,
        radix_buffer: ScratchMemory,

        pool: ResourcePool,

        pub fn run(
            gpa: std.mem.Allocator,
            storage: *Storage,
            block_count: u32,
            fuzz_ops: []const FuzzOp,
        ) !void {
            var env: Environment = undefined;
            env.state = .init;
            env.storage = storage;

            env.time_sim = fixtures.init_time(.{});
            env.trace = try fixtures.init_tracer(gpa, env.time_sim.time(), .{});
            defer env.trace.deinit(gpa);

            env.superblock = try fixtures.init_superblock(gpa, env.storage, .{});
            defer env.superblock.deinit(gpa);

            env.grid = try fixtures.init_grid(gpa, &env.trace, &env.superblock, .{
                // Grid.mark_checkpoint_not_durable releases the FreeSet checkpoints blocks into
                // FreeSet.blocks_released_prior_checkpoint_durability.
                .blocks_released_prior_checkpoint_durability_max = 0,
            });
            defer env.grid.deinit(gpa);

            try env.manifest_log.init(
                gpa,
                &env.grid,
                &ManifestLogPace.init(.{
                    .tree_count = 1,
                    .tables_max = table_count_max,
                    .compact_extra_blocks = constants.lsm_manifest_compact_extra_blocks,
                }),
            );
            defer env.manifest_log.deinit(gpa);

            try env.node_pool.init(gpa, node_count);
            defer env.node_pool.deinit(gpa);

            env.tree = undefined;
            env.lookup_value = null;

            try env.scan_buffer.init(gpa, .{ .index = 0 });
            defer env.scan_buffer.deinit(gpa);

            env.scan_results = try gpa.alloc(Value, scan_results_max);
            env.scan_results_count = 0;
            defer gpa.free(env.scan_results);

            env.scan_results_model = try gpa.alloc(Value, scan_results_max);
            defer gpa.free(env.scan_results_model);

            env.pool = try ResourcePool.init(gpa, block_count);
            defer env.pool.deinit(gpa);

            env.radix_buffer = try .init(gpa, value_count_max * @sizeOf(Value));
            defer env.radix_buffer.deinit(gpa);

            try env.open_then_apply(gpa, fuzz_ops);
        }

        fn change_state(env: *Environment, current_state: State, next_state: State) void {
            assert(env.state == current_state);
            env.state = next_state;
        }

        fn tick_until_state_change(
            env: *Environment,
            current_state: State,
            next_state: State,
        ) void {
            // Sometimes operations complete synchronously so we might already be in next_state
            // before ticking.
            assert(env.state == current_state or env.state == next_state);
            const safety_counter = 10_000_000;
            for (0..safety_counter) |_| {
                if (env.state != current_state) break;
                env.storage.run();
            } else unreachable;
            assert(env.state == next_state);
        }

        pub fn open_then_apply(
            env: *Environment,
            gpa: std.mem.Allocator,
            fuzz_ops: []const FuzzOp,
        ) !void {
            fixtures.open_superblock(&env.superblock);
            fixtures.open_grid(&env.grid);

            env.change_state(.init, .tree_init);
            // The first checkpoint is trivially durable.
            env.grid.free_set.mark_checkpoint_durable();

            try env.tree.init(gpa, &env.node_pool, &env.grid, &env.radix_buffer, .{
                .id = 1,
                .name = "Key.Value",
            }, .{
                .batch_value_count_limit = commit_entries_max,
            });
            defer env.tree.deinit(gpa);

            env.change_state(.tree_init, .manifest_log_open);

            env.tree.open_commence(&env.manifest_log);
            env.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);

            env.tick_until_state_change(.manifest_log_open, .fuzzing);
            env.tree.open_complete();

            try env.apply(gpa, fuzz_ops);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            table: *const schema.ManifestNode.TableInfo,
        ) void {
            _ = manifest_log;
            _ = table;

            // This ManifestLog is only opened during setup, when it has no blocks.
            unreachable;
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const env: *Environment = @fieldParentPtr("manifest_log", manifest_log);
            env.change_state(.manifest_log_open, .fuzzing);
        }

        pub fn compact(env: *Environment, op: u64, beats_remaining: u64) void {
            const half_bar = @divExact(constants.lsm_compaction_ops, 2);
            if (op < half_bar) return;
            const compaction_beat = op % constants.lsm_compaction_ops;

            const first_beat = compaction_beat == 0;
            const half_beat = compaction_beat == half_bar;

            const last_half_beat = compaction_beat == half_bar - 1;
            const last_beat = compaction_beat == constants.lsm_compaction_ops - 1;

            if (last_beat or last_half_beat) assert(beats_remaining == 1);

            assert(env.pool.idle());
            if (first_beat or half_beat) {
                assert(env.pool.blocks_acquired() == 0);
            }

            var compactions_active: [constants.lsm_levels]*Tree.Compaction = undefined;
            var compactions_active_count: usize = 0;
            for (&env.tree.compactions) |*compaction| {
                if ((compaction_beat < half_bar) == (compaction.level_b % 2 == 1)) {
                    compactions_active[compactions_active_count] = compaction;
                    compactions_active_count += 1;
                }
            }
            assert(compactions_active_count <= stdx.div_ceil(constants.lsm_levels, 2));
            const compactions_slice = compactions_active[0..compactions_active_count];

            // 1 since we may have partially finished index/value blocks from the previous beat.
            var beat_value_blocks_max: u64 = 1;
            var beat_index_blocks_max: u64 = 1;

            for (compactions_slice) |compaction| {
                if (first_beat or half_beat) _ = compaction.bar_commence(op);

                const input_values_remaining_bar =
                    compaction.quotas.bar - compaction.quotas.bar_done;
                const input_values_remaining_beat =
                    stdx.div_ceil(input_values_remaining_bar, beats_remaining);

                compaction.beat_commence(input_values_remaining_beat);

                // The +1 is for imperfections in pacing our immutable table, which
                // might cause us to overshoot by a single block (limited to 1 due
                // to how the immutable table values are consumed.)
                beat_value_blocks_max += stdx.div_ceil(
                    compaction.quotas.beat,
                    Table.layout.block_value_count_max,
                ) + 1;

                beat_index_blocks_max += stdx.div_ceil(
                    beat_value_blocks_max,
                    Table.value_block_count_max,
                );
            }

            env.pool.grid_reservation = env.grid.reserve(
                beat_value_blocks_max + beat_index_blocks_max,
            );

            for (compactions_slice) |compaction| {
                assert(env.pool.idle());
                env.change_state(.fuzzing, .tree_compact);

                switch (compaction.compaction_dispatch_enter(.{
                    .pool = &env.pool,
                    .callback = compact_callback,
                })) {
                    .pending => env.tick_until_state_change(.tree_compact, .fuzzing),
                    .ready => env.change_state(.tree_compact, .fuzzing),
                }
                assert(env.pool.idle());
            }

            env.grid.forfeit(env.pool.grid_reservation.?);

            if (last_beat or last_half_beat) {
                assert(env.pool.blocks_acquired() == 0);

                if (op >= constants.lsm_compaction_ops) {
                    env.change_state(.fuzzing, .manifest_log_compact);
                    env.manifest_log.compact(manifest_log_compact_callback, op);
                    env.tick_until_state_change(.manifest_log_compact, .fuzzing);
                }

                for (compactions_slice) |compaction| {
                    compaction.bar_complete();
                }

                if (op >= constants.lsm_compaction_ops) {
                    env.manifest_log.compact_end();
                }
            }

            if (last_beat) {
                env.tree.compact();
                env.tree.swap_mutable_and_immutable(
                    snapshot_min_for_table_output(compaction_op_min(op)),
                );

                // Ensure tables haven't overflowed.
                env.tree.manifest.assert_level_table_counts();
            }
        }

        fn compact_callback(pool: *ResourcePool, _: u16, _: u64) void {
            const env: *Environment = @fieldParentPtr("pool", pool);
            env.change_state(.tree_compact, .fuzzing);
        }

        fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
            const env: *Environment = @fieldParentPtr("manifest_log", manifest_log);
            env.change_state(.manifest_log_compact, .fuzzing);
        }

        pub fn checkpoint(env: *Environment, op: u64) void {
            env.tree.assert_between_bars();

            env.change_state(.fuzzing, .grid_checkpoint);
            env.grid.checkpoint(grid_checkpoint_callback);
            env.tick_until_state_change(.grid_checkpoint, .superblock_checkpoint);

            const checkpoint_op = op - constants.lsm_compaction_ops;
            env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
                .header = header: {
                    var header = vsr.Header.Prepare.root(fixtures.cluster);
                    header.op = checkpoint_op;
                    header.set_checksum();
                    break :header header;
                },
                .view_attributes = null,
                .manifest_references = std.mem.zeroes(vsr.SuperBlockManifestReferences),
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
                .commit_max = checkpoint_op + 1,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .storage_size = vsr.superblock.data_file_size_min +
                    (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
                .release = vsr.Release.minimum,
            });

            env.tick_until_state_change(.superblock_checkpoint, .fuzzing);

            // The fuzzer runs in a single process, all checkpoints are trivially durable. Use
            // free_set.mark_checkpoint_durable() instead of grid.mark_checkpoint_durable(); the
            // latter requires passing a callback, which is called synchronously in fuzzers anyway.
            env.grid.mark_checkpoint_not_durable();
            env.grid.free_set.mark_checkpoint_durable();
        }

        fn grid_checkpoint_callback(grid: *Grid) void {
            const env: *Environment = @fieldParentPtr("grid", grid);
            env.change_state(.grid_checkpoint, .superblock_checkpoint);
        }

        fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
            const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
            env.change_state(.superblock_checkpoint, .fuzzing);
        }

        pub fn get(env: *Environment, key: u64) ?Value {
            env.change_state(.fuzzing, .tree_lookup);
            env.lookup_value = null;

            if (env.tree.lookup_from_memory(key)) |value| {
                get_callback(&env.lookup_context, Tree.unwrap_tombstone(value));
            } else {
                env.tree.lookup_from_levels_storage(.{
                    .callback = get_callback,
                    .context = &env.lookup_context,
                    .snapshot = snapshot_latest,
                    .key = key,
                    .level_min = 0,
                });
            }

            env.tick_until_state_change(.tree_lookup, .fuzzing);
            return env.lookup_value;
        }

        fn get_callback(lookup_context: *Tree.LookupContext, value: ?*const Value) void {
            const env: *Environment = @fieldParentPtr("lookup_context", lookup_context);
            assert(env.lookup_value == null);
            env.lookup_value = if (value) |val| val.* else null;
            env.change_state(.tree_lookup, .fuzzing);
        }

        pub fn scan(env: *Environment, key_min: u64, key_max: u64, direction: Direction) []Value {
            assert(key_min <= key_max);

            env.change_state(.fuzzing, .scan_tree);
            env.scan_tree = ScanTree.init(
                &env.tree,
                &env.scan_buffer,
                snapshot_latest,
                key_min,
                key_max,
                direction,
            );

            env.scan_results_count = 0;
            env.scan_tree.read(env, on_scan_read);
            env.tick_until_state_change(.scan_tree, .fuzzing);

            return env.scan_results[0..env.scan_results_count];
        }

        fn on_scan_read(env: *Environment, scan_tree: *ScanTree) void {
            while (scan_tree.next() catch |err| switch (err) {
                error.Pending => return scan_tree.read(env, on_scan_read),
            }) |value| {
                if (env.scan_results_count == scan_results_max) break;

                env.scan_results[env.scan_results_count] = value;
                env.scan_results_count += 1;
            }

            env.change_state(.scan_tree, .fuzzing);
        }

        pub fn apply(env: *Environment, gpa: std.mem.Allocator, fuzz_ops: []const FuzzOp) !void {
            var model: Model = undefined;
            try model.init(gpa, table_usage);
            defer model.deinit(gpa);

            var value_count: u64 = 0;
            for (fuzz_ops, 0..) |fuzz_op, fuzz_op_index| {
                assert(env.state == .fuzzing);
                log.debug("Running fuzz_ops[{}/{}] == {}", .{
                    fuzz_op_index, fuzz_ops.len, fuzz_op,
                });

                const storage_size_used = env.storage.size_used();
                log.debug("storage.size_used = {}/{}", .{ storage_size_used, env.storage.size });

                const model_size = model.count() * @sizeOf(Value);
                log.debug("space_amplification = {d:.2}", .{
                    @as(f64, @floatFromInt(storage_size_used)) /
                        @as(f64, @floatFromInt(model_size)),
                });

                // If the fuzzer manages to fill up the whole tree (unlikely, but possible), it
                // continues to only issue read operations.
                const tree_full = tree_full: {
                    const last_level = &env.tree.manifest.levels[constants.lsm_levels - 1];
                    const last_level_tables_max =
                        std.math.pow(u32, constants.lsm_growth_factor, constants.lsm_levels);
                    if (last_level.table_count_visible == last_level_tables_max) {
                        // Sanity-check that this doesn't happen after too few ops.
                        assert(value_count >= last_level_tables_max);
                        break :tree_full true;
                    } else {
                        break :tree_full false;
                    }
                };

                // Apply fuzz_op to the tree and the model.
                switch (fuzz_op) {
                    .compact => |c| {
                        if (tree_full) continue;
                        env.compact(c.op, c.beats_remaining);
                        if (c.checkpoint) env.checkpoint(c.op);
                    },
                    .put => |value| {
                        if (tree_full) continue;
                        value_count += 2;
                        if (table_usage == .secondary_index) {
                            // Secondary index requires that the key implies the value (typically
                            // key ≡ value), and that there are no updates.
                            const canonical_value: Value = .{
                                .id = value.id,
                                .value = 0,
                                .tombstone_bit = value.tombstone_bit,
                            };
                            if (model.contains(&canonical_value)) {
                                env.tree.remove(&canonical_value);
                            }
                            env.tree.put(&canonical_value);
                            try model.put(&canonical_value);
                        } else {
                            env.tree.put(&value);
                            try model.put(&value);
                        }
                    },
                    .remove => |value| {
                        if (tree_full) continue;
                        value_count += 1;
                        if (table_usage == .secondary_index and !model.contains(&value)) {
                            // Not allowed to remove non-present keys
                        } else {
                            env.tree.remove(&value);
                            model.remove(&value);
                        }
                    },
                    .get => |key| {
                        // Get account from lsm.
                        const tree_value = env.get(key);

                        // Compare result to model.
                        const model_value = model.get(key);
                        if (model_value == null) {
                            assert(tree_value == null);
                        } else {
                            assert(stdx.equal_bytes(Value, &model_value.?, &tree_value.?));
                        }
                    },
                    .scan => |scan_range| try env.apply_scan(&model, scan_range),
                }
            }
        }

        fn apply_scan(env: *Environment, model: *const Model, scan_range: FuzzOp.Scan) !void {
            assert(scan_range.min <= scan_range.max);

            const tree_values = env.scan(
                scan_range.min,
                scan_range.max,
                scan_range.direction,
            );

            const model_values = model.scan(
                scan_range.min,
                scan_range.max,
                scan_range.direction,
                env.scan_results_model,
            );

            // Unlike the model, the tree can return some amount of tombstones in the result set.
            // They must be filtered out before comparison!
            var tombstone_count: usize = 0;
            for (tree_values, 0..) |tree_value, index| {
                assert(scan_range.min <= Table.key_from_value(&tree_value));
                assert(Table.key_from_value(&tree_value) <= scan_range.max);
                if (Table.tombstone(&tree_value)) {
                    tombstone_count += 1;
                } else {
                    if (tombstone_count > 0) {
                        tree_values[index - tombstone_count] = tree_value;
                    }
                }
            }

            const tombstone_evicted = (model_values.len + tombstone_count) -| scan_results_max;
            try testing.expectEqualSlices(
                Value,
                tree_values[0 .. tree_values.len - tombstone_count],
                model_values[0 .. model_values.len - tombstone_evicted],
            );
            assert(tree_values.len >= model_values.len);
        }
    };
}

// A tree is a sorted set. The ideal model would have been an in-memory B-tree, but there isn't
// one in Zig's standard library. Use a SortedSegmentedArray instead which is essentially a stunted
// B-tree one-level deep.
const Model = struct {
    const Array = SortedSegmentedArrayType(
        Value,
        NodePool,
        events_max,
        u64,
        Value.key_from_value,
        .{ .verify = false },
    );

    table_usage: TableUsage,
    node_pool: NodePool,
    array: Array,

    fn init(model: *Model, gpa: std.mem.Allocator, table_usage: TableUsage) !void {
        model.* = .{
            .table_usage = table_usage,

            .node_pool = undefined,
            .array = undefined,
        };

        const model_node_count = stdx.div_ceil(
            events_max * @sizeOf(Value),
            NodePool.node_size,
        );

        try model.node_pool.init(gpa, model_node_count);
        errdefer model.node_pool.deinit(gpa);

        model.array = try Array.init(gpa);
        errdefer model.array.deinit(gpa, &model.node_pool);
    }

    fn deinit(model: *Model, gpa: std.mem.Allocator) void {
        model.array.deinit(gpa, &model.node_pool);
        model.node_pool.deinit(gpa);
        model.* = undefined;
    }

    fn count(model: *const Model) u32 {
        return model.array.len();
    }

    fn contains(model: *Model, value: *const Value) bool {
        return model.get(Value.key_from_value(value)) != null;
    }

    fn get(model: *const Model, key: u64) ?Value {
        const cursor = model.array.search(key);
        if (cursor.node == model.array.node_count) return null;
        if (cursor.relative_index == model.array.node_elements(cursor.node).len) return null;
        const cursor_element = model.array.element_at_cursor(cursor);
        if (Value.key_from_value(&cursor_element) == key) {
            return cursor_element;
        } else {
            return null;
        }
    }

    fn scan(
        model: *const Model,
        key_min: u64,
        key_max: u64,
        direction: Direction,
        result: []Value,
    ) []Value {
        var result_count: usize = 0;
        switch (direction) {
            .ascending => {
                const cursor = model.array.search(key_min);
                var it = model.array.iterator_from_cursor(cursor, .ascending);
                while (it.next()) |element| {
                    const element_key = Value.key_from_value(element);
                    if (element_key <= key_max) {
                        assert(element_key >= key_min);
                        result[result_count] = element.*;
                        result_count += 1;
                        if (result_count == result.len) break;
                    } else {
                        break;
                    }
                }
            },
            .descending => {
                const cursor = model.array.search(key_max);
                var it = model.array.iterator_from_cursor(cursor, .descending);
                while (it.next()) |element| {
                    const element_key = Value.key_from_value(element);
                    if (element_key >= key_min) {
                        if (element_key <= key_max) {
                            result[result_count] = element.*;
                            result_count += 1;
                            if (result_count == result.len) break;
                        }
                    } else {
                        break;
                    }
                }
            },
        }
        return result[0..result_count];
    }

    fn put(model: *Model, value: *const Value) !void {
        model.remove(value);
        _ = model.array.insert_element(&model.node_pool, value.*);
    }

    fn remove(model: *Model, value: *const Value) void {
        const key = Value.key_from_value(value);
        const cursor = model.array.search(key);
        if (cursor.node == model.array.node_count) return;
        if (cursor.relative_index == model.array.node_elements(cursor.node).len) return;

        if (Value.key_from_value(&model.array.element_at_cursor(cursor)) == key) {
            model.array.remove_elements(
                &model.node_pool,
                model.array.absolute_index_for_cursor(cursor),
                1,
            );
        }
    }
};

fn random_id(prng: *stdx.PRNG) u64 {
    return fuzz.random_id(prng, u64, .{
        .average_hot = 8,
        .average_cold = value_count_max * table_count_max,
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
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (prng.boolean()) 0 else 1,
        // Always do puts, and always more puts than removes.
        .put = constants.lsm_compaction_ops * 2,
        // Maybe do some removes.
        .remove = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
        // Maybe do some gets.
        .get = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
        // Maybe do some scans.
        .scan = if (prng.boolean()) 0 else constants.lsm_compaction_ops,
    };
    log.info("fuzz_op_weights = {:.2}", .{fuzz_op_weights});

    log.info("puts_since_compact_max = {}", .{puts_since_compact_max});

    var op: u64 = 1;
    var persisted_op: u64 = 0;
    var puts_since_compact: usize = 0;
    for (fuzz_ops) |*fuzz_op| {
        const fuzz_op_tag = if (puts_since_compact >= puts_since_compact_max)
            // We have to compact before doing any other operations.
            FuzzOpTag.compact
        else
            // Otherwise pick a random FuzzOp.
            prng.enum_weighted(FuzzOpTag, fuzz_op_weights);
        fuzz_op.* = switch (fuzz_op_tag) {
            .compact => action: {
                const action = generate_compact(prng, .{
                    .op = op,
                    .persisted_op = persisted_op,
                });
                if (action.compact.checkpoint) {
                    persisted_op = op - constants.lsm_compaction_ops;
                }
                op += 1;
                break :action action;
            },
            .put => FuzzOp{ .put = .{
                .id = random_id(prng),
                .value = prng.int(u63),
            } },
            .remove => FuzzOp{ .remove = .{
                .id = random_id(prng),
                .value = prng.int(u63),
            } },
            .get => FuzzOp{ .get = random_id(prng) },
            .scan => blk: {
                const min = random_id(prng);
                const max = min + random_id(prng);
                const direction = prng.enum_uniform(Direction);
                assert(min <= max);
                break :blk FuzzOp{
                    .scan = .{
                        .min = min,
                        .max = max,
                        .direction = direction,
                    },
                };
            },
        };
        switch (fuzz_op.*) {
            .compact => puts_since_compact = 0,
            // Tree.remove() works by inserting a tombstone, so it counts as a put.
            .put, .remove => puts_since_compact += 1,
            .get, .scan => {},
        }
    }

    return fuzz_ops;
}

fn generate_compact(
    prng: *stdx.PRNG,
    options: struct { op: u64, persisted_op: u64 },
) FuzzOp {
    const half_bar = @divExact(constants.lsm_compaction_ops, 2);
    const checkpoint =
        // Can only checkpoint on the last beat of the bar.
        options.op % constants.lsm_compaction_ops == constants.lsm_compaction_ops - 1 and
        options.op > constants.lsm_compaction_ops and
        // Checkpoint at the normal rate.
        // TODO Make LSM (and this fuzzer) unaware of VSR's checkpoint schedule.
        options.op == vsr.Checkpoint.trigger_for_checkpoint(
            vsr.Checkpoint.checkpoint_after(options.persisted_op),
        );
    return FuzzOp{ .compact = .{
        .op = options.op,
        .checkpoint = checkpoint,
        .beats_remaining = prng.range_inclusive(u64, 1, half_bar - options.op % half_bar),
    } };
}

pub fn main(gpa: std.mem.Allocator, fuzz_args: fuzz.FuzzArgs) !void {
    var prng = stdx.PRNG.from_seed(fuzz_args.seed);

    const table_usage = prng.enum_uniform(TableUsage);
    log.info("table_usage={}", .{table_usage});

    const block_count_min =
        stdx.div_ceil(constants.lsm_levels, 2) * compaction_block_count_beat_min;

    const block_count = if (prng.chance(ratio(1, 5)))
        block_count_min
    else
        prng.range_inclusive(u32, block_count_min, block_count_min * 16);

    const fuzz_op_count = @min(
        fuzz_args.events_max orelse events_max,
        fuzz.random_int_exponential(&prng, usize, 1E6),
    );

    const fuzz_ops = try generate_fuzz_ops(gpa, &prng, fuzz_op_count);
    defer gpa.free(fuzz_ops);

    var storage = try fixtures.init_storage(gpa, .{
        .size = constants.storage_size_limit_default,
        .seed = prng.int(u64),
        .read_latency_min = .{ .ns = 0 },
        .read_latency_mean = fuzz.range_inclusive_ms(&prng, 0, 200),
        .write_latency_min = .{ .ns = 0 },
        .write_latency_mean = fuzz.range_inclusive_ms(&prng, 0, 200),
    });
    defer storage.deinit(gpa);

    try fixtures.storage_format(gpa, &storage, .{});

    switch (table_usage) {
        inline else => |usage| {
            try EnvironmentType(usage).run(gpa, &storage, block_count, fuzz_ops);
        },
    }

    log.info("Passed!", .{});
}
