const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const vsr = @import("../vsr.zig");
const schema = @import("schema.zig");
const binary_search = @import("binary_search.zig");
const allocator = fuzz.allocator;

const log = std.log.scoped(.lsm_tree_fuzz);
const tracer = @import("../tracer.zig");

const Direction = @import("../direction.zig").Direction;
const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../testing/storage.zig").Storage;
const ClusterFaultAtlas = @import("../testing/storage.zig").ClusterFaultAtlas;
const StateMachine =
    @import("../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const GridType = @import("../vsr/grid.zig").GridType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const TableUsage = @import("table.zig").TableUsage;
const TableType = @import("table.zig").TableType;
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);
const snapshot_min_for_table_output = @import("compaction.zig").snapshot_min_for_table_output;
const compaction_op_min = @import("compaction.zig").compaction_op_min;
const Exhausted = @import("compaction.zig").Exhausted;

const Grid = @import("../vsr/grid.zig").GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const FreeSetEncoded = vsr.FreeSetEncodedType(Storage);
const SortedSegmentedArray = @import("./segmented_array.zig").SortedSegmentedArray;

const CompactionHelperType = @import("compaction.zig").CompactionHelperType;
const CompactionHelper = CompactionHelperType(Grid);

const Value = packed struct(u128) {
    id: u64,
    value: u63,
    tombstone: u1 = 0,

    comptime {
        assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);
    }

    inline fn key_from_value(value: *const Value) u64 {
        return value.id;
    }

    const sentinel_key = std.math.maxInt(u64);

    inline fn tombstone(value: *const Value) bool {
        return value.tombstone != 0;
    }

    inline fn tombstone_from_key(key: u64) Value {
        return Value{
            .id = key,
            .value = 0,
            .tombstone = 1,
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
        checkpoint: bool,
    },
    put: Value,
    remove: Value,
    get: u64,
    scan: Scan,
};

const batch_size_max = constants.message_size_max - @sizeOf(vsr.Header);
const commit_entries_max = @divFloor(batch_size_max, @sizeOf(Value));
const value_count_max = constants.lsm_batch_multiple * commit_entries_max;
const snapshot_latest = @import("tree.zig").snapshot_latest;
const table_count_max = @import("tree.zig").table_count_max;

const cluster = 32;
const replica = 4;
const replica_count = 6;
const node_count = 1024;
const scan_results_max = 4096;
const events_max = 10_000_000;

// We must call compact after every 'batch'.
// Every `lsm_batch_multiple` batches may put/remove `value_count_max` values.
// Every `FuzzOp.put` issues one remove and one put.
const puts_since_compact_max = @divTrunc(commit_entries_max, 2);
const compacts_per_checkpoint = stdx.div_ceil(
    constants.journal_slot_count,
    constants.lsm_batch_multiple,
);

fn EnvironmentType(comptime table_usage: TableUsage) type {
    return struct {
        const Environment = @This();

        const Tree = @import("tree.zig").TreeType(Table, Storage);
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
        const ScanTree = ScanTreeType(*Environment, Tree, Storage);
        const CompactionWork = stdx.BoundedArray(*Tree.Compaction, constants.lsm_levels);

        const State = enum {
            init,
            superblock_format,
            superblock_open,
            free_set_open,
            tree_init,
            manifest_log_open,
            fuzzing,
            blipping,
            tree_compact,
            manifest_log_compact,
            grid_checkpoint,
            superblock_checkpoint,
            tree_lookup,
            scan_tree,
        };

        state: State,
        storage: *Storage,
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

        block_pool: CompactionHelper.CompactionBlockFIFO,
        block_pool_raw: []CompactionHelper.CompactionBlock,

        pub fn run(storage: *Storage, fuzz_ops: []const FuzzOp) !void {
            var env: Environment = undefined;
            env.state = .init;
            env.storage = storage;

            env.superblock = try SuperBlock.init(allocator, .{
                .storage = env.storage,
                .storage_size_limit = constants.storage_size_limit_max,
            });
            defer env.superblock.deinit(allocator);

            env.grid = try Grid.init(allocator, .{
                .superblock = &env.superblock,
                .missing_blocks_max = 0,
                .missing_tables_max = 0,
            });
            defer env.grid.deinit(allocator);

            env.manifest_log = try ManifestLog.init(allocator, &env.grid, .{
                .tree_id_min = 1,
                .tree_id_max = 1,
                .forest_table_count_max = table_count_max,
            });
            defer env.manifest_log.deinit(allocator);

            env.node_pool = try NodePool.init(allocator, node_count);
            defer env.node_pool.deinit(allocator);

            env.tree = undefined;
            env.lookup_value = null;

            env.scan_buffer = try ScanBuffer.init(allocator);
            defer env.scan_buffer.deinit(allocator);

            env.scan_results = try allocator.alloc(Value, scan_results_max);
            env.scan_results_count = 0;
            defer allocator.free(env.scan_results);

            env.scan_results_model = try allocator.alloc(Value, scan_results_max);
            defer allocator.free(env.scan_results_model);

            // TODO: Pull out these constants. 3 is block_count_bar_single, 8 is
            // minimum_block_count_beat.
            const block_count = 3 * stdx.div_ceil(constants.lsm_levels, 2) + 8;

            env.block_pool_raw = try allocator.alloc(CompactionHelper.CompactionBlock, block_count);
            defer allocator.free(env.block_pool_raw);

            env.block_pool = .{
                .name = "block_pool",
                .verify_push = false,
            };

            for (env.block_pool_raw) |*compaction_block| {
                compaction_block.* = .{
                    .block = try allocate_block(allocator),
                };
                env.block_pool.push(compaction_block);
            }
            defer for (env.block_pool_raw) |block| allocator.free(block.block);

            try env.open_then_apply(fuzz_ops);
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
            while (env.state == current_state) env.storage.tick();
            assert(env.state == next_state);
        }

        pub fn open_then_apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            env.change_state(.init, .superblock_format);
            env.superblock.format(superblock_format_callback, &env.superblock_context, .{
                .cluster = cluster,
                .release = vsr.Release.minimum,
                .replica = replica,
                .replica_count = replica_count,
            });

            env.tick_until_state_change(.superblock_format, .superblock_open);
            env.superblock.open(superblock_open_callback, &env.superblock_context);

            env.tick_until_state_change(.superblock_open, .free_set_open);
            env.grid.open(grid_open_callback);

            env.tick_until_state_change(.free_set_open, .tree_init);
            env.tree = try Tree.init(allocator, &env.node_pool, &env.grid, .{
                .id = 1,
                .name = "Key.Value",
            }, .{
                .batch_value_count_limit = commit_entries_max,
            });
            defer env.tree.deinit(allocator);

            env.change_state(.tree_init, .manifest_log_open);

            env.tree.open_commence(&env.manifest_log);
            env.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);

            env.tick_until_state_change(.manifest_log_open, .fuzzing);
            env.tree.open_complete();

            try env.apply(fuzz_ops);
        }

        fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
            const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
            env.change_state(.superblock_format, .superblock_open);
        }

        fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
            const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
            env.change_state(.superblock_open, .free_set_open);
        }

        fn grid_open_callback(grid: *Grid) void {
            const env: *Environment = @fieldParentPtr("grid", grid);
            env.change_state(.free_set_open, .tree_init);
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

        pub fn compact(env: *Environment, op: u64) void {
            const compaction_beat = op % constants.lsm_batch_multiple;

            const last_half_beat =
                compaction_beat == @divExact(constants.lsm_batch_multiple, 2) - 1;
            const last_beat = compaction_beat == constants.lsm_batch_multiple - 1;

            if (!last_beat and !last_half_beat) return;

            var compaction_work = CompactionWork{};
            for (&env.tree.compactions) |*compaction| {
                if (last_half_beat and compaction.level_b % 2 != 0) continue;
                if (last_beat and compaction.level_b % 2 == 0) continue;

                const maybe_compaction_work = compaction.bar_setup(&env.tree, op);
                if (maybe_compaction_work != null) {
                    compaction_work.append_assume_capacity(compaction);
                }
            }

            assert(env.block_pool.count == env.block_pool_raw.len);

            for (compaction_work.const_slice()) |compaction| {
                if (compaction.bar != null and compaction.bar.?.move_table) {
                    continue;
                }
                const source_a_immutable_block = env.block_pool.pop().?;
                const target_index_blocks = CompactionHelper.BlockFIFO.init(&env.block_pool, 2);

                const beat_blocks = .{
                    .source_index_block_a = env.block_pool.pop().?,
                    .source_index_block_b = env.block_pool.pop().?,
                    .source_value_blocks = .{
                        CompactionHelper.BlockFIFO.init(&env.block_pool, 2),
                        CompactionHelper.BlockFIFO.init(&env.block_pool, 2),
                    },
                    .target_value_blocks = CompactionHelper.BlockFIFO.init(&env.block_pool, 2),
                };

                compaction.bar_setup_budget(1, target_index_blocks, source_a_immutable_block);
                compaction.beat_grid_reserve();
                compaction.beat_blocks_assign(beat_blocks);

                env.compaction_exhausted = false;
                while (!env.compaction_exhausted) {
                    env.change_state(.fuzzing, .blipping);
                    compaction.blip_read(blip_callback, env);
                    env.tick_until_state_change(.blipping, .fuzzing);

                    env.change_state(.fuzzing, .blipping);
                    compaction.blip_merge(blip_callback, env);
                    env.tick_until_state_change(.blipping, .fuzzing);

                    env.change_state(.fuzzing, .blipping);
                    compaction.blip_write(blip_callback, env);
                    env.tick_until_state_change(.blipping, .fuzzing);
                }

                compaction.beat_blocks_unassign(&env.block_pool);
                compaction.beat_grid_forfeit();
            }

            if (op >= constants.lsm_batch_multiple) {
                env.change_state(.fuzzing, .manifest_log_compact);
                env.manifest_log.compact(manifest_log_compact_callback, op);
                env.tick_until_state_change(.manifest_log_compact, .fuzzing);
            }

            for (compaction_work.const_slice()) |compaction| {
                compaction.bar_blocks_unassign(&env.block_pool);
                compaction.bar_apply_to_manifest();
            }

            assert(env.block_pool.count == env.block_pool_raw.len);

            if (op >= constants.lsm_batch_multiple) {
                env.manifest_log.compact_end();
            }

            if (last_beat) {
                env.tree.swap_mutable_and_immutable(
                    snapshot_min_for_table_output(compaction_op_min(op)),
                );

                // Ensure tables haven't overflowed.
                env.tree.manifest.assert_level_table_counts();
            }
        }
        fn blip_callback(env_opaque: *anyopaque, maybe_exhausted: ?Exhausted) void {
            const env: *Environment = @ptrCast(
                @alignCast(env_opaque),
            );
            if (maybe_exhausted) |exhausted| {
                env.compaction_exhausted = exhausted.bar;
            }
            env.change_state(.blipping, .fuzzing);
        }

        fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
            const env: *Environment = @fieldParentPtr("manifest_log", manifest_log);
            env.change_state(.manifest_log_compact, .fuzzing);
        }

        fn tree_compact_callback(tree: *Tree) void {
            const env: *Environment = @fieldParentPtr("tree", tree);
            env.change_state(.tree_compact, .fuzzing);
        }

        pub fn checkpoint(env: *Environment, op: u64) void {
            env.tree.assert_between_bars();

            env.grid.checkpoint(grid_checkpoint_callback);
            env.change_state(.fuzzing, .grid_checkpoint);
            env.tick_until_state_change(.grid_checkpoint, .fuzzing);

            const checkpoint_op = op - constants.lsm_batch_multiple;
            env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
                .header = header: {
                    var header = vsr.Header.Prepare.root(cluster);
                    header.op = checkpoint_op;
                    header.set_checksum();
                    break :header header;
                },
                .manifest_references = std.mem.zeroes(vsr.SuperBlockManifestReferences),
                .free_set_reference = env.grid.free_set_checkpoint.checkpoint_reference(),
                .client_sessions_reference = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
                .commit_max = checkpoint_op + 1,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .sync_view = 0,
                .storage_size = vsr.superblock.data_file_size_min +
                    (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
                .release = vsr.Release.minimum,
            });

            env.change_state(.fuzzing, .superblock_checkpoint);
            env.tick_until_state_change(.superblock_checkpoint, .fuzzing);
        }

        fn grid_checkpoint_callback(grid: *Grid) void {
            const env: *Environment = @fieldParentPtr("grid", grid);
            env.change_state(.grid_checkpoint, .fuzzing);
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
                error.ReadAgain => return scan_tree.read(env, on_scan_read),
            }) |value| {
                if (env.scan_results_count == scan_results_max) break;

                env.scan_results[env.scan_results_count] = value;
                env.scan_results_count += 1;
            }

            env.change_state(.scan_tree, .fuzzing);
        }

        pub fn apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            var model = try Model.init(table_usage);
            defer model.deinit();

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

                // Apply fuzz_op to the tree and the model.
                switch (fuzz_op) {
                    .compact => |c| {
                        env.compact(c.op);
                        if (c.checkpoint) env.checkpoint(c.op);
                    },
                    .put => |value| {
                        if (table_usage == .secondary_index) {
                            // Secondary index requires that the key implies the value (typically
                            // key ≡ value), and that there are no updates.
                            const canonical_value: Value = .{
                                .id = value.id,
                                .value = 0,
                                .tombstone = value.tombstone,
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
    const Array = SortedSegmentedArray(
        Value,
        NodePool,
        events_max,
        u64,
        Value.key_from_value,
        .{ .verify = false },
    );

    array: Array,
    node_pool: NodePool,
    table_usage: TableUsage,

    fn init(table_usage: TableUsage) !Model {
        const model_node_count = stdx.div_ceil(
            events_max * @sizeOf(Value),
            NodePool.node_size,
        );
        return .{
            .array = try Array.init(allocator),
            .node_pool = try NodePool.init(allocator, model_node_count),
            .table_usage = table_usage,
        };
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

    fn deinit(model: *Model) void {
        model.array.deinit(allocator, &model.node_pool);
        model.node_pool.deinit(allocator);
        model.* = undefined;
    }
};

fn random_id(random: std.rand.Random, comptime Int: type) Int {
    // We have two opposing desires for random ids:
    const avg_int: Int = if (random.boolean())
        // 1. We want to cause many collisions.
        constants.lsm_growth_factor * 2048
    else
        // 2. We want to generate enough ids that the cache can't hold them all.
        100 * constants.lsm_growth_factor * 2048;
    return fuzz.random_int_exponential(random, Int, avg_int);
}

pub fn generate_fuzz_ops(random: std.rand.Random, fuzz_op_count: usize) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    const fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (random.boolean()) 0 else 1,
        // Always do puts, and always more puts than removes.
        .put = constants.lsm_batch_multiple * 2,
        // Maybe do some removes.
        .remove = if (random.boolean()) 0 else constants.lsm_batch_multiple,
        // Maybe do some gets.
        .get = if (random.boolean()) 0 else constants.lsm_batch_multiple,
        // Maybe do some scans.
        .scan = if (random.boolean()) 0 else constants.lsm_batch_multiple,
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
            .compact => action: {
                const action = generate_compact(random, .{
                    .op = op,
                });
                op += 1;
                break :action action;
            },
            .put => FuzzOp{ .put = .{
                .id = random_id(random, u64),
                .value = random.int(u63),
            } },
            .remove => FuzzOp{ .remove = .{
                .id = random_id(random, u64),
                .value = random.int(u63),
            } },
            .get => FuzzOp{ .get = random_id(random, u64) },
            .scan => blk: {
                const min = random_id(random, u64);
                const max = min + random_id(random, u64);
                const direction = random.enumValue(Direction);
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
    random: std.rand.Random,
    options: struct { op: u64 },
) FuzzOp {
    const checkpoint =
        // Can only checkpoint on the last beat of the bar.
        options.op % constants.lsm_batch_multiple == constants.lsm_batch_multiple - 1 and
        options.op > constants.lsm_batch_multiple and
        // Checkpoint at roughly the same rate as log wraparound.
        random.uintLessThan(usize, compacts_per_checkpoint) == 0;
    return FuzzOp{ .compact = .{
        .op = options.op,
        .checkpoint = checkpoint,
    } };
}

pub fn main(fuzz_args: fuzz.FuzzArgs) !void {
    try tracer.init(allocator);
    defer tracer.deinit(allocator);

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
        .read_fault_probability = 0,
        .write_fault_probability = 0,
        .fault_atlas = &storage_fault_atlas,
    };

    const fuzz_op_count = @min(
        fuzz_args.events_max orelse events_max,
        fuzz.random_int_exponential(random, usize, 1E6),
    );

    const fuzz_ops = try generate_fuzz_ops(random, fuzz_op_count);
    defer allocator.free(fuzz_ops);

    // Init mocked storage.
    var storage = try Storage.init(allocator, constants.storage_size_limit_max, storage_options);
    defer storage.deinit(allocator);

    switch (table_usage) {
        inline else => |usage| {
            try EnvironmentType(usage).run(&storage, fuzz_ops);
        },
    }

    log.info("Passed!", .{});
}
