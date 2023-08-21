const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const log = std.log.scoped(.forest);

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const schema = @import("schema.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;

pub fn ForestType(comptime Storage: type, comptime groove_cfg: anytype) type {
    var groove_fields: []const std.builtin.Type.StructField = &.{};
    var groove_options_fields: []const std.builtin.Type.StructField = &.{};

    for (std.meta.fields(@TypeOf(groove_cfg))) |field| {
        const Groove = @field(groove_cfg, field.name);
        groove_fields = groove_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = Groove,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };

        groove_options_fields = groove_options_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = Groove.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };
    }

    const _Grooves = @Type(.{
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

    {
        // Verify that every tree id is unique.
        comptime var ids: []const u16 = &.{};

        inline for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;

            for (std.meta.fields(@TypeOf(Groove.config.ids))) |field| {
                const id = @field(Groove.config.ids, field.name);

                assert(std.mem.indexOfScalar(u16, ids, id) == null);
                ids = ids ++ [_]u16{id};
            }
        }
    }

    // TODO ForestTableIterator
    //const tree_map = map: {
    //    var map: []const ?struct { groove: u32, tree: u32 } = &.{};
    //    inline for (std.meta.fields(_Grooves)) |groove_field| {
    //        const groove = &@field(grooves, groove_field.name);
    //        const Groove = @TypeOf(groove.*);
    //    }
    //    break :map map;
    //};

    // TODO Share with ForestTableIterator?
    const tree_count = count: {
        var count: usize = 0;
        inline for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;
            count += 1; // Object tree.
            count += @intFromBool(Groove.IdTree != void);
            count += std.meta.fields(Groove.IndexTrees).len;
        }
        break :count count;
    };

    return struct {
        const Forest = @This();

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        const Callback = *const fn (*Forest) void;
        const JoinOp = enum {
            compacting,
            checkpoint,
            open,
        };

        pub const groove_config = groove_cfg;
        pub const Grooves = _Grooves;
        pub const GroovesOptions = _GroovesOptions;

        join_op: ?JoinOp = null,
        join_pending: usize = 0,
        join_callback: ?Callback = null,
        callback: ?Callback = null,

        compaction_op: ?u64 = null, // TODO union on JoinOp

        grid: *Grid,
        grooves: Grooves,
        node_pool: *NodePool,
        manifest_log: ManifestLog,

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

            var manifest_log = try ManifestLog.init(allocator, grid, .{ .tree_count = tree_count });
            errdefer manifest_log.deinit(allocator);

            var grooves: Grooves = undefined;
            var grooves_initialized: usize = 0;

            errdefer inline for (std.meta.fields(Grooves), 0..) |field, field_index| {
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
                .manifest_log = manifest_log,
            };
        }

        pub fn deinit(forest: *Forest, allocator: mem.Allocator) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).deinit(allocator);
            }

            forest.manifest_log.deinit(allocator);
            forest.node_pool.deinit(allocator);
            allocator.destroy(forest.node_pool);
        }

        pub fn reset(forest: *Forest) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).reset();
            }

            forest.node_pool.reset();
            forest.manifest_log.reset();

            forest.* = .{
                // Don't reset the grid – replica is responsible for grid cancellation.
                .grid = forest.grid,
                .grooves = forest.grooves,
                .node_pool = forest.node_pool,
                .manifest_log = forest.manifest_log,
            };
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
                ) *const fn (*GrooveFor(groove_field_name)) void {
                    return struct {
                        fn groove_cb(groove: *GrooveFor(groove_field_name)) void {
                            const grooves = @fieldParentPtr(Grooves, groove_field_name, groove);
                            const forest = @fieldParentPtr(Forest, "grooves", grooves);

                            forest.join();
                        }
                    }.groove_cb;
                }

                pub fn manifest_log_callback(manifest_log: *ManifestLog) void {
                    const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
                    forest.join();
                }

                fn join(forest: *Forest) void {
                    assert(forest.join_op == join_op);
                    assert(forest.join_callback != null);
                    assert(forest.join_pending <= std.meta.fields(Grooves).len);

                    forest.join_pending -= 1;
                    if (forest.join_pending > 0) return;

                    if (join_op == .compacting) {
                        // TODO join_op union?

                        const half_bar = @divExact(constants.lsm_batch_multiple, 2);
                        if ((forest.compacting_op.? + 1) % half_bar == 0) {
                            if (constants.verify) forest.verify_manifest();
                            forest.manifest_log.forfeit();
                        }
                        forest.compacting_op = null;

                        inline for (std.meta.fields(Grooves)) |field| {
                            @field(forest.grooves, field.name).compact_end();
                        }
                    }

                    if (join_op == .checkpoint) {
                        if (Storage == @import("../testing/storage.zig").Storage) {
                            // We should have finished all checkpoint writes by now.
                            forest.grid.superblock.storage.assert_no_pending_writes(.grid);
                        }
                    }

                    const callback = forest.join_callback.?;
                    forest.join_op = null;
                    forest.join_callback = null;
                    callback(forest);
                }
            };
        }

        pub fn open(forest: *Forest, callback: Callback) void {
            assert(forest.callback == null);
            assert(forest.join_callback == null);
            forest.callback = callback;

            forest.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            level: u7,
            table: *const schema.ManifestLog.TableInfo,
        ) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.callback != null);
            assert(forest.join_callback == null);
            assert(level < constants.lsm_levels);

            // TODO Is there a faster (or cleaner) way to map tree-id to the tree?
            inline for (std.meta.fields(Grooves)) |groove_field| {
                const groove = @field(forest.grooves, groove_field.name);
                const Groove = @TypeOf(groove);

                if (groove.objects.config.id == table.tree_id) {
                    groove.objects.open_table(level, table);
                    return;
                }

                if (Groove.IdTree != void) {
                    if (groove.ids.config.id == table.tree_id) {
                        groove.ids.open_table(level, table);
                        return;
                    }
                }

                inline for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                    const tree = &@field(groove.indexes, tree_field.name);
                    if (tree.config.id == table.tree_id) {
                        @field(groove.indexes, tree_field.name).open_table(level, table);
                        return;
                    }
                }
            }

            log.err("manifest_log_open_event: unknown table in manifest: {}" , .{ table });
            @panic("Forest.manifest_log_open_event: unknown table in manifest");
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.callback != null);
            assert(forest.join_callback == null);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_done();
            }

            const callback = forest.callback.?;
            forest.callback = null;
            callback(forest);
        }

        pub fn compact(forest: *Forest, callback: Callback, op: u64) void {
            const Join = JoinType(.compacting);

            assert(forest.callback == null);
            assert(forest.compaction_op == null);

            forest.callback = callback;
            forest.compaction_op = op;

            if (op % @divExact(constants.lsm_batch_multiple, 2) == 0) {
                if (constants.verify) forest.verify_manifest();
                forest.join_pending += 1;
                forest.manifest_log.reserve(); // TODO inline in compact()?
                forest.manifest_log.compact(Join.manifest_log_callback);
            }

            // Start a compacting join.
            Join.start(forest, compact_callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact(Join.groove_callback(field.name), op);
            }
        }

        fn compact_callback(forest: *Forest) void {
            forest.manifest_log.compact(manifest_log_callback);
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            assert(forest.callback == null);
            assert(forest.compaction_op == null);
            forest.callback = callback;

            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all pending io before checkpointing.
                forest.grid.superblock.storage.assert_no_pending_writes(.grid);
            }

            const Join = JoinType(.checkpoint);
            Join.start(forest, checkpoint_callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).checkpoint(Join.groove_callback(field.name));
            }
        }

        fn checkpoint_callback(forest: *Forest) void {
            forest.manifest_log.checkpoint(manifest_log_callback);
        }

        fn manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            const callback = forest.callback.?;

            forest.callback = null;
            callback(forest);
        }

        fn verify(forest: *Forest, snapshot: u64) void {
            // TODO: When state sync is proactive, re-enable this.
            //if (true) return;

            _ = forest;
            _ = snapshot;

            // TODO use ForestTableIterator

            // TODO s/prev/previous; s/iter/iterator.
            //for (manifest.levels) |*level| {
            //    var key_max_prev: ?Key = null;
            //    var table_info_iter = level.iterator(
            //        .visible,
            //        &.{snapshot},
            //        .ascending,
            //        null,
            //    );
            //    while (table_info_iter.next()) |table_info| {
            //        if (key_max_prev) |k| {
            //            assert(compare_keys(k, table_info.key_min) == .lt);
            //        }
            //        // We could have key_min == key_max if there is only one value.
            //        assert(compare_keys(table_info.key_min, table_info.key_max) != .gt);
            //        key_max_prev = table_info.key_max;
            //
            //        Table.verify(
            //            Storage,
            //            manifest.manifest_log.grid.superblock.storage,
            //            table_info.address,
            //            table_info.key_min,
            //            table_info.key_max,
            //        );
            //    }
            //}
        }
    };
}
