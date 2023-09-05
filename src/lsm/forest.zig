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
                assert(id > 0);
                assert(std.mem.indexOfScalar(u16, ids, id) == null);

                ids = ids ++ [_]u16{id};
            }
        }
    }

    const TreeInfo = struct {
        Tree: type,
        tree_name: []const u8,
        tree_id: u16,
        groove_name: []const u8,
        groove_tree: union(enum) { objects, ids, indexes: []const u8 },
    };

    const tree_infos: []const TreeInfo = tree_infos: {
        var tree_infos: []const TreeInfo = &[_]TreeInfo{};
        for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;

            tree_infos = tree_infos ++ &[_]TreeInfo{.{
                .Tree = Groove.ObjectTree,
                .tree_name = groove_field.name,
                .tree_id = @field(Groove.config.ids, "timestamp"),
                .groove_name = groove_field.name,
                .groove_tree = .objects,
            }};

            if (Groove.IdTree != void) {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = Groove.IdTree,
                    .tree_name = groove_field.name ++ ".id",
                    .tree_id = @field(Groove.config.ids, "id"),
                    .groove_name = groove_field.name,
                    .groove_tree = .ids,
                }};
            }

            for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = tree_field.type,
                    .tree_name = groove_field.name ++ "." ++ tree_field.name,
                    .tree_id = @field(Groove.config.ids, tree_field.name),
                    .groove_name = groove_field.name,
                    .groove_tree = .{ .indexes = tree_field.name },
                }};
            }
        }

        break :tree_infos tree_infos;
    };

    const tree_id_range = comptime tree_id_range: {
        var tree_id_min: u16 = 1;
        var tree_id_max: u16 = 0;
        for (tree_infos) |tree_info| {
            tree_id_min = @min(tree_id_min, tree_info.tree_id);
            tree_id_max = @max(tree_id_max, tree_info.tree_id);
        }
        // There are no gaps in the tree ids.
        assert((tree_id_max - tree_id_min + 1) == tree_infos.len);
        break :tree_id_range .{ .min = tree_id_min, .max = tree_id_max };
    };

    return struct {
        const Forest = @This();

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        const Callback = *const fn (*Forest) void;
        const GroovesBitSet = std.StaticBitSet(std.meta.fields(Grooves).len);

        pub const groove_config = groove_cfg;
        pub const Grooves = _Grooves;
        pub const GroovesOptions = _GroovesOptions;

        progress: ?union(enum) {
            open: struct { callback: Callback },
            checkpoint: struct { callback: Callback },
            compact: struct {
                op: u64,
                /// Count which groove compactions are in progress.
                pending: GroovesBitSet = GroovesBitSet.initFull(),
                callback: Callback,
            },
        } = null,

        grid: *Grid,
        grooves: Grooves,
        node_pool: *NodePool,
        manifest_log: ManifestLog,
        manifest_log_progress: enum { idle, compacting, done, skip } = .idle,

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

            var manifest_log = try ManifestLog.init(allocator, grid, .{
                .tree_id_min = tree_id_range.min,
                .tree_id_max = tree_id_range.max,
            });
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

                groove.* = try Groove.init(allocator, node_pool, grid, groove_options);
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

            forest.manifest_log.reset();
            forest.node_pool.reset();

            forest.* = .{
                // Don't reset the grid – replica is responsible for grid cancellation.
                .grid = forest.grid,
                .grooves = forest.grooves,
                .node_pool = forest.node_pool,
                .manifest_log = forest.manifest_log,
            };
        }

        pub fn open(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
            forest.progress = .{ .open = .{ .callback = callback } };

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_commence(&forest.manifest_log);
            }

            forest.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            level: u7,
            table: *const schema.Manifest.TableInfo,
        ) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .open);
            assert(level < constants.lsm_levels);

            switch (table.tree_id) {
                inline tree_id_range.min...tree_id_range.max => |tree_id| {
                    var t: *TreeForIdType(tree_id) = forest.tree_for_id(tree_id);
                    t.open_table(level, table);
                },
                else => {
                    log.err("manifest_log_open_event: unknown table in manifest: {}", .{table});
                    @panic("Forest.manifest_log_open_event: unknown table in manifest");
                },
            }
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .open);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_complete();
            }

            const callback = forest.progress.?.open.callback;
            forest.progress = null;
            callback(forest);
        }

        pub fn compact(forest: *Forest, callback: Callback, op: u64) void {
            assert(forest.progress == null);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact(compact_groove_callback(field.name), op);
            }

            if (op % @divExact(constants.lsm_batch_multiple, 2) == 0) {
                assert(forest.manifest_log_progress == .idle);

                forest.manifest_log_progress = .compacting;
                forest.manifest_log.compact(compact_manifest_log_callback, op);
            } else {
                if (op == 1) {
                    assert(forest.manifest_log_progress == .idle);
                    forest.manifest_log_progress = .skip;
                } else {
                    assert(forest.manifest_log_progress != .idle);
                }
            }

            forest.progress = .{ .compact = .{ .op = op, .callback = callback } };
        }

        fn compact_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.manifest_log_progress == .compacting);

            forest.manifest_log_progress = .done;

            if (forest.progress) |progress| {
                assert(progress == .compact);

                forest.compact_callback();
            } else {
                // The manifest log compaction completed between compaction beats.
            }
        }

        fn compact_groove_callback(
            comptime groove_field_name: []const u8,
        ) *const fn (*GrooveFor(groove_field_name)) void {
            return struct {
                fn groove_callback(groove: *GrooveFor(groove_field_name)) void {
                    const grooves: *align(@alignOf(Grooves)) Grooves =
                        @alignCast(@fieldParentPtr(Grooves, groove_field_name, groove));
                    const forest = @fieldParentPtr(Forest, "grooves", grooves);

                    inline for (std.meta.fields(Grooves), 0..) |groove_field, i| {
                        if (std.mem.eql(u8, groove_field.name, groove_field_name)) {
                            assert(forest.progress.?.compact.pending.isSet(i));

                            forest.progress.?.compact.pending.unset(i);
                            break;
                        }
                    } else unreachable;

                    forest.compact_callback();
                }
            }.groove_callback;
        }

        fn compact_callback(forest: *Forest) void {
            assert(forest.progress.? == .compact);
            assert(forest.manifest_log_progress != .idle);

            const progress = &forest.progress.?.compact;
            if (progress.pending.count() > 0) return;

            const half_bar_end =
                (progress.op + 1) % @divExact(constants.lsm_batch_multiple, 2) == 0;
            // On the last beat of the bar, make sure that manifest log compaction is finished.
            if (half_bar_end and forest.manifest_log_progress == .compacting) return;

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact_end();
            }

            if (half_bar_end) {
                switch (forest.manifest_log_progress) {
                    .idle => unreachable,
                    .compacting => unreachable,
                    .done => forest.manifest_log.compact_end(),
                    .skip => {},
                }
                forest.manifest_log_progress = .idle;
            }

            const callback = progress.callback;
            forest.progress = null;
            callback(forest);
        }

        fn GrooveFor(comptime groove_field_name: []const u8) type {
            const groove_field = @field(std.meta.FieldEnum(Grooves), groove_field_name);
            return std.meta.FieldType(Grooves, groove_field);
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
            assert(forest.manifest_log_progress == .idle);
            forest.progress = .{ .checkpoint = .{ .callback = callback } };

            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all pending io before checkpointing.
                forest.grid.superblock.storage.assert_no_pending_writes(.grid);
            }

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).assert_between_bars();
            }

            forest.manifest_log.checkpoint(checkpoint_manifest_log_callback);
        }

        fn checkpoint_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .checkpoint);
            assert(forest.manifest_log_progress == .idle);

            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all checkpoint writes by now.
                forest.grid.superblock.storage.assert_no_pending_writes(.grid);
            }

            const callback = forest.progress.?.checkpoint.callback;
            forest.progress = null;
            callback(forest);
        }

        fn TreeForIdType(comptime tree_id: u16) type {
            for (tree_infos) |tree_info| {
                if (tree_info.tree_id == tree_id) return tree_info.Tree;
            }
            unreachable;
        }

        fn tree_for_id(forest: *Forest, comptime tree_id: u16) *TreeForIdType(tree_id) {
            inline for (tree_infos) |tree_info| {
                if (tree_info.tree_id == tree_id) {
                    var groove = &@field(forest.grooves, tree_info.groove_name);

                    switch (tree_info.groove_tree) {
                        .objects => return &groove.objects,
                        .ids => return &groove.ids,
                        .indexes => |index_name| return &@field(groove.indexes, index_name),
                    }
                }
            }
            unreachable;
        }
    };
}
