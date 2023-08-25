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

    const TreeInfo = struct {
        Tree: type,
        tree_name: []const u8,
        tree_id: u16,
        groove_name: []const u8,
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
            }};

            if (Groove.IdTree != void) {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = Groove.IdTree,
                    .tree_name = groove_field.name ++ ".id",
                    .tree_id = @field(Groove.config.ids, "id"),
                    .groove_name = groove_field.name,
                }};
            }

            for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = tree_field.type,
                    .tree_name = groove_field.name ++ "." ++ tree_field.name,
                    .tree_id = @field(Groove.config.ids, tree_field.name),
                    .groove_name = groove_field.name,
                }};
            }
        }

        break :tree_infos tree_infos;
    };

    return struct {
        const Forest = @This();

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        const Callback = *const fn (*Forest) void;

        pub const groove_config = groove_cfg;
        pub const Grooves = _Grooves;
        pub const GroovesOptions = _GroovesOptions;

        progress: ?union(enum) {
            open: struct { callback: Callback },
            checkpoint: struct { callback: Callback },
            compact: struct {
                op: u64,
                pending: usize,
                callback: Callback,
            },
        } = null,

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

            var manifest_log =
                try ManifestLog.init(allocator, grid, .{ .tree_count = tree_infos.len });
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
            table: *const schema.ManifestLog.TableInfo,
        ) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .open);
            assert(level < constants.lsm_levels);

            // TODO Is there a faster (or cleaner) way to map tree-id to the tree?
            inline for (std.meta.fields(Grooves)) |groove_field| {
                var groove = &@field(forest.grooves, groove_field.name);
                const Groove = @TypeOf(groove.*);

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

            log.err("manifest_log_open_event: unknown table in manifest: {}", .{table});
            @panic("Forest.manifest_log_open_event: unknown table in manifest");
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

            var pending: usize = 0;

            inline for (std.meta.fields(Grooves)) |field| {
                pending += 1;
                @field(forest.grooves, field.name).compact(compact_groove_callback(field.name), op);
            }

            if (op % @divExact(constants.lsm_batch_multiple, 2) == 0 and
                op >= constants.lsm_batch_multiple and
                !forest.grid.superblock.working.vsr_state.op_compacted(op))
            {
                // This is the first beat of a bar that we have not compacted already.
                pending += 1;
                forest.manifest_log.compact(compact_manifest_log_callback);
            }

            forest.progress = .{ .compact = .{
                .op = op,
                .pending = pending,
                .callback = callback,
            } };
        }

        fn compact_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            forest.compact_callback();
        }

        fn compact_groove_callback(
            comptime groove_field_name: []const u8,
        ) *const fn (*GrooveFor(groove_field_name)) void {
            return struct {
                fn groove_callback(groove: *GrooveFor(groove_field_name)) void {
                    const grooves: *align(@alignOf(Grooves)) Grooves =
                        @alignCast(@fieldParentPtr(Grooves, groove_field_name, groove));
                    const forest = @fieldParentPtr(Forest, "grooves", grooves);
                    forest.compact_callback();
                }
            }.groove_callback;
        }

        fn compact_callback(forest: *Forest) void {
            assert(forest.progress.? == .compact);

            const progress = &forest.progress.?.compact;
            // +1 for a manifest log compaction.
            assert(progress.pending <= std.meta.fields(Grooves).len + 1);

            progress.pending -= 1;
            if (progress.pending > 0) return;

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact_end();
            }

            if ((progress.op + 1) % @divExact(constants.lsm_batch_multiple, 2) == 0 and
                progress.op >= constants.lsm_batch_multiple and
                !forest.grid.superblock.working.vsr_state.op_compacted(progress.op))
            {
                // This is the last beat of a bar that was not already compacted.
                forest.manifest_log.compact_end();
            }

            const callback = progress.callback;
            forest.progress = null;
            callback(forest);
        }

        fn GrooveFor(comptime groove_field_name: []const u8) type {
            return @TypeOf(@field(@as(Grooves, undefined), groove_field_name));
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
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

            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all checkpoint writes by now.
                forest.grid.superblock.storage.assert_no_pending_writes(.grid);
            }

            const callback = forest.progress.?.checkpoint.callback;
            forest.progress = null;
            callback(forest);
        }
    };
}
