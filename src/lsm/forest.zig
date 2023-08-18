const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const GridType = @import("../vsr/grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);

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

    return struct {
        const Forest = @This();

        const Grid = GridType(Storage);

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

        grid: *Grid,
        grooves: Grooves,
        node_pool: *NodePool,

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
            };
        }

        pub fn deinit(forest: *Forest, allocator: mem.Allocator) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).deinit(allocator);
            }

            forest.node_pool.deinit(allocator);
            allocator.destroy(forest.node_pool);
        }

        pub fn reset(forest: *Forest) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).reset();
            }

            forest.node_pool.reset();

            forest.* = .{
                // Don't reset the grid – replica is responsible for grid cancellation.
                .grid = forest.grid,
                .grooves = forest.grooves,
                .node_pool = forest.node_pool,
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

                            assert(forest.join_op == join_op);
                            assert(forest.join_callback != null);
                            assert(forest.join_pending <= std.meta.fields(Grooves).len);

                            forest.join_pending -= 1;
                            if (forest.join_pending > 0) return;

                            if (join_op == .compacting) {
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
                    }.groove_cb;
                }
            };
        }

        pub fn open(forest: *Forest, callback: Callback) void {
            const Join = JoinType(.open);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open(Join.groove_callback(field.name));
            }
        }

        pub fn compact(forest: *Forest, callback: Callback, op: u64) void {
            // Start a compacting join.
            const Join = JoinType(.compacting);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact(Join.groove_callback(field.name), op);
            }
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all pending io before checkpointing.
                forest.grid.superblock.storage.assert_no_pending_writes(.grid);
            }

            const Join = JoinType(.checkpoint);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).checkpoint(Join.groove_callback(field.name));
            }
        }
    };
}
