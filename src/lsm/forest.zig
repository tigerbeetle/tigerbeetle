const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const GridType = @import("grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);

pub fn ForestType(comptime Storage: type, comptime groove_config: anytype) type {
    var groove_fields: []const std.builtin.TypeInfo.StructField = &.{};
    var groove_options_fields: []const std.builtin.TypeInfo.StructField = &.{};

    for (std.meta.fields(@TypeOf(groove_config))) |field| {
        const Groove = @field(groove_config, field.name);
        groove_fields = groove_fields ++ [_]std.builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = Groove,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };

        groove_options_fields = groove_options_fields ++ [_]std.builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = Groove.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };
    }

    const Grooves = @Type(.{
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

    return struct {
        const Forest = @This();

        const Grid = GridType(Storage);

        const Callback = fn (*Forest) void;
        const JoinOp = enum {
            compacting,
            checkpoint,
            open,
        };

        pub const groove_config = groove_config;
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

            errdefer inline for (std.meta.fields(Grooves)) |field, field_index| {
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
                ) fn (*GrooveFor(groove_field_name)) void {
                    return struct {
                        fn groove_cb(groove: *GrooveFor(groove_field_name)) void {
                            const grooves = @fieldParentPtr(Grooves, groove_field_name, groove);
                            const forest = @fieldParentPtr(Forest, "grooves", grooves);

                            assert(forest.join_op == join_op);
                            assert(forest.join_callback != null);
                            assert(forest.join_pending <= std.meta.fields(Grooves).len);

                            forest.join_pending -= 1;
                            if (forest.join_pending > 0) return;

                            if (join_op == .checkpoint) {
                                if (Storage == @import("../testing/storage.zig").Storage) {
                                    // We should have finished all checkpoint io by now.
                                    // TODO This may change when grid repair lands.
                                    forest.grid.superblock.storage.assert_no_pending_io(.grid);
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

        pub fn op_done(forest: *Forest, op: u64) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).op_done(op);
            }
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all pending io before checkpointing.
                // TODO This may change when grid repair lands.
                forest.grid.superblock.storage.assert_no_pending_io(.grid);
            }

            const Join = JoinType(.checkpoint);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).checkpoint(Join.groove_callback(field.name));
            }
        }
    };
}
