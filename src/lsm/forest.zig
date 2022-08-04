const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const GridType = @import("grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);

pub fn ForestType(comptime Storage: type, comptime groove_config: anytype) type {
    comptime var groove_fields: []const std.builtin.TypeInfo.StructField = &.{};

    inline for (std.meta.fields(@TypeOf(groove_config))) |field| {
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
    }

    const Grooves = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = groove_fields,
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

        const GrooveOptions = struct {
            cache_size: u32,
            commit_count_max: u32,
        };

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
            // (e.g.) .{ .transfers = .{ cache_size = 128, com_count_max = n }, .accounts = same }
            all_groove_options: anytype,
        ) !Forest {
            // NodePool must be allocated to pass in a stable address for the Grooves.
            const node_pool = try allocator.create(NodePool);
            errdefer allocator.destroy(node_pool);

            // TODO: look into using lsm_table_size_max for the node_count.
            node_pool.* = try NodePool.init(allocator, node_count);
            errdefer node_pool.deinit(allocator);

            // Ensure options contains options for all Groove types in the Grooves object.
            inline for (std.meta.fields(Grooves)) |field| {
                comptime assert(@hasField(@TypeOf(all_groove_options), field.name));
            }
            inline for (std.meta.fields(@TypeOf(all_groove_options))) |field| {
                comptime assert(@hasField(Grooves, field.name));
            }

            var grooves: Grooves = undefined;
            var grooves_initialized: usize = 0;

            errdefer inline for (std.meta.fields(Grooves)) |field, field_index| {
                if (grooves_initialized >= field_index + 1) {
                    @field(grooves, field.name).deinit(allocator);
                }
            };

            inline for (std.meta.fields(Grooves)) |groove_field| {
                const groove_options: GrooveOptions = @field(all_groove_options, groove_field.name);
                const groove = &@field(grooves, groove_field.name);

                groove.* = try @TypeOf(groove.*).init(
                    allocator,
                    node_pool,
                    grid,
                    groove_options.cache_size,
                    groove_options.commit_count_max,
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
            forest.node_pool.deinit(allocator);
            allocator.destroy(forest.node_pool);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).deinit(allocator);
            }
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

            // Queue up the storage IO operations on the grooves.
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact_io(op, Join.groove_callback(field.name));
            }

            // Tick the storage backend to start processing the IO.
            forest.grid.superblock.storage.tick();

            // While IO is processing, run/pipeline the CPU work on the grooves.
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact_cpu();
            }
        }

        pub fn checkpoint(forest: *Forest, callback: Callback, op: u64) void {
            const Join = JoinType(.checkpoint);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).checkpoint(op, Join.groove_callback(field.name));
            }
        }
    };
}
