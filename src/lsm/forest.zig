const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);

fn assert_struct_field_names_strict_equal(object_a: anytype, object_b: anytype) void {
    inline for (std.meta.fields(@TypeOf(object_a))) |field_a| {
        comptime var in_object_b = false;

        inline for (std.meta.fields(@TypeOf(object_b))) |field_b| {
            in_object_b = in_object_b or std.mem.eql(u8, field_a.name, field_b.name);
        }

        if (!in_object_b) {
            @compileError(@typeName(@TypeOf(object_a)) ++ " with invalid field " ++ field_a.name);
        }
    }
}

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
        const JoinOp = enum { compacting, checkpoint };

        join_op: ?JoinOp = null,
        join_pending: usize = 0,
        join_callback: ?Callback = null,

        grooves: Grooves,
        node_pool: *NodePool,

        pub fn init(
            allocator: mem.Allocator,
            grid: *Grid,
            // .{ .transfers = .{ cache_size = 128, com_count_max }, .accounts = 64 }
            groove_options: anytype,
        ) !Forest {
            // NodePool must be allocated to pass in a stable address for the Grooves.
            const node_pool = try allocator.create(NodePool);
            errdefer allocator.destroy(node_pool);

            // Use lsm_tables_max for the node_count.
            node_pool.* = try NodePool.init(allocator, config.lsm_tables_max);
            errdefer node_pool.deinit(allocator);

            var grooves: Grooves = undefined;
            var grooves_initialized: usize = 0;

            // Ensure groove_options contains optiosn for all Groove types in the Grooves object.
            comptime assert_struct_field_names_strict_equal(groove_options, Grooves);

            errdefer inline for (std.meta.fields(Grooves)) |field, field_index| {
                if (grooves_initialized >= field_index + 1) {
                    @field(grooves, field.name).deinit(allocator);
                }
            };

            inline for (std.meta.fields(Grooves)) |field| {
                // Ensure the options for this groove only contain cache_size and commit_count_max.
                comptime assert_struct_field_names_strict_equal(
                    @field(groove_options, field.name),
                    .{ 
                        .cache_size = @as(u32, 0), 
                        .commit_count_max = @as(usize, 0),
                    },
                );

                const groove = &@field(grooves, field.name);
                groove.* = try @TypeOf(groove.*).init(
                    allocator,
                    node_pool,
                    grid,
                    @field(groove_options, field.name).cache_size,
                    @field(groove_options, field.name).commit_count_max,
                );

                grooves_initialized += 1;
            }

            return Forest{
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
                            const forest = @fieldParentPtr(Grooves, groove_field_name, groove);
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

        pub fn compact(forest: *Forest, op: u46, callback: Callback) void {
            // Start a compacting join.
            const Join = JoinType(.compacting);
            Join.start(forest, callback);

            // Queue up the storage IO operations on the grooves.
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact_io(op, Join.groove_callback(field.name));
            }

            // Tick the storage backend to start processing the IO.
            forest.grid.storage.tick();

            // While IO is processing, run/pipeline the CPU work on the grooves.
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact_cpu();
            }
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            const Join = JoinType(.checkpoint);
            Join.start(forest, callback);

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).checkpoint(Join.groove_callback(field.name));
            }
        }
    };
}

test "Forest" {
    const Transfer = @import("../tigerbeetle.zig").Transfer;
    const Account = @import("../tigerbeetle.zig").Account;
    const Storage = @import("../storage.zig").Storage;

    const Forest = ForestType(Storage, .{
        .accounts = GrooveType(
            Storage,
            Account,
            .{
                .ignored = &[_][]const u8{"reserved", "flags", "user_data"},
                .derived = .{},
            },
        ),
        .transfers = GrooveType(
            Storage,
            Transfer,
            .{
                .ignored = &[_][]const u8{"reserved", "flags", "user_data"},
                .derived = .{},
            },
        ),
    });

    _ = Forest.init;
    _ = Forest.deinit;

    _ = Forest.compact;
    _ = Forest.checkpoint;
}