const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

pub fn ForestType(comptime grove_config: anytype) type {
    // TODO: expose all grove fields in the config while also adding decls somehow
    // forest.groves.transfers.put()

    comptime var grove_fields: []const std.builtin.TypeInfo.StructField = &.{};

    inline for (std.meta.fields(@TypeOf(grove_config))) |field| {
        const Grove = @field(grove_config, field.name);
        grove_fields = grove_fields ++ [_]std.builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = Grove,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Grove),
            },
        };
    }

    const Groves = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = grove_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    return struct {
        const Forest = @This();

        const SyncOp = enum { compacting, checkpoint };
        const Callback = fn (*Forest) void;

        sync_op: ?SyncOp = null,
        sync_pending: usize = 0,
        sync_callback: ?Callback = null,

        groves: Groves,

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            superblock: *SuperBlock,
            cache_size: usize,
            commit_count_max: usize,
        ) !Forest {

        }

        pub fn deinit(forest: *Forest, allocator: mem.Allocator) void {
            
        }

        pub fn compact(forest: *Forest, callback: Callback) void {
            // TODO foreach groves -> compact_io; foreach groves -> compact_cpu
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            // TODO foreach groves -> checkpoint
        }
    };
}
