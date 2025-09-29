const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const Command = @import("../vsr.zig").Command;
const Zone = @import("../vsr.zig").Zone;
const CommitStage = @import("../vsr/replica.zig").CommitStage;
const Operation = @import("../tigerbeetle.zig").Operation;
const Duration = stdx.Duration;

const TreeEnum = tree_enum: {
    const tree_ids = @import("../state_machine.zig").tree_ids;
    var tree_fields: []const std.builtin.Type.EnumField = &[_]std.builtin.Type.EnumField{};

    for (std.meta.declarations(tree_ids)) |groove_field| {
        const tree_ids_groove = @field(tree_ids, groove_field.name);
        for (std.meta.fieldNames(@TypeOf(tree_ids_groove))) |field_name| {
            tree_fields = tree_fields ++ &[_]std.builtin.Type.EnumField{.{
                .name = groove_field.name ++ "." ++ field_name,
                .value = @field(tree_ids_groove, field_name),
            }};
        }
    }

    break :tree_enum @Type(.{ .@"enum" = .{
        .tag_type = u32,
        .fields = tree_fields,
        .decls = &.{},
        .is_exhaustive = true,
    } });
};

/// Returns the count of an exhaustive enum.
fn enum_count(EnumOrUnion: type) u8 {
    const type_info = @typeInfo(EnumOrUnion);
    assert(type_info == .@"enum" or type_info == .@"union");

    const Enum = if (type_info == .@"enum")
        type_info.@"enum"
    else
        @typeInfo(type_info.Union.tag_type.?).@"enum";
    assert(Enum.is_exhaustive);

    return Enum.fields.len;
}

/// Maps an exhaustive enum value from an enum type that might potentially start with a non-zero
/// value or be sparse to a continuous index that fits within enum_count().
fn index_from_enum(enum_tag: anytype) u8 {
    const type_info = @typeInfo(@TypeOf(enum_tag));
    assert(type_info == .@"enum" or type_info == .@"union");

    const Enum = if (type_info == .@"enum")
        type_info.@"enum"
    else
        @typeInfo(type_info.Union.tag_type.?).@"enum";
    assert(Enum.is_exhaustive);

    inline for (Enum.fields, 0..) |enum_field, i| {
        if (enum_field.value == @intFromEnum(enum_tag)) {
            return i;
        }
    } else unreachable;
}

// TODO: It should be possible to get rid of all unbounded cardinality (eg, level_b being a u8) and
// replace them with enums instead. This would allow for calculating the stack limit automatically.

/// Base {Timing,Tracing} Event. This is further split up into two different Events that share the
/// same tag: ones for timing and ones for tracing.
///
/// This is because there's a difference between tracing and aggregate timing. When doing tracing,
/// the code needs to worry about the static allocation required for _concurrent_ traces. That is,
/// there might be multiple `scan_tree`s, with different `index`es happening at once.
///
/// When timing, this is flipped on its head: the timing code doesn't need space for concurrency
/// because it is called once, when an event has finished, and internally aggregates. The
/// aggregation is needed because there can be an unknown number of calls between flush intervals,
/// compared to tracing which is emitted as it happens.
///
/// Rather, it needs space for the cardinality of the tags you'd like to emit. In the case of
/// `scan_tree`s, this would be the tree it's scanning over, instead of the index of the scan.
pub const Event = union(enum) {
    replica_commit: struct { stage: CommitStage.Tag, op: ?usize = null },
    replica_aof_write: struct { op: usize },
    replica_aof_checkpoint,
    replica_sync_table: struct { index: usize },
    replica_request: struct { operation: Operation },
    replica_request_execute: struct { operation: Operation },
    replica_request_local: struct { operation: Operation },

    compact_beat: struct { tree: TreeEnum, level_b: u8 },
    compact_beat_merge: struct { tree: TreeEnum, level_b: u8 },
    compact_manifest,
    compact_mutable: struct { tree: TreeEnum },
    compact_mutable_suffix: struct { tree: TreeEnum },

    lookup: struct { tree: TreeEnum },
    lookup_worker: struct { index: u8, tree: TreeEnum },

    scan_tree: struct { index: u8, tree: TreeEnum },
    scan_tree_level: struct { index: u8, tree: TreeEnum, level: u8 },

    grid_read: struct { iop: usize },
    grid_write: struct { iop: usize },
    storage_read: struct { zone: Zone },
    storage_write: struct { zone: Zone },

    metrics_emit: void,

    client_request_round_trip: struct { operation: Operation },

    pub const Tag = std.meta.Tag(Event);

    /// Normally, Zig would stringify a union(enum) like this as `{"compact_beat": {"tree": ...}}`.
    /// Remove this extra layer of indirection.
    pub fn jsonStringify(event: Event, jw: anytype) !void {
        switch (event) {
            inline else => |payload, tag| {
                if (@TypeOf(payload) == void) {
                    try jw.write("");
                } else if (tag == .replica_commit) {
                    try jw.write(.{ .stage = @tagName(payload.stage), .op = payload.op });
                } else {
                    try jw.write(payload);
                }
            },
        }
    }

    /// Convert the base event to an EventTiming or EventMetric.
    pub fn as(event: *const Event, EventType: type) EventType {
        @setEvalBranchQuota(32_000);
        return switch (event.*) {
            inline else => |source_payload, tag| {
                const TargetPayload = @FieldType(EventType, @tagName(tag));
                const target_payload_info = @typeInfo(TargetPayload);
                assert(target_payload_info == .void or target_payload_info == .@"struct");

                const target_payload: TargetPayload = switch (@typeInfo(TargetPayload)) {
                    .void => {},
                    .@"struct" => blk: {
                        var target_payload: TargetPayload = undefined;
                        inline for (comptime std.meta.fieldNames(TargetPayload)) |field| {
                            @field(target_payload, field) = @field(source_payload, field);
                        }
                        break :blk target_payload;
                    },
                    else => unreachable,
                };

                return @unionInit(EventType, @tagName(tag), target_payload);
            },
        };
    }
};

pub const EventTiming = union(Event.Tag) {
    replica_commit: struct { stage: CommitStage.Tag },
    replica_aof_write,
    replica_aof_checkpoint,
    replica_sync_table,
    replica_request: struct { operation: Operation },
    replica_request_execute: struct { operation: Operation },
    replica_request_local: struct { operation: Operation },

    compact_beat: struct { tree: TreeEnum },
    compact_beat_merge: struct { tree: TreeEnum },
    compact_manifest,
    compact_mutable: struct { tree: TreeEnum },
    compact_mutable_suffix: struct { tree: TreeEnum },

    lookup: struct { tree: TreeEnum },
    lookup_worker: struct { tree: TreeEnum },

    scan_tree: struct { tree: TreeEnum },
    scan_tree_level: struct { tree: TreeEnum },

    grid_read,
    grid_write,
    storage_read: struct { zone: Zone },
    storage_write: struct { zone: Zone },

    metrics_emit,

    client_request_round_trip: struct { operation: Operation },

    pub const slot_limits = std.enums.EnumArray(Event.Tag, u32).init(.{
        .replica_commit = enum_count(CommitStage.Tag),
        .replica_aof_write = 1,
        .replica_aof_checkpoint = 1,
        .replica_sync_table = 1,
        .replica_request = enum_count(Operation),
        .replica_request_execute = enum_count(Operation),
        .replica_request_local = enum_count(Operation),
        .compact_beat = enum_count(TreeEnum),
        .compact_beat_merge = enum_count(TreeEnum),
        .compact_manifest = 1,
        .compact_mutable = enum_count(TreeEnum),
        .compact_mutable_suffix = enum_count(TreeEnum),
        .lookup = enum_count(TreeEnum),
        .lookup_worker = enum_count(TreeEnum),
        .scan_tree = enum_count(TreeEnum),
        .scan_tree_level = enum_count(TreeEnum),
        .grid_read = 1,
        .grid_write = 1,
        .metrics_emit = 1,
        .storage_read = enum_count(Zone),
        .storage_write = enum_count(Zone),
        .client_request_round_trip = enum_count(Operation),
    });

    pub const slot_bases = array: {
        var array = std.enums.EnumArray(Event.Tag, u32).initFill(0);
        var next: u32 = 0;
        for (std.enums.values(Event.Tag)) |event_type| {
            array.set(event_type, next);
            next += slot_limits.get(event_type);
        }
        break :array array;
    };

    pub const slot_count = count: {
        var count: u32 = 0;
        for (std.enums.values(Event.Tag)) |event_type| {
            count += slot_limits.get(event_type);
        }
        break :count count;
    };

    // Unlike with EventTracing, which neatly organizes related events underneath one another, the
    // order here does not matter.
    //
    // TODO: This could be computed automatically, at least in the cases where all, or all except
    // one of the values are enums.
    pub fn slot(event: *const EventTiming) u32 {
        switch (event.*) {
            // Single payload: CommitStage.Tag
            inline .replica_commit => |data| {
                const stage = index_from_enum(data.stage);
                assert(stage < slot_limits.get(event.*));

                return slot_bases.get(event.*) + stage;
            },
            // Single payload: Operation
            inline .replica_request,
            .replica_request_execute,
            .replica_request_local,
            .client_request_round_trip,
            => |data| {
                const operation = index_from_enum(data.operation);
                assert(operation < slot_limits.get(event.*));

                return slot_bases.get(event.*) + operation;
            },
            // Single payload: TreeEnum
            inline .compact_mutable,
            .compact_mutable_suffix,
            .lookup,
            .lookup_worker,
            .scan_tree,
            => |data| {
                const tree_id = index_from_enum(data.tree);
                assert(tree_id < slot_limits.get(event.*));

                return slot_bases.get(event.*) + tree_id;
            },
            inline .compact_beat, .compact_beat_merge => |data| {
                const tree_id = index_from_enum(data.tree);
                const offset = tree_id;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + offset;
            },
            inline .scan_tree_level => |data| {
                const tree_id = index_from_enum(data.tree);
                const offset = tree_id;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + offset;
            },
            inline .storage_read, .storage_write => |data| {
                const zone = index_from_enum(data.zone);
                const offset = zone;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + offset;
            },
            inline else => |data, event_tag| {
                comptime assert(@TypeOf(data) == void);
                comptime assert(slot_limits.get(event_tag) == 1);

                return comptime slot_bases.get(event_tag);
            },
        }
    }

    pub fn format(
        event: *const EventTiming,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        switch (event.*) {
            inline else => |data| {
                try format_data(data, writer);
            },
        }
    }
};

pub const EventTracing = union(Event.Tag) {
    replica_commit,
    replica_aof_write,
    replica_aof_checkpoint,
    replica_sync_table: struct { index: usize },
    replica_request,
    replica_request_execute,
    replica_request_local,

    compact_beat,
    compact_beat_merge,
    compact_manifest,
    compact_mutable,
    compact_mutable_suffix,

    lookup,
    lookup_worker: struct { index: u8 },

    scan_tree: struct { index: u8 },
    scan_tree_level: struct { index: u8, level: u8 },

    grid_read: struct { iop: usize },
    grid_write: struct { iop: usize },
    storage_read,
    storage_write,

    metrics_emit,

    client_request_round_trip,

    pub const stack_limits = std.enums.EnumArray(Event.Tag, u32).init(.{
        .replica_commit = 1,
        .replica_aof_write = 1,
        .replica_aof_checkpoint = 1,
        .replica_sync_table = constants.grid_missing_tables_max,
        .replica_request = 1,
        .replica_request_execute = 1,
        .replica_request_local = 1,
        .compact_beat = 1,
        .compact_beat_merge = 1,
        .compact_manifest = 1,
        .compact_mutable = 1,
        .compact_mutable_suffix = 1,
        .lookup = 1,
        .lookup_worker = constants.grid_iops_read_max,
        .scan_tree = constants.lsm_scans_max,
        .scan_tree_level = constants.lsm_scans_max * @as(u32, constants.lsm_levels),
        .grid_read = constants.grid_iops_read_max,
        .grid_write = constants.grid_iops_write_max,
        .storage_read = 1,
        .storage_write = 1,
        .metrics_emit = 1,
        .client_request_round_trip = 1,
    });

    pub const stack_bases = array: {
        var array = std.enums.EnumArray(Event.Tag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(Event.Tag)) |event_type| {
            array.set(event_type, next);
            next += stack_limits.get(event_type);
        }
        break :array array;
    };

    pub const stack_count = count: {
        var count: u32 = 0;
        for (std.enums.values(Event.Tag)) |event_type| {
            count += stack_limits.get(event_type);
        }
        break :count count;
    };

    // Stack is a u32 since it must be losslessly encoded as a JSON integer.
    pub fn stack(event: *const EventTracing) u32 {
        switch (event.*) {
            inline .replica_sync_table,
            .lookup_worker,
            => |data| {
                assert(data.index < stack_limits.get(event.*));
                const stack_base = stack_bases.get(event.*);
                return stack_base + @as(u32, @intCast(data.index));
            },
            .scan_tree => |data| {
                assert(data.index < constants.lsm_scans_max);
                // This event has "nested" sub-events, so its offset is calculated
                // with padding to accommodate `scan_tree_level` events in between.
                const stack_base = stack_bases.get(event.*);
                const scan_tree_offset = (constants.lsm_levels + 1) * data.index;
                return stack_base + scan_tree_offset;
            },
            .scan_tree_level => |data| {
                assert(data.index < constants.lsm_scans_max);
                assert(data.level < constants.lsm_levels);
                // This is a "nested" event, so its offset is calculated
                // relative to the parent `scan_tree`'s offset.
                const stack_base = stack_bases.get(.scan_tree);
                const scan_tree_offset = (constants.lsm_levels + 1) * data.index;
                const scan_tree_level_offset = data.level + 1;
                return stack_base + scan_tree_offset + scan_tree_level_offset;
            },
            inline .grid_read, .grid_write => |data| {
                assert(data.iop < stack_limits.get(event.*));
                const stack_base = stack_bases.get(event.*);
                return stack_base + @as(u32, @intCast(data.iop));
            },
            inline else => |data, event_tag| {
                comptime assert(@TypeOf(data) == void);
                comptime assert(stack_limits.get(event_tag) == 1);
                return comptime stack_bases.get(event_tag);
            },
        }
    }

    pub fn format(
        event: *const EventTracing,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        switch (event.*) {
            inline else => |data| {
                try format_data(data, writer);
            },
        }
    }
};

pub const EventMetric = union(enum) {
    const Tag = std.meta.Tag(EventMetric);

    table_count_visible: struct { tree: TreeEnum },
    table_count_visible_max: struct { tree: TreeEnum },
    replica_status,
    replica_view,
    replica_log_view,
    replica_op,
    replica_op_checkpoint,
    replica_commit_min,
    replica_commit_max,
    replica_pipeline_queue_length,
    replica_sync_stage,
    replica_sync_op_min,
    replica_sync_op_max,
    replica_messages_in: struct { command: Command },
    replica_messages_out: struct { command: Command },
    journal_dirty,
    journal_faulty,
    grid_blocks_acquired,
    grid_blocks_missing,
    grid_cache_hits,
    grid_cache_misses,
    lsm_nodes_free,
    lsm_manifest_block_count,
    metrics_statsd_packets,
    release,

    pub const slot_limits = std.enums.EnumArray(Tag, u32).init(.{
        .table_count_visible = enum_count(TreeEnum),
        .table_count_visible_max = enum_count(TreeEnum),
        .replica_status = 1,
        .replica_view = 1,
        .replica_log_view = 1,
        .replica_op = 1,
        .replica_op_checkpoint = 1,
        .replica_commit_min = 1,
        .replica_commit_max = 1,
        .replica_pipeline_queue_length = 1,
        .replica_sync_stage = 1,
        .replica_sync_op_min = 1,
        .replica_sync_op_max = 1,
        .replica_messages_in = enum_count(Command),
        .replica_messages_out = enum_count(Command),
        .journal_dirty = 1,
        .journal_faulty = 1,
        .grid_blocks_acquired = 1,
        .grid_blocks_missing = 1,
        .grid_cache_hits = 1,
        .grid_cache_misses = 1,
        .lsm_nodes_free = 1,
        .lsm_manifest_block_count = 1,
        .metrics_statsd_packets = 1,
        .release = 1,
    });

    pub const slot_bases = array: {
        var array = std.enums.EnumArray(Tag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(Tag)) |event_type| {
            array.set(event_type, next);
            next += slot_limits.get(event_type);
        }
        break :array array;
    };

    pub const slot_count = count: {
        var count: u32 = 0;
        for (std.enums.values(Tag)) |event_type| {
            count += slot_limits.get(event_type);
        }
        break :count count;
    };

    pub fn slot(event: *const EventMetric) u32 {
        switch (event.*) {
            inline .table_count_visible, .table_count_visible_max => |data| {
                const tree_id = index_from_enum(data.tree);
                const offset = tree_id;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + offset;
            },
            inline .replica_messages_in, .replica_messages_out => |data| {
                const command = index_from_enum(data.command);
                const offset = command;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + offset;
            },
            else => {
                return slot_bases.get(event.*);
            },
        }
    }
};

pub fn format_data(
    data: anytype,
    writer: anytype,
) !void {
    const Data = @TypeOf(data);
    if (Data == void) return;

    const fields = std.meta.fields(Data);
    inline for (fields, 0..) |data_field, i| {
        assert(data_field.type == bool or
            @typeInfo(data_field.type) == .int or
            @typeInfo(data_field.type) == .@"enum" or
            @typeInfo(data_field.type) == .@"union");

        const data_field_value = @field(data, data_field.name);
        try writer.writeAll(data_field.name);
        try writer.writeByte('=');

        if (@typeInfo(data_field.type) == .@"enum" or
            @typeInfo(data_field.type) == .@"union")
        {
            try writer.print("{s}", .{@tagName(data_field_value)});
        } else {
            try writer.print("{}", .{data_field_value});
        }

        if (i != fields.len - 1) {
            try writer.writeByte(' ');
        }
    }
}

pub const EventTimingAggregate = struct {
    event: EventTiming,
    values: struct {
        duration_min: Duration,
        duration_max: Duration,
        duration_sum: Duration,

        count: u64,
    },
};

pub const EventMetricAggregate = struct {
    pub const ValueType = u64;

    event: EventMetric,
    value: ValueType,
};

test "EventMetric slot doesn't have collisions" {
    const allocator = std.testing.allocator;
    var stacks: std.ArrayListUnmanaged(u32) = .{};
    defer stacks.deinit(allocator);
    var g: @import("../testing/exhaustigen.zig") = .{};
    while (!g.done()) {
        const event: EventMetric = switch (g.enum_value(EventMetric.Tag)) {
            .table_count_visible => .{ .table_count_visible = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .table_count_visible_max => .{ .table_count_visible_max = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .replica_messages_in => .{ .replica_messages_in = .{
                .command = g.enum_value(Command),
            } },
            .replica_messages_out => .{ .replica_messages_out = .{
                .command = g.enum_value(Command),
            } },
            inline else => |tag| tag,
        };
        try stacks.append(allocator, event.slot());
    }
    for (0..stacks.items.len) |i| {
        for (0..i) |j| {
            assert(stacks.items[i] != stacks.items[j]);
        }
    }
}

test "EventTiming slot doesn't have collisions" {
    const allocator = std.testing.allocator;
    var stacks: std.ArrayListUnmanaged(u32) = .{};
    defer stacks.deinit(allocator);
    var g: @import("../testing/exhaustigen.zig") = .{};
    while (!g.done()) {
        const event: EventTiming = switch (g.enum_value(Event.Tag)) {
            .replica_commit => .{ .replica_commit = .{ .stage = g.enum_value(CommitStage.Tag) } },
            .replica_aof_write => .replica_aof_write,
            .replica_aof_checkpoint => .replica_aof_checkpoint,
            .replica_sync_table => .replica_sync_table,
            .replica_request => .{ .replica_request = .{ .operation = g.enum_value(Operation) } },
            .replica_request_execute => .{ .replica_request_execute = .{
                .operation = g.enum_value(Operation),
            } },
            .replica_request_local => .{ .replica_request_local = .{
                .operation = g.enum_value(Operation),
            } },
            .compact_beat => .{ .compact_beat = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .compact_beat_merge => .{ .compact_beat_merge = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .compact_manifest => .compact_manifest,
            .compact_mutable => .{ .compact_mutable = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .compact_mutable_suffix => .{ .compact_mutable_suffix = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .lookup => .{ .lookup = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .lookup_worker => .{ .lookup_worker = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .scan_tree => .{ .scan_tree = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .scan_tree_level => .{ .scan_tree_level = .{
                .tree = g.enum_value(TreeEnum),
            } },
            .grid_read => .grid_read,
            .grid_write => .grid_write,
            .storage_read => .{ .storage_read = .{ .zone = g.enum_value(Zone) } },
            .storage_write => .{ .storage_write = .{ .zone = g.enum_value(Zone) } },
            .metrics_emit => .metrics_emit,
            .client_request_round_trip => .{ .client_request_round_trip = .{
                .operation = g.enum_value(Operation),
            } },
        };
        try stacks.append(allocator, event.slot());
    }
    for (0..stacks.items.len) |i| {
        for (0..i) |j| {
            assert(stacks.items[i] != stacks.items[j]);
        }
    }
}
