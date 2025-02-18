const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const CommitStage = @import("../vsr/replica.zig").CommitStage;

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

    break :tree_enum @Type(.{ .Enum = .{
        .tag_type = u64,
        .fields = tree_fields,
        .decls = &.{},
        .is_exhaustive = true,
    } });
};

/// Returns the minimum length of an array which can be indexed by the values of every enum variant.
fn enum_max(EnumOrUnion: type) u8 {
    const type_info = @typeInfo(EnumOrUnion);
    assert(type_info == .Enum or type_info == .Union);

    const Enum = if (type_info == .Enum)
        type_info.Enum
    else
        @typeInfo(type_info.Union.tag_type.?).Enum;
    assert(Enum.is_exhaustive);

    var max: u8 = Enum.fields[0].value;
    for (Enum.fields[1..]) |field| {
        max = @max(max, field.value);
    }
    return max + 1;
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
    replica_sync_table: struct { index: usize },

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

    metrics_emit: void,

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
        return switch (event.*) {
            inline else => |source_payload, tag| {
                const TargetPayload = std.meta.fieldInfo(EventType, tag).type;
                const target_payload_info = @typeInfo(TargetPayload);
                assert(target_payload_info == .Void or target_payload_info == .Struct);

                const target_payload: TargetPayload = switch (@typeInfo(TargetPayload)) {
                    .Void => {},
                    .Struct => blk: {
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
    replica_sync_table,

    compact_beat: struct { tree: TreeEnum, level_b: u8 },
    compact_beat_merge: struct { tree: TreeEnum, level_b: u8 },
    compact_manifest,
    compact_mutable: struct { tree: TreeEnum },
    compact_mutable_suffix: struct { tree: TreeEnum },

    lookup: struct { tree: TreeEnum },
    lookup_worker: struct { tree: TreeEnum },

    scan_tree: struct { tree: TreeEnum },
    scan_tree_level: struct { tree: TreeEnum, level: u8 },

    grid_read,
    grid_write,

    metrics_emit,

    pub const slot_limits = std.enums.EnumArray(Event.Tag, u32).init(.{
        .replica_commit = enum_max(CommitStage.Tag),
        .replica_aof_write = 1,
        .replica_sync_table = 1,
        .compact_beat = enum_max(TreeEnum) * @as(u32, constants.lsm_levels),
        .compact_beat_merge = enum_max(TreeEnum) * @as(u32, constants.lsm_levels),
        .compact_manifest = 1,
        .compact_mutable = enum_max(TreeEnum),
        .compact_mutable_suffix = enum_max(TreeEnum),
        .lookup = enum_max(TreeEnum),
        .lookup_worker = enum_max(TreeEnum),
        .scan_tree = enum_max(TreeEnum),
        .scan_tree_level = enum_max(TreeEnum) * @as(u32, constants.lsm_levels),
        .grid_read = 1,
        .grid_write = 1,
        .metrics_emit = 1,
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
    //
    // TODO: The enum logic is a bit wasteful. It considers the max value, so for enums that don't
    // start at 0 (eg, the trees) it's not optimal.
    pub fn slot(event: *const EventTiming) u32 {
        switch (event.*) {
            // Single payload: CommitStage.Tag
            inline .replica_commit => |data| {
                const stage = @intFromEnum(data.stage);
                assert(stage < slot_limits.get(event.*));

                return slot_bases.get(event.*) + @as(u32, @intCast(stage));
            },
            // Single payload: TreeEnum
            inline .compact_mutable,
            .compact_mutable_suffix,
            .lookup,
            .lookup_worker,
            .scan_tree,
            => |data| {
                const tree_id = @intFromEnum(data.tree);
                assert(tree_id < slot_limits.get(event.*));

                return slot_bases.get(event.*) + @as(u32, @intCast(tree_id));
            },
            // Double payload: TreeEnum + level_b
            inline .compact_beat, .compact_beat_merge => |data| {
                const tree_id = @intFromEnum(data.tree);
                const level_b = data.level_b;
                const offset = tree_id * @as(u32, constants.lsm_levels) + level_b;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + @as(u32, @intCast(offset));
            },
            // Double payload: TreeEnum + level
            inline .scan_tree_level => |data| {
                const tree_id = @intFromEnum(data.tree);
                const level = data.level;
                const offset = tree_id * @as(u32, constants.lsm_levels) + level;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + @as(u32, @intCast(offset));
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
    replica_sync_table: struct { index: usize },

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

    metrics_emit,

    pub const stack_limits = std.enums.EnumArray(Event.Tag, u32).init(.{
        .replica_commit = 1,
        .replica_aof_write = 1,
        .replica_sync_table = constants.grid_missing_tables_max,
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
        .metrics_emit = 1,
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

    table_count_visible: struct { tree: TreeEnum, level: u8 },
    table_count_visible_max: struct { tree: TreeEnum, level: u8 },

    pub const slot_limits = std.enums.EnumArray(Tag, u32).init(.{
        .table_count_visible = enum_max(TreeEnum) * @as(u32, constants.lsm_levels),
        .table_count_visible_max = enum_max(TreeEnum) * @as(u32, constants.lsm_levels),
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
            // Double payload: TreeEnum + level
            inline .table_count_visible, .table_count_visible_max => |data| {
                const tree_id = @intFromEnum(data.tree);
                const level = data.level;
                const offset = tree_id * @as(u32, constants.lsm_levels) + level;
                assert(offset < slot_limits.get(event.*));

                return slot_bases.get(event.*) + @as(u32, @intCast(offset));
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
            @typeInfo(data_field.type) == .Int or
            @typeInfo(data_field.type) == .Enum or
            @typeInfo(data_field.type) == .Union);

        const data_field_value = @field(data, data_field.name);
        try writer.writeAll(data_field.name);
        try writer.writeByte('=');

        if (@typeInfo(data_field.type) == .Enum or
            @typeInfo(data_field.type) == .Union)
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
    pub const ValueType = u64;

    event: EventTiming,
    values: struct {
        duration_min_us: ValueType,
        duration_max_us: ValueType,
        duration_sum_us: ValueType,

        count: ValueType,
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
                .level = g.int_inclusive(u6, constants.lsm_levels - 1),
            } },
            .table_count_visible_max => .{ .table_count_visible_max = .{
                .tree = g.enum_value(TreeEnum),
                .level = g.int_inclusive(u6, constants.lsm_levels - 1),
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

test "EventTiming slot doesn't have collisions" {
    const allocator = std.testing.allocator;
    var stacks: std.ArrayListUnmanaged(u32) = .{};
    defer stacks.deinit(allocator);
    var g: @import("../testing/exhaustigen.zig") = .{};
    while (!g.done()) {
        const event: EventTiming = switch (g.enum_value(Event.Tag)) {
            .replica_commit => .{ .replica_commit = .{ .stage = g.enum_value(CommitStage.Tag) } },
            .replica_aof_write => .replica_aof_write,
            .replica_sync_table => .replica_sync_table,
            .compact_beat => .{ .compact_beat = .{
                .tree = g.enum_value(TreeEnum),
                .level_b = g.int_inclusive(u6, constants.lsm_levels - 1),
            } },
            .compact_beat_merge => .{ .compact_beat_merge = .{
                .tree = g.enum_value(TreeEnum),
                .level_b = g.int_inclusive(u6, constants.lsm_levels - 1),
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
                .level = g.int_inclusive(u6, constants.lsm_levels - 1),
            } },
            .grid_read => .grid_read,
            .grid_write => .grid_write,
            .metrics_emit => .metrics_emit,
        };
        try stacks.append(allocator, event.slot());
    }
    for (0..stacks.items.len) |i| {
        for (0..i) |j| {
            assert(stacks.items[i] != stacks.items[j]);
        }
    }
}
