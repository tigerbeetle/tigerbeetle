//! The tracer records a tree of event spans.
//!
//! In order to create event spans, you need somewhere to store the `SpanStart`.
//!
//!     var slot: ?SpanStart = null;
//!     tracer.start(&slot, group, event);
//!     ... do stuff ...
//!     tracer.end(&slot, group, event);
//!
//! Each slot can be used as many times as you like,
//! but you must alternate calls to start and end,
//! and you must end every event.
//!
//!     // good
//!     tracer.start(&slot, group_a, event_a);
//!     tracer.end(&slot, group_a, event_a);
//!     tracer.start(&slot, group_b, event_b);
//!     tracer.end(&slot, group_b, event_b);
//!
//!     // bad
//!     tracer.start(&slot, group_a, event_a);
//!     tracer.start(&slot, group_b, event_b);
//!     tracer.end(&slot, group_b, event_b);
//!     tracer.end(&slot, group_a, event_a);
//!
//!     // bad
//!     tracer.end(&slot, group_a, event_a);
//!     tracer.start(&slot, group_a, event_a);
//!
//!     // bad
//!     tracer.start(&slot, group_a, event_a);
//!     std.os.exit(0);
//!
//! Before freeing a slot, you should `assert(slot == null)`
//! to ensure that you didn't forget to end an event.
//!
//! Each `Event` has an `EventGroup`.
//! Within each group, event spans should form a tree.
//!
//!     // good
//!     tracer.start(&a, group, ...);
//!     tracer.start(&b, group, ...);
//!     tracer.end(&b, group, ...);
//!     tracer.end(&a, group, ...);
//!
//!     // bad
//!     tracer.start(&a, group, ...);
//!     tracer.start(&b, group, ...);
//!     tracer.end(&a, group, ...);
//!     tracer.end(&b, group, ...);
//!
//! The tracer itself will not object to non-tree spans,
//! but some config.tracer_backends will either refuse to open the trace or will render it weirdly.
//!
//! If you're having trouble making your spans form a tree, feel free to just add new groups.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.tracer);

const config = @import("./config.zig");
const Time = @import("./time.zig").Time;

var is_initialized = false;
var timer = Time{};
var span_id_next: u64 = 0;
var spans: std.ArrayList(Span) = undefined;
var flush_slot: ?SpanStart = null;
var log_file: std.fs.File = undefined;

const span_count_max = 1 << 20;
const log_path = "./tracer.json";

/// All strings in Event must be comptime constants to ensure that they live until after `tracer.deinit` is called.
pub const Event = union(enum) {
    tracer_flush,
    commit: struct {
        op: u64,
    },
    checkpoint,
    state_machine_prefetch,
    state_machine_commit,
    state_machine_compact,
    tree_compaction_beat: struct {
        tree_name: []const u8,
    },
    tree_compaction_tick: struct {
        tree_name: []const u8,
        level_b: u8,
    },
    tree_compaction_merge: struct {
        tree_name: []const u8,
        level_b: u8,
    },
};

/// All strings in EventGroup must be comptime constants to ensure that they live until after `tracer.deinit` is called.
pub const EventGroup = union(enum) {
    main,
    tracer,
    tree: struct {
        tree_name: []const u8,
    },
};

const SpanId = u64;

pub const SpanStart = struct {
    id: SpanId,
    start_time_ns: u64,
    group: EventGroup,
    event: Event,
};

const Span = struct {
    id: SpanId,
    start_time_ns: u64,
    end_time_ns: u64,
    group: EventGroup,
    event: Event,
};

pub fn init(allocator: Allocator) !void {
    if (config.tracer_backend == .none) return;
    assert(!is_initialized);

    spans = try std.ArrayList(Span).initCapacity(allocator, span_count_max);
    errdefer spans.deinit();

    switch (config.tracer_backend) {
        .none => unreachable,
        .perfetto => {
            log_file = try std.fs.cwd().createFile(log_path, .{ .truncate = true });
            errdefer log_file.close();

            try log_file.writeAll(
                \\{"traceEvents":[
                \\
            );
        },
    }

    is_initialized = true;
}

pub fn deinit(allocator: Allocator) void {
    _ = allocator;

    if (config.tracer_backend == .none) return;
    assert(is_initialized);

    flush();
    log_file.close();
    assert(flush_slot == null);
    spans.deinit();
    is_initialized = false;
}

pub fn start(slot: *?SpanStart, event_group: EventGroup, event: Event) void {
    if (config.tracer_backend == .none) return;
    assert(is_initialized);

    // The event must not have already been started.
    assert(slot.* == null);

    slot.* = .{
        .id = span_id_next,
        .start_time_ns = timer.monotonic(),
        .group = event_group,
        .event = event,
    };
    span_id_next += 1;
}

pub fn end(slot: *?SpanStart, event_group: EventGroup, event: Event) void {
    if (config.tracer_backend == .none) return;
    assert(is_initialized);

    // The event must have already been started.
    const span_start = &slot.*.?;
    assert(std.meta.eql(span_start.group, event_group));
    assert(std.meta.eql(span_start.event, event));

    // Make sure we have room in spans.
    if (spans.items.len >= span_count_max) flush();
    assert(spans.items.len < span_count_max);

    spans.appendAssumeCapacity(.{
        .id = span_start.id,
        .start_time_ns = span_start.start_time_ns,
        .end_time_ns = timer.monotonic(),
        .group = span_start.group,
        .event = span_start.event,
    });
    slot.* = null;
}

pub fn flush() void {
    if (config.tracer_backend == .none) return;
    assert(is_initialized);

    if (spans.items.len == 0) return;

    start(&flush_slot, .tracer, .tracer_flush);
    flush_or_err() catch |err| {
        log.err("Could not flush tracer log, discarding instead: {}", .{err});
    };
    spans.shrinkRetainingCapacity(0);
    end(&flush_slot, .tracer, .tracer_flush);
}

fn flush_or_err() !void {
    switch (config.tracer_backend) {
        .none => unreachable,
        .perfetto => {
            for (spans.items) |span| {
                // Perfetto requires this json format:
                // https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
                const NameJson = struct {
                    event: Event,

                    pub fn jsonStringify(
                        name_json: @This(),
                        options: std.json.StringifyOptions,
                        writer: anytype,
                    ) @TypeOf(writer).Error!void {
                        _ = options;
                        try writer.writeAll("\"");
                        switch (name_json.event) {
                            .tracer_flush => try writer.print("tracer_flush", .{}),
                            .commit => |commit| try writer.print(
                                "commit({})",
                                .{commit.op},
                            ),
                            .checkpoint => try writer.print("checkpoint", .{}),
                            .state_machine_prefetch => try writer.print("state_machine_prefetch", .{}),
                            .state_machine_commit => try writer.print("state_machine_commit", .{}),
                            .state_machine_compact => try writer.print("state_machine_compact", .{}),
                            .tree_compaction_beat => |tree_compaction_tick| try writer.print(
                                "tree_compaction_beat({s})",
                                .{tree_compaction_tick.tree_name},
                            ),
                            .tree_compaction_tick => |tree_compaction_tick| try writer.print(
                                "tree_compaction_tick({s}, {})",
                                .{ tree_compaction_tick.tree_name, tree_compaction_tick.level_b },
                            ),
                            .tree_compaction_merge => |tree_compaction_merge| try writer.print(
                                "tree_compaction_merge({s}, {})",
                                .{ tree_compaction_merge.tree_name, tree_compaction_merge.level_b },
                            ),
                        }
                        try writer.writeAll("\"");
                    }
                };
                const SpanJson = struct {
                    name: NameJson,
                    cat: []const u8 = "default",
                    ph: []const u8 = "X",
                    ts: f64,
                    dur: f64,
                    pid: u64 = 0,
                    tid: u64,
                };
                const MetaJson = struct {
                    name: []const u8,
                    ph: []const u8 = "M",
                    pid: u64 = 0,
                    tid: u64,
                    args: struct {
                        name: []const u8,
                    },
                };

                const group_name = switch (span.group) {
                    .main => "main",
                    .tracer => "tracer",
                    .tree => |tree| tree.tree_name,
                };

                const tid_64 = switch (span.group) {
                    .main => 0,
                    .tracer => 1,
                    .tree => |tree| std.hash_map.hashString(tree.tree_name),
                };
                const tid = @truncate(u32, tid_64) ^ @truncate(u32, tid_64 >> 32);

                var buffered_writer = std.io.bufferedWriter(log_file.writer());
                const writer = buffered_writer.writer();

                try std.json.stringify(
                    SpanJson{
                        .name = .{ .event = span.event },
                        .ts = @intToFloat(f64, span.start_time_ns) / 1000,
                        .dur = @intToFloat(f64, span.end_time_ns - span.start_time_ns) / 1000,
                        .tid = tid,
                    },
                    .{},
                    writer,
                );
                try writer.writeAll(",\n");

                // TODO Only emit metadata once per group name.
                try std.json.stringify(
                    MetaJson{
                        .name = "thread_name",
                        .tid = tid,
                        .args = .{ .name = group_name },
                    },
                    .{},
                    writer,
                );
                try writer.writeAll(",\n");

                try buffered_writer.flush();
            }
        },
    }
}
