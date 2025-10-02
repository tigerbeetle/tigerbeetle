const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const ProcessID = @import("../trace.zig").ProcessID;
const IO = @import("../io.zig").IO;

const EventMetric = @import("event.zig").EventMetric;
const EventMetricAggregate = @import("event.zig").EventMetricAggregate;
const EventTiming = @import("event.zig").EventTiming;
const EventTimingAggregate = @import("event.zig").EventTimingAggregate;

const log = std.log.scoped(.statsd);

/// A reasonable value to keep the total length of the packet under a single MTU, for a local
/// network.
///
/// https://github.com/statsd/statsd/blob/master/docs/metric_types.md#multi-metric-packets
const packet_size_max = 1400;

/// No single metric may be larger than this value. If it is, it'll be dropped with an error
/// message. Since this is calculated at comptime, that means there's a bug in the calculation
/// logic.
const statsd_line_size_max = line_size_max: {
    // For each type of event, build a payload containing the maximum possible values for that
    // event. This is essentially maxInt for integer payloads, and the longest enum tag name for
    // enum payloads.
    var events_metric: [std.meta.fieldNames(EventMetric).len]EventMetricAggregate = undefined;
    for (&events_metric, std.meta.fields(EventMetric)) |*event_metric, EventMetricInner| {
        event_metric.* = .{
            .event = @unionInit(
                EventMetric,
                EventMetricInner.name,
                struct_size_max(EventMetricInner.type),
            ),
            .value = std.math.maxInt(EventMetricAggregate.ValueType),
        };
    }

    var events_timing: [std.meta.fieldNames(EventTiming).len]EventTimingAggregate = undefined;
    for (&events_timing, std.meta.fields(EventTiming)) |*event_timing, EventTimingInner| {
        event_timing.* = .{
            .event = @unionInit(
                EventTiming,
                EventTimingInner.name,
                struct_size_max(EventTimingInner.type),
            ),
            .values = .{
                .duration_min = .{ .ns = std.math.maxInt(u64) },
                .duration_max = .{ .ns = std.math.maxInt(u64) },
                .duration_sum = .{ .ns = std.math.maxInt(u64) },
                .count = std.math.maxInt(u64),
            },
        };
    }

    var buffer: [packet_size_max]u8 = undefined;
    var buffer_stream = std.io.fixedBufferStream(&buffer);
    const buffer_writer = buffer_stream.writer();

    var line_size_max: u32 = 0;
    for (events_metric) |event| {
        buffer_stream.reset();
        format_metric(
            buffer_writer,
            .{ .metric = .{ .aggregate = event } },
            .{ .cluster = std.math.maxInt(u128), .replica = constants.members_max - 1 },
        ) catch unreachable;
        line_size_max = @max(line_size_max, buffer_stream.getPos() catch unreachable);
    }
    for (events_timing) |event| {
        for (std.enums.values(TimingStat)) |stat| {
            buffer_stream.reset();
            format_metric(
                buffer_writer,
                .{ .timing = .{ .aggregate = event, .stat = stat } },
                .{ .cluster = std.math.maxInt(u128), .replica = constants.members_max - 1 },
            ) catch unreachable;
            line_size_max = @max(line_size_max, buffer_stream.getPos() catch unreachable);
        }
    }
    break :line_size_max line_size_max;
};

const packet_messages_max = @divFloor(packet_size_max, statsd_line_size_max);

comptime {
    assert(statsd_line_size_max <= packet_size_max);
    assert(packet_messages_max > 0);
}

/// This implementation emits on an open-loop: on the emit interval, it fires off up to
/// packet_count_max UDP packets, without waiting for completions.
///
/// The emit interval needs to be large enough that the kernel will have finished processing them
/// before emitting again. If not, an error will be logged.
const packet_count_max = stdx.div_ceil(
    EventMetric.slot_count + (EventTiming.slot_count * std.enums.values(TimingStat).len),
    packet_messages_max,
);

comptime {
    // Sanity-check:
    assert(packet_count_max > 0);
    assert(packet_count_max < 256);
}

pub const StatsD = struct {
    process_id: ProcessID,
    implementation: union(enum) {
        udp: struct {
            socket: std.posix.socket_t,
            io: *IO,
            send_callback_error_count: u64 = 0,
        },
        log,
    },

    send_buffer: *[packet_count_max * packet_size_max]u8,
    send_completions: [packet_count_max]IO.Completion = undefined,
    send_in_flight_count: u32 = 0,

    /// Creates a statsd instance, which will send UDP packets via the IO instance provided.
    pub fn init_udp(
        allocator: std.mem.Allocator,
        process_id: ProcessID,
        io: *IO,
        address: std.net.Address,
    ) !StatsD {
        const socket = try io.open_socket_udp(address.any.family);
        errdefer io.close_socket(socket);

        const send_buffer = try allocator.create([packet_count_max * packet_size_max]u8);
        errdefer allocator.destroy(send_buffer);

        // 'Connect' the UDP socket, so we can just send() to it normally.
        try std.posix.connect(socket, &address.any, address.getOsSockLen());

        log.info("{}: sending statsd metrics to {}", .{ process_id, address });

        return .{
            .process_id = process_id,
            .implementation = .{
                .udp = .{
                    .socket = socket,
                    .io = io,
                },
            },
            .send_buffer = send_buffer,
        };
    }

    // Creates a statsd instance, which will log out the packets that would have been sent. Useful
    // so that all of the other code can run and be tested in the simulator.
    pub fn init_log(
        allocator: std.mem.Allocator,
        process_id: ProcessID,
    ) !StatsD {
        const send_buffer = try allocator.create([packet_count_max * packet_size_max]u8);
        errdefer allocator.destroy(send_buffer);

        return .{
            .process_id = process_id,
            .implementation = .log,
            .send_buffer = send_buffer,
        };
    }

    pub fn deinit(self: *StatsD, allocator: std.mem.Allocator) void {
        if (self.implementation == .udp) {
            self.implementation.udp.io.close_socket(self.implementation.udp.socket);
        }
        allocator.destroy(self.send_buffer);

        self.* = undefined;
    }

    pub fn emit(
        self: *StatsD,
        events_metric: []const ?EventMetricAggregate,
        events_timing: []const ?EventTimingAggregate,
    ) error{ Busy, UnknownProcess }!u32 {
        const cluster, const replica = switch (self.process_id) {
            .unknown => {
                log.err("{}: process id unknown; skipping emit", .{self.process_id});
                return error.UnknownProcess;
            },
            .replica => |replica| .{ replica.cluster, replica.replica },
        };

        // This really should not happen; it means we're emitting so many packets, on a short
        // enough emit timeout, that the kernel hasn't been able to process them all (UDP doesn't
        // block or provide back-pressure like a TCP socket).
        //
        // Keep it as a log, rather than assert, to avoid the common pitfall of metrics killing
        // the whole system.
        //
        // This is also a load-bearing check: see send_callback().
        if (self.send_in_flight_count != 0) {
            log.err("{}: {} / {} packets still in flight; skipping emit", .{
                self.process_id,
                self.send_in_flight_count,
                packet_count_max,
            });
            return error.Busy;
        }

        if (self.implementation == .udp and self.implementation.udp.send_callback_error_count > 0) {
            log.warn(
                "{}: failed to send {} packets",
                .{ self.process_id, self.implementation.udp.send_callback_error_count },
            );
            self.implementation.udp.send_callback_error_count = 0;
        }

        var send_ready: u32 = 0;
        var send_sizes = stdx.BoundedArrayType(u32, packet_count_max){};
        var send_stream = std.io.fixedBufferStream(self.send_buffer);
        const send_writer = send_stream.writer();
        inline for (.{ events_metric, events_timing }) |events| {
            for (events) |event_new_maybe| {
                const event_new = event_new_maybe orelse continue;
                const stats = switch (@TypeOf(event_new)) {
                    EventMetricAggregate => [_]Stat{.{ .metric = .{ .aggregate = event_new } }},
                    EventTimingAggregate => [_]Stat{
                        .{ .timing = .{ .aggregate = event_new, .stat = .min } },
                        .{ .timing = .{ .aggregate = event_new, .stat = .max } },
                        .{ .timing = .{ .aggregate = event_new, .stat = .avg } },
                        .{ .timing = .{ .aggregate = event_new, .stat = .sum } },
                        .{ .timing = .{ .aggregate = event_new, .stat = .count } },
                    },
                    else => unreachable,
                };

                for (stats) |stat| {
                    const send_position_before = send_stream.getPos() catch unreachable;
                    format_metric(send_writer, stat, .{
                        .cluster = cluster,
                        .replica = replica,
                    }) catch |err| switch (err) {
                        // This shouldn't ever happen, but don't allow metrics to kill the system.
                        error.NoSpaceLeft => {
                            log.err("{}: insufficient buffer space", .{self.process_id});
                            break;
                        },
                    };

                    const send_position_after = send_stream.getPos() catch unreachable;
                    const send_size: u32 = @intCast(send_position_after - send_position_before);
                    assert(send_size > 0);
                    if (send_ready + send_size > packet_size_max) {
                        assert(send_ready > 0);
                        if (send_sizes.full()) {
                            log.err("{}: insufficient packet count", .{self.process_id});
                            break;
                        } else {
                            send_sizes.push(send_ready);
                        }
                        send_ready = send_size;
                    } else {
                        send_ready += send_size;
                    }
                }
            }
        }
        if (send_ready > 0) {
            if (send_sizes.full()) {
                log.err("{}: insufficient packet count", .{self.process_id});
            } else {
                send_sizes.push(send_ready);
            }
        }

        var send_offset: u32 = 0;
        for (send_sizes.const_slice()) |send_size| {
            if (self.send_in_flight_count >= self.send_completions.len) {
                // This shouldn't ever happen, but don't allow metrics to kill the system.
                log.err("{}: insufficient packets to emit any metrics", .{self.process_id});
                return 0;
            }
            const completion = &self.send_completions[self.send_in_flight_count];
            self.send_in_flight_count += 1;
            self.emit_buffer(completion, self.send_buffer[send_offset..][0..send_size]);
            send_offset += send_size;
        }

        return @intCast(send_sizes.count());
    }

    fn emit_buffer(self: *StatsD, send_completion: *IO.Completion, send_buffer: []const u8) void {
        switch (self.implementation) {
            .udp => |udp| {
                udp.io.send(
                    *StatsD,
                    self,
                    StatsD.send_callback,
                    send_completion,
                    udp.socket,
                    send_buffer,
                );
            },
            .log => {
                log.debug("{}: statsd packet: {s}", .{ self.process_id, send_buffer });
                StatsD.send_callback(self, send_completion, send_buffer.len);
            },
        }
    }

    /// The UDP packets containing the metrics are sent in a fire-and-forget manner.
    fn send_callback(self: *StatsD, completion: *IO.Completion, result: IO.SendError!usize) void {
        _ = result catch {
            // Errors are only supported when using UDP; not if calling this loopback.
            assert(self.implementation == .udp);
            self.implementation.udp.send_callback_error_count += 1;
        };

        // Completions can be returned in any order: this is _only_ safe because their emitting is
        // guarded on `self.send_in_flight_count != 0`.
        self.send_in_flight_count -= 1;
        completion.* = undefined;
    }
};

const TimingStat = enum { min, max, avg, sum, count };
const Stat = union(enum) {
    metric: struct { aggregate: EventMetricAggregate },
    timing: struct { aggregate: EventTimingAggregate, stat: TimingStat },
};

fn format_metric(
    writer: anytype,
    stat: Stat,
    options: struct { cluster: u128, replica: u8 },
) error{NoSpaceLeft}!void {
    const stat_name = switch (stat) {
        inline else => |stat_data| @tagName(stat_data.aggregate.event),
    };

    const stat_suffix, const stat_type, const stat_value = switch (stat) {
        .metric => |data| .{ "", "g", data.aggregate.value },
        .timing => |data| switch (data.stat) {
            .count => .{ "_us.count", "c", data.aggregate.values.count },
            .sum => .{ "_us.sum", "c", data.aggregate.values.duration_sum.to_us() },
            .min => .{ "_us.min", "g", data.aggregate.values.duration_min.to_us() },
            .max => .{ "_us.max", "g", data.aggregate.values.duration_max.to_us() },
            .avg => .{ "_us.avg", "g", @divFloor(
                data.aggregate.values.duration_sum.to_us(),
                data.aggregate.values.count,
            ) },
        },
    };

    try writer.print("tb.{[name]s}{[name_suffix]s}:{[value]d}|{[statsd_type]s}" ++
        "|#cluster:{[cluster]x:0>32},replica:{[replica]d}", .{
        .name = stat_name,
        .name_suffix = stat_suffix,
        .statsd_type = stat_type,
        .value = stat_value,
        .cluster = options.cluster,
        .replica = options.replica,
    });

    switch (stat) {
        inline else => |stat_data| {
            switch (stat_data.aggregate.event) {
                inline else => |data| {
                    const Tags = @TypeOf(data);
                    if (@typeInfo(Tags) == .@"struct") {
                        const fields = std.meta.fields(@TypeOf(data));
                        inline for (fields) |data_field| {
                            comptime assert(!std.mem.eql(u8, data_field.name, "cluster"));
                            comptime assert(!std.mem.eql(u8, data_field.name, "replica"));
                            comptime assert(@typeInfo(data_field.type) == .int or
                                @typeInfo(data_field.type) == .@"enum" or
                                @typeInfo(data_field.type) == .@"union");

                            const data_field_value = @field(data, data_field.name);
                            try writer.writeByte(',');
                            try writer.writeAll(data_field.name);
                            try writer.writeByte(':');

                            if (@typeInfo(data_field.type) == .@"enum" or
                                @typeInfo(data_field.type) == .@"union")
                            {
                                try writer.print("{s}", .{@tagName(data_field_value)});
                            } else {
                                try writer.print("{}", .{data_field_value});
                            }
                        }
                    } else {
                        assert(@TypeOf(data) == void);
                    }
                },
            }
        },
    }
    try writer.writeByte('\n');
}

/// Returns an instance of a Struct (or void) with all fields set to what would result in the
/// longest length when formatted.
///
/// Integers get maxInt, and Enums get a value corresponding to `enum_size_max()`.
fn struct_size_max(StructOrVoid: type) StructOrVoid {
    if (@typeInfo(StructOrVoid) == .void) return {};

    assert(@typeInfo(StructOrVoid) == .@"struct");
    const Struct = StructOrVoid;

    var output: Struct = undefined;

    for (std.meta.fields(Struct)) |field| {
        const type_info = @typeInfo(field.type);
        assert(type_info == .int or type_info == .@"enum");
        assert(type_info != .int or type_info.Int.signedness == .unsigned);
        switch (type_info) {
            .int => @field(output, field.name) = std.math.maxInt(field.type),
            .@"enum" => @field(output, field.name) =
                std.enums.nameCast(field.type, enum_size_max(field.type)),
            else => @compileError("unsupported type"),
        }
    }

    return output;
}

/// Returns the longest @tagName for a given Enum.
fn enum_size_max(Enum: type) []const u8 {
    @setEvalBranchQuota(10_000);
    var tag_longest: []const u8 = "";
    for (std.meta.fieldNames(Enum)) |field_name| {
        if (tag_longest.len < field_name.len) {
            tag_longest = field_name;
        }
    }
    return tag_longest;
}
