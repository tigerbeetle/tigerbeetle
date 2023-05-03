const std = @import("std");

const IO = @import("io.zig").IO;
const FIFO = @import("fifo.zig").FIFO;

const BufferCompletion = struct {
    next: ?*BufferCompletion = null,
    buffer: [256]u8,
    completion: IO.Completion = undefined,
};

pub const StatsD = struct {
    socket: std.os.socket_t,
    io: *IO,
    buffer_completions: []BufferCompletion,
    buffer_completions_fifo: FIFO(BufferCompletion) = .{ .name = "statsd" },

    counters: std.StringHashMap(i64) = undefined,
    emitting: bool = false,

    /// Creates a statsd instance, which will send UDP packets via the IO instance provided.
    pub fn init(allocator: std.mem.Allocator, io: *IO, address: std.net.Address) !StatsD {
        const socket = try io.open_socket(
            address.any.family,
            std.os.SOCK.DGRAM,
            std.os.IPPROTO.UDP,
        );
        errdefer std.os.closeSocket(socket);

        const buffer_completions = try allocator.alloc(BufferCompletion, 256);
        errdefer allocator.free(buffer_completions);

        var statsd = StatsD{
            .socket = socket,
            .io = io,
            .buffer_completions = buffer_completions,
            .counters = std.StringHashMap(i64).init(allocator),
        };

        errdefer statsd.counters.deinit();

        // Max number of internal counters
        try statsd.counters.ensureTotalCapacity(128);

        for (buffer_completions) |*buffer_completion| {
            buffer_completion.next = null;
            statsd.buffer_completions_fifo.push(buffer_completion);
        }

        // 'Connect' the UDP socket, so we can just send() to it normally.
        try std.os.connect(socket, &address.any, address.getOsSockLen());

        return statsd;
    }

    pub fn deinit(self: *StatsD, allocator: std.mem.Allocator) void {
        std.os.closeSocket(self.socket);
        allocator.free(self.buffer_completions);
        self.counters.deinit();
    }

    pub fn gauge(self: *StatsD, stat: []const u8, value: i64) !void {
        var buffer_completion = self.buffer_completions_fifo.pop() orelse return error.NoSpaceLeft;
        const statsd_packet = try std.fmt.bufPrint(buffer_completion.buffer[0..], "{s}:{}|g", .{ stat, value });

        self.io.send(
            *StatsD,
            self,
            StatsD.send_callback,
            &buffer_completion.completion,
            self.socket,
            statsd_packet,
        );
    }

    // TODO: Figure out a better API for this
    pub fn internal_counter(self: *StatsD, stat: []const u8, value: i64) !void {
        var v = try self.counters.getOrPut(stat);

        if (!v.found_existing) {
            v.value_ptr.* = value;
        } else {
            v.value_ptr.* += value;
        }
    }

    pub fn emit_internal_counters(self: *StatsD) !void {
        var it = self.counters.iterator();

        while (it.next()) |counter| {
            try self.gauge(counter.key_ptr.*, counter.value_ptr.*);
        }

        var buffer_completion = self.buffer_completions_fifo.pop() orelse return error.NoSpaceLeft;

        self.io.timeout(
            *StatsD,
            self,
            emit_internal_counters_callback,
            &buffer_completion.completion,
            100 * std.time.ns_per_ms,
        );

    }

    pub fn emit_internal_counters_callback(self: *StatsD, completion: *IO.Completion, result: IO.TimeoutError!void) void {
        _ = result catch {};
        var buffer_completion = @fieldParentPtr(BufferCompletion, "completion", completion);
        self.buffer_completions_fifo.push(buffer_completion);

        self.emit_internal_counters() catch {
            std.log.err("error flushing internal counters", .{});
        };
    }

    pub fn timing(self: *StatsD, stat: []const u8, ms: usize) !void {
        var buffer_completion = self.buffer_completions_fifo.pop() orelse return error.NoSpaceLeft;
        const statsd_packet = try std.fmt.bufPrint(buffer_completion.buffer[0..], "{s}:{}|ms", .{ stat, ms });

        self.io.send(
            *StatsD,
            self,
            StatsD.send_callback,
            &buffer_completion.completion,
            self.socket,
            statsd_packet,
        );
    }

    pub fn send_callback(context: *StatsD, completion: *IO.Completion, result: IO.SendError!usize) void {
        _ = result catch {};
        var buffer_completion = @fieldParentPtr(BufferCompletion, "completion", completion);
        context.buffer_completions_fifo.push(buffer_completion);
    }
};
