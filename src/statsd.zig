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
        };

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
    }

    pub fn gauge(self: *StatsD, stat: []const u8, value: usize) !void {
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
