const std = @import("std");

const IO = @import("io.zig").IO;
const FIFO = @import("fifo.zig").FIFO;

pub const StatsD = struct {
    buffer: []u8,
    socket: std.os.socket_t,
    io: *IO,
    completions: []IO.Completion,
    completions_fifo: FIFO(IO.Completion) = .{ .name = "statsd" },

    /// Creates a statsd instance, which will send UDP packets via the IO instance provided.
    /// Not thread safe, since it uses a single buffer shared between gauge and timing methods
    /// with no locking.
    pub fn init(allocator: std.mem.Allocator, io: *IO, address: std.net.Address) !StatsD {
        // Limit the max packet size to 1000.
        const buffer = try allocator.alloc(u8, 1000);
        errdefer allocator.free(buffer);

        const socket = try io.open_socket(
            address.any.family,
            std.os.SOCK.DGRAM,
            std.os.IPPROTO.UDP,
        );
        errdefer std.os.closeSocket(socket);

        // Allocate our completions - hardcoded max of 256.
        var completions = try allocator.alloc(IO.Completion, 256);
        errdefer allocator.free(completions);

        var statsd = StatsD{
            .buffer = buffer,
            .socket = socket,
            .io = io,
            .completions = completions,
        };

        for (completions) |*completion| {
            completion.next = null;
            statsd.completions_fifo.push(completion);
        }

        // 'Connect' the UDP socket, so we can just send() to it normally.
        try std.os.connect(socket, &address.any, address.getOsSockLen());

        return statsd;
    }

    pub fn deinit(self: *StatsD, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
        std.os.closeSocket(self.socket);
        allocator.free(self.completions);
    }

    pub fn gauge(self: *StatsD, stat: []const u8, value: usize) !void {
        const statsd_packet = try std.fmt.bufPrint(self.buffer, "{s}:{}|g", .{ stat, value });
        var completion = self.completions_fifo.pop() orelse return error.NoSpaceLeft;
        completion.next = null;

        self.io.send(
            *StatsD,
            self,
            StatsD.send_callback,
            completion,
            self.socket,
            statsd_packet,
        );
    }

    pub fn timing(self: *StatsD, stat: []const u8, ms: usize) !void {
        const statsd_packet = try std.fmt.bufPrint(self.buffer, "{s}:{}|ms", .{ stat, ms });
        var completion = self.completions_fifo.pop() orelse return error.NoSpaceLeft;
        completion.next = null;

        self.io.send(
            *StatsD,
            self,
            StatsD.send_callback,
            completion,
            self.socket,
            statsd_packet,
        );
    }

    pub fn send_callback(context: *StatsD, completion: *IO.Completion, result: IO.SendError!usize) void {
        _ = context;
        _ = completion;
        _ = result catch {};

        completion.next = null;
        context.completions_fifo.push(completion);
    }
};
