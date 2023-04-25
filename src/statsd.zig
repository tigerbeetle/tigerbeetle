const std = @import("std");

const IO = @import("io.zig").IO;

pub const StatsD = struct {
    buffer: []u8,
    socket: std.os.socket_t,
    address: std.net.Address,
    address_size: u32,

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

        return StatsD{
            .buffer = buffer,
            .socket = socket,
            .address = address,
            .address_size = address.getOsSockLen(),
        };
    }

    pub fn deinit(self: *StatsD, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
        std.os.closeSocket(self.socket);
    }

    pub fn gauge(self: *StatsD, stat: []const u8, value: usize) !void {
        const statsd_packet = try std.fmt.bufPrint(self.buffer, "{s}:{}|g", .{ stat, value });
        _ = try std.os.sendto(
            self.socket,
            statsd_packet,
            0,
            &self.address.any,
            self.address_size,
        );
    }

    pub fn timing(self: *StatsD, stat: []const u8, ms: usize) !void {
        const statsd_packet = try std.fmt.bufPrint(self.buffer, "{s}:{}|ms", .{ stat, ms });
        _ = try std.os.sendto(
            self.socket,
            statsd_packet,
            0,
            &self.address.any,
            self.address_size,
        );
    }
};
