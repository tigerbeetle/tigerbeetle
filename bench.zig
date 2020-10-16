const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;
const testing = std.testing;

usingnamespace @import("types.zig");

const port = 3001;

pub fn main() !void {
    var addr = try net.Address.parseIp4("127.0.0.1", port);
    var connection = try net.tcpConnectToAddress(addr);
    errdefer os.close(connection.handle);

    // Disable Nagle's delay:
    const enable = &mem.toBytes(@as(c_int, 1));
    try os.setsockopt(connection.handle, os.IPPROTO_TCP, os.TCP_NODELAY, enable);

    var header = NetworkHeader {
        .id = 7000,
        .command = .create_transfers,
        .data_size = 128
    };
    var data = [_]u8{0} ** 128;
    header.set_checksum_data(data[0..]);
    header.set_checksum_meta();

    std.debug.print("{x}\n", .{ mem.asBytes(&header) });
    const meta = @bitCast([@sizeOf(NetworkHeader)]u8, header);

    _ = try os.sendto(connection.handle, meta[0..32], 0, null, 0);
    os.nanosleep(1, 0);
    _ = try os.sendto(connection.handle, meta[32..], 0, null, 0);
    os.nanosleep(1, 0);
    _ = try os.sendto(connection.handle, data[0..1], 0, null, 0);
    os.nanosleep(1, 0);
    _ = try os.sendto(connection.handle, data[1..], 0, null, 0);
    // Pipeline another request:
    _ = try os.sendto(connection.handle, meta[0..], 0, null, 0);
    _ = try os.sendto(connection.handle, data[0..], 0, null, 0);
    var recv: [64]u8 = undefined;
    _ = try os.recvfrom(connection.handle, recv[0..], 0, null, null);
    std.debug.print("ack: {x}\n", .{ recv });
    _ = try os.recvfrom(connection.handle, recv[0..], 0, null, null);
    std.debug.print("ack: {x}\n", .{ recv });
    os.close(connection.handle);
}
