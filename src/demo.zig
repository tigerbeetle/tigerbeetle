const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

pub fn connect(port: u16) !std.os.fd_t {
    var addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var connection = try std.net.tcpConnectToAddress(addr);
    return connection.handle;
}

pub var request_id: u128 = 0;

pub fn send(fd: std.os.fd_t, command: Command, batch: anytype, Results: anytype) !void {
    request_id += 1;

    var data = std.mem.asBytes(batch[0..]);

    var request = NetworkHeader{
        .id = request_id,
        .command = command,
        .size = @intCast(u32, @sizeOf(NetworkHeader) + data.len),
    };
    request.set_checksum_data(data[0..]);
    request.set_checksum_meta();

    const meta = std.mem.asBytes(&request);
    assert((try std.os.sendto(fd, meta[0..], 0, null, 0)) == meta.len);
    if (data.len > 0) {
        assert((try std.os.sendto(fd, data[0..], 0, null, 0)) == data.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    const recv_bytes = try std.os.recvfrom(fd, recv[0..], 0, null, null);
    assert(recv_bytes >= @sizeOf(NetworkHeader));

    var response = std.mem.bytesAsValue(NetworkHeader, recv[0..@sizeOf(NetworkHeader)]);

    const stdout = std.io.getStdOut().writer();
    try response.jsonStringify(.{}, stdout);
    try stdout.writeAll("\n");

    assert(response.magic == Magic);
    assert(response.valid_checksum_meta());
    assert(response.valid_size());
    assert(recv_bytes >= response.size);

    const response_data = recv[@sizeOf(NetworkHeader)..response.size];
    assert(response.valid_checksum_data(response_data));

    for (std.mem.bytesAsSlice(Results, response_data)) |result| {
        try result.jsonStringify(.{}, stdout);
        try stdout.writeAll("\n");
    }
}
