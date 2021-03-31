const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

pub const Header = @import("vr.zig").Header;

pub const Operation = @import("state_machine.zig").Operation;

pub fn connect(port: u16) !std.os.fd_t {
    var addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var connection = try std.net.tcpConnectToAddress(addr);
    return connection.handle;
}

pub const cluster_id: u128 = 746649394563965214; // a5ca1ab1ebee11e
pub const client_id: u128 = 123;
pub var request_number: u32 = 0;

pub fn send(fd: std.os.fd_t, operation: Operation, batch: anytype, Results: anytype) !void {
    request_number += 1;

    var body = std.mem.asBytes(batch[0..]);

    var request = Header{
        .cluster = cluster_id,
        .client = client_id,
        .view = 0,
        .request = request_number,
        .command = .request,
        .operation = operation,
        .size = @intCast(u32, @sizeOf(Header) + body.len),
    };
    request.set_checksum_body(body[0..]);
    request.set_checksum();

    const header = std.mem.asBytes(&request);
    assert((try std.os.sendto(fd, header[0..], 0, null, 0)) == header.len);
    if (body.len > 0) {
        assert((try std.os.sendto(fd, body[0..], 0, null, 0)) == body.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    const recv_bytes = try std.os.recvfrom(fd, recv[0..], 0, null, null);
    assert(recv_bytes >= @sizeOf(Header));

    var response = std.mem.bytesAsValue(Header, recv[0..@sizeOf(Header)]);

    // TODO Add jsonStringfy to vr.Header:
    //const stdout = std.io.getStdOut().writer();
    //try response.jsonStringify(.{}, stdout);
    //try stdout.writeAll("\n");
    log.info("{}", .{response});

    assert(response.valid_checksum());
    assert(recv_bytes >= response.size);

    const response_body = recv[@sizeOf(Header)..response.size];
    assert(response.valid_checksum_body(response_body));

    for (std.mem.bytesAsSlice(Results, response_body)) |result| {
        // TODO
        //try result.jsonStringify(.{}, stdout);
        //try stdout.writeAll("\n");
        log.info("{}", .{result});
    }
}
