const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

pub const vr = @import("vr.zig");
pub const Header = vr.Header;
pub const StateMachine = @import("state_machine.zig").StateMachine;

pub fn connect(port: u16) !std.os.fd_t {
    var addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var connection = try std.net.tcpConnectToAddress(addr);
    return connection.handle;
}

// TODO We can share most of what follows with `src/benchmark.zig`:

pub const cluster_id: u32 = 0;
pub const client_id: u128 = 135;
pub var session_number: u64 = 0;
pub var request_number: u32 = 0;
pub var request_checksum: u128 = 0;
pub var reply_checksum: u128 = 0;
pub var view_number: u32 = 0;

pub fn send(
    fd: std.os.fd_t,
    operation: StateMachine.Operation,
    batch: anytype,
    Results: anytype,
) !void {
    // Register a client session with the cluster before we make further requests:
    if (session_number == 0) try register(fd);

    var body = std.mem.asBytes(batch[0..]);

    var request = Header{
        .cluster = cluster_id,
        .parent = reply_checksum,
        .client = client_id,
        .context = session_number,
        .request = request_number,
        .view = view_number,
        .command = .request,
        .operation = vr.Operation.from(StateMachine, operation),
        .size = @intCast(u32, @sizeOf(Header) + body.len),
    };
    request.set_checksum_body(body[0..]);
    request.set_checksum();
    request_checksum = request.checksum;

    const header = std.mem.asBytes(&request);
    assert((try std.os.sendto(fd, header[0..], 0, null, 0)) == header.len);
    if (body.len > 0) {
        assert((try std.os.sendto(fd, body[0..], 0, null, 0)) == body.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    const recv_bytes = try std.os.recvfrom(fd, recv[0..], 0, null, null);
    assert(recv_bytes >= @sizeOf(Header));

    const reply_header = std.mem.bytesAsValue(Header, recv[0..@sizeOf(Header)]);

    // TODO Add jsonStringfy to vr.Header:
    //const stdout = std.io.getStdOut().writer();
    //try response.jsonStringify(.{}, stdout);
    //try stdout.writeAll("\n");
    std.debug.print("{}\n", .{reply_header});

    assert(reply_header.valid_checksum());
    assert(reply_header.parent == request_checksum);
    reply_checksum = reply_header.checksum;
    assert(reply_header.view >= view_number);
    view_number = reply_header.view;
    assert(recv_bytes >= reply_header.size);

    const reply_body = recv[@sizeOf(Header)..reply_header.size];
    assert(reply_header.valid_checksum_body(reply_body));

    for (std.mem.bytesAsSlice(Results, reply_body)) |result| {
        // TODO
        //try result.jsonStringify(.{}, stdout);
        //try stdout.writeAll("\n");
        std.debug.print("{}\n", .{result});
    }

    request_number += 1;
}

fn register(fd: std.os.fd_t) !void {
    assert(session_number == 0);
    assert(request_number == 0);

    var request = Header{
        .command = .request,
        .operation = .register,
        .cluster = cluster_id,
        .parent = reply_checksum,
        .client = client_id,
        .context = session_number,
        .request = request_number,
        .view = 0,
    };
    request.set_checksum_body(&[0]u8{});
    request.set_checksum();
    request_checksum = request.checksum;

    assert((try std.os.sendto(fd, std.mem.asBytes(&request), 0, null, 0)) == @sizeOf(Header));

    var reply: [@sizeOf(Header)]u8 = undefined;
    var reply_size: u64 = 0;
    while (reply_size < @sizeOf(Header)) {
        var reply_bytes = try std.os.recvfrom(fd, reply[reply_size..], 0, null, null);
        if (reply_bytes == 0) @panic("server closed the connection (while registering)");
        reply_size += reply_bytes;
    }
    assert(reply_size >= @sizeOf(Header));

    const reply_header = std.mem.bytesAsValue(Header, reply[0..@sizeOf(Header)]);
    assert(reply_header.valid_checksum());
    assert(reply_header.parent == request_checksum);
    reply_checksum = reply_header.checksum;
    assert(reply_header.view >= view_number);
    view_number = reply_header.view;

    std.debug.print("registered session number {}\n", .{reply_header});
    session_number = reply_header.commit;

    request_number += 1;
}
