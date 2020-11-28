const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const net = std.net;
const os = std.os;
const IO_Uring = os.linux.IO_Uring;
const io_uring_cqe = os.linux.io_uring_cqe;

const Event = packed struct {
    fd: i32,
    op: Op
};

const Op = enum(u32) {
    Accept,
    Recv,
    Send
};

const port = 3001;
const kernel_backlog = 512;
const max_connections = 4096;
const max_buffer = 1000;
var buffers: [max_connections][max_buffer]u8 = undefined;

pub fn main() !void {
    if (builtin.os.tag != .linux) return error.LinuxRequired;

    const address = try net.Address.parseIp4("127.0.0.1", port);
    const domain = address.any.family;
    const socket_type = os.SOCK_STREAM | os.SOCK_CLOEXEC;
    const protocol = os.IPPROTO_TCP;
    const server = try os.socket(domain, socket_type, protocol);
    errdefer os.close(server);
    try os.setsockopt(server, os.SOL_SOCKET, os.SO_REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try os.bind(server, &address.any, address.getOsSockLen());
    try os.listen(server, kernel_backlog);
    std.debug.print("net: echo server: io_uring: listening on {}...\n", .{ address });

    var ring = try IO_Uring.init(512, 0);
    defer ring.deinit();

    var cqes: [512]io_uring_cqe = undefined;
    var accept_addr: os.sockaddr = undefined;
    var accept_addr_len: os.socklen_t = @sizeOf(@TypeOf(accept_addr));
    for (buffers) |b, index| {
        buffers[index] = [_]u8{0} ** max_buffer;
    }

    try accept(&ring, server, &accept_addr, &accept_addr_len);

    while (true) {
        const count = try ring.copy_cqes(cqes[0..], 0);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const cqe = cqes[i];
            if (cqe.res < 0) {
                switch (-cqe.res) {
                    os.EPIPE => std.debug.print("EPIPE {}\n", .{ cqe }),
                    os.ECONNRESET => std.debug.print("ECONNRESET {}\n", .{ cqe }),
                    else => std.debug.print("ERROR {}\n", .{ cqe })
                }
                os.exit(1);
            }
            const event = @bitCast(Event, cqe.user_data);
            switch (event.op) {
                .Accept => {
                    try accept(&ring, server, &accept_addr, &accept_addr_len);
                    try recv(&ring, cqe.res);
                },
                .Recv => {
                    if (cqe.res != 0) {
                        const size = @intCast(usize, cqe.res);
                        try send(&ring, event.fd, size);
                    }
                },
                .Send => {
                    try recv(&ring, event.fd);
                }
            }
        }
        _ = try ring.submit_and_wait(1);
    }
}

fn accept(ring: *IO_Uring, fd: os.fd_t, addr: *os.sockaddr, addr_len: *os.socklen_t) !void {
    var user_data = get_user_data(fd, .Accept);
    _ = try ring.accept(user_data, fd, addr, addr_len, 0);
}

fn recv(ring: *IO_Uring, fd: os.fd_t) !void {
    var user_data = get_user_data(fd, .Recv);
    _ = try ring.recv(user_data, fd, get_buffer(fd)[0..], os.MSG_NOSIGNAL);
}

fn send(ring: *IO_Uring, fd: os.fd_t, size: usize) !void {
    var user_data = get_user_data(fd, .Send);
    _ = try ring.send(user_data, fd, get_buffer(fd)[0..size], os.MSG_NOSIGNAL);
}

fn get_buffer(fd: os.fd_t) []u8 {
    return buffers[@intCast(usize, fd)][0..];
}

fn get_user_data(fd: os.fd_t, op: Op) u64 {
    var event: Event = .{ .fd = fd, .op = op };
    var user_data = @bitCast(u64, event);
    return user_data;
}
