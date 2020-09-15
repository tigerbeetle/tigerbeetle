const builtin = @import("builtin");
const std = @import("std");
const mem = std.mem;
const net = std.net;
const os = std.os;

const io_uring = @import("io_uring.zig");
const IO_Uring = io_uring.IO_Uring;

fn setup_tcp_server(address: net.Address) !os.fd_t {
    const kernel_backlog = 64;
    const domain = address.any.family;
    const socket_type = os.SOCK_STREAM | os.SOCK_CLOEXEC;
    const protocol = os.IPPROTO_TCP;
    const fd = try os.socket(domain, socket_type, protocol);
    errdefer os.close(fd);
    try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try os.bind(fd, &address.any, address.getOsSockLen());
    try os.listen(fd, kernel_backlog);
    return fd;
}

// TODO Use an enum for these flags:
const event_accept = 1;
const event_read = 2;
const event_write = 3;

fn setup_event_loop(ring: *IO_Uring, server_socket: os.fd_t) !void {
    // TODO We're going to do something proper with all these static allocations.
    // For example, we will have multiple sockaddr buffers to accept up to N connections.
    // Another example, each connection will have it's own read and write buffer.
    // For now we allocate everything on the stack, which is fine since this function never exits.

    // Statically allocate a sockaddr struct to store accepted client connection address:
    var accept_addr: os.sockaddr = undefined;
    var accept_addr_len: os.socklen_t = @sizeOf(@TypeOf(accept_addr));
    
    // Statically allocate a client socket fd:
    var client_fd: os.fd_t = undefined;

    // Statically allocate a read buffer to read from a network connection:
    var read_buffer: [1344]u8 = undefined;
    var read_iovecs = [1]os.iovec{os.iovec{
        .iov_base = &read_buffer,
        .iov_len = read_buffer.len,
    }};

    // Statically allocate a write buffer to write to a network connection:
    var write_buffer: [64]u8 = undefined;
    var write_iovecs = [1]os.iovec_const{os.iovec_const{
        .iov_base = &write_buffer,
        .iov_len = write_buffer.len,
    }};

    // Start accepting a connection:
    try ring.queue_accept(event_accept, server_socket, &accept_addr, &accept_addr_len, 0);
    _ = try ring.submit();
    
    // Start the event loop to process CQEs and submit SQEs:
    while (true) {
        const cqe = try ring.wait_cqe();
        if (cqe.res < 0) {
            std.debug.print("cqe failed, user_data={} res={} \n", .{ cqe.user_data, cqe.res });
            os.exit(1);
        }
        // TODO Use exhaustive switch statement to make sure we cover all event types,
        // see https://ziglang.org/documentation/master/#Exhaustive-Switching
        if (cqe.user_data == event_accept) {
            std.debug.print("accepted a connection\n", .{});
            // Listen for another connection:
            try ring.queue_accept(event_accept, server_socket, &accept_addr, &accept_addr_len, 0);
            // Read from this connection:
            client_fd = cqe.res; // TODO This races if another connection comes in.
            try ring.queue_readv(event_read, client_fd, read_iovecs[0..], 0);
        } else if (cqe.user_data == event_read) {
            std.debug.print("read {} bytes from connection\n", .{ cqe.res });
            // Now ack back to the client (as if we had processed their command):
            try ring.queue_writev(event_write, client_fd, write_iovecs[0..], 0, 0);
        } else if (cqe.user_data == event_write) {
            std.debug.print("wrote {} bytes to connection\n", .{ cqe.res });
            // We have now done a read-write for this connection using static read/write buffers.
            // Let's rinse and repeat and queue another read on the connection.
            // We don't want to queue reads before we have acked a batch, because otherwise a client
            // could run ahead of us and exhaust resources.
            // This gives us nice flow control.
            try ring.queue_readv(event_read, client_fd, read_iovecs[0..], 0);
        } else {
            std.debug.print("unexpected user_data={}\n", .{ cqe.user_data });
            os.exit(1);
        }
        // Submit SQEs queued:
        // TODO Fold this into wait_cqe() above by using submit_and_wait().
        _ = try ring.submit();
    }
}

pub fn main() !void {
    if (builtin.os.tag != .linux) return error.LinuxRequired;

    // Setup io_uring:
    std.debug.print("initializing io_uring...\n", .{});
    const entries = 128;
    const flags = 0;
    var ring = try IO_Uring.init(entries, flags);
    defer ring.deinit();

    // Setup server to listen on localhost only (not exposed to the Internet):
    std.debug.print("initializing tcp server...\n", .{});
    var addr = try net.Address.parseIp4("0.0.0.0", 3001);
    var fd = try setup_tcp_server(addr);
    std.debug.print("listening on {}...\n", .{ addr });

    // Setup event loop:
    std.debug.print("initializing event loop...\n", .{});
    try setup_event_loop(&ring, fd);
}
