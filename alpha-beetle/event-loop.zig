fn event_loop(ring: *IO_Uring, server_socket: os.fd_t) !void {
    std.debug.warn("setting up event loop...\n", .{});
    
    var client_addr: os.sockaddr = undefined;
    var client_addr_len: os.socklen_t = @sizeOf(@TypeOf(client_addr));

    var cqes: [8]io_uring_cqe = undefined;
    
    try ring.queue_accept(event_accept, server_socket, &client_addr, &client_addr_len, 0);
    _ = try ring.submit();

    var buf: [100]u8 = undefined;

    var iovecs = [1]os.iovec{os.iovec{
        .iov_base = &buf,
        .iov_len = buf.len,
    }};
    
    while (true) {
        const cqe = try wait_cqe(ring);
        if (cqe.res < 0) {
            std.debug.warn("io_uring request failed, res={} ud={}\n", .{ cqe.res, cqe.user_data });
            os.exit(1);
        }
        const event_type = cqe.user_data;
        // TODO Use exhaustive switch statement.
        if (event_type == event_accept) {
            std.debug.warn("accepted a connection\n", .{});
            // Listen for another connection:
            try ring.queue_accept(event_accept, server_socket, &client_addr, &client_addr_len, 0);
            // Read from this connection:
            try ring.queue_readv(event_read, cqe.res, iovecs[0..], 0);
            // Submit these SQEs:
            _ = try ring.submit();
        } else if (event_type == event_read) {
            std.debug.warn("read {} bytes from connection\n", .{ cqe.res });
            break;
        }
    }
}
