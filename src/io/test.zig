const std = @import("std");
const builtin = @import("builtin");
const os = std.os;
const testing = std.testing;
const assert = std.debug.assert;

const Time = @import("../time.zig").Time;
const IO = @import("../io.zig").IO;

test "write/read/close" {
    try struct {
        const Context = @This();

        io: IO,
        done: bool = false,
        fd: os.fd_t,

        write_buf: [20]u8 = [_]u8{97} ** 20,
        read_buf: [20]u8 = [_]u8{98} ** 20,

        written: usize = 0,
        read: usize = 0,

        fn run_test() !void {
            const path = "test_io_write_read_close";
            const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
            defer std.fs.cwd().deleteFile(path) catch {};

            var self: Context = .{
                .io = try IO.init(32, 0),
                .fd = file.handle,
            };
            defer self.io.deinit();

            var completion: IO.Completion = undefined;

            self.io.write(
                *Context,
                &self,
                write_callback,
                &completion,
                self.fd,
                &self.write_buf,
                10,
            );
            while (!self.done) try self.io.tick();

            try testing.expectEqual(self.write_buf.len, self.written);
            try testing.expectEqual(self.read_buf.len, self.read);
            try testing.expectEqualSlices(u8, &self.write_buf, &self.read_buf);
        }

        fn write_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.WriteError!usize,
        ) void {
            self.written = result catch @panic("write error");
            self.io.read(*Context, self, read_callback, completion, self.fd, &self.read_buf, 10);
        }

        fn read_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ReadError!usize,
        ) void {
            self.read = result catch @panic("read error");
            self.io.close(*Context, self, close_callback, completion, self.fd);
        }

        fn close_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.CloseError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("close error");

            self.done = true;
        }
    }.run_test();
}

test "accept/connect/send/receive" {
    try struct {
        const Context = @This();

        io: *IO,
        done: bool = false,
        server: os.socket_t,
        client: os.socket_t,

        accepted_sock: os.socket_t = undefined,

        send_buf: [10]u8 = [_]u8{ 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 },
        recv_buf: [5]u8 = [_]u8{ 0, 1, 0, 1, 0 },

        sent: usize = 0,
        received: usize = 0,

        fn run_test() !void {
            var io = try IO.init(32, 0);
            defer io.deinit();

            const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
            const kernel_backlog = 1;
            const server = try io.open_socket(address.any.family, os.SOCK.STREAM, os.IPPROTO.TCP);
            defer os.closeSocket(server);

            const client = try io.open_socket(address.any.family, os.SOCK.STREAM, os.IPPROTO.TCP);
            defer os.closeSocket(client);

            try os.setsockopt(
                server,
                os.SOL.SOCKET,
                os.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );
            try os.bind(server, &address.any, address.getOsSockLen());
            try os.listen(server, kernel_backlog);

            var self: Context = .{
                .io = &io,
                .server = server,
                .client = client,
            };

            var client_completion: IO.Completion = undefined;
            self.io.connect(
                *Context,
                &self,
                connect_callback,
                &client_completion,
                client,
                address,
            );

            var server_completion: IO.Completion = undefined;
            self.io.accept(*Context, &self, accept_callback, &server_completion, server);

            while (!self.done) try self.io.tick();

            try testing.expectEqual(self.send_buf.len, self.sent);
            try testing.expectEqual(self.recv_buf.len, self.received);

            try testing.expectEqualSlices(u8, self.send_buf[0..self.received], &self.recv_buf);
        }

        fn connect_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            _ = result catch @panic("connect error");

            self.io.send(
                *Context,
                self,
                send_callback,
                completion,
                self.client,
                &self.send_buf,
            );
        }

        fn send_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.SendError!usize,
        ) void {
            _ = completion;

            self.sent = result catch @panic("send error");
        }

        fn accept_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!os.socket_t,
        ) void {
            self.accepted_sock = result catch @panic("accept error");
            self.io.recv(
                *Context,
                self,
                recv_callback,
                completion,
                self.accepted_sock,
                &self.recv_buf,
            );
        }

        fn recv_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            _ = completion;

            self.received = result catch @panic("recv error");
            self.done = true;
        }
    }.run_test();
}

test "timeout" {
    const ms = 20;
    const margin = 5;
    const count = 10;

    try struct {
        const Context = @This();

        io: IO,
        timer: *Time,
        count: u32 = 0,
        stop_time: u64 = 0,

        fn run_test() !void {
            var timer = Time{};
            const start_time = timer.monotonic();
            var self: Context = .{
                .timer = &timer,
                .io = try IO.init(32, 0),
            };
            defer self.io.deinit();

            var completions: [count]IO.Completion = undefined;
            for (completions) |*completion| {
                self.io.timeout(
                    *Context,
                    &self,
                    timeout_callback,
                    completion,
                    ms * std.time.ns_per_ms,
                );
            }
            while (self.count < count) try self.io.tick();

            try self.io.tick();
            try testing.expectEqual(@as(u32, count), self.count);

            try testing.expectApproxEqAbs(
                @as(f64, ms),
                @intToFloat(f64, (self.stop_time - start_time) / std.time.ns_per_ms),
                margin,
            );
        }

        fn timeout_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("timeout error");

            if (self.stop_time == 0) self.stop_time = self.timer.monotonic();
            self.count += 1;
        }
    }.run_test();
}

test "submission queue full" {
    const ms = 20;
    const count = 10;

    try struct {
        const Context = @This();

        io: IO,
        count: u32 = 0,

        fn run_test() !void {
            var self: Context = .{ .io = try IO.init(1, 0) };
            defer self.io.deinit();

            var completions: [count]IO.Completion = undefined;
            for (completions) |*completion| {
                self.io.timeout(
                    *Context,
                    &self,
                    timeout_callback,
                    completion,
                    ms * std.time.ns_per_ms,
                );
            }
            while (self.count < count) try self.io.tick();

            try self.io.tick();
            try testing.expectEqual(@as(u32, count), self.count);
        }

        fn timeout_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("timeout error");

            self.count += 1;
        }
    }.run_test();
}

test "tick to wait" {
    // Use only IO.tick() to see if pending IO is actually processsed

    try struct {
        const Context = @This();

        io: IO,
        accepted: os.socket_t = IO.INVALID_SOCKET,
        connected: bool = false,
        received: bool = false,

        fn run_test() !void {
            var self: Context = .{ .io = try IO.init(1, 0) };
            defer self.io.deinit();

            const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
            const kernel_backlog = 1;

            const server = try self.io.open_socket(address.any.family, os.SOCK.STREAM, os.IPPROTO.TCP);
            defer os.closeSocket(server);

            try os.setsockopt(
                server,
                os.SOL.SOCKET,
                os.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );
            try os.bind(server, &address.any, address.getOsSockLen());
            try os.listen(server, kernel_backlog);

            const client = try self.io.open_socket(address.any.family, os.SOCK.STREAM, os.IPPROTO.TCP);
            defer os.closeSocket(client);

            // Start the accept
            var server_completion: IO.Completion = undefined;
            self.io.accept(*Context, &self, accept_callback, &server_completion, server);

            // Start the connect
            var client_completion: IO.Completion = undefined;
            self.io.connect(
                *Context,
                &self,
                connect_callback,
                &client_completion,
                client,
                address,
            );

            // Tick the IO to drain the accept & connect completions
            assert(!self.connected);
            assert(self.accepted == IO.INVALID_SOCKET);

            while (self.accepted == IO.INVALID_SOCKET or !self.connected)
                try self.io.tick();

            assert(self.connected);
            assert(self.accepted != IO.INVALID_SOCKET);
            defer os.closeSocket(self.accepted);

            // Start receiving on the client
            var recv_completion: IO.Completion = undefined;
            var recv_buffer: [64]u8 = undefined;
            std.mem.set(u8, &recv_buffer, 0xaa);
            self.io.recv(
                *Context,
                &self,
                recv_callback,
                &recv_completion,
                client,
                &recv_buffer,
            );

            // Drain out the recv completion from any internal IO queues
            try self.io.tick();
            try self.io.tick();
            try self.io.tick();

            // Complete the recv() *outside* of the IO instance.
            // Other tests already check .tick() with IO based completions.
            // This simulates IO being completed by an external system
            var send_buf = std.mem.zeroes([64]u8);
            const wrote = try os_send(self.accepted, &send_buf, 0);
            try testing.expectEqual(wrote, send_buf.len);

            // Wait for the recv() to complete using only IO.tick().
            // If tick is broken, then this will deadlock
            assert(!self.received);
            while (!self.received) {
                try self.io.tick();
            }

            // Make sure the receive actually happened
            assert(self.received);
            try testing.expect(std.mem.eql(u8, &recv_buffer, &send_buf));
        }

        fn accept_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!os.socket_t,
        ) void {
            _ = completion;

            assert(self.accepted == IO.INVALID_SOCKET);
            self.accepted = result catch @panic("accept error");
        }

        fn connect_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("connect error");

            assert(!self.connected);
            self.connected = true;
        }

        fn recv_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            _ = completion;
            _ = result catch |err| std.debug.panic("recv error: {}", .{err});

            assert(!self.received);
            self.received = true;
        }

        // TODO: use os.send() instead when it gets fixed for windows
        fn os_send(sock: os.socket_t, buf: []const u8, flags: u32) !usize {
            if (builtin.target.os.tag != .windows) {
                return os.send(sock, buf, flags);
            }

            const rc = os.windows.sendto(sock, buf.ptr, buf.len, flags, null, 0);
            if (rc == os.windows.ws2_32.SOCKET_ERROR) {
                switch (os.windows.ws2_32.WSAGetLastError()) {
                    .WSAEACCES => return error.AccessDenied,
                    .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
                    .WSAECONNRESET => return error.ConnectionResetByPeer,
                    .WSAEMSGSIZE => return error.MessageTooBig,
                    .WSAENOBUFS => return error.SystemResources,
                    .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                    .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
                    .WSAEDESTADDRREQ => unreachable, // A destination address is required.
                    .WSAEFAULT => unreachable, // The lpBuffers, lpTo, lpOverlapped, lpNumberOfBytesSent, or lpCompletionRoutine parameters are not part of the user address space, or the lpTo parameter is too small.
                    .WSAEHOSTUNREACH => return error.NetworkUnreachable,
                    // TODO: WSAEINPROGRESS, WSAEINTR
                    .WSAEINVAL => unreachable,
                    .WSAENETDOWN => return error.NetworkSubsystemFailed,
                    .WSAENETRESET => return error.ConnectionResetByPeer,
                    .WSAENETUNREACH => return error.NetworkUnreachable,
                    .WSAENOTCONN => return error.SocketNotConnected,
                    .WSAESHUTDOWN => unreachable, // The socket has been shut down; it is not possible to WSASendTo on a socket after shutdown has been invoked with how set to SD_SEND or SD_BOTH.
                    .WSAEWOULDBLOCK => return error.WouldBlock,
                    .WSANOTINITIALISED => unreachable, // A successful WSAStartup call must occur before using this function.
                    else => |err| return os.windows.unexpectedWSAError(err),
                }
            } else {
                return @intCast(usize, rc);
            }
        }
    }.run_test();
}

test "pipe data over socket" {
    try struct {
        io: IO,
        tx: Pipe,
        rx: Pipe,
        server: Socket = .{},

        const buffer_size = 1 * 1024 * 1024;

        const Context = @This();
        const Socket = struct {
            fd: os.socket_t = IO.INVALID_SOCKET,
            completion: IO.Completion = undefined,
        };
        const Pipe = struct {
            socket: Socket = .{},
            buffer: []u8,
            transferred: usize = 0,
        };

        fn run() !void {
            const tx_buf = try testing.allocator.alloc(u8, buffer_size);
            defer testing.allocator.free(tx_buf);
            const rx_buf = try testing.allocator.alloc(u8, buffer_size);
            defer testing.allocator.free(rx_buf);

            std.mem.set(u8, tx_buf, 1);
            std.mem.set(u8, rx_buf, 0);
            var self = Context{
                .io = try IO.init(32, 0),
                .tx = .{ .buffer = tx_buf },
                .rx = .{ .buffer = rx_buf },
            };
            defer self.io.deinit();

            self.server.fd = try self.io.open_socket(os.AF.INET, os.SOCK.STREAM, os.IPPROTO.TCP);
            defer os.closeSocket(self.server.fd);

            const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
            try os.setsockopt(
                self.server.fd,
                os.SOL.SOCKET,
                os.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );

            try os.bind(self.server.fd, &address.any, address.getOsSockLen());
            try os.listen(self.server.fd, 1);

            self.io.accept(
                *Context,
                &self,
                on_accept,
                &self.server.completion,
                self.server.fd,
            );

            self.tx.socket.fd = try self.io.open_socket(os.AF.INET, os.SOCK.STREAM, os.IPPROTO.TCP);
            defer os.closeSocket(self.tx.socket.fd);

            self.io.connect(
                *Context,
                &self,
                on_connect,
                &self.tx.socket.completion,
                self.tx.socket.fd,
                address,
            );

            var tick: usize = 0xdeadbeef;
            while (self.rx.transferred != self.rx.buffer.len) : (tick +%= 1) {
                if (tick % 61 == 0) {
                    const timeout_ns = tick % (10 * std.time.ns_per_ms);
                    try self.io.run_for_ns(@intCast(u63, timeout_ns));
                } else {
                    try self.io.tick();
                }
            }

            try testing.expect(self.server.fd != IO.INVALID_SOCKET);
            try testing.expect(self.tx.socket.fd != IO.INVALID_SOCKET);
            try testing.expect(self.rx.socket.fd != IO.INVALID_SOCKET);
            os.closeSocket(self.rx.socket.fd);

            try testing.expectEqual(self.tx.transferred, buffer_size);
            try testing.expectEqual(self.rx.transferred, buffer_size);
            try testing.expect(std.mem.eql(u8, self.tx.buffer, self.rx.buffer));
        }

        fn on_accept(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!os.socket_t,
        ) void {
            assert(self.rx.socket.fd == IO.INVALID_SOCKET);
            assert(&self.server.completion == completion);
            self.rx.socket.fd = result catch |err| std.debug.panic("accept error {}", .{err});

            assert(self.rx.transferred == 0);
            self.do_receiver(0);
        }

        fn on_connect(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            _ = result catch unreachable;

            assert(self.tx.socket.fd != IO.INVALID_SOCKET);
            assert(&self.tx.socket.completion == completion);

            assert(self.tx.transferred == 0);
            self.do_sender(0);
        }

        fn do_sender(self: *Context, bytes: usize) void {
            self.tx.transferred += bytes;
            assert(self.tx.transferred <= self.tx.buffer.len);

            if (self.tx.transferred < self.tx.buffer.len) {
                self.io.send(
                    *Context,
                    self,
                    on_send,
                    &self.tx.socket.completion,
                    self.tx.socket.fd,
                    self.tx.buffer[self.tx.transferred..],
                );
            }
        }

        fn on_send(
            self: *Context,
            completion: *IO.Completion,
            result: IO.SendError!usize,
        ) void {
            const bytes = result catch |err| std.debug.panic("send error: {}", .{err});
            assert(&self.tx.socket.completion == completion);
            self.do_sender(bytes);
        }

        fn do_receiver(self: *Context, bytes: usize) void {
            self.rx.transferred += bytes;
            assert(self.rx.transferred <= self.rx.buffer.len);

            if (self.rx.transferred < self.rx.buffer.len) {
                self.io.recv(
                    *Context,
                    self,
                    on_recv,
                    &self.rx.socket.completion,
                    self.rx.socket.fd,
                    self.rx.buffer[self.rx.transferred..],
                );
            }
        }

        fn on_recv(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            const bytes = result catch |err| std.debug.panic("recv error: {}", .{err});
            assert(&self.rx.socket.completion == completion);
            self.do_receiver(bytes);
        }
    }.run();
}
