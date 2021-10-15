const std = @import("std");
const os = std.os;
const testing = std.testing;
const assert = std.debug.assert;
const is_darwin = std.Target.current.isDarwin();

const IO = @import("io.zig").IO;
const io_flags = if (is_darwin) 0 else os.MSG_NOSIGNAL;
const sock_flags = os.SOCK_CLOEXEC | (if (is_darwin) os.SOCK_NONBLOCK else 0);

test "IO data pipe" {
    try struct {
        io: IO,
        tx: Pipe,
        rx: Pipe,
        server: Socket = .{},

        const buffer_size = 256 * 1024 * 1024;

        const Context = @This();
        const Socket = struct {
            fd: os.socket_t = -1,
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
            
            self.server.fd = try os.socket(os.AF_INET, os.SOCK_STREAM | sock_flags, 0);
            defer os.close(self.server.fd);

            const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
            try os.setsockopt(
                self.server.fd,
                os.SOL_SOCKET,
                os.SO_REUSEADDR,
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
                sock_flags,
            );

            self.tx.socket.fd = try os.socket(os.AF_INET, os.SOCK_STREAM | sock_flags, 0);
            defer os.close(self.tx.socket.fd);

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

            try testing.expect(self.server.fd != -1);
            try testing.expect(self.tx.socket.fd != -1);
            try testing.expect(self.rx.socket.fd != -1);
            os.close(self.rx.socket.fd);

            try testing.expectEqual(self.tx.transferred, buffer_size);
            try testing.expectEqual(self.rx.transferred, buffer_size);
            try testing.expect(std.mem.eql(u8, self.tx.buffer, self.rx.buffer));
        }

        fn on_accept(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!os.socket_t,
        ) void {
            assert(self.rx.socket.fd == -1);
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
            assert(self.tx.socket.fd != -1);
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
                    io_flags,
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
                    io_flags,
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

test "IO data echo" {
    // test socket IO duplex functionality

    // measure total throughput over like 1s
}

test "IO data dropping" {
    // test pending socket IO cancellation
}

test "IO data concurrency" {
    // test mass IO concurrency

    // N clients -> server -> N sockets
    //
    // client N sends to server socket N
    // server socket N receives
    // server socket N sends to client N + 1
    // client N + 1 receives
    // repeat with N = N + 1
}