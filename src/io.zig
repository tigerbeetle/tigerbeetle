const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;

const FIFO = @import("fifo.zig").FIFO;
const IO_Linux = @import("io_linux.zig").IO;
const IO_Darwin = @import("io_darwin.zig").IO;

pub const IO = switch (std.Target.current.os.tag) {
    .linux => IO_Linux,
    .macos, .tvos, .watchos, .ios => IO_Darwin,
    else => @compileError("IO is not supported for platform"),
};

pub fn buffer_limit(buffer_len: usize) usize {
    // Linux limits how much may be written in a `pwrite()/pread()` call, which is `0x7ffff000` on
    // both 64-bit and 32-bit systems, due to using a signed C int as the return value, as well as
    // stuffing the errno codes into the last `4096` values.
    // Darwin limits writes to `0x7fffffff` bytes, more than that returns `EINVAL`.
    // The corresponding POSIX limit is `std.math.maxInt(isize)`.
    const limit = switch (std.Target.current.os.tag) {
        .linux => 0x7ffff000,
        .macos, .ios, .watchos, .tvos => std.math.maxInt(i32),
        else => std.math.maxInt(isize),
    };
    return std.math.min(limit, buffer_len);
}

test "ref all decls" {
    std.testing.refAllDecls(IO);
}

test "write/fsync/read" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
        done: bool = false,
        fd: os.fd_t,

        write_buf: [20]u8 = [_]u8{97} ** 20,
        read_buf: [20]u8 = [_]u8{98} ** 20,

        written: usize = 0,
        fsynced: bool = false,
        read: usize = 0,

        fn run_test() !void {
            const path = "test_io_write_fsync_read";
            const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
            defer file.close();
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
            try testing.expect(self.fsynced);
            try testing.expectEqual(self.read_buf.len, self.read);
            try testing.expectEqualSlices(u8, &self.write_buf, &self.read_buf);
        }

        fn write_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.WriteError!usize,
        ) void {
            self.written = result catch @panic("write error");
            self.io.fsync(*Context, self, fsync_callback, completion, self.fd, 0);
        }

        fn fsync_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.FsyncError!void,
        ) void {
            result catch @panic("fsync error");
            self.fsynced = true;
            self.io.read(*Context, self, read_callback, completion, self.fd, &self.read_buf, 10);
        }

        fn read_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ReadError!usize,
        ) void {
            self.read = result catch @panic("read error");
            self.done = true;
        }
    }.run_test();
}

test "openat/close" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
        done: bool = false,
        fd: os.fd_t = 0,

        fn run_test() !void {
            const path = "test_io_openat_close";
            defer std.fs.cwd().deleteFile(path) catch {};

            var self: Context = .{ .io = try IO.init(32, 0) };
            defer self.io.deinit();

            var completion: IO.Completion = undefined;
            self.io.openat(
                *Context,
                &self,
                openat_callback,
                &completion,
                linux.AT_FDCWD,
                path,
                os.O_CLOEXEC | os.O_RDWR | os.O_CREAT,
                0o666,
            );
            while (!self.done) try self.io.tick();

            try testing.expect(self.fd > 0);
        }

        fn openat_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.OpenatError!os.fd_t,
        ) void {
            self.fd = result catch @panic("openat error");
            self.io.close(*Context, self, close_callback, completion, self.fd);
        }

        fn close_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.CloseError!void,
        ) void {
            result catch @panic("close error");
            self.done = true;
        }
    }.run_test();
}

test "accept/connect/send/receive" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
        done: bool = false,
        server: os.socket_t,
        client: os.socket_t,

        accepted_sock: os.socket_t = undefined,

        send_buf: [10]u8 = [_]u8{ 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 },
        recv_buf: [5]u8 = [_]u8{ 0, 1, 0, 1, 0 },

        sent: usize = 0,
        received: usize = 0,

        fn run_test() !void {
            const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
            const kernel_backlog = 1;
            const server = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
            defer os.close(server);

            const client = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
            defer os.close(client);

            try os.setsockopt(
                server,
                os.SOL_SOCKET,
                os.SO_REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );
            try os.bind(server, &address.any, address.getOsSockLen());
            try os.listen(server, kernel_backlog);

            var self: Context = .{
                .io = try IO.init(32, 0),
                .server = server,
                .client = client,
            };
            defer self.io.deinit();

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
            self.io.accept(*Context, &self, accept_callback, &server_completion, server, 0);

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
            result catch @panic("connect error");
            self.io.send(
                *Context,
                self,
                send_callback,
                completion,
                self.client,
                &self.send_buf,
                if (std.Target.current.os.tag == .linux) os.MSG_NOSIGNAL else 0,
            );
        }

        fn send_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.SendError!usize,
        ) void {
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
                if (std.Target.current.os.tag == .linux) os.MSG_NOSIGNAL else 0,
            );
        }

        fn recv_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            self.received = result catch @panic("recv error");
            self.done = true;
        }
    }.run_test();
}

test "timeout" {
    const testing = std.testing;

    const ms = 20;
    const margin = 5;
    const count = 10;

    try struct {
        const Context = @This();

        io: IO,
        count: u32 = 0,
        stop_time: i64 = 0,

        fn run_test() !void {
            const start_time = std.time.milliTimestamp();
            var self: Context = .{ .io = try IO.init(32, 0) };
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
                @intToFloat(f64, self.stop_time - start_time),
                margin,
            );
        }

        fn timeout_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            result catch @panic("timeout error");
            if (self.stop_time == 0) self.stop_time = std.time.milliTimestamp();
            self.count += 1;
        }
    }.run_test();
}

test "submission queue full" {
    const testing = std.testing;

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
            result catch @panic("timeout error");
            self.count += 1;
        }
    }.run_test();
}
