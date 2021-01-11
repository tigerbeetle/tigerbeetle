const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const IO_Uring = os.linux.IO_Uring;
const io_uring_cqe = os.linux.io_uring_cqe;

fn server_accept_loop(io: *IO, server: os.fd_t) !void {
    while (true) {
        var addr: os.sockaddr = undefined;
        var addr_len: os.socklen_t = @sizeOf(@TypeOf(addr));
        var buffer: [1024]u8 = undefined;

        const fd = try io.accept(server, &addr, &addr_len);
        std.debug.print("fd {}: accepted connection\n", .{ fd });

        const recv_size = try io.recv(fd, buffer[0..]);
        std.debug.print("fd {}: read {} bytes\n", .{ fd, recv_size });
        
        os.close(fd);
    }
}

fn server_listen(port: u16) !os.fd_t {
    const backlog = 512;
    const address = try std.net.Address.parseIp4("127.0.0.1", port);
    const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, os.IPPROTO_TCP);
    errdefer os.close(fd);
    try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try os.bind(fd, &address.any, address.getOsSockLen());
    try os.listen(fd, backlog);
    std.debug.print("listening on {}...\n", .{ address });
    return fd;
}

const IO = struct {
    ring: IO_Uring,

    const Completion = struct {
        frame: anyframe,
        result: i32 = undefined,
    };

    pub fn init() !IO {
        return IO{
            .ring = try IO_Uring.init(128, 0)
        };
    }

    pub fn deinit(self: *IO) void {
        self.ring.deinit();
    }

    pub fn poll(self: *IO) !void {
        var cqes: [256]io_uring_cqe = undefined;
        while (true) {
            const count = try self.ring.copy_cqes(cqes[0..], 0);
            for (cqes[0..count]) |cqe| {
                const completion = @intToPtr(*IO.Completion, @intCast(usize, cqe.user_data));
                completion.result = cqe.res;
                std.debug.print("{} resuming...\n", .{ cqe.user_data });
                resume completion.frame;
                std.debug.print("{} control flow returned to poll() resume call site\n", .{ cqe.user_data });
            }
            std.debug.print("submitting {} sqe(s) and waiting...\n", .{ self.ring.sq_ready() });
            _ = try self.ring.submit_and_wait(1);
        }
    }

    pub fn accept(self: *IO, fd: os.fd_t, addr: *os.sockaddr, addr_len: *os.socklen_t) !os.fd_t {
        var completion = IO.Completion{ .frame = @frame() };
        _ = try self.ring.accept(@ptrToInt(&completion), fd, addr, addr_len, os.SOCK_CLOEXEC);
        suspend;
        if (completion.result < 0) return os.unexpectedErrno(@intCast(usize, -completion.result));
        return completion.result;
    }

    pub fn recv(self: *IO, fd: os.fd_t, buffer: []u8) !usize {
        var completion = IO.Completion{ .frame = @frame() };
        _ = try self.ring.recv(@ptrToInt(&completion), fd, buffer, os.MSG_NOSIGNAL);
        suspend;
        if (completion.result < 0) return os.unexpectedErrno(@intCast(usize, -completion.result));
        return @intCast(usize, completion.result);
    }
};

pub fn main() !void {
    var server = try server_listen(3001);
    defer std.os.close(server);

    var io = try IO.init();
    defer io.deinit();

    _ = async server_accept_loop(&io, server);

    while (true) {
        try io.poll();
    }
}
