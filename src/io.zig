const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

pub const IO = struct {
    ring: IO_Uring,

    /// The number of SQEs queued but not yet submitted to the kernel:
    queued: u32 = 0,

    /// The number of SQEs submitted and inflight but not yet completed:
    submitted: u32 = 0,

    /// A linked list of completions that are ready to resume (FIFO):
    completed_head: ?*Completion = null,
    completed_tail: ?*Completion = null,

    const Completion = struct {
        frame: anyframe,
        result: i32 = undefined,
        next: ?*Completion = null,
    };

    pub fn init(entries: u12, flags: u32) !IO {
        return IO{ .ring = try IO_Uring.init(entries, flags) };
    }

    pub fn deinit(self: *IO) void {
        self.ring.deinit();
    }

    pub fn run(self: *IO) !void {
        // Run the event loop while there is IO pending:
        while (self.queued + self.submitted > 0 or self.completed_head != null) {
            // We already use `io_uring_enter()` to submit SQEs so reuse that to wait for CQEs:
            try self.flush_submissions(true);
            // We can now just peek for any CQEs without waiting, and without another syscall:
            try self.flush_completions(false);
            // Resume completion frames only after all completions have been flushed:
            // Loop on a copy of the linked list, having reset the linked list first, so that any
            // synchronous append on resume is executed only the next time round the event loop,
            // without creating an infinite suspend/resume cycle within `while (head)`.
            var head = self.completed_head;
            self.completed_head = null;
            self.completed_tail = null;
            while (head) |completion| {
                head = completion.next;
                resume completion.frame;
            }
        }
        assert(self.completed_head == null);
        assert(self.completed_tail == null);
    }

    fn append_completion(self: *IO, completion: *Completion) void {
        assert(completion.next == null);
        if (self.completed_head == null) {
            assert(self.completed_tail == null);
            self.completed_head = completion;
            self.completed_tail = completion;
        } else {
            self.completed_tail.?.next = completion;
            self.completed_tail = completion;
        }
    }

    fn flush_completions(self: *IO, wait: bool) !void {
        var cqes: [256]io_uring_cqe = undefined;
        var wait_nr: u32 = if (wait) 1 else 0;
        while (true) {
            // Guard against waiting indefinitely (if there are too few requests inflight),
            // especially if this is not the first time round the loop:
            wait_nr = std.math.min(self.submitted, wait_nr);
            const completed = self.ring.copy_cqes(&cqes, wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            self.submitted -= completed;
            for (cqes[0..completed]) |cqe| {
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                completion.result = cqe.res;
                completion.next = null;
                // We do not resume the completion frame here (instead appending to a linked list):
                // * to avoid recursion through `flush_submissions()` and `flush_completions()`,
                // * to avoid unbounded stack usage, and
                // * to avoid confusing stack traces.
                self.append_completion(completion);
            }
            if (completed < cqes.len) break;
        }
    }

    fn flush_submissions(self: *IO, wait: bool) !void {
        var wait_nr: u32 = if (wait) 1 else 0;
        while (true) {
            wait_nr = std.math.min(self.queued + self.submitted, wait_nr);
            _ = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                // Wait for some completions and then try again:
                // See https://github.com/axboe/liburing/issues/281 re: error.SystemResources.
                // Be careful also that copy_cqes() will flush before entering to wait (it does):
                // https://github.com/axboe/liburing/commit/35c199c48dfd54ad46b96e386882e7ac341314c5
                error.CompletionQueueOvercommitted, error.SystemResources => {
                    try self.flush_completions(true);
                    continue;
                },
                else => return err,
            };
            self.submitted += self.queued;
            self.queued = 0;
            break;
        }
    }

    fn get_sqe(self: *IO) *io_uring_sqe {
        while (true) {
            const sqe = self.ring.get_sqe() catch |err| switch (err) {
                error.SubmissionQueueFull => {
                    var completion = Completion{ .frame = @frame(), .result = 0 };
                    self.append_completion(&completion);
                    suspend;
                    continue;
                },
            };
            self.queued += 1;
            return sqe;
        }
    }

    pub fn accept(
        self: *IO,
        socket: os.socket_t,
        address: *os.sockaddr,
        address_size: *os.socklen_t,
    ) !os.socket_t {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_accept(sqe, socket, address, address_size, os.SOCK_CLOEXEC);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EAGAIN => return error.WouldBlock,
                    os.EBADF => return error.FileDescriptorInvalid,
                    os.ECONNABORTED => return error.ConnectionAborted,
                    os.EFAULT => unreachable,
                    os.EINVAL => return error.SocketNotListening,
                    os.EMFILE => return error.ProcessFdQuotaExceeded,
                    os.ENFILE => return error.SystemFdQuotaExceeded,
                    os.ENOBUFS => return error.SystemResources,
                    os.ENOMEM => return error.SystemResources,
                    os.ENOTSOCK => return error.FileDescriptorNotASocket,
                    os.EOPNOTSUPP => return error.OperationNotSupported,
                    os.EPERM => return error.PermissionDenied,
                    os.EPROTO => return error.ProtocolFailure,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                return @intCast(os.socket_t, completion.result);
            }
        }
    }

    pub fn close(self: *IO, fd: os.fd_t) !void {
        var completion = Completion{ .frame = @frame() };
        const sqe = self.get_sqe();
        linux.io_uring_prep_close(sqe, fd);
        sqe.user_data = @ptrToInt(&completion);
        suspend;
        if (completion.result < 0) {
            switch (-completion.result) {
                os.EINTR => return, // A success, see https://github.com/ziglang/zig/issues/2425.
                os.EBADF => return error.FileDescriptorInvalid,
                os.EDQUOT => return error.DiskQuota,
                os.EIO => return error.InputOutput,
                os.ENOSPC => return error.NoSpaceLeft,
                else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
            }
        } else {
            assert(completion.result == 0);
        }
    }

    pub fn connect(
        self: *IO,
        socket: os.socket_t,
        address: *const os.sockaddr,
        address_size: os.socklen_t,
    ) !void {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_connect(sqe, socket, address, address_size);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EACCES => return error.AccessDenied,
                    os.EADDRINUSE => return error.AddressInUse,
                    os.EADDRNOTAVAIL => return error.AddressNotAvailable,
                    os.EAFNOSUPPORT => return error.AddressFamilyNotSupported,
                    os.EAGAIN, os.EINPROGRESS => return error.WouldBlock,
                    os.EALREADY => return error.OpenAlreadyInProgress,
                    os.EBADF => return error.FileDescriptorInvalid,
                    os.ECONNREFUSED => return error.ConnectionRefused,
                    os.EFAULT => unreachable,
                    os.EISCONN => return error.AlreadyConnected,
                    os.ENETUNREACH => return error.NetworkUnreachable,
                    os.ENOENT => return error.FileNotFound,
                    os.ENOTSOCK => return error.FileDescriptorNotASocket,
                    os.EPERM => return error.PermissionDenied,
                    os.EPROTOTYPE => return error.ProtocolNotSupported,
                    os.ETIMEDOUT => return error.ConnectionTimedOut,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                assert(completion.result == 0);
                return;
            }
        }
    }

    pub fn fsync(self: *IO, fd: os.fd_t) !void {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_fsync(sqe, fd, 0);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EBADF => return error.FileDescriptorInvalid,
                    os.EDQUOT => return error.DiskQuota,
                    os.EINVAL => return error.ArgumentsInvalid,
                    os.EIO => return error.InputOutput,
                    os.ENOSPC => return error.NoSpaceLeft,
                    os.EROFS => return error.ReadOnlyFileSystem,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                assert(completion.result == 0);
                return;
            }
        }
    }

    pub fn openat(
        self: *IO,
        dir_fd: os.fd_t,
        pathname: []const u8,
        flags: u32,
        mode: os.mode_t,
    ) !os.fd_t {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const pathname_c = try os.toPosixPath(pathname);
            const sqe = self.get_sqe();
            linux.io_uring_prep_openat(sqe, dir_fd, &pathname_c, flags, mode);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EACCES => return error.AccessDenied,
                    os.EBADF => return error.FileDescriptorInvalid,
                    os.EBUSY => return error.DeviceBusy,
                    os.EEXIST => return error.PathAlreadyExists,
                    os.EFAULT => unreachable,
                    os.EFBIG => return error.FileTooBig,
                    os.EINVAL => return error.ArgumentsInvalid,
                    os.EISDIR => return error.IsDir,
                    os.ELOOP => return error.SymLinkLoop,
                    os.EMFILE => return error.ProcessFdQuotaExceeded,
                    os.ENAMETOOLONG => return error.NameTooLong,
                    os.ENFILE => return error.SystemFdQuotaExceeded,
                    os.ENODEV => return error.NoDevice,
                    os.ENOENT => return error.FileNotFound,
                    os.ENOMEM => return error.SystemResources,
                    os.ENOSPC => return error.NoSpaceLeft,
                    os.ENOTDIR => return error.NotDir,
                    os.EOPNOTSUPP => return error.FileLocksNotSupported,
                    os.EOVERFLOW => return error.FileTooBig,
                    os.EPERM => return error.AccessDenied,
                    os.EWOULDBLOCK => return error.WouldBlock,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                return @intCast(os.fd_t, completion.result);
            }
        }
    }

    pub fn read(self: *IO, fd: os.fd_t, buffer: []u8, offset: u64) !usize {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_read(sqe, fd, buffer[0..buffer_limit(buffer.len)], offset);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EAGAIN => return error.WouldBlock,
                    os.EBADF => return error.NotOpenForReading,
                    os.ECONNRESET => return error.ConnectionResetByPeer,
                    os.EFAULT => unreachable,
                    os.EINVAL => return error.Alignment,
                    os.EIO => return error.InputOutput,
                    os.EISDIR => return error.IsDir,
                    os.ENOBUFS => return error.SystemResources,
                    os.ENOMEM => return error.SystemResources,
                    os.ENXIO => return error.Unseekable,
                    os.EOVERFLOW => return error.Unseekable,
                    os.ESPIPE => return error.Unseekable,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                return @intCast(usize, completion.result);
            }
        }
    }

    pub fn recv(self: *IO, socket: os.socket_t, buffer: []u8) !usize {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_recv(sqe, socket, buffer, os.MSG_NOSIGNAL);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EAGAIN => return error.WouldBlock,
                    os.EBADF => return error.FileDescriptorInvalid,
                    os.ECONNREFUSED => return error.ConnectionRefused,
                    os.EFAULT => unreachable,
                    os.EINVAL => unreachable,
                    os.ENOMEM => return error.SystemResources,
                    os.ENOTCONN => return error.SocketNotConnected,
                    os.ENOTSOCK => return error.FileDescriptorNotASocket,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                return @intCast(usize, completion.result);
            }
        }
    }

    pub fn send(self: *IO, socket: os.socket_t, buffer: []const u8) !usize {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_send(sqe, socket, buffer, os.MSG_NOSIGNAL);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EACCES => return error.AccessDenied,
                    os.EAGAIN => return error.WouldBlock,
                    os.EALREADY => return error.FastOpenAlreadyInProgress,
                    os.EAFNOSUPPORT => return error.AddressFamilyNotSupported,
                    os.EBADF => return error.FileDescriptorInvalid,
                    os.ECONNRESET => return error.ConnectionResetByPeer,
                    os.EDESTADDRREQ => unreachable,
                    os.EFAULT => unreachable,
                    os.EINVAL => unreachable,
                    os.EISCONN => unreachable,
                    os.EMSGSIZE => return error.MessageTooBig,
                    os.ENOBUFS => return error.SystemResources,
                    os.ENOMEM => return error.SystemResources,
                    os.ENOTCONN => return error.SocketNotConnected,
                    os.ENOTSOCK => return error.FileDescriptorNotASocket,
                    os.EOPNOTSUPP => return error.OperationNotSupported,
                    os.EPIPE => return error.BrokenPipe,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                return @intCast(usize, completion.result);
            }
        }
    }

    pub fn sleep(self: *IO, nanoseconds: u64) !void {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const ts: os.__kernel_timespec = .{
                .tv_sec = 0,
                .tv_nsec = @intCast(i64, nanoseconds),
            };
            const sqe = self.get_sqe();
            linux.io_uring_prep_timeout(sqe, &ts, 0, 0);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.ECANCELED => return error.Canceled,
                    os.ETIME => return, // A success.
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                unreachable;
            }
        }
    }

    pub fn write(self: *IO, fd: os.fd_t, buffer: []const u8, offset: u64) !usize {
        while (true) {
            var completion = Completion{ .frame = @frame() };
            const sqe = self.get_sqe();
            linux.io_uring_prep_write(sqe, fd, buffer[0..buffer_limit(buffer.len)], offset);
            sqe.user_data = @ptrToInt(&completion);
            suspend;
            if (completion.result < 0) {
                switch (-completion.result) {
                    os.EINTR => continue,
                    os.EAGAIN => return error.WouldBlock,
                    os.EBADF => return error.NotOpenForWriting,
                    os.EDESTADDRREQ => return error.NotConnected,
                    os.EDQUOT => return error.DiskQuota,
                    os.EFAULT => unreachable,
                    os.EFBIG => return error.FileTooBig,
                    os.EINVAL => return error.Alignment,
                    os.EIO => return error.InputOutput,
                    os.ENOSPC => return error.NoSpaceLeft,
                    os.ENXIO => return error.Unseekable,
                    os.EOVERFLOW => return error.Unseekable,
                    os.EPERM => return error.AccessDenied,
                    os.EPIPE => return error.BrokenPipe,
                    os.ESPIPE => return error.Unseekable,
                    else => |errno| return os.unexpectedErrno(@intCast(usize, errno)),
                }
            } else {
                return @intCast(usize, completion.result);
            }
        }
    }
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

const testing = std.testing;

fn test_write_fsync_read(io: *IO) !void {
    const path = "test_io_write_fsync_read";
    const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};
    const fd = file.handle;

    const buffer_write = [_]u8{97} ** 20;
    var buffer_read = [_]u8{98} ** 20;

    const bytes_written = try io.write(fd, buffer_write[0..], 10);
    testing.expectEqual(@as(usize, buffer_write.len), bytes_written);

    try io.fsync(fd);

    const bytes_read = try io.read(fd, buffer_read[0..], 10);
    testing.expectEqual(@as(usize, buffer_read.len), bytes_read);

    testing.expectEqualSlices(u8, buffer_write[0..], buffer_read[0..]);
}

fn test_openat_close(io: *IO) !void {
    const path = "test_io_openat_close";
    defer std.fs.cwd().deleteFile(path) catch {};

    const fd = try io.openat(linux.AT_FDCWD, path, os.O_CLOEXEC | os.O_RDWR | os.O_CREAT, 0o666);
    defer io.close(fd) catch unreachable;
    testing.expect(fd > 0);
}

fn test_sleep(io: *IO) !void {
    {
        const ms = 100;
        const margin = 5;

        const started = std.time.milliTimestamp();
        try io.sleep(ms * std.time.ns_per_ms);
        const stopped = std.time.milliTimestamp();

        testing.expectApproxEqAbs(@as(f64, ms), @intToFloat(f64, stopped - started), margin);
    }
    {
        const frames = try testing.allocator.alloc(@Frame(test_sleep_coroutine), 10);
        defer testing.allocator.free(frames);

        const ms = 27;
        const margin = 5;
        var count: usize = 0;

        const started = std.time.milliTimestamp();
        for (frames) |*frame| {
            frame.* = async test_sleep_coroutine(io, ms, &count);
        }
        for (frames) |*frame| {
            try await frame;
        }
        const stopped = std.time.milliTimestamp();

        testing.expect(count == frames.len);
        testing.expectApproxEqAbs(@as(f64, ms), @intToFloat(f64, stopped - started), margin);
    }
}

fn test_sleep_coroutine(io: *IO, ms: u64, count: *usize) !void {
    try io.sleep(ms * std.time.ns_per_ms);
    count.* += 1;
}

fn test_accept_connect_send_receive(io: *IO) !void {
    const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
    const kernel_backlog = 1;
    const server = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
    defer io.close(server) catch unreachable;
    try os.setsockopt(server, os.SOL_SOCKET, os.SO_REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try os.bind(server, &address.any, address.getOsSockLen());
    try os.listen(server, kernel_backlog);

    const client = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
    defer io.close(client) catch unreachable;

    const buffer_send = [_]u8{ 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 };
    var buffer_recv = [_]u8{ 0, 1, 0, 1, 0 };

    var accept_address: os.sockaddr = undefined;
    var accept_address_size: os.socklen_t = @sizeOf(@TypeOf(accept_address));

    var accept_frame = async io.accept(server, &accept_address, &accept_address_size);
    try io.connect(client, &address.any, address.getOsSockLen());
    var accept = try await accept_frame;
    defer io.close(accept) catch unreachable;

    const send_size = try io.send(client, buffer_send[0..]);
    testing.expectEqual(buffer_send.len, send_size);

    const recv_size = try io.recv(accept, buffer_recv[0..]);
    testing.expectEqual(buffer_recv.len, recv_size);

    testing.expectEqualSlices(u8, buffer_send[0..buffer_recv.len], buffer_recv[0..]);
}

fn test_submission_queue_full(io: *IO) !void {
    var a = async io.sleep(0);
    var b = async io.sleep(0);
    var c = async io.sleep(0);
    try await a;
    try await b;
    try await c;
}

fn test_run(entries: u12, comptime test_fn: anytype) void {
    var io = IO.init(entries, 0) catch unreachable;
    defer io.deinit();
    var frame = async test_fn(&io);
    io.run() catch unreachable;
    nosuspend await frame catch unreachable;
}

test "write/fsync/read" {
    test_run(32, test_write_fsync_read);
}

test "openat/close" {
    test_run(32, test_openat_close);
}

test "sleep" {
    test_run(32, test_sleep);
}

test "accept/connect/send/receive" {
    test_run(32, test_accept_connect_send_receive);
}

test "SubmissionQueueFull" {
    test_run(1, test_submission_queue_full);
}
