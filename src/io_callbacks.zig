const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

pub const IO = struct {
    /// An intrusive first in/first out linked list.
    const Fifo = struct {
        in: ?*Completion = null,
        out: ?*Completion = null,

        fn push(fifo: *Fifo, completion: *Completion) void {
            assert(completion.next == null);
            if (fifo.in) |in| {
                in.next = completion;
                fifo.in = completion;
            } else {
                assert(fifo.out == null);
                fifo.in = completion;
                fifo.out = completion;
            }
        }
    };

    ring: IO_Uring,

    /// Completions not yet submitted to the kernel and waiting on available space in the
    /// submission queue.
    unqueued: Fifo = .{},

    /// The number of SQEs queued but not yet submitted to the kernel:
    queued: u32 = 0,

    /// The number of SQEs submitted and inflight but not yet completed.
    submitted: u32 = 0,

    /// Completions that are ready to have their callbacks run.
    completed: Fifo = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        return IO{ .ring = try IO_Uring.init(entries, flags) };
    }

    pub fn deinit(self: *IO) void {
        self.ring.deinit();
    }

    /// Pass all queued submissions to the kernel and run the event loop
    /// until there is no longer any i/o pending.
    pub fn run(self: *IO) !void {
        while (self.queued + self.submitted > 0 or
            self.unqueued.out != null or self.completed.out != null)
        {
            // We already use `io_uring_enter()` to submit SQEs so reuse that to wait for CQEs:
            try self.flush_submissions(true);
            // We can now just peek for any CQEs without waiting, and without another syscall:
            try self.flush_completions(false);
            // Run completions only after all completions have been flushed:
            // Loop on a copy of the linked list, having reset the linked list first, so that any
            // synchronous append on running a completion is executed only the next time round
            // the event loop, without creating an infinite suspend/resume cycle.
            var out = self.completed.out;
            self.completed = .{};
            while (out) |completion| {
                out = completion.next;
                completion.run();
            }
            // Again, run on a copy of the list to avoid an infinite loop
            out = self.unqueued.out;
            self.unqueued = .{};
            while (out) |completion| {
                out = completion.next;
                self.get_sqe(completion);
            }
        }
        assert(self.unqueued.in == null);
        assert(self.unqueued.out == null);
        assert(self.completed.in == null);
        assert(self.completed.out == null);
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
                // We do not run the completion here (instead appending to a linked list):
                // * to avoid recursion through `flush_submissions()` and `flush_completions()`,
                // * to avoid unbounded stack usage, and
                // * to avoid confusing stack traces.
                self.completed.push(completion);
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

    fn get_sqe(self: *IO, completion: *Completion) void {
        const sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => {
                completion.next = null;
                self.unqueued.push(completion);
                return;
            },
        };
        self.queued += 1;
        completion.complete_prep(sqe);
    }

    /// This struct holds the data needed for a single io_uring operation
    pub const Completion = struct {
        io: *IO,
        result: i32 = undefined,
        next: ?*Completion = undefined,
        operation: Operation,
        // This is one of the usecases for c_void outside of C code and as such c_void will
        // be replaced with anyopaque eventually: https://github.com/ziglang/zig/issues/323
        context: ?*c_void,
        callback: fn (context: ?*c_void, completion: *Completion, result: *const c_void) callconv(.C) void,

        fn prep(completion: *Completion) void {
            completion.io.get_sqe(completion);
        }

        fn complete_prep(completion: *Completion, sqe: *io_uring_sqe) void {
            switch (completion.operation) {
                .accept => |*op| {
                    linux.io_uring_prep_accept(
                        sqe,
                        op.socket,
                        &op.address,
                        &op.address_size,
                        op.flags,
                    );
                },
                .close => |*op| {
                    linux.io_uring_prep_close(sqe, op.fd);
                },
                .openat => |*op| {
                    linux.io_uring_prep_openat(
                        sqe,
                        op.fd,
                        op.path,
                        op.flags,
                        op.mode,
                    );
                },
            }
            sqe.user_data = @ptrToInt(completion);
        }

        fn run(completion: *Completion) void {
            switch (completion.operation) {
                .accept => |op| {
                    const result = result: {
                        if (completion.result < 0) {
                            break :result switch (-completion.result) {
                                os.EINTR => {
                                    completion.prep();
                                    return;
                                },
                                os.EAGAIN => error.WouldBlock,
                                os.EBADF => error.FileDescriptorInvalid,
                                os.ECONNABORTED => error.ConnectionAborted,
                                os.EFAULT => unreachable,
                                os.EINVAL => error.SocketNotListening,
                                os.EMFILE => error.ProcessFdQuotaExceeded,
                                os.ENFILE => error.SystemFdQuotaExceeded,
                                os.ENOBUFS => error.SystemResources,
                                os.ENOMEM => error.SystemResources,
                                os.ENOTSOCK => error.FileDescriptorNotASocket,
                                os.EOPNOTSUPP => error.OperationNotSupported,
                                os.EPERM => error.PermissionDenied,
                                os.EPROTO => error.ProtocolFailure,
                                else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                            };
                        } else {
                            break :result @intCast(os.socket_t, completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .close => |op| {
                    const result = result: {
                        if (completion.result < 0) {
                            break :result switch (-completion.result) {
                                os.EINTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425.
                                os.EBADF => error.FileDescriptorInvalid,
                                os.EDQUOT => error.DiskQuota,
                                os.EIO => error.InputOutput,
                                os.ENOSPC => error.NoSpaceLeft,
                                else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                            };
                        } else {
                            assert(completion.result == 0);
                            break :result {};
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .openat => |op| {
                    const result = result: {
                        if (completion.result < 0) {
                            break :result switch (-completion.result) {
                                os.EINTR => {
                                    completion.prep();
                                    return;
                                },
                                os.EACCES => error.AccessDenied,
                                os.EBADF => error.FileDescriptorInvalid,
                                os.EBUSY => error.DeviceBusy,
                                os.EEXIST => error.PathAlreadyExists,
                                os.EFAULT => unreachable,
                                os.EFBIG => error.FileTooBig,
                                os.EINVAL => error.ArgumentsInvalid,
                                os.EISDIR => error.IsDir,
                                os.ELOOP => error.SymLinkLoop,
                                os.EMFILE => error.ProcessFdQuotaExceeded,
                                os.ENAMETOOLONG => error.NameTooLong,
                                os.ENFILE => error.SystemFdQuotaExceeded,
                                os.ENODEV => error.NoDevice,
                                os.ENOENT => error.FileNotFound,
                                os.ENOMEM => error.SystemResources,
                                os.ENOSPC => error.NoSpaceLeft,
                                os.ENOTDIR => error.NotDir,
                                os.EOPNOTSUPP => error.FileLocksNotSupported,
                                os.EOVERFLOW => error.FileTooBig,
                                os.EPERM => error.AccessDenied,
                                os.EWOULDBLOCK => error.WouldBlock,
                                else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                            };
                        } else {
                            break :result @intCast(os.fd_t, completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
            }
        }
    };

    /// This union encodes the set of operations supported as well as their arguments.
    const Operation = union(enum) {
        accept: struct {
            socket: os.fd_t,
            address: os.sockaddr,
            address_size: os.socklen_t,
            flags: u32,
        },
        close: struct {
            fd: os.fd_t,
        },
        openat: struct {
            fd: os.fd_t,
            path: [*:0]const u8,
            flags: u32,
            mode: os.mode_t,
        },
    };

    pub const AcceptError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionAborted,
        SocketNotListening,
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        FileDescriptorNotASocket,
        OperationNotSupported,
        PermissionDenied,
        ProtocolFailure,
    } || os.UnexpectedError;

    pub fn accept(
        self: *IO,
        completion: *Completion,
        comptime Context: type,
        context: Context,
        comptime callback: fn (context: Context, completion: *Completion, result: AcceptError!os.socket_t) void,
        socket: os.socket_t,
        address: os.sockaddr,
        address_size: os.socklen_t,
        flags: u32,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) callconv(.C) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const AcceptError!os.socket_t, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .accept = .{
                    .socket = socket,
                    .address = address,
                    .address_size = address_size,
                    .flags = flags,
                },
            },
        };
        completion.prep();
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || os.UnexpectedError;

    pub fn close(
        self: *IO,
        completion: *Completion,
        comptime Context: type,
        context: Context,
        comptime callback: fn (context: Context, completion: *Completion, result: CloseError!void) void,
        fd: os.fd_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) callconv(.C) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const CloseError!void, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .close = .{ .fd = fd },
            },
        };
        completion.prep();
    }

    pub const OpenatError = error{
        AccessDenied,
        FileDescriptorInvalid,
        DeviceBusy,
        PathAlreadyExists,
        FileTooBig,
        ArgumentsInvalid,
        IsDir,
        SymLinkLoop,
        ProcessFdQuotaExceeded,
        NameTooLong,
        SystemFdQuotaExceeded,
        NoDevice,
        FileNotFound,
        SystemResources,
        NoSpaceLeft,
        NotDir,
        FileLocksNotSupported,
        WouldBlock,
    } || os.UnexpectedError;

    pub fn openat(
        self: *IO,
        completion: *Completion,
        comptime Context: type,
        context: Context,
        comptime callback: fn (context: Context, completion: *Completion, result: OpenatError!os.fd_t) void,
        fd: os.fd_t,
        path: [*:0]const u8,
        flags: u32,
        mode: os.mode_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) callconv(.C) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const OpenatError!os.fd_t, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .openat = .{
                    .fd = fd,
                    .path = path,
                    .flags = flags,
                    .mode = mode,
                },
            },
        };
        completion.prep();
    }
};

test "openat/close" {
    const testing = std.testing;

    var io = try IO.init(32, 0);
    const path = "test_io_openat_close";
    defer std.fs.cwd().deleteFile(path) catch {};

    var fd: os.fd_t = 0;
    var completion: IO.Completion = undefined;
    io.openat(
        &completion,
        *os.fd_t,
        &fd,
        openat_callback,
        linux.AT_FDCWD,
        path,
        os.O_CLOEXEC | os.O_RDWR | os.O_CREAT,
        0o666,
    );

    try io.run();

    testing.expect(fd > 0);
}

fn openat_callback(context: *os.fd_t, completion: *IO.Completion, result: IO.OpenatError!os.fd_t) void {
    context.* = result catch @panic("openat error");
}
