const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

const FIFO = @import("fifo.zig").FIFO;

pub const IO = struct {
    ring: IO_Uring,

    /// Operations not yet submitted to the kernel and waiting on available space in the
    /// submission queue.
    unqueued: FIFO(Completion) = .{},

    /// Completions that are ready to have their callbacks run.
    completed: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        return IO{ .ring = try IO_Uring.init(entries, flags) };
    }

    pub fn deinit(self: *IO) void {
        self.ring.deinit();
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        // We assume that all timeouts submitted by `run_for_ns()` will be reaped by `run_for_ns()`
        // and that `tick()` and `run_for_ns()` cannot be run concurrently.
        // Therefore `timeouts` here will never be decremented and `etime` will always be false.
        var timeouts: usize = 0;
        var etime = false;

        try self.flush(0, &timeouts, &etime);
        assert(etime == false);

        // Flush any SQEs that were queued while running completion callbacks in `flush()`:
        // This is an optimization to avoid delaying submissions until the next tick.
        // At the same time, we do not flush any ready CQEs since SQEs may complete synchronously.
        // We guard against an io_uring_enter() syscall if we know we do not have any queued SQEs.
        // We cannot use `self.ring.sq_ready()` here since this counts flushed and unflushed SQEs.
        const queued = self.ring.sq.sqe_tail -% self.ring.sq.sqe_head;
        if (queued > 0) {
            try self.flush_submissions(0, &timeouts, &etime);
            assert(etime == false);
        }
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// in the __kernel_timespec struct.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        var current_ts: os.timespec = undefined;
        // Any kernel that supports io_uring supports CLOCK_MONOTONIC as well:
        os.clock_gettime(os.CLOCK_MONOTONIC, &current_ts) catch unreachable;
        // The absolute CLOCK_MONOTONIC time after which we may return from this function:
        const timeout_ts: os.__kernel_timespec = .{
            .tv_sec = current_ts.tv_sec,
            .tv_nsec = current_ts.tv_nsec + nanoseconds,
        };
        var timeouts: usize = 0;
        var etime = false;
        while (!etime) {
            const timeout_sqe = self.ring.get_sqe() catch blk: {
                // The submission queue is full, so flush submissions to make space:
                try self.flush_submissions(0, &timeouts, &etime);
                break :blk self.ring.get_sqe() catch unreachable;
            };
            // Submit an absolute timeout that will be canceled if any other SQE completes first:
            linux.io_uring_prep_timeout(timeout_sqe, &timeout_ts, 1, os.IORING_TIMEOUT_ABS);
            timeout_sqe.user_data = 0;
            timeouts += 1;
            // The amount of time this call will block is bounded by the timeout we just submitted:
            try self.flush(1, &timeouts, &etime);
        }
        // Reap any remaining timeouts, which reference the timespec in the current stack frame.
        // The busy loop here is required to avoid a potential deadlock, as the kernel determines
        // when the timeouts are pushed to the completion queue, not us.
        while (timeouts > 0) _ = try self.flush_completions(0, &timeouts, &etime);
    }

    fn flush(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
        try self.flush_submissions(wait_nr, timeouts, etime);
        // We can now just peek for any CQEs without waiting and without another syscall:
        try self.flush_completions(0, timeouts, etime);
        // Run completions only after all completions have been flushed:
        // Loop on a copy of the linked list, having reset the list first, so that any synchronous
        // append on running a completion is executed only the next time round the event loop,
        // without creating an infinite loop.
        {
            var copy = self.completed;
            self.completed = .{};
            while (copy.pop()) |completion| completion.complete();
        }
        // Again, loop on a copy of the list to avoid an infinite loop:
        {
            var copy = self.unqueued;
            self.unqueued = .{};
            while (copy.pop()) |completion| self.enqueue(completion);
        }
    }

    fn flush_completions(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        var cqes: [256]io_uring_cqe = undefined;
        var wait_remaining = wait_nr;
        while (true) {
            // Guard against waiting indefinitely (if there are too few requests inflight),
            // especially if this is not the first time round the loop:
            const completed = self.ring.copy_cqes(&cqes, wait_remaining) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            if (completed > wait_remaining) wait_remaining = 0 else wait_remaining -= completed;
            for (cqes[0..completed]) |cqe| {
                if (cqe.user_data == 0) {
                    timeouts.* -= 1;
                    // We are only done if the timeout submitted was completed due to time, not if
                    // it was completed due to the completion of an event, in which case `cqe.res`
                    // would be 0. It is possible for multiple timeout operations to complete at the
                    // same time if the nanoseconds value passed to `run_for_ns()` is very short.
                    if (-cqe.res == os.ETIME) etime.* = true;
                    continue;
                }
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                completion.result = cqe.res;
                // We do not run the completion here (instead appending to a linked list) to avoid:
                // * recursion through `flush_submissions()` and `flush_completions()`,
                // * unbounded stack usage, and
                // * confusing stack traces.
                self.completed.push(completion);
            }
            if (completed < cqes.len) break;
        }
    }

    fn flush_submissions(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        while (true) {
            _ = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                // Wait for some completions and then try again:
                // See https://github.com/axboe/liburing/issues/281 re: error.SystemResources.
                // Be careful also that copy_cqes() will flush before entering to wait (it does):
                // https://github.com/axboe/liburing/commit/35c199c48dfd54ad46b96e386882e7ac341314c5
                error.CompletionQueueOvercommitted, error.SystemResources => {
                    try self.flush_completions(1, timeouts, etime);
                    continue;
                },
                else => return err,
            };
            break;
        }
    }

    fn enqueue(self: *IO, completion: *Completion) void {
        const sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => {
                self.unqueued.push(completion);
                return;
            },
        };
        completion.prep(sqe);
    }

    /// This struct holds the data needed for a single io_uring operation
    pub const Completion = struct {
        io: *IO,
        result: i32 = undefined,
        next: ?*Completion = null,
        operation: Operation,
        // This is one of the usecases for c_void outside of C code and as such c_void will
        // be replaced with anyopaque eventually: https://github.com/ziglang/zig/issues/323
        context: ?*c_void,
        callback: fn (context: ?*c_void, completion: *Completion, result: *const c_void) void,

        fn prep(completion: *Completion, sqe: *io_uring_sqe) void {
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
                .close => |op| {
                    linux.io_uring_prep_close(sqe, op.fd);
                },
                .connect => |*op| {
                    linux.io_uring_prep_connect(
                        sqe,
                        op.socket,
                        &op.address.any,
                        op.address.getOsSockLen(),
                    );
                },
                .fsync => |op| {
                    linux.io_uring_prep_fsync(sqe, op.fd, op.flags);
                },
                .openat => |op| {
                    linux.io_uring_prep_openat(sqe, op.fd, op.path, op.flags, op.mode);
                },
                .read => |op| {
                    linux.io_uring_prep_read(
                        sqe,
                        op.fd,
                        op.buffer[0..buffer_limit(op.buffer.len)],
                        op.offset,
                    );
                },
                .recv => |op| {
                    linux.io_uring_prep_recv(sqe, op.socket, op.buffer, op.flags);
                },
                .send => |op| {
                    linux.io_uring_prep_send(sqe, op.socket, op.buffer, op.flags);
                },
                .timeout => |*op| {
                    linux.io_uring_prep_timeout(sqe, &op.timespec, 0, 0);
                },
                .write => |op| {
                    linux.io_uring_prep_write(
                        sqe,
                        op.fd,
                        op.buffer[0..buffer_limit(op.buffer.len)],
                        op.offset,
                    );
                },
            }
            sqe.user_data = @ptrToInt(completion);
        }

        fn complete(completion: *Completion) void {
            switch (completion.operation) {
                .accept => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
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
                    } else @intCast(os.socket_t, completion.result);
                    completion.callback(completion.context, completion, &result);
                },
                .close => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425
                        os.EBADF => error.FileDescriptorInvalid,
                        os.EDQUOT => error.DiskQuota,
                        os.EIO => error.InputOutput,
                        os.ENOSPC => error.NoSpaceLeft,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else assert(completion.result == 0);
                    completion.callback(completion.context, completion, &result);
                },
                .connect => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.EACCES => error.AccessDenied,
                        os.EADDRINUSE => error.AddressInUse,
                        os.EADDRNOTAVAIL => error.AddressNotAvailable,
                        os.EAFNOSUPPORT => error.AddressFamilyNotSupported,
                        os.EAGAIN, os.EINPROGRESS => error.WouldBlock,
                        os.EALREADY => error.OpenAlreadyInProgress,
                        os.EBADF => error.FileDescriptorInvalid,
                        os.ECONNREFUSED => error.ConnectionRefused,
                        os.ECONNRESET => error.ConnectionResetByPeer,
                        os.EFAULT => unreachable,
                        os.EISCONN => error.AlreadyConnected,
                        os.ENETUNREACH => error.NetworkUnreachable,
                        os.ENOENT => error.FileNotFound,
                        os.ENOTSOCK => error.FileDescriptorNotASocket,
                        os.EPERM => error.PermissionDenied,
                        os.EPROTOTYPE => error.ProtocolNotSupported,
                        os.ETIMEDOUT => error.ConnectionTimedOut,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else assert(completion.result == 0);
                    completion.callback(completion.context, completion, &result);
                },
                .fsync => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.EBADF => error.FileDescriptorInvalid,
                        os.EDQUOT => error.DiskQuota,
                        os.EINVAL => error.ArgumentsInvalid,
                        os.EIO => error.InputOutput,
                        os.ENOSPC => error.NoSpaceLeft,
                        os.EROFS => error.ReadOnlyFileSystem,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else assert(completion.result == 0);
                    completion.callback(completion.context, completion, &result);
                },
                .openat => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
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
                    } else @intCast(os.fd_t, completion.result);
                    completion.callback(completion.context, completion, &result);
                },
                .read => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.EAGAIN => error.WouldBlock,
                        os.EBADF => error.NotOpenForReading,
                        os.ECONNRESET => error.ConnectionResetByPeer,
                        os.EFAULT => unreachable,
                        os.EINVAL => error.Alignment,
                        os.EIO => error.InputOutput,
                        os.EISDIR => error.IsDir,
                        os.ENOBUFS => error.SystemResources,
                        os.ENOMEM => error.SystemResources,
                        os.ENXIO => error.Unseekable,
                        os.EOVERFLOW => error.Unseekable,
                        os.ESPIPE => error.Unseekable,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else @intCast(usize, completion.result);
                    completion.callback(completion.context, completion, &result);
                },
                .recv => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.EAGAIN => error.WouldBlock,
                        os.EBADF => error.FileDescriptorInvalid,
                        os.ECONNREFUSED => error.ConnectionRefused,
                        os.EFAULT => unreachable,
                        os.EINVAL => unreachable,
                        os.ENOMEM => error.SystemResources,
                        os.ENOTCONN => error.SocketNotConnected,
                        os.ENOTSOCK => error.FileDescriptorNotASocket,
                        os.ECONNRESET => error.ConnectionResetByPeer,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else @intCast(usize, completion.result);
                    completion.callback(completion.context, completion, &result);
                },
                .send => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.EACCES => error.AccessDenied,
                        os.EAGAIN => error.WouldBlock,
                        os.EALREADY => error.FastOpenAlreadyInProgress,
                        os.EAFNOSUPPORT => error.AddressFamilyNotSupported,
                        os.EBADF => error.FileDescriptorInvalid,
                        os.ECONNRESET => error.ConnectionResetByPeer,
                        os.EDESTADDRREQ => unreachable,
                        os.EFAULT => unreachable,
                        os.EINVAL => unreachable,
                        os.EISCONN => unreachable,
                        os.EMSGSIZE => error.MessageTooBig,
                        os.ENOBUFS => error.SystemResources,
                        os.ENOMEM => error.SystemResources,
                        os.ENOTCONN => error.SocketNotConnected,
                        os.ENOTSOCK => error.FileDescriptorNotASocket,
                        os.EOPNOTSUPP => error.OperationNotSupported,
                        os.EPIPE => error.BrokenPipe,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else @intCast(usize, completion.result);
                    completion.callback(completion.context, completion, &result);
                },
                .timeout => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.ECANCELED => error.Canceled,
                        os.ETIME => {}, // A success.
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else unreachable;
                    completion.callback(completion.context, completion, &result);
                },
                .write => {
                    const result = if (completion.result < 0) switch (-completion.result) {
                        os.EINTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        os.EAGAIN => error.WouldBlock,
                        os.EBADF => error.NotOpenForWriting,
                        os.EDESTADDRREQ => error.NotConnected,
                        os.EDQUOT => error.DiskQuota,
                        os.EFAULT => unreachable,
                        os.EFBIG => error.FileTooBig,
                        os.EINVAL => error.Alignment,
                        os.EIO => error.InputOutput,
                        os.ENOSPC => error.NoSpaceLeft,
                        os.ENXIO => error.Unseekable,
                        os.EOVERFLOW => error.Unseekable,
                        os.EPERM => error.AccessDenied,
                        os.EPIPE => error.BrokenPipe,
                        os.ESPIPE => error.Unseekable,
                        else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                    } else @intCast(usize, completion.result);
                    completion.callback(completion.context, completion, &result);
                },
            }
        }
    };

    /// This union encodes the set of operations supported as well as their arguments.
    const Operation = union(enum) {
        accept: struct {
            socket: os.socket_t,
            address: os.sockaddr = undefined,
            address_size: os.socklen_t = @sizeOf(os.sockaddr),
            flags: u32,
        },
        close: struct {
            fd: os.fd_t,
        },
        connect: struct {
            socket: os.socket_t,
            address: std.net.Address,
        },
        fsync: struct {
            fd: os.fd_t,
            flags: u32,
        },
        openat: struct {
            fd: os.fd_t,
            path: [*:0]const u8,
            flags: u32,
            mode: os.mode_t,
        },
        read: struct {
            fd: os.fd_t,
            buffer: []u8,
            offset: u64,
        },
        recv: struct {
            socket: os.socket_t,
            buffer: []u8,
            flags: u32,
        },
        send: struct {
            socket: os.socket_t,
            buffer: []const u8,
            flags: u32,
        },
        timeout: struct {
            timespec: os.__kernel_timespec,
        },
        write: struct {
            fd: os.fd_t,
            buffer: []const u8,
            offset: u64,
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
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!os.socket_t,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        flags: u32,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
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
                    .address = undefined,
                    .address_size = @sizeOf(os.sockaddr),
                    .flags = flags,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || os.UnexpectedError;

    pub fn close(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
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
        self.enqueue(completion);
    }

    pub const ConnectError = error{
        AccessDenied,
        AddressInUse,
        AddressNotAvailable,
        AddressFamilyNotSupported,
        WouldBlock,
        OpenAlreadyInProgress,
        FileDescriptorInvalid,
        ConnectionRefused,
        AlreadyConnected,
        NetworkUnreachable,
        FileNotFound,
        FileDescriptorNotASocket,
        PermissionDenied,
        ProtocolNotSupported,
        ConnectionTimedOut,
    } || os.UnexpectedError;

    pub fn connect(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        address: std.net.Address,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const ConnectError!void, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .connect = .{
                    .socket = socket,
                    .address = address,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const FsyncError = error{
        FileDescriptorInvalid,
        DiskQuota,
        ArgumentsInvalid,
        InputOutput,
        NoSpaceLeft,
        ReadOnlyFileSystem,
    } || os.UnexpectedError;

    pub fn fsync(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: FsyncError!void,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        flags: u32,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const FsyncError!void, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .fsync = .{
                    .fd = fd,
                    .flags = flags,
                },
            },
        };
        self.enqueue(completion);
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
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: OpenatError!os.fd_t,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        path: [*:0]const u8,
        flags: u32,
        mode: os.mode_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
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
        self.enqueue(completion);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
    } || os.UnexpectedError;

    pub fn read(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const ReadError!usize, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const RecvError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionRefused,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
    } || os.UnexpectedError;

    pub fn recv(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        buffer: []u8,
        flags: u32,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const RecvError!usize, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                    .flags = flags,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const SendError = error{
        AccessDenied,
        WouldBlock,
        FastOpenAlreadyInProgress,
        AddressFamilyNotSupported,
        FileDescriptorInvalid,
        ConnectionResetByPeer,
        MessageTooBig,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        OperationNotSupported,
        BrokenPipe,
    } || os.UnexpectedError;

    pub fn send(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        buffer: []const u8,
        flags: u32,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const SendError!usize, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                    .flags = flags,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const TimeoutError = error{Canceled} || os.UnexpectedError;

    pub fn timeout(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const TimeoutError!void, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .timeout = .{
                    .timespec = .{ .tv_sec = 0, .tv_nsec = nanoseconds },
                },
            },
        };
        self.enqueue(completion);
    }

    pub const WriteError = error{
        WouldBlock,
        NotOpenForWriting,
        NotConnected,
        DiskQuota,
        FileTooBig,
        Alignment,
        InputOutput,
        NoSpaceLeft,
        Unseekable,
        AccessDenied,
        BrokenPipe,
    } || os.UnexpectedError;

    pub fn write(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*c_void, comp: *Completion, res: *const c_void) void {
                    callback(
                        @intToPtr(Context, @ptrToInt(ctx)),
                        comp,
                        @intToPtr(*const WriteError!usize, @ptrToInt(res)).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
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

test "ref all decls" {
    std.testing.refAllDecls(IO);
}

test "write/fsync/read" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
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
            try self.io.run();

            testing.expectEqual(self.write_buf.len, self.written);
            testing.expect(self.fsynced);
            testing.expectEqual(self.read_buf.len, self.read);
            testing.expectEqualSlices(u8, &self.write_buf, &self.read_buf);
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
        }
    }.run_test();
}

test "openat/close" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
        fd: os.fd_t = 0,
        closed: bool = false,

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
            try self.io.run();
            testing.expect(self.fd > 0);
            testing.expect(self.closed);
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
            self.closed = true;
        }
    }.run_test();
}

test "accept/connect/send/receive" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
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

            try self.io.run();

            testing.expectEqual(self.send_buf.len, self.sent);
            testing.expectEqual(self.recv_buf.len, self.received);

            testing.expectEqualSlices(u8, self.send_buf[0..self.received], &self.recv_buf);
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
                os.MSG_NOSIGNAL,
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
                os.MSG_NOSIGNAL,
            );
        }

        fn recv_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            self.received = result catch @panic("recv error");
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
            try self.io.run();

            testing.expectEqual(@as(u32, count), self.count);

            testing.expectApproxEqAbs(
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
            try self.io.run();

            testing.expectEqual(@as(u32, count), self.count);
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
