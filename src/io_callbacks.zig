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

    /// Queue a command for submission to the kernel and register a callback.
    /// Memory for the Completion struct is provided by the caller and must be
    /// valid until the passed command's callback function is run.
    /// The value provided as the context argument will be passed to the
    /// command's callback function when it is run.
    pub fn submit(
        self: *IO,
        completion: *Completion,
        comptime Context: type,
        context: Context,
        command: Operation(Context),
    ) void {
        completion.* = .{
            .io = self,
            .result = undefined,
            .next = undefined,
            .context = @as(?*c_void, context),
            // TODO: this could be done with some meta programming. Not sure if it's worth the complexity though
            .command = switch (command) {
                .accept => |cmd| .{
                    .accept = .{
                        .socket = cmd.socket,
                        .address = cmd.address,
                        .address_size = cmd.address_size,
                        .flags = cmd.flags,
                        .callback = @ptrCast(fn (result: *const AcceptError!os.socket_t, context: ?*c_void) callconv(.C) void, cmd.callback),
                    },
                },
                .close => |cmd| .{
                    .close = .{
                        .fd = cmd.fd,
                        .callback = @ptrCast(fn (result: *const CloseError!void, context: ?*c_void) callconv(.C) void, cmd.callback),
                    },
                },
                .openat => |cmd| .{
                    .openat = .{
                        .fd = cmd.fd,
                        .path = cmd.path,
                        .flags = cmd.flags,
                        .mode = cmd.mode,
                        .callback = @ptrCast(fn (result: *const OpenatError!os.socket_t, context: ?*c_void) callconv(.C) void, cmd.callback),
                    },
                },
            },
        };
        completion.prep();
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
        result: i32,
        next: ?*Completion,
        // This is one of the usecases for c_void outside of C code and will be supported soon:
        // https://github.com/ziglang/zig/issues/323
        context: ?*c_void,
        command: Operation(?*c_void),

        fn prep(completion: *Completion) void {
            completion.io.get_sqe(completion);
        }

        fn complete_prep(completion: *Completion, sqe: *io_uring_sqe) void {
            switch (completion.command) {
                .accept => |*accept| {
                    linux.io_uring_prep_accept(
                        sqe,
                        accept.socket,
                        &accept.address,
                        &accept.address_size,
                        accept.flags,
                    );
                },
                .close => |*close| {
                    linux.io_uring_prep_close(sqe, close.fd);
                },
                .openat => |*openat| {
                    linux.io_uring_prep_openat(
                        sqe,
                        openat.fd,
                        openat.path,
                        openat.flags,
                        openat.mode,
                    );
                },
            }
            sqe.user_data = @ptrToInt(completion);
        }

        fn run(completion: *Completion) void {
            switch (completion.command) {
                .accept => |accept| {
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
                    accept.callback(&result, completion.context);
                },
                .close => |close| {
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
                    close.callback(&result, completion.context);
                },
                .openat => |openat| {
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
                    openat.callback(&result, completion.context);
                },
            }
        }
    };

    /// This union encodes the set of operations supported as well as their arguments.
    /// In addtion a callback with a generic Context is required which will be run on
    /// completion of the operation.
    /// The Context type must be a pointer type.
    /// Callbacks use the c calling convention to allow non-UB calling after type erasure.
    pub fn Operation(comptime Context: type) type {
        assert(@sizeOf(Context) == @sizeOf(?*c_void));
        assert(@alignOf(Context) == @alignOf(?*c_void));
        return union(enum) {
            accept: struct {
                socket: os.fd_t,
                address: os.sockaddr,
                address_size: os.socklen_t,
                flags: u32,
                callback: fn (result: *const AcceptError!os.socket_t, context: Context) callconv(.C) void,
            },
            close: struct {
                fd: os.fd_t,
                callback: fn (result: *const CloseError!void, context: Context) callconv(.C) void,
            },
            openat: struct {
                fd: os.fd_t,
                path: [*:0]const u8,
                flags: u32,
                mode: os.mode_t,
                callback: fn (result: *const OpenatError!os.fd_t, context: Context) callconv(.C) void,
            },
        };
    }

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

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || os.UnexpectedError;

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
};

test "openat/close" {
    const testing = std.testing;

    var io = try IO.init(32, 0);
    const path = "test_io_openat_close";
    defer std.fs.cwd().deleteFile(path) catch {};

    var fd: os.fd_t = 0;
    var openat: IO.Completion = undefined;
    io.submit(&openat, *os.fd_t, &fd, .{
        .openat = .{
            .fd = linux.AT_FDCWD,
            .path = path,
            .flags = os.O_CLOEXEC | os.O_RDWR | os.O_CREAT,
            .mode = 0o666,
            .callback = openat_callback,
        },
    });

    try io.run();

    testing.expect(fd > 0);
}

fn openat_callback(result: *const IO.OpenatError!os.fd_t, context: *os.fd_t) callconv(.C) void {
    context.* = result.* catch @panic("openat error");
}
