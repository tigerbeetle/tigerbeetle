const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const FIFO = @import("fifo.zig").FIFO;
const Driver = switch (std.Target.current.os.tag) {
    .linux => @import("io_uring.zig").Driver,
    .macos, .ios, .watchos, .tvos => @import("io_darwin.zig").Driver,
    else => @compileError("OS I/O driver is not supported"),
};

pub const IO = struct {
    /// The I/O subsystem/backend used to interact with the system and submit/reap completions.
    driver: Driver,
    /// A list of completions that counldn't be immediately submitted as the Driver's submission queue was full.
    unqueued: FIFO(Completion) = .{},
    /// A list of completions that the driver has marked as completed and ready to have their .callback invoked with the .result.
    completed: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        return IO{ .driver = try Driver.init(entries) };    
    }

    pub fn deinit(self: *IO) void {
        self.driver.deinit();
    }

    /// Error returned by Driver.submit().
    pub const SubmitError = error{
        /// The Drivers's submission queue is full and 
        /// Driver.entry(flush_submissions=true, ...) must be called
        /// to make room in it's submission queue.
        SubmissionQueueFull,
    };
    
    /// Error returned by Driver.enter(flush_submissions, wait_for_completions)
    pub const EnterError = error{
        /// The Driver's submission queue contains an invalid submission.
        /// This generally indicates a programming error in Completion submission.
        InvalidSubmission,
        /// Tried to flush the submission queue and either:
        /// - the Driver ran out of memory for the submissions
        /// - the Driver would overcommit the completion queue
        /// The caller should wait and poll for completions, then try again.
        WaitForCompletions,
        /// An internal error occured in the Driver when either submitting or waiting for completions.
        /// This is generally not recoverable.
        InternalError,
        /// The Driver's submission queue contains a valid submission, but one it doesn't currently support.
        /// This generally indcates a programming error in the Driver submission process.
        OperationNotSupported,
        /// The Driver's enter() call was interrupted by the system.
        /// The caller should retry the enter() operation again.
        Retry,
    } || os.UnexpectedError;

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        return self.run_tick(false);
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// internally in some Driver implementations.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        // Create and submit a completion which times out by setting timed_out = true.
        var timed_out = false;
        var completion: Completion = undefined;
        const on_timed_out = struct {
            fn callback(timed_out_ptr: *bool, _: *Completion, result: TimeoutError!void) void {
                _ = result catch @panic("run_for_ns timeout failed");
                timed_out_ptr.* = true;
            }
        }.callback;
        
        // Submit the timeout completion...
        const timed_out_ptr = &timed_out;
        self.timeout(
            *bool, 
            timed_out_ptr, 
            on_timed_out, 
            &completion, 
            nanoseconds,
        );

        // ...then wait for the timeout to completion to occur
        // while running and waiting for other submissions/completions in the process. 
        while (!(timed_out_ptr.*)) {
            try self.run_tick(true);
        }
    }
    
    fn run_tick(self: *IO, wait_for_completions: bool) !void {
        // Flush the completions from the previous run_tick()
        // as an optimization to avoid error.WaitForCompletions
        // in the following flush_completions().
        self.poll_completions();

        // Flush any pending submissions, reusing the same enter()
        // call to also wait for any completions.
        try self.flush_submissions(wait_for_completions);

        // Gather the completions from the flush above
        self.poll_completions();

        // Having just flushed pending submissions,
        // try to fill up the driver's submission queue with unqueued submissions.
        while (self.unqueued.peek()) |completion| {
            if (self.driver.submit(completion)) |_| {
                self.unqueued.remove(completion);
            } else |_| {
                break;
            }
        }

        // Run all *current* completions by taking the current FIFO
        // and popping from that, avoiding completions pushed during the callbacks.
        var completed = self.completed;
        self.completed = .{};
        while (completed.pop()) |completion| completion.callback(completion);

        // Flush any submissions made from either the unqueued driver.submit()'s
        // or completion callback self.enqueue()S as an optimization to avoid 
        // the submissions waiting in the queue for the next tick.
        try self.flush_submissions(false);
    }

    fn poll_completions(self: *IO) void {
        // Reap all the completions available at this point in time
        while (true) {
            var completions = self.driver.poll();
            if (completions.peek() == null) break;
            self.completed.push_all(completions);
        }
    }

    fn flush_submissions(self: *IO, wait_for_completions: bool) !void {
        while (true) {
            // Don't enter() (syscall) if there's no submissions 
            // and if there's no completions to wait for.
            if (!self.driver.has_submissions()) {
                if (!wait_for_completions) return;
                if (self.completed.peek() != null or self.driver.has_completions()) return;
            }

            // Flush any submissions while also potentially waiting for completions
            // when necessary using the same enter() (syscall).
            while (true) {
                return self.driver.enter(true, wait_for_completions) catch |err| switch (err) {
                    error.Retry => continue,
                    error.WaitForCompletions => break,
                    else => |e| return e,
                };
            }

            // Either the driver is under memory pressure or it's completion queue is overflowing.
            // Regardless, wait for some completions to occur to either:
            // - make room in the completion queue and avoid overflow
            // - release some memory by consuming some completions
            // then break to try and do the submissions again. 
            while (true) {
                self.driver.enter(false, true) catch |err| switch (err) {
                    error.Retry => continue,
                    else => |e| return e,
                };
                self.poll_completions();
                break;
            }
        }
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion = undefined,
        io: *IO,
        context: ?*c_void,
        callback: fn(*Completion) void,
        result: isize = undefined,
        op: Operation,
    };
    
    /// Defines an IO operation taken by the Driver
    const Operation = union(enum) {
        close: os.fd_t,
        timeout: struct {
            expires: u64,
            ts: if (@hasDecl(os, "__kernel_timespec")) os.__kernel_timespec else os.timespec,
        },
        read: struct {
            fd: os.fd_t,
            buffer: []u8,
            offset: u64,
        },
        write: struct {
            fd: os.fd_t,
            buffer: []const u8,
            offset: u64,
        },
        fsync: struct {
            fd: os.fd_t,
            flags: u32,
        },
        accept: struct {
            socket: os.socket_t,
            address: os.sockaddr = undefined,
            address_size: os.socklen_t = @sizeOf(os.sockaddr),
        },
        connect: struct {
            socket: os.socket_t,
            address: std.net.Address,
        },
        recv: struct {
            socket: os.fd_t,
            buffer: []u8,
        },
        send: struct {
            socket: os.fd_t,
            buffer: []const u8,
        },
    };

    fn enqueue(self: *IO, completion: *Completion) void {
        self.driver.submit(completion) catch {
            completion.next = null;
            self.unqueued.push(completion);
        };
    }

    fn submit(
        self: *IO, 
        completion: *Completion, 
        context: ?*c_void,
        operation: Operation, 
        comptime CallbackHandler: type,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = CallbackHandler.onCompletion,
            .op = operation,
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
        _completion: *Completion,
        fd: os.fd_t,
    ) void {
        return self.submit(
            _completion,
            context,
            .{ .close = fd },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
                            os.EINTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425
                            os.EBADF => error.FileDescriptorInvalid,
                            os.EDQUOT => error.DiskQuota,
                            os.EIO => error.InputOutput,
                            os.ENOSPC => error.NoSpaceLeft,
                            else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                        } else assert(completion.result == 0),
                    );
                }
            },
        );
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
        _completion: *Completion,
        nanoseconds: u63,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .timeout = .{
                    .expires = self.driver.timestamp() + nanoseconds,
                    .ts = undefined,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
                            os.EINTR => {
                                completion.io.enqueue(completion);
                                return;
                            },
                            os.ECANCELED => error.Canceled,
                            os.ETIME => {}, // A success.
                            else => |errno| os.unexpectedErrno(@intCast(usize, errno)),
                        } else unreachable,
                    );
                }
            },
        );
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
        _completion: *Completion,
        fd: os.fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else @intCast(usize, completion.result),
                    );
                }
            },
        );
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
        _completion: *Completion,
        fd: os.fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else @intCast(usize, completion.result),
                    );
                }
            },
        );
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
        _completion: *Completion,
        fd: os.fd_t,
        flags: u32,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .fsync = .{
                    .fd = fd,
                    .flags = flags,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else assert(completion.result == 0),
                    );
                }
            },
        );
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

    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!os.socket_t,
        ) void,
        _completion: *Completion,
        socket: os.socket_t,
    ) void {
        return self.submit(
            _completion,
            context,
            .{ .accept = .{ .socket = socket } },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else @intCast(os.socket_t, completion.result),
                    );
                }
            },
        );
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
        ConnectionResetByPeer,
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
        _completion: *Completion,
        socket: os.socket_t,
        address: std.net.Address,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .connect = .{
                    .socket = socket,
                    .address = address,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else assert(completion.result == 0),
                    );
                }
            },
        );
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
        _completion: *Completion,
        socket: os.socket_t,
        buffer: []const u8,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else @intCast(usize, completion.result),
                    );
                }
            },
        );
    }

    pub const RecvError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionRefused,
        ConnectionResetByPeer,
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
        _completion: *Completion,
        socket: os.socket_t,
        buffer: []u8,
    ) void {
        return self.submit(
            _completion,
            context,
            .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            struct {
                pub fn onCompletion(completion: *Completion) void {
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), completion.context)),
                        completion,
                        if (completion.result < 0) switch (-completion.result) {
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
                        } else @intCast(usize, completion.result),
                    );
                }
            },
        );
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

test "close" {
    const testing = std.testing;

    try struct {
        const Context = @This();

        io: IO,
        fd: os.fd_t = 0,
        closed: bool = false,

        fn run_test() !void {
            const path = "test_io_close";
            defer std.fs.cwd().deleteFile(path) catch {};

            var self: Context = .{ .io = try IO.init(32, 0) };
            defer self.io.deinit();

            var completion: IO.Completion = undefined;
            self.fd = try os.openat(
                os.AT_FDCWD, 
                path, 
                os.O_CLOEXEC | os.O_RDWR | os.O_CREAT,
                0o666,
            );

            self.io.close(*Context, self, close_callback, completion, self.fd);
            try self.io.run();

            testing.expect(self.fd > 0);
            testing.expect(self.closed);
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