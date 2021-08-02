const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const Time = @import("time.zig").Time;
const FIFO = @import("fifo.zig").FIFO;

pub const IO = struct {
    driver: Driver,
    completed: FIFO(Completion) = .{},
    submitted: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        return IO{ .driver = Driver.init(entries) };    
    }

    pub fn deinit(self: *IO) void {
        self.driver.deinit();
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// in the __kernel_timespec struct.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        
    }

    const DriverEnterError = error{
        InvalidSubmission,
        WaitForCompletions,
        InternalError,
        OperationNotSupported,
        Retry,
    } || os.UnexpectedError;

    const Driver = switch (std.Target.current.os.tag) {
        .linux => IoUringDriver,
        .macos, .ios, .watchos, .tvos => DarwinDriver,
        else => @compileError("OS is not supported"),
    };

    const IoUringDriver = struct {
        ring: linux.IO_Uring,

        const linux = os.linux;

        pub fn init(entries: u12) !Driver {
            return Driver{ .ring = try linux.IO_Uring.init(entries, 0) };
        }

        pub fn deinit(self: *Driver) void {
            self.ring.deinit();
        }

        pub fn timestamp(self: *Driver) u64 {
            var ts: os.timespec = undefined;
            os.clock_gettime(os.CLOCK_MONOTONIC, &ts) catch unreachable;
            return @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec);
        }

        pub fn has_submissions(self: *Driver) bool {
            return self.ring.sq_ready() > 0;
        }

        pub fn submit(self: *Driver, submissions: *FIFO(Completion)) bool {
            while (submissions.peek()) |completion| {
                const sqe = self.ring.get_sqe() orelse break;
                submissions.remove(completion);
                self.prepare(sqe, completion);
            }
            return self.ring.flush_sq() > 0;
        }

        pub fn poll(self: *Driver, completions: *FIFO(Completion)) bool {
            var cqes: [256]io_uring_cqe = undefined;
            const completed = self.ring.copy_cqes(&cqes, 0) catch unreachable;
            for (cqes[0..completed]) |cqe| {
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                completion.result = cqe.res;
                completions.push(completion);
            }
            return completed > 0;
        }

        pub fn enter(
            self: *Driver,
            flush_submissions: bool,
            wait_for_completions: bool,
        ) DriverEnterError!void {
            return self.ring.enter(
                if (flush_submissions) self.ring.sq_ready() else 0, 
                if (wait_for_completions) 1 else 0, 
                if (wait_for_completions) linux.IORING_ENTER_GETEVENTS else 0,
            ) catch |err| switch (err) {
                error.FileDescriptorInvalid, error.SubmissionQueueEntryInvalid => error.InvalidSubmission,
                error.CompletionQueueOvercommitted, error.SystemResources => error.WaitForCompletions,
                error.FileDescriptorInBadState, error.RingShuttingDown => error.InternalError,
                error.OpcodeNotSupported => error.OperationNotSupported,
                error.UnexpectedError => error.UnexpectedError,
                error.SignalInterrupt => error.Retry,
            };
        }

        fn prepare(self: *Driver, sqe: *linux.io_uring_sqe, completion: *Completion) void {
            switch (completion.op) {
                .close => |fd| linux.io_uring_prep_close(sqe, fd),
                .timeout => |timespec| linux.io_uring_prep_timeout(
                    sqe, 
                    timespec,
                    0, 
                    linux.IORING_TIMEOUT_ABS,
                ),
                .read => |op| linux.io_uring_prep_read(
                    sqe,
                    op.fd,
                    op.buffer[0..buffer_limit(op.buffer.len)],
                    op.offset,
                ),
                .write => |op| linux.io_uring_prep_write(
                    sqe,
                    op.fd,
                    op.buffer[0..buffer_limit(op.buffer.len)],
                    op.offset,
                ),
                .accept => |*op| linux.io_uring_prep_accept(
                    sqe,
                    op.socket,
                    &op.address,
                    &op.address_size,
                    os.SOCK_CLOEXEC,
                ),
                .connect => |*op| linux.io_uring_prep_connect(
                    sqe,
                    op.socket,
                    &op.address.any,
                    &op.address.getOsSockLen(),
                ),
                .send => |op| linux.io_uring_prep_send(
                    sqe,
                    op.socket,
                    op.buffer[0..buffer_limit(op.buffer.len)],
                    os.MSG_NOSIGNAL,
                ),
                .recv => |op| linux.io_uring_prep_recv(
                    sqe,
                    op.socket,
                    op.buffer[0..buffer_limit(op.buffer.len)],
                    os.MSG_NOSIGNAL,
                ),
            }
        }
    };

    const DarwinDriver = struct {
        kq: os.fd_t,
        time: Time = .{},
        timeouts: FIFO(Completion) = .{},
        submitted: FIFO(Completion) = .{},
        completed: FIFO(Completion) = .{},

        pub fn init(entries: u12) !Driver {
            return Driver{ .kq = try os.kqueue() };
        }

        pub fn deinit(self: *Driver) void {
            os.close(self.kq);
        }

        pub fn timestamp(self: *Driver) u64 {
            return self.time.monotonic();
        }

        pub fn has_submissions(self: *Driver) bool {
            return self.submitted.peek() != null;
        }

        pub fn submit(self: *Driver, submissions: *FIFO(Completion)) bool {
            const submitted = submissions.peek() != null;
            self.submitted.push_all(submissions.*);
            submissions.* = .{};
            return submitted;
        }

        pub fn poll(self: *Driver, completions: *FIFO(Completion)) bool {
            const polled = self.completed.peek() != null;
            completions.push_all(self.completed);
            self.completed = .{};
            return polled;
        }

        pub fn enter(
            self: *Driver,
            flush_submissions: bool,
            wait_for_completions: bool,
        ) DriverEnterError!void {
            var events: [256]os.Kevent = undefined;

            var evented: FIFO(Completion) = .{};
            defer if (evented.peek() != null) {
                evented.push_all(self.submitted);
                self.submitted = evented;
            };
            
            var change_events: u32 = 0;
            if (flush_submissions and self.submissions.peek() != null) {
                const now = self.timestamp();
                while (change_events < events.len) {
                    const completion = self.submitted.pop() orelse break;
                    if (self.process(completion, &events[change_events], now)) {
                        change_events += 1;
                        evented.push(completion);
                    }
                }
            }

            var timeout_ts = std.mem.zeroes(os.timespec);
            var timeout_ptr: ?*const os.timespec = &timeout_ts;
            if (wait_for_completions) blk: {
                // Check for timers if this call to enter() is allowed to wait
                const next = self.next_expire() orelse {
                    timeout_ptr = null;
                    break :blk;
                };
                const wait_ns = next - now;
                timeout_ts.tv_sec = @intCast(@TypeOf(timeout_ts.tv_sec), wait_ns / std.time.ns_per_s);
                timeout_ts.tv_nsec = @intCast(@TypeOf(timeout_ts.tv_nsec), wait_ns % std.time.ns_per_s);
            }

            const rc = os.system.kevent(
                self.kq,
                events[0..].ptr,
                @intCast(c_int, change_events),
                events[0..].ptr,
                @intCast(c_int, events.len),
                timeout_ptr,
            );

            const num_events = switch (os.errno(rc)) {
                0 => @intCast(usize, rc),
                os.EACCES => return error.InternalError,
                os.EFAULT => unreachable, // the events memory should always be valid
                os.EBADF => return error.InternalError,
                os.EINTR => return error.Retry,
                os.EINVAL => unreachable, // time limit or event.filter is invalid
                os.ENOENT => unreachable, // only for event modification and deletion
                os.ENOMEM => return error.WaitForCompletions,
                os.ESRCH => unreachable, // only for process attaching,
                else => |err| os.unexpectedErrno(err),
            }''

            const now = self.timestamp();
            _ = self.next_expire(now);

            evented = .{};
            for (events[0..num_events]) |*event| {
                const completion = @intToPtr(*Completion, @intCast(usize, event.udata));
                evented.push(completion);
            }
        }

        // Iterate the timers and either invalidate them or decide the 
        // smallest amount of time to wait on kevent() to expire one. 
        fn next_expire(self: *Driver, now: u64) ?u64 {
            var next_expire: ?u64 = null;
            var timed = self.timeout.peek();
            while (timed) |completion| {
                timed = completion.next;

                const expires = exp: {
                    const ts = completion.op.timeout;
                    const expires = @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec);
                    if (expires > now) break :exp expires;
                    completion.result = -os.ETIME;
                    self.timeouts.remove(completion);
                    self.completed.push(completion);
                    continue;
                };

                if (next_expire) |next| {
                    next_expire = std.math.min(next, expires);
                } else {
                    next_expire = expires;
                }
            }
            return next_expire;
        }

        fn process(self: *Driver, completion: *Completion, event: *os.Kevent, now: u64) bool {
            const P = struct {
                fn syscall(comptime func: anytype, args: anytype) error{WouldBlock}!i32 {
                    const rc = @call(.{}, func, args);
                    return switch (os.errno(rc)) {
                        0 => rc,
                        os.EAGAIN => error.WouldBlock,
                        else => |e| -e,
                    };
                }

                fn evented(fd: os.fd_t, filter: u32, _completion: *Completion, _event: *os.Kevent) bool {
                    _event.* = .{
                        .ident = fd,
                        .filter = filter,
                        .flags = os.EV_CLEAR | os.EV_ADD | os.EV_ENABLE | os.EV_ONESHOT,
                        .fflags = 0,
                        .data = 0,
                        .udata = @ptrToInt(_completion),
                    };
                    return true;
                }
            };

            switch (completion.op) {
                .close => |fd| {
                    completion.result = P.syscall(os.system.close, .{fd}) catch unreachable;
                    self.completed.push(completion);
                    return false;
                },
                .timeout => |ts| {
                    const expires = @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec);
                    if (now >= expires) {
                        completion.result = -os.ETIME;
                        self.completed.push(completion);
                    } else {
                        self.timeouts.push(completion);
                    }
                    return false;
                },
                .read => |op| {
                    completion.result = P.syscall(os.system.pread, .{
                        op.fd, 
                        op.buffer.ptr,
                        buffer_limit(os.buffer.len),
                        @bitCast(i64, op.offset),
                    }) catch unreachable;
                    self.completed.push(completion);
                    return false;
                },
                .write => |op| {
                    completion.result = P.syscall(os.system.pwrite, .{
                        op.fd, 
                        op.buffer.ptr,
                        buffer_limit(os.buffer.len),
                        @bitCast(i64, op.offset),
                    }) catch unreachable;
                    self.completed.push(completion);
                    return false;
                },
                .accept => |*op| blk: {
                    completion.result = P.syscall(os.system.accept, .{
                        op.fd, 
                        &op.address,
                        &op.address_size,
                    }) catch return P.evented(op.fd, os.EVFILT_READ, completion, event);
                    self.completed.push(completion);
                    return false;
                },
                .connect => |*op| {
                    completion.result = P.syscall(os.system.accept, .{
                        op.fd, 
                        &op.address.any,
                        &op.address.getOsSockLen(),
                    }) catch return P.evented(op.fd, os.EVFILT_WRITE, completion, event);
                    self.completed.push(completion);
                    return false;
                },
                .recv => |op| {
                    completion.result = P.syscall(os.system.recv, .{
                        op.fd, 
                        op.buffer.ptr,
                        op.buffer.len,
                    }) catch return P.evented(op.fd, os.EVFILT_READ, completion, event);
                    self.completed.push(completion);
                    return false;
                },
                .send => |op| {
                    completion.result = P.syscall(os.system.send, .{
                        op.fd, 
                        op.buffer.ptr,
                        op.buffer.len,
                    }) catch return P.evented(op.fd, os.EVFILT_WRITE, completion, event);
                    self.completed.push(completion);
                    return false;
                },
            }
        }
    };

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion = null,
        context: ?*c_void,
        callback: fn(*Completion) void,
        result: i32,
        op: union(enum) {
            close: os.fd_t,
            timeout: os.timespec,
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
        },
    };

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
        @compileError("TODO");
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
        @compileError("TODO");
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
        @compileError("TODO");
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
        @compileError("TODO");
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
        completion: *Completion,
        socket: os.socket_t,
    ) void {
        @compileError("TODO");
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
        @compileError("TODO");
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
    ) void {
        @compileError("TODO");
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
    ) void {
        @compileError("TODO");
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