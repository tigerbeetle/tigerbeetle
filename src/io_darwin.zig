const std = @import("std");
const os = std.os;

const FIFO = @import("fifo.zig").FIFO;
const Time = @import("time.zig").Time;
const buffer_limit = @import("io.zig").buffer_limit;

pub const IO = struct {
    kq: os.fd_t,
    time: Time = .{},
    timeouts: FIFO(Completion) = .{},
    completed: FIFO(Completion) = .{},
    io_pending: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        return IO{ .kq = try os.kqueue() };
    }

    pub fn deinit(self: *IO) void {
        os.close(self.kq);
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        return self.flush(false);
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// in the __kernel_timespec struct.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        var running = true;
        var completion: Completion = undefined;
        const timeout_callback = struct {
            fn callback(running_ptr: *bool, _completion: *Completion, result: TimeoutError!void) void {
                running_ptr.* = false;
            }
        }.callback;

        const running_ptr = &running;
        self.timeout(
            *bool,
            running_ptr,
            timeout_callback,
            &completion,
            nanoseconds,
        );

        while (running_ptr.*) {
            try self.flush(true);
        }
    }

    fn flush(self: *IO, wait_for_completions: bool) !void {
        try self.flush_submissions();
        try self.flush_completions();

        var completed = self.completed;
        self.completed = .{};
        while (completed.pop()) |completion| {
            (completion.callback)(completion);
        }
    }

    fn flush_submissions(self: *IO) !void {
        while (self.io_pending.peek()) |_| {
            self.enter(true, false) catch |err| switch (err) {
                error.SystemResources => {
                    try self.flush_completions();
                    continue;
                },
                else => |e| return e,
            };
        }
    }

    fn flush_completions(self: *IO) !void {
        while (self.completed.peek() == null) {
            try self.enter(false, true);
        }
    }

    fn enter(self: *IO, flush_io: bool, wait_event: bool) !void {
        var change_events: u32 = 0;
        var io_top = self.io_pending.out;
        var events: [256]os.Kevent = undefined;
        
        if (flush_io) {
            while (io_top) |completion| {
                if (change_events == events.len) break;
                io_top = completion.next;

                const event_info = switch (completion.operation) {
                    .accept => |op| [2]c_int{ op.socket, os.EVFILT_READ },
                    .connect => |op| [2]c_int{ op.socket, os.EVFILT_WRITE },
                    .send => |op| [2]c_int{ op.socket, os.EVFILT_WRITE },
                    .recv => |op| [2]c_int{ op.socket, os.EVFILT_READ },
                    else => unreachable,
                };

                defer change_events += 1;
                events[change_events] = .{
                    .ident = @intCast(u32, event_info[0]),
                    .filter = @intCast(i16, event_info[1]),
                    .flags = os.EV_ADD | os.EV_ENABLE | os.EV_ONESHOT,
                    .fflags = 0,
                    .data = 0,
                    .udata = @ptrToInt(completion),
                };
            }
        }

        var ts = std.mem.zeroes(os.timespec);
        var timeout_ptr: ?*const os.timespec = &ts;

        if (self.timeouts.peek()) |next_timeout| {
            var min_expire: ?u64 = null;
            const now = self.time.monotonic();

            var next: ?*Completion = next_timeout;
            while (true) {
                const completion = next orelse break;
                next = completion.next;

                const expires = completion.operation.timeout.expires;
                if (now >= expires) {
                    self.timeouts.remove(completion);
                    self.completed.push(completion);
                } else {
                    const current_expire = min_expire orelse std.math.maxInt(u64);
                    min_expire = std.math.min(expires, current_expire);
                }
            }

            if (min_expire) |expires| {
                const timeout_ns = expires - now;
                ts.tv_sec = @intCast(@TypeOf(ts.tv_sec), timeout_ns / std.time.ns_per_s);
                ts.tv_nsec = @intCast(@TypeOf(ts.tv_nsec), timeout_ns % std.time.ns_per_s);
            }
        }

        if (change_events == 0) blk: {
            if (self.timeouts.peek() != null) break :blk;
            if (!wait_event) return;
            timeout_ptr = null;
        }

        const new_events = try os.kevent(
            self.kq, 
            events[0..change_events],
            events[0..events.len],
            timeout_ptr,
        );
        
        self.io_pending.out = io_top;
        for (events[0..new_events]) |event| {
            const completion = @intToPtr(*Completion, event.udata);
            self.completed.push(completion);
        }
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion,
        context: ?*c_void,
        callback: fn(*Completion) void,
        operation: Operation,
    };

    const Operation = union(enum) {
        accept: struct {
            socket: os.socket_t,
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
            expires: u64,
        },
        write: struct {
            fd: os.fd_t,
            buffer: []const u8,
            offset: u64,
        },
    };

    fn submit(
        self: *IO, 
        context: ?*c_void,
        completion: *Completion,
        operation: Operation,
        comptime CallbackImpl: type,
    ) void {
        completion.* = .{
            .next = null,
            .context = context,
            .callback = CallbackImpl.onCompletion,
            .operation = operation,
        };

        switch (operation) {
            .accept, .connect, .recv, .send => self.io_pending.push(completion),
            .timeout => self.timeouts.push(completion),
            else => self.completed.push(completion),
        }
    }

    pub const AcceptError = os.AcceptError;

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
        self.submit(
            context,
            completion, 
            .{
                .accept = .{
                    .socket = socket,
                    .flags = flags,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.accept;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        os.accept(op.socket, null, null, op.flags),
                    );
                }
            },
        );
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
        self.submit(
            context,
            completion, 
            .{
                .close = .{
                    .fd = fd,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.close;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        switch (os.errno(os.system.close(op.fd))) {
                            0 => {},
                            os.EBADF => error.FileDescriptorInvalid,
                            os.EINTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425
                            os.EIO => error.InputOutput,
                            else => |errno| os.unexpectedErrno(errno),
                        },
                    );
                }
            },
        );
    }

    pub const ConnectError = os.ConnectError;

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
        self.submit(
            context,
            completion, 
            .{
                .connect = .{
                    .socket = socket,
                    .address = address,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.connect;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        os.connect(op.socket, &op.address.any, op.address.getOsSockLen()),
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
        completion: *Completion,
        fd: os.fd_t,
        flags: u32,
    ) void {
        self.submit(
            context,
            completion, 
            .{
                .fsync = .{
                    .fd = fd,
                    .flags = flags,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.fsync;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        blk: {
                            _ = os.fcntl(op.fd, os.F_FULLFSYNC, 1) catch break :blk os.fsync(op.fd);
                            break :blk {};
                        },
                    );
                }
            },
        );
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
        self.submit(
            context,
            completion, 
            .{
                .openat = .{
                    .fd = fd,
                    .path = path,
                    .mode = mode,
                    .flags = flags,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.openat;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        os.openatZ(op.fd, op.path, op.flags, op.mode),
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
        completion: *Completion,
        fd: os.fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        self.submit(
            context,
            completion, 
            .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.read;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        blk: {
                            while (true) {
                                const rc = os.system.pread(
                                    op.fd, 
                                    op.buffer.ptr, 
                                    buffer_limit(op.buffer.len),
                                    @bitCast(isize, op.offset),
                                );
                                break :blk switch (os.errno(rc)) {
                                    0 => @intCast(usize, rc),
                                    os.EINTR => continue,
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
                                    else => |err| os.unexpectedErrno(err),
                                };
                            }
                        }
                    );
                }
            },
        );
    }

    pub const RecvError = os.RecvFromError;

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
        self.submit(
            context,
            completion, 
            .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                    .flags = flags,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.recv;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        os.recv(op.socket, op.buffer, op.flags),
                    );
                }
            },
        );
    }

    pub const SendError = os.SendError;

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
        self.submit(
            context,
            completion, 
            .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                    .flags = flags,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.send;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        os.send(op.socket, op.buffer, op.flags),
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
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        self.submit(
            context,
            completion, 
            .{
                .timeout = .{
                    .expires = self.time.monotonic() + nanoseconds,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.timeout;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        {},
                    );
                }
            },
        );
    }

    pub const WriteError = os.PWriteError;

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
        self.submit(
            context,
            completion, 
            .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            struct {
                fn onCompletion(_completion: *Completion) void {
                    const op = _completion.operation.write;
                    return callback(
                        @ptrCast(Context, @alignCast(@alignOf(Context), _completion.context)),
                        _completion,
                        os.pwrite(op.fd, op.buffer, op.offset),
                    );
                }
            },
        );
    }
};