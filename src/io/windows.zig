const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const FIFO = @import("../fifo.zig").FIFO;
const Time = @import("../time.zig").Time;
const buffer_limit = @import("../io.zig").buffer_limit;

pub const IO = struct {
    afd: Afd,
    iocp: os.windows.HANDLE,
    timer: Time = .{},
    io_pending: usize = 0,
    timeouts: FIFO(Completion) = .{},
    completed: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;

        _ = try os.windows.WSAStartup(2, 2);
        errdefer os.windows.WSACleanup() catch unreachable;

        const iocp = try os.windows.CreateIoCompletionPort(os.windows.INVALID_HANDLE_VALUE, null, 0, 0);
        errdefer os.windows.CloseHandle(iocp);

        const afd = try Afd.init(iocp);
        errdefer afd.deinit();

        return IO{
            .afd = afd,
            .iocp = iocp,
        };
    }

    pub fn deinit(self: *IO) void {
        self.afd.deinit();
        self.afd = undefined;

        assert(self.iocp != os.windows.INVALID_HANDLE_VALUE);
        os.windows.CloseHandle(self.iocp);
        self.iocp = os.windows.INVALID_HANDLE_VALUE;

        os.windows.WSACleanup() catch unreachable;
    }

    pub fn tick(self: *IO) !void {
        return self.flush(.non_blocking);
    }

    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        const Callback = struct {
            fn onTimeout(timed_out: *bool, completion: *Completion, result: TimeoutError!void) void {
                _ = result catch unreachable;
                _ = completion;
                timed_out.* = true;
            }
        };

        var timed_out = false;
        var completion: Completion = undefined;
        self.timeout(*bool, &timed_out, Callback.onTimeout, &completion, nanoseconds);

        while (!timed_out) {
            try self.flush(.blocking);
        }
    }

    const FlushMode = enum {
        blocking,
        non_blocking,
    };

    fn flush(self: *IO, mode: FlushMode) !void {
        if (self.completed.peek() == null) {
            // Compute how long to poll by flushing timeout completions.
            // NOTE: this may push to completed queue
            var timeout_ms: ?os.windows.DWORD = null;
            if (self.flush_timeouts()) |expires_ns| {
                // 0ns expires should have been completed not returned
                assert(expires_ns != 0); 
                // Round up sub-millisecond expire times to the next millisecond
                const expires_ms = (expires_ns + (std.time.ns_per_ms / 2)) / std.time.ns_per_ms;
                // Saturating cast to DWORD milliseconds
                const expires = std.math.cast(os.windows.DWORD, expires_ms) catch std.math.maxInt(os.windows.DWORD);
                // max DWORD is reserved for INFINITE so cap the cast at max - 1
                timeout_ms = if (expires == os.windows.INFINITE) expires - 1 else expires;
            }
            
            // Poll for IO iff theres IO pending and flush_timeouts() found no ready completions
            if (self.io_pending > 0 and self.completed.peek() == null) {
                // In blocking mode, we're always waiting at least until the timeout by run_for_ns.
                // In non-blocking mode, we shouldn't wait at all.
                const io_timeout = switch (mode) {
                    .blocking => timeout_ms orelse @panic("IO.flush blocking unbounded"),
                    .non_blocking => 0,
                };

                var events: [64]Afd.Event = undefined;
                const num_events = try self.afd.poll(self.iocp, &events, io_timeout);
                
                assert(self.io_pending >= num_events);
                self.io_pending -= num_events;

                for (events[0..num_events]) |event| {
                    const afd_completion = event.as_completion() orelse unreachable;
                    const completion = @fieldParentPtr(Completion, "afd_completion", afd_completion);
                    completion.next = null;
                    self.completed.push(completion);
                }
            }
        }

        // Dequeue and invoke all the completions currently ready.
        // Must read all `completions` before invoking the callbacks
        // as the callbacks could potentially submit more completions. 
        var completed = self.completed;
        self.completed = .{};
        while (completed.pop()) |completion| {
            (completion.callback)(self, completion);
        }
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_expires: ?u64 = null;
        var current_time: ?u64 = null;
        var timeouts: ?*Completion = self.timeouts.peek();

        // iterate through the timeouts, returning min_expires at the end
        while (timeouts) |completion| {
            timeouts = completion.next;

            // lazily get the current time
            const now = current_time orelse self.timer.monotonic();
            current_time = now;

            // move the completion to completed if it expired
            if (now >= completion.operation.timeout.deadline) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            // if it's still waiting, update min_timeout
            const expires = completion.operation.timeout.deadline - now;
            if (min_expires) |current_min_expires| {
                min_expires = std.math.min(expires, current_min_expires);
            } else {
                min_expires = expires;
            }
        }

        return min_expires;
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion,
        afd_completion: Afd.Completion,
        context: ?*c_void,
        callback: fn (*IO, *Completion) void,
        operation: Operation,
    };

    const Operation = union(enum) {
        accept: struct {
            socket: os.socket_t,
        },
        close: struct {
            fd: os.fd_t,
        },
        connect: struct {
            socket: os.socket_t,
            address: std.net.Address,
            initiated: bool,
        },
        fsync: struct {
            fd: os.fd_t,
        },
        read: struct {
            fd: os.fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
        },
        recv: struct {
            socket: os.socket_t,
            buf: [*]u8,
            len: u32,
        },
        send: struct {
            socket: os.socket_t,
            buf: [*]const u8,
            len: u32,
        },
        timeout: struct {
            deadline: u64,
        },
        write: struct {
            fd: os.fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
        },
    };

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime op_tag: std.meta.Tag(Operation),
        op_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const Context = @TypeOf(context);
        const Callback = struct {
            fn onComplete(io: *IO, _completion: *Completion) void {
                // Perform the operation and get the result
                const _op_data = &@field(_completion.operation, @tagName(op_tag));
                const result = OperationImpl.doOperation(_op_data);

                // Schedule the socket and completion onto windows AFD when WouldBlock is detected
                if (switch (op_tag) {
                    .accept, .recv => @as(?Afd.Operation, .read),
                    .connect, .send => @as(?Afd.Operation, .write),
                    else => null,
                }) |afd_op| {
                    _ = result catch |err| switch (err) {
                        error.WouldBlock => {
                            const afd_completion = &_completion.afd_completion;
                            io.afd.schedule(_op_data.socket, afd_op, afd_completion) catch {
                                // On AFD schedulue failure try to perform the op again next tick
                                _completion.next = null;
                                io.completed.push(_completion);
                                return;
                            };

                            // We've scheduled the completion to be received when polling on IO
                            io.io_pending += 1;
                            return;
                        },
                        else => {},
                    };
                }
                
                // The completion is finally ready to invoke the callback
                callback(
                    @intToPtr(Context, @ptrToInt(_completion.context)),
                    _completion,
                    result,
                );
            }
        };

        // Setup the completion with the callback wrapper above
        completion.* = .{
            .next = null,
            .afd_completion = undefined, 
            .context = @ptrCast(?*c_void, context),
            .callback = Callback.onComplete,
            .operation = @unionInit(Operation, @tagName(op_tag), op_data),
        };

        // Submit the completion onto the right queue
        switch (op_tag) {
            .timeout => self.timeouts.push(completion),
            else => self.completed.push(completion),
        }
    }

    pub const AcceptError = os.AcceptError || os.SetSockOptError;

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
        self.submit(
            context,
            callback,
            completion,
            .accept,
            .{ .socket = socket },
            struct {
                fn doOperation(op: anytype) AcceptError!os.socket_t {
                    return os.accept(op.socket, null, null, 0);
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
            callback,
            completion,
            .close,
            .{ .fd = fd },
            struct {
                fn doOperation(op: anytype) CloseError!void {
                    // Check if the fd is a SOCKET by seeing if getsockopt() returns ENOTSOCK
                    // https://stackoverflow.com/a/50981652
                    const socket = @ptrCast(os.socket_t, op.fd);
                    getsockoptError(socket) catch |err| switch (err) {
                        error.FileDescriptorNotASocket => return os.windows.CloseHandle(op.fd),
                        else => {},
                    };
                    os.closeSocket(socket);
                }
            },
        );
    }

    pub const ConnectError = os.ConnectError || error{FileDescriptorNotASocket};

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
            callback,
            completion,
            .connect,
            .{
                .socket = socket,
                .address = address,
                .initiated = false,
            },
            struct {
                fn doOperation(op: anytype) ConnectError!void {
                    // Don't call connect after being rescheduled by io_pending as it gives EISCONN.
                    // Instead, check the socket error to see if has been connected successfully.
                    if (op.initiated)
                        return getsockoptError(op.socket);

                    op.initiated = true;
                    const rc = os.windows.ws2_32.connect(
                        op.socket, 
                        &op.address.any, 
                        @intCast(i32, op.address.getOsSockLen()),
                    );

                    // Need to hardcode as os.connect has unreachable on .WSAEWOULDBLOCK
                    if (rc == 0) return;                    
                    switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSAEADDRINUSE => return error.AddressInUse,
                        .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
                        .WSAECONNREFUSED => return error.ConnectionRefused,
                        .WSAECONNRESET => return error.ConnectionResetByPeer,
                        .WSAETIMEDOUT => return error.ConnectionTimedOut,
                        .WSAEHOSTUNREACH,
                        .WSAENETUNREACH,
                        => return error.NetworkUnreachable,
                        .WSAEFAULT => unreachable,
                        .WSAEINVAL => unreachable,
                        .WSAEISCONN => unreachable,
                        .WSAENOTSOCK => unreachable,
                        .WSAEWOULDBLOCK => return error.WouldBlock,
                        .WSAEACCES => unreachable,
                        .WSAENOBUFS => return error.SystemResources,
                        .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
                        else => |err| return os.windows.unexpectedWSAError(err),
                    }
                }
            },
        );
    }

    pub const FsyncError = os.SyncError;

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
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .fsync,
            .{ .fd = fd },
            struct {
                fn doOperation(op: anytype) FsyncError!void {
                    return os.fsync(op.fd);
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
            callback,
            completion,
            .read,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
                .offset = offset,
            },
            struct {
                fn doOperation(op: anytype) ReadError!usize {
                    return os.pread(op.fd, op.buf[0..op.len], op.offset) catch |err| switch (err) {
                        error.OperationAborted => unreachable,
                        error.BrokenPipe => unreachable,
                        error.ConnectionTimedOut => unreachable,
                        error.AccessDenied => error.InputOutput,
                        else => |e| e,
                    };
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
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .recv,
            .{
                .socket = socket,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
            },
            struct {
                fn doOperation(op: anytype) RecvError!usize {
                    return os.recv(op.socket, op.buf[0..op.len], 0);
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
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .send,
            .{
                .socket = socket,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
            },
            struct {
                fn doOperation(op: anytype) SendError!usize {
                    return os.send(op.socket, op.buf[0..op.len], 0);
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
            callback,
            completion,
            .timeout,
            .{ .deadline = self.timer.monotonic() + nanoseconds },
            struct {
                fn doOperation(op: anytype) TimeoutError!void {
                    _ = op;
                    return;
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
            callback,
            completion,
            .write,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
                .offset = offset,
            },
            struct {
                fn doOperation(op: anytype) WriteError!usize {
                    return os.pwrite(op.fd, op.buf[0..op.len], op.offset);
                }
            },
        );
    }

    pub const INVALID_SOCKET = os.windows.ws2_32.INVALID_SOCKET;

    pub fn openSocket(family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        return os.socket(family, sock_type | os.SOCK_NONBLOCK | os.SOCK_CLOEXEC, protocol);
    }
};

// TODO: use os.getsockoptError when fixed in stdlib
fn getsockoptError(socket: os.socket_t) IO.ConnectError!void {
    var err_code: u32 = undefined;
    var size: i32 = @sizeOf(u32);
    const rc = os.windows.ws2_32.getsockopt(
        socket, 
        os.SOL_SOCKET,
        os.SO_ERROR, 
        std.mem.asBytes(&err_code), 
        &size,
    );
    
    if (rc != 0) {
        switch (os.windows.ws2_32.WSAGetLastError()) {
            .WSAENETDOWN => return error.NetworkUnreachable,
            .WSANOTINITIALISED => unreachable, // WSAStartup() was never called
            .WSAEFAULT => unreachable, // The address pointed to by optval or optlen is not in a valid part of the process address space.
            .WSAEINVAL => unreachable, // The level parameter is unknown or invalid
            .WSAENOPROTOOPT => unreachable, // The option is unknown at the level indicated.
            .WSAENOTSOCK => return error.FileDescriptorNotASocket,
            else => |err| return os.unexpectedErrno(@enumToInt(err)),
        }
    }
    
    assert(size == 4);
    switch (err_code) {
        0 => return,
        os.EACCES => return error.PermissionDenied,
        os.EPERM => return error.PermissionDenied,
        os.EADDRINUSE => return error.AddressInUse,
        os.EADDRNOTAVAIL => return error.AddressNotAvailable,
        os.EAFNOSUPPORT => return error.AddressFamilyNotSupported,
        os.EAGAIN => return error.SystemResources,
        os.EALREADY => return error.ConnectionPending,
        os.EBADF => unreachable, // sockfd is not a valid open file descriptor.
        os.ECONNREFUSED => return error.ConnectionRefused,
        os.EFAULT => unreachable, // The socket structure address is outside the user's address space.
        os.EISCONN => unreachable, // The socket is already connected.
        os.ENETUNREACH => return error.NetworkUnreachable,
        os.ENOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
        os.EPROTOTYPE => unreachable, // The socket type does not support the requested communications protocol.
        os.ETIMEDOUT => return error.ConnectionTimedOut,
        os.ECONNRESET => return error.ConnectionResetByPeer,
        else => |err| return os.unexpectedErrno(err),
    }
}

/// Windows AFD is an internal system not exposed by kernel32
/// which is used to monitor sockets and pipes for IO readiness.
/// It's an alternative to stable IOCP calls and is used in other projects:
/// - https://github.com/piscisaureus/wepoll
/// - https://github.com/tokio-rs/mio/blob/e55ec59cf287dd4bdc06c70bfadea74f85c8bf09/src/sys/windows/afd.rs
/// - https://github.com/libuv/libuv/blob/47e0c5c575e92a25e0da10fc25b2732942c929f3/src/win/winsock.c#L461
const Afd = struct {
    const AFD_POLL_HANDLE_INFO = extern struct {
        Handle: os.windows.HANDLE,
        Events: os.windows.ULONG,
        Status: os.windows.NTSTATUS,
    };

    const AFD_POLL_INFO = extern struct {
        Timeout: os.windows.LARGE_INTEGER,
        NumberOfHandles: os.windows.ULONG,
        Exclusive: os.windows.ULONG,
        Handles: [1]AFD_POLL_HANDLE_INFO,
    };

    const IOCTL_AFD_POLL = 0x00012024;
    const AFD_POLL_RECEIVE = 0x0001;
    const AFD_POLL_SEND = 0x0004;
    const AFD_POLL_DISCONNECT = 0x0008;
    const AFD_POLL_ABORT = 0b0010;
    const AFD_POLL_LOCAL_CLOSE = 0x020;
    const AFD_POLL_ACCEPT = 0x0080;
    const AFD_POLL_CONNECT_FAIL = 0x0100;

    handle: os.windows.HANDLE,

    fn init(iocp_handle: os.windows.HANDLE) !Afd {
        const name = "\\Device\\Afd\\tigerbeetle";
        var buf: [name.len]os.windows.WCHAR = undefined;
        const len = try std.unicode.utf8ToUtf16Le(&buf, name);

        const bytes_len = try std.math.cast(c_ushort, len * @sizeOf(os.windows.WCHAR));
        var afd_name = os.windows.UNICODE_STRING{
            .Buffer = &buf,
            .Length = bytes_len,
            .MaximumLength = bytes_len,
        };

        var afd_attr = os.windows.OBJECT_ATTRIBUTES{
            .Length = @sizeOf(os.windows.OBJECT_ATTRIBUTES),
            .RootDirectory = null,
            .ObjectName = &afd_name,
            .Attributes = 0,
            .SecurityDescriptor = null,
            .SecurityQualityOfService = null,
        };

        var afd_handle: os.windows.HANDLE = undefined;
        var io_status_block: os.windows.IO_STATUS_BLOCK = undefined;
        switch (os.windows.ntdll.NtCreateFile(
            &afd_handle,
            os.windows.SYNCHRONIZE,
            &afd_attr,
            &io_status_block,
            null,
            0,
            os.windows.FILE_SHARE_READ | os.windows.FILE_SHARE_WRITE,
            os.windows.FILE_OPEN,
            0,
            null,
            0,
        )) {
            .SUCCESS => {},
            .OBJECT_NAME_INVALID => unreachable,
            else => |status| return os.windows.unexpectedStatus(status),
        }
        errdefer os.windows.CloseHandle(afd_handle);

        const registered_handle = try os.windows.CreateIoCompletionPort(afd_handle, iocp_handle, 1, 0);
        assert(registered_handle == iocp_handle);

        const notify_mode = os.windows.FILE_SKIP_SET_EVENT_ON_HANDLE;
        try os.windows.SetFileCompletionNotificationModes(afd_handle, notify_mode);

        return Afd{ .handle = afd_handle };
    }

    fn deinit(self: Afd) void {
        os.windows.CloseHandle(self.handle);
    }

    const Operation = enum { read, write };
    const Completion = struct {
        next: ?*Completion = null,
        afd_poll_info: AFD_POLL_INFO,
        io_status_block: os.windows.IO_STATUS_BLOCK,
    };

    // Schedule a Completion event to be reported by poll()
    // once the Operation is observed to be ready on the socket.
    // This is similar to EPOLL_CTL_MOD with EPOLLONESHOT.
    fn schedule(
        self: Afd,
        socket: os.socket_t,
        operation: Operation,
        completion: *Completion,
    ) !void {
        const afd_events: os.windows.ULONG = switch (operation) {
            .read => AFD_POLL_RECEIVE | AFD_POLL_ACCEPT | AFD_POLL_DISCONNECT,
            .write => AFD_POLL_SEND,
        };

        completion.afd_poll_info = .{
            .Timeout = std.math.maxInt(os.windows.LARGE_INTEGER),
            .NumberOfHandles = 1,
            .Exclusive = os.windows.FALSE,
            .Handles = [_]AFD_POLL_HANDLE_INFO{.{
                .Handle = @ptrCast(os.windows.HANDLE, socket),
                .Status = .SUCCESS,
                .Events = afd_events | AFD_POLL_ABORT | AFD_POLL_LOCAL_CLOSE | AFD_POLL_CONNECT_FAIL,
            }},
        };

        completion.io_status_block = .{
            .u = .{ .Status = .PENDING },
            .Information = 0,
        };

        const status = os.windows.ntdll.NtDeviceIoControlFile(
            self.handle,
            null,
            null,
            &completion.io_status_block,
            &completion.io_status_block,
            IOCTL_AFD_POLL,
            &completion.afd_poll_info,
            @sizeOf(AFD_POLL_INFO),
            &completion.afd_poll_info,
            @sizeOf(AFD_POLL_INFO),
        );

        switch (status) {
            .SUCCESS => {},
            .PENDING => {},
            .INVALID_HANDLE => unreachable,
            else => return os.windows.unexpectedStatus(status),
        }
    }

    const Event = extern struct {
        overlapped_entry: os.windows.OVERLAPPED_ENTRY,

        fn as_completion(self: Event) ?*Completion {
            const overlapped = self.overlapped_entry.lpOverlapped;
            const io_status_block = @ptrCast(*os.windows.IO_STATUS_BLOCK, overlapped);
            return @fieldParentPtr(Completion, "io_status_block", io_status_block);
        }
    };
    
    fn poll(
        self: Afd,
        iocp_handle: os.windows.HANDLE,
        events: []Event,
        timeout_ms: os.windows.DWORD,
    ) !usize {
        return os.windows.GetQueuedCompletionStatusEx(
            iocp_handle, 
            @ptrCast([*]os.windows.OVERLAPPED_ENTRY, events.ptr)[0..events.len],
            timeout_ms,
            true, // alertable wait
        ) catch |err| switch (err) {
            error.Timeout,
            error.Aborted,
            error.Cancelled => @as(usize, 0),
            else => |e| return e,
        };
    }
};