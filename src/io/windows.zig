const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const log = std.log.scoped(.io);
const config = @import("../config.zig");

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
                const result = OperationImpl.do_operation(_op_data);

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
                fn do_operation(op: anytype) AcceptError!os.socket_t {
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
                fn do_operation(op: anytype) CloseError!void {
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
                fn do_operation(op: anytype) ConnectError!void {
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
                fn do_operation(op: anytype) FsyncError!void {
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
                fn do_operation(op: anytype) ReadError!usize {
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
                fn do_operation(op: anytype) RecvError!usize {
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
                fn do_operation(op: anytype) SendError!usize {
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
                fn do_operation(op: anytype) TimeoutError!void {
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
                fn do_operation(op: anytype) WriteError!usize {
                    return os.pwrite(op.fd, op.buf[0..op.len], op.offset);
                }
            },
        );
    }

    pub const INVALID_SOCKET = os.windows.ws2_32.INVALID_SOCKET;

    pub fn open_socket(family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        return os.socket(family, sock_type | os.SOCK_NONBLOCK | os.SOCK_CLOEXEC, protocol);
    }

    pub fn open_file(
        dir_handle: os.fd_t,
        relative_path: [:0]const u8,
        size: u64,
        must_create: bool,
    ) !os.fd_t {
        const path_w = try os.windows.sliceToPrefixedFileW(relative_path);

        // FILE_CREATE = O_CREAT | O_EXCL
        var creation_disposition: os.windows.DWORD = 0;
        if (must_create) {
            log.info("creating \"{s}\"...", .{relative_path});
            creation_disposition = os.windows.FILE_CREATE;
        } else {
            log.info("opening \"{s}\"...", .{relative_path});
            creation_disposition = os.windows.OPEN_EXISTING;
        }

        // O_EXCL
        var shared_mode: os.windows.DWORD = 0;

        // O_RDWR
        var access_mask: os.windows.DWORD = 0;
        access_mask |= os.windows.GENERIC_READ;
        access_mask |= os.windows.GENERIC_WRITE;

        // O_DIRECT
        var attributes: os.windows.DWORD = 0;
        attributes |= os.windows.FILE_FLAG_NO_BUFFERING;
        attributes |= os.windows.FILE_FLAG_WRITE_THROUGH;

        const handle = os.windows.kernel32.CreateFileW(
            path_w.span(),
            access_mask,
            shared_mode,
            null, // no security attributes required
            creation_disposition,
            attributes,
            null, // no existing template file
        );

        if (handle == os.windows.INVALID_HANDLE_VALUE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .ACCESS_DENIED => error.AccessDenied,
                else => |err| os.windows.unexpectedError(err),
            };
        }

        errdefer os.windows.CloseHandle(handle);

        // Obtain an advisory exclusive lock
        // even when we haven't given shared access to other processes.
        try fs_lock(handle, size);

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        if (must_create) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(handle, size) catch {
                log.warn("file system failed to preallocate the file memory", .{});
                log.notice("allocating by writing to the last sector of the file instead...", .{});

                const sector_size = config.sector_size;
                const sector: [sector_size]u8 align(sector_size) = [_]u8{0} ** sector_size;

                // Handle partial writes where the physical sector is less than a logical sector:
                const write_offset = size - sector.len;
                var written: usize = 0;
                while (written < sector.len) {
                    written += try os.pwrite(handle, sector[written..], write_offset + written);
                }
            };
        }

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try os.fsync(handle);

        // Don't fsync the directory handle as it's not open with write access
        // try os.fsync(dir_handle);

        const file_size = try os.windows.GetFileSizeEx(handle);
        if (file_size != size) @panic("data file inode size was truncated or corrupted");

        return handle;
    }

    fn fs_lock(handle: os.fd_t, size: u64) !void {
        // TODO: Look into using SetFileIoOverlappedRange() for better unbuffered async IO perf
        // NOTE: Requires SeLockMemoryPrivilege.

        const kernel32 = struct {
            const LOCKFILE_EXCLUSIVE_LOCK = 0x2;
            const LOCKFILE_FAIL_IMMEDIATELY = 01;

            extern "kernel32" fn LockFileEx(
                hFile: os.windows.HANDLE,
                dwFlags: os.windows.DWORD,
                dwReserved: os.windows.DWORD,
                nNumberOfBytesToLockLow: os.windows.DWORD,
                nNumberOfBytesToLockHigh: os.windows.DWORD,
                lpOverlapped: ?*os.windows.OVERLAPPED,
            ) callconv(os.windows.WINAPI) os.windows.BOOL;
        };

        // hEvent = null 
        // Offset & OffsetHigh = 0
        var lock_overlapped = std.mem.zeroes(os.windows.OVERLAPPED);

        // LOCK_EX | LOCK_NB
        var lock_flags: os.windows.DWORD = 0;
        lock_flags |= kernel32.LOCKFILE_EXCLUSIVE_LOCK;
        lock_flags |= kernel32.LOCKFILE_FAIL_IMMEDIATELY;        

        const locked = kernel32.LockFileEx(
            handle,
            lock_flags,
            0, // reserved param is always zero
            @truncate(u32, size), // low bits of size
            @truncate(u32, size >> 32), // high bits of size
            &lock_overlapped,
        );

        if (locked == os.windows.FALSE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .IO_PENDING => error.WouldBlock,
                else => |err| os.windows.unexpectedError(err),
            };
        }
    }

    fn fs_allocate(handle: os.fd_t, size: u64) !void {
        // TODO: Look into using SetFileValidData() instead
        // NOTE: Requires SE_MANAGE_VOLUME_NAME privilege

        // Move the file pointer to the start + size
        const seeked = os.windows.kernel32.SetFilePointerEx(
            handle, 
            @intCast(i64, size), 
            null, // no reference to new file pointer
            os.windows.FILE_BEGIN,
        );
        
        if (seeked == os.windows.FALSE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .INVALID_HANDLE => unreachable,
                .INVALID_PARAMETER => unreachable,
                else => |err| os.windows.unexpectedError(err),
            };
        }

        // Mark the moved file pointer (start + size) as the physical EOF.
        const allocated = os.windows.kernel32.SetEndOfFile(handle);
        if (allocated == os.windows.FALSE) {
            const err = os.windows.kernel32.GetLastError();
            return os.windows.unexpectedError(err);
        }
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
    if (err_code == 0)
        return;

    const ws_err = @intToEnum(os.windows.ws2_32.WinsockError, @intCast(u16, err_code));
    return switch (ws_err) {
        .WSAEACCES => error.PermissionDenied,
        .WSAEADDRINUSE => error.AddressInUse,
        .WSAEADDRNOTAVAIL => error.AddressNotAvailable,
        .WSAEAFNOSUPPORT => error.AddressFamilyNotSupported,
        .WSAEALREADY => error.ConnectionPending,
        .WSAEBADF => unreachable,
        .WSAECONNREFUSED => error.ConnectionRefused,
        .WSAEFAULT => unreachable,
        .WSAEISCONN => unreachable, // error.AlreadyConnected,
        .WSAENETUNREACH => error.NetworkUnreachable,
        .WSAENOTSOCK => error.FileDescriptorNotASocket,
        .WSAEPROTOTYPE => unreachable,
        .WSAETIMEDOUT => error.ConnectionTimedOut,
        .WSAECONNRESET => error.ConnectionResetByPeer,
        else => |e| blk: {
            std.debug.print("winsock error: {}", .{e});
            break :blk error.Unexpected;
        },
    };
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