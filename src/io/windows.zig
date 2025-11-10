const std = @import("std");
const stdx = @import("stdx");
const os = std.os;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.io);
const constants = @import("../constants.zig");
const common = @import("./common.zig");

const QueueType = @import("../queue.zig").QueueType;
const TimeOS = @import("../time.zig").TimeOS;
const buffer_limit = @import("../io.zig").buffer_limit;
const DirectIO = @import("../io.zig").DirectIO;

pub const IO = struct {
    pub const TCPOptions = common.TCPOptions;
    pub const ListenOptions = common.ListenOptions;

    iocp: os.windows.HANDLE,
    time_os: TimeOS = .{},
    io_pending: usize = 0,
    timeouts: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_timeouts" }),
    completed: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_completed" }),

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;

        _ = try os.windows.WSAStartup(2, 2);
        errdefer os.windows.WSACleanup() catch unreachable;

        const iocp = try os.windows.CreateIoCompletionPort(
            os.windows.INVALID_HANDLE_VALUE,
            null,
            0,
            0,
        );
        return IO{ .iocp = iocp };
    }

    pub fn deinit(self: *IO) void {
        assert(self.iocp != os.windows.INVALID_HANDLE_VALUE);
        os.windows.CloseHandle(self.iocp);
        self.iocp = os.windows.INVALID_HANDLE_VALUE;

        os.windows.WSACleanup() catch unreachable;
    }

    pub fn run(self: *IO) !void {
        return self.flush(.non_blocking);
    }

    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        const Callback = struct {
            fn on_timeout(
                timed_out: *bool,
                completion: *Completion,
                result: TimeoutError!void,
            ) void {
                _ = result catch unreachable;
                _ = completion;
                timed_out.* = true;
            }
        };

        var timed_out = false;
        var completion: Completion = undefined;
        self.timeout(*bool, &timed_out, Callback.on_timeout, &completion, nanoseconds);

        while (!timed_out) {
            try self.flush(.blocking);
        }
    }

    const FlushMode = enum {
        blocking,
        non_blocking,
    };

    fn flush(self: *IO, mode: FlushMode) !void {
        if (self.completed.empty()) {
            // Compute how long to poll by flushing timeout completions.
            // NOTE: this may push to completed queue.
            var timeout_ms: ?os.windows.DWORD = null;
            if (self.flush_timeouts()) |expires_ns| {
                // 0ns expires should have been completed not returned.
                assert(expires_ns != 0);
                // Round up sub-millisecond expire times to the next millisecond.
                const expires_ms = (expires_ns + (std.time.ns_per_ms / 2)) / std.time.ns_per_ms;
                // Saturating cast to DWORD milliseconds.
                const expires = std.math.cast(os.windows.DWORD, expires_ms) orelse
                    std.math.maxInt(os.windows.DWORD);
                // Max DWORD is reserved for INFINITE so cap the cast at max - 1.
                timeout_ms = if (expires == os.windows.INFINITE) expires - 1 else expires;
            }

            // Poll for IO iff there's IO pending and flush_timeouts() found no ready completions.
            if (self.io_pending > 0 and self.completed.empty()) {
                // In blocking mode, we're always waiting at least until the timeout by run_for_ns.
                // In non-blocking mode, we shouldn't wait at all.
                const io_timeout = switch (mode) {
                    .blocking => timeout_ms orelse @panic("IO.flush blocking unbounded"),
                    .non_blocking => 0,
                };

                var events: [64]os.windows.OVERLAPPED_ENTRY = undefined;
                const num_events: u32 = os.windows.GetQueuedCompletionStatusEx(
                    self.iocp,
                    &events,
                    io_timeout,
                    false, // Non-alertable wait.
                ) catch |err| switch (err) {
                    error.Timeout => 0,
                    error.Aborted => unreachable,
                    else => |e| return e,
                };

                assert(self.io_pending >= num_events);
                self.io_pending -= num_events;

                for (events[0..num_events]) |event| {
                    const raw_overlapped = event.lpOverlapped;
                    const overlapped: *Completion.Overlapped = @fieldParentPtr(
                        "raw",
                        raw_overlapped,
                    );
                    const completion = overlapped.completion;
                    completion.link = .{};
                    self.completed.push(completion);
                }
            }
        }

        // Dequeue and invoke all the completions currently ready.
        // Must read all `completions` before invoking the callbacks
        // as the callbacks could potentially submit more completions.
        var completed = self.completed;
        self.completed.reset();
        while (completed.pop()) |completion| {
            (completion.callback)(Completion.Context{
                .io = self,
                .completion = completion,
            });
        }
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_expires: ?u64 = null;
        var current_time: ?u64 = null;

        // Iterate through the timeouts, returning min_expires at the end.
        var timeouts_iterator = self.timeouts.iterate();
        while (timeouts_iterator.next()) |completion| {
            // Lazily get the current time.
            const now = current_time orelse self.time_os.time().monotonic().ns;
            current_time = now;

            // Move the completion to completed if it expired.
            if (now >= completion.operation.timeout.deadline) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            // If it's still waiting, update min_timeout.
            const expires = completion.operation.timeout.deadline - now;
            if (min_expires) |current_min_expires| {
                min_expires = @min(expires, current_min_expires);
            } else {
                min_expires = expires;
            }
        }

        return min_expires;
    }

    /// This struct holds the data needed for a single IO operation.
    pub const Completion = struct {
        link: QueueType(Completion).Link,
        context: ?*anyopaque,
        callback: *const fn (Context) void,
        operation: Operation,

        const Context = struct {
            io: *IO,
            completion: *Completion,
        };

        const Overlapped = struct {
            raw: os.windows.OVERLAPPED,
            completion: *Completion,
        };

        const Transfer = struct {
            socket: socket_t,
            buf: os.windows.ws2_32.WSABUF,
            overlapped: Overlapped,
            pending: bool,
        };

        const Operation = union(enum) {
            accept: struct {
                overlapped: Overlapped,
                listen_socket: socket_t,
                client_socket: ?socket_t,
                addr_buffer: [(@sizeOf(std.net.Address) + 16) * 2]u8 align(4),
            },
            connect: struct {
                socket: socket_t,
                address: std.net.Address,
                overlapped: Overlapped,
                pending: bool,
            },
            fsync: struct {
                fd: fd_t,
            },
            send: Transfer,
            recv: Transfer,
            read: struct {
                fd: fd_t,
                buf: [*]u8,
                len: u32,
                offset: u64,
                overlapped: Overlapped,
                pending: bool,
            },
            write: struct {
                fd: fd_t,
                buf: [*]const u8,
                len: u32,
                offset: u64,
                overlapped: Overlapped,
                pending: bool,
            },
            close: struct {
                fd: fd_t,
            },
            timeout: struct {
                deadline: u64,
            },
            event: Overlapped,
        };
    };

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime op_tag: std.meta.Tag(Completion.Operation),
        op_data: std.meta.TagPayload(Completion.Operation, op_tag),
        comptime OperationImpl: type,
    ) void {
        const Callback = struct {
            fn onComplete(ctx: Completion.Context) void {
                // Perform the operation and get the result.
                const data = &@field(ctx.completion.operation, @tagName(op_tag));
                const result = OperationImpl.do_operation(ctx, data);

                // For OVERLAPPED IO, error.WouldBlock assumes that it will be completed by IOCP.
                switch (op_tag) {
                    .accept, .read, .recv, .connect, .write, .send => {
                        _ = result catch |err| switch (err) {
                            error.WouldBlock => {
                                ctx.io.io_pending += 1;
                                return;
                            },
                            else => {},
                        };
                    },
                    else => {},
                }

                // The completion is finally ready to invoke the callback.
                callback(
                    @ptrCast(@alignCast(ctx.completion.context)),
                    ctx.completion,
                    result,
                );
            }
        };

        // Setup the completion with the callback wrapper above.
        completion.* = .{
            .link = .{},
            .context = @ptrCast(context),
            .callback = Callback.onComplete,
            .operation = @unionInit(Completion.Operation, @tagName(op_tag), op_data),
        };

        // Submit the completion onto the right queue.
        switch (op_tag) {
            .timeout => self.timeouts.push(completion),
            else => self.completed.push(completion),
        }
    }

    pub fn cancel_all(_: *IO) void {
        // TODO Cancel in-flight async IO and wait for all completions.
    }

    pub const CancelError = error{
        NotRunning,
        NotInterruptable,
    } || posix.UnexpectedError;

    pub fn cancel(
        _: *IO,
        comptime Context: type,
        _: Context,
        comptime _: fn (
            context: Context,
            completion: *Completion,
            result: CancelError!void,
        ) void,
        _: struct {
            completion: *Completion,
            target: *Completion,
        },
    ) void {
        @panic("cancelation is not supported on windows");
    }

    pub const AcceptError = posix.AcceptError || posix.SetSockOptError;

    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!socket_t,
        ) void,
        completion: *Completion,
        socket: socket_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .accept,
            .{
                .overlapped = undefined,
                .listen_socket = socket,
                .client_socket = null,
                .addr_buffer = undefined,
            },
            struct {
                fn do_operation(
                    ctx: Completion.Context,
                    op: anytype,
                ) AcceptError!socket_t {
                    var flags: os.windows.DWORD = undefined;
                    var transferred: os.windows.DWORD = undefined;

                    const rc = if (op.client_socket == null) blk: {
                        // When first called, the client_socket is invalid so we start the op.
                        // Create the socket that will be used for accept.
                        op.client_socket = ctx.io.open_socket(
                            posix.AF.INET,
                            posix.SOCK.STREAM,
                            posix.IPPROTO.TCP,
                        ) catch |err| switch (err) {
                            error.AddressFamilyNotSupported => unreachable,
                            error.ProtocolNotSupported => unreachable,
                            else => |e| return e,
                        };

                        var sync_bytes_read: os.windows.DWORD = undefined;
                        op.overlapped = .{
                            .raw = std.mem.zeroes(os.windows.OVERLAPPED),
                            .completion = ctx.completion,
                        };

                        // Start the asynchronous accept with the created socket.
                        break :blk os.windows.ws2_32.AcceptEx(
                            op.listen_socket,
                            op.client_socket.?,
                            &op.addr_buffer,
                            0,
                            @sizeOf(std.net.Address) + 16,
                            @sizeOf(std.net.Address) + 16,
                            &sync_bytes_read,
                            &op.overlapped.raw,
                        );
                    } else blk: {
                        // Called after accept was started, so get the result.
                        break :blk os.windows.ws2_32.WSAGetOverlappedResult(
                            op.listen_socket,
                            &op.overlapped.raw,
                            &transferred,
                            os.windows.FALSE, // Don't wait.
                            &flags,
                        );
                    };

                    // Return the socket if we succeed in accepting.
                    if (rc != os.windows.FALSE) {
                        // Enables getsockopt, setsockopt, getsockname, getpeername.
                        _ = os.windows.ws2_32.setsockopt(
                            op.client_socket.?,
                            os.windows.ws2_32.SOL.SOCKET,
                            os.windows.ws2_32.SO.UPDATE_ACCEPT_CONTEXT,
                            null,
                            0,
                        );

                        return op.client_socket.?;
                    }

                    // Destroy the client_socket we created if we get a non WouldBlock error.
                    errdefer |err| switch (err) {
                        error.WouldBlock => {},
                        else => {
                            ctx.io.close_socket(op.client_socket.?);
                            op.client_socket = null;
                        },
                    };

                    return switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSA_IO_PENDING, .WSAEWOULDBLOCK, .WSA_IO_INCOMPLETE => error.WouldBlock,
                        .WSANOTINITIALISED => unreachable, // WSAStartup() was called.
                        .WSAENETDOWN => unreachable, // WinSock error.
                        .WSAENOTSOCK => error.FileDescriptorNotASocket,
                        .WSAEOPNOTSUPP => error.OperationNotSupported,
                        .WSA_INVALID_HANDLE => unreachable, // We don't use hEvent in OVERLAPPED.
                        .WSAEFAULT, .WSA_INVALID_PARAMETER => unreachable, // Params should be ok.
                        .WSAECONNRESET => error.ConnectionAborted,
                        .WSAEMFILE => unreachable, // We create our own descriptor so its available.
                        .WSAENOBUFS => error.SystemResources,
                        .WSAEINTR, .WSAEINPROGRESS => unreachable, // No blocking calls.
                        else => |err| os.windows.unexpectedWSAError(err),
                    };
                }
            },
        );
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || posix.UnexpectedError;

    pub const ConnectError = posix.ConnectError || error{FileDescriptorNotASocket};

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
        socket: socket_t,
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
                .overlapped = undefined,
                .pending = false,
            },
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) ConnectError!void {
                    var flags: os.windows.DWORD = undefined;
                    var transferred: os.windows.DWORD = undefined;

                    const rc = blk: {
                        // Poll for the result if we've already started the connect op.
                        if (op.pending) {
                            break :blk os.windows.ws2_32.WSAGetOverlappedResult(
                                op.socket,
                                &op.overlapped.raw,
                                &transferred,
                                os.windows.FALSE, // Don't wait.
                                &flags,
                            );
                        }

                        // ConnectEx requires the socket to be initially bound (INADDR_ANY).
                        const inaddr_any: [4]u8 = @splat(0);
                        const bind_addr = std.net.Address.initIp4(inaddr_any, 0);
                        posix.bind(
                            op.socket,
                            &bind_addr.any,
                            bind_addr.getOsSockLen(),
                        ) catch |err| switch (err) {
                            error.AccessDenied => unreachable,
                            error.SymLinkLoop => unreachable,
                            error.NameTooLong => unreachable,
                            error.NotDir => unreachable,
                            error.ReadOnlyFileSystem => unreachable,
                            error.NetworkSubsystemFailed => unreachable,
                            error.AlreadyBound => unreachable,
                            else => |e| return e,
                        };

                        const LPFN_CONNECTEX = *const fn (
                            Socket: os.windows.ws2_32.SOCKET,
                            SockAddr: *const os.windows.ws2_32.sockaddr,
                            SockLen: posix.socklen_t,
                            SendBuf: ?*const anyopaque,
                            SendBufLen: os.windows.DWORD,
                            BytesSent: *os.windows.DWORD,
                            Overlapped: *os.windows.OVERLAPPED,
                        ) callconv(os.windows.WINAPI) os.windows.BOOL;

                        // Find the ConnectEx function by dynamically looking it up on the socket.
                        // TODO: use `os.windows.loadWinsockExtensionFunction` once the function
                        // pointer is no longer required to be comptime.
                        var connect_ex: LPFN_CONNECTEX = undefined;
                        var num_bytes: os.windows.DWORD = undefined;
                        const guid = os.windows.ws2_32.WSAID_CONNECTEX;
                        const socket_error = os.windows.ws2_32.SOCKET_ERROR;
                        switch (os.windows.ws2_32.WSAIoctl(
                            op.socket,
                            os.windows.ws2_32.SIO_GET_EXTENSION_FUNCTION_POINTER,
                            @ptrCast(&guid),
                            @sizeOf(os.windows.GUID),
                            @ptrCast(&connect_ex),
                            @sizeOf(LPFN_CONNECTEX),
                            &num_bytes,
                            null,
                            null,
                        )) {
                            socket_error => switch (os.windows.ws2_32.WSAGetLastError()) {
                                .WSAEOPNOTSUPP => unreachable,
                                .WSAENOTSOCK => unreachable,
                                else => |err| return os.windows.unexpectedWSAError(err),
                            },
                            else => assert(num_bytes == @sizeOf(LPFN_CONNECTEX)),
                        }

                        op.pending = true;
                        op.overlapped = .{
                            .raw = std.mem.zeroes(os.windows.OVERLAPPED),
                            .completion = ctx.completion,
                        };

                        // Start the connect operation.
                        break :blk (connect_ex)(
                            op.socket,
                            &op.address.any,
                            op.address.getOsSockLen(),
                            null,
                            0,
                            &transferred,
                            &op.overlapped.raw,
                        );
                    };

                    // Return if we succeeded in connecting.
                    if (rc != os.windows.FALSE) {
                        // Enables getsockopt, setsockopt, getsockname, getpeername.
                        _ = os.windows.ws2_32.setsockopt(
                            op.socket,
                            os.windows.ws2_32.SOL.SOCKET,
                            os.windows.ws2_32.SO.UPDATE_CONNECT_CONTEXT,
                            null,
                            0,
                        );

                        return;
                    }

                    return switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSA_IO_PENDING, .WSAEWOULDBLOCK, .WSA_IO_INCOMPLETE => error.WouldBlock,
                        .WSAEALREADY => error.WouldBlock,
                        .WSANOTINITIALISED => unreachable, // WSAStartup() was called.
                        .WSAENETDOWN => unreachable, // Network subsystem is down.
                        .WSAEADDRNOTAVAIL => error.AddressNotAvailable,
                        .WSAEAFNOSUPPORT => error.AddressFamilyNotSupported,
                        .WSAECONNREFUSED => error.ConnectionRefused,
                        .WSAEFAULT => unreachable, // All addresses should be valid.
                        .WSAEINVAL => unreachable, // Invalid socket type.
                        .WSAEHOSTUNREACH, .WSAENETUNREACH => error.NetworkUnreachable,
                        .WSAENOBUFS => error.SystemResources,
                        .WSAENOTSOCK => unreachable, // Socket is not bound or is listening.
                        .WSAETIMEDOUT => error.ConnectionTimedOut,
                        .WSA_INVALID_HANDLE => unreachable, // We don't use hEvent in OVERLAPPED.
                        else => |err| os.windows.unexpectedWSAError(err),
                    };
                }
            },
        );
    }

    pub const FsyncError = posix.SyncError || posix.UnexpectedError;

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
        fd: fd_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .fsync,
            .{
                .fd = fd,
            },
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) FsyncError!void {
                    _ = ctx;
                    return posix.fsync(op.fd);
                }
            },
        );
    }

    pub const SendError = posix.SendError;

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
        socket: socket_t,
        buffer: []const u8,
    ) void {
        const transfer = Completion.Transfer{
            .socket = socket,
            .buf = os.windows.ws2_32.WSABUF{
                .len = @intCast(buffer_limit(buffer.len)),
                .buf = @constCast(buffer.ptr),
            },
            .overlapped = undefined,
            .pending = false,
        };

        self.submit(
            context,
            callback,
            completion,
            .send,
            transfer,
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) SendError!usize {
                    var flags: os.windows.DWORD = undefined;
                    var transferred: os.windows.DWORD = undefined;

                    const rc = blk: {
                        // Poll for the result if we've already started the send op.
                        if (op.pending) {
                            break :blk os.windows.ws2_32.WSAGetOverlappedResult(
                                op.socket,
                                &op.overlapped.raw,
                                &transferred,
                                os.windows.FALSE, // Don't wait.
                                &flags,
                            );
                        }

                        op.pending = true;
                        op.overlapped = .{
                            .raw = std.mem.zeroes(os.windows.OVERLAPPED),
                            .completion = ctx.completion,
                        };

                        // Start the send operation.
                        break :blk switch (os.windows.ws2_32.WSASend(
                            op.socket,
                            @ptrCast(&op.buf),
                            1, // One buffer.
                            &transferred,
                            0, // No flags.
                            &op.overlapped.raw,
                            null,
                        )) {
                            os.windows.ws2_32.SOCKET_ERROR => @as(
                                os.windows.BOOL,
                                os.windows.FALSE,
                            ),
                            0 => os.windows.TRUE,
                            else => unreachable,
                        };
                    };

                    // Return bytes transferred on success.
                    if (rc != os.windows.FALSE)
                        return transferred;

                    return switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSA_IO_PENDING, .WSAEWOULDBLOCK, .WSA_IO_INCOMPLETE => error.WouldBlock,
                        .WSANOTINITIALISED => unreachable, // WSAStartup() was called
                        .WSA_INVALID_HANDLE => unreachable, // We don't use OVERLAPPED.hEvent
                        .WSA_INVALID_PARAMETER => unreachable, // Parameters are fine.
                        .WSAECONNABORTED => error.ConnectionResetByPeer,
                        .WSAECONNRESET => error.ConnectionResetByPeer,
                        .WSAEFAULT => unreachable, // Invalid buffer.
                        .WSAEINTR => unreachable, // This is non blocking.
                        .WSAEINPROGRESS => unreachable, // This is non blocking.
                        .WSAEINVAL => unreachable, // Invalid socket type.
                        .WSAEMSGSIZE => error.MessageTooBig,
                        .WSAENETDOWN => error.NetworkSubsystemFailed,
                        .WSAENETRESET => error.ConnectionResetByPeer,
                        .WSAENOBUFS => error.SystemResources,
                        .WSAENOTCONN => error.FileDescriptorNotASocket,
                        .WSAEOPNOTSUPP => unreachable, // We don't use MSG_OOB or MSG_PARTIAL.
                        .WSAESHUTDOWN => error.BrokenPipe,
                        .WSA_OPERATION_ABORTED => unreachable, // Operation was cancelled.
                        else => |err| os.windows.unexpectedWSAError(err),
                    };
                }
            },
        );
    }

    pub fn send_now(_: *IO, _: socket_t, _: []const u8) ?usize {
        return null; // No support for best-effort non-blocking synchronous send.
    }

    pub const RecvError = posix.RecvFromError;

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
        socket: socket_t,
        buffer: []u8,
    ) void {
        const transfer = Completion.Transfer{
            .socket = socket,
            .buf = os.windows.ws2_32.WSABUF{
                .len = @intCast(buffer_limit(buffer.len)),
                .buf = buffer.ptr,
            },
            .overlapped = undefined,
            .pending = false,
        };

        self.submit(
            context,
            callback,
            completion,
            .recv,
            transfer,
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) RecvError!usize {
                    var flags: os.windows.DWORD = 0; // Used both as input and output.
                    var transferred: os.windows.DWORD = undefined;

                    const rc = blk: {
                        // Poll for the result if we've already started the recv op.
                        if (op.pending) {
                            break :blk os.windows.ws2_32.WSAGetOverlappedResult(
                                op.socket,
                                &op.overlapped.raw,
                                &transferred,
                                os.windows.FALSE, // Don't wait.
                                &flags,
                            );
                        }

                        op.pending = true;
                        op.overlapped = .{
                            .raw = std.mem.zeroes(os.windows.OVERLAPPED),
                            .completion = ctx.completion,
                        };

                        // Start the recv operation.
                        break :blk switch (os.windows.ws2_32.WSARecv(
                            op.socket,
                            @ptrCast(&op.buf),
                            1, // one buffer
                            &transferred,
                            &flags,
                            &op.overlapped.raw,
                            null,
                        )) {
                            os.windows.ws2_32.SOCKET_ERROR => @as(
                                os.windows.BOOL,
                                os.windows.FALSE,
                            ),
                            0 => os.windows.TRUE,
                            else => unreachable,
                        };
                    };

                    // Return bytes received on success.
                    if (rc != os.windows.FALSE)
                        return transferred;

                    return switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSA_IO_PENDING, .WSAEWOULDBLOCK, .WSA_IO_INCOMPLETE => error.WouldBlock,
                        .WSANOTINITIALISED => unreachable, // WSAStartup() was called
                        .WSA_INVALID_HANDLE => unreachable, // We don't use OVERLAPPED.hEvent.
                        .WSA_INVALID_PARAMETER => unreachable, // Parameters are fine.
                        .WSAECONNABORTED => error.ConnectionRefused,
                        .WSAECONNRESET => error.ConnectionResetByPeer,
                        .WSAEDISCON => unreachable, // We only stream sockets.
                        .WSAEFAULT => unreachable, // Invalid buffer.
                        .WSAEINTR => unreachable, // This is non blocking.
                        .WSAEINPROGRESS => unreachable, // This is non blocking.
                        .WSAEINVAL => unreachable, // Invalid socket type
                        .WSAEMSGSIZE => error.MessageTooBig,
                        .WSAENETDOWN => error.NetworkSubsystemFailed,
                        .WSAENETRESET => error.ConnectionResetByPeer,
                        .WSAENOTCONN => error.SocketNotConnected,
                        .WSAEOPNOTSUPP => unreachable, // We don't use MSG_OOB or MSG_PARTIAL.
                        .WSAESHUTDOWN => error.SocketNotConnected,
                        .WSAETIMEDOUT => error.ConnectionRefused,
                        .WSA_OPERATION_ABORTED => unreachable, // Operation was cancelled.
                        else => |err| os.windows.unexpectedWSAError(err),
                    };
                }
            },
        );
    }

    pub const OpenatError = posix.OpenError || posix.UnexpectedError;

    fn do_file_io(ctx: Completion.Context, op: anytype, comptime overlapped_fn: anytype) !usize {
        var transferred: os.windows.DWORD = undefined;
        const rc = blk: {
            // Poll result if already started.
            if (op.pending) break :blk os.windows.kernel32.GetOverlappedResult(
                op.fd,
                &op.overlapped.raw,
                &transferred,
                os.windows.FALSE, // Don't wait here.
            );

            // Start the operation.
            op.pending = true;
            op.overlapped = .{
                .raw = .{
                    .Internal = 0,
                    .InternalHigh = 0,
                    .DUMMYUNIONNAME = .{
                        .DUMMYSTRUCTNAME = .{
                            .Offset = @truncate(op.offset),
                            .OffsetHigh = @truncate(op.offset >> 32),
                        },
                    },
                    .hEvent = null,
                },
                .completion = ctx.completion,
            };
            break :blk overlapped_fn(op.fd, op.buf, op.len, &transferred, &op.overlapped.raw);
        };

        // Operation completed successfully.
        if (rc != os.windows.FALSE) {
            return transferred;
        }

        return switch (os.windows.kernel32.GetLastError()) {
            .IO_PENDING => error.WouldBlock,
            .INVALID_USER_BUFFER, .NOT_ENOUGH_MEMORY => error.SystemResources,
            .NOT_ENOUGH_QUOTA => error.SystemResources,
            .OPERATION_ABORTED => unreachable, // overlapped_fn() doesn't get cancelled.
            // ReadFile and WriteFile don't allow partial IO (acting more like readAll/writeAll)
            // so assume the offset is correct and simulate partial IO by returning 0 bytes moved.
            .HANDLE_EOF => return 0,
            else => |err| return os.windows.unexpectedError(err),
        };
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
        ConnectionTimedOut,
    } || posix.UnexpectedError;

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
        fd: fd_t,
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
                .len = @as(u32, @intCast(buffer_limit(buffer.len))),
                .offset = offset,
                .overlapped = undefined,
                .pending = false,
            },
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) ReadError!usize {
                    return do_file_io(ctx, op, os.windows.kernel32.ReadFile);
                }
            },
        );
    }

    pub const WriteError = posix.PWriteError;

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
        fd: fd_t,
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
                .len = @as(u32, @intCast(buffer_limit(buffer.len))),
                .offset = offset,
                .overlapped = undefined,
                .pending = false,
            },
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) WriteError!usize {
                    return do_file_io(ctx, op, os.windows.kernel32.WriteFile);
                }
            },
        );
    }

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
        fd: fd_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .close,
            .{ .fd = fd },
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) CloseError!void {
                    // Check if the fd is a SOCKET by seeing if getsockopt() returns ENOTSOCK
                    // https://stackoverflow.com/a/50981652
                    const socket: socket_t = @ptrCast(op.fd);
                    getsockoptError(socket) catch |err| switch (err) {
                        error.FileDescriptorNotASocket => return os.windows.CloseHandle(op.fd),
                        else => {},
                    };

                    ctx.io.close_socket(socket);
                }
            },
        );
    }

    pub const TimeoutError = error{Canceled} || posix.UnexpectedError;

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
        // Special case a zero timeout as a yield.
        if (nanoseconds == 0) {
            completion.* = .{
                .link = .{},
                .context = @ptrCast(context),
                .operation = undefined,
                .callback = struct {
                    fn on_complete(ctx: Completion.Context) void {
                        const _context: Context = @ptrCast(@alignCast(ctx.completion.context));
                        callback(_context, ctx.completion, {});
                    }
                }.on_complete,
            };

            self.completed.push(completion);
            return;
        }

        self.submit(
            context,
            callback,
            completion,
            .timeout,
            .{ .deadline = self.time_os.time().monotonic().ns + nanoseconds },
            struct {
                fn do_operation(ctx: Completion.Context, op: anytype) TimeoutError!void {
                    _ = ctx;
                    _ = op;
                    return;
                }
            },
        );
    }

    pub const Event = u1;
    pub const INVALID_EVENT: Event = 0;

    pub fn open_event(
        self: *IO,
    ) !Event {
        _ = self;
        // Events on Windows don't need an identifier,
        // they're handled just by the OVERLAPPED structure.
        return INVALID_EVENT + 1;
    }

    pub fn event_listen(
        self: *IO,
        event: Event,
        completion: *Completion,
        comptime on_event: fn (*Completion) void,
    ) void {
        assert(event != INVALID_EVENT);
        completion.* = .{
            .link = .{},
            .context = null,
            .operation = .{
                .event = .{
                    .raw = std.mem.zeroes(os.windows.OVERLAPPED),
                    .completion = completion,
                },
            },
            .callback = struct {
                fn on_complete(ctx: Completion.Context) void {
                    on_event(ctx.completion);
                }
            }.on_complete,
        };

        // Conceptually start listening by bumping the io_pending count.
        self.io_pending += 1;
    }

    pub fn event_trigger(self: *IO, event: Event, completion: *Completion) void {
        assert(event != INVALID_EVENT);
        os.windows.PostQueuedCompletionStatus(
            self.iocp,
            undefined,
            undefined,
            &completion.operation.event.raw,
        ) catch unreachable;
    }

    pub fn close_event(self: *IO, event: Event) void {
        _ = self;
        // Nothing to close as events are just intrusive OVERLAPPED structs.
        assert(event != INVALID_EVENT);
    }

    pub const socket_t = posix.socket_t;

    /// Creates a TCP socket that can be used for async operations with the IO instance.
    pub fn open_socket_tcp(self: *IO, family: u32, options: TCPOptions) !socket_t {
        const socket = try self.open_socket(
            @bitCast(family),
            posix.SOCK.STREAM,
            posix.IPPROTO.TCP,
        );
        errdefer self.close_socket(socket);

        try common.tcp_options(socket, options);
        return socket;
    }

    /// Creates a UDP socket that can be used for async operations with the IO instance.
    pub fn open_socket_udp(self: *IO, family: u32) !socket_t {
        return try self.open_socket(
            @bitCast(family),
            posix.SOCK.DGRAM,
            posix.IPPROTO.UDP,
        );
    }

    fn open_socket(self: *IO, family: u32, sock_type: i32, protocol: i32) !socket_t {
        // Equivalent to SOCK_NONBLOCK | SOCK_CLOEXEC.
        const socket_flags: os.windows.DWORD =
            os.windows.ws2_32.WSA_FLAG_OVERLAPPED |
            os.windows.ws2_32.WSA_FLAG_NO_HANDLE_INHERIT;

        const socket = try os.windows.WSASocketW(
            @bitCast(family),
            sock_type,
            protocol,
            null,
            0,
            socket_flags,
        );
        errdefer self.close_socket(socket);

        try self.register_handle(@ptrCast(socket));
        return socket;
    }

    /// Register the IO handle for overlapped operations.
    fn register_handle(self: *IO, handle: os.windows.HANDLE) !void {
        const iocp_handle = try os.windows.CreateIoCompletionPort(handle, self.iocp, 0, 0);
        assert(iocp_handle == self.iocp);

        // Ensure that synchronous IO completion doesn't queue an unneeded overlapped
        // and that the event for the handle (WaitForSingleObject) doesn't need to be set.
        var mode: os.windows.BYTE = 0;
        mode |= os.windows.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS;
        mode |= os.windows.FILE_SKIP_SET_EVENT_ON_HANDLE;
        try os.windows.SetFileCompletionNotificationModes(handle, mode);
    }

    /// Closes a socket opened by the IO instance.
    pub fn close_socket(self: *IO, socket: socket_t) void {
        _ = self;
        posix.close(socket);
    }

    /// Listen on the given TCP socket.
    /// Returns socket resolved address, which might be more specific
    /// than the input address (e.g., listening on port 0).
    pub fn listen(
        _: *IO,
        fd: socket_t,
        address: std.net.Address,
        options: ListenOptions,
    ) !std.net.Address {
        return common.listen(fd, address, options);
    }

    pub fn shutdown(_: *IO, socket: socket_t, how: posix.ShutdownHow) posix.ShutdownError!void {
        return posix.shutdown(socket, how);
    }

    /// Opens a directory with read only access.
    pub fn open_dir(dir_path: []const u8) !fd_t {
        const dir = try std.fs.cwd().openDir(dir_path, .{});
        return dir.fd;
    }

    pub const fd_t = posix.fd_t;
    pub const INVALID_FILE = os.windows.INVALID_HANDLE_VALUE;

    fn open_file_handle(
        self: *IO,
        dir_handle: fd_t,
        relative_path: []const u8,
        purpose: enum { format, open, inspect },
    ) !fd_t {
        const path_w = try os.windows.sliceToPrefixedFileW(dir_handle, relative_path);

        // FILE_CREATE = O_CREAT | O_EXCL
        var creation_disposition: os.windows.DWORD = 0;
        switch (purpose) {
            .format => {
                creation_disposition = os.windows.FILE_CREATE;
                log.info("creating \"{s}\"...", .{relative_path});
            },
            .open, .inspect => {
                creation_disposition = os.windows.OPEN_EXISTING;
                log.info("opening \"{s}\"...", .{relative_path});
            },
        }

        // O_EXCL
        const shared_mode: os.windows.DWORD = 0;

        // O_RDWR
        // Zig's mask seems wonky; according to
        // https://learn.microsoft.com/en-us/windows/win32/api/winternl/nf-winternl-ntcreatefile
        // FILE_GENERIC_READ should include SYNCHRONIZE but it does not.
        var access_mask: os.windows.DWORD = 0;
        access_mask |= os.windows.SYNCHRONIZE;
        access_mask |= os.windows.GENERIC_READ;

        if (purpose != .inspect) {
            access_mask |= os.windows.GENERIC_WRITE;
        }

        // O_DIRECT | O_DSYNC
        // NB: These are NtDll flags, not to be confused with the Win32 style flags that are
        // similar but different (!).
        var attributes: os.windows.DWORD = 0;
        attributes |= os.windows.FILE_NO_INTERMEDIATE_BUFFERING;
        attributes |= os.windows.FILE_WRITE_THROUGH;

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert((attributes & os.windows.FILE_WRITE_THROUGH) > 0);

        // It's a little confusing, but with NtCreateFile, which is what windows_open_file uses
        // under the hood, not specifying anything gets you a file capable of overlapped IO.
        // FILE_FLAG_OVERLAPPED and co belong to the higher level kernel32 API.
        const handle = try windows_open_file(path_w.span(), .{
            .access_mask = access_mask,
            .dir = dir_handle,
            .sa = null,
            .share_access = shared_mode,
            .creation = creation_disposition,
            .filter = .file_only,
            .follow_symlinks = false,
        }, attributes);

        if (handle == os.windows.INVALID_HANDLE_VALUE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .FILE_NOT_FOUND => error.FileNotFound,
                .SHARING_VIOLATION, .ACCESS_DENIED => error.AccessDenied,
                else => |err| {
                    return os.windows.unexpectedError(err);
                },
            };
        }

        errdefer os.windows.CloseHandle(handle);

        // Register the file with the IO handle for overlapped operations.
        try self.register_handle(handle);

        return handle;
    }

    pub const OpenDataFilePurpose = enum { format, open, inspect };
    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (required on windows).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    /// - Allocates the file contiguously on disk if this is supported by the file system.
    /// - Ensures that the file data is durable on disk.
    ///   The caller is responsible for ensuring that the parent directory inode is durable.
    /// - Verifies that the file size matches the expected file size before returning.
    pub fn open_data_file(
        self: *IO,
        dir_handle: fd_t,
        relative_path: []const u8,
        size: u64,
        purpose: OpenDataFilePurpose,
        direct_io: DirectIO,
    ) !fd_t {
        assert(relative_path.len > 0);
        assert(size % constants.sector_size == 0);
        // On windows, assume that Direct IO is always available.
        _ = direct_io;

        const handle = switch (purpose) {
            .format => try self.open_file_handle(dir_handle, relative_path, .format),
            .open => try self.open_file_handle(dir_handle, relative_path, .open),
            .inspect => try self.open_file_handle(
                dir_handle,
                relative_path,
                .inspect,
            ),
        };
        errdefer os.windows.CloseHandle(handle);

        // Obtain an advisory exclusive lock
        // even when we haven't given shared access to other processes.
        fs_lock(handle, size) catch |err| switch (err) {
            error.WouldBlock => {
                if (purpose == .inspect) {
                    log.warn(
                        "another process holds the data file lock - results may be inconsistent",
                        .{},
                    );
                } else {
                    @panic("another process holds the data file lock");
                }
            },
            else => return err,
        };

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        if (purpose == .format) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(handle, size) catch {
                log.warn("file system failed to preallocate the file memory", .{});
                log.info("allocating by writing to the last sector of the file instead...", .{});

                const sector_size = constants.sector_size;
                const sector: [sector_size]u8 align(sector_size) = @splat(0);

                // Handle partial writes where the physical sector is less than a logical sector:
                const write_offset = size - sector.len;
                var written: usize = 0;
                while (written < sector.len) {
                    written += try posix.pwrite(
                        handle,
                        sector[written..],
                        write_offset + written,
                    );
                }
            };
        }

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        if (purpose != .inspect) {
            try posix.fsync(handle);
        }

        // We cannot fsync the directory handle on Windows.
        // We have no way to open a directory with write access.
        //
        // try posix.fsync(dir_handle);

        const file_size = try os.windows.GetFileSizeEx(handle);
        if (file_size < size) @panic("data file inode size was truncated or corrupted");

        return handle;
    }

    fn fs_lock(handle: fd_t, size: u64) !void {
        // TODO: Look into using SetFileIoOverlappedRange() for better unbuffered async IO perf
        // NOTE: Requires SeLockMemoryPrivilege.

        // hEvent = null
        // Offset & OffsetHigh = 0
        var lock_overlapped = std.mem.zeroes(os.windows.OVERLAPPED);

        // LOCK_EX | LOCK_NB
        var lock_flags: os.windows.DWORD = 0;
        lock_flags |= stdx.windows.LOCKFILE_EXCLUSIVE_LOCK;
        lock_flags |= stdx.windows.LOCKFILE_FAIL_IMMEDIATELY;

        const locked = stdx.windows.LockFileEx(
            handle,
            lock_flags,
            0, // Reserved param is always zero.
            @as(u32, @truncate(size)), // Low bits of size.
            @as(u32, @truncate(size >> 32)), // High bits of size.
            &lock_overlapped,
        );

        if (locked == os.windows.FALSE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .IO_PENDING => error.WouldBlock,
                else => |err| os.windows.unexpectedError(err),
            };
        }
    }

    fn fs_allocate(handle: fd_t, size: u64) !void {
        // TODO: Look into using SetFileValidData() instead
        // NOTE: Requires SE_MANAGE_VOLUME_NAME privilege

        // Move the file pointer to the start + size.
        const seeked = os.windows.kernel32.SetFilePointerEx(
            handle,
            @intCast(size),
            null, // No reference to new file pointer.
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
        const allocated = stdx.windows.SetEndOfFile(handle);
        if (allocated == os.windows.FALSE) {
            const err = os.windows.kernel32.GetLastError();
            return os.windows.unexpectedError(err);
        }
    }

    pub const PReadError = posix.PReadError;

    pub fn aof_blocking_write_all(_: *IO, fd: fd_t, buffer: []const u8) posix.WriteError!void {
        return common.aof_blocking_write_all(fd, buffer);
    }

    pub fn aof_blocking_pread_all(_: *IO, fd: fd_t, buffer: []u8, offset: u64) PReadError!usize {
        return common.aof_blocking_pread_all(fd, buffer, offset);
    }

    pub fn aof_blocking_close(_: *IO, fd: fd_t) void {
        return common.aof_blocking_close(fd);
    }
};

// TODO: use posix.getsockoptError when fixed for windows in stdlib.
fn getsockoptError(socket: posix.socket_t) IO.ConnectError!void {
    var err_code: u32 = undefined;
    var size: i32 = @sizeOf(u32);
    const rc = os.windows.ws2_32.getsockopt(
        socket,
        posix.SOL.SOCKET,
        posix.SO.ERROR,
        std.mem.asBytes(&err_code),
        &size,
    );

    if (rc != 0) {
        switch (os.windows.ws2_32.WSAGetLastError()) {
            .WSAENETDOWN => return error.NetworkUnreachable,
            .WSANOTINITIALISED => unreachable, // WSAStartup() was never called.

            // The address pointed to by optval or optlen is not in a valid part of the process
            // address space.
            .WSAEFAULT => unreachable,

            .WSAEINVAL => unreachable, // The level parameter is unknown or invalid.
            .WSAENOPROTOOPT => unreachable, // The option is unknown at the level indicated.
            .WSAENOTSOCK => return error.FileDescriptorNotASocket,
            else => |err| return os.windows.unexpectedWSAError(err),
        }
    }

    assert(size == 4);
    if (err_code == 0)
        return;

    const ws_err: os.windows.ws2_32.WinsockError = @enumFromInt(@as(u16, @intCast(err_code)));
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
        else => |e| os.windows.unexpectedWSAError(e),
    };
}

// Vendor std.os.windows.OpenFile so we can set file attributes. Add it as a parameter after
// `options`, so we don't have to vendor that struct too.
pub fn windows_open_file(
    sub_path_w: []const u16,
    options: os.windows.OpenFileOptions,
    file_flags: os.windows.ULONG,
) os.windows.OpenError!os.windows.HANDLE {
    if (std.mem.eql(u16, sub_path_w, &[_]u16{'.'}) and options.filter == .file_only) {
        return error.IsDir;
    }
    if (std.mem.eql(u16, sub_path_w, &[_]u16{ '.', '.' }) and options.filter == .file_only) {
        return error.IsDir;
    }

    var result: os.windows.HANDLE = undefined;

    const path_len_bytes = std.math.cast(u16, sub_path_w.len * 2) orelse return error.NameTooLong;
    var nt_name = os.windows.UNICODE_STRING{
        .Length = path_len_bytes,
        .MaximumLength = path_len_bytes,
        .Buffer = @constCast(sub_path_w.ptr),
    };
    var attr = os.windows.OBJECT_ATTRIBUTES{
        .Length = @sizeOf(os.windows.OBJECT_ATTRIBUTES),
        .RootDirectory = if (std.fs.path.isAbsoluteWindowsWTF16(sub_path_w)) null else options.dir,
        .Attributes = 0, // Note we do not use OBJ_CASE_INSENSITIVE here.
        .ObjectName = &nt_name,
        .SecurityDescriptor = if (options.sa) |ptr| ptr.lpSecurityDescriptor else null,
        .SecurityQualityOfService = null,
    };
    var io: os.windows.IO_STATUS_BLOCK = undefined;
    const file_or_dir_flag: os.windows.ULONG = switch (options.filter) {
        .file_only => os.windows.FILE_NON_DIRECTORY_FILE,
        .dir_only => os.windows.FILE_DIRECTORY_FILE,
        .any => 0,
    };
    // This code is changed slightly from Zig's stdlib: there, options.follow_symlinks enforces
    // FILE_SYNCHRONOUS_IO_NONALERT which stops overlapped IO.
    assert(!options.follow_symlinks);
    const flags: os.windows.ULONG = file_or_dir_flag | os.windows.FILE_OPEN_REPARSE_POINT;

    while (true) {
        const rc = os.windows.ntdll.NtCreateFile(
            &result,
            options.access_mask,
            &attr,
            &io,
            null,
            os.windows.FILE_ATTRIBUTE_NORMAL,
            options.share_access,
            options.creation,
            flags | file_flags,
            null,
            0,
        );
        switch (rc) {
            .SUCCESS => return result,
            .OBJECT_NAME_INVALID => unreachable,
            .OBJECT_NAME_NOT_FOUND => return error.FileNotFound,
            .OBJECT_PATH_NOT_FOUND => return error.FileNotFound,
            .BAD_NETWORK_PATH => return error.NetworkNotFound, // \\server was not found.
            // \\server was found but \\server\share wasn't.
            .BAD_NETWORK_NAME => return error.NetworkNotFound,
            .NO_MEDIA_IN_DEVICE => return error.NoDevice,
            .INVALID_PARAMETER => unreachable,
            .SHARING_VIOLATION => return error.AccessDenied,
            .ACCESS_DENIED => return error.AccessDenied,
            .PIPE_BUSY => return error.PipeBusy,
            .OBJECT_PATH_SYNTAX_BAD => unreachable,
            .OBJECT_NAME_COLLISION => return error.PathAlreadyExists,
            .FILE_IS_A_DIRECTORY => return error.IsDir,
            .NOT_A_DIRECTORY => return error.NotDir,
            .USER_MAPPED_FILE => return error.AccessDenied,
            .INVALID_HANDLE => unreachable,
            .DELETE_PENDING => {
                // This error means that there *was* a file in this location on
                // the file system, but it was deleted. However, the OS is not
                // finished with the deletion operation, and so this CreateFile
                // call has failed. There is not really a sane way to handle
                // this other than retrying the creation after the OS finishes
                // the deletion.
                std.time.sleep(std.time.ns_per_ms);
                continue;
            },
            else => return os.windows.unexpectedStatus(rc),
        }
    }
}
