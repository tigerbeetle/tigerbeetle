//! Epoll-only IO backend for TigerBeetle client libraries.
//!
//! Clients only perform network I/O (TCP to replicas), so this backend uses:
//!   - epoll for all async operations (connect, send, recv, accept, eventfd reads)
//!   - Synchronous syscalls for close
//!   - timerfd for user-submitted timeout() operations
//!
//! Disk operations (read for files, write, fsync, openat) are unreachable; clients
//! never open or write data files. For server disk I/O use the io_uring backend.

const std = @import("std");
const assert = std.debug.assert;
const posix = std.posix;
const linux = std.os.linux;
const log = std.log.scoped(.io);

const stdx = @import("stdx");
const common = @import("./common.zig");
const QueueType = @import("../queue.zig").QueueType;
const DoublyLinkedListType = @import("../list.zig").DoublyLinkedListType;

pub const IO = struct {
    pub const TCPOptions = common.TCPOptions;
    pub const ListenOptions = common.ListenOptions;
    pub const Stats = common.Stats;
    const TimeoutList = DoublyLinkedListType(Completion, .timeout_back, .timeout_next);

    const timerfd_epoll_tag: usize = 1;
    comptime {
        // alignments of Completion leaves the last bits available for pointer-tagging
        assert(@alignOf(Completion) > timerfd_epoll_tag);
    }

    epoll_fd: posix.fd_t,
    timer_fd: posix.fd_t,
    timer_armed: bool = false,
    timer_expiry_ns: i128 = 0,
    timeouts: TimeoutList = .{},

    /// Completions that are ready to have their callbacks run.
    completed: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_completed" }),

    stats: common.Stats = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;

        log.debug("Starting epoll I/O", .{});

        const epoll_fd = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
        errdefer posix.close(epoll_fd);

        const timer_fd = try posix.timerfd_create(
            linux.timerfd_clockid_t.MONOTONIC,
            .{ .CLOEXEC = true, .NONBLOCK = true },
        );
        errdefer posix.close(timer_fd);

        var timer_event = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = timerfd_epoll_tag },
        };

        switch (posix.errno(linux.epoll_ctl(
            epoll_fd,
            linux.EPOLL.CTL_ADD,
            timer_fd,
            &timer_event,
        ))) {
            .SUCCESS => {},
            else => |err| return stdx.unexpected_errno("epoll_ctl", err),
        }

        return IO{
            .epoll_fd = epoll_fd,
            .timer_fd = timer_fd,
        };
    }

    pub fn deinit(self: *IO) void {
        posix.close(self.timer_fd);
        posix.close(self.epoll_fd);
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn run(self: *IO) !void {
        try self.poll_epoll(0);
        self.fire_expired_timeouts();

        var timer = try std.time.Timer.start();
        self.run_callbacks();
        self.stats.window.time_callbacks.ns += timer.read();
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        defer self.stats.trace();

        var total_timer = try std.time.Timer.start();
        defer self.stats.window.time_run_for_ns.ns += total_timer.read();

        const deadline: i128 = monotonic_now_ns() + @as(i128, nanoseconds);

        while (true) {
            self.fire_expired_timeouts();

            var cb_timer = try std.time.Timer.start();
            self.run_callbacks();
            self.stats.window.time_callbacks.ns += cb_timer.read();

            const now: i128 = monotonic_now_ns();
            if (now >= deadline) break;

            const remaining_ns = deadline - now;
            const wait_ms: i32 = @intCast(
                @min(@divFloor(remaining_ns, std.time.ns_per_ms), std.math.maxInt(i32)),
            );

            try self.poll_epoll(wait_ms);
        }
    }

    /// Run all queued completion callbacks.
    fn run_callbacks(self: *IO) void {
        while (self.completed.pop()) |completion| {
            completion.in_awaiting = false;
            completion.complete();
        }
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
    } || posix.UnexpectedError;

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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, AcceptError!socket_t, callback),
            .operation = .{
                .accept = .{
                    .socket = socket,
                    .address = undefined,
                    .address_size = @sizeOf(posix.sockaddr),
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
    } || posix.UnexpectedError;

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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, CloseError!void, callback),
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
        ConnectionResetByPeer,
        AlreadyConnected,
        NetworkUnreachable,
        HostUnreachable,
        FileNotFound,
        FileDescriptorNotASocket,
        PermissionDenied,
        ProtocolNotSupported,
        ConnectionTimedOut,
        SystemResources,
        Canceled,
    } || posix.UnexpectedError;

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
        address: stdx.SocketAddress,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, ConnectError!void, callback),
            .operation = .{
                .connect = .{
                    .socket = socket,
                    .address = address.to_std(),
                },
            },
        };
        self.enqueue(completion);
    }

    pub const FsyncError = error{
        FileDescriptorInvalid,
        InputOutput,
    } || posix.UnexpectedError;

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
        _ = self;
        _ = context;
        _ = callback;
        _ = completion;
        _ = fd;
        unreachable; // clients do not perform disk I/O
    }

    pub const OpenatError = posix.OpenError || posix.UnexpectedError;

    pub fn openat(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: OpenatError!fd_t,
        ) void,
        completion: *Completion,
        dir_fd: fd_t,
        file_path: [*:0]const u8,
        flags: posix.O,
        mode: posix.mode_t,
    ) void {
        _ = self;
        _ = context;
        _ = callback;
        _ = completion;
        _ = dir_fd;
        _ = file_path;
        _ = flags;
        _ = mode;
        unreachable; // clients do not perform disk I/O
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
        _ = self;
        _ = context;
        _ = callback;
        _ = completion;
        _ = fd;
        _ = buffer;
        _ = offset;
        unreachable; // clients do not perform disk I/O
    }

    pub const RecvError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionRefused,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        ConnectionResetByPeer,
        ConnectionTimedOut,
        OperationNotSupported,
        Canceled,
    } || posix.UnexpectedError;

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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, RecvError!usize, callback),
            .operation = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
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
        ConnectionTimedOut,
        ConnectionRefused,
        Canceled,
    } || posix.UnexpectedError;

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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, SendError!usize, callback),
            .operation = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    /// Best effort to synchronously transfer bytes to the kernel.
    pub fn send_now(self: *IO, socket: socket_t, buffer: []const u8) ?usize {
        _ = self;
        return posix.sendto(
            socket,
            buffer,
            posix.MSG.DONTWAIT | posix.MSG.NOSIGNAL,
            null,
            0,
        ) catch return null;
    }

    pub const StatxError = error{
        SymLinkLoop,
        FileNotFound,
        NameTooLong,
        NotDir,
    } || std.fs.File.StatError || posix.UnexpectedError;

    pub fn statx(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: StatxError!void,
        ) void,
        completion: *Completion,
        dir_fd: fd_t,
        file_path: [*:0]const u8,
        flags: u32,
        mask: u32,
        statxbuf: *std.os.linux.Statx,
    ) void {
        _ = self;
        _ = context;
        _ = callback;
        _ = completion;
        _ = dir_fd;
        _ = file_path;
        _ = flags;
        _ = mask;
        _ = statxbuf;
        unreachable; // clients do not perform disk I/O
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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, TimeoutError!void, callback),
            .operation = .{
                .timeout = .{ .nanoseconds = nanoseconds },
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
    } || posix.UnexpectedError;

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
        _ = self;
        _ = context;
        _ = callback;
        _ = completion;
        _ = fd;
        _ = buffer;
        _ = offset;
        unreachable; // clients do not perform disk I/O
    }

    pub const Event = posix.fd_t;
    pub const INVALID_EVENT: Event = -1;

    pub fn open_event(self: *IO) !Event {
        _ = self;

        const event_fd = posix.eventfd(
            0,
            linux.EFD.CLOEXEC | linux.EFD.NONBLOCK,
        ) catch |err| switch (err) {
            error.SystemResources,
            error.SystemFdQuotaExceeded,
            error.ProcessFdQuotaExceeded,
            => return error.SystemResources,
            error.Unexpected => return error.Unexpected,
        };
        assert(event_fd != INVALID_EVENT);
        return event_fd;
    }

    pub fn event_listen(
        self: *IO,
        event: Event,
        completion: *Completion,
        comptime on_event: fn (*Completion) void,
    ) void {
        assert(event != INVALID_EVENT);
        const Context = struct {
            fn on_read(
                _: *Completion,
                completion_inner: *Completion,
                result: ReadError!usize,
            ) void {
                const bytes = result catch unreachable; // eventfd reads should not fail.
                assert(bytes == @sizeOf(u64));
                on_event(completion_inner);
            }
        };

        completion.* = .{
            .io = self,
            .context = completion,
            .callback = erase_types(*Completion, ReadError!usize, Context.on_read),
            .operation = .{
                .eventfd_read = .{ .fd = event },
            },
        };
        self.enqueue(completion);
    }

    pub fn event_trigger(self: *IO, event: Event, completion: *Completion) void {
        assert(event != INVALID_EVENT);
        _ = self;
        _ = completion;

        const value: u64 = 1;
        while (true) {
            const return_code = linux.write(event, @ptrCast(&value), @sizeOf(u64));
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    assert(return_code == @sizeOf(u64));
                    return;
                },
                .INTR => continue,
                .AGAIN => return,
                else => |err| {
                    log.err("eventfd write failed: {}", .{err});
                    return;
                },
            }
        }
    }

    pub fn close_event(self: *IO, event: Event) void {
        assert(event != INVALID_EVENT);
        _ = self;

        posix.close(event);
    }

    pub const socket_t = posix.socket_t;

    /// Creates a TCP socket that can be used for async operations with the IO instance.
    /// The socket is set to non-blocking mode for use with epoll.
    pub fn open_socket_tcp(
        self: *IO,
        family: stdx.IPAddress.Family,
        options: TCPOptions,
    ) !socket_t {
        const fd = try posix.socket(
            family.to_std(),
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK,
            posix.IPPROTO.TCP,
        );
        errdefer self.close_socket(fd);

        try common.tcp_options(fd, options);
        return fd;
    }

    /// Creates a UDP socket that can be used for async operations with the IO instance.
    pub fn open_socket_udp(self: *IO, family: stdx.IPAddress.Family) !socket_t {
        _ = self;
        return try posix.socket(
            family.to_std(),
            posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK,
            posix.IPPROTO.UDP,
        );
    }

    /// Closes a socket opened by the IO instance.
    pub fn close_socket(self: *IO, socket: socket_t) void {
        _ = self;
        posix.close(socket);
    }

    /// Listen on the given TCP socket.
    pub fn listen(
        _: *IO,
        fd: socket_t,
        address: stdx.SocketAddress,
        options: ListenOptions,
    ) !stdx.SocketAddress {
        return common.listen(fd, address, options);
    }

    pub fn shutdown(_: *IO, socket: socket_t, how: posix.ShutdownHow) posix.ShutdownError!void {
        return posix.shutdown(socket, how);
    }

    pub const fd_t = posix.fd_t;
    pub const INVALID_FILE: fd_t = -1;

    fn enqueue(self: *IO, completion: *Completion) void {
        switch (completion.operation) {
            .accept => self.enqueue_accept(completion),
            .close => self.enqueue_close(completion),
            .connect => self.enqueue_connect(completion),
            .fsync => unreachable,
            .openat => unreachable,
            .read => unreachable,
            .eventfd_read => self.enqueue_eventfd_read(completion),
            .recv => self.enqueue_recv(completion),
            .send => self.enqueue_send(completion),
            .statx => unreachable,
            .timeout => self.enqueue_timeout(completion),
            .write => unreachable,
        }
    }

    fn enqueue_accept(self: *IO, completion: *Completion) void {
        const operation = &completion.operation.accept;
        while (true) {
            const return_code = linux.accept4(
                operation.socket,
                @ptrCast(&operation.address),
                &operation.address_size,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, operation.socket, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn enqueue_close(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(!completion.in_awaiting);

        const operation = completion.operation.close;
        const return_code = linux.close(operation.fd);
        // EINTR on close on Linux means the fd was closed; treat as success.
        switch (posix.errno(return_code)) {
            .SUCCESS, .INTR => completion.result = 0,
            else => |err| completion.result = -@as(i32, @intFromEnum(err)),
        }
        self.completed.push(completion);
    }

    fn enqueue_connect(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(!completion.in_awaiting);

        const operation = completion.operation.connect;
        const return_code = linux.connect(
            operation.socket,
            @ptrCast(&operation.address.any),
            operation.address.getOsSockLen(),
        );
        switch (posix.errno(return_code)) {
            .SUCCESS => {
                completion.result = 0;
                self.completed.push(completion);
            },
            // Connection in progress: wait for EPOLLOUT.
            .INPROGRESS => {
                self.epoll_enqueue(completion, operation.socket, linux.EPOLL.OUT);
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
            },
        }
    }

    fn enqueue_eventfd_read(self: *IO, completion: *Completion) void {
        const operation = completion.operation.eventfd_read;
        while (true) {
            const return_code = linux.read(
                operation.fd,
                @ptrCast(&completion.eventfd_read_buffer),
                @sizeOf(u64),
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, operation.fd, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn enqueue_recv(self: *IO, completion: *Completion) void {
        const operation = completion.operation.recv;
        while (true) {
            const return_code = linux.recvfrom(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                0,
                null,
                null,
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, operation.socket, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn enqueue_send(self: *IO, completion: *Completion) void {
        const operation = completion.operation.send;
        while (true) {
            const return_code = linux.sendto(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                linux.MSG.NOSIGNAL,
                null,
                0,
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, operation.socket, linux.EPOLL.OUT);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn enqueue_timeout(self: *IO, completion: *Completion) void {
        const nanoseconds = completion.operation.timeout.nanoseconds;

        if (nanoseconds == 0) {
            completion.result = -@as(i32, @intFromEnum(posix.E.TIME));
            self.completed.push(completion);
            return;
        }

        const now = monotonic_now_ns();
        completion.expiry_ns = now + @as(i128, nanoseconds);
        completion.in_awaiting = true;
        completion.in_timeouts = true;

        self.timeout_insert_sorted(completion);

        if (self.timeouts.tail == completion) {
            self.arm_timerfd(completion.expiry_ns, now);
        }
    }

    /// Register a completion with epoll and mark it in-flight.
    fn epoll_enqueue(self: *IO, completion: *Completion, fd: posix.fd_t, events: u32) void {
        assert(!completion.in_awaiting);
        assert(!completion.epoll_registered);

        var ev = linux.epoll_event{
            .events = events | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = @intFromPtr(completion) },
        };
        switch (posix.errno(linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, fd, &ev))) {
            .SUCCESS => {},
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
                return;
            },
        }
        completion.epoll_registered = true;
        completion.in_awaiting = true;
    }

    /// Re-register a completion with epoll (used after spurious EAGAIN on epoll wakeup).
    /// The completion remains in-flight; only the epoll registration is refreshed.
    fn epoll_reregister(self: *IO, completion: *Completion, fd: posix.fd_t, events: u32) void {
        assert(completion.in_awaiting);
        assert(!completion.epoll_registered);

        var ev = linux.epoll_event{
            .events = events | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = @intFromPtr(completion) },
        };
        switch (posix.errno(linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, fd, &ev))) {
            .SUCCESS => {
                completion.epoll_registered = true;
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
            },
        }
    }

    /// Wait up to `timeout_ms` milliseconds for epoll events and process them.
    fn poll_epoll(self: *IO, timeout_ms: i32) !void {
        var events: [256]linux.epoll_event = undefined;
        while (true) {
            const return_code = linux.epoll_wait(self.epoll_fd, &events, events.len, timeout_ms);
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    for (events[0..return_code]) |event| {
                        self.process_epoll_event(event);
                    }
                    return;
                },
                // Signal interrupted the wait; return and let the caller retry.
                .INTR => return,
                else => |err| return stdx.unexpected_errno("epoll_wait", err),
            }
        }
    }

    fn process_epoll_event(self: *IO, event: linux.epoll_event) void {
        if (event.data.ptr == timerfd_epoll_tag) {
            self.drain_timerfd();
            self.fire_expired_timeouts();
            return;
        }

        const completion: *Completion = @ptrFromInt(event.data.ptr);

        // Remove from epoll. The fd is embedded in the operation.
        const fd = operation_fd(completion);
        _ = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null);
        assert(completion.epoll_registered);
        completion.epoll_registered = false;

        switch (completion.operation) {
            .accept => self.process_accept(completion),
            .recv => self.process_recv(completion),
            .send => self.process_send(completion),
            .connect => self.process_connect(completion),
            .eventfd_read => self.process_eventfd_read(completion),
            else => unreachable,
        }
    }

    fn process_accept(self: *IO, completion: *Completion) void {
        const operation = &completion.operation.accept;
        while (true) {
            const return_code = linux.accept4(
                operation.socket,
                @ptrCast(&operation.address),
                &operation.address_size,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                // Spurious wakeup: re-register with epoll.
                .AGAIN => {
                    self.epoll_reregister(completion, operation.socket, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn process_eventfd_read(self: *IO, completion: *Completion) void {
        const operation = completion.operation.eventfd_read;
        while (true) {
            const return_code = linux.read(
                operation.fd,
                @ptrCast(&completion.eventfd_read_buffer),
                @sizeOf(u64),
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_reregister(completion, operation.fd, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn process_recv(self: *IO, completion: *Completion) void {
        const operation = completion.operation.recv;
        while (true) {
            const return_code = linux.recvfrom(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                0,
                null,
                null,
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_reregister(completion, operation.socket, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn process_send(self: *IO, completion: *Completion) void {
        const operation = completion.operation.send;
        while (true) {
            const return_code = linux.sendto(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                linux.MSG.NOSIGNAL,
                null,
                0,
            );
            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_reregister(completion, operation.socket, linux.EPOLL.OUT);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        }
    }

    fn process_connect(self: *IO, completion: *Completion) void {
        assert(!completion.epoll_registered);

        const operation = completion.operation.connect;

        var so_error: c_int = 0;
        var so_error_len: posix.socklen_t = @sizeOf(c_int);

        // Check the connection result via getsockopt(SO_ERROR).
        const return_code = std.os.linux.getsockopt(
            operation.socket,
            posix.SOL.SOCKET,
            posix.SO.ERROR,
            @ptrCast(&so_error),
            &so_error_len,
        );
        assert(so_error_len == @sizeOf(c_int));

        switch (posix.errno(return_code)) {
            .SUCCESS => {},
            .INTR => {
                self.epoll_reregister(completion, operation.socket, linux.EPOLL.OUT);
                return;
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
                return;
            },
        }

        completion.result = -@as(i32, @intCast(so_error));
        self.completed.push(completion);
    }

    fn monotonic_now_ns() i128 {
        var ts: linux.timespec = undefined;
        const return_code = linux.clock_gettime(.MONOTONIC, &ts);
        assert(posix.errno(return_code) == .SUCCESS);

        return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
    }

    /// Fire all user timeout() completions whose expiry_ns has passed.
    fn fire_expired_timeouts(self: *IO) void {
        const now = monotonic_now_ns();

        while (self.timeouts.tail) |completion| {
            if (completion.expiry_ns > now) break;

            self.timeouts.remove(completion);
            completion.in_timeouts = false;

            assert(completion.in_awaiting);
            completion.in_awaiting = false;

            completion.result = -@as(i32, @intFromEnum(posix.E.TIME));
            self.completed.push(completion);
        }

        if (self.timeouts.tail) |next| {
            self.arm_timerfd(next.expiry_ns, now);
        } else {
            self.disarm_timerfd();
        }
    }

    fn drain_timerfd(self: *IO) void {
        assert(self.timer_fd >= 0);
        assert(self.epoll_fd >= 0);

        while (true) {
            var expirations: u64 = undefined;
            const return_code = linux.read(
                self.timer_fd,
                @ptrCast(&expirations),
                @sizeOf(u64),
            );

            switch (posix.errno(return_code)) {
                .SUCCESS => {
                    assert(return_code == @sizeOf(u64));
                    self.timer_armed = false;
                    self.timer_expiry_ns = 0;
                    continue;
                },
                .INTR => continue,
                .AGAIN => return,
                else => |err| {
                    log.err("timerfd read failed: {}", .{err});
                    return;
                },
            }
        }
    }

    fn arm_timerfd(self: *IO, expiry_ns: i128, now: i128) void {
        assert(expiry_ns > 0);
        if (self.timer_armed and self.timer_expiry_ns == expiry_ns) return;

        const delta_ns: u64 = @intCast(@max(1, expiry_ns - now));
        assert(delta_ns >= 1);

        var spec = linux.itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(delta_ns / std.time.ns_per_s),
                .nsec = @intCast(delta_ns % std.time.ns_per_s),
            },
        };

        switch (posix.errno(linux.timerfd_settime(self.timer_fd, .{}, &spec, null))) {
            .SUCCESS => {
                self.timer_armed = true;
                self.timer_expiry_ns = expiry_ns;
            },
            else => |err| log.err("timerfd_settime failed: {}", .{err}),
        }
    }

    fn disarm_timerfd(self: *IO) void {
        assert(self.timer_fd != INVALID_FILE);
        assert(self.timeouts.tail == null);
        if (!self.timer_armed) return;

        var spec = linux.itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{ .sec = 0, .nsec = 0 },
        };

        switch (posix.errno(linux.timerfd_settime(self.timer_fd, .{}, &spec, null))) {
            .SUCCESS => {
                self.timer_armed = false;
                self.timer_expiry_ns = 0;
            },
            else => |err| log.err("timerfd disarm failed: {}", .{err}),
        }
    }

    fn timeout_insert_sorted(self: *IO, completion: *Completion) void {
        assert(completion.operation == .timeout);
        assert(completion.in_timeouts);
        assert(completion.timeout_back == null);
        assert(completion.timeout_next == null);

        // Invariant:
        // - self.timeouts.tail is the earliest timeout.
        // - timeout_back walks toward later timeouts.
        // - timeout_next walks toward earlier timeouts.

        var current = self.timeouts.tail;

        while (current) |node| {
            // Insert completion on the earlier side of node.
            if (completion.expiry_ns <= node.expiry_ns) {
                completion.timeout_back = node;
                completion.timeout_next = node.timeout_next;

                if (node.timeout_next) |next| {
                    next.timeout_back = completion;
                }

                node.timeout_next = completion;

                if (self.timeouts.tail == node) {
                    self.timeouts.tail = completion;
                }

                self.timeouts.count += 1;
                return;
            }

            current = node.timeout_back;
        }

        // completion is later than all existing timeouts.
        // Insert it at the far back of the list.
        if (self.timeouts.tail) |tail| {
            var latest = tail;
            while (latest.timeout_back) |back| latest = back;

            latest.timeout_back = completion;
            completion.timeout_next = latest;
            self.timeouts.count += 1;
        } else {
            self.timeouts.push(completion);
        }
    }

    /// Returns the fd for operations registered with epoll.
    fn operation_fd(completion: *Completion) posix.fd_t {
        return switch (completion.operation) {
            .accept => |operation| operation.socket,
            .recv => |operation| operation.socket,
            .send => |operation| operation.socket,
            .connect => |operation| operation.socket,
            .eventfd_read => |operation| operation.fd,
            else => unreachable,
        };
    }

    fn erase_types(
        comptime Context: type,
        comptime Result: type,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: Result,
        ) void,
    ) *const fn (?*anyopaque, *Completion, *const anyopaque) void {
        return &struct {
            fn erased(
                ctx_any: ?*anyopaque,
                completion: *Completion,
                result_any: *const anyopaque,
            ) void {
                const ctx: Context = @ptrCast(@alignCast(ctx_any));
                const result: *const Result = @ptrCast(@alignCast(result_any));
                callback(ctx, completion, result.*);
            }
        }.erased;
    }

    pub const Completion = struct {
        io: *IO,
        result: i32 = undefined,
        link: QueueType(Completion).Link = .{},
        operation: Operation,
        context: ?*anyopaque,
        callback: *const fn (
            context: ?*anyopaque,
            completion: *Completion,
            result: *const anyopaque,
        ) void,

        /// True when this completion is in-flight (registered in epoll or awaiting a timeout).
        in_awaiting: bool = false,

        /// True when this completion is registered in epoll.
        epoll_registered: bool = false,

        /// Absolute expiry time for timeout() operations (nanoseconds, CLOCK_MONOTONIC).
        expiry_ns: i128 = 0,
        timeout_back: ?*Completion = null,
        timeout_next: ?*Completion = null,
        in_timeouts: bool = false,

        eventfd_read_buffer: u64 = undefined,

        fn complete(completion: *Completion) void {
            switch (completion.operation) {
                .accept => {
                    const result: AcceptError!socket_t = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNABORTED => error.ConnectionAborted,
                                .FAULT => unreachable,
                                .INVAL => error.SocketNotListening,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PERM => error.PermissionDenied,
                                .PROTO => error.ProtocolFailure,
                                else => |errno| stdx.unexpected_errno("accept", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .close => {
                    const result: CloseError!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {},
                                .BADF => error.FileDescriptorInvalid,
                                .DQUOT => error.DiskQuota,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                else => |errno| stdx.unexpected_errno("close", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .connect => {
                    const result: ConnectError!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .ADDRINUSE => error.AddressInUse,
                                .ADDRNOTAVAIL => error.AddressNotAvailable,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .AGAIN, .INPROGRESS => error.WouldBlock,
                                .ALREADY => error.OpenAlreadyInProgress,
                                .BADF => error.FileDescriptorInvalid,
                                .CANCELED => error.Canceled,
                                .CONNREFUSED => error.ConnectionRefused,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .ISCONN => error.AlreadyConnected,
                                .NETUNREACH => error.NetworkUnreachable,
                                .HOSTUNREACH => error.HostUnreachable,
                                .NOENT => error.FileNotFound,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .PERM => error.PermissionDenied,
                                .PROTOTYPE => error.ProtocolNotSupported,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| stdx.unexpected_errno("connect", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .fsync, .openat, .read, .statx, .write => unreachable,
                .eventfd_read => {
                    const result: ReadError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR, .AGAIN => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .BADF => error.NotOpenForReading,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .ISDIR => error.IsDir,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .SPIPE => error.Unseekable,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| stdx.unexpected_errno("read", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .recv => {
                    const result: RecvError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CANCELED => error.Canceled,
                                .CONNREFUSED => error.ConnectionRefused,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                .OPNOTSUPP => error.OperationNotSupported,
                                else => |errno| stdx.unexpected_errno("recv", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .send => {
                    const result: SendError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .AGAIN => error.WouldBlock,
                                .ALREADY => error.FastOpenAlreadyInProgress,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .DESTADDRREQ => unreachable,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .ISCONN => unreachable,
                                .MSGSIZE => error.MessageTooBig,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PIPE => error.BrokenPipe,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                .CANCELED => error.Canceled,
                                else => |errno| stdx.unexpected_errno("send", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .timeout => {
                    assert(completion.result < 0);
                    const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                        .INTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        .CANCELED => error.Canceled,
                        .TIME => {}, // Success.
                        else => |errno| stdx.unexpected_errno("timeout", errno),
                    };
                    const result: TimeoutError!void = err;
                    completion.callback(completion.context, completion, &result);
                },
            }
        }
    };

    const Operation = union(enum) {
        accept: struct {
            socket: socket_t,
            address: posix.sockaddr = undefined,
            address_size: posix.socklen_t = @sizeOf(posix.sockaddr),
        },
        close: struct {
            fd: fd_t,
        },
        connect: struct {
            socket: socket_t,
            address: std.net.Address,
        },
        fsync: struct {
            fd: fd_t,
        },
        openat: struct {
            dir_fd: fd_t,
            file_path: [*:0]const u8,
            flags: posix.O,
            mode: posix.mode_t,
        },
        read: struct {
            fd: fd_t,
            buffer: []u8,
            offset: u64,
        },
        eventfd_read: struct {
            fd: fd_t,
        },
        recv: struct {
            socket: socket_t,
            buffer: []u8,
        },
        send: struct {
            socket: socket_t,
            buffer: []const u8,
        },
        statx: struct {
            dir_fd: fd_t,
            file_path: [*:0]const u8,
            flags: u32,
            mask: u32,
            statxbuf: *std.os.linux.Statx,
        },
        timeout: struct {
            nanoseconds: u63,
        },
        write: struct {
            fd: fd_t,
            buffer: []const u8,
            offset: u64,
        },
    };
};
