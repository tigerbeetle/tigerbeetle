//! Epoll-only IO backend for TigerBeetle client libraries.
//!
//! Clients only perform network I/O (TCP to replicas), so this backend uses:
//! - epoll for all async operations (connect, send, recv, accept, eventfd reads)
//! - Synchronous syscalls for close
//! - timerfd for timeout() operations
//!
//! Disk operations (read for files, write, fsync, openat) are not implemented; clients never open
//! or write data files.

const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const posix = stdx.posix;
const linux = std.os.linux;
const log = std.log.scoped(.io);

const stdx = @import("stdx");
const common = @import("./common.zig");
const QueueType = @import("../queue.zig").QueueType;

pub const IO = struct {
    epoll_fd: posix.fd_t,
    timer_fd: posix.fd_t,
    timer_expiry: ?stdx.Instant = null,
    /// Ordered by expiry, from soonest-to-expire (out) to latest-to-expire (in).
    timeouts: QueueType(Completion) = .init(.{ .name = "timeouts" }),

    /// Completions that are ready to have their callbacks run.
    completed: QueueType(Completion) = .init(.{ .name = "io_completed" }),

    stats: common.Stats = .{},
    time: stdx.TimeOS = .{},

    pub const TCPOptions = common.TCPOptions;
    pub const ListenOptions = common.ListenOptions;
    pub const Stats = common.Stats;

    const timerfd_epoll_tag: usize = 1;
    comptime {
        // alignments of Completion leaves the last bits available for pointer-tagging
        assert(@alignOf(Completion) > timerfd_epoll_tag);
    }

    pub fn init(io_std: std.Io, entries: u12, flags: u32) !IO {
        _ = io_std;
        _ = entries;
        _ = flags;

        log.debug("Starting epoll I/O", .{});

        const epoll_create_result = linux.epoll_create1(linux.EPOLL.CLOEXEC);
        const epoll_fd: fd_t = switch (linux.errno(epoll_create_result)) {
            .SUCCESS => @intCast(epoll_create_result),
            .MFILE => return error.ProcessFdQuotaExceeded,
            else => |err| return stdx.unexpected_errno("epoll_create1", err),
        };
        errdefer posix.close(epoll_fd);

        const timer_create_result = linux.timerfd_create(
            linux.timerfd_clockid_t.MONOTONIC,
            .{ .CLOEXEC = true, .NONBLOCK = true },
        );
        const timer_fd: fd_t = switch (linux.errno(timer_create_result)) {
            .SUCCESS => @intCast(timer_create_result),
            .MFILE => return error.ProcessFdQuotaExceeded,
            else => |err| return stdx.unexpected_errno("timerfd_create", err),
        };
        errdefer posix.close(timer_fd);

        var timer_event = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = timerfd_epoll_tag },
        };

        switch (linux.errno(linux.epoll_ctl(
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
        try self.poll_epoll(.{ .ns = 0 });

        const timer = self.time.monotonic();
        defer self.stats.window.time_callbacks.ns +=
            timer.until(self.time.monotonic()).ns;

        self.run_callbacks();
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        defer self.stats.trace();

        const timer_run_for_ns = self.time.monotonic();
        defer self.stats.window.time_run_for_ns.ns +=
            timer_run_for_ns.until(self.time.monotonic()).ns;

        var now = self.time.monotonic();
        const deadline = now.add(.{ .ns = nanoseconds });
        while (now.ns < deadline.ns) : (now = self.time.monotonic()) {
            try self.poll_epoll(now.until(deadline));

            const timer_callback = self.time.monotonic();
            defer self.stats.window.time_callbacks.ns +=
                timer_callback.until(self.time.monotonic()).ns;

            self.run_callbacks();
        }
    }

    /// Run all queued completion callbacks.
    fn run_callbacks(self: *IO) void {
        while (self.completed.pop()) |completion| {
            assert(completion.io == self);
            assert(!completion.awaiting);

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
                    .address = address.to_raw(),
                },
            },
        };
        self.enqueue(completion);
    }

    pub const OpenatError = posix.OpenError || posix.UnexpectedError;

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
            .operation = .{ .timeout = .{
                .duration = .{ .ns = nanoseconds },
                .expiry = self.time.monotonic().add(.{ .ns = nanoseconds }),
            } },
        };

        self.enqueue(completion);
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
        errdefer std.os.close(event_fd);

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
        for (0..10_000) |_| {
            const return_code = linux.write(event, @ptrCast(&value), @sizeOf(u64));
            switch (linux.errno(return_code)) {
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
        } else @panic("event_trigger: EINTR loop");
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
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        switch (completion.operation) {
            .accept => self.enqueue_accept(completion),
            .close => self.enqueue_close(completion),
            .connect => self.enqueue_connect(completion),
            .eventfd_read => self.enqueue_eventfd_read(completion),
            .recv => self.enqueue_recv(completion),
            .send => self.enqueue_send(completion),
            .timeout => self.enqueue_timeout(completion),
        }
    }

    fn enqueue_accept(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const operation = &completion.operation.accept;
        for (0..10_000) |_| {
            const return_code = linux.accept4(
                operation.socket,
                @ptrCast(&operation.address),
                &operation.address_size,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("enqueue_accept: EINTR loop");
    }

    fn enqueue_close(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const operation = completion.operation.close;
        const return_code = linux.close(operation.fd);
        // EINTR on close on Linux means the fd was closed; treat as success.
        completion.result = switch (linux.errno(return_code)) {
            .SUCCESS, .INTR => 0,
            else => |err| -@as(i32, @intFromEnum(err)),
        };
        self.completed.push(completion);
    }

    fn enqueue_connect(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const operation = completion.operation.connect;
        const return_code = linux.connect(
            operation.socket,
            @ptrCast(&operation.address.any),
            operation.address.getOsSockLen(),
        );
        switch (linux.errno(return_code)) {
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
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const operation = completion.operation.eventfd_read;
        for (0..10_000) |_| {
            const return_code = linux.read(
                operation.fd,
                @ptrCast(&completion.eventfd_read_buffer),
                @sizeOf(u64),
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("enqueue_eventfd_read: EINTR loop");
    }

    fn enqueue_recv(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const operation = completion.operation.recv;
        for (0..10_000) |_| {
            const return_code = linux.recvfrom(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                0,
                null,
                null,
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("enqueue_recv: EINTR loop");
    }

    fn enqueue_send(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const operation = completion.operation.send;
        for (0..10_000) |_| {
            const return_code = linux.sendto(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                linux.MSG.NOSIGNAL,
                null,
                0,
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("enqueue_send: EINTR loop");
    }

    fn enqueue_timeout(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(!completion.awaiting);

        const timeout_duration = completion.operation.timeout.duration;
        if (timeout_duration.ns == 0) {
            completion.result = -@as(i32, @intFromEnum(posix.E.TIME));
            self.completed.push(completion);
            return;
        }

        self.timeout_insert_sorted(completion);

        if (self.timeouts.peek() == completion) {
            self.timer_arm(completion.operation.timeout.expiry);
        } else {
            assert(self.timeouts.count() > 1);
        }
    }

    /// Register a completion with epoll and mark it in-flight.
    fn epoll_enqueue(self: *IO, completion: *Completion, fd: posix.fd_t, events: u32) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(completion.operation != .timeout);
        assert(!completion.awaiting);

        var event = linux.epoll_event{
            .events = events | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = @intFromPtr(completion) },
        };
        switch (linux.errno(linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, fd, &event))) {
            .SUCCESS => {
                completion.awaiting = true;
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
            },
        }
    }

    /// Re-register a completion with epoll (used after spurious EAGAIN on epoll wakeup).
    /// The completion remains in-flight; only the epoll registration is refreshed.
    fn epoll_reregister(self: *IO, completion: *Completion, fd: posix.fd_t, events: u32) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(completion.operation != .timeout);
        assert(!completion.awaiting);

        var event = linux.epoll_event{
            .events = events | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = @intFromPtr(completion) },
        };
        switch (linux.errno(linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, fd, &event))) {
            .SUCCESS => {
                completion.awaiting = true;
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
            },
        }
    }

    /// Wait up to `remaining` for epoll events and process them.
    fn poll_epoll(self: *IO, remaining: stdx.Duration) !void {
        const remaining_ms: i32 = @intCast(remaining.to_ms());
        assert(remaining_ms >= 0);
        // -1 would indicate that we wait indefinitely.
        assert(remaining_ms != -1);

        var events: [256]linux.epoll_event = undefined;
        const wait_result = linux.epoll_wait(self.epoll_fd, &events, events.len, remaining_ms);
        switch (linux.errno(wait_result)) {
            .SUCCESS => {
                const events_ready_count = wait_result;
                for (events[0..events_ready_count]) |event| {
                    self.process_epoll_event(event);
                }
            },
            // Signal interrupted the wait; ignore and let the caller retry.
            .INTR => {},
            else => |err| return stdx.unexpected_errno("epoll_wait", err),
        }
    }

    fn process_epoll_event(self: *IO, event: linux.epoll_event) void {
        if (event.data.ptr == timerfd_epoll_tag) {
            assert(self.timer_expiry != null);

            const now = self.time.monotonic();

            self.timer_drain();
            self.timer_fire(now);

            if (self.timeouts.peek()) |next| {
                assert(next.operation.timeout.expiry.ns > now.ns);
                self.timer_arm(next.operation.timeout.expiry);
            }
        } else {
            const completion: *Completion = @ptrFromInt(event.data.ptr);
            assert(completion.awaiting);
            assert(completion.operation != .timeout);
            assert(completion.operation != .close);
            assert(completion.io == self);

            // Remove from epoll. The fd is embedded in the operation.
            const fd = operation_fd(completion);
            const epoll_ctl_result = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null);
            switch (linux.errno(epoll_ctl_result)) {
                .SUCCESS => {},
                else => |err| std.debug.panic("epoll_ctl unexpected error: {}", .{err}),
            }

            completion.awaiting = false;

            switch (completion.operation) {
                .accept => self.process_accept(completion),
                .recv => self.process_recv(completion),
                .send => self.process_send(completion),
                .connect => self.process_connect(completion),
                .eventfd_read => self.process_eventfd_read(completion),
                .timeout => unreachable,
                .close => unreachable,
            }
        }
    }

    fn process_accept(self: *IO, completion: *Completion) void {
        assert(!completion.awaiting);

        const operation = &completion.operation.accept;
        for (0..10_000) |_| {
            const return_code = linux.accept4(
                operation.socket,
                @ptrCast(&operation.address),
                &operation.address_size,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            );
            switch (linux.errno(return_code)) {
                .SUCCESS => {
                    completion.result = @intCast(return_code);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    // Spurious wakeup: re-register with epoll.
                    self.epoll_reregister(completion, operation.socket, linux.EPOLL.IN);
                    return;
                },
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    self.completed.push(completion);
                    return;
                },
            }
        } else @panic("process_accept: EINTR loop");
    }

    fn process_eventfd_read(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(!completion.awaiting);

        const operation = completion.operation.eventfd_read;
        for (0..10_000) |_| {
            const return_code = linux.read(
                operation.fd,
                @ptrCast(&completion.eventfd_read_buffer),
                @sizeOf(u64),
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("process_eventfd_read: EINTR loop");
    }

    fn process_recv(self: *IO, completion: *Completion) void {
        assert(!completion.awaiting);

        const operation = completion.operation.recv;
        for (0..10_000) |_| {
            const return_code = linux.recvfrom(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                0,
                null,
                null,
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("process_recv: EINTR loop");
    }

    fn process_send(self: *IO, completion: *Completion) void {
        assert(!completion.awaiting);

        const operation = completion.operation.send;
        for (0..10_000) |_| {
            const return_code = linux.sendto(
                operation.socket,
                operation.buffer.ptr,
                operation.buffer.len,
                linux.MSG.NOSIGNAL,
                null,
                0,
            );
            switch (linux.errno(return_code)) {
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
        } else @panic("process_send: EINTR loop");
    }

    fn process_connect(self: *IO, completion: *Completion) void {
        assert(!completion.awaiting);

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

        switch (linux.errno(return_code)) {
            .SUCCESS => {
                completion.result = -@as(i32, @intCast(so_error));
                self.completed.push(completion);
            },
            .INTR => {
                self.epoll_reregister(completion, operation.socket, linux.EPOLL.OUT);
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
            },
        }
    }

    /// Fire all user timeout() completions whose expiry has passed.
    fn timer_fire(self: *IO, now: stdx.Instant) void {
        while (self.timeouts.peek()) |completion| {
            assert(completion.operation == .timeout);
            assert(completion.awaiting);

            if (completion.operation.timeout.expiry.ns > now.ns) break;
            assert(completion.operation.timeout.expiry.ns <= now.ns);

            const completion_removed = self.timeouts.pop();
            assert(completion_removed == completion);

            completion.awaiting = false;
            completion.result = -@as(i32, @intFromEnum(posix.E.TIME));
            self.completed.push(completion);
        }
    }

    fn timer_drain(self: *IO) void {
        assert(self.timer_fd >= 0);

        var expiration_count: u64 = undefined;
        const return_code = linux.read(self.timer_fd, @ptrCast(&expiration_count), @sizeOf(u64));

        switch (linux.errno(return_code)) {
            .SUCCESS => {
                assert(return_code == @sizeOf(u64));
                assert(expiration_count == 1);
                self.timer_expiry = null;
            },
            // Epoll will report this again on the next epoll_wait().
            .INTR => {},
            // Spurious wakeup; ignore.
            .AGAIN => {},
            else => |err| {
                std.debug.panic("timer_drain unexpected error: {}", .{err});
            },
        }
    }

    fn timer_arm(self: *IO, expiry: stdx.Instant) void {
        assert(self.timer_fd >= 0);

        if (self.timer_expiry) |timer_expiry| {
            assert(expiry.ns <= timer_expiry.ns);
            if (expiry.ns == timer_expiry.ns) return;
        }
        maybe(self.timer_expiry != null);

        const spec = linux.itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(expiry.ns / std.time.ns_per_s),
                .nsec = @intCast(expiry.ns % std.time.ns_per_s),
            },
        };
        // A duration of 0 would disarm the timer instead.
        assert(spec.it_value.sec != 0 or spec.it_value.nsec != 0);

        switch (linux.errno(linux.timerfd_settime(
            self.timer_fd,
            .{ .ABSTIME = true },
            &spec,
            null,
        ))) {
            .SUCCESS => self.timer_expiry = expiry,
            else => |err| log.err("timer_arm failed: {}", .{err}),
        }
    }

    fn timeout_insert_sorted(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(completion.link.next == null);
        assert(completion.operation == .timeout);
        assert(completion.operation.timeout.duration.ns > 0);
        assert(!completion.awaiting);

        completion.awaiting = true;

        var timeout_previous: ?*Completion = null;
        var timeout_iterator = self.timeouts.iterate();
        while (timeout_iterator.next()) |node| {
            assert(node.operation == .timeout);
            assert(node.awaiting);

            // Insert completion on the earlier side of `node`.
            if (completion.operation.timeout.expiry.ns <= node.operation.timeout.expiry.ns) {
                if (timeout_previous) |node_previous| {
                    assert(node_previous.operation.timeout.expiry.ns <
                        node.operation.timeout.expiry.ns);
                    self.timeouts.insert_after(node_previous, completion);
                } else {
                    // completion is earlier than all existing timeouts.
                    self.timeouts.push_first(completion);
                }
                return;
            }
            timeout_previous = node;
        }
        // completion is later than all existing timeouts.
        self.timeouts.push(completion);
    }

    /// Returns the fd for operations registered with epoll.
    fn operation_fd(completion: *Completion) posix.fd_t {
        return switch (completion.operation) {
            .accept => |operation| operation.socket,
            .close => unreachable,
            .connect => |operation| operation.socket,
            .eventfd_read => |operation| operation.fd,
            .recv => |operation| operation.socket,
            .send => |operation| operation.socket,
            .timeout => unreachable,
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

        /// operation=timeout: Whether the completion is in the `timeouts` queue.
        /// operation≠timeout: Whether the completion is registered in epoll.
        awaiting: bool = false,

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
            address: stdx.RawAddress,
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
        timeout: struct {
            duration: stdx.Duration,
            expiry: stdx.Instant,
        },
    };
};
