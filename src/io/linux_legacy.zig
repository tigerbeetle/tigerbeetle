//! Epoll + Linux kernel AIO based IO backend.
//!
//! An alternative to the io_uring backend (src/io/linux.zig) that uses:
//!   - epoll for network/eventfd readiness notification
//!   - Linux kernel AIO (io_submit/io_getevents) for disk reads, writes, and fsync
//!   - Synchronous syscalls for openat, close, statx
//!   - timerfd timeout for user-submitted timeout() operations
//!   - epoll watches aio-fd and timerfd-fd for events
//!
//! Activated by building with -Dno_uring=true.

const std = @import("std");
const assert = std.debug.assert;
const posix = std.posix;
const linux = std.os.linux;
const log = std.log.scoped(.io);

const constants = @import("../constants.zig");
const stdx = @import("stdx");
const common = @import("./common.zig");
const QueueType = @import("../queue.zig").QueueType;
const buffer_limit = @import("../io.zig").buffer_limit;
const DirectIO = @import("../io.zig").DirectIO;
const DoublyLinkedListType = @import("../list.zig").DoublyLinkedListType;
const parse_dirty_semver = stdx.parse_dirty_semver;
const maybe = stdx.maybe;
const linux_aio = stdx.linux_aio;

pub const IO = struct {
    pub const TCPOptions = common.TCPOptions;
    pub const ListenOptions = common.ListenOptions;
    pub const Stats = common.Stats;
    const CompletionList = DoublyLinkedListType(Completion, .awaiting_back, .awaiting_next);
    const TimeoutList = DoublyLinkedListType(Completion, .timeout_back, .timeout_next);

    const aio_eventfd_epoll_tag: usize = 1;
    const timerfd_epoll_tag: usize = 2;
    comptime {
        // alignments of Completion points leaves the last bits available for pointer-tagging
        assert(@alignOf(Completion) > aio_eventfd_epoll_tag);
        assert(@alignOf(Completion) > timerfd_epoll_tag);
    }

    epoll_fd: posix.fd_t,
    aio_ctx: linux_aio.Context,
    aio_event_fd: posix.fd_t,

    /// AIO ops whose iocbs are ready but not yet submitted to the kernel.
    aio_unsubmitted: QueueType(Completion) = QueueType(Completion).init(.{ .name = "aio_unsubmitted" }),

    timer_fd: posix.fd_t,
    timer_armed: bool = false,
    timeouts: TimeoutList = .{},

    /// Completions that are ready to have their callbacks run.
    completed: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_completed" }),

    /// All operations that are in-flight in epoll or the AIO context, or awaiting a timeout.
    awaiting: CompletionList = .{},

    cancel_completion: Completion = undefined,

    cancel_all_status: union(enum) {
        inactive,
        next,
        queued: struct { target: *Completion },
        wait: struct { target: *Completion },
        done,
    } = .inactive,

    stats: common.Stats = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        _ = flags;

        const epoll_fd = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
        errdefer posix.close(epoll_fd);

        const aio_event_fd = try posix.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        errdefer posix.close(aio_event_fd);

        var aio_event = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ERR | linux.EPOLL.HUP,
            .data = .{ .ptr = aio_eventfd_epoll_tag },
        };
        switch (posix.errno(linux.epoll_ctl(
            epoll_fd,
            linux.EPOLL.CTL_ADD,
            aio_event_fd,
            &aio_event,
        ))) {
            .SUCCESS => {},
            else => |err| return stdx.unexpected_errno("io:init:aio_eventfd:epoll_ctl", err),
        }

        var aio_ctx = try linux_aio.Context.init(@as(u32, entries));
        errdefer aio_ctx.deinit();

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
            else => |err| return stdx.unexpected_errno("io:init:timerfd:epoll_ctl", err),
        }

        return IO{
            .epoll_fd = epoll_fd,
            .aio_ctx = aio_ctx,
            .aio_event_fd = aio_event_fd,
            .timer_fd = timer_fd,
        };
    }

    pub fn deinit(self: *IO) void {
        self.aio_ctx.deinit();
        posix.close(self.timer_fd);
        posix.close(self.aio_event_fd);
        posix.close(self.epoll_fd);
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn run(self: *IO) !void {
        assert(self.cancel_all_status != .done);

        self.flush_aio_pending();

        try self.drain_aio();
        try self.poll_epoll(0);
        self.fire_expired_timeouts();

        var timer = try std.time.Timer.start();
        self.run_callbacks();
        self.stats.now.time_callbacks.ns += timer.read();

        // Flush any AIO ops queued by completion callbacks.
        self.flush_aio_pending();
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        assert(self.cancel_all_status != .done);
        defer self.stats.trace();

        var total_timer = try std.time.Timer.start();
        defer self.stats.now.time_run_for_ns.ns += total_timer.read();

        const deadline: i128 = monotonic_now_ns() + @as(i128, nanoseconds);

        while (true) {
            self.flush_aio_pending();

            // Non-blocking AIO drain.
            try self.drain_aio();

            // Check deadline after draining AIO so we process any completions that arrived.
            const now: i128 = monotonic_now_ns();
            if (now >= deadline) break;

            // Compute how long to wait: bounded by the deadline and by the nearest user timeout.
            const remaining_ns = deadline - now;
            const wait_ms: i32 = @intCast(
                @min(@divFloor(remaining_ns, std.time.ns_per_ms), std.math.maxInt(i32)),
            );

            try self.poll_epoll(wait_ms);

            try self.drain_aio();
            self.fire_expired_timeouts();

            var cb_timer = try std.time.Timer.start();
            self.run_callbacks();
            self.stats.now.time_callbacks.ns += cb_timer.read();
        }

        // Flush any AIO ops queued by completion callbacks.
        self.flush_aio_pending();
    }

    /// Run all queued completion callbacks, respecting cancel_all_status.
    fn run_callbacks(self: *IO) void {
        while (self.completed.pop()) |completion| {
            if (completion.in_awaiting) {
                assert(!self.awaiting.empty());
                self.awaiting.remove(completion);
                completion.in_awaiting = false;
            }

            switch (self.cancel_all_status) {
                .inactive => completion.complete(),
                .next => {},
                .queued => if (completion.operation == .cancel) completion.complete(),
                .wait => |wait| if (wait.target == completion) {
                    self.cancel_all_status = .next;
                },
                .done => unreachable,
            }
        }
    }

    pub fn cancel_all(self: *IO) void {
        assert(self.cancel_all_status == .inactive);

        defer self.cancel_all_status = .done;

        self.cancel_all_status = .next;

        // Discard any AIO ops staged but not yet submitted to the kernel.
        while (self.aio_unsubmitted.pop()) |_| {}

        while (self.awaiting.tail) |target| {
            assert(!self.awaiting.empty());
            assert(self.cancel_all_status == .next);
            assert(target.operation != .cancel);

            self.cancel_all_status = .{ .queued = .{ .target = target } };

            self.cancel(
                *IO,
                self,
                cancel_all_callback,
                .{
                    .completion = &self.cancel_completion,
                    .target = target,
                },
            );

            while (self.cancel_all_status == .queued or self.cancel_all_status == .wait) {
                self.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    std.debug.panic("IO.cancel_all: run_for_ns error: {}", .{err});
                };
            }
            assert(self.cancel_all_status == .next);
        }
        assert(self.awaiting.empty());
    }

    fn cancel_all_callback(self: *IO, completion: *Completion, result: CancelError!void) void {
        assert(self.cancel_all_status == .queued);
        assert(completion == &self.cancel_completion);
        assert(completion.operation == .cancel);
        assert(completion.operation.cancel.target == self.cancel_all_status.queued.target);

        self.cancel_all_status = status: {
            result catch |err| switch (err) {
                error.NotRunning => break :status .next,
                error.NotInterruptable => {},
                error.Unexpected => unreachable,
            };
            break :status .{ .wait = .{ .target = self.cancel_all_status.queued.target } };
        };
    }

    pub const CancelError = error{
        NotRunning,
        NotInterruptable,
    } || posix.UnexpectedError;

    pub fn cancel(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CancelError!void,
        ) void,
        options: struct {
            completion: *Completion,
            target: *Completion,
        },
    ) void {
        options.completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, CancelError!void, callback),
            .operation = .{ .cancel = .{ .target = options.target } },
        };

        self.enqueue(options.completion);
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
        address: std.net.Address,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, ConnectError!void, callback),
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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, FsyncError!void, callback),
            .operation = .{
                .fsync = .{ .fd = fd },
            },
        };
        self.enqueue(completion);
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
        var new_flags = flags;
        new_flags.CLOEXEC = true;

        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, OpenatError!fd_t, callback),
            .operation = .{
                .openat = .{
                    .dir_fd = dir_fd,
                    .file_path = file_path,
                    .flags = new_flags,
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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, ReadError!usize, callback),
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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, StatxError!void, callback),
            .operation = .{
                .statx = .{
                    .dir_fd = dir_fd,
                    .file_path = file_path,
                    .flags = flags,
                    .mask = mask,
                    .statxbuf = statxbuf,
                },
            },
        };
        self.enqueue(completion);
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
        completion.* = .{
            .io = self,
            .context = context,
            .callback = erase_types(Context, WriteError!usize, callback),
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

    pub const Event = posix.fd_t;
    pub const INVALID_EVENT: Event = -1;

    pub fn open_event(self: *IO) !Event {
        _ = self;

        const event_fd = posix.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK) catch |err| switch (err) {
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
            const Context = @This();
            var buffer: u64 = undefined;

            fn on_read(
                _: *Context,
                completion_inner: *Completion,
                result: ReadError!usize,
            ) void {
                const bytes = result catch unreachable; // eventfd reads should not fail.
                assert(bytes == @sizeOf(u64));
                on_event(completion_inner);
            }
        };

        self.read(
            *Context,
            undefined,
            Context.on_read,
            completion,
            event,
            std.mem.asBytes(&Context.buffer),
            0,
        );
    }

    pub fn event_trigger(self: *IO, event: Event, completion: *Completion) void {
        assert(event != INVALID_EVENT);
        _ = self;
        _ = completion;

        const value: u64 = 1;
        const bytes = posix.write(event, std.mem.asBytes(&value)) catch unreachable;
        assert(bytes == @sizeOf(u64));
    }

    pub fn close_event(self: *IO, event: Event) void {
        assert(event != INVALID_EVENT);
        _ = self;

        posix.close(event);
    }

    pub const socket_t = posix.socket_t;

    /// Creates a TCP socket that can be used for async operations with the IO instance.
    /// The socket is set to non-blocking mode for use with epoll.
    pub fn open_socket_tcp(self: *IO, family: u32, options: TCPOptions) !socket_t {
        const fd = try posix.socket(
            family,
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK,
            posix.IPPROTO.TCP,
        );
        errdefer self.close_socket(fd);

        try common.tcp_options(fd, options);
        return fd;
    }

    /// Creates a UDP socket that can be used for async operations with the IO instance.
    pub fn open_socket_udp(self: *IO, family: u32) !socket_t {
        _ = self;
        return try posix.socket(
            family,
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
        return posix.open(dir_path, .{ .CLOEXEC = true, .ACCMODE = .RDONLY }, 0);
    }

    pub const fd_t = posix.fd_t;
    pub const INVALID_FILE: fd_t = -1;

    pub const OpenDataFilePurpose = enum { format, open, inspect };

    pub fn open_data_file(
        self: *IO,
        dir_fd: fd_t,
        relative_path: []const u8,
        size: u64,
        purpose: OpenDataFilePurpose,
        direct_io: DirectIO,
    ) !fd_t {
        _ = self;

        assert(relative_path.len > 0);
        assert(size % constants.sector_size == 0);
        assert(!std.fs.path.isAbsolute(relative_path));

        var flags: posix.O = .{
            .CLOEXEC = true,
            .ACCMODE = if (purpose == .inspect) .RDONLY else .RDWR,
            .DSYNC = true,
        };
        var mode: posix.mode_t = 0;

        const kind: enum { file, block_device } = blk: {
            const stat = posix.fstatat(
                dir_fd,
                relative_path,
                0,
            ) catch |err| switch (err) {
                error.FileNotFound => {
                    if (purpose == .format) {
                        break :blk .file;
                    } else {
                        @panic("Path does not exist.");
                    }
                },
                else => |err_| return err_,
            };
            if (posix.S.ISBLK(stat.mode)) {
                break :blk .block_device;
            } else {
                if (!posix.S.ISREG(stat.mode)) {
                    @panic("file path does not point to block device or regular file.");
                }
                break :blk .file;
            }
        };

        if (@hasField(posix.O, "LARGEFILE")) flags.LARGEFILE = true;

        switch (kind) {
            .block_device => {
                if (direct_io != .direct_io_disabled) {
                    flags.DIRECT = true;
                    flags.EXCL = true;
                }
                log.info("opening block device \"{s}\"...", .{relative_path});
            },
            .file => {
                var direct_io_supported = false;
                const dir_on_tmpfs = try fs_is_tmpfs(dir_fd);

                if (dir_on_tmpfs) {
                    log.warn(
                        "tmpfs is not durable, and your data will be lost on reboot",
                        .{},
                    );
                }

                if (direct_io != .direct_io_disabled and !dir_on_tmpfs) {
                    direct_io_supported = try fs_supports_direct_io(dir_fd);
                    if (direct_io_supported) {
                        flags.DIRECT = true;
                    } else if (direct_io == .direct_io_optional) {
                        log.warn("This file system does not support Direct I/O.", .{});
                    } else {
                        assert(direct_io == .direct_io_required);
                        log.err("This file system does not support Direct I/O.", .{});
                        log.err("TigerBeetle uses Direct I/O to bypass the kernel page cache, " ++
                            "to ensure that data is durable when writes complete.", .{});
                        log.err("If this is a production replica, Direct I/O is required.", .{});
                        log.err("If this is a development/testing replica, " ++
                            "re-run with --development set to bypass this error.", .{});
                        @panic("file system does not support Direct I/O");
                    }
                }

                switch (purpose) {
                    .format => {
                        flags.CREAT = true;
                        flags.EXCL = true;
                        mode = 0o666;
                        log.info("creating \"{s}\"...", .{relative_path});
                    },
                    .open, .inspect => {
                        log.info("opening \"{s}\"...", .{relative_path});
                    },
                }
            },
        }

        assert(flags.DSYNC);

        const fd = try posix.openat(dir_fd, relative_path, flags, mode);
        errdefer posix.close(fd);

        {
            const stat = try posix.fstat(fd);
            switch (kind) {
                .file => assert(posix.S.ISREG(stat.mode)),
                .block_device => assert(posix.S.ISBLK(stat.mode)),
            }
        }

        const lock_acquired = blk: {
            for (0..4) |_| {
                posix.flock(fd, posix.LOCK.EX | posix.LOCK.NB) catch |err| switch (err) {
                    error.WouldBlock => {
                        std.time.sleep(50 * std.time.ns_per_ms);
                        continue;
                    },
                    else => return err,
                };
                break :blk true;
            } else {
                posix.flock(fd, posix.LOCK.EX | posix.LOCK.NB) catch |err| switch (err) {
                    error.WouldBlock => break :blk false,
                    else => return err,
                };
                break :blk true;
            }
        };

        if (purpose == .inspect) {
            assert(flags.ACCMODE == .RDONLY);
            maybe(lock_acquired);

            if (!lock_acquired) {
                log.warn(
                    "another process holds the data file lock - results may be inconsistent",
                    .{},
                );
            }
        } else if (!lock_acquired) {
            @panic("another process holds the data file lock");
        }

        assert(flags.ACCMODE == .RDONLY or lock_acquired);

        if (purpose == .format and kind == .file) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(fd, size) catch |err| switch (err) {
                error.OperationNotSupported => {
                    log.warn("file system does not support fallocate(), an ENOSPC will panic", .{});
                    log.info("allocating by writing to the last sector " ++
                        "of the file instead...", .{});

                    const sector_size = constants.sector_size;
                    const sector: [sector_size]u8 align(sector_size) = @splat(0);

                    const write_offset = size - sector.len;
                    var written: usize = 0;
                    while (written < sector.len) {
                        written += try posix.pwrite(fd, sector[written..], write_offset + written);
                    }
                },
                else => |e| return e,
            };
        }

        try posix.fsync(fd);
        try posix.fsync(dir_fd);

        switch (kind) {
            .file => {
                if ((try posix.fstat(fd)).size < size) {
                    @panic("data file inode size was truncated or corrupted");
                }
            },
            .block_device => {
                const BLKGETSIZE64 = std.os.linux.IOCTL.IOR(0x12, 114, usize);
                var block_device_size: usize = 0;

                switch (std.os.linux.E.init(std.os.linux.ioctl(
                    fd,
                    BLKGETSIZE64,
                    @intFromPtr(&block_device_size),
                ))) {
                    .SUCCESS => {},
                    .BADF => return error.InvalidFileDescriptor,
                    .NOTTY => return error.BadRequest,
                    .FAULT => return error.InvalidAddress,
                    else => |err| return stdx.unexpected_errno("open_file:ioctl", err),
                }

                if (block_device_size < size) {
                    std.debug.panic(
                        "The block device used is too small ({} available/{} needed).",
                        .{
                            std.fmt.fmtIntSizeBin(block_device_size),
                            std.fmt.fmtIntSizeBin(size),
                        },
                    );
                }

                if (purpose == .format) {
                    const superblock_zone_size =
                        @import("../vsr/superblock.zig").superblock_zone_size;
                    var read_buf: [superblock_zone_size]u8 align(constants.sector_size) = undefined;

                    assert(superblock_zone_size == try posix.read(fd, &read_buf));
                    if (!std.mem.allEqual(u8, &read_buf, 0)) {
                        std.debug.panic(
                            "Superblock on block device not empty. " ++
                                "If this is the correct block device to use, " ++
                                "please zero the first {} using a tool like dd.",
                            .{std.fmt.fmtIntSizeBin(superblock_zone_size)},
                        );
                    }
                    try posix.lseek_CUR(fd, -superblock_zone_size);
                    assert(try posix.lseek_CUR_get(fd) == 0);
                }
            },
        }

        return fd;
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

    pub fn aof_blocking_stat(_: *IO, path: []const u8) std.fs.Dir.StatFileError!std.fs.File.Stat {
        return common.aof_blocking_stat(path);
    }

    pub fn aof_blocking_fstat(_: *IO, fd: fd_t) std.fs.Dir.StatError!std.fs.File.Stat {
        return common.aof_blocking_fstat(fd);
    }

    pub fn aof_blocking_open(io: *IO, path: []const u8) !fd_t {
        stdx.maybe(std.fs.path.isAbsolute(path));

        const dir_path = std.fs.path.dirname(path) orelse ".";
        const dir_fd = try IO.open_dir(dir_path);
        defer io.aof_blocking_close(dir_fd);

        const file_path = std.fs.path.basename(path);

        return common.aof_blocking_open(dir_fd, file_path);
    }

    // -------------------------------------------------------------------------
    // Internal: Enqueue
    // -------------------------------------------------------------------------

    fn enqueue(self: *IO, completion: *Completion) void {
        switch (self.cancel_all_status) {
            .inactive => {},
            .queued => assert(completion.operation == .cancel),
            else => unreachable,
        }

        switch (completion.operation) {
            .cancel => self.enqueue_cancel(completion),
            .accept => self.enqueue_accept(completion),
            .close => self.enqueue_close(completion),
            .connect => self.enqueue_connect(completion),
            .fsync => self.enqueue_fsync(completion),
            .openat => self.enqueue_openat(completion),
            .read => self.enqueue_read(completion),
            .recv => self.enqueue_recv(completion),
            .send => self.enqueue_send(completion),
            .statx => self.enqueue_statx(completion),
            .timeout => self.enqueue_timeout(completion),
            .write => self.enqueue_write(completion),
        }
    }

    /// Cancel completes synchronously. For AIO ops, io_cancel() is called. For epoll ops,
    /// the fd is removed from epoll and the target is pushed to completed with ECANCELED.
    /// The cancel completion is pushed to completed before the target to preserve the expected
    /// ordering: cancel callback fires → .wait state → target fires → .next state.
    fn enqueue_cancel(self: *IO, completion: *Completion) void {
        const target = completion.operation.cancel.target;

        switch (target.operation) {
            .read, .write, .fsync => {
                // AIO op: attempt to cancel it.
                var result_event: linux_aio.Event = undefined;
                self.aio_ctx.cancel(&target.aio_iocb, &result_event) catch |err| {
                    completion.result = switch (err) {
                        // Not cancellable right now; target will complete on its own.
                        error.Again => -@as(i32, @intFromEnum(posix.E.ALREADY)),
                        // Not found in context; already completed or never submitted.
                        else => -@as(i32, @intFromEnum(posix.E.NOENT)),
                    };
                    self.completed.push(completion);
                    return;
                };
                // Successful cancel: target will appear in io_getevents with ECANCELED.
                completion.result = 0;
                self.completed.push(completion);
            },
            .accept, .recv, .send, .connect => {
                if (target.epoll_registered) {
                    const fd = operation_fd(target);
                    _ = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null);
                    target.epoll_registered = false;
                    target.result = -@as(i32, @intFromEnum(posix.E.CANCELED));
                    // Push cancel first, then target; FIFO order ensures cancel fires before target.
                    completion.result = 0;
                    self.completed.push(completion);
                    self.completed.push(target);
                } else {
                    // Target is not registered (already completed or completing).
                    completion.result = -@as(i32, @intFromEnum(posix.E.NOENT));
                    self.completed.push(completion);
                }
            },
            .timeout => {
                completion.result = 0;
                target.result = -@as(i32, @intFromEnum(posix.E.CANCELED));

                if (target.in_timeouts) {
                    const was_head = self.timeouts.tail == target;
                    self.timeouts.remove(target);
                    target.in_timeouts = false;

                    if (was_head) {
                        if (self.timeouts.tail) |next| {
                            self.arm_timerfd(next.expiry_ns, monotonic_now_ns());
                        } else {
                            self.disarm_timerfd();
                        }
                    }
                }

                self.completed.push(completion);
                self.completed.push(target);
            },
            .cancel, .openat, .close, .statx => {
                // These complete synchronously; they won't be in awaiting when cancel runs.
                completion.result = -@as(i32, @intFromEnum(posix.E.NOENT));
                self.completed.push(completion);
            },
        }
    }

    fn enqueue_accept(self: *IO, completion: *Completion) void {
        const op = &completion.operation.accept;
        while (true) {
            const rc = linux.accept4(
                op.socket,
                @ptrCast(&op.address),
                &op.address_size,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            );
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, op.socket, linux.EPOLL.IN);
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

        const op = completion.operation.close;
        const rc = linux.close(op.fd);
        // EINTR on close on Linux means the fd was closed; treat as success.
        switch (posix.errno(rc)) {
            .SUCCESS, .INTR => completion.result = 0,
            else => |err| completion.result = -@as(i32, @intFromEnum(err)),
        }
        self.completed.push(completion);
    }

    fn enqueue_connect(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(!completion.in_awaiting);

        const op = completion.operation.connect;
        const rc = linux.connect(
            op.socket,
            @ptrCast(&op.address.any),
            op.address.getOsSockLen(),
        );
        switch (posix.errno(rc)) {
            .SUCCESS => {
                completion.result = 0;
                self.completed.push(completion);
            },
            // Connection in progress: wait for EPOLLOUT.
            .INPROGRESS => {
                self.epoll_enqueue(completion, op.socket, linux.EPOLL.OUT);
            },
            else => |err| {
                completion.result = -@as(i32, @intFromEnum(err));
                self.completed.push(completion);
            },
        }
    }

    fn enqueue_fsync(self: *IO, completion: *Completion) void {
        const op = completion.operation.fsync;
        // Use fdatasync (matching io_uring IORING_FSYNC_DATASYNC behavior).
        completion.aio_iocb = linux_aio.Iocb.fdatasync(
            op.fd,
            .{
                .data = @intFromPtr(completion),
                .resfd = self.aio_event_fd,
            },
        );
        self.aio_unsubmitted.push(completion);
    }

    fn enqueue_openat(self: *IO, completion: *Completion) void {
        const op = completion.operation.openat;
        while (true) {
            const rc = linux.openat(op.dir_fd, op.file_path, op.flags, op.mode);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    break;
                },
                .INTR => continue,
                else => |err| {
                    completion.result = -@as(i32, @intFromEnum(err));
                    break;
                },
            }
        }
        self.completed.push(completion);
    }

    /// Routes to AIO for regular files and block devices.
    /// Do not use for other fd types.
    fn enqueue_read(self: *IO, completion: *Completion) void {
        const op = completion.operation.read;

        const limit = buffer_limit(op.buffer.len);
        completion.aio_iocb = linux_aio.Iocb.pread(
            op.fd,
            op.buffer[0..limit],
            @intCast(op.offset),
            .{
                .data = @intFromPtr(completion),
                .resfd = self.aio_event_fd,
            },
        );
        self.aio_unsubmitted.push(completion);
    }

    fn enqueue_recv(self: *IO, completion: *Completion) void {
        const op = completion.operation.recv;
        while (true) {
            const rc = linux.recvfrom(op.socket, op.buffer.ptr, op.buffer.len, 0, null, null);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, op.socket, linux.EPOLL.IN);
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
        const op = completion.operation.send;
        while (true) {
            const rc = linux.sendto(
                op.socket,
                op.buffer.ptr,
                op.buffer.len,
                linux.MSG.NOSIGNAL,
                null,
                0,
            );
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_enqueue(completion, op.socket, linux.EPOLL.OUT);
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

    fn enqueue_statx(self: *IO, completion: *Completion) void {
        assert(completion.io == self);
        assert(!completion.in_awaiting);

        const op = completion.operation.statx;
        const rc = linux.statx(op.dir_fd, op.file_path, op.flags, op.mask, op.statxbuf);
        switch (posix.errno(rc)) {
            .SUCCESS => completion.result = 0,
            else => |err| completion.result = -@as(i32, @intFromEnum(err)),
        }
        self.completed.push(completion);
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

        self.awaiting.push(completion);
        self.timeout_insert_sorted(completion);

        if (self.timeouts.tail == completion) {
            self.arm_timerfd(completion.expiry_ns, now);
        }
    }

    /// Routes a write to AIO for regular files/block devices.
    /// Do not use for other fd types.
    fn enqueue_write(self: *IO, completion: *Completion) void {
        const op = completion.operation.write;

        const limit = buffer_limit(op.buffer.len);
        completion.aio_iocb = linux_aio.Iocb.pwrite(
            op.fd,
            op.buffer[0..limit],
            @intCast(op.offset),
            .{
                .data = @intFromPtr(completion),
                .resfd = self.aio_event_fd,
            },
        );
        self.aio_unsubmitted.push(completion);
    }

    // -------------------------------------------------------------------------
    // Internal: AIO
    // -------------------------------------------------------------------------

    fn flush_aio_pending(self: *IO) void {
        // Collect up to 256 staged iocb pointers into a stack-local scratch array and
        // submit them as a single io_submit batch. Any the kernel cannot accept are
        // pushed back to aio_unsubmitted for the next tick.
        var iocbs: [256]*linux_aio.Iocb = undefined;
        var count: usize = 0;

        while (count < iocbs.len) {
            const completion = self.aio_unsubmitted.pop() orelse break;
            iocbs[count] = &completion.aio_iocb;
            count += 1;
        }
        if (count == 0) return;

        var start: usize = 0;
        while (start < count) {
            const sr = self.aio_ctx.submit(iocbs[start..count]);
            if (sr.errno != .SUCCESS) {
                if (sr.errno == .AGAIN) break;
                // Fatal submit error: fail all unsubmitted ops with the raw errno.
                const err_result = -@as(i32, @intFromEnum(sr.errno));
                for (iocbs[start..count]) |iocb| {
                    const completion: *Completion =
                        @ptrFromInt(@as(usize, @intCast(iocb.aio_data)));
                    completion.result = err_result;
                    self.completed.push(completion);
                }
                return;
            }
            if (sr.count == 0) break;

            for (iocbs[start..][0..sr.count]) |iocb| {
                const completion: *Completion =
                    @ptrFromInt(@as(usize, @intCast(iocb.aio_data)));
                completion.in_awaiting = true;
                self.awaiting.push(completion);
            }
            start += sr.count;
        }

        // Push any unsubmitted ops back for the next tick.
        for (iocbs[start..count]) |iocb| {
            const completion: *Completion =
                @ptrFromInt(@as(usize, @intCast(iocb.aio_data)));
            self.aio_unsubmitted.push(completion);
        }
    }

    /// Non-blocking drain of the AIO completion queue.
    fn drain_aio(self: *IO) !void {
        var events: [256]linux_aio.Event = undefined;
        while (true) {
            const n = self.aio_ctx.poll(&events) catch |err| switch (err) {
                error.Interrupted => continue,
                else => return err,
            };
            if (n == 0) break;

            for (events[0..n]) |event| {
                const completion: *Completion = @ptrFromInt(event.userData());
                // AIO res is i64; completion.result is i32. Byte counts and error codes fit.
                completion.result = @intCast(event.res);
                self.completed.push(completion);
            }
        }
    }

    fn drain_aio_eventfd(self: *IO) void {
        assert(self.aio_event_fd >= 0);

        while (true) {
            var value: u64 = undefined;
            const rc = linux.read(
                self.aio_event_fd,
                @ptrCast(&value),
                @sizeOf(u64),
            );

            switch (posix.errno(rc)) {
                .SUCCESS => {
                    assert(rc == @sizeOf(u64));
                    continue;
                },
                .INTR => continue,
                .AGAIN => return,
                else => |err| {
                    log.err("aio eventfd read failed: {}", .{err});
                    return;
                },
            }
        }
    }

    // -------------------------------------------------------------------------
    // Internal: Epoll
    // -------------------------------------------------------------------------

    /// Register a completion with epoll and add it to awaiting.
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
        self.awaiting.push(completion);
    }

    /// Re-register a completion with epoll (used after spurious EAGAIN on epoll wakeup).
    /// The completion remains in awaiting; only the epoll registration is refreshed.
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
            const rc = linux.epoll_wait(self.epoll_fd, &events, events.len, timeout_ms);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    const n: usize = rc;
                    for (events[0..n]) |event| {
                        try self.process_epoll_event(event);
                    }
                    return;
                },
                // Signal interrupted the wait; return and let the caller retry.
                .INTR => return,
                else => |err| {
                    log.err("epoll_wait failed: {}", .{err});
                    return;
                },
            }
        }
    }

    fn process_epoll_event(self: *IO, event: linux.epoll_event) !void {
        if (event.data.ptr == aio_eventfd_epoll_tag) {
            self.drain_aio_eventfd();
            try self.drain_aio();
            return;
        }
        if (event.data.ptr == timerfd_epoll_tag) {
            self.drain_timerfd();
            self.fire_expired_timeouts();
            return;
        }

        const completion: *Completion = @ptrFromInt(event.data.ptr);

        // Remove from epoll. The fd is embedded in the operation.
        const fd = operation_fd(completion);
        _ = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null);
        completion.epoll_registered = false;

        switch (completion.operation) {
            .accept => self.process_accept(completion),
            .recv => self.process_recv(completion),
            .send => self.process_send(completion),
            .connect => self.process_connect(completion),
            else => unreachable,
        }
    }

    fn process_accept(self: *IO, completion: *Completion) void {
        const op = &completion.operation.accept;
        while (true) {
            const rc = linux.accept4(
                op.socket,
                @ptrCast(&op.address),
                &op.address_size,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            );
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                // Spurious wakeup: re-register with epoll.
                .AGAIN => {
                    self.epoll_reregister(completion, op.socket, linux.EPOLL.IN);
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
        const op = completion.operation.recv;
        while (true) {
            const rc = linux.recvfrom(op.socket, op.buffer.ptr, op.buffer.len, 0, null, null);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_reregister(completion, op.socket, linux.EPOLL.IN);
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
        const op = completion.operation.send;
        while (true) {
            const rc = linux.sendto(
                op.socket,
                op.buffer.ptr,
                op.buffer.len,
                linux.MSG.NOSIGNAL,
                null,
                0,
            );
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    completion.result = @intCast(rc);
                    self.completed.push(completion);
                    return;
                },
                .INTR => continue,
                .AGAIN => {
                    self.epoll_reregister(completion, op.socket, linux.EPOLL.OUT);
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

        const op = completion.operation.connect;

        var so_error: c_int = 0;
        var so_error_len: posix.socklen_t = @sizeOf(c_int);

        // Check the connection result via getsockopt(SO_ERROR).
        const rc = std.os.linux.getsockopt(
            op.socket,
            posix.SOL.SOCKET,
            posix.SO.ERROR,
            @ptrCast(&so_error),
            &so_error_len,
        );
        assert(so_error_len == @sizeOf(c_int));

        if (std.os.linux.E.init(rc) != .SUCCESS) {
            completion.result = -@as(i32, @intFromEnum(posix.E.IO));
            self.completed.push(completion);
            return;
        }

        completion.result = -@as(i32, @intCast(so_error));
        self.completed.push(completion);
    }

    // -------------------------------------------------------------------------
    // Internal: Timeouts
    // -------------------------------------------------------------------------

    fn monotonic_now_ns() i128 {
        var ts: linux.timespec = undefined;
        const rc = linux.clock_gettime(.MONOTONIC, &ts);
        assert(posix.errno(rc) == .SUCCESS);

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
            self.awaiting.remove(completion);
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
            const rc = linux.read(
                self.timer_fd,
                @ptrCast(&expirations),
                @sizeOf(u64),
            );

            switch (posix.errno(rc)) {
                .SUCCESS => {
                    assert(rc == @sizeOf(u64));
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
            .SUCCESS => {},
            else => |err| log.err("timerfd_settime failed: {}", .{err}),
        }
    }

    fn disarm_timerfd(self: *IO) void {
        assert(self.timer_fd != INVALID_FILE);
        assert(self.timeouts.tail == null);

        var spec = linux.itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{ .sec = 0, .nsec = 0 },
        };

        switch (posix.errno(linux.timerfd_settime(self.timer_fd, .{}, &spec, null))) {
            .SUCCESS => {},
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

    // -------------------------------------------------------------------------
    // Internal: Helpers
    // -------------------------------------------------------------------------

    /// Returns the fd for operations that are registered with epoll.
    fn operation_fd(completion: *Completion) posix.fd_t {
        return switch (completion.operation) {
            .accept => |op| op.socket,
            .recv => |op| op.socket,
            .send => |op| op.socket,
            .connect => |op| op.socket,
            .read => |op| op.fd,
            .write => |op| op.fd,
            else => unreachable,
        };
    }

    fn fs_is_tmpfs(dir_fd: fd_t) !bool {
        var statfs: stdx.StatFs = undefined;

        while (true) {
            const res = stdx.fstatfs(dir_fd, &statfs);
            switch (std.os.linux.E.init(res)) {
                .SUCCESS => {
                    return statfs.f_type == stdx.TmpfsMagic;
                },
                .INTR => continue,
                else => |err| return stdx.unexpected_errno("fs_is_tmpfs", err),
            }
        }
    }

    fn fs_supports_direct_io(dir_fd: fd_t) !bool {
        if (!@hasField(posix.O, "DIRECT")) return false;

        var cookie: [16]u8 = @splat('0');
        _ = stdx.array_print(16, &cookie, "{0x}", .{std.crypto.random.int(u64)});

        const path: [:0]const u8 = "fs_supports_direct_io-" ++ cookie ++ "";
        const dir = std.fs.Dir{ .fd = dir_fd };
        const flags: posix.O = .{ .CLOEXEC = true, .CREAT = true, .TRUNC = true };
        const fd = try posix.openatZ(dir_fd, path, flags, 0o666);
        defer posix.close(fd);
        defer dir.deleteFile(path) catch {};

        while (true) {
            const dir_flags: posix.O = .{ .CLOEXEC = true, .ACCMODE = .RDONLY, .DIRECT = true };
            const res = std.os.linux.openat(dir_fd, path, dir_flags, 0);
            switch (std.os.linux.E.init(res)) {
                .SUCCESS => {
                    posix.close(@intCast(res));
                    return true;
                },
                .INTR => continue,
                .INVAL => return false,
                else => |err| return stdx.unexpected_errno("fs_supports_direct_io", err),
            }
        }
    }

    fn fs_allocate(fd: fd_t, size: u64) !void {
        const mode: i32 = 0;
        const offset: i64 = 0;
        const length: i64 = @intCast(size);

        while (true) {
            const rc = std.os.linux.fallocate(fd, mode, offset, length);
            switch (std.os.linux.E.init(rc)) {
                .SUCCESS => return,
                .BADF => return error.FileDescriptorInvalid,
                .FBIG => return error.FileTooBig,
                .INTR => continue,
                .INVAL => return error.ArgumentsInvalid,
                .IO => return error.InputOutput,
                .NODEV => return error.NoDevice,
                .NOSPC => return error.NoSpaceLeft,
                .NOSYS => return error.SystemOutdated,
                .OPNOTSUPP => return error.OperationNotSupported,
                .PERM => return error.PermissionDenied,
                .SPIPE => return error.Unseekable,
                .TXTBSY => return error.FileBusy,
                else => |errno| return stdx.unexpected_errno("fs_allocate", errno),
            }
        }
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

    // -------------------------------------------------------------------------
    // Completion
    // -------------------------------------------------------------------------

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

        /// Used by the IO.awaiting doubly-linked list.
        awaiting_back: ?*Completion = null,
        awaiting_next: ?*Completion = null,

        /// AIO iocb for disk read/write/fsync operations.
        aio_iocb: linux_aio.Iocb = undefined,

        /// True when this completion is in the IO.awaiting doubly-linked list.
        in_awaiting: bool = false,

        /// True when this completion is registered in epoll.
        epoll_registered: bool = false,

        /// Absolute expiry time for timeout() operations (nanoseconds, CLOCK_MONOTONIC).
        expiry_ns: i128 = 0,
        timeout_back: ?*Completion = null,
        timeout_next: ?*Completion = null,
        in_timeouts: bool = false,

        fn complete(completion: *Completion) void {
            switch (completion.operation) {
                .cancel => {
                    const result: CancelError!void = result: {
                        if (completion.result < 0) {
                            break :result switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .NOENT => error.NotRunning,
                                .ALREADY => error.NotInterruptable,
                                .INVAL => unreachable,
                                else => |errno| stdx.unexpected_errno("cancel", errno),
                            };
                        }
                        break :result {};
                    };
                    completion.callback(completion.context, completion, &result);
                },
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
                .fsync => {
                    const result: anyerror!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .BADF => error.FileDescriptorInvalid,
                                .IO => error.InputOutput,
                                .INVAL => unreachable,
                                else => |errno| stdx.unexpected_errno("fsync", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .openat => {
                    const result: OpenatError!fd_t = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .BADF => unreachable,
                                .ACCES => error.AccessDenied,
                                .FBIG => error.FileTooBig,
                                .OVERFLOW => error.FileTooBig,
                                .ISDIR => error.IsDir,
                                .LOOP => error.SymLinkLoop,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NAMETOOLONG => error.NameTooLong,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NODEV => error.NoDevice,
                                .NOENT => error.FileNotFound,
                                .NOMEM => error.SystemResources,
                                .NOSPC => error.NoSpaceLeft,
                                .NOTDIR => error.NotDir,
                                .PERM => error.AccessDenied,
                                .EXIST => error.PathAlreadyExists,
                                .BUSY => error.DeviceBusy,
                                .OPNOTSUPP => error.FileLocksNotSupported,
                                .AGAIN => error.WouldBlock,
                                .TXTBSY => error.FileBusy,
                                else => |errno| stdx.unexpected_errno("openat", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .read => {
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
                .statx => {
                    const result: StatxError!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .BADF => unreachable,
                                .ACCES => error.AccessDenied,
                                .LOOP => error.SymLinkLoop,
                                .NAMETOOLONG => error.NameTooLong,
                                .NOENT => error.FileNotFound,
                                .NOMEM => error.SystemResources,
                                .NOTDIR => error.NotDir,
                                else => |errno| stdx.unexpected_errno("statx", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
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
                .write => {
                    const result: WriteError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.NotOpenForWriting,
                                .DESTADDRREQ => error.NotConnected,
                                .DQUOT => error.DiskQuota,
                                .FAULT => unreachable,
                                .FBIG => error.FileTooBig,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .PERM => error.AccessDenied,
                                .PIPE => error.BrokenPipe,
                                .SPIPE => error.Unseekable,
                                else => |errno| stdx.unexpected_errno("write", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
            }
        }
    };

    // -------------------------------------------------------------------------
    // Operation
    // -------------------------------------------------------------------------

    const Operation = union(enum) {
        cancel: struct {
            target: *Completion,
        },
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
