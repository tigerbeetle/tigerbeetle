const std = @import("std");
const os = std.os;
const mem = std.mem;
const assert = std.debug.assert;
const log = std.log.scoped(.io);

const constants = @import("../constants.zig");
const FIFO = @import("../fifo.zig").FIFO;
const Time = @import("../time.zig").Time;
const buffer_limit = @import("../io.zig").buffer_limit;

pub const IO = struct {
    const Pool = struct {
        mutex: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        queue: FIFO(Completion) = .{ .name = null },
        notified: bool = false,
        shutdown: bool = false,
        spawned: u32 = 0,
        idle: u32 = 0,

        fn spawn(pool: *Pool, concurrency: u32) !void {
            errdefer pool.join();

            while (pool.spawned < concurrency) : (pool.spawned += 1) {
                const thread = try std.Thread.spawn(.{}, worker, .{pool});
                thread.detach();
            }
        }

        fn join(pool: *Pool) void {
            pool.mutex.lock();
            defer pool.mutex.unlock();

            assert(!pool.shutdown);
            pool.shutdown = true;

            if (pool.idle > 0) pool.cond.broadcast();
            while (pool.spawned == 0) pool.cond.wait(&pool.mutex);
        }

        fn submit(pool: *Pool, completion: *Completion) void {
            pool.mutex.lock();
            defer pool.mutex.unlock();

            pool.queue.push(completion);
            pool.notify();
        }

        fn notify(pool: *Pool) void {
            if (pool.queue.empty() or pool.notified or pool.idle == 0) return;
            pool.notified = true;
            pool.cond.signal();
        }

        fn worker(pool: *Pool) void {
            pool.mutex.lock();
            defer pool.mutex.unlock();

            while (true) {
                while (pool.queue.pop()) |completion| {
                    pool.notify();

                    pool.mutex.unlock();
                    defer pool.mutex.lock();

                    const io = @fieldParentPtr(IO, "fs_pool", pool);
                    completion.callback(io, completion);
                }

                if (pool.shutdown) {
                    pool.spawned -= 1;
                    if (pool.spawned == 0) pool.cond.signal();
                    return;
                }

                pool.idle += 1;
                defer pool.idle -= 1;

                pool.cond.wait(&pool.mutex);
                pool.notified = false;
            }
        }
    };

    const Injector = struct {
        mutex: std.Thread.Mutex = .{},
        queue: FIFO(Completion) = .{ .name = null },
        waiting: bool = false,
        pending: bool = false,

        fn push(injector: *Injector, completion: *Completion) bool {
            injector.mutex.lock();
            defer injector.mutex.unlock();

            const was_empty = injector.queue.empty();
            injector.queue.push(completion);

            @atomicStore(bool, &injector.pending, true, .Monotonic);
            return was_empty and injector.waiting;
        }

        fn poll(injector: *Injector, completed: *FIFO(Completion), wait: bool) void {
            assert(completed.empty());

            if (!@atomicLoad(bool, &injector.pending, .Monotonic)) blk: {
                if (wait and !injector.waiting) break :blk;
                return;
            }

            injector.mutex.lock();
            defer injector.mutex.unlock();

            std.mem.swap(FIFO(Completion), completed, &injector.queue);
            std.mem.swap(?[]const u8, &completed.name, &injector.queue.name);

            injector.pending = false;
            injector.waiting = completed.empty() and wait;
        }
    };

    time: Time = .{},
    timeouts: FIFO(Completion) = .{ .name = "io_timeouts" },
    completed: FIFO(Completion) = .{ .name = "io_completed" },

    io_inflight: u32 = 0,
    io_pending: [64]*Completion = undefined,

    fs_spawned: bool = false,
    fs_pool: Pool = .{},

    injector: Injector = .{},
    event_set: bool = false,
    event_fd: os.fd_t,

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;

        const event_fd = try os.eventfd(0, os.linux.EFD.CLOEXEC);
        errdefer os.close(event_fd);

        return IO{ .event_fd = event_fd };
    }

    pub fn deinit(self: *IO) void {
        if (self.fs_spawned) self.fs_pool.join();
        os.close(self.event_fd);
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        return self.flush(false);
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// in the __kernel_timespec struct.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        var timed_out = false;
        var completion: Completion = undefined;
        const on_timeout = struct {
            fn callback(
                timed_out_ptr: *bool,
                _completion: *Completion,
                result: TimeoutError!void,
            ) void {
                _ = _completion;
                _ = result catch unreachable;

                timed_out_ptr.* = true;
            }
        }.callback;

        // Submit a timeout which sets the timed_out value to true to terminate the loop below.
        self.timeout(
            *bool,
            &timed_out,
            on_timeout,
            &completion,
            nanoseconds,
        );

        // Loop until our timeout completion is processed above, which sets timed_out to true.
        // LLVM shouldn't be able to cache timed_out's value here since its address escapes above.
        while (!timed_out) {
            try self.flush(true);
        }
    }

    fn flush(self: *IO, wait_for_completions: bool) !void {
        if (!self.fs_spawned) {
            try self.fs_pool.spawn(64);
            self.fs_spawned = true;
        }

        // Timeouts are expired here and possibly pushed to the completed queue.
        const inflight = self.io_inflight;
        const next_timeout = self.flush_timeouts();
        if (self.completed.empty()) self.injector.poll(&self.completed, true);

        // Only call poll() if we need to check pending IO or if we need to wait for completions.
        if (inflight > 0 or self.completed.empty()) {
            // Zero timeouts implies a non-blocking poll
            var timeout_ms: i32 = 0;

            // We need to wait (not check) on poll if there's nothing inflight or complete.
            // We should never wait indefinitely (timeout_ms = -1 for poll) given:
            // - tick() is non-blocking (wait_for_completions = false)
            // - run_for_ns() always submits a timeout
            if (self.completed.empty()) {
                if (wait_for_completions) {
                    const timeout_ns = next_timeout orelse @panic("poll() blocking forever");
                    timeout_ms = @intCast(i32, @maximum(1, timeout_ns / std.time.ns_per_ms));
                } else if (inflight == 0) {
                    return;
                }
            }

            var pfds: [self.io_pending.len + 1]os.pollfd = undefined;
            pfds[0] = .{
                .fd = self.event_fd,
                .events = os.POLL.IN,
                .revents = 0,
            };

            for (self.io_pending[0..inflight]) |completion, i| {
                const info = switch (completion.operation) {
                    .accept => |op| [2]c_int{ op.socket, os.POLL.IN },
                    .connect => |op| [2]c_int{ op.socket, os.POLL.OUT },
                    .recv => |op| [2]c_int{ op.socket, os.POLL.IN },
                    .send => |op| [2]c_int{ op.socket, os.POLL.OUT },
                    else => @panic("invalid completion operation queued for io"),
                };

                pfds[i + 1] = .{
                    .fd = info[0],
                    .events = @intCast(i16, info[1]),
                    .revents = 0,
                };
            }

            const pfd_count = inflight + 1;
            const ready = try os.poll(pfds[0..pfd_count], timeout_ms);

            if (ready > 0) {
                if (pfds[0].revents != 0) {
                    var value: u64 = undefined;
                    const bytes = try os.read(self.event_fd, std.mem.asBytes(&value));
                    assert(bytes == @sizeOf(u64));
                    assert(value == 1);
                    assert(@atomicRmw(bool, &self.event_set, .Xchg, false, .Acquire));
                }

                self.io_inflight = 0;
                for (pfds[1..pfd_count]) |pfd, i| {
                    const completion = self.io_pending[i];
                    if (pfd.revents != 0) {
                        self.completed.push(completion);
                    } else {
                        self.io_pending[self.io_inflight] = completion;
                        self.io_inflight += 1;
                    }
                }
            }
        }

        while (!self.completed.empty()) {
            while (self.completed.pop()) |completion| (completion.callback)(self, completion);
            self.injector.poll(&self.completed, false);
        }
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_timeout: ?u64 = null;
        var timeouts: ?*Completion = self.timeouts.peek();
        while (timeouts) |completion| {
            timeouts = completion.next;

            // NOTE: We could cache `now` above the loop but monotonic() should be cheap to call.
            const now = self.time.monotonic();
            const expires = completion.operation.timeout.expires;

            // NOTE: remove() could be O(1) here with a doubly-linked-list
            // since we know the previous Completion.
            if (now >= expires) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            const timeout_ns = expires - now;
            if (min_timeout) |min_ns| {
                min_timeout = std.math.min(min_ns, timeout_ns);
            } else {
                min_timeout = timeout_ns;
            }
        }
        return min_timeout;
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion,
        context: ?*anyopaque,
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
        read: struct {
            fd: os.fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
            result: ?(ReadError!usize) = null,
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
            expires: u64,
        },
        write: struct {
            fd: os.fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
            result: ?(WriteError!usize) = null,
        },
    };

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime operation_tag: std.meta.Tag(Operation),
        operation_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const Context = @TypeOf(context);
        const onCompleteFn = struct {
            fn onComplete(io: *IO, _completion: *Completion) void {
                // Perform the actual operaton
                const op_data = &@field(_completion.operation, @tagName(operation_tag));
                const result = blk: {
                    if (@hasField(@TypeOf(op_data.*), "result")) {
                        if (op_data.result) |r| break :blk r;
                    }
                    break :blk OperationImpl.do_operation(op_data);
                };

                // Requeue onto io_pending if error.WouldBlock
                switch (operation_tag) {
                    .accept, .connect, .send, .recv => {
                        _ = result catch |err| switch (err) {
                            error.WouldBlock => {
                                _completion.next = null;
                                io.io_pending[io.io_inflight] = _completion;
                                io.io_inflight += 1;
                                return;
                            },
                            else => {},
                        };
                    },
                    .read, .write => blk: {
                        if (op_data.result != null) break :blk;
                        op_data.result = result;

                        if (io.injector.push(_completion)) {
                            if (!@atomicRmw(bool, &io.event_set, .Xchg, true, .Release)) {
                                var value: u64 = 1;
                                const wrote = os.write(io.event_fd, std.mem.asBytes(&value)) catch unreachable;
                                assert(wrote == @sizeOf(u64));
                            }
                        }
                        return;
                    },
                    else => {},
                }

                // Complete the Completion
                return callback(
                    @intToPtr(Context, @ptrToInt(_completion.context)),
                    _completion,
                    result,
                );
            }
        }.onComplete;

        completion.* = .{
            .next = null,
            .context = context,
            .callback = onCompleteFn,
            .operation = @unionInit(Operation, @tagName(operation_tag), operation_data),
        };

        switch (operation_tag) {
            .read, .write => self.fs_pool.submit(completion),
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
            .{
                .socket = socket,
            },
            struct {
                fn do_operation(op: anytype) AcceptError!os.socket_t {
                    return os.accept(
                        op.socket,
                        null,
                        null,
                        os.SOCK.NONBLOCK | os.SOCK.CLOEXEC,
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
            callback,
            completion,
            .close,
            .{
                .fd = fd,
            },
            struct {
                fn do_operation(op: anytype) CloseError!void {
                    return switch (os.errno(os.system.close(op.fd))) {
                        .SUCCESS => {},
                        .BADF => error.FileDescriptorInvalid,
                        .INTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425
                        .IO => error.InputOutput,
                        else => |errno| os.unexpectedErrno(errno),
                    };
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
                    const result = switch (op.initiated) {
                        true => os.getsockoptError(op.socket),
                        else => os.connect(op.socket, &op.address.any, op.address.getOsSockLen()),
                    };

                    op.initiated = true;
                    return result;
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
        ConnectionTimedOut,
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
                    while (true) {
                        const rc = os.system.pread(
                            op.fd,
                            op.buf,
                            op.len,
                            @bitCast(isize, op.offset),
                        );
                        return switch (os.errno(rc)) {
                            .SUCCESS => @intCast(usize, rc),
                            .INTR => continue,
                            .AGAIN => error.WouldBlock,
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
                            else => |err| os.unexpectedErrno(err),
                        };
                    }
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
                    return os.send(op.socket, op.buf[0..op.len], os.MSG.NOSIGNAL);
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
        // Special case a zero timeout as a yield.
        if (nanoseconds == 0) {
            completion.* = .{
                .next = null,
                .context = context,
                .operation = undefined,
                .callback = struct {
                    fn on_complete(_io: *IO, _completion: *Completion) void {
                        _ = _io;
                        const _context = @intToPtr(Context, @ptrToInt(_completion.context));
                        callback(_context, _completion, {});
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
            .{
                .expires = self.time.monotonic() + nanoseconds,
            },
            struct {
                fn do_operation(_: anytype) TimeoutError!void {
                    return; // timeouts don't have errors for now
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

    pub const INVALID_SOCKET = -1;

    /// Creates a socket that can be used for async operations with the IO instance.
    pub fn open_socket(_: *IO, family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        return os.socket(family, sock_type | os.SOCK.NONBLOCK, protocol);
    }

    /// Opens a directory with read only access.
    pub fn open_dir(dir_path: []const u8) !os.fd_t {
        return os.open(dir_path, os.O.CLOEXEC | os.O.RDONLY, 0);
    }

    pub const INVALID_FILE: os.fd_t = -1;

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    /// - Allocates the file contiguously on disk if this is supported by the file system.
    /// - Ensures that the file data (and file inode in the parent directory) is durable on disk.
    ///   The caller is responsible for ensuring that the parent directory inode is durable.
    /// - Verifies that the file size matches the expected file size before returning.
    pub fn open_file(
        dir_fd: os.fd_t,
        relative_path: []const u8,
        size: u64,
        method: enum { create, create_or_open, open },
    ) !os.fd_t {
        assert(relative_path.len > 0);
        assert(size % constants.sector_size == 0);

        // TODO Use O_EXCL when opening as a block device to obtain a mandatory exclusive lock.
        // This is much stronger than an advisory exclusive lock, and is required on some platforms.

        var flags: u32 = os.O.CLOEXEC | os.O.RDWR | os.O.DSYNC;
        var mode: os.mode_t = 0;

        // TODO Document this and investigate whether this is in fact correct to set here.
        if (@hasDecl(os.O, "LARGEFILE")) flags |= os.O.LARGEFILE;

        var direct_io_supported = false;
        if (constants.direct_io) {
            direct_io_supported = try fs_supports_direct_io(dir_fd);
            if (direct_io_supported) {
                flags |= os.O.DIRECT;
            } else if (!constants.direct_io_required) {
                log.warn("file system does not support Direct I/O", .{});
            } else {
                // We require Direct I/O for safety to handle fsync failure correctly, and therefore
                // panic in production if it is not supported.
                @panic("file system does not support Direct I/O");
            }
        }

        switch (method) {
            .create => {
                flags |= os.O.CREAT;
                flags |= os.O.EXCL;
                mode = 0o666;
                log.info("creating \"{s}\"...", .{relative_path});
            },
            .create_or_open => {
                flags |= os.O.CREAT;
                mode = 0o666;
                log.info("opening or creating \"{s}\"...", .{relative_path});
            },
            .open => {
                log.info("opening \"{s}\"...", .{relative_path});
            },
        }

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert((flags & os.O.DSYNC) > 0);

        // Be careful with openat(2): "If pathname is absolute, then dirfd is ignored." (man page)
        assert(!std.fs.path.isAbsolute(relative_path));
        const fd = try os.openat(dir_fd, relative_path, flags, mode);
        // TODO Return a proper error message when the path exists or does not exist (init/start).
        errdefer os.close(fd);

        // TODO Check that the file is actually a file.

        // Obtain an advisory exclusive lock that works only if all processes actually use flock().
        // LOCK_NB means that we want to fail the lock without waiting if another process has it.
        os.flock(fd, os.LOCK.EX | os.LOCK.NB) catch |err| switch (err) {
            error.WouldBlock => @panic("another process holds the data file lock"),
            else => return err,
        };

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // If the file system does not support `fallocate()`, then this could mean more seeks or a
        // panic if we run out of disk space (ENOSPC).
        if (method == .create) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(fd, size) catch |err| switch (err) {
                error.OperationNotSupported => {
                    log.warn("file system does not support fallocate(), an ENOSPC will panic", .{});
                    log.info("allocating by writing to the last sector of the file instead...", .{});

                    const sector_size = constants.sector_size;
                    const sector: [sector_size]u8 align(sector_size) = [_]u8{0} ** sector_size;

                    // Handle partial writes where the physical sector is less than a logical sector:
                    const write_offset = size - sector.len;
                    var written: usize = 0;
                    while (written < sector.len) {
                        written += try os.pwrite(fd, sector[written..], write_offset + written);
                    }
                },
                else => |e| return e,
            };
        }

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try os.fsync(fd);

        // We fsync the parent directory to ensure that the file inode is durably written.
        // The caller is responsible for the parent directory inode stored under the grandparent.
        // We always do this when opening because we don't know if this was done before crashing.
        try os.fsync(dir_fd);

        const stat = try os.fstat(fd);
        if (stat.size < size) @panic("data file inode size was truncated or corrupted");

        return fd;
    }

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
    fn fs_supports_direct_io(dir_fd: std.os.fd_t) !bool {
        if (!@hasDecl(std.os.O, "DIRECT")) return false;

        const path = "fs_supports_direct_io";
        const dir = std.fs.Dir{ .fd = dir_fd };
        const fd = try os.openatZ(dir_fd, path, os.O.CLOEXEC | os.O.CREAT | os.O.TRUNC, 0o666);
        defer os.close(fd);
        defer dir.deleteFile(path) catch {};

        while (true) {
            const res = os.linux.openat(dir_fd, path, os.O.CLOEXEC | os.O.RDONLY | os.O.DIRECT, 0);
            switch (os.linux.getErrno(res)) {
                .SUCCESS => {
                    os.close(@intCast(os.fd_t, res));
                    return true;
                },
                .INTR => continue,
                .INVAL => return false,
                else => |err| return os.unexpectedErrno(err),
            }
        }
    }

    /// Allocates a file contiguously using fallocate() if supported.
    /// Alternatively, writes to the last sector so that at least the file size is correct.
    fn fs_allocate(fd: os.fd_t, size: u64) !void {
        const mode: i32 = 0;
        const offset: i64 = 0;
        const length = @intCast(i64, size);

        while (true) {
            const rc = os.linux.fallocate(fd, mode, offset, length);
            switch (os.linux.getErrno(rc)) {
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
                else => |errno| return os.unexpectedErrno(errno),
            }
        }
    }
};
