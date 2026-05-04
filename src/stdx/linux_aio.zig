//! A small Zig-native wrapper around Linux native AIO.
//! This is NOT a binding to libaio. It uses raw Linux syscalls:
//!
//!   io_setup, io_destroy, io_submit, io_getevents, io_cancel

const std = @import("std");
const builtin = @import("builtin");

pub const linux_aio = struct {
    const linux = std.os.linux;

    comptime {
        if (builtin.os.tag != .linux) {
            @compileError("linux_aio is Linux-only");
        }
        if (@sizeOf(usize) != 8) {
            @compileError("this linux_aio wrapper currently supports only 64-bit Linux targets");
        }
        if (builtin.cpu.arch.endian() != .little) {
            @compileError("this linux_aio wrapper currently supports only little-endian Linux targets");
        }
    }

    pub const AioError = error{
        PermissionDenied,
        NotSupported,
        InvalidArgument,
        Again,
        NoMemory,
        BadFileDescriptor,
        Fault,
        Interrupted,
        ResourceLimit,
        Canceled,
        Io,
        NoSpaceLeft,
        FileTooBig,
        UnknownErrno,
    };

    pub const CompletionError = error{
        Canceled,
        Again,
        BadFileDescriptor,
        Fault,
        Interrupted,
        InvalidArgument,
        Io,
        NoSpaceLeft,
        FileTooBig,
        PermissionDenied,
        NotSupported,
        UnknownErrno,
    };

    /// Kernel aio_context_t. On 64-bit Linux this is unsigned long / u64.
    pub const RawContext = u64;

    pub const Opcode = enum(u16) {
        pread = 0,
        pwrite = 1,
        fsync = 2,
        fdatasync = 3,
        poll = 5,
        noop = 6,
        preadv = 7,
        pwritev = 8,
    };

    pub const RequestOptions = struct {
        /// Copied to Event.data. Use it for a pointer, integer id, enum tag, etc.
        data: usize = 0,
        reqprio: i16 = 0,

        /// Raw per-request flags. This field corresponds to iocb.aio_rw_flags.
        /// It may contain RWF_* bits on kernels/filesystems that support them.
        rw_flags: u32 = 0,

        /// Optional eventfd for completion notification. Sets IOCB_FLAG_RESFD.
        resfd: ?std.posix.fd_t = null,
    };

    /// From linux/aio_abi.h: #define IOCB_FLAG_RESFD (1 << 0)
    const IOCB_FLAG_RESFD: u32 = 1;

    /// Exact userspace ABI for struct iocb from linux/aio_abi.h for 64-bit
    /// little-endian Linux.
    pub const Iocb = extern struct {
        aio_data: u64 = 0,
        aio_key: u32 = 0,
        aio_rw_flags: u32 = 0,
        aio_lio_opcode: u16 = 0,
        aio_reqprio: i16 = 0,
        aio_fildes: u32 = 0,
        aio_buf: u64 = 0,
        aio_nbytes: u64 = 0,
        aio_offset: i64 = 0,
        aio_reserved2: u64 = 0,
        aio_flags: u32 = 0,
        aio_resfd: u32 = 0,

        fn applyOptions(self: *Iocb, opts: RequestOptions) void {
            self.aio_data = @as(u64, @intCast(opts.data));
            self.aio_reqprio = opts.reqprio;
            self.aio_rw_flags = opts.rw_flags;
            if (opts.resfd) |efd| {
                self.aio_flags |= IOCB_FLAG_RESFD;
                self.aio_resfd = @as(u32, @intCast(efd));
            }
        }

        pub fn prep(
            fd: std.posix.fd_t,
            opcode: Opcode,
            ptr: [*]u8,
            len: usize,
            offset: i64,
            opts: RequestOptions,
        ) Iocb {
            var cb = Iocb{
                .aio_lio_opcode = @intFromEnum(opcode),
                .aio_fildes = @as(u32, @intCast(fd)),
                .aio_buf = @as(u64, @intCast(@intFromPtr(ptr))),
                .aio_nbytes = @as(u64, @intCast(len)),
                .aio_offset = offset,
            };
            cb.applyOptions(opts);
            return cb;
        }

        pub fn pread(fd: std.posix.fd_t, buf: []u8, offset: i64, opts: RequestOptions) Iocb {
            return prep(fd, .pread, buf.ptr, buf.len, offset, opts);
        }

        pub fn pwrite(fd: std.posix.fd_t, buf: []const u8, offset: i64, opts: RequestOptions) Iocb {
            return prep(fd, .pwrite, @constCast(buf.ptr), buf.len, offset, opts);
        }

        pub fn fsync(fd: std.posix.fd_t, opts: RequestOptions) Iocb {
            var cb = Iocb{
                .aio_lio_opcode = @intFromEnum(Opcode.fsync),
                .aio_fildes = @as(u32, @intCast(fd)),
            };
            cb.applyOptions(opts);
            return cb;
        }

        pub fn fdatasync(fd: std.posix.fd_t, opts: RequestOptions) Iocb {
            var cb = Iocb{
                .aio_lio_opcode = @intFromEnum(Opcode.fdatasync),
                .aio_fildes = @as(u32, @intCast(fd)),
            };
            cb.applyOptions(opts);
            return cb;
        }
    };

    /// Exact userspace ABI for struct io_event from linux/aio_abi.h.
    pub const Event = extern struct {
        data: u64,
        obj: u64,
        res: i64,
        res2: i64,

        pub fn userData(self: Event) usize {
            return @as(usize, @intCast(self.data));
        }

        pub fn iocb(self: Event) *Iocb {
            return @as(*Iocb, @ptrFromInt(@as(usize, @intCast(self.obj))));
        }

        /// Returns the successful byte count / operation result, or maps the
        /// negative Linux errno stored in res into a Zig error.
        pub fn result(self: Event) CompletionError!usize {
            if (self.res >= 0) return @as(usize, @intCast(self.res));
            const errno: std.posix.E = @enumFromInt(-self.res);
            return completionErrno(errno);
        }
    };

    /// Kernel ABI timespec for 64-bit Linux.
    pub const Timespec = extern struct {
        tv_sec: isize,
        tv_nsec: isize,

        pub fn fromMillis(ms: u64) Timespec {
            return .{
                .tv_sec = @as(isize, @intCast(ms / 1000)),
                .tv_nsec = @as(isize, @intCast((ms % 1000) * 1_000_000)),
            };
        }
    };

    pub const Context = struct {
        raw: RawContext = 0,

        pub fn init(max_events: u32) AioError!Context {
            var ctx: RawContext = 0;
            const rc = linux.syscall2(.io_setup, max_events, @intFromPtr(&ctx));
            try checkSyscall(rc);
            return .{ .raw = ctx };
        }

        pub fn deinit(self: *Context) void {
            if (self.raw == 0) return;
            _ = linux.syscall1(.io_destroy, self.raw);
            self.raw = 0;
        }

        /// Submit request pointers. Returns the number accepted by the kernel and
        /// any errno. On errno .SUCCESS, count is 0..cbs.len (partial is possible).
        /// On errno != .SUCCESS, count is 0 and no iocbs were submitted.
        pub fn submit(self: Context, cbs: []const *Iocb) struct { count: usize, errno: std.posix.E } {
            if (cbs.len == 0) return .{ .count = 0, .errno = .SUCCESS };
            const rc = linux.syscall3(.io_submit, self.raw, cbs.len, @intFromPtr(cbs.ptr));
            const errno = std.posix.errno(rc);
            return .{
                .count = if (errno == .SUCCESS) rc else 0,
                .errno = errno,
            };
        }

        /// Wait for completions.
        ///
        /// min_events may be 0 for a nonblocking poll. timeout == null means
        /// wait indefinitely when min_events > 0.
        pub fn getEvents(
            self: Context,
            min_events: usize,
            events: []Event,
            timeout: ?*Timespec,
        ) AioError!usize {
            if (events.len == 0) return 0;
            const timeout_ptr: usize = if (timeout) |t| @intFromPtr(t) else 0;
            const rc = linux.syscall5(
                .io_getevents,
                self.raw,
                min_events,
                events.len,
                @intFromPtr(events.ptr),
                timeout_ptr,
            );
            return try checkSyscallCount(rc);
        }

        /// Nonblocking completion poll.
        pub fn poll(self: Context, events: []Event) AioError!usize {
            var ts = Timespec{ .tv_sec = 0, .tv_nsec = 0 };
            return self.getEvents(0, events, &ts);
        }

        /// Cancel a request. On success, result_event is filled with the completed
        /// or canceled event. Linux AIO cancellation is best-effort.
        pub fn cancel(self: Context, cb: *Iocb, result_event: *Event) AioError!void {
            const rc = linux.syscall3(
                .io_cancel,
                self.raw,
                @intFromPtr(cb),
                @intFromPtr(result_event),
            );
            try checkSyscall(rc);
        }
    };

    fn checkSyscall(rc: usize) AioError!void {
        const errno = std.posix.errno(rc);
        if (errno == .SUCCESS) return;
        return aioErrno(errno);
    }

    fn checkSyscallCount(rc: usize) AioError!usize {
        const errno = std.posix.errno(rc);
        if (errno == .SUCCESS) return rc;
        return aioErrno(errno);
    }

    fn aioErrno(errno: anytype) AioError {
        return switch (errno) {
            .PERM => error.PermissionDenied,
            .ACCES => error.PermissionDenied,
            .NOSYS => error.NotSupported,
            .OPNOTSUPP => error.NotSupported,
            .INVAL => error.InvalidArgument,
            .AGAIN => error.Again,
            .NOMEM => error.NoMemory,
            .BADF => error.BadFileDescriptor,
            .FAULT => error.Fault,
            .INTR => error.Interrupted,
            .CANCELED => error.Canceled,
            .IO => error.Io,
            .NOSPC => error.NoSpaceLeft,
            .FBIG => error.FileTooBig,
            else => error.UnknownErrno,
        };
    }

    fn completionErrno(errno: std.posix.E) CompletionError {
        return switch (errno) {
            .PERM => error.PermissionDenied,
            .INTR => error.Interrupted,
            .IO => error.Io,
            .BADF => error.BadFileDescriptor,
            .AGAIN => error.Again,
            .FAULT => error.Fault,
            .INVAL => error.InvalidArgument,
            .FBIG => error.FileTooBig,
            .NOSPC => error.NoSpaceLeft,
            .OPNOTSUPP => error.NotSupported,
            .CANCELED => error.Canceled,
            else => error.UnknownErrno,
        };
    }

    comptime {
        std.debug.assert(@sizeOf(Iocb) == 64);
        std.debug.assert(@alignOf(Iocb) == 8);
        std.debug.assert(@sizeOf(Event) == 32);
        std.debug.assert(@alignOf(Event) == 8);
        std.debug.assert(@sizeOf(Timespec) == 16);
    }
};
