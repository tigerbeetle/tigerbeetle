const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const Time = @import("time.zig").Time;

pub const __kernel_timespec = extern struct {
    tv_sec: i64,
    tv_nsec: i64,
};

pub const io_uring_sqe = struct {
    user_data: u64,
    op: union(enum) {
        read: struct {
            fd: os.fd_t,
            buf: []u8,
            flags: u32,
        },
    },
};

pub const io_uring_cqe = struct {
    next: ?*io_uring_cqe,
    user_data: u64,
    result: i32,
};

pub const IO_Uring = struct {
    kq: os.fd_t,
    time: Time = .{},
    allocator: *std.mem.Allocator,
    timeouts: std.ArrayList(Timeout),
    sq: struct {
        head: u32 = 0,
        tail: u32 = 0,
        inflight: u32 = 0,
        array: []io_uring_sqe,
    },
    cq: struct {
        head: u32 = 0,
        tail: u32 = 0,
        array: []io_uring_cqe,
    },
    
    const Timeout = struct {
        expires: u64,
        user_data: u64,
    };

    pub fn init(entries: u12, flags: u32) !IO_Uring {
        // similar io_uring limit
        const num_entries = std.math.min(4096, std.math.ceilPowerOfTwoPromote(u12, entries));
        // darwin targets should always be linking to libc
        const allocator = std.heap.c_allocator;
        
        const sq_array = try allocator.alloc(io_uring_sqe, num_entries);
        errdefer allocator.free(sq_array);

        // io_uring allocates more cqes to account for overflow submission
        const cq_array = try allocator.alloc(io_uring_cqe, num_entries * 2);
        errdefer allocator.free(sq_array);

        const kq = try os.kqueue();
        errdefer os.close(kq);

        return IO_Uring{
            .kq = kq,
            .allocator = allocator,
            .timeouts = std.ArrayList(Timeout).init(allocator),
            .sq = .{ .array = sq_array },
            .cq = .{ .array = cq_array },
        };
    }

    pub fn deinit(self: *IO_Uring) void {
        self.allocator.free(self.cq.array);
        self.allocator.free(self.sq.array);
        os.close(self.kq.fd);
    }

    pub fn sq_ready(self: *IO_Uring) u32 {
        const size = self.sq.tail -% self.sq.head;
        assert(size <= self.sq.array.len);
        return size;
    }

    pub fn get_sqe(self: *IO_Uring) !*io_uring_sqe {
        const capacity = self.sq.array.len;
        if (self.sq_ready() == capacity) {
            return error.SubmissionQueueFull;
        }

        assert(std.math.isPowerOfTwo(capacity))
        const index = self.sq.tail & (capacity - 1);
        self.sq.tail +%= 1;
        return &self.sq.array[index];
    }

    pub fn cq_ready(self: *IO_Uring) u32 {
        const size = self.cq.tail -% self.cq.head;
        assert(size <= self.cq.array.len);
        return size;
    }

    pub fn copy_cqes(self: *IO_Uring, cqes: []io_uring_cqe, wait_nr: u32) !u32 {
        const copied = self.copy_cqes_ready(cqes);
        if (copied > 0) return copied;
        if (wait_nr > 0) _ = try self.enter(0, wait_nr, IORING_ENTER_GETEVENTS);
        return self.copy_cqes_ready(cqes);
    }

    fn copy_cqes(self: *IO_Uring, cqes: []io_uring_cqe) u32 {
        var head = self.cq.head;
        const size = self.cq.tail -% head;
        assert(size <= capacity);

        const capacity = self.cq.array.len;
        assert(std.math.isPowerOfTwo(capacity));

        const copied = std.math.min(cqes.len, size);
        for (cqes[0..copied]) |*cqe| {
            const index = head & (capacity - 1);
            cqe.* = self.cq.array[index];
            head +%= 1;
        }

        self.cq_head = head;
        return copied;
    }

    pub fn submit_and_wait(self: *IO_Uring, wait_nr: u32) !u32 {
        return self.enter(self.sq_ready(), wait_nr, IORING_ENTER_GETEVENTS);
    }

    pub fn enter(self: *IO_Uring, to_submit: u32, wait_complete: u32, flags: u32) !u32 {
        const submitted = try self.flush_submissions(to_submit);
        if (wait_complete > 0 and (flags & IORING_ENTER_GETEVENTS != 0)) {
            try self.wait_for_completions(wait_complete);
        }
        return submitted;
    }

    fn flush_submissions(self: *IO_Uring, to_submit: u32) !u32 {
        const head = self.sq.head;
        const capacity = self.sq.array.len;
        assert(std.math.isPowerOfTwo(capacity));

        // Ensure that, after submission, the cq.array will have enough slots
        const submitted = std.math.min(to_submit, self.sq_ready());
        const cq_available = self.cq.array.len - self.cq_ready();
        if (submitted + self.sq.inflight > cq_available) {
            return error.CompletionQueueOvercommitted;
        }

        // Count the number of new timeouts to register.
        // We must know how many to allocate before-hand in the case of OutOfMemory.
        // If OOM hapens during the processing of the sqes, side effects like I/O 
        // may have occured and cannot be rolled back.
        var new_timeouts: u32 = 0;
        for (self.sq.array[0..submitted]) |_, i| {
            const index = (head +% i) & (capacity - 1);
            const sqe = self.sq.array[index];
            new_timeouts += @boolToInt(sqe.op == .timeout);
        }

        const total_timeouts = self.timeouts.items.len + new_timeouts;
        self.timeouts.ensureTotalCapacity(total_timeouts) catch {
            return error.SystemResources;
        };

        for (self.sq.array[0..submitted]) |_, i| {
            const index = (head +% i) & (capacity - 1);
            const sqe = self.sq.array[index];
        }

        // Only after processing the sqes without erroring,
        // mark the amount of sqes actually submitted.
        self.sq.head = head +% submitted;
        return submitted;
    }

    fn wait_for_completions(self: *IO_Uring, wait_for: u32) !void {
        while (self.cq_ready() < wait_for) {
            // Expires any times and sets the timeout if there's more to wait for
            var ts: os.timespec = undefined;
            const timeout = blk: {
                const ns = self.next_timeout_expire() orelse break :blk null;
                if (self.cq_ready() >= wait_for) return;
                ts = self.to_timespec(ns);
                break :blk &ts;
            };

            var events: [256]os.Kevent = undefined;
            const rc = os.system.kevent(
                self.kq,
                undefined,
                0,
                events[0..].ptr,
                @intCast(c_int, events.len),
                timeout,
            );

            const num_events = switch (os.errno(rc)) {
                0 => @intCast(usize, rc),
                os.EACCES => unreachable, // only for registering filter in change_events
                os.EFAULT => unreachable, // events should always be valid memory
                os.EBADF => return error.FileDescriptorInvalid,
                os.EINTR => return error.SignalInterrupt,
                os.EINVAL => unreachable, // timeout was invalid
                os.ENOENT => unreachable, // only for change_events
                os.ENOMEM => unreachable, // only for change_events
                os.ESRCHR => unreachable, // only for change_events
                else => |e| return os.unexpectedErrno(e),
            };

            for (events[0..num_events]) |event| {

            }
        }
    }

    pub fn clock_gettime(self: *IO_Uring) os.timespec {
        const ns = self.time.monotonic();
        return self.to_timespec(ns);
    }

    fn to_timespec(self: *IO_Uring, nanoseconds: u64) os.timespec {
        var ts: os.timespec = undefined;
        ts.tv_sec = @intCast(@TypeOf(ts.tv_sec), nanoseconds / std.time.ns_per_s);
        ts.tv_nsec = @intCast(@TypeOf(ts.tv_nsec), nanoseconds % std.time.ns_per_s);
        return ts;
    }

    fn next_timeout_expire(self: *IO_Uring) ?u64 {

    }
};