const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const mem = std.mem;
const net = std.net;
const testing = std.testing;

pub const io_uring_params = linux.io_uring_params;
pub const io_uring_cqe = linux.io_uring_cqe;

// TODO Patch linux.zig to refine and expand the struct definition of io_uring_sqe.
pub const io_uring_sqe = extern struct {
    opcode: linux.IORING_OP,
    flags: u8 = 0,
    ioprio: u16 = 0,
    fd: i32 = 0,
    off: u64 = 0,
    addr: u64 = 0,
    len: u32 = 0,
    opflags: u32 = 0,
    user_data: u64 = 0,
    buffer: u16 = 0,
    personality: u16 = 0,
    splice_fd_in: i32 = 0,
    options: [2]u64 = [2]u64{ 0, 0 }
};

// TODO Add this to zig/std/os/bits/linux.zig:
pub const IORING_SQ_CQ_OVERFLOW = 1 << 1;

comptime {
    assert(@sizeOf(io_uring_params) == 120);
    assert(@sizeOf(io_uring_sqe) == 64);
    assert(@sizeOf(io_uring_cqe) == 16);

    assert(linux.IORING_OFF_SQ_RING == 0);
    assert(linux.IORING_OFF_CQ_RING == 0x8000000);
    assert(linux.IORING_OFF_SQES == 0x10000000);
}

pub const IO_Uring = struct {
    fd: i32 = -1,
    sq: SubmissionQueue,
    cq: CompletionQueue,
    flags: u32,

    /// A friendly way to setup an io_uring, with default io_uring_params.
    /// `entries` must be a power of two between 1 and 4096, although the kernel will make the final
    /// call on how many entries the submission and completion queues will ultimately have,
    /// see https://github.com/torvalds/linux/blob/v5.8/fs/io_uring.c#L8027-L8050.
    /// Matches the interface of io_uring_queue_init() in liburing.
    pub fn init(entries: u32, flags: u32) !IO_Uring {
        var params = io_uring_params {
            .sq_entries = 0,
            .cq_entries = 0,
            .flags = flags,
            .sq_thread_cpu = 0,
            .sq_thread_idle = 1000,
            .features = 0,
            .wq_fd = 0,
            .resv = [_]u32{0} ** 3,
            .sq_off = undefined,
            .cq_off = undefined,
        };
        // The kernel will zero the memory of the sq_off and cq_off structs in io_uring_create(),
        // see https://github.com/torvalds/linux/blob/v5.8/fs/io_uring.c#L7986-L8002.
        return try IO_Uring.init_params(entries, &params);
    }

    /// A powerful way to setup an io_uring, if you want to tweak io_uring_params such as submission
    /// queue thread cpu affinity or thread idle timeout (the kernel and our default is 1 second).
    /// `params` is passed by reference because the kernel needs to modify the parameters.
    /// You may only set the `flags`, `sq_thread_cpu` and `sq_thread_idle` parameters.
    /// Every other parameter belongs to the kernel and must be zeroed.
    /// Matches the interface of io_uring_queue_init_params() in liburing.
    pub fn init_params(entries: u32, p: *io_uring_params) !IO_Uring {
        assert(entries >= 1 and entries <= 4096 and std.math.isPowerOfTwo(entries));
        assert(p.*.sq_entries == 0);
        assert(p.*.cq_entries == 0);
        assert(p.*.features == 0);
        assert(p.*.wq_fd == 0);
        assert(p.*.resv[0] == 0);
        assert(p.*.resv[1] == 0);
        assert(p.*.resv[2] == 0);

        const result = linux.io_uring_setup(entries, p);
        const errno = linux.getErrno(result);
        if (errno != 0) return os.unexpectedErrno(errno);
        const fd = @intCast(i32, result);
        assert(fd >= 0);
        errdefer os.close(fd);

        // Kernel versions 5.4 and up use only one mmap() for the submission and completion queues.
        // This is not an optional feature for us... if the kernel does it, we have to do it.
        // The thinking on this by the kernel developers was that both the submission and the
        // completion queue rings have sizes just over a power of two, but the submission queue ring
        // is significantly smaller with u32 slots. By bundling both in a single mmap, the kernel
        // gets the submission queue ring for free.
        // See https://patchwork.kernel.org/patch/11115257 for the kernel patch.
        // We do not support the double mmap() done before 5.4, because we want to keep the
        // init/deinit mmap paths simple and because io_uring has had many bug fixes even since 5.4.
        if ((p.*.features & linux.IORING_FEAT_SINGLE_MMAP) == 0) {
            return error.IO_UringKernelNotSupported;
        }

        // Check that the kernel has actually set params and that "impossible is nothing".
        assert(p.*.sq_entries != 0);
        assert(p.*.cq_entries != 0);
        assert(p.*.cq_entries >= p.*.sq_entries);

        // From here on, we only need to read from params, so pass `p` by value for convenience.
        // The completion queue shares the mmap with the submission queue, so pass `sq` there too.
        var sq = try SubmissionQueue.init(fd, p.*);
        errdefer sq.deinit();
        var cq = try CompletionQueue.init(fd, p.*, sq);
        errdefer cq.deinit();

        // Check that our starting state is as we expect.
        assert(sq.head.* == 0);
        assert(sq.tail.* == 0);
        assert(sq.mask.* == p.*.sq_entries - 1);
        // Allow flags.* to be non-zero, since the kernel may set IORING_SQ_NEED_WAKEUP at any time.
        assert(sq.dropped.* == 0);
        assert(sq.array.len == p.*.sq_entries);
        assert(sq.sqes.len == p.*.sq_entries);
        assert(sq.sqe_head == 0);
        assert(sq.sqe_tail == 0);

        assert(cq.head.* == 0);
        assert(cq.tail.* == 0);
        assert(cq.mask.* == p.*.cq_entries - 1);
        assert(cq.overflow.* == 0);
        assert(cq.cqes.len == p.*.cq_entries);

        // Alles in Ordnung!
        return IO_Uring {
            .fd = fd,
            .sq = sq,
            .cq = cq,
            .flags = p.*.flags
        };
    }

    pub fn deinit(self: *IO_Uring) void {
        assert(self.fd >= 0);
        // The mmap's depend on the fd, so the order is important:
        self.cq.deinit();
        self.sq.deinit();
        os.close(self.fd);
        self.fd = -1;
    }

    /// Returns a vacant SQE, or an error if the submission queue is full.
    /// We follow the implementation (and atomics) of liburing's `io_uring_get_sqe()` exactly.
    /// However, instead of a null we return an error to force safe handling.
    /// Any situation where the submission queue is full tends more towards a control flow error,
    /// and the null return in liburing is more a C idiom than anything else, for lack of a better
    /// alternative. In Zig, we have first-class error handling... so let's use it.
    /// Matches the implementation of io_uring_get_sqe() in liburing.
    pub fn get_sqe(self: *IO_Uring) !*io_uring_sqe {
        const head = @atomicLoad(u32, self.sq.head, .Acquire);
        // Remember that these head and tail offsets wrap around every four billion operations.
        // We must therefore use wrapping addition and subtraction to avoid a runtime crash.
        const next = self.sq.sqe_tail +% 1;
        if (next -% head > self.sq.sqes.len) return error.IO_UringSubmissionQueueFull;
        var sqe = &self.sq.sqes[self.sq.sqe_tail & self.sq.mask.*];
        self.sq.sqe_tail = next;
        return sqe;
    }

    /// Submits the SQEs acquired via get_sqe() to the kernel. You can call this once after you have
    /// called get_sqe() multiple times to set up multiple I/O requests.
    /// Returns the number of SQEs submitted.
    /// Matches the implementation of io_uring_submit() in liburing.
    pub fn submit(self: *IO_Uring) !u32 {
        return self.submit_and_wait(0);
    }

    /// Like submit(), but allows waiting for events as well.
    /// Returns the number of SQEs submitted.
    /// Matches the implementation of io_uring_submit_and_wait() in liburing.
    pub fn submit_and_wait(self: *IO_Uring, wait_nr: u32) !u32 {
        var submitted = self.flush_sq();
        var flags: u32 = 0;
        if (self.sq_ring_needs_enter(submitted, &flags) or wait_nr > 0) {
            if (wait_nr > 0 or (self.flags & linux.IORING_SETUP_IOPOLL) > 0) {
                flags |= linux.IORING_ENTER_GETEVENTS;
            }
            try self.enter(submitted, wait_nr, flags);
        }
        return submitted;
    }

    // Tell the kernel we have submitted SQEs and/or want to wait for CQEs.
    fn enter(self: *IO_Uring, to_submit: u32, min_complete: u32, flags: u32) !void {
        const res = linux.io_uring_enter(self.fd, to_submit, min_complete, flags, null);
        const errno = linux.getErrno(res);
        if (errno != 0) return os.unexpectedErrno(errno);
    }

    // Sync internal state with kernel ring state on the SQ side.
    // Returns the number of all pending events in the SQ ring, for the shared ring.
    // This return value includes previously flushed SQEs, as per liburing.
    // The reasoning for this is to suggest that an io_uring_enter() call is needed rather than not.
    // Matches the implementation of __io_uring_flush_sq() in liburing.
    fn flush_sq(self: *IO_Uring) u32 {
        if (self.sq.sqe_head != self.sq.sqe_tail) {
            // Fill in SQEs that we have queued up, adding them to the kernel ring.
            const to_submit = self.sq.sqe_tail -% self.sq.sqe_head;
            const mask = self.sq.mask.*;
            var tail = self.sq.tail.*;
            var i: usize = 0;
            while (i < to_submit) : (i += 1) {
                self.sq.array[tail & mask] = self.sq.sqe_head & mask;
                tail +%= 1;
                self.sq.sqe_head +%= 1;
            }
            // Ensure that the kernel can actually see the SQE updates when it sees the tail update.
            @atomicStore(u32, self.sq.tail, tail, .Release);
        }
        return self.sq_ready();
    }

    /// Returns true if we are not using an SQ thread (thus nobody submits but us),
    /// or if IORING_SQ_NEED_WAKEUP is set and the SQ thread must be explicitly awakened.
    /// For the latter case, we set the SQ thread wakeup flag.
    /// Matches the implementation of sq_ring_needs_enter() in liburing.
    fn sq_ring_needs_enter(self: *IO_Uring, submitted: u32, flags: *u32) bool {
        assert(flags.* == 0);
        if ((self.flags & linux.IORING_SETUP_SQPOLL) == 0 and submitted > 0) return true;
        if ((@atomicLoad(u32, self.sq.flags, .Unordered) & linux.IORING_SQ_NEED_WAKEUP) > 0) {
            flags.* |= linux.IORING_ENTER_SQ_WAKEUP;
            return true;
        }
        return false;
    }

    /// Returns the number of flushed and unflushed SQEs pending in the submission queue.
    /// In other words, this is the number of SQEs in the submission queue, i.e. its length.
    /// These are SQEs that the kernel is yet to consume.
    /// Matches the implementation of io_uring_sq_ready in liburing.
    pub fn sq_ready(self: *IO_Uring) u32 {
        // Always use the shared ring state (i.e. head and not sqe_head) to avoid going out of sync,
        // see https://github.com/axboe/liburing/issues/92.
        return self.sq.sqe_tail -% @atomicLoad(u32, self.sq.head, .Acquire);
    }

    /// Returns the number of CQEs in the completion queue, i.e. its length.
    /// These are CQEs that the application is yet to consume.
    /// Matches the implementation of io_uring_cq_ready in liburing.
    pub fn cq_ready(self: *IO_Uring) u32 {
        return @atomicLoad(u32, self.cq.tail, .Acquire) -% self.cq.head.*;
    }

    /// Copy as many CQEs as are ready, and that can fit into the destination `cqes` slice.
    /// If none are available, enter into the kernel to wait for at most `wait_nr` CQEs.
    /// Returns the number of CQEs copied.
    /// Provides all the wait/peek methods found in liburing, but with batching and a single method.
    /// The rationale for copying CQEs rather than copying pointers is that pointers are 8 bytes
    /// whereas CQEs are not much more at only 16 bytes, and this provides a safer faster interface.
    /// Safer because you no longer need to call cqe_seen() exactly once, avoiding idempotency bugs.
    /// Faster because we can now amortize the atomic store release to `cq.head` across the batch.
    /// See https://github.com/axboe/liburing/issues/103#issuecomment-686665007.
    /// Matches the implementation of io_uring_peek_batch_cqe() in liburing, but allows waiting.
    pub fn copy_cqes(self: *IO_Uring, cqes: []io_uring_cqe, wait_nr: u32) !u32 {
        const count = self.copy_cqes_ready(cqes, wait_nr);
        if (count > 0) return count;
        if (self.cq_ring_needs_flush() or wait_nr > 0) {
            try self.enter(0, wait_nr, linux.IORING_ENTER_GETEVENTS);
            return self.copy_cqes_ready(cqes, wait_nr);
        }
        return 0;
    }

    fn copy_cqes_ready(self: *IO_Uring, cqes: []io_uring_cqe, wait_nr: u32) u32 {
        const ready = self.cq_ready();
        const count = std.math.min(cqes.len, ready);
        const mask = self.cq.mask.*;
        var head = self.cq.head.*;
        var tail = head +% count;
        // TODO Optimize this by using 1 or 2 memcpy's (if the tail wraps) rather than a loop.
        var i: usize = 0;
        // Do not use "less-than" operator since head and tail may wrap:
        while (head != tail) {
            cqes[i] = self.cq.cqes[head & mask]; // Copy struct by value.
            head +%= 1;
            i += 1;
        }
        self.cq_advance(count);
        return count;
    }

    // Matches the implementation of cq_ring_needs_flush() in liburing.
    fn cq_ring_needs_flush(self: *IO_Uring) bool {
        return (@atomicLoad(u32, self.sq.flags, .Unordered) & IORING_SQ_CQ_OVERFLOW) > 0;
    }

    /// If you use copy_cqes() you do not need to (and must not) call cqe_seen() or cq_advance().
    /// Must be called EXACTLY ONCE after a zero-copy CQE has been processed by your application.
    /// Not idempotent, calling more than once will result in other CQEs being lost.
    /// Matches the implementation of cqe_seen() in liburing.
    pub fn cqe_seen(self: *IO_Uring, cqe: *io_uring_cqe) void {
        self.cq_advance(1);
    }

    /// Matches the implementation of cq_advance() in liburing.
    pub fn cq_advance(self: *IO_Uring, count: u32) void {
        if (count > 0) {
            // Ensure the kernel only sees the new head value after the CQEs have been read.
            @atomicStore(u32, self.cq.head, self.cq.head.* +% count, .Release);
        }
    }

    /// Matches the implementation of io_uring_prep_rw() in liburing, used for many prep_* methods.
    pub fn prep_rw(
        op: linux.IORING_OP, sqe: *io_uring_sqe, fd: i32, addr: u64, len: u32, offset: u64
    ) void {
        sqe.* = .{
            .opcode = op,
            .flags = 0,
            .ioprio = 0,
            .fd = fd,
            .off = offset,
            .addr = addr,
            .len = len,
            .opflags = 0,
            .user_data = 0,
            .buffer = 0,
            .personality = 0,
            .splice_fd_in = 0,
            .options = [2]u64{ 0, 0 }
        };
    }

    pub fn queue_accept(
        self: *IO_Uring,
        user_data: u64,
        fd: os.fd_t,
        addr: *os.sockaddr,
        addrlen: *os.socklen_t,
        accept_flags: u32
    ) !void {
        // "sqe->fd is the file descriptor, sqe->addr holds a pointer to struct sockaddr,
        // sqe->addr2 holds a pointer to socklen_t, and finally sqe->accept_flags holds the flags
        // for accept(4)." - https://lwn.net/ml/linux-block/20191025173037.13486-1-axboe@kernel.dk/
        const sqe = try self.get_sqe();
        sqe.* = .{
            .opcode = .ACCEPT,
            .fd = fd,
            .off = @ptrToInt(addrlen),
            .addr = @ptrToInt(addr),
            .user_data = user_data,
            .opflags = accept_flags
        };
    }

    pub fn queue_readv(
        self: *IO_Uring,
        user_data: u64,
        fd: os.fd_t,
        iovecs: []const os.iovec,
        offset: u64
    ) !void {
        const sqe = try self.get_sqe();
        sqe.* = .{
            .opcode = .READV,
            .fd = fd,
            .off = offset,
            .addr = @ptrToInt(iovecs.ptr),
            .len = @truncate(u32, iovecs.len),
            .opflags = 0,
            .user_data = user_data
        };
    }

    pub fn queue_nop(self: *IO_Uring, user_data: u64) !void {
        const sqe = try self.get_sqe();
        sqe.* = .{
            .opcode = .NOP,
            .user_data = user_data
        };
    }
};

pub const SubmissionQueue = struct {
    head: *u32,
    tail: *u32,
    mask: *u32,
    flags: *u32,
    dropped: *u32,
    array: []u32,
    sqes: []io_uring_sqe,
    mmap: []align(std.mem.page_size) u8,
    mmap_sqes: []align(std.mem.page_size) u8,

    // We use `sqe_head` and `sqe_tail` in the same way as liburing:
    // We increment `sqe_tail` (but not `tail`) for each call to `get_sqe()`.
    // We then set `tail` to `sqe_tail` once, only when these events are actually submitted.
    // This allows us to amortize the cost of the @atomicStore to `tail` across multiple SQEs.
    sqe_head: u32 = 0,
    sqe_tail: u32 = 0,
    
    pub fn init(fd: i32, p: io_uring_params) !SubmissionQueue {
        assert(fd >= 0);
        assert((p.features & linux.IORING_FEAT_SINGLE_MMAP) > 0);
        const size = std.math.max(
            p.sq_off.array + p.sq_entries * @sizeOf(u32),
            p.cq_off.cqes + p.cq_entries * @sizeOf(io_uring_cqe)
        );
        const mmap = try os.mmap(
            null,
            size,
            os.PROT_READ | os.PROT_WRITE,
            os.MAP_SHARED | os.MAP_POPULATE,
            fd,
            linux.IORING_OFF_SQ_RING,
        );
        errdefer os.munmap(mmap);
        assert(mmap.len == size);

        // The motivation for the `sqes` and `array` indirection is to make it possible for the
        // application to preallocate static io_uring_sqe entries and then submit them when needed.
        const size_sqes = p.sq_entries * @sizeOf(io_uring_sqe);
        const mmap_sqes = try os.mmap(
            null,
            size_sqes,
            os.PROT_READ | os.PROT_WRITE,
            os.MAP_SHARED | os.MAP_POPULATE,
            fd,
            linux.IORING_OFF_SQES,
        );
        errdefer os.munmap(mmap_sqes);
        assert(mmap_sqes.len == size_sqes);

        const array = @ptrCast([*]u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.array]));
        const sqes = @ptrCast([*]io_uring_sqe, @alignCast(@alignOf(io_uring_sqe), &mmap_sqes[0]));
        // We expect the kernel copies p.sq_entries to the u32 pointed to by p.sq_off.ring_entries,
        // see https://github.com/torvalds/linux/blob/v5.8/fs/io_uring.c#L7843-L7844.
        assert(
            p.sq_entries ==
            @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.ring_entries])).*
        );
        return SubmissionQueue {
            .head = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.head])),
            .tail = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.tail])),
            .mask = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.ring_mask])),
            .flags = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.flags])),
            .dropped = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.sq_off.dropped])),
            .array = array[0..p.sq_entries],
            .sqes = sqes[0..p.sq_entries],
            .mmap = mmap,
            .mmap_sqes = mmap_sqes
        };
    }

    pub fn deinit(self: *SubmissionQueue) void {
        os.munmap(self.mmap_sqes);
        os.munmap(self.mmap);
    }
};

pub const CompletionQueue = struct {
    head: *u32,
    tail: *u32,
    mask: *u32,
    overflow: *u32,
    cqes: []io_uring_cqe,

    pub fn init(fd: i32, p: io_uring_params, sq: SubmissionQueue) !CompletionQueue {
        assert(fd >= 0);
        assert((p.features & linux.IORING_FEAT_SINGLE_MMAP) > 0);
        const mmap = sq.mmap;
        const cqes = @ptrCast(
            [*]io_uring_cqe,
            @alignCast(@alignOf(io_uring_cqe), &mmap[p.cq_off.cqes])
        );
        assert(
            p.cq_entries ==
            @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.cq_off.ring_entries])).*
        );
        return CompletionQueue {
            .head = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.cq_off.head])),
            .tail = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.cq_off.tail])),
            .mask = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.cq_off.ring_mask])),
            .overflow = @ptrCast(*u32, @alignCast(@alignOf(u32), &mmap[p.cq_off.overflow])),
            .cqes = cqes[0..p.cq_entries]
        };
    }

    pub fn deinit(self: *CompletionQueue) void {
        // A no-op since we now share the mmap with the submission queue.
        // Here for symmetry with the submission queue, and for any future feature support.
    }
};

// TODO Move this into the ring struct.
fn wait_cqe(ring: *IO_Uring) !io_uring_cqe {
    var cqes: [1]io_uring_cqe = undefined;
    const count = try ring.copy_cqes(&cqes, 1);
    assert(count == 1);
    return cqes[0];
}

test "uring" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    // TODO Add more tests when we get to BetaBeetle:

    var ring = try IO_Uring.init(16, 0);
    defer ring.deinit();

    try ring.queue_nop(@intCast(u64, 0xdeadbeef));
    testing.expectEqual(ring.sq.sqe_head, 0);
    testing.expectEqual(ring.sq.sqe_tail, 1);
    testing.expectEqual(ring.cq.head.*, 0);
    
    testing.expectEqual(try ring.submit(), 1);
    testing.expectEqual(ring.sq.sqe_head, 1);
    testing.expectEqual(ring.sq.sqe_tail, 1);
    
    testing.expectEqual(io_uring_cqe{
        .user_data = 0xdeadbeef,
        .res = 0,
        .flags = 0
    }, try wait_cqe(&ring));
    testing.expectEqual(ring.cq.head.*, 1);

    const zero = try os.openZ("/dev/zero", os.O_RDONLY | os.O_CLOEXEC, 0);
    defer os.close(zero);

    { // read some 0 bytes from /dev/zero
        var buf: [100]u8 = undefined;
        var iovecs = [1]os.iovec{os.iovec{
            .iov_base = &buf,
            .iov_len = buf.len,
        }};
        try ring.queue_readv(
            0xcafebabe,
            zero,
            iovecs[0..],
            0
        );
        testing.expectEqual(@as(u32, 1), try ring.submit());
        var cqe = try wait_cqe(&ring);
        testing.expectEqual(linux.io_uring_cqe {
            .user_data = 0xcafebabe,
            .res = 100,
            .flags = 0,
        }, cqe);
        //testing.expectEqualSlices(u8, [_]u8{0} ** 100, buf[0..]);
    }
}
