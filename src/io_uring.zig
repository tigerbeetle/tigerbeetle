const std = @import("std");
const os = std.os;
const linux = os.linux;

const FIFO = @import("fifo.zig").FIFO;
const IO = @import("io.zig").IO;
const Completion = IO.Completion;

pub const Driver = struct {
    ring: linux.IO_Uring,

    pub fn init(entries: u12) !Driver {
        return Driver{ .ring = try linux.IO_Uring.init(entries, 0) };
    }

    pub fn deinit(self: *Driver) void {
        self.ring.deinit();
    }

    pub fn timestamp(self: *Driver) u64 {
        var ts: os.timespec = undefined;
        os.clock_gettime(os.CLOCK_MONOTONIC, &ts) catch unreachable;
        return @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec);
    }

    pub fn has_submissions(self: *Driver) bool {
        return self.ring.sq_ready() > 0;
    }

    pub fn submit(self: *Driver, completion: *Completion) IO.SubmitError!void {
        const sqe = try self.ring.get_sqe();
        self.prepare(sqe, completion);
    }

    pub fn has_completions(self: *Driver) bool {
        return self.ring.cq_ready() > 0;
    }

    pub fn poll(self: *Driver) FIFO(Completion) {
        var completions: FIFO(Completion) = .{};
        while (true) {
            var cqes: [256]io_uring_cqe = undefined;
            const num_cqes = self.ring.copy_cqes(&cqes, 0) catch unreachable;
            if (num_cqes == 0) break;
            for (cqes[0..num_cqes]) |cqe| {
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                completion.result = cqe.res;
                completions.push(completion);
            }
        }
        return completions;
    }

    pub fn enter(
        self: *Driver,
        flush_submissions: bool,
        wait_for_completions: bool,
    ) IO.EnterError!void {
        return self.ring.enter(
            if (flush_submissions) self.ring.flush_sq() else 0, 
            if (wait_for_completions) 1 else 0, 
            if (wait_for_completions) linux.IORING_ENTER_GETEVENTS else 0,
        ) catch |err| switch (err) {
            error.FileDescriptorInvalid, error.SubmissionQueueEntryInvalid => error.InvalidSubmission,
            error.CompletionQueueOvercommitted, error.SystemResources => error.WaitForCompletions,
            error.FileDescriptorInBadState, error.RingShuttingDown => error.InternalError,
            error.OpcodeNotSupported => error.OperationNotSupported,
            error.UnexpectedError => error.UnexpectedError,
            error.SignalInterrupt => error.Retry,
        };
    }

    fn prepare(self: *Driver, sqe: *linux.io_uring_sqe, completion: *Completion) void {
        switch (completion.op) {
            .close => |fd| linux.io_uring_prep_close(sqe, fd),
            .timeout => |*op| {
                op.ts.tv_sec = @intCast(@TypeOf(op.ts.tv_sec), op.expires / std.time.ns_per_s);
                op.ts.tv_nsec = @intCast(@TypeOf(op.ts.tv_nsec), op.expires % std.time.ns_per_s);
                linux.io_uring_prep_timeout(
                    sqe, 
                    &op.ts,
                    0, 
                    linux.IORING_TIMEOUT_ABS,
                );
            },
            .read => |op| linux.io_uring_prep_read(
                sqe,
                op.fd,
                op.buffer[0..buffer_limit(op.buffer.len)],
                op.offset,
            ),
            .write => |op| linux.io_uring_prep_write(
                sqe,
                op.fd,
                op.buffer[0..buffer_limit(op.buffer.len)],
                op.offset,
            ),
            .fsync => |op| linux.io_uring_prep_fsync(sqe, op.fd, op.flags),
            .accept => |*op| linux.io_uring_prep_accept(
                sqe,
                op.socket,
                &op.address,
                &op.address_size,
                os.SOCK_CLOEXEC,
            ),
            .connect => |*op| linux.io_uring_prep_connect(
                sqe,
                op.socket,
                &op.address.any,
                &op.address.getOsSockLen(),
            ),
            .send => |op| linux.io_uring_prep_send(
                sqe,
                op.socket,
                op.buffer[0..buffer_limit(op.buffer.len)],
                os.MSG_NOSIGNAL,
            ),
            .recv => |op| linux.io_uring_prep_recv(
                sqe,
                op.socket,
                op.buffer[0..buffer_limit(op.buffer.len)],
                os.MSG_NOSIGNAL,
            ),
        }
    }
};
