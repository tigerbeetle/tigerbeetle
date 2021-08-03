const std = @import("std");
const os = std.os;
const linux = os.linux;

const Time = @import("time.zig").Time;
const FIFO = @import("fifo.zig").FIFO;

const io = @import("io.zig");
const IO = io.IO;
const Completion = IO.Completion;
const buffer_limit = io.buffer_limit;

pub const Driver = struct {
    kq: os.fd_t,
    time: Time = .{},
    num_submitted: u32 = 0,
    max_submitted: u32 = 0,
    timeouts: FIFO(Completion) = .{}, // TODO: binary tree to avoid O(n) remove in next_expire()?
    submitted: FIFO(Completion) = .{},
    completed: FIFO(Completion) = .{},

    pub fn init(entries: u12) !Driver {
        const num_entries = std.math.max(1, std.math.ceilPowerOfTwoPromote(u12, entries));
        return Driver{
            .kq = try os.kqueue(),
            .max_submitted = num_entries,
        };
    }

    pub fn deinit(self: *Driver) void {
        os.close(self.kq);
    }

    pub fn timestamp(self: *Driver) u64 {
        return self.time.monotonic();
    }

    pub fn has_submissions(self: *Driver) bool {
        return self.submitted.peek() != null;
    }

    pub fn submit(self: *Driver, completion: *Completion) IO.SubmitError!void {
        if (self.num_submitted == self.max_submitted) return error.SubmissionQueueFull;
        self.submitted.push(completion);
        self.num_submitted += 1;
    }

    pub fn has_completions(self: *Driver) bool {
        return self.completed.peek() != null;
    }

    pub fn poll(self: *Driver) FIFO(Completion) {
        const completed = self.completed;
        self.completed = .{};
        return completed;
    }

    pub fn enter(
        self: *Driver,
        flush_submissions: bool,
        wait_for_completions: bool,
    ) IO.EnterError!void {
        var events: [256]os.Kevent = undefined;

        // A list of submissions waiting on a kevent that are pushed back to the submitted list
        // in the event that their kevent's fail to get registered. 
        var num_evented: u32 = 0;
        var evented: FIFO(Completion) = .{};
        defer if (num_evented > 0) {
            self.num_submitted += num_evented;
            evented.push_all(self.submitted);
            self.submitted = evented;
        };
        
        // Try to flush the submission list, filling up the events array 
        // with change_events for completions that need to wait on the kqueue. 
        var change_events: u32 = 0;
        if (flush_submissions and self.num_submitted > 0) {
            const now = self.timestamp();
            while (change_events < events.len) {
                const completion = self.submitted.pop() orelse break;
                self.num_submitted -= 1;
                self.process(completion, &events[change_events], now) catch {
                    change_events += 1;
                    num_evented += 1;
                    evented.push(completion);
                    continue;
                };
            }
        }

        // Get the kevent() timeout structure pointer.
        // For polling (!wait_for_completions) it points to a zeroed out os.timespec.
        var timeout_ts = std.mem.zeroes(os.timespec);
        var timeout_ptr: ?*const os.timespec = &timeout_ts;
        if (wait_for_completions) blk: {
            // No timers when waiting means that the timeout should be null to wait forever.
            if (self.timeouts.peek() == null) {
                timeout_ptr = null;
                break :blk;
            }

            // Check for timers if this call to enter() is allowed to wait.
            // Wait forever if all the timeouts expire by nulling out the timespec
            const now = self.timestamp();
            const next_exp = self.next_expire(now) orelse {
                timeout_ptr = null;
                break :blk;
            };

            // The timoeut already points to the os.timespec
            // Fill the timespec with relative wait time for the next timeout completion to expire.
            const wait_ns = next_exp - now;
            timeout_ts.tv_sec = @intCast(@TypeOf(timeout_ts.tv_sec), wait_ns / std.time.ns_per_s);
            timeout_ts.tv_nsec = @intCast(@TypeOf(timeout_ts.tv_nsec), wait_ns % std.time.ns_per_s);
        }

        const rc = os.system.kevent(
            self.kq,
            &events,
            @intCast(c_int, change_events),
            &events,
            @intCast(c_int, events.len),
            timeout_ptr,
        );

        const num_events = switch (os.errno(rc)) {
            0 => @intCast(usize, rc),
            os.EACCES => return error.InternalError, // access denied?
            os.EFAULT => unreachable, // the events memory should always be valid
            os.EBADF => return error.InternalError, // invalid kq file descriptor
            os.EINTR => return error.Retry, // signal interrupted the call
            os.EINVAL => unreachable, // time limit or event.filter is invalid
            os.ENOENT => unreachable, // only for event modification and deletion
            os.ENOMEM => return error.WaitForCompletions, // no memory for the change_events
            os.ESRCH => unreachable, // only for process attaching,
            else => |err| return os.unexpectedErrno(err),
        };

        // Change events were successfully submitted
        // So we don't need to rollback any completions that tried to wait on events.
        evented = .{};
        num_evented = 0;

        // Check for expired timers after successfully polling in case there are any.    
        if (self.timeouts.peek() != null) {
            const now = self.timestamp();
            _ = self.next_expire(now);
        }

        // Retry any completions ready from the events.
        // Those which are still not ready are added back to the submission list via evented.
        for (events[0..num_events]) |*event| {
            const completion = @intToPtr(*Completion, @intCast(usize, event.udata));
            self.processEvented(completion, event) catch {
                num_evented += 1;
                evented.push(completion);
                continue;
            };
        }
    }

    /// Iterate the timers and either invalidate them or decide the 
    /// smallest amount of time to wait on kevent() to expire one. 
    fn next_expire(self: *Driver, now: u64) ?u64 {
        var next: ?u64 = null;
        var timed = self.timeouts.peek();
        while (timed) |completion| {
            timed = completion.next;

            const expires = completion.op.timeout.expires;
            if (now > expires) {
                completion.result = -os.ETIME;
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            const current_expire = next orelse std.math.maxInt(u64);
            next = std.math.min(current_expire, expires);
        }
        return next;
    }

    /// Perform a system call, returning the result in a similar format to linux.io_uring_cqe.result
    fn syscall(self: *Driver, comptime func: anytype, args: anytype) isize {
        const rc = @call(.{}, func, args);
        return switch (os.errno(rc)) {
            0 => rc,
            else => |e| -e,
        };
    }

    /// Tries to perform the completion operation using the `now` timestamp.
    /// If the operation must wait on an event, it returns `error.Evented` and writes the event to `event`.
    /// Otherwise the completion's result is set and it is pushed to the `completed` list.
    fn process(self: *Driver, completion: *Completion, event: *os.Kevent, now: u64) error{Evented}!void {
        switch (completion.op) {
            .close => |fd| {
                completion.result = self.syscall(os.system.close, .{fd});
                self.completed.push(completion);
            },
            .timeout => |op| {
                if (now >= op.expires) {
                    completion.result = -os.ETIME;
                    self.completed.push(completion);
                } else {
                    self.timeouts.push(completion);
                }
            },
            .read => |op| {
                completion.result = self.syscall(os.system.pread, .{
                    op.fd, 
                    op.buffer.ptr,
                    buffer_limit(op.buffer.len),
                    @bitCast(i64, op.offset),
                });
                self.completed.push(completion);
            },
            .write => |op| {
                completion.result = self.syscall(os.system.pwrite, .{
                    op.fd, 
                    op.buffer.ptr,
                    buffer_limit(op.buffer.len),
                    @bitCast(i64, op.offset),
                });
            },
            .fsync => |op| {
                // Try to use F_FULLFSYNC over fsync() when possible
                //
                // https://github.com/untitaker/python-atomicwrites/issues/6
                // https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
                // https://github.com/rust-lang/rust/blob/1195bea5a7b73e079fa14b37ac7e375fc77d368a/library/std/src/sys/unix/fs.rs#L787
                completion.result = self.syscall(os.system.fcntl, .{op.fd, os.F_FULLFSYNC, @as(usize, 0)});
                if (completion.result < 0) completion.result = self.syscall(os.system.fsync, .{op.fd});
                self.completed.push(completion);
            },
            .accept, .connect, .recv, .send => {
                return self.processEvented(completion, event);
            },
        }
    }

    /// Similar to `process()` but is only used for completions with operations that could be evented.
    fn processEvented(self: *Driver, completion: *Completion, event: *os.Kevent) error{Evented}!void {
        completion.result = switch (completion.op) {
            .accept => |*op| self.syscall(os.system.accept, .{
                op.socket, 
                &op.address,
                &op.address_size,
            }),
            .connect => |*op| self.syscall(os.system.accept, .{
                op.socket, 
                &op.address.any,
                &op.address.getOsSockLen(),
            }),
            .recv => |op| self.syscall(os.system.recv, .{
                op.socket, 
                op.buffer.ptr,
                op.buffer.len,
                0,
            }),
            .send => |op| self.syscall(os.system.send, .{
                op.socket, 
                op.buffer.ptr,
                op.buffer.len,
                0,
            }),
            else => unreachable, // operation is not evented
        };

        // Complete the completion if it doesn't need to wait for an event.
        if (completion.result != -os.EAGAIN) {
            // On a valid accept call, make the socket non-blocking
            if (completion.result >= 0 and completion.op == .accept) {
                const fd = @intCast(os.fd_t, completion.result);

                // Store any errors that occur in the non-blocking process in completion.result
                completion.result = self.syscall(os.system.fcntl, .{fd, os.F_GETFL, @as(usize, 0)});
                if (completion.result >= 0) {
                    const new = @intCast(usize, completion.result) | os.O_NONBLOCK;
                    completion.result = self.syscall(os.system.fcntl, .{fd, os.F_SETFL, new});
                }

                // Make sure to restore the fd result or destroy it in case of error
                if (completion.result >= 0) {
                    completion.result = fd;
                } else {
                    os.close(fd);
                }
            }

            self.completed.push(completion);
            return;
        }
        
        // Fill the os.Kevent with a change event describing a filter
        // to wait for the socket in the operation to become either readable or writable.
        // The filter is created as ONESHOT so that it is only triggered/generated once.
        event.* = .{
            .ident = @intCast(usize, switch (completion.op) {
                .accept => |op| op.socket,
                .connect => |op| op.socket,
                .recv => |op| op.socket,
                .send => |op| op.socket,
                else => unreachable, // operation is not evented
            }),
            .filter = switch (completion.op) {
                .accept, .recv => os.EVFILT_READ,
                .connect, .send => os.EVFILT_WRITE,
                else => unreachable, // operation is not evented
            },
            .flags = os.EV_CLEAR | os.EV_ADD | os.EV_ENABLE | os.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = @ptrToInt(completion),
        };

        return error.Evented;
    }
};