

pub const IO = struct {
    afd: Afd,
    iocp: os.windows.HANDLE,
    timer: Time = .{},
    io_pending: usize = 0,
    timeouts: FIFO(Completion) = .{},
    completed: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;

        const iocp = try os.windows.CreateIoCompletionPort(os.windows.INVALID_HANDLE_VALUE, null, 0, 0);
        errdefer os.windows.CloseHandle(iocp);

        const afd = try Afd.init(iocp);
        errdefer afd.deinit();

        return IO{
            .afd = afd,
            .iocp = iocp,
        };
    }

    pub fn deinit(self: *IO) void {
        self.afd.deinit();
        self.afd = undefined;

        assert(self.iocp != os.windows.INVALID_HANDLE_VALUE);
        os.windows.CloseHandle(self.iocp);
        self.iocp = os.windows.INVALID_HANDLE_VALUE;
    }

    pub fn tick(self: *IO) !void {
        return self.flush(.non_blocking);
    }

    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        const Callback = struct {
            fn onTimeout(timed_out: *bool, completion: *Completion, result: TimeoutError!void) void {
                _ = result;
                _ = completion;
                timed_out.* = true;
            }
        };

        var timed_out = false;
        var completion: Completion = undefined;
        self.timeout(*bool, &timed_out, Callback.onTimeout, &completion, nanoseconds);

        while (!timed_out) {
            try self.flush(.blocking);
        }
    }

    const FlushMode = enum {
        blocking,
        non_blocking,
    };

    fn flush(self: *IO, mode: FlushMode) !void {
        if (self.completed.peek() == null) {
            // Compute how long to poll by flushing timeout completions.
            // NOTE: this may push to completed queue
            var timeout_ms: ?os.windows.DWORD = null;
            if (self.flush_timeouts()) |expires_ns| {
                assert(expires_ns != 0); // 0ns expires should have been completed not returned
                const expires_ms = @divCeil(expires_ns, std.time.ns_per_ms);
                assert(expires_ms != 0); // divCeil should round up to 1ms for sub-millis timeouts
                timeout_ms = std.math.cast(os.windows.DWORD, expires_ms) catch os.windows.INFINITE - 1;
            }
            
            // Poll for IO iff theres IO pending and flush_timeouts() found no ready completions
            if (self.io_pending > 0 and self.completed.peek() == null) {
                // In blocking mode, we're always waiting at least until the timeout by run_for_ns.
                // In non-blocking mode, we shouldn't wait at all.
                const io_timeout = switch (mode) {
                    .blocking => timeout_ms orelse @panic("IO.flush blocking unbounded"),
                    .non_blocking => 0,
                };

                var events: [64]Afd.Event = undefined;
                const num_events = try self.afd.poll(self.iocp, &events, io_timeout);
                
                assert(self.io_pending >= num_events);
                self.io_pending -= num_events;

                for (events[0..num_events]) |event| {
                    const afd_completion = event.asCompletion();
                    const completion = @fieldParentPtr(Completion, "afd_completion", afd_completion);
                    completion.next = null;
                    self.completed.push(completion);
                }
            }
        }

        // Dequeue and invoke all the completions currently ready.
        // Must read all `completions` before invoking the callbacks
        // as the callbacks could potentially submit more completions. 
        var completed = self.completed;
        self.completed = .{};
        while (completed.pop()) |completion| {
            (completion.callback)(self, completion);
        }
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_timeout: ?u64 = null;
        var current_time: ?u64 = null;
        var timeouts: ?*Completion = self.timeouts.peek();

        // iterate through the timeouts, returning min_timeout at the end
        while (timeouts) |completion| {
            timeouts = completion.next;

            // lazily get the current time
            const now = current_time orelse self.timer.monotonic();
            current_time = now;

            // move the completion to completed if it expired
            if (now >= completion.operation.timeout.deadline) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            // if it's still waiting, update min_timeout
            const expires = completion.operation.timeout.deadline - now;
            const min_expires = min_timeout orelse 0;
            min_timeout = std.math.min(expires, min_expires);
        }

        return min_timeout;
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion,
        afd_completion: Afd.Completion,
        context: ?*c_void,
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
        fsync: struct {
            fd: os.fd_t,
        },
        read: struct {
            fd: os.fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
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
            deadline: u64,
        },
        write: struct {
            fd: os.fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
        },
    };

    fn submit(
        self: *IO,
        context: anytype,
        callback: anytype,
        completion: *Completion,
        comptime op_tag: std.meta.Tag(Operation),
        op_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const Context = @TypeOf(context);
        const Callback = struct {
            fn onComplete(io: *IO, _completion: *Completion) void {
                // Perform the operation and get the result
                const op_data = &@field(_completion.operation, @tagName(op_tag));
                const result = OperationImpl.doOperation(op_data);

                // Schedule the socket and completion onto windows AFD when WouldBlock is detected
                if (switch (op_tag) {
                    .accept, .recv => Afd.Operation.read,
                    .connect, .send => Afd.Operation.write,
                    else => null,
                }) |afd_op| blk: {
                    _ = result catch |err| switch (err) {
                        error.WouldBlock => {
                            io.afd.schedule(
                                op_data.socket,
                                afd_op,
                                &_completion.afd_completion,
                            ) catch break :blk;
                            return;
                        },
                        else => {},
                    };
                }
                
                // The completion is finally ready to invoke the callback
                const _context = @ptrCast(Context, _completion.context);
                callback(_context, _completion, result);
            }
        };

        // Setup the completion with the callback wrapper above
        completion.* = .{
            .next = null,
            .afd_completion = undefined, 
            .context = @ptrCast(?*c_void, context),
            .callback = Callback.onComplete,
            .operation = @unionInit(Operation, @tagName(op_tag), op_data),
        };

        // Submit the completion onto the right queue
        switch (op_tag) {
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
            .{ .socket = socket },
            struct {
                fn doOperation(op: anytype) AcceptError!os.socket_t {
                    return os.accept(op.socket, null, null, 0);
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
            .{ .fd = fd },
            struct {
                fn doOperation(op: anytype) CloseError!void {
                    const socket = @ptrCast(os.socket_t, op.fd);
                    os.closeSocket(socket);
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
            },
            struct {
                fn doOperation(op: anytype) ConnectError!void {
                    return os.connect(op.socket, &op.address.any, op.address.getOsSockLen());
                }
            },
        );
    }

    pub const FsyncError = os.SyncError;

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
        fd: os.fd_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .fsync,
            .{ .fd = fd },
            struct {
                fn doOperation(op: anytype) FsyncError!void {
                    return os.fsync(op.fd);
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
                .len = buffer.len,
                .offset = offset,
            },
            struct {
                fn doOperation(op: anytype) ReadError!void {
                    return os.pread(op.fd, op.buf[0..op.len], op.offset);
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
                .len = buffer.len,
            },
            struct {
                fn doOperation(op: anytype) RecvError!void {
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
                .len = buffer.len,
            },
            struct {
                fn doOperation(op: anytype) SendError!void {
                    return os.send(op.socket, op.buf[0..op.len], 0);
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
        self.submit(
            context,
            callback,
            completion,
            .timeout,
            .{ .deadline = self.timer.monotonic() + nanoseconds },
            struct {
                fn doOperation(op: anytype) TimeoutError!void {
                    _ = op;
                    return;
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
                .len = buffer.len,
                .offset = offset,
            },
            struct {
                fn doOperation(op: anytype) WriteError!void {
                    return os.pwrite(op.fd, op.buf[0..op.len], op.offset);
                }
            },
        );
    }

    pub fn openSocket(family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        return os.socket(family, sock_type | os.SOCK_NONBLOCK | os.SOCK_CLOEXEC, protocol);
    }
};

const Afd = struct {
    const AFD_POLL_HANDLE_INFO = extern struct {
        Handle: os.windows.HANDLE,
        Events: os.windows.ULONG,
        Status: os.windows.NTSTATUS,
    };

    const AFD_POLL_INFO = extern struct {
        Timeout: os.windows.LARGE_INTEGER,
        NumberOfHandles: os.windows.ULONG,
        Exclusive: os.windows.ULONG,
        Handles: [1]AFD_POLL_HANDLE_INFO,
    };

    const IOCTL_AFD_POLL = 0x00012024;
    const AFD_POLL_RECEIVE = 0x0001;
    const AFD_POLL_SEND = 0x0004;
    const AFD_POLL_DISCONNECT = 0x0008;
    const AFD_POLL_ABORT = 0b0010;
    const AFD_POLL_LOCAL_CLOSE = 0x020;
    const AFD_POLL_ACCEPT = 0x0080;
    const AFD_POLL_CONNECT_FAIL = 0x0100;

    afd_handle: os.windows.HANDLE,

    fn open(iocp_handle: os.windows.HANDLE) !Afd {
        const name = "\\Device\\Afd\\tigerbeetle";
        var buf: [name.len]os.windows.WCHAR = undefined;
        const len = try std.unicode.utf8ToUtf16Le(&buf, name);

        const bytes_len = try math.cast(c_ushort, name_len * @sizeOf(os.windows.WCHAR));
        const afd_name = os.windows.UNICODE_STRING{
            .buffer = &buf,
            .Length = bytes_len,
            .MaximumLength = bytes_len,
        };

        var afd_attr = os.windows.OBJECT_ATTRIBUTES{
            .Length = @sizeOf(os.windows.OBJECT_ATTRIBUTES),
            .RootDirectory = null,
            .ObjectName = &afd_name,
            .Attributes = 0,
            .SecurityDescriptor = null,
            .SecurityQualityOfService = null,
        };

        var afd_handle: os.windows.HANDLE = undefined;
        var io_status_block: os.windows.IO_STATUS_BLOCK = undefined;
        switch (os.windows.ntdll.NtCreateFile(
            &afd_handle,
            os.windows.SYNCHRONIZE,
            &afd_attr,
            &io_status_block,
            null,
            0,
            os.windows.FILE_SHARE_READ | os.windows.FILE_SHARE_WRITE,
            os.windows.FILE_OPEN,
            0,
            null,
            0,
        )) {
            .SUCCESS => {},
            .OBJECT_NAME_INVALID => unreachable,
            else => |status| return os.windows.unexpectedStatus(status),
        }
        errdefer os.windows.CloseHandle(afd_handle);

        const registered_handle = try os.windows.CreateIoCompletionPort(afd_handle, iocp_handle, 1, 0);
        assert(registered_handle == iocp_handle);

        const notify_mode = os.windows.FILE_SKIP_SET_EVENT_ON_HANDLE;
        try os.windows.SetFileCompletionNotificationModes(afd_handle, notify_mode);

        return Afd{ .handle = afd_handle };
    }

    fn deinit(self: Afd) void {
        os.windows.CloseHandle(self.handle);
    }

    const Operation = enum { read, write };
    const Completion = struct {
        next: ?*Completion = null,
        afd_poll_info: AFD_POLL_INFO,
        io_status_block: os.windows.IO_STATUS_BLOCK,
    };

    fn schedule(
        self: Afd,
        socket: os.socket_t,
        completion: *Completion,
    ) !void {
        const afd_events: os.windows.ULONG = switch (readable) {
            true => AFD_POLL_RECEIVE | AFD_POLL_ACCEPT | AFD_POLL_DISCONNECT,
            else => AFD_POLL_SEND,
        };

        completion.afd_poll_info = .{
            .Timeout = std.math.maxInt(os.windows.LARGE_INTEGER),
            .NumberOfHandles = 1,
            .Exclusive = os.windows.FALSE,
            .Handles = [_]AFD_POLL_HANDLE_INFO{.{
                .Handle = @ptrCast(os.windows.HANDLE, socket),
                .Status = .SUCCESS,
                .Events = afd_events | AFD_POLL_ABORT | AFD_POLL_LOCAL_CLOSE | AFD_POLL_CONNECT_FAIL,
            }},
        };

        completion.io_status_block = .{
            .u = .{ .Status = .PENDING },
            .Information = 0,
        };

        const status = os.windows.ntdll.NtDeviceIoControlFile(
            self.handle,
            null,
            null,
            &completion.io_status_block,
            &completion.io_status_block,
            IOCTL_AFD_POLL,
            &completion.afd_poll_info,
            @sizeOf(AFD_POLL_INFO),
            &completion.afd_poll_info,
            @sizeOf(AFD_POLL_INFO),
        );

        switch (status) {
            .SUCCESS => {},
            .PENDING => {},
            .INVALID_HANDLE => unreachable,
            else => return os.windows.unexpectedStatus(status),
        }
    }

    const Event = extern struct {
        overlapped_entry: os.windows.OVERLAPPED_ENTRY,

        fn asCompletion(self: Event) ?*Completion {
            const overlapped = self.overlapped_entry.lpOverlapped;
            const io_status_block = @ptrCast(*os.windows.IO_STATUS_BLOCK, overlapped);
            return @fieldParentPtr(Completion, "io_status_block", io_status_block);
        }
    };
    
    fn poll(
        self: Afd,
        iocp_handle: os.windows.HANDLE,
        events: []Event,
        timeout_ms: os.windows.DWORD,
    ) !usize {
        return os.windows.GetQueuedCompletionStatusEx(
            iocp_handle, 
            @ptrCast([*]os.windows.OVERLAPPED_ENTRY, events.ptr)[0..events.len],
            timeout_ms,
            true, // alertable wait
        ) catch |err| switch (err) {
            error.Aborted => {},
            error.Cancelled => {},
            error.EOF => {},
            error.Timeout => {},
            else => |e| return e,
        };
    }
};