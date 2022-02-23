const std = @import("std");
const builtin = @import("builtin");

const os = std.os;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

const config = @import("../config.zig");
const log = std.log.scoped(.tb_client);

const vsr = @import("../vsr.zig");
const Header = vsr.Header;

const IO = @import("../io.zig").IO;
const MessagePool = @import("../message_pool.zig");

pub fn ClientThread(
    comptime StateMachine: type,
    comptime MessageBus: type,
) type {
    return struct {
        pub const Client = vsr.Client(StateMachine, MessageBus);
        pub const Operation = StateMachine.Operation;

        const allowed_operations = [_]Operation{
            .create_accounts,
            .create_transfers,
            .commit_transfers,
            .lookup_accounts,
            .lookup_transfers,
        };

        fn operation_size_of(op: Operation) usize {
            inline for (allowed_operations) |operation| {
                if (op == operation) {
                    return @sizeOf(StateMachine.Event(operation));
                }
            }
            unreachable;
        }

        fn operation_name_of(comptime op: Operation) []const u8 {
            inline for (std.meta.fields(Operation)) |op_field| {
                if (op == @field(Operation, op_field.name)) {
                    return op_field.name;
                }
            }
            unreachable;
        }

        fn OperationUnion(comptime FieldType: anytype) type {
            comptime var fields: [allowed_operations.len]std.builtin.TypeInfo.UnionField = undefined; 
            inline for (fields) |*field, index| {
                const op = allowed_operations[index];
                field.name = operation_name_of(op);
                field.field_type = FieldType(op);
                field.alignment = @alignOf(field.field_type);
            }

            return @Type(std.builtin.TypeInfo{
                .Union = .{
                    .layout = .Extern,
                    .tag_type = null,
                    .fields = &fields,
                    .decls = &.{},
                },
            });
        }

        pub const Event = OperationUnion(StateMachine.Event);
        pub const Result = OperationUnion(StateMachine.Result);

        pub const Packet = extern struct {
            next: ?*Packet,
            user_data: usize,
            operation: Operation,
            data: extern union {
                request: Event,
                response: Result,
            },

            pub const List = extern struct {
                head: ?*Packet = null,
                tail: ?*Packet = null,

                pub fn from(packet: *Packet) List {
                    packet.next = null;
                    return List{
                        .head = packet,
                        .tail = packet,
                    };
                }

                pub fn push(self: *List, list: List) void {
                    const prev = if (self.tail) |tail| &tail.next else &self.head;
                    prev.* = list.head orelse return;
                    self.tail = list.tail orelse unreachable;
                }

                pub fn peek(self: List) ?*Packet {
                    return self.head orelse {
                        assert(self.tail == null);
                        return null;
                    };
                }

                pub fn pop(self: *List) ?*Packet {
                    const packet = self.head orelse return null;
                    self.head = packet.next;
                    if (self.head == null) self.tail = null;
                    return packet;
                }
            };

            const Stack = struct {
                pushed: Atomic(?*Packet) = Atomic(?*Packet).init(null),
                popped: ?*Packet = null,

                fn push(self: *Stack, list: List) void {
                    const head = list.head orelse return;
                    const tail = list.tail orelse unreachable;

                    var pushed = self.pushed.load(.Monotonic);
                    while (true) {
                        tail.next = pushed;
                        pushed = self.pushed.tryCompareAndSwap(
                            pushed,
                            head,
                            .Release,
                            .Monotonic,
                        ) orelse break;
                    }
                }

                fn pop(self: *Stack) ?*Packet {
                    if (self.popped == null) self.popped = self.pushed.swap(null, .Acquire);
                    const packet = self.popped orelse return null;
                    self.popped = packet.next;
                    return packet;
                }
            };
        };

        /////////////////////////////////////////////////////////////////////////

        const Self = @This();

        allocator: std.mem.Allocator,
        client_id: u128,
        packets: []Packet,

        addresses: []std.net.Address,
        io: IO,
        message_bus: MessageBus,
        client: Client,
        
        retry: Packet.List,
        submitted: Packet.Stack,
        available_messages: usize, 
        on_completion_fn: fn (*Self, Packet.List) callconv(.C) void,

        signal: Signal,
        thread: std.Thread,

        pub const Error = error{
            Unexpected,
            OutOfMemory,
            InvalidAddress,
            SystemResources,
            NetworkSubsystemFailed,
        };

        pub fn init(
            self: *Self,
            allocator: std.mem.Allocator,
            cluster_id: u32, 
            addresses: []const u8, 
            num_packets: u32,
            on_completion_fn: fn (*Self, Packet.List) callconv(.C) void,
        ) Error!void {
            self.allocator = allocator;
            self.client_id = std.crypto.random.int(u128);
            log.debug("init: initializing client_id={}.", .{self.client_id});

            log.debug("init: allocating tb_packets.", .{});
            self.packets = self.allocator.alloc(Packet, num_packets) catch |err| {
                log.err("failed to allocate tb_packets: {}", .{err});
                return Error.OutOfMemory;
            };
            errdefer self.allocator.free(self.packets);

            log.debug("init: parsing vsr addresses.", .{});
            self.addresses = vsr.parse_addresses(self.allocator, addresses) catch |err| {
                log.err("failed to parse addresses: {}.", .{err});
                return Error.InvalidAddress;
            };
            errdefer self.allocator.free(self.addresses);

            log.debug("init: initializing IO.", .{});
            self.io = IO.init(32, 0) catch |err| {
                log.err("failed to initialize IO: {}.", .{err});
                return switch (err) {
                    error.ProcessFdQuotaExceeded => error.SystemResources,
                    error.Unexpected => error.Unexpected,
                    else => unreachable,
                };
            }; 
            errdefer self.io.deinit();

            log.debug("init: initializing MessageBus.", .{});
            self.message_bus = MessageBus.init(
                self.allocator,
                cluster_id,
                self.addresses,
                self.client_id,
                &self.io,
            ) catch |err| {
                log.err("failed to initialize message bus: {}.", .{err});
                return err;
            };
            errdefer self.message_bus.deinit();

            log.debug("init: Initializing client(cluster_id={d}, client_id={d}, addresses={o})", .{
                cluster_id,
                self.client_id,
                self.addresses,
            });
            self.client = Client.init(
                allocator,
                self.client_id,
                cluster_id,
                @intCast(u8, self.addresses.len),
                &self.message_bus,
            ) catch |err| {
                log.err("failed to initalize client: {}", .{err});
                return err;
            };
            errdefer self.client.deinit();

            self.retry = .{};
            self.submitted = .{};
            self.available_messages = MessagePool.messages_max_client;
            self.on_completion_fn = on_completion_fn;

            log.debug("init: initializing Signal.", .{});
            self.signal.init(&self.io, Self.on_signal) catch |err| {
                log.err("failed to initialize Signal: {}.", .{err});
                return err;
            };
            errdefer self.signal.deinit();

            log.debug("init: spawning Context thread.", .{});
            self.thread = std.Thread.spawn(.{}, Self.run, .{self}) catch |err| {
                log.err("failed to spawn context thread: {}.", .{err});
                return switch (err) {
                    error.Unexpected => error.Unexpected,
                    error.OutOfMemory => error.OutOfMemory,
                    error.SystemResources,
                    error.ThreadQuotaExceeded,
                    error.LockedMemoryLimitExceeded => error.SystemResources,
                };
            };
        }
        
        pub fn deinit(self: *Self) void {
            self.signal.shutdown();
            self.thread.join();
            self.signal.deinit();

            self.client.deinit();
            self.message_bus.deinit();
            self.io.deinit();

            self.allocator.free(self.addresses);
            self.allocator.free(self.packets);
            self.* = undefined;
        }

        pub fn submit(self: *Self, list: Packet.List) void {
            if (list.peek() == null) return;
            self.submitted.push(list);
            self.signal.notify();
        }
        
        fn run(self: *Self) void {
            while (!self.signal.is_shutdown()) {
                self.client.tick();
                self.io.run_for_ns(config.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("IO.run() failed with {}", .{err});
                    return;
                };
            }
        }

        fn on_signal(signal: *Signal) void {
            const self = @fieldParentPtr(Self, "signal", signal);
            self.client.tick();

            // Consume all of retry here to avoid infinite loop
            // if the code below pushes to self.retry while we're dequeueing.
            var pending = self.retry;
            self.retry = .{};

            // The loop below can exit early without processing all of pending
            // if available_messages becomes zero.
            // In such a case we need to restore self.retry we consumed above
            // with those that weren't processed.
            defer {
                pending.push(self.retry);
                self.retry = pending;
            }

            // Process packets from either pending or submitted as long as we have messages.
            while (self.available_messages > 0) {
                const packet = pending.pop() orelse self.submitted.pop() orelse break;
                const message = self.client.get_message();
                self.available_messages -= 1;

                const bytes = operation_size_of(packet.operation);
                const request = @ptrCast([*]const u8, &packet.data.request)[0..bytes];

                const writable = message.buffer[@sizeOf(Header)..];
                assert(writable.len <= config.message_size_max);

                std.mem.copy(u8, writable, request);
                self.client.request(
                    @bitCast(u128, UserData{
                        .self = self,
                        .packet = packet,
                    }),
                    Self.on_result,
                    packet.operation,
                    message,
                    request.len,
                );
            }
        }

        const UserData = packed struct {
            self: *Self,
            packet: *Packet,
        };

        fn on_result(raw_user_data: u128, op: Operation, results: Client.Error![]const u8) void {
            const user_data = @bitCast(UserData, raw_user_data);
            const self = user_data.self;
            const packet = user_data.packet;

            assert(self.available_messages < MessagePool.messages_max_client);
            self.available_messages += 1;

            const readable = results catch |e| switch (e) {
                error.TooManyOutstandingRequests => {
                    self.retry.push(Packet.List.from(packet));
                    return;
                },
            };

            const bytes = operation_size_of(op);
            const response = @ptrCast([*]u8, &packet.data.response)[0..bytes];
            std.mem.copy(u8, response, readable[0..bytes]);

            var completed = Packet.List.from(packet);
            self.on_completion_fn(self, completed);
        }
    };
}

/// A Signal is a way to trigger a registered callback on a tigerbeetle IO instace 
/// when notification occurs from another thread.
/// It does this by using OS sockets (which are thread safe) 
/// to resolve IO.Completions on the tigerbeetle thread.
const Signal = struct {
    io: *IO,
    server_socket: os.socket_t,
    accept_socket: os.socket_t,
    connect_socket: os.socket_t,

    completion: IO.Completion,
    recv_buffer: [1]u8,
    send_buffer: [1]u8,
    
    on_signal_fn: fn (*Signal) void,
    state: Atomic(enum(u8) {
        running,
        waiting,
        notified,
        shutdown,
    }),

    pub fn init(self: *Signal, io: *IO, on_signal_fn: fn (*Signal) void) !void {
        self.io = io;
        self.server_socket = os.socket(
            os.AF.INET, 
            os.SOCK.STREAM | os.SOCK.NONBLOCK, 
            os.IPPROTO.TCP,
        ) catch |err| {
            log.err("failed to create signal server socket: {}", .{err});
            return switch (err) {
                error.PermissionDenied,
                error.ProtocolNotSupported,
                error.SocketTypeNotSupported,
                error.AddressFamilyNotSupported,
                error.ProtocolFamilyNotAvailable => error.NetworkSubsystemFailed,
                error.ProcessFdQuotaExceeded,
                error.SystemFdQuotaExceeded,
                error.SystemResources => error.SystemResources,
                error.Unexpected => error.Unexpected,
            };
        };
        errdefer os.closeSocket(self.server_socket);

        os.listen(self.server_socket, 1) catch |err| {
            log.err("failed to listen on signal server socket: {}", .{err});
            return switch (err) {
                error.AddressInUse => unreachable,
                error.FileDescriptorNotASocket => unreachable,
                error.AlreadyConnected => unreachable,
                error.SocketNotBound => unreachable,
                error.OperationNotSupported,
                error.NetworkSubsystemFailed => error.NetworkSubsystemFailed,
                error.SystemResources => error.SystemResources,
                error.Unexpected => error.Unexpected,
            };
        };

        var addr = std.net.Address.initIp4(undefined, undefined);
        var addr_len = addr.getOsSockLen();
        os.getsockname(self.server_socket, &addr.any, &addr_len) catch |err| {
            log.err("failed to get address of signal server socket: {}", .{err});
            return switch (err) {
                error.SocketNotBound => unreachable,
                error.FileDescriptorNotASocket => unreachable,
                error.SystemResources => error.SystemResources,
                error.NetworkSubsystemFailed => error.NetworkSubsystemFailed,
                error.Unexpected => error.Unexpected,
            };
        };

        self.connect_socket = self.io.open_socket(
            os.AF.INET,
            os.SOCK.STREAM,
            os.IPPROTO.TCP,
        ) catch |err| {
            log.err("failed to create signal connect socket: {}", .{err});
            return error.Unexpected;
        };
        errdefer os.closeSocket(self.connect_socket);

        // Tracks when the connect_socket connects to the server_socket
        const DoConnect = struct {
            result: IO.ConnectError!void = undefined,
            completion: IO.Completion = undefined,
            is_connected: bool = false,
            
            fn on_connect(
                do_connect: *@This(),
                _completion: *IO.Completion,
                result: IO.ConnectError!void,
            ) void {
                assert(&do_connect.completion == _completion);
                assert(!do_connect.is_connected);
                do_connect.is_connected = true;
                do_connect.result = result;
            }
        };

        var do_connect = DoConnect{};
        self.io.connect(
            *DoConnect,
            &do_connect,
            DoConnect.on_connect,
            &do_connect.completion,
            self.connect_socket,
            addr,
        );
        
        // Wait for the connect_socket to connect to the server_socket.
        self.accept_socket = IO.INVALID_SOCKET;
        while (!do_connect.is_connected) {
            self.io.tick() catch |err| {
                log.err("failed to tick IO when setting up signal: {}", .{err});
                return error.Unexpected;
            };

            // Try to accept the connection from the connect_socket as the accept_socket
            if (self.accept_socket == IO.INVALID_SOCKET) {
                self.accept_socket = os.accept(self.server_socket, null, null, 0) catch |e| switch (e) {
                    error.WouldBlock => continue,
                    error.ConnectionAborted => unreachable,
                    error.ConnectionResetByPeer => unreachable,
                    error.FileDescriptorNotASocket => unreachable,
                    error.ProcessFdQuotaExceeded,
                    error.SystemFdQuotaExceeded,
                    error.SystemResources => return error.SystemResources,
                    error.SocketNotListening => unreachable,
                    error.BlockedByFirewall => unreachable,
                    error.ProtocolFailure,
                    error.OperationNotSupported,
                    error.NetworkSubsystemFailed => return error.NetworkSubsystemFailed,
                    error.Unexpected => return error.Unexpected,
                };
            }
        }

        _ = do_connect.result catch |err| {
            log.err("failed to connect on signal client socket: {}", .{err});
            return switch (err) {
                error.FileNotFound => unreachable,
                error.SystemResources => error.SystemResources,
                error.WouldBlock => unreachable,
                error.AddressInUse => unreachable,
                error.AddressFamilyNotSupported => unreachable,
                error.AddressNotAvailable,
                error.PermissionDenied,
                error.ConnectionTimedOut,
                error.ConnectionRefused,
                error.ConnectionResetByPeer => error.Unexpected,
                error.NetworkUnreachable => error.NetworkSubsystemFailed,
                error.ConnectionPending => unreachable,
                error.FileDescriptorNotASocket => unreachable,
                error.Unexpected => error.Unexpected,
            };
        };

        assert(do_connect.is_connected);
        assert(self.accept_socket != IO.INVALID_SOCKET);
        assert(self.connect_socket != IO.INVALID_SOCKET);

        self.completion = undefined;
        self.recv_buffer = undefined;
        self.send_buffer = undefined;
        
        self.state = @TypeOf(self.state).init(.running);
        self.on_signal_fn = on_signal_fn;
        self.wait();
    }

    pub fn deinit(self: *Signal) void {
        os.closeSocket(self.server_socket);
        os.closeSocket(self.accept_socket);
        os.closeSocket(self.connect_socket);
    }

    /// Schedules the on_signal callback to be invoked on the IO thread.
    /// Safe to call from multiple threads.
    pub fn notify(self: *Signal) void {
        if (self.state.swap(.notified, .Release) == .waiting) {
            self.wake();
        }
    }

    /// Stops the signal from firing on_signal callbacks on the IO thread.
    /// Safe to call from multiple threads.
    pub fn shutdown(self: *Signal) void {
        if (self.state.swap(.shutdown, .Release) == .waiting) {
            self.wake();
        }
    }

    /// Returns true if the Signal was marked disabled and should no longer fire on_signal callbacks.
    /// Safe to call from multiple threads.
    pub fn is_shutdown(self: *const Signal) bool {
        return self.state.load(.Acquire) == .shutdown;
    }

    fn wake(self: *Signal) void {
        assert(self.accept_socket != IO.INVALID_SOCKET);
        self.send_buffer[0] = 0;

        // TODO: use os.send() instead when it gets fixed for windows
        if (builtin.target.os.tag != .windows) {
            _ = os.send(self.accept_socket, &self.send_buffer, 0) catch unreachable;
            return;
        }

        const buf: []const u8 = &self.send_buffer;
        const rc = os.windows.sendto(self.accept_socket, buf.ptr, buf.len, 0, null, 0);
        assert(rc != os.windows.ws2_32.SOCKET_ERROR);
    }

    fn wait(self: *Signal) void {
        const state = self.state.compareAndSwap(
            .running,
            .waiting,
            .Acquire,
            .Acquire,
        ) orelse return self.io.recv(
            *Signal,
            self,
            on_recv,
            &self.completion,
            self.connect_socket,
            &self.recv_buffer,
        );

        switch (state) {
            .running => unreachable, // Not possible due to CAS semantics.
            .waiting => unreachable, // We should be the only ones who could've started waiting.
            .notified => {}, // A thread woke us up before we started waiting so reschedule below.
            .shutdown => return, // A thread shut down the signal before we started waiting.
        }

        self.io.timeout(
            *Signal,
            self,
            on_timeout,
            &self.completion,
            0, // zero-timeout functions as a yield
        );
    }

    fn on_recv(
        self: *Signal,
        completion: *IO.Completion,
        result: IO.RecvError!usize,
    ) void {
        assert(completion == &self.completion);
        _ = result catch |err| std.debug.panic("Signal recv error: {}", .{err});
        self.on_signal();
    }

    fn on_timeout(
        self: *Signal,
        completion: *IO.Completion,
        result: IO.TimeoutError!void,
    ) void {
        assert(completion == &self.completion);
        _ = result catch |err| std.debug.panic("Signal timeout error: {}", .{err});
        self.on_signal();
    }

    fn on_signal(self: *Signal) void {
        const state = self.state.compareAndSwap(
            .notified,
            .running,
            .Acquire,
            .Acquire,
        ) orelse {
            (self.on_signal_fn)(self);
            return self.wait();
        };

        switch (state) {
            .running => unreachable, // Multiple racing calls to on_signal().
            .waiting => unreachable, // on_signal() called without transitioning to a waking state.
            .notified => unreachable, // Not possible due to CAS semantics.
            .shutdown => return, // A thread shut down the signal before we started running.
        }
    }
};
