const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const log = std.log.scoped(.tb_client_context);

const vsr = @import("../tb_client.zig").vsr;

const constants = vsr.constants;
const stdx = vsr.stdx;
const maybe = stdx.maybe;
const Header = vsr.Header;

const MultiBatchDecoder = vsr.multi_batch.MultiBatchDecoder;

const IO = vsr.io.IO;
const TimeOS = stdx.TimeOS;
const message_pool = vsr.message_pool;

const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;
const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;

const KiB = stdx.KiB;

const io_thread_stack_size = 512 * KiB;

pub const InitParameters = extern struct {
    cluster_id: u128,
    client_id: u128,
    addresses_ptr: [*]const u8,
    addresses_len: u64,
};

pub const InitError = std.mem.Allocator.Error || error{
    /// Invalid IP addresses, or malformed string.
    AddressInvalid,
    /// Too many IP addresses.
    AddressLimitExceeded,
    /// Insuficient systems resources to initialize the client.
    SystemResources,
    /// Network failure.
    NetworkSubsystemFailed,
    Unexpected,
};

pub const ClientError = error{
    /// The client was closed.
    Closed,
    /// Client interface not initialized.
    /// This is a bug in the application code.
    NotInitialized,
};

/// Completion errors encompass any kind of failure that
/// can prevent a submitted batch from completing.
/// For example, validation failures (`Packet.Error`), client shutdown,
/// and cluster eviction.
pub const CompletionError = Packet.Error || error{
    /// The client was closed.
    Closed,
    /// The client session was evicted by the cluster,
    /// usually due to too many clients being connected.
    Evicted,
    /// Client release is too low.
    ReleaseTooLow,
    /// Client release is too high.
    ReleaseTooHigh,
};

/// Thread-safe client interface allocated by the user.
/// Contains the `VTable` with function pointers to the StateMachine-specific implementation
/// and the synchronization status.
/// Safe to call from multiple threads, even after `deinit` is called.
pub const ClientInterface = extern struct {
    pub const VTable = struct {
        submit_fn: *const fn (*anyopaque, *Packet.Extern) void,
        completion_context_fn: *const fn (*anyopaque) usize,
        deinit_fn: *const fn (*anyopaque) void,
        init_parameters_fn: *const fn (*anyopaque, *InitParameters) void,
    };

    /// Magic number used as a tag, preventing the use of uninitialized pointers.
    const beetle: u64 = 0xBEE71E;

    // Since the client interface is an intrusive struct allocated by the user,
    // it is exported as an opaque `[_]u64` array.
    // An `extern union` is used to ensure a platform-independent size for pointer fields,
    // avoiding the need for different versions of `tb_client.h` on 32-bit targets.

    context: extern union {
        ptr: ?*anyopaque,
        int_ptr: u64,
    },
    vtable: extern union {
        ptr: *const VTable,
        int_ptr: u64,
    },
    locker: Locker,
    reserved: u32,
    magic_number: u64,

    pub fn init(interface: *ClientInterface, context: *anyopaque, vtable: *const VTable) void {
        interface.* = .{
            .context = .{ .ptr = context },
            .vtable = .{ .ptr = vtable },
            .locker = .{},
            .reserved = 0,
            .magic_number = 0,
        };
    }

    pub fn submit(interface: *ClientInterface, packet: *Packet.Extern) ClientError!void {
        if (interface.magic_number != beetle) return ClientError.NotInitialized;
        assert(interface.reserved == 0);

        interface.locker.lock();
        defer interface.locker.unlock();

        const context: *anyopaque = interface.context.ptr orelse return ClientError.Closed;
        interface.vtable.ptr.submit_fn(context, packet);
    }

    pub fn completion_context(interface: *ClientInterface) ClientError!usize {
        if (interface.magic_number != beetle) return ClientError.NotInitialized;
        assert(interface.reserved == 0);

        interface.locker.lock();
        defer interface.locker.unlock();

        const context: *anyopaque = interface.context.ptr orelse return ClientError.Closed;
        return interface.vtable.ptr.completion_context_fn(context);
    }

    pub fn deinit(interface: *ClientInterface) ClientError!void {
        if (interface.magic_number != beetle) return ClientError.NotInitialized;
        assert(interface.reserved == 0);

        const context: *anyopaque = context: {
            interface.locker.lock();
            defer interface.locker.unlock();

            const context: *anyopaque = interface.context.ptr orelse return ClientError.Closed;
            interface.context = .{ .ptr = null };

            break :context context;
        };

        interface.vtable.ptr.deinit_fn(context);
    }

    pub fn init_parameters(
        interface: *ClientInterface,
        out_parameters: *InitParameters,
    ) ClientError!void {
        if (interface.magic_number != beetle) return ClientError.NotInitialized;
        assert(interface.reserved == 0);

        interface.locker.lock();
        defer interface.locker.unlock();

        const context: *anyopaque = interface.context.ptr orelse return ClientError.Closed;
        return interface.vtable.ptr.init_parameters_fn(context, out_parameters);
    }

    comptime {
        assert(@sizeOf(ClientInterface) == 32);
        assert(@alignOf(ClientInterface) == 8);
    }
};

/// The function pointer called by the IO thread when a request is completed or fails.
/// The memory referenced by `result` is only valid for the duration of this callback.
/// `result_ptr` is `null` for unsuccessful requests. See `packet.status` for more details.
pub const CompletionCallback = *const fn (
    context: usize,
    packet: *Packet.Extern,
    timestamp: u64,
    result: ?[*]const u8,
    result_size: u32,
) callconv(.c) void;

/// Implements a `ClientInterface` with specialized `vsr.Client` and
/// `StateMachine.Operation` types.
pub fn ContextType(
    comptime Client: type,
    comptime operations_allowed: []const Client.Operation,
) type {
    return struct {
        /// Thread-local variable to track whether the current thread is
        /// the IO thread or a user thread.
        /// Used to assert that certain functions are only called from the
        /// correct thread.
        threadlocal var thread_caller: union(enum) {
            user,
            io: std.Thread.Id,
        } = .user;

        gpa: GPA,
        time_os: TimeOS = .{},
        client_id: u128,
        cluster_id: u128,
        addresses_owned: []const u8,

        addresses: stdx.BoundedArrayType(stdx.SocketAddress, constants.replicas_max) = .{},
        io: IO,
        message_pool: MessagePool,
        client: Client,
        batch_size_limit: ?u32,
        eviction_reason: ?vsr.Header.Eviction.Reason,

        completion_callback: CompletionCallback,
        completion_context: usize,

        interface: *ClientInterface,
        submitted: Packet.Queue,
        pending: Packet.Queue,

        signal: Signal,
        thread: std.Thread,

        request_timer: stdx.Instant,
        request_latency: ?stdx.Duration,

        const Context = @This();
        const GPA = std.heap.DebugAllocator(.{
            .thread_safe = true,
        });

        const Operation = Client.Operation;

        const UserData = extern struct {
            self: *Context,
            packet: *Packet,

            comptime {
                assert(@sizeOf(UserData) == @sizeOf(u128));
            }
        };

        pub fn init(
            root_allocator: std.mem.Allocator,
            client_out: *ClientInterface,
            cluster_id: u128,
            addresses: []const u8,
            completion_ctx: usize,
            completion_callback: CompletionCallback,
        ) InitError!void {
            var context: *Context = context: {
                // Wrap the root allocator - usually heap.c_allocator when built as a library - in
                // a GPA to keep maximum compatibility while gaining the extra safety. As a library,
                // libtbclient is running inside another process's address space.
                var gpa = GPA{
                    .backing_allocator = root_allocator,
                };
                errdefer assert(gpa.deinit() == .ok);

                const context = try gpa.allocator().create(Context);

                // Moving the GPA is safe, since we don't have any live reference to `allocator`.
                context.gpa = gpa;

                break :context context;
            };

            errdefer {
                var gpa: GPA = context.gpa;
                gpa.allocator().destroy(context);
                assert(gpa.deinit() == .ok);
            }

            const allocator = context.gpa.allocator();

            context.* = .{
                .gpa = context.gpa,

                .client_id = stdx.crypto_u128(std.Io.Threaded.global_single_threaded.io()),
                .cluster_id = cluster_id,

                .completion_callback = completion_callback,
                .completion_context = completion_ctx,

                .interface = client_out,
                .submitted = Packet.Queue.init(.{
                    .name = null,
                    .verify_push = builtin.is_test,
                }),
                .pending = Packet.Queue.init(.{
                    .name = null,
                    .verify_push = builtin.is_test,
                }),

                .addresses_owned = undefined,
                .io = undefined,
                .message_pool = undefined,
                .client = undefined,
                .batch_size_limit = null,
                .eviction_reason = null,
                .signal = undefined,
                .thread = undefined,
                .request_timer = undefined,
                .request_latency = null,
            };
            context.addresses_owned = try allocator.dupe(u8, addresses);
            errdefer allocator.free(context.addresses_owned);

            const time = context.time_os.interface();

            log.debug("{}: init: parsing vsr addresses: {s}", .{ context.client_id, addresses });
            context.addresses = .{};
            const addresses_parsed = vsr.parse_addresses(
                addresses,
                context.addresses.unused_capacity_slice(),
            ) catch |err| return switch (err) {
                error.AddressLimitExceeded => error.AddressLimitExceeded,
                error.AddressHasMoreThanOneColon,
                error.AddressHasTrailingComma,
                error.AddressInvalid,
                error.PortInvalid,
                => error.AddressInvalid,
            };
            assert(addresses_parsed.len > 0);
            assert(addresses_parsed.len <= constants.replicas_max);
            context.addresses.resize(addresses_parsed.len) catch unreachable;

            log.debug("{}: init: initializing IO", .{context.client_id});
            context.io = IO.init(std.Io.Threaded.global_single_threaded.io(), 32, 0) catch |err| {
                log.err("{}: failed to initialize IO: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.ProcessFdQuotaExceeded,
                    error.SystemFdQuotaExceeded,
                    error.SystemResources,
                    => error.SystemResources,
                    error.PermissionDenied,
                    error.SystemOutdated,
                    => error.NetworkSubsystemFailed,
                    error.Unexpected => error.Unexpected,
                };
            };
            errdefer context.io.deinit();

            log.debug("{}: init: initializing MessagePool", .{context.client_id});
            context.message_pool = try MessagePool.init(allocator, .client);
            errdefer context.message_pool.deinit(allocator);

            log.debug("{}: init: initializing client (cluster_id={x:0>32}, addresses={f})", .{
                context.client_id,
                cluster_id,
                vsr.format_addresses(context.addresses.const_slice()),
            });
            context.client = Client.init(
                allocator,
                time,
                &context.message_pool,
                .{
                    .id = context.client_id,
                    .cluster = cluster_id,
                    .replica_count = context.addresses.count_as(u8),
                    .aof_recovery = false,
                    .message_bus_options = .{
                        .configuration = context.addresses.const_slice(),
                        .io = &context.io,
                        .trace = null,
                        .time = time,
                    },
                    .eviction_callback = client_eviction_callback,
                },
            ) catch |err| {
                log.err("{}: failed to initialize Client: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                };
            };
            errdefer context.client.deinit(allocator);

            ClientInterface.init(client_out, context, comptime &.{
                .submit_fn = &vtable_submit_fn,
                .completion_context_fn = &vtable_completion_context_fn,
                .deinit_fn = &vtable_deinit_fn,
                .init_parameters_fn = &vtable_init_parameters_fn,
            });

            log.debug("{}: init: initializing signal", .{context.client_id});
            try context.signal.init(&context.io, Context.signal_notify_callback);
            errdefer context.signal.deinit();

            context.request_timer = context.client.time.monotonic();
            context.client.register(client_register_callback, @intFromPtr(context));

            log.debug("{}: init: spawning thread", .{context.client_id});
            context.thread = std.Thread.spawn(
                .{ .stack_size = io_thread_stack_size },
                Context.io_thread,
                .{context},
            ) catch |err| {
                log.err("{}: failed to spawn thread: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.Unexpected => error.Unexpected,
                    error.OutOfMemory => error.OutOfMemory,
                    error.SystemResources,
                    error.ThreadQuotaExceeded,
                    error.LockedMemoryLimitExceeded,
                    => error.SystemResources,
                };
            };

            // Setting `magic_number` tags the interface as initialized.
            // Writing it at the end so that if `init` fails part-way through and the
            // user doesn’t handle the error before using it, we'll still be able to validate.
            client_out.magic_number = ClientInterface.beetle;
        }

        fn deinit(self: *Context) void {
            assert(thread_caller == .user);
            assert(self.signal.status() == .shutdown_completed);
            assert(self.submitted.pop() == null);
            assert(self.pending.pop() == null);
            assert(self.client.shutdown_complete());
            maybe(self.eviction_reason != null);

            self.signal.deinit();
            self.client.deinit(self.gpa.allocator());
            self.message_pool.deinit(self.gpa.allocator());
            self.io.deinit();

            self.gpa.allocator().free(self.addresses_owned);

            // NB: Copy the allocator back out before trying to destroy `self` with it!
            var gpa: GPA = self.gpa;
            gpa.allocator().destroy(self);
            assert(gpa.deinit() == .ok);
        }

        fn tick(self: *Context) void {
            if (self.client.evicted) {
                assert(self.eviction_reason != null);
                return;
            }

            assert(self.eviction_reason == null);
            self.client.tick();
        }

        fn io_thread(self: *Context) void {
            // Initializing the flag as the IO thread.
            assert(thread_caller == .user);
            thread_caller = .{ .io = std.Thread.getCurrentId() };
            defer thread_caller = .user;

            while (self.signal.status() != .shutdown_completed) {
                self.tick();
                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed: {s}", .{
                        self.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }

            self.cancel_request_inflight();

            while (self.pending.pop()) |packet| {
                packet.assert_phase(.pending);
                self.packet_cancel(packet);
            }

            // The submitted queue is no longer accessible to user threads,
            // so synchronization is not required here.
            while (self.submitted.pop()) |packet| {
                packet.assert_phase(.submitted);
                self.packet_cancel(packet);
            }

            // Close every connection and drain outstanding IO before tearing the
            // client down.
            self.client.shutdown();
            while (!self.client.shutdown_complete()) {
                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed during shutdown: {s}", .{
                        self.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }
        }

        /// Cancel the current inflight request (and the entire batched linked list of packets),
        /// as it won't be replied anymore.
        fn cancel_request_inflight(self: *Context) void {
            assert(thread_caller == .io);
            if (self.client.request_inflight) |inflight| {
                const operation = inflight.message.header.operation;

                self.client.request_inflight = null;
                self.client.release_message(inflight.message.base());

                if (operation != .register) {
                    const packet: *Packet = @as(UserData, @bitCast(inflight.user_data)).packet;
                    packet.assert_phase(.sent);
                    self.packet_cancel(packet);
                }
            }
        }

        /// Calls the user callback when a packet (the entire batched linked list of packets)
        /// is canceled due to the client being either evicted or shutdown.
        fn packet_cancel(self: *Context, packet_list: *Packet) void {
            assert(thread_caller == .io);
            assert(packet_list.link.next == null);
            assert(packet_list.phase != .complete);
            packet_list.assert_phase(packet_list.phase);

            const result: CompletionError = result: {
                // When the client is explicitly closed by the user, submitted batches
                // are canceled with `Closed` regardless of any previous eviction reason.
                if (self.signal.status() != .running) {
                    maybe(self.eviction_reason != null);

                    break :result CompletionError.Closed;
                }
                assert(self.eviction_reason != null);

                // While eviction reasons are very detailed, the surfaced error codes
                // are limited to those that are the client's responsibility.
                break :result switch (self.eviction_reason.?) {
                    .reserved => unreachable,

                    // Client evicted due to an invalid session.
                    // The application has no option other than to try reconnecting.
                    .no_session,
                    .session_too_low,
                    .session_release_mismatch,
                    => CompletionError.Evicted,

                    // Invalid client release.
                    // The application should resolve the version mismatch.
                    .client_release_too_low => CompletionError.ReleaseTooLow,
                    .client_release_too_high => CompletionError.ReleaseTooHigh,

                    // Invalid operation or malformed request.
                    // Language clients and applications using the `tb_client`
                    // library directly should never encounter these eviction
                    // reasons (it would indicate a bug in `vsr.Client`).
                    // However, network messages could be corrupted, so these
                    // reasons are grouped as `Evicted`.
                    .invalid_request_operation,
                    .invalid_request_body,
                    .invalid_request_body_size,
                    => CompletionError.Evicted,
                };
            };

            var it: ?*Packet = packet_list;
            while (it) |batched| {
                if (batched != packet_list) batched.assert_phase(.batched);
                it = batched.multi_batch_next;
                self.notify_completion(batched, result);
            }
        }

        fn packet_enqueue(self: *Context, packet: *Packet) void {
            assert(thread_caller == .io);
            packet.assert_phase(.submitted);
            maybe(self.batch_size_limit == null);

            // Nothing inflight means the packet should be submitted right now.
            if (self.client.request_inflight == null) {
                assert(self.pending.empty());

                if (self.batch_size_limit == null) {
                    // Evicted during registration.
                    assert(self.client.evicted);
                    assert(self.eviction_reason != null);
                    return self.packet_cancel(packet);
                }

                // The client might have been evicted, but we don't return early,
                // so that batch validation errors are surfaced first.
                maybe(self.eviction_reason != null);

                const batch = packet.batch_validate(
                    Operation,
                    operations_allowed,
                    .{
                        .batch_size_limit = self.batch_size_limit.?,
                    },
                ) catch |err| {
                    return self.notify_completion(packet, err);
                };

                packet.phase = .pending;
                packet.multi_batch_time_monotonic = self.client.time.monotonic().ns;
                packet.multi_batch_count = 1;
                packet.multi_batch_event_count = @intCast(batch.event_count);
                packet.multi_batch_result_count_expected = @intCast(batch.result_count_expected);
                return self.packet_send(packet);
            }
            assert(self.client.request_inflight != null);
            assert(self.batch_size_limit != null);
            // Upon eviction, `request_inflight` is cleaned up.
            assert(self.eviction_reason == null);
            maybe(self.pending.empty());

            packet.batch_enqueue(
                Operation,
                operations_allowed,
                .{
                    .target = &self.pending,
                    .batch_size_limit = self.batch_size_limit.?,
                    .time = self.client.time,
                },
            ) catch |err| {
                return self.notify_completion(packet, err);
            };
        }

        /// Sends the packet (the entire batched linked list of packets) through the vsr client.
        /// Always called by the io thread.
        fn packet_send(self: *Context, packet_list: *Packet) void {
            assert(thread_caller == .io);
            assert(self.batch_size_limit != null);
            assert(self.client.request_inflight == null);
            packet_list.assert_phase(.pending);

            // Avoid making a packet inflight by cancelling it
            // if the client was closed or evicted.
            if (self.signal.status() != .running or self.eviction_reason != null) {
                return self.packet_cancel(packet_list);
            }
            assert(self.eviction_reason == null);

            const message = self.client.get_message().build(.request);
            defer {
                self.client.release_message(message.base());
                packet_list.assert_phase(.sent);
            }

            const batch = packet_list.batch_write(
                Operation,
                operations_allowed,
                .{
                    .output_buffer = message.buffer[@sizeOf(Header)..],
                    .batch_size_limit = self.batch_size_limit.?,
                },
            );

            // Sending the request.
            const previous_request_latency =
                self.request_latency orelse stdx.Duration{ .ns = 0 };
            message.header.* = .{
                .release = self.client.release,
                .client = self.client.id,
                .request = 0, // Set by client.raw_request.
                .cluster = self.client.cluster,
                .command = .request,
                .operation = batch.operation.to_vsr(),
                .size = @sizeOf(vsr.Header) + batch.request_size,
                .previous_request_latency = @intCast(@min(
                    previous_request_latency.to_us(),
                    std.math.maxInt(u32),
                )),
            };

            self.request_timer = .{ .ns = packet_list.multi_batch_time_monotonic };

            packet_list.phase = .sent;
            self.client.raw_request(
                Context.client_result_callback,
                @bitCast(UserData{
                    .self = self,
                    .packet = packet_list,
                }),
                message.ref(),
            );
            assert(message.header.request != 0);
        }

        fn signal_notify_callback(signal: *Signal) void {
            assert(thread_caller == .io);

            const self: *Context = @alignCast(@fieldParentPtr("signal", signal));
            switch (self.signal.status()) {
                .running => if (self.batch_size_limit == null) {
                    if (self.client.request_inflight) |request_inflight| {
                        // Don't send any requests until registration completes.
                        assert(request_inflight.message.header.operation == .register);
                        assert(!self.client.evicted);
                        assert(self.eviction_reason == null);
                        return;
                    }

                    // Evicted during registration (e.g., `client_release_too_{low,high}`).
                    // N.B. Don't assert the exact eviction reason here to avoid coupling
                    // too tightly with the cluster logic.
                    assert(self.client.request_inflight == null);
                    assert(self.client.evicted);
                    assert(self.eviction_reason != null);
                },
                // Shutdown flushes pending requests.
                .shutdown_completed, .shutdown_requested => return,
            }
            maybe(self.batch_size_limit == null);

            // Prevents IO thread starvation under heavy client load.
            // Process only the minimal number of packets for the next pending request.
            const enqueued_count = self.pending.count();
            const safety_limit = 8 * 1024; // Avoid unbounded loop in case of invalid packets.
            for (0..safety_limit) |_| {
                const packet: *Packet = pop: {
                    self.interface.locker.lock();
                    defer self.interface.locker.unlock();

                    break :pop self.submitted.pop() orelse return;
                };
                self.packet_enqueue(packet);

                // Packets can be processed without increasing `pending.count`:
                // - If the packet is invalid.
                // - If there's no in-flight request, the packet is sent immediately without
                //   using the pending queue.
                // - If the packet can be batched with another previously enqueued packet.
                if (self.pending.count() > enqueued_count) break;
            }

            // Defer this work to later,
            // allowing the IO thread to remain free for processing completions.
            const empty: bool = empty: {
                self.interface.locker.lock();
                defer self.interface.locker.unlock();

                break :empty self.submitted.empty();
            };
            if (!empty) {
                self.signal.notify();
            }
        }

        fn client_register_callback(user_data: u128, result: *const vsr.RegisterResult) void {
            assert(thread_caller == .io);

            const self: *Context = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(self.client.request_inflight == null);
            assert(self.batch_size_limit == null);
            assert(result.batch_size_limit > 0);

            const current_timestamp = self.client.time.monotonic();
            self.request_latency =
                self.request_timer.until(current_timestamp);

            // The client might have a smaller message size limit.
            maybe(constants.message_body_size_max < result.batch_size_limit);
            self.batch_size_limit = @min(result.batch_size_limit, constants.message_body_size_max);

            // Some requests may have queued up while the client was registering.
            signal_notify_callback(&self.signal);
        }

        fn client_eviction_callback(client: *Client, eviction: *const Message.Eviction) void {
            assert(thread_caller == .io);

            assert(eviction.header.command == .eviction);
            assert(eviction.header.reason != .reserved);

            const self: *Context = @fieldParentPtr("client", client);
            assert(self.eviction_reason == null);
            assert(self.client.evicted);
            self.eviction_reason = eviction.header.reason;

            log.debug("{}: client_eviction_callback: reason={?s} reason_int={}", .{
                self.client_id,
                std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                @intFromEnum(eviction.header.reason),
            });

            self.cancel_request_inflight();
            signal_notify_callback(&self.signal);
        }

        fn client_result_callback(
            raw_user_data: u128,
            operation_vsr: vsr.Operation,
            timestamp: u64,
            reply: []align(constants.cache_line_size) const u8,
        ) void {
            assert(thread_caller == .io);

            const user_data: UserData = @bitCast(raw_user_data);
            const self: *Context = user_data.self;
            const packet_list: *Packet = user_data.packet;
            const operation = operation_vsr.cast(Client.Operation);
            assert(self.eviction_reason == null);
            assert(packet_list.operation == @intFromEnum(operation));
            assert(timestamp > 0);
            packet_list.assert_phase(.sent);

            const current_timestamp = self.client.time.monotonic();
            self.request_latency =
                self.request_timer.until(current_timestamp);

            // Submit the next pending packet (if any) now that VSR has completed this one.
            assert(self.client.request_inflight == null);
            while (self.pending.pop()) |packet_next| {
                self.packet_send(packet_next);
                if (self.client.request_inflight != null) break;
            }

            const batch = packet_list.batch_validate(
                Operation,
                operations_allowed,
                .{
                    .batch_size_limit = self.batch_size_limit.?,
                },
            ) catch unreachable; // The callback should never be called with an invalid packet.
            assert(batch.result_size > 0);

            if (!batch.operation.is_multi_batch()) {
                assert(packet_list.multi_batch_next == null);
                assert(reply.len % batch.result_size == 0);
                return self.notify_completion(packet_list, .{
                    .timestamp = timestamp,
                    .reply = reply,
                });
            }
            assert(batch.operation.is_multi_batch());

            var reply_decoder = MultiBatchDecoder.init(reply, .{
                .element_size = batch.result_size,
            }) catch unreachable;
            assert(packet_list.multi_batch_count == reply_decoder.batch_count());

            // Copying it because `packet` is no longer valid after the callback.
            const multi_batch_result_count_expected: u32 =
                packet_list.multi_batch_result_count_expected;

            var multi_batch_results_actual: u16 = 0;
            var it: ?*Packet = packet_list;
            while (it) |packet_next| {
                if (packet_next != packet_list) packet_next.assert_phase(.batched);
                assert(packet_next.operation == @intFromEnum(batch.operation));

                // NB: The reference to `packet` isn't valid after `notify_completion`.
                it = packet_next.multi_batch_next;

                const batched_reply: []const u8 = reply_decoder.pop().?;
                multi_batch_results_actual += @intCast(@divExact(
                    batched_reply.len,
                    batch.result_size,
                ));
                self.notify_completion(packet_next, .{
                    .timestamp = timestamp,
                    .reply = batched_reply,
                });
            }
            assert(reply_decoder.pop() == null);
            assert(multi_batch_results_actual <= multi_batch_result_count_expected);
        }

        fn notify_completion(
            self: *Context,
            packet: *Packet,
            completion: CompletionError!struct {
                timestamp: u64,
                reply: []const u8,
            },
        ) void {
            assert(thread_caller == .io);

            const result = completion catch |err| {
                packet.status = switch (err) {
                    CompletionError.Closed => .client_closed,
                    CompletionError.Evicted => .client_evicted,
                    CompletionError.ReleaseTooLow => .client_release_too_low,
                    CompletionError.ReleaseTooHigh => .client_release_too_high,
                    CompletionError.InvalidOperation => .invalid_operation,
                    CompletionError.InvalidDataSize => .invalid_data_size,
                    CompletionError.TooMuchData => .too_much_data,
                };
                assert(packet.status != .ok);
                packet.phase = .complete;

                // The packet completed with an error.
                self.completion_callback(
                    self.completion_context,
                    packet.cast(),
                    0,
                    null,
                    0,
                );
                return;
            };

            // The packet completed normally.
            assert(packet.status == .ok);
            packet.phase = .complete;
            self.completion_callback(
                self.completion_context,
                packet.cast(),
                result.timestamp,
                result.reply.ptr,
                @intCast(result.reply.len),
            );
        }

        // VTable functions called by `ClientInterface`, which are thread-safe.

        fn vtable_submit_fn(context: *anyopaque, packet_extern: *Packet.Extern) void {
            assert(thread_caller == .user);

            const self: *Context = @ptrCast(@alignCast(context));

            // Packet is caller-allocated to enable elastic intrusive-link-list-based
            // memory management. However, some of Packet's fields are essentially private.
            // Initialize them here to avoid threading default fields through FFI boundary.
            const packet: *Packet = packet_extern.cast();
            packet.* = .init(packet_extern);

            // Enqueue the packet and notify the IO thread to process it asynchronously.
            // The mutex is locked during this operation, so it's guaranteed
            // that the I/O thread hasn't been canceled.
            assert(self.signal.status() == .running);
            self.submitted.push(packet);
            self.signal.notify();
        }

        fn vtable_completion_context_fn(context: *anyopaque) usize {
            const self: *Context = @ptrCast(@alignCast(context));
            return self.completion_context;
        }

        fn vtable_deinit_fn(context: *anyopaque) void {
            assert(thread_caller == .user);

            const self: *Context = @ptrCast(@alignCast(context));
            self.signal.stop();
            self.thread.join();
            self.deinit();
        }

        fn vtable_init_parameters_fn(context: *anyopaque, out_parameters: *InitParameters) void {
            assert(thread_caller == .user);

            const self: *Context = @ptrCast(@alignCast(context));
            assert(self.signal.status() == .running);

            out_parameters.cluster_id = self.cluster_id;
            out_parameters.client_id = self.client_id;
            out_parameters.addresses_ptr = self.addresses_owned.ptr;
            out_parameters.addresses_len = self.addresses_owned.len;
        }
    };
}

/// Implements the `Mutex` API as an `extern` struct, based on the futex operations of `std.Io`.
/// Adapted from Zig 0.14's `std.Thread.Mutex.FutexImpl`.
const Locker = extern struct {
    const Futex = struct {
        const io = std.Io.Threaded.global_single_threaded.io();

        fn wait(ptr: *const std.atomic.Value(u32), expect: u32) void {
            io.futexWaitUncancelable(u32, &ptr.raw, expect);
        }

        fn wake(ptr: *const std.atomic.Value(u32), max_waiters: u32) void {
            io.futexWake(u32, &ptr.raw, max_waiters);
        }
    };
    const unlocked: u32 = 0b00;
    const locked: u32 = 0b01;
    const contended: u32 = 0b11; // Must contain the `locked` bit for x86 optimization below.

    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(unlocked),

    fn lock(self: *Locker) void {
        if (!self.try_lock()) {
            self.lock_slow();
        }
    }

    fn try_lock(self: *Locker) bool {
        // On x86, use `lock bts` instead of `lock cmpxchg` as:
        // - they both seem to mark the cache-line as modified regardless: https://stackoverflow.com/a/63350048.
        // - `lock bts` is smaller instruction-wise which makes it better for inlining.
        if (comptime builtin.target.cpu.arch.isX86()) {
            const locked_bit = @ctz(locked);
            return self.state.bitSet(locked_bit, .acquire) == 0;
        }

        // Acquire barrier ensures grabbing the lock happens before the critical section
        // and that the previous lock holder's critical section happens before we grab the lock.
        return self.state.cmpxchgWeak(unlocked, locked, .acquire, .monotonic) == null;
    }

    fn lock_slow(self: *Locker) void {
        @branchHint(.cold);

        // Avoid doing an atomic swap below if we already know the state is contended.
        // An atomic swap unconditionally stores which marks the cache-line as modified
        // unnecessarily.
        if (self.state.load(.monotonic) == contended) {
            Futex.wait(&self.state, contended);
        }

        // Try to acquire the lock while also telling the existing lock holder that there are
        // threads waiting.
        //
        // Once we sleep on the Futex, we must acquire the mutex using `contended` rather than
        // `locked`.
        // If not, threads sleeping on the Futex wouldn't see the state change in unlock and
        // potentially deadlock.
        // The downside is that the last mutex unlocker will see `contended` and do an unnecessary
        // Futex wake but this is better than having to wake all waiting threads on mutex unlock.
        //
        // Acquire barrier ensures grabbing the lock happens before the critical section
        // and that the previous lock holder's critical section happens before we grab the lock.
        while (self.state.swap(contended, .acquire) != unlocked) {
            Futex.wait(&self.state, contended);
        }
    }

    fn unlock(self: *Locker) void {
        // Unlock the mutex and wake up a waiting thread if any.
        //
        // A waiting thread will acquire with `contended` instead of `locked`
        // which ensures that it wakes up another thread on the next unlock().
        //
        // Release barrier ensures the critical section happens before we let go of the lock
        // and that our critical section happens before the next lock holder grabs the lock.
        const state = self.state.swap(unlocked, .release);
        assert(state != unlocked);

        if (state == contended) {
            Futex.wake(&self.state, 1);
        }
    }
};

const testing = std.testing;

test "Locker: smoke test" {
    var locker = Locker{};

    try testing.expect(locker.try_lock());
    try testing.expect(!locker.try_lock());
    locker.unlock();

    locker.lock();
    try testing.expect(!locker.try_lock());
    locker.unlock();
}

test "Locker: contended" {
    const threads_count = 4;
    const increments = 1000;

    const State = struct {
        locker: Locker = .{},
        counter: u32 = 0,
    };

    const Runner = struct {
        thread: std.Thread = undefined,
        state: *State,
        fn run(self: *@This()) void {
            while (true) {
                self.state.locker.lock();
                defer self.state.locker.unlock();

                if (self.state.counter == increments) break;
                self.state.counter += 1;
            }
        }
    };

    var state = State{};
    var runners: [threads_count]Runner = undefined;
    for (&runners) |*runner| {
        runner.* = .{ .state = &state };
        runner.thread = try std.Thread.spawn(.{}, Runner.run, .{runner});
    }
    for (&runners) |*runner| {
        runner.thread.join();
    }

    try testing.expectEqual(state.counter, increments);
}
