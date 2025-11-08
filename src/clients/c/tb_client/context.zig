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
const MultiBatchEncoder = vsr.multi_batch.MultiBatchEncoder;

const IO = vsr.io.IO;
const TimeOS = vsr.time.TimeOS;
const message_pool = vsr.message_pool;

const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;
const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;

pub const InitParameters = extern struct {
    cluster_id: u128,
    client_id: u128,
    addresses_ptr: [*]const u8,
    addresses_len: u64,
};

/// Thread-safe client interface allocated by the user.
/// Contains the `VTable` with function pointers to the StateMachine-specific implementation
/// and the synchronization status.
/// Safe to call from multiple threads, even after `deinit` is called.
pub const ClientInterface = extern struct {
    pub const Error = error{ClientInvalid};
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

    pub fn init(self: *ClientInterface, context: *anyopaque, vtable: *const VTable) void {
        self.* = .{
            .context = .{ .ptr = context },
            .vtable = .{ .ptr = vtable },
            .locker = .{},
            .reserved = 0,
            .magic_number = 0,
        };
    }

    pub fn submit(client: *ClientInterface, packet: *Packet.Extern) Error!void {
        if (client.magic_number != beetle) return Error.ClientInvalid;
        assert(client.reserved == 0);

        client.locker.lock();
        defer client.locker.unlock();
        const context = client.context.ptr orelse return Error.ClientInvalid;
        client.vtable.ptr.submit_fn(context, packet);
    }

    pub fn completion_context(client: *ClientInterface) Error!usize {
        if (client.magic_number != beetle) return Error.ClientInvalid;
        assert(client.reserved == 0);

        client.locker.lock();
        defer client.locker.unlock();
        const context = client.context.ptr orelse return Error.ClientInvalid;
        return client.vtable.ptr.completion_context_fn(context);
    }

    pub fn deinit(client: *ClientInterface) Error!void {
        if (client.magic_number != beetle) return Error.ClientInvalid;
        assert(client.reserved == 0);

        const context: *anyopaque = context: {
            client.locker.lock();
            defer client.locker.unlock();
            if (client.context.ptr == null) return Error.ClientInvalid;

            defer client.context.ptr = null;
            break :context client.context.ptr.?;
        };
        client.vtable.ptr.deinit_fn(context);
    }

    pub fn init_parameters(client: *ClientInterface, out_parameters: *InitParameters) Error!void {
        if (client.magic_number != beetle) return Error.ClientInvalid;
        assert(client.reserved == 0);

        client.locker.lock();
        defer client.locker.unlock();
        const context = client.context.ptr orelse return Error.ClientInvalid;
        return client.vtable.ptr.init_parameters_fn(context, out_parameters);
    }

    comptime {
        assert(@sizeOf(ClientInterface) == 32);
        assert(@alignOf(ClientInterface) == 8);
    }
};

/// The function pointer called by the IO thread when a request is completed or fails.
/// The memory referenced by `result_ptr` is only valid for the duration of this callback.
/// `result_ptr` is `null` for unsuccessful requests. See `packet.status` for more details.
pub const CompletionCallback = *const fn (
    context: usize,
    packet: *Packet.Extern,
    timestamp: u64,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.c) void;

pub const InitError = std.mem.Allocator.Error || error{
    Unexpected,
    AddressInvalid,
    AddressLimitExceeded,
    SystemResources,
    NetworkSubsystemFailed,
};

/// Implements a `ClientInterface` with specialized `vsr.Client` and `StateMachine` types.
pub fn ContextType(
    comptime Client: type,
) type {
    return struct {
        const Context = @This();
        const GPA = std.heap.GeneralPurposeAllocator(.{
            .thread_safe = true,
        });

        const Operation = Client.Operation;
        const allowed_operations = [_]Operation{
            .create_accounts,
            .create_transfers,
            .lookup_accounts,
            .lookup_transfers,
            .get_account_transfers,
            .get_account_balances,
            .query_accounts,
            .query_transfers,
            .get_change_events,
        };

        const UserData = extern struct {
            self: *Context,
            packet: *Packet,

            comptime {
                assert(@sizeOf(UserData) == @sizeOf(u128));
            }
        };

        const PacketError = error{
            TooMuchData,
            ClientShutdown,
            ClientEvicted,
            ClientReleaseTooLow,
            ClientReleaseTooHigh,
            InvalidOperation,
            InvalidDataSize,
        };

        gpa: GPA,
        time_os: TimeOS,
        client_id: u128,
        cluster_id: u128,
        addresses_copy: []const u8,

        addresses: stdx.BoundedArrayType(std.net.Address, constants.replicas_max),
        io: IO,
        message_pool: MessagePool,
        client: Client,
        batch_size_limit: ?u32,

        completion_callback: CompletionCallback,
        completion_context: usize,

        interface: *ClientInterface,
        submitted: Packet.Queue,
        pending: Packet.Queue,

        signal: Signal,
        eviction_reason: ?vsr.Header.Eviction.Reason,
        thread: std.Thread,

        previous_request_instant: ?stdx.Instant = null,
        previous_request_latency: ?stdx.Duration = null,

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
            context.client_id = stdx.unique_u128();
            context.cluster_id = cluster_id;
            context.addresses_copy = try allocator.dupe(u8, addresses);
            errdefer allocator.free(context.addresses_copy);

            context.time_os = .{};
            const time = context.time_os.time();

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
                error.PortOverflow,
                => error.AddressInvalid,
            };
            assert(addresses_parsed.len > 0);
            assert(addresses_parsed.len <= constants.replicas_max);
            context.addresses.resize(addresses_parsed.len) catch unreachable;

            log.debug("{}: init: initializing IO", .{context.client_id});
            context.io = IO.init(32, 0) catch |err| {
                log.err("{}: failed to initialize IO: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.ProcessFdQuotaExceeded => error.SystemResources,
                    error.Unexpected => error.Unexpected,
                    else => unreachable,
                };
            };
            errdefer context.io.deinit();

            log.debug("{}: init: initializing MessagePool", .{context.client_id});
            context.message_pool = try MessagePool.init(allocator, .client);
            errdefer context.message_pool.deinit(allocator);

            log.debug("{}: init: initializing client (cluster_id={x:0>32}, addresses={any})", .{
                context.client_id,
                cluster_id,
                context.addresses.const_slice(),
            });
            context.client = Client.init(
                allocator,
                time,
                &context.message_pool,
                .{
                    .id = context.client_id,
                    .cluster = cluster_id,
                    .replica_count = context.addresses.count_as(u8),
                    .message_bus_options = .{
                        .configuration = context.addresses.const_slice(),
                        .io = &context.io,
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
            context.interface = client_out;
            context.submitted = Packet.Queue.init(.{
                .name = null,
                .verify_push = builtin.is_test,
            });
            context.pending = Packet.Queue.init(.{
                .name = null,
                .verify_push = builtin.is_test,
            });
            context.completion_context = completion_ctx;
            context.completion_callback = completion_callback;
            context.eviction_reason = null;

            log.debug("{}: init: initializing signal", .{context.client_id});
            try context.signal.init(&context.io, Context.signal_notify_callback);
            errdefer context.signal.deinit();

            context.batch_size_limit = null;
            context.client.register(client_register_callback, @intFromPtr(context));

            log.debug("{}: init: spawning thread", .{context.client_id});
            context.thread = std.Thread.spawn(.{}, Context.io_thread, .{context}) catch |err| {
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

        fn tick(self: *Context) void {
            if (self.eviction_reason == null) {
                self.client.tick();
            }
        }

        fn io_thread(self: *Context) void {
            while (self.signal.status() != .stopped) {
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

            self.io.cancel_all();
            self.signal.deinit();
            self.client.deinit(self.gpa.allocator());
            self.message_pool.deinit(self.gpa.allocator());
            self.io.deinit();
        }

        /// Cancel the current inflight request (and the entire batched linked list of packets),
        /// as it won't be replied anymore.
        fn cancel_request_inflight(self: *Context) void {
            if (self.client.request_inflight) |*inflight| {
                if (inflight.message.header.operation != .register) {
                    const packet: *Packet = @as(UserData, @bitCast(inflight.user_data)).packet;
                    packet.assert_phase(.sent);
                    self.packet_cancel(packet);
                }
            }
        }

        /// Calls the user callback when a packet (the entire batched linked list of packets)
        /// is canceled due to the client being either evicted or shutdown.
        fn packet_cancel(self: *Context, packet_list: *Packet) void {
            assert(packet_list.link.next == null);
            assert(packet_list.phase != .complete);
            packet_list.assert_phase(packet_list.phase);

            const result = if (self.eviction_reason) |reason| switch (reason) {
                .reserved => unreachable,
                .client_release_too_low => error.ClientReleaseTooLow,
                .client_release_too_high => error.ClientReleaseTooHigh,
                else => error.ClientEvicted,
            } else result: {
                assert(self.signal.status() != .running);
                break :result error.ClientShutdown;
            };

            var it: ?*Packet = packet_list;
            while (it) |batched| {
                if (batched != packet_list) batched.assert_phase(.batched);
                it = batched.multi_batch_next;
                self.notify_completion(batched, result);
            }
        }

        fn packet_enqueue(self: *Context, packet: *Packet) void {
            assert(self.batch_size_limit != null);
            packet.assert_phase(.submitted);

            if (self.eviction_reason != null) {
                return self.packet_cancel(packet);
            }

            const operation: Operation = operation_from_int(packet.operation) orelse {
                return self.notify_completion(packet, error.InvalidOperation);
            };

            // Make sure the packet.data wouldn't overflow a request,
            // and that the corresponding results won't overflow a reply.
            const batch: struct {
                event_size: u32,
                result_size: u32,
                event_count: u32,
                result_count_expected: u32,
            } = batch: {
                const event_size: u32 = operation.event_size();
                assert(event_size > 0);

                const result_size: u32 = operation.result_size();
                assert(result_size > 0);

                const slice: []const u8 = packet.slice();
                assert(slice.len == packet.data_size);
                maybe(slice.len == 0);
                if (slice.len % event_size != 0) {
                    return self.notify_completion(packet, error.InvalidDataSize);
                }

                const event_count: u32 = @intCast(@divExact(slice.len, event_size));
                const event_max: u32 = operation.event_max(self.batch_size_limit.?);
                if (event_count > event_max) {
                    return self.notify_completion(packet, error.TooMuchData);
                }
                const result_max: u32 = operation.result_max(self.batch_size_limit.?);
                const result_count_expected: u32 = operation.result_count_expected(slice);
                if (result_count_expected > result_max) {
                    return self.notify_completion(packet, error.TooMuchData);
                }

                break :batch .{
                    .event_size = event_size,
                    .result_size = result_size,
                    .event_count = @intCast(@divExact(slice.len, event_size)),
                    .result_count_expected = result_count_expected,
                };
            };
            assert(packet.data_size == batch.event_count * batch.event_size);
            maybe(batch.event_count == 0);
            maybe(batch.result_count_expected == 0);

            // Avoid making a packet inflight by cancelling it if the client was shutdown.
            if (self.signal.status() != .running) {
                self.packet_cancel(packet);
                return;
            }

            // Nothing inflight means the packet should be submitted right now.
            if (self.client.request_inflight == null) {
                assert(self.pending.count() == 0);
                packet.phase = .pending;
                packet.multi_batch_time_monotonic = self.client.time.monotonic().ns;
                packet.multi_batch_count = 1;
                packet.multi_batch_event_count = @intCast(batch.event_count);
                packet.multi_batch_result_count_expected = @intCast(batch.result_count_expected);
                self.packet_send(packet);
                return;
            }

            var it = self.pending.iterate();
            while (it.next()) |root| {
                root.assert_phase(.pending);

                if (root.operation != packet.operation) continue;

                // Check if the message has enough space for the submitted number of events:
                const request_size: u32 = size: {
                    const trailer_size = vsr.multi_batch.trailer_total_size(.{
                        .element_size = batch.event_size,
                        .batch_count = root.multi_batch_count + 1,
                    });
                    const event_count: u32 = batch.event_count +
                        root.multi_batch_event_count;
                    break :size (event_count * batch.event_size) + trailer_size;
                };
                if (request_size > self.batch_size_limit.?) continue;

                // Check if the reply has enough space for the maximum expected number of results:
                const reply_size_expected: u32 = size: {
                    const trailer_size = vsr.multi_batch.trailer_total_size(.{
                        .element_size = batch.result_size,
                        .batch_count = root.multi_batch_count + 1,
                    });
                    const event_count: u32 = batch.result_count_expected +
                        root.multi_batch_result_count_expected;
                    break :size (event_count * batch.result_size) + trailer_size;
                };
                if (reply_size_expected > constants.message_body_size_max) continue;

                packet.phase = .batched;
                if (root.multi_batch_next == null) {
                    assert(root.multi_batch_tail == null);
                    assert(root.multi_batch_count == 1);
                    root.multi_batch_next = packet;
                    root.multi_batch_tail = packet;
                } else {
                    assert(root.multi_batch_tail != null);
                    assert(root.multi_batch_count > 1);
                    root.multi_batch_tail.?.multi_batch_next = packet;
                    root.multi_batch_tail = packet;
                }
                root.multi_batch_count += 1;
                root.multi_batch_event_count += @intCast(batch.event_count);
                root.multi_batch_result_count_expected += @intCast(batch.result_count_expected);
                return;
            }

            // Couldn't batch with existing packet so push to pending directly.
            packet.phase = .pending;
            packet.multi_batch_time_monotonic = self.client.time.monotonic().ns;
            packet.multi_batch_count = 1;
            packet.multi_batch_event_count = @intCast(batch.event_count);
            packet.multi_batch_result_count_expected = @intCast(batch.result_count_expected);
            self.pending.push(packet);
        }

        /// Sends the packet (the entire batched linked list of packets) through the vsr client.
        /// Always called by the io thread.
        fn packet_send(self: *Context, packet_list: *Packet) void {
            assert(self.batch_size_limit != null);
            assert(self.client.request_inflight == null);
            packet_list.assert_phase(.pending);

            // On shutdown, cancel this packet as well as any others batched onto it.
            if (self.signal.status() != .running) {
                return self.packet_cancel(packet_list);
            }

            const message = self.client.get_message().build(.request);
            defer {
                self.client.release_message(message.base());
                packet_list.assert_phase(.sent);
            }

            const operation: Operation = operation_from_int(packet_list.operation).?;
            const event_size: u32 = operation.event_size();
            const request_size: u32 = request_size: {
                if (!operation.is_multi_batch()) {
                    assert(packet_list.multi_batch_next == null);
                    const source: []const u8 = packet_list.slice();
                    stdx.copy_disjoint(
                        .inexact,
                        u8,
                        message.buffer[@sizeOf(Header)..],
                        source,
                    );
                    break :request_size @intCast(source.len);
                }
                assert(operation.is_multi_batch());

                var message_encoder = MultiBatchEncoder.init(message.buffer[@sizeOf(Header)..], .{
                    .element_size = event_size,
                });

                var it: ?*Packet = packet_list;
                var multi_batch_events_count: u16 = 0;
                while (it) |batched| {
                    if (batched != packet_list) batched.assert_phase(.batched);
                    it = batched.multi_batch_next;

                    const source: []const u8 = batched.slice();
                    const target = message_encoder.writable().?;
                    assert(target.len >= source.len);
                    stdx.copy_disjoint(
                        .exact,
                        u8,
                        target[0..source.len],
                        source,
                    );
                    message_encoder.add(@intCast(source.len));

                    const events_count: u16 = @intCast(@divExact(source.len, event_size));
                    multi_batch_events_count += events_count;
                }
                assert(multi_batch_events_count == packet_list.multi_batch_event_count);
                assert(message_encoder.batch_count == packet_list.multi_batch_count);

                // Check if the reply has enough space for the maximum expected number of results.
                const result_size: u32 = operation.result_size();
                const trailer_size = vsr.multi_batch.trailer_total_size(.{
                    .element_size = result_size,
                    .batch_count = packet_list.multi_batch_count,
                });
                const reply_size_max: u32 = (result_size *
                    packet_list.multi_batch_result_count_expected) + trailer_size;
                assert(reply_size_max % result_size == 0);
                assert(reply_size_max <= constants.message_body_size_max);

                break :request_size message_encoder.finish();
            };
            assert(request_size % event_size == 0);
            assert(request_size <= self.batch_size_limit.?);

            // Sending the request.
            const previous_request_latency =
                self.previous_request_latency orelse stdx.Duration{ .ns = 0 };
            message.header.* = .{
                .release = self.client.release,
                .client = self.client.id,
                .request = 0, // Set by client.raw_request.
                .cluster = self.client.cluster,
                .command = .request,
                .operation = operation.to_vsr(),
                .size = @sizeOf(vsr.Header) + request_size,
                .previous_request_latency = @intCast(@min(
                    previous_request_latency.ns,
                    std.math.maxInt(u32),
                )),
            };

            assert((self.previous_request_instant == null) ==
                (self.previous_request_latency == null));
            self.previous_request_instant = .{ .ns = packet_list.multi_batch_time_monotonic };

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
            const self: *Context = @alignCast(@fieldParentPtr("signal", signal));
            assert(self.signal.status() != .stopped);

            // Don't send any requests until registration completes.
            if (self.batch_size_limit == null) {
                assert(self.client.request_inflight != null);
                assert(self.client.request_inflight.?.message.header.operation == .register);
                return;
            }

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
            const self: *Context = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(self.client.request_inflight == null);
            assert(self.batch_size_limit == null);
            assert(result.batch_size_limit > 0);

            // The client might have a smaller message size limit.
            maybe(constants.message_body_size_max < result.batch_size_limit);
            self.batch_size_limit = @min(result.batch_size_limit, constants.message_body_size_max);

            // Some requests may have queued up while the client was registering.
            signal_notify_callback(&self.signal);
        }

        fn client_eviction_callback(client: *Client, eviction: *const Message.Eviction) void {
            const self: *Context = @fieldParentPtr("client", client);
            assert(self.eviction_reason == null);

            log.debug("{}: client_eviction_callback: reason={?s} reason_int={}", .{
                self.client_id,
                std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                @intFromEnum(eviction.header.reason),
            });

            // Now that the client is evicted, no more requests can be submitted to it and we can
            // safely deinitialize it. First, we stop the IO thread, which then deinitializes the
            // client before it exits (see `io_thread`).
            self.eviction_reason = eviction.header.reason;
            self.signal.stop();
        }

        fn client_result_callback(
            raw_user_data: u128,
            operation_vsr: vsr.Operation,
            timestamp: u64,
            reply: []const u8,
        ) void {
            const user_data: UserData = @bitCast(raw_user_data);
            const self: *Context = user_data.self;
            const packet_list: *Packet = user_data.packet;
            const operation = operation_vsr.cast(Client.Operation);
            assert(packet_list.operation == @intFromEnum(operation));
            assert(timestamp > 0);
            packet_list.assert_phase(.sent);

            const current_timestamp = self.client.time.monotonic();
            self.previous_request_latency =
                current_timestamp.duration_since(self.previous_request_instant.?);

            // Submit the next pending packet (if any) now that VSR has completed this one.
            assert(self.client.request_inflight == null);
            while (self.pending.pop()) |packet_next| {
                self.packet_send(packet_next);
                if (self.client.request_inflight != null) break;
            }

            // The callback should never be called with an operation not in `allowed_operations`.
            // This also guards from sending an unsupported operation.
            assert(operation_from_int(@intFromEnum(operation)) != null);

            if (!operation.is_multi_batch()) {
                assert(packet_list.multi_batch_next == null);
                return self.notify_completion(packet_list, .{
                    .timestamp = timestamp,
                    .reply = reply,
                });
            }
            assert(operation.is_multi_batch());

            const result_size: u32 = operation.result_size();
            assert(result_size > 0);
            var reply_decoder = MultiBatchDecoder.init(reply, .{
                .element_size = result_size,
            }) catch unreachable;
            assert(packet_list.multi_batch_count == reply_decoder.batch_count());

            // Copying it because `packet` is no longer valid after the callback.
            const multi_batch_results_expected: u16 =
                packet_list.multi_batch_result_count_expected;
            var multi_batch_results_actual: u16 = 0;
            var it: ?*Packet = packet_list;
            while (it) |batched| {
                if (batched != packet_list) batched.assert_phase(.batched);
                assert(batched.operation == @intFromEnum(operation));

                // NB: The reference to `batched` isn't valid after `notify_completion`.
                it = batched.multi_batch_next;

                const batched_reply: []const u8 = reply_decoder.pop().?;
                multi_batch_results_actual += @intCast(@divExact(
                    batched_reply.len,
                    result_size,
                ));
                self.notify_completion(batched, .{
                    .timestamp = timestamp,
                    .reply = batched_reply,
                });
            }
            assert(reply_decoder.pop() == null);
            assert(multi_batch_results_actual <= multi_batch_results_expected);
        }

        fn notify_completion(
            self: *Context,
            packet: *Packet,
            completion: PacketError!struct {
                timestamp: u64,
                reply: []const u8,
            },
        ) void {
            const result = completion catch |err| {
                packet.status = switch (err) {
                    error.TooMuchData => .too_much_data,
                    error.ClientEvicted => .client_evicted,
                    error.ClientReleaseTooLow => .client_release_too_low,
                    error.ClientReleaseTooHigh => .client_release_too_high,
                    error.ClientShutdown => .client_shutdown,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                assert(packet.status != .ok);
                packet.phase = .complete;

                // The packet completed with an error.
                (self.completion_callback)(
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
            (self.completion_callback)(
                self.completion_context,
                packet.cast(),
                result.timestamp,
                result.reply.ptr,
                @intCast(result.reply.len),
            );
        }

        // VTable functions called by `ClientInterface`, which are thread-safe.

        fn vtable_submit_fn(context: *anyopaque, packet_extern: *Packet.Extern) void {
            const self: *Context = @ptrCast(@alignCast(context));

            // Packet is caller-allocated to enable elastic intrusive-link-list-based
            // memory management. However, some of Packet's fields are essentially private.
            // Initialize them here to avoid threading default fields through FFI boundary.
            const packet: *Packet = packet_extern.cast();
            packet.* = .{
                .user_data = packet_extern.user_data,
                .operation = packet_extern.operation,
                .data_size = packet_extern.data_size,
                .data = packet_extern.data,
                .user_tag = packet_extern.user_tag,
                .status = .ok,
                .link = .{},
                .multi_batch_time_monotonic = 0,
                .multi_batch_next = null,
                .multi_batch_tail = null,
                .multi_batch_count = 0,
                .multi_batch_event_count = 0,
                .multi_batch_result_count_expected = 0,
                .phase = .submitted,
            };

            if (self.eviction_reason == null) {
                // Enqueue the packet and notify the IO thread to process it asynchronously.
                assert(self.signal.status() == .running);
                self.submitted.push(packet);
                self.signal.notify();
            } else {
                // Cancel the packet since we stop the IO thread during eviction.
                assert(self.signal.status() != .running);
                self.packet_cancel(packet);
            }
        }

        fn vtable_completion_context_fn(context: *anyopaque) usize {
            const self: *Context = @ptrCast(@alignCast(context));
            return self.completion_context;
        }

        fn vtable_deinit_fn(context: *anyopaque) void {
            const self: *Context = @ptrCast(@alignCast(context));

            self.signal.stop();
            self.thread.join();

            assert(self.submitted.pop() == null);
            assert(self.pending.pop() == null);

            self.gpa.allocator().free(self.addresses_copy);

            // NB: Copy the allocator back out before trying to destroy `self` with it!
            var gpa: GPA = self.gpa;
            gpa.allocator().destroy(self);
            assert(gpa.deinit() == .ok);
        }

        fn vtable_init_parameters_fn(context: *anyopaque, out_parameters: *InitParameters) void {
            const self: *Context = @ptrCast(@alignCast(context));
            assert(self.signal.status() == .running);

            out_parameters.cluster_id = self.cluster_id;
            out_parameters.client_id = self.client_id;
            out_parameters.addresses_ptr = self.addresses_copy.ptr;
            out_parameters.addresses_len = self.addresses_copy.len;
        }

        fn operation_from_int(op: u8) ?Operation {
            inline for (allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return operation;
                }
            }
            return null;
        }
    };
}

/// Implements the `Mutex` API as an `extern` struct, based on `std.Thread.Futex`.
/// Vendored from `std.Thread.Mutex.FutexImpl`.
const Locker = extern struct {
    const Futex = std.Thread.Futex;
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
