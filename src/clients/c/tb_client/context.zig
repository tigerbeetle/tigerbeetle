//! tb_client context: the bridge between user threads and the VSR client.
//!
//! A tb_client instance runs the VSR replication client on a dedicated IO thread,
//! exposing a thread-safe API to user code. The architecture has three layers:
//!
//! 1. ClientInterface - a fixed-layout extern struct allocated by the caller (the
//!    language binding). It holds a Locker (futex-based mutex), a vtable, and a
//!    context pointer. All user-facing operations (submit, deinit, etc.) go through
//!    this struct's methods, which acquire the lock and dispatch through the vtable.
//!    The fixed ABI layout lets C, Rust, Java, etc. embed it without knowing the
//!    internal types.
//!
//! 2. Shared - the fields that both user threads and the IO thread touch. The vtable
//!    trampolines receive `*Shared` (not the full IoThread), restricting what client
//!    threads can access. Contains the submitted-packet queue (lock-protected),
//!    the Signal (cross-thread wakeup via eventfd), the join handle, and identity
//!    fields (client_id, cluster_id, addresses).
//!
//! 3. IoThread - the IO-thread-private state. Owns the IO event loop, the GPA,
//!    the eviction flag, and the phase state machine. The phase union
//!    (registering/running/disconnecting/settled) carries all phase-specific data
//!    so that fields only exist in phases that use them. The VSR Client and
//!    MessagePool live in a heap-allocated ClientState, threaded only through
//!    the phases that use them.
//!
//! ## Locking
//!
//! The only lock is the Locker inside ClientInterface. It serializes
//! user-thread operations, protects the submitted-packet queue, and serializes
//! the context-pointer null that gates all cross-thread access. The IO thread
//! acquires it only to pop from the submitted queue or to check if it is empty.
//! All other IO-thread state is single-threaded. The Signal provides the
//! cross-thread wakeup: user threads call signal.notify() after pushing a packet,
//! which triggers an eventfd write that breaks the IO thread out of run_for_ns.
//! The Signal is thread-safe via its own atomics; the Locker does not protect it
//! directly, but by serializing the context-pointer null it ensures no new
//! signal.notify() calls occur after shutdown begins.
//!
//! ## Shutdown
//!
//! The user calls ClientInterface.deinit, which nulls the context pointer
//! (rejecting further submissions) and calls signal.stop(). The IO thread detects
//! shutdown and breaks out of the main loop. On eviction, the server sends
//! an eviction message; the callback nulls the context pointer and sets
//! eviction_reason. The IO thread self-stops the signal since no user-initiated
//! deinit will arrive.
//!
//! ## Eviction
//!
//! A rare but subtle case. The server evicts a client by removing
//! its session and replying with an eviction message instead of the normal
//! result. Subsequent requests from the evicted client receive fresh eviction
//! responses.
//!
//! On the client side, the eviction callback fires once during run_for_ns.
//! It nulls the context pointer (rejecting new submissions), sets
//! eviction_reason, and yields. The main loop sees eviction_reason, enters
//! the disconnecting phase (which cancels inflight, pending, and submitted packets
//! with the eviction error), then settles. Subsequent requests from the client thread
//! immediately return ClientInvalid. Because deinit checks the context
//! pointer (already null), it returns ClientInvalid. The IO thread self-stops
//! the signal and exits on its own.
//!
//! There are multiple footguns to be aware of here:
//!
//! 1. Callers must handle ClientInvalid in all cases.
//! 2. It is possible to be silently evicted during registration.
//!    Subsequent calls, including the dtor, will return ClientInvalid.
//!    Caller never knows they were evicted.
//! 3. It is possible that multiple requests get evicted packet statuses,
//!    because all enqueued requests are purged on eviction.
//! 3. After eviction the tb_client_deinit function will report failure
//!    (though it has already shutdown and freed resources).
//!    It is not possible to successfully destruct after eviction.
//!
//! ## File organization
//!
//! This file is organized top down and is intended to read
//! intuitively following the parallel-concurrent data flow.
//! Types are used for data hiding to prevent maintenence errors
//! on this historically difficult code.
//!
//! - ClientInterface               - user-facing ABI types
//! - Shared, vtable trampolines    - cross-thread shared state and dispatch
//! - IoThreadType                  - the isolated IO thread and its callbacks
//! - PhaseType                     - the state machine phases and their isolated methods
//! - Locker                        - the shared locking mechanism

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

const KiB = stdx.KiB;

const io_thread_stack_size = 512 * KiB;

pub const InitParameters = extern struct {
    cluster_id: u128,
    client_id: u128,
    addresses_ptr: [*]const u8,
    addresses_len: u64,
};

/// Fixed-layout handle allocated by the caller / language binding.
///
/// This is the public API surface exposed through the C ABI. Its layout is
/// fixed at 32 bytes / 8-byte so that C headers and FFI bindings can
/// embed it without knowing internal types. The `context` pointer targets the
/// Shared struct; it is nulled on deinit or eviction to atomically reject
/// further submissions. All methods acquire the Locker before touching state.
///
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

    pub fn init(interface: *ClientInterface, context: *anyopaque, vtable: *const VTable) void {
        interface.* = .{
            .context = .{ .ptr = context },
            .vtable = .{ .ptr = vtable },
            .locker = .{},
            .reserved = 0,
            .magic_number = 0,
        };
    }

    pub fn submit(interface: *ClientInterface, packet: *Packet.Extern) Error!void {
        if (interface.magic_number != beetle) return Error.ClientInvalid;
        assert(interface.reserved == 0);

        interface.locker.lock();
        defer interface.locker.unlock();

        const context = interface.context.ptr orelse return Error.ClientInvalid;
        interface.vtable.ptr.submit_fn(context, packet);
    }

    pub fn completion_context(interface: *ClientInterface) Error!usize {
        if (interface.magic_number != beetle) return Error.ClientInvalid;
        assert(interface.reserved == 0);

        interface.locker.lock();
        defer interface.locker.unlock();

        const context = interface.context.ptr orelse return Error.ClientInvalid;
        return interface.vtable.ptr.completion_context_fn(context);
    }

    pub fn deinit(interface: *ClientInterface) Error!void {
        if (interface.magic_number != beetle) return Error.ClientInvalid;
        assert(interface.reserved == 0);

        const context: *anyopaque = context: {
            interface.locker.lock();
            defer interface.locker.unlock();

            const context = interface.context.ptr orelse return Error.ClientInvalid;
            interface.context = .{ .ptr = null };

            break :context context;
        };
        interface.vtable.ptr.deinit_fn(context);
    }

    pub fn init_parameters(
        interface: *ClientInterface,
        out_parameters: *InitParameters,
    ) Error!void {
        if (interface.magic_number != beetle) return Error.ClientInvalid;
        assert(interface.reserved == 0);

        interface.locker.lock();
        defer interface.locker.unlock();

        const context = interface.context.ptr orelse return Error.ClientInvalid;
        return interface.vtable.ptr.init_parameters_fn(context, out_parameters);
    }

    comptime {
        assert(@sizeOf(ClientInterface) == 32);
        assert(@alignOf(ClientInterface) == 8);
    }
};

/// Fields accessed by both the IO thread and client threads.
/// Vtable functions receive `*Shared` (not the full IO-thread context),
/// limiting client thread access to only these fields.
const Shared = struct {
    signal: Signal,
    submitted: Packet.Queue,
    completion_context: usize,
    thread: std.Thread,
    cluster_id: u128,
    client_id: u128,
    addresses_owned: []const u8,
};

// VTable trampolines: called by ClientInterface methods on user threads.
// Each receives `*Shared` via the context pointer and operates
// under the ClientInterface Locker.

fn vtable_submit_fn(context: *anyopaque, packet_extern: *Packet.Extern) void {
    const shared: *Shared = @ptrCast(@alignCast(context));

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

    // Enqueue the packet and notify the IO thread to process it asynchronously.
    assert(shared.signal.status() == .running);
    shared.submitted.push(packet);
    shared.signal.notify();
}

fn vtable_completion_context_fn(context: *anyopaque) usize {
    const shared: *Shared = @ptrCast(@alignCast(context));
    return shared.completion_context;
}

fn vtable_deinit_fn(context: *anyopaque) void {
    const shared: *Shared = @ptrCast(@alignCast(context));

    // Copy the thread handle here, since stopping the I/O thread deinitializes
    // the context and invalidates the `shared` pointer.
    const thread = shared.thread;
    defer thread.join();

    shared.signal.stop();
}

fn vtable_init_parameters_fn(context: *anyopaque, out_parameters: *InitParameters) void {
    const shared: *Shared = @ptrCast(@alignCast(context));
    assert(shared.signal.status() == .running);

    out_parameters.cluster_id = shared.cluster_id;
    out_parameters.client_id = shared.client_id;
    out_parameters.addresses_ptr = shared.addresses_owned.ptr;
    out_parameters.addresses_len = shared.addresses_owned.len;
}

/// Called by the IO thread when a request completes or fails.
///
/// `result` points to the reply body and is only valid for the duration of
/// this callback. It is null for unsuccessful requests (see `packet.status`).
pub const CompletionCallback = *const fn (
    context: usize,
    packet: *Packet.Extern,
    timestamp: u64,
    result: ?[*]const u8,
    result_size: u32,
) callconv(.c) void;

pub const InitError = std.mem.Allocator.Error || error{
    Unexpected,
    AddressInvalid,
    AddressLimitExceeded,
    SystemResources,
    NetworkSubsystemFailed,
};

/// IO-thread-private state backing a ClientInterface, generic over the VSR Client type.
///
/// `init` allocates the IoThread, sets up the VSR client and IO subsystem, spawns
/// the IO thread, and wires the ClientInterface vtable to the Shared fields.
/// The IO thread runs `io_thread`, a straight-line event loop that switches on
/// `self.phase` each tick. VSR callbacks (register, eviction, result) fire within
/// `io.run_for_ns` and set flags or data that the main loop acts on next iteration.
pub fn IoThreadType(
    comptime Client_: type,
) type {
    return struct {
        const IoThread = @This();
        const Client = Client_;
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
            self: *IoThread,
            packet: *Packet,

            comptime {
                assert(@sizeOf(UserData) == @sizeOf(u128));
            }
        };

        /// Heap-allocated VSR client and its dependencies.
        ///
        /// Separate from IoThread so that @fieldParentPtr("client") in callbacks
        /// recovers *ClientState (and thus *IoThread via the backref), without
        /// requiring Client to be embedded at a fixed offset in IoThread.
        /// Threaded through phase variants: whoever owns the current phase owns
        /// the ClientState pointer.
        const ClientState = struct {
            time_os: TimeOS,
            message_pool: MessagePool,
            client: Client,
            context: *IoThread,
        };

        const Phase = PhaseType(IoThread);

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
        io: IO,

        completion_callback: CompletionCallback,
        interface: *ClientInterface,

        eviction_reason: ?vsr.Header.Eviction.Reason,
        phase: Phase,
        shared: Shared,

        pub fn init(
            root_allocator: std.mem.Allocator,
            client_out: *ClientInterface,
            cluster_id: u128,
            addresses: []const u8,
            completion_ctx: usize,
            completion_callback: CompletionCallback,
        ) InitError!void {
            var context: *IoThread = context: {
                // Wrap the root allocator - usually heap.c_allocator when built as a library - in
                // a GPA to keep maximum compatibility while gaining the extra safety. As a library,
                // libtbclient is running inside another process's address space.
                var gpa = GPA{
                    .backing_allocator = root_allocator,
                };
                errdefer assert(gpa.deinit() == .ok);

                const context = try gpa.allocator().create(IoThread);

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
            context.shared.client_id = stdx.unique_u128();
            context.shared.cluster_id = cluster_id;
            context.shared.addresses_owned = try allocator.dupe(u8, addresses);
            errdefer allocator.free(context.shared.addresses_owned);

            log.debug("{}: init: parsing vsr addresses: {s}", .{
                context.shared.client_id,
                addresses,
            });
            var parsed_addresses: stdx.BoundedArrayType(
                std.net.Address,
                constants.replicas_max,
            ) = .{};
            const addresses_parsed = vsr.parse_addresses(
                addresses,
                parsed_addresses.unused_capacity_slice(),
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
            parsed_addresses.resize(addresses_parsed.len) catch unreachable;

            log.debug("{}: init: initializing IO", .{context.shared.client_id});
            context.io = IO.init(32, 0) catch |err| {
                log.err("{}: failed to initialize IO: {s}", .{
                    context.shared.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.ProcessFdQuotaExceeded => error.SystemResources,
                    error.Unexpected => error.Unexpected,
                    else => unreachable,
                };
            };
            errdefer context.io.deinit();

            log.debug("{}: init: initializing ClientState", .{context.shared.client_id});
            const client_state = try allocator.create(ClientState);
            errdefer allocator.destroy(client_state);

            client_state.context = context;
            client_state.time_os = .{};
            client_state.message_pool = try MessagePool.init(allocator, .client);
            errdefer client_state.message_pool.deinit(allocator);

            log.debug("{}: init: initializing client (cluster_id={x:0>32}, addresses={any})", .{
                context.shared.client_id,
                cluster_id,
                parsed_addresses.const_slice(),
            });
            client_state.client = Client.init(
                allocator,
                client_state.time_os.time(),
                &client_state.message_pool,
                .{
                    .id = context.shared.client_id,
                    .cluster = cluster_id,
                    .replica_count = parsed_addresses.count_as(u8),
                    .aof_recovery = false,
                    .message_bus_options = .{
                        .configuration = parsed_addresses.const_slice(),
                        .io = &context.io,
                        .trace = null,
                    },
                    .eviction_callback = callback_client_eviction,
                },
            ) catch |err| {
                log.err("{}: failed to initialize Client: {s}", .{
                    context.shared.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                };
            };
            errdefer client_state.client.deinit(allocator);

            ClientInterface.init(client_out, &context.shared, comptime &.{
                .submit_fn = &vtable_submit_fn,
                .completion_context_fn = &vtable_completion_context_fn,
                .deinit_fn = &vtable_deinit_fn,
                .init_parameters_fn = &vtable_init_parameters_fn,
            });
            context.interface = client_out;
            context.shared.submitted = Packet.Queue.init(.{
                .name = null,
                .verify_push = builtin.is_test,
            });
            context.shared.completion_context = completion_ctx;
            context.completion_callback = completion_callback;
            context.eviction_reason = null;
            context.phase = .{ .registering = .{
                .client_state = client_state,
                .batch_size_limit = null,
                .pending = Packet.Queue.init(.{
                    .name = null,
                    .verify_push = builtin.is_test,
                }),
            } };

            log.debug("{}: init: initializing signal", .{context.shared.client_id});
            try context.shared.signal.init(&context.io, IoThread.callback_signal_notify);
            errdefer context.shared.signal.deinit();

            client_state.client.register(callback_client_register, @intFromPtr(context));

            log.debug("{}: init: spawning thread", .{context.shared.client_id});
            context.shared.thread = std.Thread.spawn(
                .{ .stack_size = io_thread_stack_size },
                IoThread.io_thread,
                .{context},
            ) catch |err| {
                log.err("{}: failed to spawn thread: {s}", .{
                    context.shared.client_id,
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

        const has_message_bus = @hasField(Client, "message_bus");

        /// Main event loop running on the dedicated IO thread.
        ///
        /// Each iteration: check phase, do phase-specific work, then call
        /// io.run_for_ns to process IO completions and wait for the next tick.
        /// VSR callbacks (register, eviction, result, signal) fire inside
        /// run_for_ns; they set flags/data that the next loop iteration acts on.
        ///
        /// Some callbacks call io.yield in order to return quickly to the event loop
        /// without waiting for the wait_for_ns timeout. This allows the state
        /// transition logic to be entirely encoded directly in the event loop
        /// without extra induced latency.
        ///
        /// ### Safety note re io.yield ###
        ///
        /// io.yield may dispatch additional callbacks between the yield
        /// call and the actual return to the event loop. This means that within each phase,
        /// even after yielding, additional callbacks in that phase continue to be observed.
        /// Yielding only eliminates latency, it doesn't sequence control flow in
        /// in the way you might expect and want.
        fn io_thread(self: *IoThread) void {
            main: while (true) {
                const should_stop = self.shared.signal.status() == .shutdown;

                switch (self.phase) {
                    .registering => |*reg| {
                        if (self.eviction_reason != null or should_stop) {
                            self.phase = .{ .disconnecting = .{
                                .client_state = reg.client_state,
                                .pending = reg.pending,
                            } };
                            continue :main;
                        }

                        if (reg.batch_size_limit) |batch_size_limit| {
                            self.phase = .{ .running = .{
                                .client_state = reg.client_state,
                                .batch_size_limit = batch_size_limit,
                                .pending = reg.pending,
                                .previous_request_instant = null,
                                .previous_request_latency = null,
                            } };
                            continue :main;
                        }

                        assert(self.eviction_reason == null);
                        reg.client_state.client.tick();
                    },

                    .running => |*running| {
                        if (self.eviction_reason != null or should_stop) {
                            self.phase = .{ .disconnecting = .{
                                .client_state = running.client_state,
                                .pending = running.pending,
                            } };
                            continue :main;
                        }

                        assert(self.eviction_reason == null);
                        running.client_state.client.tick();
                        running.drain_submitted_packets(self);
                    },

                    .disconnecting => |*disconnecting| {
                        if (has_message_bus) {
                            const bus = &disconnecting.client_state.client.message_bus;
                            if (!bus.shutdown_requested) {
                                bus.shutdown();
                            }
                        }

                        disconnecting.cancel_inflight(self);

                        while (disconnecting.pending.pop()) |packet| {
                            packet.assert_phase(.pending);
                            disconnecting.packet_cancel(self, packet);
                        }

                        disconnecting.cancel_submitted(self);

                        if (disconnecting.client_io_settled()) {
                            const allocator = self.gpa.allocator();
                            disconnecting.client_state.client.deinit(allocator);
                            disconnecting.client_state.message_pool.deinit(allocator);
                            allocator.destroy(disconnecting.client_state);
                            self.phase = .settled;
                            continue :main;
                        }
                    },

                    // On eviction, deinit returns ClientInvalid (context
                    // pointer already nulled), so no user stop signal will
                    // arrive. The IO thread stops the signal itself.
                    .settled => {
                        if (self.eviction_reason != null) {
                            self.shared.signal.stop();
                        }

                        if (should_stop) break :main;
                    },
                }

                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed: {s}", .{
                        self.shared.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }

            assert(self.phase == .settled);
            self.shared.signal.deinit();
            self.io.deinit();

            // Copy GPA to the stack before freeing the IoThread that contains it,
            // since the allocator holds a pointer back to the GPA.
            var gpa = self.gpa;
            const allocator = gpa.allocator();
            allocator.free(self.shared.addresses_owned);
            allocator.destroy(self);
            _ = gpa.deinit();
        }

        // IO-thread callbacks: invoked by the VSR client or Signal within
        // io.run_for_ns. They set state that the main loop acts on.

        fn callback_signal_notify(signal: *Signal) void {
            const shared: *Shared = @alignCast(@fieldParentPtr("signal", signal));
            const self: *IoThread = @fieldParentPtr("shared", shared);
            self.io.yield();
        }

        fn callback_client_register(user_data: u128, result: *const vsr.RegisterResult) void {
            const self: *IoThread = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(self.eviction_reason == null);
            assert(result.batch_size_limit > 0);

            switch (self.phase) {
                .registering => |*reg| {
                    assert(reg.client_state.client.request_inflight == null);
                    assert(reg.batch_size_limit == null);

                    maybe(constants.message_body_size_max < result.batch_size_limit);
                    reg.batch_size_limit = @min(
                        result.batch_size_limit,
                        constants.message_body_size_max,
                    );

                    self.io.yield();
                },
                .running, .disconnecting, .settled => unreachable,
            }
        }

        fn callback_client_eviction(client: *Client, eviction: *const Message.Eviction) void {
            const cs: *ClientState = @fieldParentPtr("client", client);
            const self: *IoThread = cs.context;
            assert(self.phase == .registering or self.phase == .running);
            assert(self.eviction_reason == null);

            log.debug("{}: callback_client_eviction: reason={?s} reason_int={}", .{
                self.shared.client_id,
                std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                @intFromEnum(eviction.header.reason),
            });

            // Clear the interface context so that no more requests can be
            // submitted from user threads. Subsequent submissions will fail
            // synchronously with ClientInvalid.
            self.interface.locker.lock();
            defer self.interface.locker.unlock();

            self.interface.context = .{ .ptr = null };

            // Set the eviction reason. The io_thread main loop will detect
            // this and initiate graceful disconnection: cancel the inflight
            // and pending packets, terminate connections, and deinit the
            // client once all connection IO has settled.
            self.eviction_reason = eviction.header.reason;
            self.io.yield();
        }

        fn callback_client_result(
            raw_user_data: u128,
            operation_vsr: vsr.Operation,
            timestamp: u64,
            reply: []const u8,
        ) void {
            const user_data: UserData = @bitCast(raw_user_data);
            const self: *IoThread = user_data.self;
            const packet_list: *Packet = user_data.packet;
            const operation = operation_vsr.cast(Client.Operation);
            assert(self.phase == .running);
            const running: *Phase.Running = &self.phase.running;
            assert(self.eviction_reason == null);
            assert(packet_list.operation == @intFromEnum(operation));
            assert(timestamp > 0);
            packet_list.assert_phase(.sent);

            const current_timestamp = running.client_state.client.time.monotonic();
            running.previous_request_latency =
                current_timestamp.duration_since(running.previous_request_instant.?);

            // Submit the next pending packet (if any) now that VSR has completed this one.
            assert(running.client_state.client.request_inflight == null);
            while (running.pending.pop()) |packet_next| {
                running.packet_send(self, packet_next);
                if (running.client_state.client.request_inflight != null) break;
            }

            // The callback should never be called with an operation not in `allowed_operations`.
            // This also guards from sending an unsupported operation.
            assert(Phase.operation_from_int(@intFromEnum(operation)) != null);

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
            self: *IoThread,
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
                self.completion_callback(
                    self.shared.completion_context,
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
                self.shared.completion_context,
                packet.cast(),
                result.timestamp,
                result.reply.ptr,
                @intCast(result.reply.len),
            );
        }
    };
}

/// IO thread phase state machine.
///
/// The lifecycle is: registering -> running -> disconnecting -> settled.
///
/// - Registering: waiting for the VSR register callback to provide
///   batch_size_limit. Packets may arrive but are not sent yet.
/// - Running: normal operation. Drains the submitted queue, batches
///   compatible packets, sends requests through the VSR client.
/// - Disconnecting: entered on eviction or shutdown. Cancels inflight,
///   pending, and submitted packets. Waits for message_bus IO to settle
///   before deiniting the client. All packet cancellation is consolidated
///   here, maintaining FIFO ordering (inflight -> pending -> submitted).
/// - Settled: client resources freed. Waits for the shutdown signal
///   (or self-stops on eviction) then exits the main loop.
///
/// Each variant carries only the state relevant to that phase. Phase
/// transitions transfer ownership of shared resources (ClientState,
/// pending queue) from one variant to the next.
fn PhaseType(comptime IoThread: type) type {
    const Client = IoThread.Client;
    const Operation = IoThread.Operation;
    const has_message_bus = @hasField(Client, "message_bus");

    return union(enum) {
        registering: Registering,
        running: Running,
        disconnecting: Disconnecting,
        settled: void,

        fn operation_from_int(op: u8) ?Operation {
            inline for (IoThread.allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return operation;
                }
            }
            return null;
        }

        /// Registering: awaiting the VSR register callback.
        /// Packets accumulate in `pending` but are not sent until
        /// `batch_size_limit` is set by `callback_client_register`.
        const Registering = struct {
            client_state: *IoThread.ClientState,
            batch_size_limit: ?u32,
            pending: Packet.Queue,
        };

        /// Running: normal request processing.
        /// Drains submitted packets, batches them, sends through the VSR client.
        const Running = struct {
            client_state: *IoThread.ClientState,
            batch_size_limit: u32,
            pending: Packet.Queue,
            previous_request_instant: ?stdx.Instant,
            previous_request_latency: ?stdx.Duration,

            fn packet_enqueue(self: *Running, ctx: *IoThread, packet: *Packet) void {
                packet.assert_phase(.submitted);

                const operation: Operation = operation_from_int(packet.operation) orelse {
                    return ctx.notify_completion(packet, error.InvalidOperation);
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
                        return ctx.notify_completion(packet, error.InvalidDataSize);
                    }

                    const event_count: u32 = @intCast(@divExact(slice.len, event_size));
                    const event_max: u32 = operation.event_max(self.batch_size_limit);
                    if (event_count > event_max) {
                        return ctx.notify_completion(packet, error.TooMuchData);
                    }
                    const result_max: u32 = operation.result_max(self.batch_size_limit);
                    const result_count_expected: u32 = operation.result_count_expected(slice);
                    if (result_count_expected > result_max) {
                        return ctx.notify_completion(packet, error.TooMuchData);
                    }

                    break :batch .{
                        .event_size = event_size,
                        .result_size = result_size,
                        .event_count = event_count,
                        .result_count_expected = result_count_expected,
                    };
                };
                assert(packet.data_size == batch.event_count * batch.event_size);
                maybe(batch.event_count == 0);
                maybe(batch.result_count_expected == 0);

                assert(ctx.eviction_reason == null);

                // Nothing inflight means the packet should be submitted right now.
                if (self.client_state.client.request_inflight == null) {
                    assert(self.pending.count() == 0);
                    packet.phase = .pending;
                    packet.multi_batch_time_monotonic =
                        self.client_state.client.time.monotonic().ns;
                    packet.multi_batch_count = 1;
                    packet.multi_batch_event_count = @intCast(batch.event_count);
                    packet.multi_batch_result_count_expected =
                        @intCast(batch.result_count_expected);
                    self.packet_send(ctx, packet);
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
                    if (request_size > self.batch_size_limit) continue;

                    // Check if the reply has enough space for the maximum
                    // expected number of results:
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
                packet.multi_batch_time_monotonic = self.client_state.client.time.monotonic().ns;
                packet.multi_batch_count = 1;
                packet.multi_batch_event_count = @intCast(batch.event_count);
                packet.multi_batch_result_count_expected = @intCast(batch.result_count_expected);
                self.pending.push(packet);
            }

            /// Encode the packet batch into a VSR message and submit it to the client.
            fn packet_send(self: *Running, ctx: *IoThread, packet_list: *Packet) void {
                assert(ctx.eviction_reason == null);
                packet_list.assert_phase(.pending);

                assert(self.client_state.client.request_inflight == null);

                const message = self.client_state.client.get_message().build(.request);
                defer {
                    self.client_state.client.release_message(message.base());
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

                    var message_encoder = MultiBatchEncoder.init(
                        message.buffer[@sizeOf(Header)..],
                        .{
                            .element_size = event_size,
                        },
                    );

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

                    // Check if the reply has enough space for the maximum
                    // expected number of results.
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
                assert(request_size <= self.batch_size_limit);

                // Sending the request.
                const previous_request_latency =
                    self.previous_request_latency orelse stdx.Duration{ .ns = 0 };
                message.header.* = .{
                    .release = self.client_state.client.release,
                    .client = self.client_state.client.id,
                    .request = 0, // Set by client.raw_request.
                    .cluster = self.client_state.client.cluster,
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
                self.client_state.client.raw_request(
                    IoThread.callback_client_result,
                    @bitCast(IoThread.UserData{
                        .self = ctx,
                        .packet = packet_list,
                    }),
                    message.ref(),
                );
                assert(message.header.request != 0);
            }

            fn drain_submitted_packets(self: *Running, ctx: *IoThread) void {
                assert(ctx.eviction_reason == null);

                const enqueued_count = self.pending.count();
                const safety_limit = 8 * 1024; // Avoid unbounded loop in case of invalid packets.
                for (0..safety_limit) |_| {
                    const packet: *Packet = pop: {
                        ctx.interface.locker.lock();
                        defer ctx.interface.locker.unlock();

                        break :pop ctx.shared.submitted.pop() orelse return;
                    };
                    self.packet_enqueue(ctx, packet);

                    // Packets can be processed without increasing `pending.count`:
                    // - If the packet is invalid.
                    // - If there's no in-flight request, the packet is sent immediately without
                    //   using the pending queue.
                    // - If the packet can be batched with another previously enqueued packet.
                    if (self.pending.count() > enqueued_count) break;
                }

                // Defer remaining work to later,
                // allowing the IO thread to remain free for processing completions.
                const empty: bool = empty: {
                    ctx.interface.locker.lock();
                    defer ctx.interface.locker.unlock();

                    break :empty ctx.shared.submitted.empty();
                };
                if (!empty) {
                    ctx.shared.signal.notify();
                }
            }
        };

        /// Disconnecting: tearing down the VSR client.
        ///
        /// All packet cancellation is consolidated here, maintaining FIFO order.
        /// On entry, pending queue is transferred from the previous phase.
        /// Each tick: cancel inflight if needed, drain pending, drain submitted,
        /// then check if message_bus IO has settled. Once settled, deinit the
        /// client and transition to settled.
        const Disconnecting = struct {
            client_state: *IoThread.ClientState,
            pending: Packet.Queue,
            inflight_canceled: bool = false,

            /// Cancel a packet (and its entire batched chain) by calling
            /// the user completion callback with the appropriate error status.
            fn packet_cancel(_: *const Disconnecting, ctx: *IoThread, packet_list: *Packet) void {
                assert(packet_list.link.next == null);
                assert(packet_list.phase != .complete);
                packet_list.assert_phase(packet_list.phase);

                const result = if (ctx.eviction_reason) |reason| switch (reason) {
                    .reserved => unreachable,
                    .client_release_too_low => error.ClientReleaseTooLow,
                    .client_release_too_high => error.ClientReleaseTooHigh,
                    else => error.ClientEvicted,
                } else result: {
                    assert(ctx.shared.signal.status() != .running);
                    break :result error.ClientShutdown;
                };

                var it: ?*Packet = packet_list;
                while (it) |batched| {
                    if (batched != packet_list) batched.assert_phase(.batched);
                    it = batched.multi_batch_next;
                    ctx.notify_completion(batched, result);
                }
            }

            /// Cancel the inflight user packet.
            ///
            /// After eviction or message_bus shutdown, the VSR client will not
            /// call the result callback, so the packet must be canceled here.
            /// Skips .register operations (no associated user packet). Must run
            /// only once: after cancellation the packet is returned to the user
            /// and the memory may be freed.
            fn cancel_inflight(self: *Disconnecting, ctx: *IoThread) void {
                if (self.inflight_canceled) return;
                self.inflight_canceled = true;

                const inflight = &(self.client_state.client.request_inflight orelse return);
                if (inflight.message.header.operation == .register) return;

                const ud: IoThread.UserData = @bitCast(inflight.user_data);
                const packet: *Packet = ud.packet;
                packet.assert_phase(.sent);
                self.packet_cancel(ctx, packet);
            }

            /// Drain and cancel all submitted packets.
            fn cancel_submitted(self: *Disconnecting, ctx: *IoThread) void {
                while (true) {
                    const packet: *Packet = pop: {
                        ctx.interface.locker.lock();
                        defer ctx.interface.locker.unlock();

                        break :pop ctx.shared.submitted.pop() orelse break;
                    };
                    packet.assert_phase(.submitted);
                    self.packet_cancel(ctx, packet);
                }
            }

            /// Returns true when all client IO operations have completed and
            /// it is safe to free the client's resources.
            fn client_io_settled(self: *const Disconnecting) bool {
                if (has_message_bus) {
                    return self.client_state.client.message_bus.shutdown_complete();
                }
                return true;
            }
        };
    };
}

/// Futex-based mutex with extern struct layout for ABI stability.
/// Vendored from `std.Thread.Mutex.FutexImpl` because std.Thread.Mutex
/// is not an extern struct and cannot be embedded in ClientInterface.
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
        // - they both seem to mark the cache-line as modified regardless:
        //   https://stackoverflow.com/a/63350048.
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
