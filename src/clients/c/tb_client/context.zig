const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const Atomic = std.atomic.Value;

// When referenced from unit_test.zig, there is no vsr import module so use path.
const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../../vsr.zig");

const constants = vsr.constants;
const log = std.log.scoped(.tb_client_context);

const stdx = vsr.stdx;
const Header = vsr.Header;

const IO = vsr.io.IO;
const FIFO = vsr.fifo.FIFO;
const message_pool = vsr.message_pool;

const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;
const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;

const api = @import("../tb_client.zig");
const tb_status_t = api.tb_status_t;
const tb_client_t = api.tb_client_t;
const tb_completion_t = api.tb_completion_t;

pub const ContextImplementation = struct {
    completion_ctx: usize,
    submit_fn: *const fn (*ContextImplementation, *Packet) void,
    deinit_fn: *const fn (*ContextImplementation) void,
};

pub const Error = std.mem.Allocator.Error || error{
    Unexpected,
    AddressInvalid,
    AddressLimitExceeded,
    SystemResources,
    NetworkSubsystemFailed,
};

pub fn ContextType(
    comptime Client: type,
) type {
    return struct {
        const Context = @This();

        const allowed_operations = [_]Client.StateMachine.Operation{
            .create_accounts,
            .create_transfers,
            .lookup_accounts,
            .lookup_transfers,
            .get_account_transfers,
            .get_account_balances,
            .query_accounts,
            .query_transfers,
        };

        const StateMachine = Client.StateMachine;
        const UserData = extern struct {
            self: *Context,
            packet: *Packet,
        };

        comptime {
            assert(@sizeOf(UserData) == @sizeOf(u128));
        }

        fn operation_from_int(op: u8) ?Client.StateMachine.Operation {
            inline for (allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return operation;
                }
            }
            return null;
        }

        fn operation_event_size(op: u8) ?usize {
            inline for (allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return @sizeOf(Client.StateMachine.Event(operation));
                }
            }
            return null;
        }

        const PacketError = error{
            TooMuchData,
            ClientShutdown,
            InvalidOperation,
            InvalidDataSize,
        };

        allocator: std.mem.Allocator,
        client_id: u128,

        addresses: stdx.BoundedArray(std.net.Address, constants.replicas_max),
        io: IO,
        message_pool: MessagePool,
        client: Client,
        batch_size_limit: ?u32,

        completion_fn: tb_completion_t,
        implementation: ContextImplementation,

        signal: Signal,
        submitted: Packet.SubmissionStack,
        pending: FIFO(Packet),
        shutdown: Atomic(bool),
        thread: std.Thread,

        pub fn init(
            allocator: std.mem.Allocator,
            cluster_id: u128,
            addresses: []const u8,
            completion_ctx: usize,
            completion_fn: tb_completion_t,
        ) Error!*Context {
            var context = try allocator.create(Context);
            errdefer allocator.destroy(context);

            context.allocator = allocator;
            context.client_id = std.crypto.random.int(u128);
            assert(context.client_id != 0); // Broken CSPRNG is the likeliest explanation for zero.

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
            errdefer context.message_pool.deinit(context.allocator);

            log.debug("{}: init: initializing client (cluster_id={x:0>32}, addresses={any})", .{
                context.client_id,
                cluster_id,
                context.addresses.const_slice(),
            });
            context.client = try Client.init(
                allocator,
                .{
                    .id = context.client_id,
                    .cluster = cluster_id,
                    .replica_count = context.addresses.count_as(u8),
                    .message_pool = &context.message_pool,
                    .message_bus_options = .{
                        .configuration = context.addresses.const_slice(),
                        .io = &context.io,
                    },
                },
            );
            errdefer context.client.deinit(context.allocator);

            context.completion_fn = completion_fn;
            context.implementation = .{
                .completion_ctx = completion_ctx,
                .submit_fn = Context.on_submit,
                .deinit_fn = Context.on_deinit,
            };

            context.submitted = .{};
            context.shutdown = Atomic(bool).init(false);
            context.pending = .{ .name = null };

            log.debug("{}: init: initializing signal", .{context.client_id});
            try context.signal.init(&context.io, Context.on_signal);
            errdefer context.signal.deinit();

            log.debug("{}: init: spawning thread", .{context.client_id});
            context.thread = std.Thread.spawn(.{}, Context.run, .{context}) catch |err| {
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

            return context;
        }

        pub fn deinit(self: *Context) void {
            // Only one thread calls deinit() and it's UB for any further Context interaction.
            const already_shutdown = self.shutdown.swap(true, .release);
            assert(!already_shutdown);

            // Wake up the run() thread for it to observe shutdown=true, cancel inflight/pending
            // packets, and finish running.
            self.signal.notify();
            self.thread.join();

            self.signal.deinit();
            self.client.deinit(self.allocator);
            self.message_pool.deinit(self.allocator);
            self.io.deinit();

            self.allocator.destroy(self);
        }

        fn client_register_callback(user_data: u128, result: *const vsr.RegisterResult) void {
            const self: *Context = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(self.batch_size_limit == null);
            assert(result.batch_size_limit > 0);
            assert(result.batch_size_limit <= constants.message_body_size_max);

            self.batch_size_limit = result.batch_size_limit;
            // Some requests may have queued up while the client was registering.
            on_signal(&self.signal);
        }

        pub fn tick(self: *Context) void {
            self.client.tick();
        }

        pub fn run(self: *Context) void {
            self.batch_size_limit = null;
            self.client.register(client_register_callback, @intFromPtr(self));

            while (!self.shutdown.load(.acquire)) {
                self.tick();
                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed: {s}", .{
                        self.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }

            // Cancel the request_inflight packet if any.
            //
            // TODO: Look into completing the inflight packet with a different error than
            // `error.ClientShutdown`, allow the client user to make a more informed decision
            // e.g. retrying the inflight packet and just abandoning the ClientShutdown ones.
            if (self.client.request_inflight) |*inflight| {
                if (inflight.message.header.operation != .register) {
                    const packet = @as(UserData, @bitCast(inflight.user_data)).packet;
                    assert(packet.next == null); // Inflight packet should not be pending.
                    self.cancel(packet);
                }
            }

            // Cancel pending and submitted packets.
            while (self.pending.pop()) |packet| self.cancel(packet);
            while (self.submitted.pop()) |packet| self.cancel(packet);
        }

        fn on_signal(signal: *Signal) void {
            const self: *Context = @alignCast(@fieldParentPtr("signal", signal));

            // Don't send any requests until registration completes.
            if (self.batch_size_limit == null) {
                assert(self.client.request_inflight != null);
                assert(self.client.request_inflight.?.message.header.operation == .register);
                return;
            }

            while (self.submitted.pop()) |packet| {
                self.request(packet);
            }
        }

        fn request(self: *Context, packet: *Packet) void {
            const operation = operation_from_int(packet.operation) orelse {
                return self.on_complete(packet, error.InvalidOperation);
            };

            // Get the size of each request structure in the packet.data:
            const event_size, const result_size = switch (operation) {
                inline else => |operation_comptime| [_]usize{
                    @sizeOf(Client.StateMachine.Event(operation_comptime)),
                    @sizeOf(Client.StateMachine.Result(operation_comptime)),
                },
            };

            // Make sure the packet.data size is correct:
            const events = @as([*]const u8, @ptrCast(packet.data))[0..packet.data_size];
            if (events.len == 0 or events.len % event_size != 0) {
                return self.on_complete(packet, error.InvalidDataSize);
            }

            const event_count = std.math.cast(u16, @divExact(events.len, event_size)) orelse {
                return self.on_complete(packet, error.InvalidDataSize);
            };

            // Make sure the packet.data wouldn't overflow a request, and that the corresponding
            // results won't overflow a reply.
            const events_batch_max = switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| StateMachine.operation_batch_max(
                    operation_comptime,
                    self.batch_size_limit.?,
                ),
            };

            if (event_count > events_batch_max) {
                return self.on_complete(packet, error.TooMuchData);
            } else {
                assert(events.len <= self.batch_size_limit.?);
            }

            packet.batch_next = null;
            packet.batch_tail = packet;
            packet.batch_count_packets = 1;
            packet.batch_count_events = @intCast(event_count);
            packet.batch_count_results = @intCast(switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| StateMachine.operation_result_max(
                    operation_comptime,
                    events,
                ),
            });

            // Avoid making a packet inflight by cancelling it if the client was shutdown.
            if (self.shutdown.load(.acquire)) {
                return self.cancel(packet);
            }

            // Nothing inflight means the packet should be submitted right now.
            if (self.client.request_inflight == null) {
                return self.submit(packet);
            }

            // Otherwise, try to batch the packet with another already in self.pending.
            var it = self.pending.peek();
            while (it) |root| {
                it = root.next;

                // Check for pending packets of the same operation which can be batched.
                if (root.operation != packet.operation) continue;
                if (root.batch_count_packets == vsr.RequestBatch.count_max) continue;
                if (root.batch_count_events + event_count > events_batch_max) continue;

                const merged_results = root.batch_count_results + packet.batch_count_results;
                if (merged_results * result_size > vsr.RequestBatch.value_size_max) continue;

                root.batch_count_packets += 1;
                root.batch_count_events += event_count;
                root.batch_count_results += packet.batch_count_results;

                root.batch_tail.?.batch_next = packet;
                root.batch_tail = packet;
                return;
            }

            // Couldn't batch with existing packet so push to pending directly.
            packet.next = null;
            self.pending.push(packet);
        }

        fn submit(self: *Context, packet: *Packet) void {
            assert(self.client.request_inflight == null);

            // On shutdown, cancel this packet as well as any others batched onto it.
            if (self.shutdown.load(.acquire)) {
                self.cancel(packet);
                return;
            }

            const message = self.client.get_message().build(.request);
            errdefer self.client.release_message(message.base());

            const operation: StateMachine.Operation = @enumFromInt(packet.operation);
            message.header.* = .{
                .release = self.client.release,
                .client = self.client.id,
                .request = 0, // Set by client.raw_request.
                .cluster = self.client.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @sizeOf(vsr.Header),
            };

            // Copy all batched packet event data into the message.
            switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| {
                    var writer = vsr.RequestBatch.WriterType(
                        StateMachine.Event(operation_comptime),
                    ).init(message.buffer[@sizeOf(vsr.Header)..]);

                    var it: ?*Packet = packet;
                    while (it) |batched| {
                        it = batched.batch_next;
                        writer.write(@as([*]u8, @ptrCast(batched.data.?))[0..batched.data_size]);
                    }

                    assert(writer.wrote <= constants.message_body_size_max);
                    message.header.size += writer.wrote;
                },
            }

            self.client.raw_request(
                Context.on_result,
                @bitCast(UserData{
                    .self = self,
                    .packet = packet,
                }),
                message,
            );
        }

        fn on_result(
            raw_user_data: u128,
            op: StateMachine.Operation,
            reply: []u8,
        ) void {
            const user_data: UserData = @bitCast(raw_user_data);
            const self = user_data.self;
            const packet = user_data.packet;
            assert(packet.next == null); // (previously) inflight packet should not be pending.

            // Submit the next pending packet (if any) now that VSR has completed this one.
            // The submit() call may complete it inline so keep submitting until theres an inflight.
            while (self.pending.pop()) |packet_next| {
                self.submit(packet_next);
                if (self.client.request_inflight != null) break;
            }

            switch (op) {
                inline else => |operation| {
                    // on_result should never be called with an operation not green-lit by request()
                    if (comptime operation_event_size(@intFromEnum(operation)) == null) {
                        unreachable;
                    }

                    var reader = vsr.RequestBatch.ReaderType(
                        StateMachine.Result(operation),
                    ).init(reply);

                    var it: ?*Packet = packet;
                    var event_count: usize = packet.batch_count_events;
                    while (it) |batched| {
                        it = batched.batch_next;

                        const results = reader.next() orelse {
                            @panic("client received invalid batched response");
                        };

                        assert(event_count >= results.len);
                        event_count -= results.len;

                        self.on_complete(batched, std.mem.sliceAsBytes(results));
                    }
                },
            }
        }

        fn cancel(self: *Context, packet: *Packet) void {
            var it: ?*Packet = packet;
            while (it) |batched| {
                it = batched.batch_next;
                self.on_complete(batched, error.ClientShutdown);
            }
        }

        fn on_complete(
            self: *Context,
            packet: *Packet,
            result: PacketError![]const u8,
        ) void {
            const completion_ctx = self.implementation.completion_ctx;
            const tb_client = api.context_to_client(&self.implementation);
            const bytes = result catch |err| {
                packet.status = switch (err) {
                    error.TooMuchData => .too_much_data,
                    error.ClientShutdown => .client_shutdown,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                (self.completion_fn)(completion_ctx, tb_client, packet, null, 0);
                return;
            };

            // The packet completed normally.
            packet.status = .ok;
            (self.completion_fn)(completion_ctx, tb_client, packet, bytes.ptr, @intCast(bytes.len));
        }

        inline fn get_context(implementation: *ContextImplementation) *Context {
            return @alignCast(@fieldParentPtr("implementation", implementation));
        }

        fn on_submit(implementation: *ContextImplementation, packet: *Packet) void {
            const self = get_context(implementation);

            const already_shutdown = self.shutdown.load(.acquire);
            assert(!already_shutdown);

            self.submitted.push(packet);
            self.signal.notify();
        }

        fn on_deinit(implementation: *ContextImplementation) void {
            const self = get_context(implementation);
            self.deinit();
        }
    };
}
