const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const Atomic = std.atomic.Atomic;

const constants = @import("../../../constants.zig");
const log = std.log.scoped(.tb_client_context);

const stdx = @import("../../../stdx.zig");
const vsr = @import("../../../vsr.zig");
const Header = vsr.Header;

const IO = @import("../../../io.zig").IO;
const message_pool = @import("../../../message_pool.zig");

const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;
const ThreadType = @import("thread.zig").ThreadType;

const api = @import("../tb_client.zig");
const tb_status_t = api.tb_status_t;
const tb_client_t = api.tb_client_t;
const tb_completion_t = api.tb_completion_t;

pub const ContextImplementation = struct {
    completion_ctx: usize,
    submit_fn: *const fn (*ContextImplementation, *Packet) bool,
    deinit_fn: *const fn (*ContextImplementation) void,
};

pub const Error = std.mem.Allocator.Error || error{
    Unexpected,
    AddressInvalid,
    AddressLimitExceeded,
    SystemResources,
    NetworkSubsystemFailed,
};

pub fn ContextType(comptime Client: type) type {
    return struct {
        const Context = @This();
        const Thread = ThreadType(Context);

        fn operation_event_size(op: u8) ?usize {
            const allowed_operations = [_]Client.StateMachine.Operation{
                .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
                .get_account_transfers,
                .get_account_history,
            };

            inline for (allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return @sizeOf(Client.StateMachine.Event(operation));
                }
            }

            return null;
        }

        const PacketError = error{
            TooMuchData,
            InvalidOperation,
            InvalidDataSize,
        };

        const State = packed struct(u32) {
            is_shutdown: bool = false,
            pending_packets: u31 = 0,
        };

        allocator: std.mem.Allocator,
        client_id: u128,

        addresses: []const std.net.Address,
        io: IO,
        message_pool: MessagePool,
        client: Client,

        completion_fn: tb_completion_t,
        implementation: ContextImplementation,
        thread: Thread,
        state: Atomic(u32) = Atomic(u32).init(0),

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
            log.debug("{}: init: initializing", .{context.client_id});

            log.debug("{}: init: parsing vsr addresses: {s}", .{ context.client_id, addresses });
            context.addresses = vsr.parse_addresses(
                context.allocator,
                addresses,
                constants.replicas_max,
            ) catch |err| return switch (err) {
                error.AddressLimitExceeded => error.AddressLimitExceeded,
                else => error.AddressInvalid,
            };
            errdefer context.allocator.free(context.addresses);

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
                context.addresses,
            });
            context.client = try Client.init(
                allocator,
                context.client_id,
                cluster_id,
                @intCast(context.addresses.len),
                &context.message_pool,
                .{
                    .configuration = context.addresses,
                    .io = &context.io,
                },
            );
            errdefer context.client.deinit(context.allocator);

            context.completion_fn = completion_fn;
            context.implementation = .{
                .completion_ctx = completion_ctx,
                .submit_fn = Context.on_submit,
                .deinit_fn = Context.on_deinit,
            };

            log.debug("{}: init: initializing thread", .{context.client_id});
            try context.thread.init(context);
            errdefer context.thread.deinit(context.allocator);

            return context;
        }

        pub fn deinit(self: *Context) void {
            const is_shutdown: u32 = @bitCast(State{ .is_shutdown = true });
            const old_state: State = @bitCast(self.state.fetchOr(is_shutdown, .AcqRel));
            if (!old_state.is_shutdown) {
                self.thread.deinit();

                self.client.deinit(self.allocator);
                self.message_pool.deinit(self.allocator);
                self.io.deinit();

                self.allocator.free(self.addresses);
                self.allocator.destroy(self);
            }
        }

        pub fn tick(self: *Context) void {
            self.client.tick();
        }

        pub fn run(self: *Context) void {
            while (true) {
                // Stop running when deinit() has been called and there's no more active packets.
                const state: State = @bitCast(self.state.load(.Acquire));
                if (state.is_shutdown and state.pending_packets == 0) {
                    break;
                }

                self.tick();
                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed: {s}", .{
                        self.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }
        }

        pub fn request(self: *Context, packet: *Packet) void {
            // Get the size of each packet structure in the packet.request.body:
            const event_size: usize = operation_event_size(@intFromEnum(packet.request.operation)) orelse {
                return self.on_complete(packet, error.InvalidOperation);
            };

            // Make sure the packet.request.body size is correct:
            const event_data = packet.request.body[0..packet.request.body_len];
            if (event_data.len == 0 or event_data.len % event_size != 0) {
                return self.on_complete(packet, error.InvalidDataSize);
            }

            // Make sure the request.body wouldn't overflow a message:
            if (event_data.len > constants.message_body_size_max) {
                return self.on_complete(packet, error.TooMuchData);
            }

            // Submit the packet request for processing:
            packet.context = self;
            self.client.submit(
                Context.on_result,
                &packet.request,
                packet.request.operation.cast(Client.StateMachine),
                event_data,
            );
        }

        fn on_result(
            vsr_request: *Client.Request,
            results: []const u8,
        ) void {
            const packet = @fieldParentPtr(Packet, "request", vsr_request);
            const self: *Context = @alignCast(@ptrCast(packet.context.?));
            self.on_complete(packet, results);
        }

        fn on_complete(
            self: *Context,
            packet: *Packet,
            result: PacketError![]const u8,
        ) void {
            const completion_ctx = self.implementation.completion_ctx;
            const tb_client = api.context_to_client(&self.implementation);

            // Make sure to bump down the pending_packets counter before calling completion handler.
            const pending_packet: u32 = @bitCast(State{ .pending_packets = 1 });
            const old_state: State = @bitCast(self.state.fetchSub(pending_packet, .Monotonic));
            assert(old_state.pending_packets > 0);

            // Complete the packet with an error if any.
            const bytes = result catch |err| {
                packet.status = switch (err) {
                    error.TooMuchData => .too_much_data,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                return (self.completion_fn)(completion_ctx, tb_client, packet, null, 0);
            };

            // The packet completed normally.
            packet.status = .ok;
            (self.completion_fn)(completion_ctx, tb_client, packet, bytes.ptr, @intCast(bytes.len));
        }

        inline fn get_context(implementation: *ContextImplementation) *Context {
            return @fieldParentPtr(Context, "implementation", implementation);
        }

        fn on_submit(implementation: *ContextImplementation, packet: *Packet) bool {
            const self = get_context(implementation);
            const pending_packet: u32 = @bitCast(State{ .pending_packets = 1 });

            // Bump the state counter's pending_packet count. Faster than compareAndSwap loop.
            var old_state: State = @bitCast(self.state.fetchAdd(pending_packet, .Monotonic));
            assert(old_state.pending_packets < std.math.maxInt(@TypeOf(old_state.pending_packets)));
            
            // Atomically check if the context is being shutdown when we bumped the count.
            // If so, unbump and return false for submit().
            if (old_state.is_shutdown) {
                old_state = @bitCast(self.state.fetchSub(pending_packet, .Monotonic));
                assert(old_state.pending_packets > 0);
                assert(old_state.is_shutdown);
                return false;
            }

            self.thread.submit(packet);
            return true;
        }

        fn on_deinit(implementation: *ContextImplementation) void {
            const self = get_context(implementation);
            self.deinit();
        }
    };
}
