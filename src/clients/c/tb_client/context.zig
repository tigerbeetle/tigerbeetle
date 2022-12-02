const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const constants = @import("../../../constants.zig");
const log = std.log.scoped(.tb_client_context);

const util = @import("../../../util.zig");
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
const tb_packet_t = api.tb_packet_t;
const tb_packet_list_t = api.tb_packet_list_t;

pub const ContextImplementation = struct {
    submit_fn: fn (*ContextImplementation, *tb_packet_list_t) void,
    deinit_fn: fn (*ContextImplementation) void,
};

pub const Error = std.mem.Allocator.Error || error{
    Unexpected,
    AddressInvalid,
    AddressLimitExceeded,
    PacketsCountInvalid,
    SystemResources,
    NetworkSubsystemFailed,
};

pub fn ContextType(
    comptime Client: type,
) type {
    return struct {
        const Context = @This();
        const Thread = ThreadType(Context);

        const UserData = extern struct {
            self: *Context,
            packet: *Packet,
        };

        comptime {
            assert(@sizeOf(UserData) == @sizeOf(u128));
        }

        fn operation_event_size(op: u8) ?usize {
            const allowed_operations = [_]Client.StateMachine.Operation{
                .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
            };

            inline for (allowed_operations) |operation| {
                if (op == @enumToInt(operation)) {
                    return @sizeOf(Client.StateMachine.Event(operation));
                }
            }

            return null;
        }

        const PacketError = Client.Error || error{
            TooMuchData,
            InvalidOperation,
            InvalidDataSize,
        };

        allocator: std.mem.Allocator,
        client_id: u128,
        packets: []Packet,

        addresses: []const std.net.Address,
        io: IO,
        message_pool: MessagePool,
        messages_available: usize,
        client: Client,

        on_completion_ctx: usize,
        on_completion_fn: tb_completion_t,
        implementation: ContextImplementation,
        thread: Thread,

        pub fn init(
            allocator: std.mem.Allocator,
            cluster_id: u32,
            addresses: []const u8,
            packets_count: u32,
            on_completion_ctx: usize,
            on_completion_fn: tb_completion_t,
        ) Error!*Context {
            var context = try allocator.create(Context);
            errdefer allocator.destroy(context);

            context.allocator = allocator;
            context.client_id = std.crypto.random.int(u128);
            log.debug("{}: init: initializing", .{context.client_id});

            const packets_count_max = 4096;
            if (packets_count > packets_count_max) {
                return error.PacketsCountInvalid;
            }

            log.debug("{}: init: allocating tb_packets", .{context.client_id});
            context.packets = try context.allocator.alloc(Packet, packets_count);
            errdefer context.allocator.free(context.packets);

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

            log.debug("{}: init: initializing client (cluster_id={}, addresses={any})", .{
                context.client_id,
                cluster_id,
                context.addresses,
            });
            context.client = try Client.init(
                allocator,
                context.client_id,
                cluster_id,
                @intCast(u8, context.addresses.len),
                &context.message_pool,
                .{
                    .configuration = context.addresses,
                    .io = &context.io,
                },
            );
            errdefer context.client.deinit(context.allocator);

            context.messages_available = constants.client_request_queue_max;
            context.on_completion_ctx = on_completion_ctx;
            context.on_completion_fn = on_completion_fn;
            context.implementation = .{
                .submit_fn = Context.on_submit,
                .deinit_fn = Context.on_deinit,
            };

            log.debug("{}: init: initializing thread", .{context.client_id});
            try context.thread.init(context);
            errdefer context.thread.deinit(context.allocator);

            return context;
        }

        pub fn deinit(self: *Context) void {
            self.thread.deinit();

            self.client.deinit(self.allocator);
            self.message_pool.deinit(self.allocator);
            self.io.deinit();

            self.allocator.free(self.addresses);
            self.allocator.free(self.packets);
            self.allocator.destroy(self);
        }

        pub fn tick(self: *Context) void {
            self.client.tick();
        }

        pub fn run(self: *Context) void {
            while (!self.thread.signal.is_shutdown()) {
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
            const message = self.message_pool.get_message();
            defer self.message_pool.unref(message);
            self.messages_available -= 1;

            // Get the size of each request structure in the packet.data:
            const event_size: usize = operation_event_size(packet.operation) orelse {
                return self.on_complete(packet, error.InvalidOperation);
            };

            // Make sure the packet.data size is correct:
            const readable = @ptrCast([*]const u8, packet.data)[0..packet.data_size];
            if (readable.len == 0 or readable.len % event_size != 0) {
                return self.on_complete(packet, error.InvalidDataSize);
            }

            // Make sure the packet.data wouldn't overflow a message:
            const writable = message.buffer[@sizeOf(Header)..][0..constants.message_body_size_max];
            if (readable.len > writable.len) {
                return self.on_complete(packet, error.TooMuchData);
            }

            // Write the packet data to the message:
            util.copy_disjoint(.inexact, u8, writable, readable);
            const wrote = readable.len;

            // Submit the message for processing:
            self.client.request(
                @bitCast(u128, UserData{
                    .self = self,
                    .packet = packet,
                }),
                Context.on_result,
                @intToEnum(Client.StateMachine.Operation, packet.operation),
                message,
                wrote,
            );
        }

        fn on_result(
            raw_user_data: u128,
            op: Client.StateMachine.Operation,
            results: Client.Error![]const u8,
        ) void {
            const user_data = @bitCast(UserData, raw_user_data);
            const self = user_data.self;
            const packet = user_data.packet;

            assert(packet.operation == @enumToInt(op));
            self.on_complete(packet, results);
        }

        fn on_complete(
            self: *Context,
            packet: *Packet,
            result: PacketError![]const u8,
        ) void {
            self.messages_available += 1;
            assert(self.messages_available <= constants.client_request_queue_max);

            // Signal to resume sending requests that was waiting for available messages.
            if (self.messages_available == 1) self.thread.signal.notify();

            const tb_client = api.context_to_client(&self.implementation);
            const bytes = result catch |err| {
                packet.status = switch (err) {
                    // If there's too many requests, (re)try submitting the packet later.
                    error.TooManyOutstandingRequests => {
                        return self.thread.retry.push(Packet.List.from(packet));
                    },
                    error.TooMuchData => .too_much_data,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                return (self.on_completion_fn)(self.on_completion_ctx, tb_client, packet, null, 0);
            };

            // The packet completed normally.
            packet.status = .ok;
            (self.on_completion_fn)(self.on_completion_ctx, tb_client, packet, bytes.ptr, @intCast(u32, bytes.len));
        }

        fn on_submit(implementation: *ContextImplementation, packets: *tb_packet_list_t) void {
            const context = @fieldParentPtr(Context, "implementation", implementation);
            context.thread.submit(packets.*);
        }

        fn on_deinit(implementation: *ContextImplementation) void {
            const context = @fieldParentPtr(Context, "implementation", implementation);
            context.deinit();
        }
    };
}
