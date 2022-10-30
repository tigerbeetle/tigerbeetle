const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const config = @import("../../config.zig");
const log = std.log.scoped(.tb_client);

const vsr = @import("../../vsr.zig");
const Header = vsr.Header;

const IO = @import("../../io.zig").IO;
const message_pool = @import("../../message_pool.zig");

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

pub const Error = error{
    Unexpected,
    OutOfMemory,
    InvalidAddress,
    SystemResources,
    NetworkSubsystemFailed,
};

pub fn ContextType(
    comptime StateMachine: type,
    comptime MessageBus: type,
) type {
    return struct {
        const Context = @This();
        const Thread = ThreadType(Context);
        const Client = vsr.Client(StateMachine, MessageBus);
        const Operation = StateMachine.Operation;

        const UserData = packed struct {
            self: *Context,
            packet: *Packet,
        };

        const PacketError = Client.Error || error{
            TooMuchData,
            InvalidOperation,
            InvalidDataSize,
        };

        fn operation_size_of(op: u8) ?usize {
            const allowed_operations = [_]Operation{
                .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
            };

            inline for (allowed_operations) |operation| {
                if (op == @enumToInt(operation)) {
                    return @sizeOf(StateMachine.Event(operation));
                }
            }

            return null;
        }

        /////////////////////////////////////////////////////////////////////////

        allocator: std.mem.Allocator,
        client_id: u128,
        packets: []Packet,

        addresses: []std.net.Address,
        io: IO,
        message_pool: MessagePool,
        message_bus: MessageBus,
        client: Client,

        on_completion_ctx: usize,
        on_completion_fn: tb_completion_t,
        implementation: ContextImplementation,
        thread: Thread,
        available_messages: usize,

        pub fn create(
            allocator: std.mem.Allocator,
            cluster_id: u32,
            addresses: []const u8,
            num_packets: u32,
            on_completion_ctx: usize,
            on_completion_fn: tb_completion_t,
        ) Error!*Context {
            var context = allocator.create(Context) catch |err| {
                log.err("failed to create context: {}", .{err});
                return err;
            };
            errdefer allocator.destroy(context);

            context.allocator = allocator;
            context.client_id = std.crypto.random.int(u128);
            log.debug("init: initializing client_id={}.", .{context.client_id});

            log.debug("init: allocating tb_packets.", .{});
            context.packets = context.allocator.alloc(Packet, num_packets) catch |err| {
                log.err("failed to allocate tb_packets: {}", .{err});
                return err;
            };
            errdefer context.allocator.free(context.packets);

            log.debug("init: parsing vsr addresses.", .{});
            context.addresses = vsr.parse_addresses(context.allocator, addresses, config.replicas_max) catch |err| {
                log.err("failed to parse vsr addresses: {}.", .{err});
                return error.InvalidAddress;
            };
            errdefer context.allocator.free(context.addresses);

            log.debug("init: initializing IO.", .{});
            context.io = IO.init(32, 0) catch |err| {
                log.err("failed to initialize IO: {}.", .{err});
                return switch (err) {
                    error.ProcessFdQuotaExceeded => error.SystemResources,
                    error.Unexpected => error.Unexpected,
                    else => unreachable,
                };
            };
            errdefer context.io.deinit();

            log.debug("init: initializing MessagePool", .{});
            context.message_pool = MessagePool.init(allocator, .client) catch |err| {
                log.err("failed to initialize MessagePool: {}", .{err});
                return err;
            };
            errdefer context.message_pool.deinit(context.allocator);

            log.debug("init: initializing MessageBus.", .{});
            context.message_bus = MessageBus.init(
                context.allocator,
                cluster_id,
                .{ .client = context.client_id },
                &context.message_pool,
                Client.on_message,
                .{
                    .configuration = context.addresses,
                    .io = &context.io,
                },
            ) catch |err| {
                log.err("failed to initialize MessageBus: {}.", .{err});
                return err;
            };
            errdefer context.message_bus.deinit(context.allocator);

            log.debug("init: Initializing client(cluster_id={d}, client_id={d}, addresses={o})", .{
                cluster_id,
                context.client_id,
                context.addresses,
            });
            context.client = Client.init(
                allocator,
                context.client_id,
                cluster_id,
                @intCast(u8, context.addresses.len),
                &context.message_pool,
                .{
                    .configuration = context.addresses,
                    .io = &context.io,
                },
            ) catch |err| {
                log.err("failed to initalize client: {}", .{err});
                return err;
            };
            errdefer context.client.deinit(context.allocator);

            context.available_messages = message_pool.messages_max_client;
            context.on_completion_ctx = on_completion_ctx;
            context.on_completion_fn = on_completion_fn;
            context.implementation = .{
                .submit_fn = Context.on_submit,
                .deinit_fn = Context.on_deinit,
            };

            log.debug("init: initializing thread.", .{});
            context.thread.init(
                context,
            ) catch |err| {
                log.err("failed to initalize thread: {}", .{err});
                return err;
            };
            errdefer context.thread.deinit(context.allocator);

            return context;
        }

        pub fn destroy(self: *Context) void {
            self.thread.deinit();

            self.client.deinit(self.allocator);
            self.message_bus.deinit(self.allocator);
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
                self.client.tick();
                self.io.run_for_ns(config.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("IO.run() failed with {}", .{err});
                    @panic("IO.run() failed.");
                };
            }
        }

        pub fn request(self: *Context, packet: *Packet) void {
            const message = self.message_pool.get_message();
            defer self.message_pool.unref(message);
            self.available_messages -= 1;

            // Get the size of each request structure in the packet.data
            const request_size: usize = operation_size_of(packet.operation) orelse {
                return self.on_complete(packet, error.InvalidOperation);
            };

            // Make sure the packet.data size is correct.
            const readable = packet.data[0..packet.data_size];
            if (readable.len == 0 or readable.len % request_size != 0) {
                return self.on_complete(packet, error.InvalidDataSize);
            }

            // Make sure the packet.data wouldn't overflow a message.
            const writable = message.buffer[@sizeOf(Header)..];
            if (readable.len > writable.len) {
                return self.on_complete(packet, error.TooMuchData);
            }

            // Write the packet data to the message
            std.mem.copy(u8, writable, readable);
            const wrote = readable.len;

            // .. and submit the message for processing
            self.client.request(
                @bitCast(u128, UserData{
                    .self = self,
                    .packet = packet,
                }),
                Context.on_result,
                @intToEnum(Operation, packet.operation),
                message,
                wrote,
            );
        }

        fn on_result(raw_user_data: u128, op: Operation, results: Client.Error![]const u8) void {
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
            assert(self.available_messages < message_pool.messages_max_client);
            self.available_messages += 1;

            const tb_client = api.context_to_client(&self.implementation);
            const bytes = result catch |err| {
                packet.status = switch (err) {
                    // If there's too many requests, (re)try submitting the packet later
                    error.TooManyOutstandingRequests => {
                        return self.thread.submit(Packet.List.from(packet));
                    },
                    error.TooMuchData => .too_much_data,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                return (self.on_completion_fn)(self.on_completion_ctx, tb_client, packet, null, 0);
            };

            // The packet completed normally
            packet.status = .ok;
            (self.on_completion_fn)(self.on_completion_ctx, tb_client, packet, bytes.ptr, @intCast(u32, bytes.len));
        }

        fn on_submit(implementation: *ContextImplementation, packets: *tb_packet_list_t) void {
            const context = @fieldParentPtr(Context, "implementation", implementation);
            context.thread.submit(packets.*);
        }

        fn on_deinit(implementation: *ContextImplementation) void {
            const context = @fieldParentPtr(Context, "implementation", implementation);
            context.destroy();
        }
    };
}
