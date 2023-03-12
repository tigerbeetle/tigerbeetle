const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

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

        fn operation_cast(op: u8) ?Client.StateMachine.Operation {
            const allowed_operations = [_]Client.StateMachine.Operation{
                .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
            };

            inline for (allowed_operations) |operation| {
                if (op == @enumToInt(operation)) {
                    return operation;
                }
            } else return null;
        }

        const PacketError = Client.Error || error{
            OperationInvalid,
        };

        allocator: std.mem.Allocator,
        client_id: u128,
        packets: []Packet,

        addresses: []const std.net.Address,
        io: IO,
        message_pool: MessagePool,
        messages_available: bool,
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
                packets_count_max,
                &context.message_pool,
                .{
                    .configuration = context.addresses,
                    .io = &context.io,
                },
            );
            errdefer context.client.deinit(context.allocator);

            context.messages_available = true;
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
            // Check if it's a valid operation.
            const operation = operation_cast(packet.operation) orelse {
                return self.on_complete(packet, PacketError.OperationInvalid);
            };

            var batch_logical = self.client.get_batch(
                operation,
                packet.data_size,
            ) catch |err| {
                switch (err) {
                    error.BatchTooManyOutstanding => {
                        // If there's too many requests, (re)try submitting the packet later.
                        self.messages_available = false;
                        self.thread.retry.push(Packet.List.from(packet));
                    },
                    else => self.on_complete(packet, err),
                }
                return;
            };

            // Write the packet data to the message:
            const data = @ptrCast([*]const u8, packet.data)[0..packet.data_size];
            stdx.copy_disjoint(.exact, u8, batch_logical.slice(), data);

            // Submit the message for processing:
            self.client.submit_batch(
                @bitCast(u128, UserData{
                    .self = self,
                    .packet = packet,
                }),
                Context.on_result,
                batch_logical,
            );
        }

        fn on_result(
            raw_user_data: u128,
            op: Client.StateMachine.Operation,
            results: []const u8,
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
            // Signal to resume sending requests that was waiting for available messages.
            if (!self.messages_available) {
                self.messages_available = true;
                self.thread.signal.notify();
            }

            const tb_client = api.context_to_client(&self.implementation);
            const bytes = result catch |err| {
                packet.status = switch (err) {
                    PacketError.BatchBodySizeInvalid => .invalid_data_size,
                    PacketError.BatchBodySizeExceeded => .too_much_data,
                    PacketError.OperationInvalid => .invalid_operation,
                    PacketError.BatchTooManyOutstanding => unreachable,
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
