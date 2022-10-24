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

pub fn ThreadType(
    comptime StateMachine: type,
    comptime MessageBus: type,
) type {
    return struct {
        pub const Client = vsr.Client(StateMachine, MessageBus);
        pub const Operation = StateMachine.Operation;

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

        const Self = @This();

        allocator: std.mem.Allocator,
        client_id: u128,
        packets: []Packet,

        addresses: []std.net.Address,
        io: IO,
        message_pool: MessagePool,
        message_bus: MessageBus,
        client: Client,

        retry: Packet.List,
        submitted: Packet.Stack,
        available_messages: usize,
        on_completion_fn: CompletionFn,

        signal: Signal,
        thread: std.Thread,

        pub const CompletionFn = *const fn (
            client_thread: *Self,
            packet: *Packet,
            result: ?[]const u8,
        ) void;

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
            on_completion_fn: CompletionFn,
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
            const address_limit = std.mem.count(u8, addresses, ",") + 1;
            self.addresses = vsr.parse_addresses(self.allocator, addresses, address_limit) catch |err| {
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

            log.debug("init: initializing MessagePool", .{});
            self.message_pool = MessagePool.init(allocator, .client) catch |err| {
                log.err("failed to initialize MessagePool: {}", .{err});
                return err;
            };
            errdefer self.message_pool.deinit(self.allocator);

            log.debug("init: initializing MessageBus.", .{});
            self.message_bus = MessageBus.init(
                self.allocator,
                cluster_id,
                .{ .client = self.client_id },
                &self.message_pool,
                Client.on_message,
                .{
                    .configuration = self.addresses,
                    .io = &self.io,
                },
            ) catch |err| {
                log.err("failed to initialize message bus: {}.", .{err});
                return err;
            };
            errdefer self.message_bus.deinit(self.allocator);

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
                &self.message_pool,
                .{
                    .configuration = self.addresses,
                    .io = &self.io,
                },
            ) catch |err| {
                log.err("failed to initalize client: {}", .{err});
                return err;
            };
            errdefer self.client.deinit(self.allocator);

            self.retry = .{};
            self.submitted = .{};
            self.available_messages = message_pool.messages_max_client;
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
                    error.SystemResources, error.ThreadQuotaExceeded, error.LockedMemoryLimitExceeded => error.SystemResources,
                };
            };
        }

        pub fn deinit(self: *Self) void {
            self.signal.shutdown();
            self.thread.join();
            self.signal.deinit();

            self.client.deinit(self.allocator);
            self.message_bus.deinit(self.allocator);
            self.message_pool.deinit(self.allocator);
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
                defer self.client.unref(message);

                self.available_messages -= 1;
                self.request(packet, message);
            }
        }

        fn request(self: *Self, packet: *Packet, message: *Message) void {
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
                Self.on_result,
                @intToEnum(Operation, packet.operation),
                message,
                wrote,
            );
        }

        const UserData = packed struct {
            self: *Self,
            packet: *Packet,
        };

        fn on_result(raw_user_data: u128, op: Operation, results: Client.Error![]const u8) void {
            const user_data = @bitCast(UserData, raw_user_data);
            const self = user_data.self;
            const packet = user_data.packet;

            assert(packet.operation == @enumToInt(op));
            self.on_complete(packet, results);
        }

        const PacketError = Client.Error || error{
            TooMuchData,
            InvalidOperation,
            InvalidDataSize,
        };

        fn on_complete(
            self: *Self,
            packet: *Packet,
            result: PacketError![]const u8,
        ) void {
            assert(self.available_messages < message_pool.messages_max_client);
            self.available_messages += 1;

            const bytes = result catch |err| {
                packet.status = switch (err) {
                    // If there's too many requests, (re)try submitting the packet later
                    error.TooManyOutstandingRequests => {
                        return self.retry.push(Packet.List.from(packet));
                    },
                    error.TooMuchData => .too_much_data,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                return self.on_completion_fn(self, packet, null);
            };

            // The packet completed normally
            packet.status = .ok;
            self.on_completion_fn(self, packet, bytes);
        }
    };
}
