const std = @import("std");
const assert = std.debug.assert;

const config = @import("../../config.zig");
const log = std.log.scoped(.tb_client);

const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;

pub fn ThreadType(
    comptime Context: type,
) type {
    return struct {
        const Self = @This();

        context: *Context,

        retry: Packet.List,
        submitted: Packet.Stack,

        signal: Signal,
        thread: std.Thread,

        pub fn init(
            self: *Self,
            context: *Context,
        ) !void {
            self.context = context;
            self.retry = .{};
            self.submitted = .{};

            log.debug("init: initializing Signal.", .{});
            self.signal.init(&context.io, Self.on_signal) catch |err| {
                log.err("failed to initialize Signal: {}.", .{err});
                return err;
            };
            errdefer self.signal.deinit();

            log.debug("init: spawning Context thread.", .{});
            self.thread = std.Thread.spawn(.{}, Context.run, .{context}) catch |err| {
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

            self.* = undefined;
        }

        pub fn submit(self: *Self, list: Packet.List) void {
            if (list.peek() == null) return;
            self.submitted.push(list);
            self.signal.notify();
        }

        fn on_signal(signal: *Signal) void {
            const self = @fieldParentPtr(Self, "signal", signal);
            self.context.tick();

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
            while (self.context.available_messages > 0) {
                const packet = pending.pop() orelse self.submitted.pop() orelse break;
                self.context.request(packet);
            }
        }
<<<<<<< HEAD
=======

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

            // Signal to resume sending requests that was waiting for available messages.
            if (self.available_messages == 1) self.signal.notify();

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
>>>>>>> 05aee274e84125edca674c2eb7e1da18b06e8707
    };
}
