const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.tb_client_thread);

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

            log.debug("{}: init: initializing signal", .{context.client_id});
            try self.signal.init(&context.io, Self.on_signal);
            errdefer self.signal.deinit();

            log.debug("{}: init: spawning thread", .{context.client_id});
            self.thread = std.Thread.spawn(.{}, Context.run, .{context}) catch |err| {
                log.err("{}: failed to spawn thread: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
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
            while (self.context.messages_available > 0) {
                const packet = pending.pop() orelse self.submitted.pop() orelse break;
                self.context.request(packet);
            }
        }
    };
}
