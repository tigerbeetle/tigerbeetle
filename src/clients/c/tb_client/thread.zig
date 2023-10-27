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

        submitted: Packet.SubmissionStack,

        signal: Signal,
        thread: std.Thread,

        pub fn init(
            self: *Self,
            context: *Context,
        ) !void {
            self.context = context;
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
                    error.SystemResources,
                    error.ThreadQuotaExceeded,
                    error.LockedMemoryLimitExceeded,
                    => error.SystemResources,
                };
            };
        }

        pub fn deinit(self: *Self) void {
            self.thread.join();
            self.signal.deinit();

            self.* = undefined;
        }

        pub fn submit(self: *Self, packet: *Packet) void {
            self.submitted.push(packet);
            self.signal.notify();
        }

        fn on_signal(signal: *Signal) void {
            const self = @fieldParentPtr(Self, "signal", signal);
            self.context.tick();

            // Process packets from either pending or submitted as long as we have messages.
            while (self.submitted.pop()) |packet| {
                self.context.request(packet) catch |err| switch (err) {
                    error.TooManyOutstanding => {
                        self.submitted.push(packet);
                        break;
                    },
                };
            }
        }
    };
}
