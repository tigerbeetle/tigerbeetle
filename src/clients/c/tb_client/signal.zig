const std = @import("std");

const vsr = @import("../tb_client.zig").vsr;
const IO = vsr.io.IO;

const assert = std.debug.assert;
const Atomic = std.atomic.Value;

/// A Signal is a way to trigger a registered callback on a tigerbeetle IO instance
/// when notification occurs from another thread.
/// It does this by using OS sockets (which are thread safe)
/// to resolve IO.Completions on the tigerbeetle thread.
pub const Signal = struct {
    io: *IO,
    event: i32,
    completion: IO.Completion,

    on_signal_fn: *const fn (*Signal) void,
    state: Atomic(enum(u8) {
        running,
        waiting,
        notified,
    }),

    pub fn init(self: *Signal, io: *IO, on_signal_fn: *const fn (*Signal) void) !void {
        self.io = io;
        self.event = try io.open_event(&self.completion, on_event);
        errdefer io.close_event(self.event, &self.completion);

        self.on_signal_fn = on_signal_fn;
        self.state = @TypeOf(self.state).init(.waiting);
    }

    pub fn deinit(self: *Signal) void {
        self.io.close_event(self.event, &self.completion);
        self.* = undefined;
    }

    /// Schedules the on_signal callback to be invoked on the IO thread.
    /// Safe to call from multiple threads.
    pub fn notify(self: *Signal) void {
        if (self.state.swap(.notified, .release) == .waiting) {
            self.io.event_trigger(self.event, &self.completion);
        }
    }

    fn on_event(completion: *IO.Completion) void {
        const self: *Signal = @fieldParentPtr("completion", completion);
        while (true) {
            // We must have been notified. Begin trying to run the signal handler.
            assert(self.state.swap(.running, .acquire) == .notified);
            self.on_signal_fn(self);

            // Transition from `running` to `waiting` to return listen for the event handler.
            // If this fails, it means another thread notified us while we were running so retry.
            const state = self.state.cmpxchgStrong(
                .running,
                .waiting,
                .release,
                .monotonic,
            ) orelse return;
            assert(state == .notified);
        }
    }
};
