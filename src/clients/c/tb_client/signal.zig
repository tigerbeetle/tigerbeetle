const std = @import("std");
const builtin = @import("builtin");
const IO = @import("../../../io.zig").IO;

const os = std.os;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

const log = std.log.scoped(.tb_client_signal);

/// A Signal is a way to trigger a registered callback on a tigerbeetle IO instance when 
/// notification occurs from another thread, generally to resolve IO.Completions on the 
/// tigerbeetle main thread.
pub const Signal = struct {
    const State = enum(u8) {
        waiting,
        notified,
        running,
    };

    io: *IO,
    on_signal_fn: fn (*Signal) void,

    event: i32,
    event_completion: IO.Completion,

    state: Atomic(State),
    terminated: Atomic(bool),

    pub fn init(self: *Signal, io: *IO, on_signal_fn: fn (*Signal) void) !void {
        self.io = io;
        self.on_signal_fn = on_signal_fn;

        self.event = try io.open_event(&self.event_completion, on_event);
        errdefer io.close_event(self.event, &self.event_completion);

        self.state = Atomic(State).init(.waiting);
        self.terminated = Atomic(bool).init(false);
    }

    pub fn deinit(self: *Signal) void {
        self.io.close_event(self.event, &self.event_completion);
        self.* = undefined;
    }

    /// Schedules the on_signal callback to be invoked on the IO thread.
    /// Safe to call from multiple threads.
    pub fn notify(self: *Signal) void {
        if (self.state.swap(.notified, .Release) == .waiting) {
            self.io.trigger_event(self.event, &self.event_completion);
        }
    }

    /// Stops the signal from firing on_signal callbacks on the IO thread.
    /// Safe to call from multiple threads.
    pub fn shutdown(self: *Signal) void {
        self.terminated.store(true, .Monotonic);
        self.notify();
    }

    /// Return true if the Signal was marked disabled and should no longer fire on_signal callbacks.
    /// Safe to call from multiple threads.
    pub fn is_shutdown(self: *const Signal) bool {
        return self.terminated.load(.Monotonic);
    }

    fn on_event(completion: *IO.Completion) IO.EventResponse {
        const self = @fieldParentPtr(Signal, "event_completion", completion);
        while (true) {
            // We must have been notified. Begin trying to run the signal handler.
            assert(self.state.swap(.running, .Acquire) == .notified);

            // Bail if we were shutdown.
            if (self.is_shutdown()) return .cancel;

            self.on_signal_fn(self);

            // Transition from running to waiting to return .listen for the event handler.
            // If this fails, it means another thread notified us while we were running so retry.
            const state = self.state.compareAndSwap(.running, .waiting, .Release, .Monotonic) orelse return .listen;
            assert(state == .notified);
        }
    }
};
