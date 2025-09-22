const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../tb_client.zig").vsr;
const TimeOS = vsr.time.TimeOS;
const IO = vsr.io.IO;

const Atomic = std.atomic.Value;

/// A Signal is a way to trigger a registered callback on a IO instance when notification
/// occurs from another thread.
pub const Signal = struct {
    io: *IO,
    completion: IO.Completion,
    event: IO.Event,
    event_state: Atomic(enum(u8) {
        running,
        waiting,
        notified,
        shutdown,
    }),

    listening: Atomic(bool),
    on_signal_fn: *const fn (*Signal) void,

    pub fn init(self: *Signal, io: *IO, on_signal_fn: *const fn (*Signal) void) !void {
        const event = try io.open_event();
        errdefer io.close_event(event);

        self.* = .{
            .io = io,
            .completion = undefined,
            .event = event,
            .event_state = @TypeOf(self.event_state).init(.running),
            .listening = Atomic(bool).init(true),
            .on_signal_fn = on_signal_fn,
        };

        self.wait();
    }

    pub fn deinit(self: *Signal) void {
        assert(self.event != IO.INVALID_EVENT);
        assert(self.status() == .stopped);

        self.io.close_event(self.event);
        self.* = undefined;
    }

    /// Requests to stop listening for notifications.
    /// The caller must continue processing `IO.run()` until `state() == .stopped`.
    /// Safe to call from multiple threads.
    pub fn stop(self: *Signal) void {
        const listening = self.listening.swap(false, .release);
        if (listening) {
            self.notify();
        }
    }

    /// Returns the current state.
    /// Safe to call from multiple threads.
    pub fn status(self: *const Signal) enum {
        /// Listening for event notifications.
        /// Call `notify()` to trigger the callback.
        running,
        /// `stop()` was called, but the event listener is still waiting for the IO operation
        /// to complete. Further calls to `notify()` have no effect.
        stop_requested,
        /// No pending listening events. It is safe to call `deinit()`.
        stopped,
    } {
        return switch (self.event_state.load(.acquire)) {
            .shutdown => .stopped,
            .running,
            .waiting,
            .notified,
            => if (self.listening.load(.acquire))
                .running
            else
                .stop_requested,
        };
    }

    /// Schedules the `on_signal` callback to be invoked on the IO thread.
    /// Calling `notify()` when `state() != .running` has no effect.
    /// Safe to call from multiple threads.
    pub fn notify(self: *Signal) void {
        // Try to transition from `waiting` to `notified`.
        // If it fails, analyze the current state to determine if a notification is needed.
        var state: @TypeOf(self.event_state.raw) = .waiting;
        while (self.event_state.cmpxchgStrong(
            state,
            .notified,
            .release,
            .acquire,
        )) |state_actual| {
            switch (state_actual) {
                .waiting, .running => state = state_actual, // Try again.
                .notified => return, // Already notified.
                .shutdown => return, // Ignore notifications after shutdown.
            }
        }

        if (state == .waiting) {
            self.io.event_trigger(self.event, &self.completion);
        }
    }

    fn wait(self: *Signal) void {
        // It is not guaranteed to be `running` here, as another caller might have requested
        // a stop during the callback.
        assert(self.status() != .stopped);

        const state = self.event_state.swap(.waiting, .acquire);
        self.io.event_listen(self.event, &self.completion, on_event);
        switch (state) {
            // We should be the only ones who could've started waiting.
            .waiting => unreachable,
            // Wait for a `notify`.
            .running => {},
            // A `notify` was already called in the meantime,
            // calling it again asynchronously.
            .notified => self.notify(),
            // Cannot be called after shutdown.
            .shutdown => unreachable,
        }
    }

    fn on_event(completion: *IO.Completion) void {
        const self: *Signal = @fieldParentPtr("completion", completion);
        const listening: bool = self.listening.load(.acquire);
        const state = self.event_state.cmpxchgStrong(
            .notified,
            if (listening) .running else .shutdown,
            .release,
            .acquire,
        ) orelse {
            if (listening) {
                (self.on_signal_fn)(self);
                self.wait();
            }
            return;
        };

        switch (state) {
            .running => unreachable, // Multiple racing calls to on_signal().
            .waiting => unreachable, // on_signal() called without transitioning to a waking state.
            .notified => unreachable, // Not possible due to CAS semantics.
            .shutdown => unreachable, // Shutdown is a final state.
        }
    }
};

test "signal" {
    try struct {
        const Context = @This();

        io: IO,
        count: u32 = 0,
        main_thread_id: std.Thread.Id,
        signal: Signal,

        const delay = 5 * std.time.ns_per_ms;
        const events_count = 5;

        fn run_test() !void {
            var self: Context = .{
                .io = try IO.init(32, 0),
                .main_thread_id = std.Thread.getCurrentId(),
                .signal = undefined,
            };
            defer self.io.deinit();

            try Signal.init(&self.signal, &self.io, on_signal);
            defer self.signal.deinit();

            var time_os = TimeOS{};
            const timer = time_os.time();
            const start = timer.monotonic();

            const thread = try std.Thread.spawn(.{}, Context.notify, .{&self});

            // Wait for the number of events to complete.
            while (self.count < events_count) try self.io.run();

            // Begin shutdown and keep ticking until it's completed.
            self.signal.stop();
            while (self.signal.status() != .stopped) try self.io.run();
            thread.join();

            // Notify after shutdown should be ignored.
            self.signal.notify();

            // Make sure the event was triggered multiple times.
            assert(self.count == events_count);

            // Make sure at least some time has passed.
            const elapsed = timer.monotonic().duration_since(start);
            assert(elapsed.ns >= delay);
        }

        fn notify(self: *Context) void {
            assert(std.Thread.getCurrentId() != self.main_thread_id);
            while (self.signal.status() != .stopped) {
                std.time.sleep(delay + 1);

                // Triggering the event:
                self.signal.notify();

                // The same signal may be triggered multiple times,
                // but it should only fire once.
                self.signal.notify();
            }
        }

        fn on_signal(signal: *Signal) void {
            const self: *Context = @fieldParentPtr("signal", signal);
            assert(std.Thread.getCurrentId() == self.main_thread_id);
            switch (self.signal.status()) {
                .running => {
                    assert(self.count < events_count);
                    self.count += 1;
                },
                .stop_requested => assert(self.count == events_count),
                .stopped => unreachable,
            }
        }
    }.run_test();
}
