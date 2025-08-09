const std = @import("std");
const assert = std.debug.assert;

const Signal = @import("./signal.zig").Signal;
const IO = @import("../../../io.zig").IO;
const stdx = @import("stdx");
const fuzz = @import("../../../testing/fuzz.zig");

const threads_limit = 8;
const Threads = stdx.BoundedArrayType(std.Thread, threads_limit);

const StopRequest = enum(u8) {
    none,
    io_thread,
    user_thread,
};

const Context = struct {
    const Atomic = std.atomic.Value(StopRequest);

    main_thread_id: std.Thread.Id,
    signal: Signal,
    running_count: u32 = 0,
    stop_request: Atomic = Atomic.init(.none),
};

pub fn main(_: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    var prng = stdx.PRNG.from_seed(args.seed);
    const events_max = args.events_max orelse 100;

    for (0..events_max) |_| {
        var io = try IO.init(32, 0);
        defer io.deinit();

        var context: Context = .{
            .main_thread_id = std.Thread.getCurrentId(),
            .signal = undefined,
        };

        try Signal.init(&context.signal, &io, on_signal);
        defer context.signal.deinit();

        const threads_max = prng.range_inclusive(u32, 1, threads_limit);
        var threads: Threads = .{};
        for (0..threads_max) |_| {
            const thread = try std.Thread.spawn(.{}, notify, .{&context});
            threads.push(thread);
        }

        while (context.signal.status() != .stopped) {
            if (context.running_count > 0) {
                // Setting a random `stop_request`.
                _ = context.stop_request.cmpxchgStrong(
                    .none,
                    prng.enum_uniform(StopRequest),
                    .acquire,
                    .monotonic,
                );
            }

            const tick_us = 10;
            try io.run_for_ns(tick_us * std.time.ns_per_us);
        }

        for (threads.slice()) |*thread| {
            thread.join();
        }

        assert(context.running_count > 0);
        assert(context.stop_request.load(.monotonic) != .none);
    }
}

fn notify(context: *Context) void {
    assert(std.Thread.getCurrentId() != context.main_thread_id);
    while (context.signal.status() != .stopped) {
        const delay_us = 1; // Shorter than `tick_us`.
        std.time.sleep(delay_us * std.time.ns_per_us);

        if (context.stop_request.load(.monotonic) == .user_thread) {
            // Stop can be called by multiple threads.
            context.signal.stop();
        }

        // Notify has no effect if called after `stop()`.
        context.signal.notify();
    }
}

fn on_signal(signal: *Signal) void {
    const context: *Context = @fieldParentPtr("signal", signal);
    assert(std.Thread.getCurrentId() == context.main_thread_id);
    switch (context.signal.status()) {
        .running => {
            context.running_count += 1;
            if (context.stop_request.load(.monotonic) == .io_thread) {
                // Stop the signal while the notification is running.
                context.signal.stop();
            }
        },
        .stop_requested => {
            // It's not possible if `stop` was called from the IO thread.
            assert(context.stop_request.load(.monotonic) == .user_thread);

            // Requested while running, so still counts as one event.
            context.running_count += 1;
        },
        .stopped => unreachable,
    }
}
