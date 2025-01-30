const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_signal);

const Signal = @import("./signal.zig").Signal;
const IO = @import("../../../io.zig").IO;
const stdx = @import("../../../stdx.zig");
const fuzz = @import("../../../testing/fuzz.zig");

const Context = struct {
    const Atomic = std.atomic.Value(enum(u8) {
        none,
        from_io_thread,
        from_user_thread,
    });

    io: IO,
    main_thread_id: std.Thread.Id,
    signal: Signal,

    random: std.Random,
    running_count: u32 = 0,
    stop_request: Atomic = Atomic.init(.none),
};

const Threads = stdx.BoundedArrayType(std.Thread, threads_limit);

const threads_limit = 8;
const stop_chance_percentage = 10;

pub fn main(args: fuzz.FuzzArgs) !void {
    var prng = std.rand.DefaultPrng.init(args.seed);
    const events_max = args.events_max orelse 1_000_000;

    var context: Context = .{
        .io = try IO.init(32, 0),
        .main_thread_id = std.Thread.getCurrentId(),
        .signal = undefined,
        .random = prng.random(),
    };
    defer context.io.deinit();

    try Signal.init(&context.signal, &context.io, on_signal);
    defer context.signal.deinit();

    const threads_max = context.random.intRangeAtMost(u32, 1, threads_limit);
    var threads: Threads = .{};
    for (0..threads_max) |_| {
        const thread: *std.Thread = threads.add_one_assume_capacity();
        thread.* = try std.Thread.spawn(.{}, notify, .{&context});
    }

    while (context.signal.status() != .stopped) {
        if (context.running_count >= events_max) {
            if (context.stop_request.cmpxchgStrong(
                .none,
                .from_io_thread,
                .acquire,
                .monotonic,
            ) == null) context.signal.stop();
        }
        try context.io.tick();
    }

    for (threads.slice()) |*thread| {
        thread.join();
    }

    assert(context.stop_request.load(.monotonic) != .none);
}

fn notify(context: *Context) void {
    const delay = 10 * std.time.ns_per_us;
    assert(std.Thread.getCurrentId() != context.main_thread_id);
    while (context.signal.status() != .stopped) {
        std.time.sleep(delay);

        // Chance to stop the signal between notifications.
        if (chance(context.random, stop_chance_percentage)) {
            if (context.stop_request.cmpxchgStrong(
                .none,
                .from_user_thread,
                .acquire,
                .monotonic,
            ) == null) context.signal.stop();
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

            // Chance to stop the signal while the notification is running.
            if (chance(context.random, stop_chance_percentage)) {
                if (context.stop_request.cmpxchgStrong(
                    .none,
                    .from_io_thread,
                    .acquire,
                    .monotonic,
                ) == null) context.signal.stop();
            }
        },
        .stop_requested => {
            // It's not possible if `stop` was called from the IO thread.
            assert(context.stop_request.load(.monotonic) == .from_user_thread);

            // Requested while running, so still counts as one event.
            context.running_count += 1;
        },
        .stopped => unreachable,
    }
}

/// Returns true, `p` percent of the time, else false.
fn chance(random: std.rand.Random, p: u8) bool {
    assert(p <= 100);
    return random.uintLessThanBiased(u8, 100) < p;
}
