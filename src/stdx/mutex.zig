//! Mutex implementation.
//!
//! This is primarily for our C library, where we need to synchronize with
//! threads, we don't control. That's why we don't want to bind to our (or std) IO.

const std = @import("std");
const assert = std.debug.assert;

inner: std.Io.Mutex = .init,

const Mutex = @This();

pub fn lock(mutex: *Mutex) void {
    std.Io.Threaded.mutexLock(&mutex.inner);
}

pub fn unlock(mutex: *Mutex) void {
    std.Io.Threaded.mutexUnlock(&mutex.inner);
}

test Mutex {
    const T = struct {
        mutex: Mutex,
        counter: u32,

        const thread_count = 10;
        const iteration_count = 100;

        fn thread_main(t: *@This()) void {
            for (0..iteration_count) |_| {
                t.mutex.lock();
                defer t.mutex.unlock();

                t.counter += 1;
            }
        }
    };

    var t = .{
        .mutex = .{},
        .counter = 0,
    };

    var threads: [T.thread_count]std.Thread = undefined;
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, T.thread_main, .{&t});
    }
    for (&threads) |*thread| {
        thread.join();
    }
    assert(t.counter == T.thread_count * T.iteration_count);
}
