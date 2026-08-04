//! Vendored from Zig 0.14's `std.once`, which was removed in Zig 0.16.
const std = @import("std");

pub fn once(comptime f: fn () void) OnceType(f) {
    return .{};
}

pub fn OnceType(comptime f: fn () void) type {
    return struct {
        done: bool = false,
        mutex: std.Io.Mutex = .init,

        pub fn call(self: *@This()) void {
            if (@atomicLoad(bool, &self.done, .acquire)) return;
            return self.call_slow();
        }

        fn call_slow(self: *@This()) void {
            @branchHint(.cold);
            // Callers might not have an `Io` instance in scope (for example, arbitrary JVM
            // threads). The single-threaded `Io` still locks via the OS futex.
            const io = std.Io.Threaded.global_single_threaded.io();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);

            if (!self.done) {
                f();
                @atomicStore(bool, &self.done, true, .release);
            }
        }
    };
}

test once {
    const Counter = struct {
        var count: u32 = 0;

        fn increment() void {
            count += 1;
        }
    };

    var counter_once = once(Counter.increment);
    counter_once.call();
    counter_once.call();
    try std.testing.expectEqual(1, Counter.count);
}
