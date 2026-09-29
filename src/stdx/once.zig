//! Vendored from Zig 0.14's `std.once`, which was removed in Zig 0.16.
const std = @import("std");

pub fn once(comptime f: fn () void) OnceType(f) {
    return .{};
}

pub fn OnceType(comptime f: fn () void) type {
    return struct {
        done: bool = false,
        mutex: std.Io.Mutex = .init,

        const Once = @This();

        pub fn call(self: *Once) void {
            if (@atomicLoad(bool, &self.done, .acquire)) return;
            return self.call_slow();
        }

        fn call_slow(self: *Once) void {
            @branchHint(.cold);
            std.Io.Threaded.mutexLock(&self.mutex);
            defer std.Io.Threaded.mutexUnlock(&self.mutex);

            // An unsynchronized load is fine here: it doesn't synchronize with the store,
            // but we've already synchronized via the mutex unlock.
            // <https://www.open-std.org/JTC1/SC22/WG21/docs/papers/2024/p2135r1.pdf#page=6>
            if (self.done) return;

            f();
            @atomicStore(bool, &self.done, true, .release);
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
