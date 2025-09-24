const std = @import("std");
const builtin = @import("builtin");
const jni = @import("jni.zig");

const log = std.log.scoped(.tb_client_jni);
const assert = std.debug.assert;

/// Helper for managing the `AttachCurrentThread`/`DetachCurrentThread` lifecycle
/// when the JNI layer is unaware of when the native thread exits.
/// https://developer.android.com/training/articles/perf-jni#threads
pub const JNIThreadCleaner = struct {
    var tls_key: ?tls.Key = null;
    var create_key_once = std.once(create_key);

    /// This function calls `AttachCurrentThreadAsDaemon` to attach the current native thread to
    /// the JVM as a daemon thread. It also registers a callback to call `DetachCurrentThread`
    /// when the thread exits (e.g., when the client is closed or evicted).
    pub fn attach_current_thread_with_cleanup(jvm: *jni.JavaVM) *jni.JNIEnv {
        // Create the tls key once per JVM.
        create_key_once.call();

        // Set the JVM handler to the thread-local storage slot for each time a native
        // thread is started.
        tls.set_key(tls_key.?, jvm);

        return attach_current_thread(jvm);
    }

    /// Create the thread-local storage key and the corresponding destructor callback.
    /// Note: We don't need to delete the key because the JNI module cannot be unloaded,
    /// so it will always be available for the duration of the JVM process.
    fn create_key() void {
        assert(tls_key == null);
        tls_key = tls.create_key(&destructor_callback);
    }

    // Will be called by the OS with the JVM handler when the thread finalizes.
    fn destructor_callback(jvm: *anyopaque) callconv(.c) void {
        assert(tls_key != null);
        detach_current_thread(@ptrCast(jvm));
    }

    fn attach_current_thread(jvm: *jni.JavaVM) *jni.JNIEnv {
        var env: *jni.JNIEnv = undefined;
        const jni_result = jvm.attach_current_thread_as_daemon(&env, null);
        if (jni_result != .ok) {
            const message = "Unexpected result calling JavaVM.AttachCurrentThreadAsDaemon";
            log.err(
                message ++ "; Error = {} ({s})",
                .{ @intFromEnum(jni_result), @tagName(jni_result) },
            );
            @panic("JNI: " ++ message);
        }

        return env;
    }

    fn detach_current_thread(jvm: *jni.JavaVM) void {
        const jni_result = jvm.detach_current_thread();
        if (jni_result != .ok) {
            const message = "Unexpected result calling JavaVM.DetachCurrentThread";
            log.err(
                message ++ "; Error = {} ({s})",
                .{ @intFromEnum(jni_result), @tagName(jni_result) },
            );
            @panic("JNI: " ++ message);
        }
    }

    /// Thread-local storage abstraction,
    /// based on `pthread_key_create` for Linux/MacOS and `FlsAlloc` for Windows.
    const tls = switch (builtin.os.tag) {
        .linux, .macos => struct {
            const Key = std.c.pthread_key_t;

            fn create_key(destructor: ?*const fn (value: *anyopaque) callconv(.c) void) Key {
                var key: Key = undefined;
                const ret = std.c.pthread_key_create(&key, destructor);
                if (ret != .SUCCESS) {
                    const message = "Unexpected result calling pthread_key_create";
                    log.err(message ++ "; Error = {} ({s})", .{
                        @intFromEnum(ret),
                        @tagName(ret),
                    });
                    @panic("JNI: " ++ message);
                }

                return key;
            }

            fn set_key(key: Key, value: *anyopaque) void {
                const ret = std.c.pthread_setspecific(key, value);
                if (ret != 0) {
                    const message = "Unexpected result calling pthread_setspecific";
                    log.err(message ++ "; Error = {}", .{ret});
                    @panic("JNI: " ++ message);
                }
            }
        },
        .windows => struct {
            const windows = struct {
                const FLS_OUT_OF_INDEXES: std.os.windows.DWORD = 0xffffffff;
                // https://learn.microsoft.com/en-us/windows/win32/api/fibersapi/nf-fibersapi-flsalloc
                extern "kernel32" fn FlsAlloc(
                    ?*const fn (value: *anyopaque) callconv(.c) void,
                ) callconv(.c) std.os.windows.DWORD;
                // https://learn.microsoft.com/en-us/windows/win32/api/fibersapi/nf-fibersapi-flssetvalue
                extern "kernel32" fn FlsSetValue(
                    std.os.windows.DWORD,
                    *anyopaque,
                ) callconv(.c) std.os.windows.BOOL;
            };

            const Key = std.os.windows.DWORD;

            fn create_key(destructor: ?*const fn (value: *anyopaque) callconv(.c) void) Key {
                const key = windows.FlsAlloc(destructor);
                if (key == windows.FLS_OUT_OF_INDEXES) {
                    const message = "Unexpected result calling FlsAlloc";
                    log.err(message ++ "; Error = {}", .{key});
                    @panic("JNI: " ++ message);
                }

                return key;
            }

            fn set_key(key: Key, value: *anyopaque) void {
                const ret = windows.FlsSetValue(key, value);
                if (ret == std.os.windows.FALSE) {
                    const message = "Unexpected result calling FlsSetValue";
                    log.err(message ++ "; Error = {}", .{ret});
                    @panic("JNI: " ++ message);
                }
            }
        },
        else => unreachable,
    };
};

test "JNIThreadCleaner:tls" {
    const tls = JNIThreadCleaner.tls;
    const TestContext = struct {
        const TestContext = @This();

        var tls_key: ?tls.Key = null;
        var event: std.Thread.ResetEvent = .{};

        counter: std.atomic.Value(u32),

        fn init() TestContext {
            if (tls_key == null) {
                tls_key = tls.create_key(&destructor_callback);
            }

            return .{
                .counter = std.atomic.Value(u32).init(0),
            };
        }

        fn thread_main(self: *TestContext) void {
            tls.set_key(tls_key.?, self);
            event.wait();
        }

        fn destructor_callback(tls_value: *anyopaque) callconv(.c) void {
            assert(tls_key != null);

            const self: *TestContext = @ptrCast(@alignCast(tls_value));
            _ = self.counter.fetchAdd(1, .monotonic);
        }
    };

    var context = TestContext.init();
    var threads: [10]std.Thread = undefined;
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, TestContext.thread_main, .{&context});
    }

    // Assert that the callback only fires when the thread finishes.
    try std.testing.expect(context.counter.load(.monotonic) == 0);

    // Signal all threads to complete and wait for them.
    TestContext.event.set();
    for (&threads) |*thread| {
        thread.join();
    }

    // Assert that all callbacks have fired.
    try std.testing.expect(context.counter.load(.monotonic) == threads.len);
}
