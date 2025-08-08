const builtin = @import("builtin");
const std = @import("std");

const assert = std.debug.assert;

var original_posix_sigill_handler: ?*const fn (
    i32,
    *const std.posix.siginfo_t,
    ?*anyopaque,
) callconv(.C) void = null;

fn handle_sigill_windows(
    info: *std.os.windows.EXCEPTION_POINTERS,
) callconv(std.os.windows.WINAPI) c_long {
    if (info.ExceptionRecord.ExceptionCode == std.os.windows.EXCEPTION_ILLEGAL_INSTRUCTION) {
        display_message();
    }
    return std.os.windows.EXCEPTION_CONTINUE_SEARCH;
}

fn handle_sigill_posix(
    sig: i32,
    info: *const std.posix.siginfo_t,
    ctx_ptr: ?*anyopaque,
) callconv(.C) noreturn {
    display_message();
    original_posix_sigill_handler.?(sig, info, ctx_ptr);
    unreachable;
}

fn display_message() void {
    std.log.err("", .{});
    std.log.err("TigerBeetle's binary releases are compiled targeting modern CPU", .{});
    std.log.err("instructions such as NEON / AES on ARM and x86_64_v3 / AES-NI on", .{});
    std.log.err("x86-64.", .{});
    std.log.err("", .{});
    std.log.err("These instructions can be unsupported on older processors, leading to", .{});
    std.log.err("\"illegal instruction\" panics.", .{});
    std.log.err("", .{});
}

pub fn install_handler() !void {
    switch (builtin.os.tag) {
        .windows => {
            // The 1 indicates this will run first, before Zig's built in handler. Internally,
            // it returns EXCEPTION_CONTINUE_SEARCH so that Zig's handler is called next.
            _ = std.os.windows.kernel32.AddVectoredExceptionHandler(1, handle_sigill_windows);
        },
        .linux, .macos => {
            // For Linux / macOS, save the original signal handler so it can be called by this
            // new handler once the log message has been printed.
            assert(original_posix_sigill_handler == null);
            var act = std.posix.Sigaction{
                .handler = .{ .sigaction = handle_sigill_posix },
                .mask = std.posix.empty_sigset,
                .flags = (std.posix.SA.SIGINFO | std.posix.SA.RESTART | std.posix.SA.RESETHAND),
            };

            var oact: std.posix.Sigaction = undefined;

            std.posix.sigaction(std.posix.SIG.ILL, &act, &oact);
            original_posix_sigill_handler = oact.handler.sigaction.?;
        },
        else => unreachable,
    }
}
