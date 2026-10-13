const std = @import("std");
const builtin = @import("builtin");
const os = std.os;

const IO_Linux = @import("io/linux.zig").IO;
const IO_LinuxEpoll = @import("io/linux_epoll.zig").IO;
const IO_Darwin = @import("io/darwin.zig").IO;
const IO_Windows = @import("io/windows.zig").IO;
const IO_Testing = @import("io/io_testing.zig").IO;

pub const IO = switch (builtin.target.os.tag) {
    .linux => IO_Linux,
    .windows => IO_Windows,
    .macos, .tvos, .watchos, .ios => IO_Darwin,
    else => @compileError("IO is not supported for platform"),
};

// On Linux, clients use the lightweight epoll-only IO backend instead of io_uring, for
// compatibility with older kernels and restricted containers (e.g. Docker with io_uring disabled).
// All other platforms always use their native IO.
pub const ClientIO = if (builtin.target.os.tag == .linux) IO_LinuxEpoll else IO;

pub const IOTest = IO_Testing;
