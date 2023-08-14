const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const os = std.os;

const FIFO = @import("fifo.zig").FIFO;
const IO_Linux = @import("io/linux.zig").IO;
const IO_Darwin = @import("io/darwin.zig").IO;
const IO_Windows = @import("io/windows.zig").IO;

pub const IO = switch (builtin.target.os.tag) {
    .linux => IO_Linux,
    .windows => IO_Windows,
    .macos, .tvos, .watchos, .ios => IO_Darwin,
    else => @compileError("IO is not supported for platform"),
};

pub fn buffer_limit(buffer_len: usize) usize {
    // Linux limits how much may be written in a `pwrite()/pread()` call, which is `0x7ffff000` on
    // both 64-bit and 32-bit systems, due to using a signed C int as the return value, as well as
    // stuffing the errno codes into the last `4096` values.
    // Darwin limits writes to `0x7fffffff` bytes, more than that returns `EINVAL`.
    // The corresponding POSIX limit is `std.math.maxInt(isize)`.
    const limit = switch (builtin.target.os.tag) {
        .linux => 0x7ffff000,
        .macos, .ios, .watchos, .tvos => std.math.maxInt(i32),
        else => std.math.maxInt(isize),
    };
    return @min(limit, buffer_len);
}

test "I/O" {
    _ = @import("io/test.zig");
}
