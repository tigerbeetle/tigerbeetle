const std = @import("std");
const assert = std.debug.assert;

/// Using blocking syscalls, write a page, fsync the write, then read the page back in.
/// Rinse and repeat to iterate across a large file.
///
/// This should be the fastest candidate after io_uring, faster than Zig's evented I/O on Linux
/// since it does not context switch to an async I/O thread.
pub fn main() !void {
    if (std.builtin.os.tag != .linux) return error.LinuxRequired;

    const size: usize = 256 * 1024 * 1024;
    const page: usize = 4096;
    const runs: usize = 5;

    const path = "file_blocking";
    const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var buffer_w = [_]u8{1} ** page;
    var buffer_r = [_]u8{0} ** page;

    var run: usize = 0;
    while (run < runs) : (run += 1) {
        var start = std.time.milliTimestamp();
        var pages: usize = 0;
        var syscalls: usize = 0;
        var offset: usize = 0;

        while (offset < size) {
            // We don't want to undercount syscalls and therefore we don't use pwriteAll() or
            // preadAll() because these might use more than one syscall.
            var wrote = try file.pwrite(buffer_w[0..page], offset);
            assert(wrote == page);
            try std.os.fsync(file.handle);
            var read = try file.pread(buffer_r[0..page], offset);
            assert(read == page);
            pages += 1;
            syscalls += 3;
            offset += page;
        }

        std.debug.print(
            "fs blocking: write({})/fsync/read({}) * {} pages = {} syscalls in {}ms\n",
            .{ page, page, pages, size, syscalls, std.time.milliTimestamp() - start }
        );
    }
}
