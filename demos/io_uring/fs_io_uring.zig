const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("io_uring.zig");

/// Using non-blocking io_uring, write a page, fsync the write, then read the page back in.
/// Rinse and repeat to iterate across a large file.
///
/// Note that this is all non-blocking, but with a pure single-threaded event loop, something that
/// is not otherwise possible on Linux for cached I/O.
///
/// There are several io_uring optimizations that we don't take advantage of here:
/// * SQPOLL to eliminate submission syscalls entirely.
/// * Registered file descriptors to eliminate atomic file referencing in the kernel.
/// * Registered buffers to eliminate page mapping in the kernel.
/// * Finegrained submission, at present we simply submit a batch of events then wait for a batch.
///   We could also submit a batch of events, and then submit partial batches as events complete.
pub fn main() !void {
    if (std.builtin.os.tag != .linux) return error.LinuxRequired;

    const size: usize = 256 * 1024 * 1024;
    const page: usize = 4096;
    const runs: usize = 5;

    const path = "file_io_uring";
    const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};
    const fd = file.handle;

    var buffer_w = [_]u8{1} ** page;
    var buffer_r = [_]u8{0} ** page;

    const event_w = 1;
    const event_f = 2;
    const event_r = 3;

    var cqes: [512]io_uring_cqe = undefined;
    var ring = try IO_Uring.init(cqes.len, 0);

    var run: usize = 0;
    while (run < runs) : (run += 1) {
        var start = std.time.milliTimestamp();
        var pages: usize = 0;
        var syscalls: usize = 0;
        var offset_submitted: usize = 0;
        var offset_completed: usize = 0;

        event_loop:
        while (true) {
            // Consume groups of completed events:
            const count = try ring.copy_cqes(cqes[0..], 0);
            var i: usize = 0;
            while (i < count) : (i += 1) {
                const cqe = cqes[i];
                switch (cqe.user_data) {
                    event_w => assert(cqe.res == page),
                    event_f => assert(cqe.res == 0),
                    event_r => {
                        assert(cqe.res == page);
                        pages += 1;
                        offset_completed += page;
                        if (offset_completed >= size) {
                            assert(offset_completed == offset_submitted);
                            break :event_loop;
                        }
                    },
                    else => {
                        std.debug.print("ERROR {}\n", .{ cqe });
                        std.os.exit(1);
                    }
                }
            }

            // Enqueue groups of read/fsync/write calls within the event loop:
            var events: u32 = 0;
            while (offset_submitted < size and events + 3 <= cqes.len) {
                var w = try ring.write(event_w, fd, buffer_w[0..], offset_submitted);
                w.flags |= std.os.linux.IOSQE_IO_LINK;
                var f = try ring.fsync(event_f, fd, 0);
                f.flags |= std.os.linux.IOSQE_IO_LINK;
                var r = try ring.read(event_r, fd, buffer_r[0..], offset_submitted);
                offset_submitted += page;
                events += 3;
            }

            // Up until now, we have only appended to the SQ ring buffer, but without any syscalls.
            // Now submit and wait for these groups of events with a single syscall.
            // If we used SQPOLL, we wouldn't need this syscall to submit, only to wait, which
            // `copy_cqes(N)` also supports. At present, `copy_cqes(0)` above does not wait because
            // we do that here using `submit_and_wait(N)`.
            _ = try ring.submit_and_wait(events);
            syscalls += 1;
        }

        std.debug.print(
            "fs io_uring: write({})/fsync/read({}) * {} pages = {} syscalls in {}ms\n",
            .{ page, page, pages, syscalls, std.time.milliTimestamp() - start }
        );
    }
}
