//! This tests:
//!   - Context.init / deinit
//!   - pwrite
//!   - pread
//!   - fdatasync
//!   - fsync
//!   - poll
//!   - getEvents
//!   - cancel, best effort
//!
//! Notes:
//!   - This intentionally does NOT use O_DIRECT, so it should be easy to run.
//!   - Linux AIO is best with O_DIRECT, but this test is for API/sandbox/seccomp smoke testing.
//!   - cancel may return InvalidArgument / Again / UnknownErrno depending on timing and kernel behavior.

const std = @import("std");
const stdx = @import("stdx");
const aio = stdx.linux_aio;

fn expect(ok: bool, comptime msg: []const u8) !void {
    if (!ok) {
        std.debug.print("FAIL: " ++ msg ++ "\n", .{});
        return error.TestFailed;
    }
}

fn drainExactly(ctx: aio.Context, want: usize, label: []const u8) ![8]aio.Event {
    var events: [8]aio.Event = undefined;
    var got: usize = 0;

    while (got < want) {
        const n = try ctx.getEvents(1, events[got..], null);
        std.debug.print("{s}: got {} event(s)\n", .{ label, n });
        got += n;
    }

    return events;
}

fn printCompletion(ev: aio.Event, label: []const u8) !usize {
    const result = try ev.result();
    std.debug.print(
        "{s}: data=0x{x}, iocb=0x{x}, result={}\n",
        .{ label, ev.userData(), @intFromPtr(ev.iocb()), result },
    );
    return result;
}

pub fn main() !void {
    std.debug.print("linux aio smoke test starting\n", .{});

    var ctx = aio.Context.init(32) catch |err| {
        std.debug.print("Context.init failed: {s}\n", .{@errorName(err)});
        std.debug.print("If this is PermissionDenied, your container/seccomp profile blocks Linux AIO syscalls.\n", .{});
        return err;
    };
    defer ctx.deinit();

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("aio-test.bin", .{
        .read = true,
        .truncate = true,
    });
    defer file.close();

    // Make the file large enough for offset tests.
    try file.setEndPos(16 * 1024);

    // Buffers are page-aligned even though this test does not use O_DIRECT.
    var write_buf: [4096]u8 align(4096) = undefined;
    var read_buf: [4096]u8 align(4096) = undefined;
    @memset(&write_buf, 0);
    @memset(&read_buf, 0);

    const msg = "hello from linux native aio in zig 0.14";
    @memcpy(write_buf[0..msg.len], msg);

    // 1. pwrite
    var write_cb = aio.Iocb.pwrite(file.handle, write_buf[0..4096], 0, .{ .data = 0x1001 });
    const submitted_write = try ctx.submitOne(&write_cb);
    try expect(submitted_write == 1, "pwrite submitted exactly one request");

    var events = try drainExactly(ctx, 1, "pwrite");
    const written = try printCompletion(events[0], "pwrite completion");
    try expect(written == 4096, "pwrite wrote 4096 bytes");
    try expect(events[0].userData() == 0x1001, "pwrite user data roundtrip");

    // 2. pread
    var read_cb = aio.Iocb.pread(file.handle, read_buf[0..4096], 0, .{ .data = 0x1002 });
    const submitted_read = try ctx.submitOne(&read_cb);
    try expect(submitted_read == 1, "pread submitted exactly one request");

    events = try drainExactly(ctx, 1, "pread");
    const read_n = try printCompletion(events[0], "pread completion");
    try expect(read_n == 4096, "pread read 4096 bytes");
    try expect(events[0].userData() == 0x1002, "pread user data roundtrip");
    try expect(std.mem.eql(u8, read_buf[0..msg.len], msg), "pread data matches pwrite data");

    // 3. fdatasync
    var fdatasync_cb = aio.Iocb.fdatasync(file.handle, .{ .data = 0x1003 });
    try ctx.submitAll(&[_]*aio.Iocb{&fdatasync_cb});

    events = try drainExactly(ctx, 1, "fdatasync");
    _ = try printCompletion(events[0], "fdatasync completion");
    try expect(events[0].userData() == 0x1003, "fdatasync user data roundtrip");

    // 4. fsync
    var fsync_cb = aio.Iocb.fsync(file.handle, .{ .data = 0x1004 });
    try ctx.submitAll(&[_]*aio.Iocb{&fsync_cb});

    events = try drainExactly(ctx, 1, "fsync");
    _ = try printCompletion(events[0], "fsync completion");
    try expect(events[0].userData() == 0x1004, "fsync user data roundtrip");

    // 5. poll should be nonblocking and should usually return 0 now.
    var poll_events: [4]aio.Event = undefined;
    const poll_n = try ctx.poll(poll_events[0..]);
    std.debug.print("poll after drain: {} event(s)\n", .{poll_n});

    // 6. Batch submit pwrite + pread.
    @memset(&write_buf, 0);
    @memset(&read_buf, 0);
    const msg2 = "batch submit works";
    @memcpy(write_buf[0..msg2.len], msg2);

    var batch_write_cb = aio.Iocb.pwrite(file.handle, write_buf[0..4096], 4096, .{ .data = 0x2001 });
    var batch_read_cb = aio.Iocb.pread(file.handle, read_buf[0..4096], 4096, .{ .data = 0x2002 });

    const batch = [_]*aio.Iocb{ &batch_write_cb, &batch_read_cb };
    const batch_submitted = try ctx.submit(batch[0..]);
    std.debug.print("batch submitted {} request(s)\n", .{batch_submitted});
    try expect(batch_submitted >= 1, "batch submitted at least one request");

    // If only one was accepted, submit the remainder.
    if (batch_submitted < batch.len) {
        const rem = try ctx.submit(batch[batch_submitted..]);
        try expect(rem == batch.len - batch_submitted, "batch remainder submitted");
    }

    events = try drainExactly(ctx, 2, "batch");
    for (events[0..2]) |ev| {
        _ = try printCompletion(ev, "batch completion");
    }
    try expect(std.mem.eql(u8, read_buf[0..msg2.len], msg2), "batch pread data matches batch pwrite data");

    // 7. cancel smoke test.
    // Linux AIO cancellation is inherently racy. The read may complete before we cancel it.
    var cancel_buf: [4096]u8 align(4096) = undefined;
    var cancel_cb = aio.Iocb.pread(file.handle, cancel_buf[0..], 0, .{ .data = 0x3001 });
    _ = try ctx.submitOne(&cancel_cb);

    var cancel_event: aio.Event = undefined;
    const cancel_result = ctx.cancel(&cancel_cb, &cancel_event);
    if (cancel_result) {
        std.debug.print("cancel syscall succeeded\n", .{});
        if (cancel_event.result()) |_| {} else |err| {
            std.debug.print("cancel completion result error: {s}\n", .{@errorName(err)});
        }
    } else |err| {
        std.debug.print("cancel syscall returned {s}; this is often OK because the I/O already completed\n", .{@errorName(err)});
        // Drain the request if cancellation did not return the completion event.
        events = try drainExactly(ctx, 1, "cancel-drain");
        if (events[0].result()) |_| {} else |completion_err| {
            std.debug.print("cancel-drain completion error: {s}\n", .{@errorName(completion_err)});
        }
    }

    std.debug.print("linux aio smoke test passed\n", .{});
}
