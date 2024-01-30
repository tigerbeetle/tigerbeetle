const std = @import("std");
const assert = std.debug.assert;
const ChildProcess = std.ChildProcess;

const cli = @import("./cli.zig");
const benchmark_load = @import("./benchmark_load.zig");

const log = std.log;

// Note: we intentionally don't use a temporary directory for this data file, and instead just put
// it into CWD, as performance of TigerBeetle very much depends on a specific file system.
const data_file = "0_0.tigerbeetle.benchmark";

pub fn main(allocator: std.mem.Allocator, args: *const cli.Command.Benchmark) !void {
    var data_file_created = false;
    defer {
        if (data_file_created) {
            std.fs.cwd().deleteFile(data_file) catch {};
        }
    }

    var tigerbeetle_process: ?TigerBeetleProcess = null;
    defer if (tigerbeetle_process) |*p| p.deinit();

    if (args.addresses == null) {
        const me = try std.fs.selfExePathAlloc(allocator);
        defer allocator.free(me);

        try format(allocator, .{ .tigerbeetle = me, .data_file = data_file });
        data_file_created = true;

        tigerbeetle_process = try start(allocator, .{ .tigerbeetle = me, .data_file = data_file });
    }

    const addresses = args.addresses orelse &.{tigerbeetle_process.?.address};
    try benchmark_load.main(allocator, addresses, args);
}

fn format(allocator: std.mem.Allocator, options: struct {
    tigerbeetle: []const u8,
    data_file: []const u8,
}) !void {
    const format_result = try ChildProcess.exec(.{
        .allocator = allocator,
        .argv = &.{
            options.tigerbeetle,
            "format",
            "--cluster=0",
            "--replica=0",
            "--replica-count=1",
            options.data_file,
        },
    });
    defer {
        allocator.free(format_result.stdout);
        allocator.free(format_result.stderr);
    }
    errdefer log.err("stderr: {s}", .{format_result.stderr});

    switch (format_result.term) {
        .Exited => |code| if (code != 0) return error.BadFormat,
        else => return error.BadFormat,
    }
}

const TigerBeetleProcess = struct {
    child: std.ChildProcess,
    address: std.net.Address,

    fn deinit(self: *TigerBeetleProcess) void {
        // Although we could just kill the child here, let's exercise the "normal" termination logic
        // through stdin closure, such that, from the perspective of the child, there's no
        // difference between the parent process exiting normally or just crashing.
        self.child.stdin.?.close();
        self.child.stdin = null;
        _ = self.child.wait() catch {};
        self.* = undefined;
    }
};

fn start(allocator: std.mem.Allocator, options: struct {
    tigerbeetle: []const u8,
    data_file: []const u8,
}) !TigerBeetleProcess {
    var child = std.ChildProcess.init(
        &.{
            options.tigerbeetle,
            "start",
            "--addresses=0",
            options.data_file,
        },
        allocator,
    );
    child.stdin_behavior = .Pipe;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;
    try child.spawn();
    errdefer {
        _ = child.kill() catch {};
    }

    const port = port: {
        errdefer log.err("failed to read port number from tigerbeetle process", .{});
        var port_buf: [std.fmt.count("{}\n", .{std.math.maxInt(u16)})]u8 = undefined;
        const port_buf_len = try child.stdout.?.readAll(&port_buf);
        break :port try std.fmt.parseInt(u16, port_buf[0 .. port_buf_len - 1], 10);
    };

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);

    return .{ .child = child, .address = address };
}
