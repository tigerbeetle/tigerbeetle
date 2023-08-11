//! TmpTigerBeetle is an utility for integration tests, which spawns a single node TigerBeetle
//! cluster in a temporary directory.

const std = @import("std");
const builtin = @import("builtin");

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");

const log = std.log.scoped(.tmptigerbeetle);

const TmpTigerBeetle = @This();

/// Port the TigerBeetle instance is listening on.
port: u16,
/// For convenience, the same port pre-converted to string.
port_str: std.BoundedArray(u8, 8),

tmp_dir: std.testing.TmpDir,
process: std.ChildProcess,
stderr_reader: std.Thread,

pub fn init(gpa: std.mem.Allocator) !TmpTigerBeetle {
    const shell = try Shell.create(gpa);
    defer shell.destroy();

    const tigerbeetle_exe = comptime "tigerbeetle" ++ builtin.target.exeFileExt();

    // If tigerbeetle binary does not exist yet, build it.
    // TODO: just run `zig build run` unconditionally here, when that doesn't do spurious rebuilds.
    _ = shell.project_root.statFile(tigerbeetle_exe) catch {
        log.info("building TigerBeetle", .{});
        try shell.zig("build", .{});
    };

    const tigerbeetle: []const u8 = try shell.project_root.realpathAlloc(gpa, tigerbeetle_exe);
    defer gpa.free(tigerbeetle);

    var tmp_dir = std.testing.tmpDir(.{});
    errdefer tmp_dir.cleanup();

    const tmp_dir_path = try tmp_dir.dir.realpathAlloc(gpa, ".");
    defer gpa.free(tmp_dir_path);

    const data_file: []const u8 = try std.fs.path.join(gpa, &.{ tmp_dir_path, "0_0.tigerbeetle" });
    defer gpa.free(data_file);

    try shell.exec(
        "{tigerbeetle} format --cluster=0 --replica=0 --replica-count=1 {data_file}",
        .{ .tigerbeetle = tigerbeetle, .data_file = data_file },
    );

    // Pass `--addresses=0` to let the OS pick a port for us.
    var process = try shell.spawn(
        "{tigerbeetle} start --cache-grid=128MB --addresses=0 {data_file}",
        .{ .tigerbeetle = tigerbeetle, .data_file = data_file },
    );
    errdefer {
        _ = process.kill() catch {};
        _ = process.wait() catch unreachable;
    }

    // Parse stderr to learn the assigned port. After that, spawn a dedicated thread to read the
    // rest of stderr to avoid blocking stderr. No need to read stdout as we don't print there.
    var buf: [4096]u8 = undefined;
    var buf_cursor: usize = 0;
    var limit: u32 = 0;
    const port = while (limit < 32) : (limit += 1) {
        buf_cursor += try process.stderr.?.read(buf[buf_cursor..]);
        if (parse_port(buf[0..buf_cursor])) |port| break port;
    } else {
        log.err("can't start TigerBeetle", .{});
        std.debug.print("stderr:\n{s}\n", .{buf[0..buf_cursor]});
        return error.NoPort;
    };

    var port_str: std.BoundedArray(u8, 8) = .{};
    std.fmt.formatInt(port, 10, .lower, .{}, port_str.writer()) catch unreachable;

    const stderr_reader = try read_stderr_in_background(&process);
    errdefer stderr_reader.join();

    return TmpTigerBeetle{
        .tmp_dir = tmp_dir,
        .process = process,
        .port = port,
        .port_str = port_str,
        .stderr_reader = stderr_reader,
    };
}

pub fn deinit(tb: *TmpTigerBeetle) void {
    _ = tb.process.kill() catch {};
    tb.stderr_reader.join();
    _ = tb.process.wait() catch unreachable;
    tb.tmp_dir.cleanup();
}

fn parse_port(stderr_log: []const u8) ?u16 {
    var result = stderr_log;
    result = (stdx.cut(result, "listening on ") orelse return null).suffix;
    result = (stdx.cut(result, "\n") orelse return null).prefix;
    result = (stdx.cut(result, ":") orelse return null).suffix;
    const port = std.fmt.parseInt(u16, result, 10) catch return null;
    return port;
}

test parse_port {
    try std.testing.expectEqual(
        parse_port("info(main): 0: cluster=1: listening on 127.0.0.1:39047\n"),
        39047,
    );
}

fn read_stderr_in_background(process: *const std.ChildProcess) !std.Thread {
    return try std.Thread.spawn(
        .{},
        struct {
            fn read_stderr(stderr: std.fs.File) void {
                var buf: [4096]u8 = undefined;
                while (true) {
                    const n = stderr.read(&buf) catch return;
                    if (n == 0) return;
                }
            }
        }.read_stderr,
        .{process.stderr.?},
    );
}
