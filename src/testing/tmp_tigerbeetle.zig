//! TmpTigerBeetle is an utility for integration tests, which spawns a single node TigerBeetle
//! cluster in a temporary directory.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");

const log = std.log.scoped(.tmptigerbeetle);

const TmpTigerBeetle = @This();

/// Port the TigerBeetle instance is listening on.
port: u16,
/// For convenience, the same port pre-converted to string.
port_str: stdx.BoundedArray(u8, 8),

tmp_dir: std.testing.TmpDir,

process: std.process.Child,

pub fn init(
    gpa: std.mem.Allocator,
    options: struct {
        prebuilt: ?[]const u8 = null,
    },
) !TmpTigerBeetle {
    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var from_source_path: ?[]const u8 = null;
    defer if (from_source_path) |path| gpa.free(path);

    if (options.prebuilt == null) {
        const tigerbeetle_exe = comptime "tigerbeetle" ++ builtin.target.exeFileExt();

        // If tigerbeetle binary does not exist yet, build it.
        //
        // TODO: just run `zig build run` unconditionally here, when that doesn't do spurious
        // rebuilds.
        _ = shell.project_root.statFile(tigerbeetle_exe) catch {
            log.info("building TigerBeetle", .{});
            try shell.zig("build", .{});

            _ = try shell.project_root.statFile(tigerbeetle_exe);
        };

        from_source_path = try shell.project_root.realpathAlloc(gpa, tigerbeetle_exe);
    }

    const tigerbeetle: []const u8 = options.prebuilt orelse from_source_path.?;
    assert(std.fs.path.isAbsolute(tigerbeetle));

    var tmp_dir = std.testing.tmpDir(.{});
    errdefer tmp_dir.cleanup();

    const tmp_dir_path = try tmp_dir.dir.realpathAlloc(gpa, ".");
    defer gpa.free(tmp_dir_path);

    const data_file: []const u8 = try std.fs.path.join(gpa, &.{ tmp_dir_path, "0_0.tigerbeetle" });
    defer gpa.free(data_file);

    try shell.exec_options(
        .{ .echo = false },
        "{tigerbeetle} format --cluster=0 --replica=0 --replica-count=1 {data_file}",
        .{ .tigerbeetle = tigerbeetle, .data_file = data_file },
    );

    // Pass `--addresses=0` to let the OS pick a port for us.
    var process = try shell.spawn(
        .{
            .stdin_behavior = .Pipe,
            .stdout_behavior = .Pipe,
            .stderr_behavior = .Ignore,
        },
        "{tigerbeetle} start --development --addresses=0 {data_file}",
        .{ .tigerbeetle = tigerbeetle, .data_file = data_file },
    );
    errdefer {
        _ = process.kill() catch unreachable;
    }

    const port = port: {
        var exit_status: ?std.process.Child.Term = null;
        errdefer log.err(
            "failed to read port number from tigerbeetle process: {?}",
            .{exit_status},
        );

        var port_buf: [std.fmt.count("{}\n", .{std.math.maxInt(u16)})]u8 = undefined;
        const port_buf_len = try process.stdout.?.readAll(&port_buf);
        if (port_buf_len == 0) {
            exit_status = try process.wait();
            return error.NoPort;
        }

        break :port try std.fmt.parseInt(u16, port_buf[0 .. port_buf_len - 1], 10);
    };

    var port_str: stdx.BoundedArray(u8, 8) = .{};
    std.fmt.formatInt(port, 10, .lower, .{}, port_str.writer()) catch unreachable;

    return TmpTigerBeetle{
        .port = port,
        .port_str = port_str,
        .tmp_dir = tmp_dir,
        .process = process,
    };
}

pub fn deinit(tb: *TmpTigerBeetle, gpa: std.mem.Allocator) void {
    _ = gpa;
    _ = tb.process.kill() catch unreachable;
    tb.tmp_dir.cleanup();
}
