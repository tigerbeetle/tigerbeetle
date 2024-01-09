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

// Lifetimes are tricky here! The order is:
// - `kill` the process,
// - `join` the thread,
// - `destroy` the reader
// - `wait` the process
//
// That is, we need to kill the process first so that the thread is unblocked, but we
// need to wait for it last, so that stderr file descriptor is still good while we use it!
process: std.ChildProcess,
stderr_reader: *StderrReader,
stderr_reader_thread: std.Thread,

pub fn init(
    gpa: std.mem.Allocator,
    options: struct {
        echo: bool = true,
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
        .{ .echo = options.echo },
        "{tigerbeetle} format --cluster=0 --replica=0 --replica-count=1 {data_file}",
        .{ .tigerbeetle = tigerbeetle, .data_file = data_file },
    );

    // Pass `--addresses=0` to let the OS pick a port for us.
    var process = try shell.spawn(
        "{tigerbeetle} start --cache-grid=512MB --addresses=0 {data_file}",
        .{ .tigerbeetle = tigerbeetle, .data_file = data_file },
    );
    errdefer {
        _ = process.kill() catch unreachable;
    }

    var stderr_reader = try StderrReader.create(gpa, process.stderr.?);
    errdefer stderr_reader.destroy(gpa);

    stderr_reader.echo = options.echo;

    // Parse stderr to learn the assigned port. After that, spawn a dedicated thread to read the
    // rest of stderr to avoid blocking stderr. No need to read stdout as we don't print there.
    var limit: u32 = 0;
    const port = while (limit < 32) : (limit += 1) {
        const line = try stderr_reader.read_line();
        if (parse_port(line)) |port| break port;
    } else {
        log.err("failed to read port number from tigerbeetle process", .{});
        return error.NoPort;
    };

    var port_str: stdx.BoundedArray(u8, 8) = .{};
    std.fmt.formatInt(port, 10, .lower, .{}, port_str.writer()) catch unreachable;

    const stderr_reader_thread = try std.Thread.spawn(.{}, StderrReader.read_all, .{stderr_reader});
    errdefer stderr_reader_thread.join();

    return TmpTigerBeetle{
        .port = port,
        .port_str = port_str,
        .tmp_dir = tmp_dir,
        .stderr_reader = stderr_reader,
        .stderr_reader_thread = stderr_reader_thread,
        .process = process,
    };
}

pub fn deinit(tb: *TmpTigerBeetle, gpa: std.mem.Allocator) void {
    // Signal to the `stderr_reader_thread` that it can exit
    // TODO(Zig) https://github.com/ziglang/zig/issues/16820
    if (builtin.os.tag == .windows) {
        const exit_code = 1;
        std.os.windows.TerminateProcess(tb.process.id, exit_code) catch {};
    } else {
        std.os.kill(tb.process.id, std.os.SIG.TERM) catch {};
    }

    tb.stderr_reader_thread.join();
    tb.stderr_reader.destroy(gpa);
    _ = tb.process.wait() catch unreachable;
    tb.tmp_dir.cleanup();
}

const StderrReader = struct {
    fd: std.fs.File, // owned by the caller
    echo: bool = false,
    buf: [4096]u8 = undefined,

    fn create(gpa: std.mem.Allocator, fd: std.fs.File) !*StderrReader {
        var reader = try gpa.create(StderrReader);
        errdefer gpa.destroy(reader);

        reader.* = .{ .fd = fd };
        return reader;
    }

    fn destroy(reader: *StderrReader, gpa: std.mem.Allocator) void {
        gpa.destroy(reader);
    }

    fn read_line(reader: *StderrReader) ![]const u8 {
        const line = try reader.fd.reader().readUntilDelimiter(&reader.buf, '\n');
        if (reader.echo) {
            std.debug.print("{s}\n", .{line});
        }
        return line;
    }

    fn read_all(reader: *StderrReader) void {
        while (true) {
            _ = reader.read_line() catch return;
        }
    }
};

fn parse_port(stderr_log: []const u8) ?u16 {
    assert(std.mem.indexOf(u8, stderr_log, "\n") == null);

    var result = stderr_log;
    result = (stdx.cut(result, "listening on ") orelse return null).suffix;
    result = (stdx.cut(result, ":") orelse return null).suffix;
    const port = std.fmt.parseInt(u16, result, 10) catch return null;
    return port;
}

test parse_port {
    try std.testing.expectEqual(
        parse_port("info(main): 0: cluster=1: listening on 127.0.0.1:39047"),
        39047,
    );
}

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const gpa = gpa_allocator.allocator();

    var tb = try TmpTigerBeetle.init(gpa, .{});
    defer tb.deinit(gpa);

    std.debug.print("\n{}\n", .{tb.port});
}
