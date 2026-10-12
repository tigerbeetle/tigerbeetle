//! TmpTigerBeetle is an utility for integration tests, which spawns a single node TigerBeetle
//! cluster in a temporary directory.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = stdx.Shell;

const MiB = stdx.MiB;

const log = std.log.scoped(.tmptigerbeetle);

const TmpTigerBeetle = @This();

/// Path to the executable.
tigerbeetle_exe: []const u8,
/// Port the TigerBeetle instance is listening on.
port: u16,
/// For convenience, the same port pre-converted to string.
port_str: []const u8,

io: std.Io,
tmp_dir_path: []const u8,

// A separate thread for reading process stderr without blocking it. The process must be terminated
// before stopping the StreamReader.
//
// StreamReader echoes process' stderr on exit unless explicitly instructed otherwise.
stderr_reader: *StreamReader,

process: std.process.Child,

pub fn init(
    gpa: std.mem.Allocator,
    io: std.Io,
    environ_map: *const std.process.Environ.Map,
    options: struct {
        development: bool,
        prebuilt: ?[]const u8,
    },
) !TmpTigerBeetle {
    const shell = try Shell.create(gpa, io, environ_map);
    defer shell.destroy();

    var from_source_path: ?[:0]const u8 = null;
    defer if (from_source_path) |path| gpa.free(path);

    if (options.prebuilt == null) {
        const tigerbeetle_exe = comptime "tigerbeetle" ++ builtin.target.exeFileExt();

        // If tigerbeetle binary does not exist yet, build it.
        //
        // TODO: just run `zig build run` unconditionally here, when that doesn't do spurious
        // rebuilds.
        _ = shell.project_root.statFile(io, tigerbeetle_exe, .{}) catch {
            log.info("building TigerBeetle", .{});
            try shell.exec_zig("build", .{});

            _ = try shell.project_root.statFile(io, tigerbeetle_exe, .{});
        };

        from_source_path = try shell.project_root.realPathFileAlloc(io, tigerbeetle_exe, gpa);
    } else {
        // The build system passes paths relative to the current working directory.
        from_source_path = try std.Io.Dir.cwd().realPathFileAlloc(io, options.prebuilt.?, gpa);
    }

    const tigerbeetle_exe: []const u8 = try gpa.dupe(u8, from_source_path.?);
    errdefer gpa.free(tigerbeetle_exe);
    assert(std.fs.path.isAbsolute(tigerbeetle_exe));

    const tmp_dir_path = try gpa.dupe(u8, try shell.create_tmp_dir());
    errdefer gpa.free(tmp_dir_path);
    errdefer std.Io.Dir.cwd().deleteTree(io, tmp_dir_path) catch {};

    const data_file: []const u8 = try std.fs.path.join(gpa, &.{ tmp_dir_path, "0_0.tigerbeetle" });
    defer gpa.free(data_file);

    try shell.exec(
        "{tigerbeetle} format --cluster=0 --replica=0 --replica-count=1 {data_file}",
        .{ .tigerbeetle = tigerbeetle_exe, .data_file = data_file },
    );

    var reader_maybe: ?*StreamReader = null;
    // Pass `--addresses=0` to let the OS pick a port for us.
    var process = try shell.spawn(
        .{
            .stdin_behavior = .pipe,
            .stdout_behavior = .pipe,
            .stderr_behavior = .pipe,
        },
        "{tigerbeetle} start --development={development} --addresses=0 {data_file}",
        .{
            .tigerbeetle = tigerbeetle_exe,
            .development = if (options.development) "true" else "false",
            .data_file = data_file,
        },
    );

    errdefer {
        if (reader_maybe) |reader| {
            reader.stop(gpa, &process); // Will log stderr.
        } else {
            process.kill(io);
        }
    }

    reader_maybe = try StreamReader.start(gpa, io, process.stderr.?);

    const port = port: {
        var exit_status: ?std.process.Child.Term = null;
        errdefer log.err(
            "failed to read port number from tigerbeetle process: {?}",
            .{exit_status},
        );

        var port_reader = process.stdout.?.readerStreaming(io, &.{});
        var port_buf: [std.fmt.count("{}\n", .{std.math.maxInt(u16)})]u8 = undefined;
        const port_buf_len = try port_reader.interface.readSliceShort(&port_buf);
        if (port_buf_len == 0) {
            exit_status = try process.wait(io);
            return error.NoPort;
        }

        break :port try stdx.parse_int(u16, port_buf[0 .. port_buf_len - 1], .{});
    };

    const port_str = try std.fmt.allocPrint(gpa, "{d}", .{port});
    errdefer gpa.free(port_str);

    return TmpTigerBeetle{
        .tigerbeetle_exe = tigerbeetle_exe,
        .port = port,
        .port_str = port_str,
        .io = io,
        .tmp_dir_path = tmp_dir_path,
        .stderr_reader = reader_maybe.?,
        .process = process,
    };
}

pub fn deinit(tb: *TmpTigerBeetle, gpa: std.mem.Allocator) void {
    if (tb.stderr_reader.log_stderr.load(.seq_cst) == .on_early_exit) {
        tb.stderr_reader.log_stderr.store(.no, .seq_cst);
    }
    assert(tb.process.id != null);
    tb.stderr_reader.stop(gpa, &tb.process);
    assert(tb.process.id == null);
    gpa.free(tb.port_str);
    std.Io.Dir.cwd().deleteTree(tb.io, tb.tmp_dir_path) catch {};
    gpa.free(tb.tmp_dir_path);
    gpa.free(tb.tigerbeetle_exe);
}

pub fn log_stderr(tb: *TmpTigerBeetle) void {
    tb.stderr_reader.log_stderr.store(.yes, .seq_cst);
}

const StreamReader = struct {
    const LogStderr = std.atomic.Value(enum(u8) { no, yes, on_early_exit });

    log_stderr: LogStderr = LogStderr.init(.on_early_exit),
    thread: std.Thread,
    io: std.Io,
    file: std.Io.File,

    pub fn start(gpa: std.mem.Allocator, io: std.Io, file: std.Io.File) !*StreamReader {
        var result = try gpa.create(StreamReader);
        errdefer gpa.destroy(result);

        result.* = .{
            .thread = undefined,
            .io = io,
            .file = file,
        };

        result.thread = try std.Thread.spawn(.{}, thread_main, .{result});
        return result;
    }

    pub fn stop(self: *StreamReader, gpa: std.mem.Allocator, process: *std.process.Child) void {
        // Shutdown sequence is tricky:
        // 1. Terminate the process, but _don't_ close our side of the pipe.
        // 2. Wait until the thread exits.
        // 3. Close stderr file descriptor.
        // TODO(Zig) https://github.com/ziglang/zig/issues/16820
        if (builtin.os.tag == .windows) {
            const exit_code = 1;
            stdx.windows.terminate_process(process.id.?, exit_code) catch {};
        } else {
            std.posix.kill(process.id.?, std.posix.SIG.TERM) catch {};
        }
        assert(process.stderr != null);
        self.thread.join();
        _ = process.wait(self.io) catch unreachable;
        assert(process.stderr == null);
        gpa.destroy(self);
    }

    fn thread_main(reader: *StreamReader) void {
        // NB: Zig allocators are not thread safe, so use mmap directly to hold process' stderr.
        const allocator = std.heap.page_allocator;

        var buffer: std.ArrayList(u8) = .empty;
        defer buffer.deinit(allocator);

        // NB: don't use `readAllAlloc` to get partial output in case of errors.
        var file_reader = reader.file.readerStreaming(reader.io, &.{});
        file_reader.interface.appendRemaining(allocator, &buffer, .limited(100 * MiB)) catch {};
        switch (reader.log_stderr.load(.seq_cst)) {
            .on_early_exit, .yes => {
                log.err("tigerbeetle stderr:\n++++\n{s}\n++++", .{buffer.items});
            },
            .no => {},
        }
    }
};
