//! TmpTigerBeetle is an utility for integration tests, which spawns a single node TigerBeetle
//! cluster in a temporary directory.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = @import("../shell.zig");

const MiB = stdx.MiB;

const log = std.log.scoped(.tmptigerbeetle);

const TmpTigerBeetle = @This();

/// Path to the executable.
tigerbeetle_exe: []const u8,
/// Port the TigerBeetle instance is listening on.
port: u16,
/// For convenience, the same port pre-converted to string.
port_str: []const u8,

tmp_dir: std.testing.TmpDir,

// A separate thread for reading process stderr without blocking it. The process must be terminated
// before stopping the StreamReader.
//
// StreamReader echoes process' stderr on exit unless explicitly instructed otherwise.
stderr_reader: *StreamReader,

process: std.process.Child,

pub fn init(
    gpa: std.mem.Allocator,
    options: struct {
        development: bool,
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
            try shell.exec_zig("build", .{});

            _ = try shell.project_root.statFile(tigerbeetle_exe);
        };

        from_source_path = try shell.project_root.realpathAlloc(gpa, tigerbeetle_exe);
    }

    const tigerbeetle_exe: []const u8 = try gpa.dupe(
        u8,
        options.prebuilt orelse from_source_path.?,
    );
    errdefer gpa.free(tigerbeetle_exe);
    assert(std.fs.path.isAbsolute(tigerbeetle_exe));

    var tmp_dir = std.testing.tmpDir(.{});
    errdefer tmp_dir.cleanup();

    const tmp_dir_path = try tmp_dir.dir.realpathAlloc(gpa, ".");
    defer gpa.free(tmp_dir_path);

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
            .stdin_behavior = .Pipe,
            .stdout_behavior = .Pipe,
            .stderr_behavior = .Pipe,
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
            _ = process.kill() catch unreachable;
        }
    }

    reader_maybe = try StreamReader.start(gpa, process.stderr.?);

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

    const port_str = try std.fmt.allocPrint(gpa, "{d}", .{port});
    errdefer gpa.free(port_str);

    return TmpTigerBeetle{
        .tigerbeetle_exe = tigerbeetle_exe,
        .port = port,
        .port_str = port_str,
        .tmp_dir = tmp_dir,
        .stderr_reader = reader_maybe.?,
        .process = process,
    };
}

pub fn deinit(tb: *TmpTigerBeetle, gpa: std.mem.Allocator) void {
    if (tb.stderr_reader.log_stderr.load(.seq_cst) == .on_early_exit) {
        tb.stderr_reader.log_stderr.store(.no, .seq_cst);
    }
    assert(tb.process.term == null);
    tb.stderr_reader.stop(gpa, &tb.process);
    assert(tb.process.term != null);
    gpa.free(tb.port_str);
    tb.tmp_dir.cleanup();
    gpa.free(tb.tigerbeetle_exe);
}

pub fn log_stderr(tb: *TmpTigerBeetle) void {
    tb.stderr_reader.log_stderr.store(.yes, .seq_cst);
}

const StreamReader = struct {
    const LogStderr = std.atomic.Value(enum(u8) { no, yes, on_early_exit });

    log_stderr: LogStderr = LogStderr.init(.on_early_exit),
    thread: std.Thread,
    file: std.fs.File,

    pub fn start(gpa: std.mem.Allocator, file: std.fs.File) !*StreamReader {
        var result = try gpa.create(StreamReader);
        errdefer gpa.destroy(result);

        result.* = .{
            .thread = undefined,
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
            std.os.windows.TerminateProcess(process.id, exit_code) catch {};
        } else {
            std.posix.kill(process.id, std.posix.SIG.TERM) catch {};
        }
        assert(process.stderr != null);
        self.thread.join();
        _ = process.wait() catch unreachable;
        assert(process.stderr == null);
        gpa.destroy(self);
    }

    fn thread_main(reader: *StreamReader) void {
        // NB: Zig allocators are not thread safe, so use mmap directly to hold process' stderr.
        const allocator = std.heap.page_allocator;

        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        // NB: don't use `readAllAlloc` to get partial output in case of errors.
        reader.file.reader().readAllArrayList(&buffer, 100 * MiB) catch {};
        switch (reader.log_stderr.load(.seq_cst)) {
            .on_early_exit, .yes => {
                log.err("tigerbeetle stderr:\n++++\n{s}\n++++", .{buffer.items});
            },
            .no => {},
        }
    }
};
