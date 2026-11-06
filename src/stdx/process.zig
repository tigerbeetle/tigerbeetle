//! `Process` is a statically allocated resource for spawning child processes.
//!
//! The design is driven by memory: to spawn a child, you need to allocate its argv and environ,
//! in the specific format expected by the OS. It's hard to predict the exact size you need: the
//! environ is proportional to parent's environment, and, on windows, WTF-8 ->  UTF-16 conversion
//! takes place.
//!
//! `Process` guesstimates the size of memory slab it'll need, and then re-uses this arena for
//! spawning many children. `Process` supports incremental "builder" API for constructing command
//! line piecemeal.
//!
//! Currently it relies on `std.Io` for actual spawning, but that is an implementation detail.
//!
//! Example usage:
test Process {
    if (true) return; // Only compile this test to avoid 3rd party processes.

    const io = std.testing.io;
    const gpa = std.testing.allocator;
    const environ = try std.testing.environ.createMap(gpa);
    defer environ.deinit(gpa);

    // Process is a resource, it is created once at a startup and re-used.
    var process = try Process.init(gpa, io, environment.map, .{
        .working_directory = std.Io.Dir.cwd(), // Caller owns the file descriptor.
        .spawn_size_extra = 4 * KiB, // Memory to hold the command line and the environment.
    });

    // Process has several API layers, from more convenient to more powerful.

    // The `run_` family of methods is for one-short execution of commands.
    // Commands support injection-proof shell-like interpolation syntax.
    // An error is raised if the exit code is non-zero.
    {
        try process.run("git switch {branch}", .{ .branch = "process" });

        // When capturing output, the caller is responsible for managing the memory:
        var buffer: [u8]KiB = undefined;
        const stdout_full: []u8 = process.run_stdout("ls {path}", .{ .path = "./" }, &buffer);
        _ = stdout_full;

        // The `_line` methods strip the final `\n` from the output:
        const path = process.run_stdout_line("pwd", .{}, &buffer);
        assert(!std.mem.endsWith(u8, path, "\n"));

        // If it is expected that a process might return a non-zero exit code, use
        // `_status` method to capture its numeric value. Note that `catch`ing an
        // error is not an equivalent, as that would emit an error log.
        const status = process.run_status("git merge-tree branch_a branch_b", .{});
        if (status != 0) log.info("merge failed", .{});
    }

    // The `prepare` & `execute` family of methods allow incremental
    // construction of the command line. `run_` is implemented as a `prepare`
    // followed by an `execute`.
    //
    // This is helpful for optional and variable arguments:
    {
        const user_name: ?[]const u8 = "Bob Dylan"; // Spaces in arguments are handled correctly.
        const parents: []const []const u8 = &.{"16df1828b03f80dc12b41e0c3eb78cc96a64c144"};

        process.prepare();
        try process.argument("git");
        if (user_name) |name| {
            // Note how interpolation syntax supports adding literal
            // prefixes/suffixes to arguments.
            try process.arguments_format("-c user.name={name}", .{ .name = name });
        }
        try process.arguments(&.{ "commit-tree", "-m", "work in progress" });

        for (parents) |parent| {
            // NB: `.spawn_size_extra` limits the total size of all arguments,
            // with some overhead. Sadly, there's no way to predict the precise
            // amount of bytes required for a command line.
            try process.arguments(&.{ "-p", parent });
        }

        // You can also configure the environment. Configuration is for a single
        // `execute_`. That is, a call to `.prepare` erases all arguments and
        // environment.
        try process.environment("DEBUG", "true");

        // The `execute_` family supports the same variety of suffixes as the `_run`
        // family. In particular, the `_exact` variant checks the size of the output
        // (note that `_line` strips final `\n`):
        var rev: [40]u8 = undefined;
        try process.execute_stdout_line_exact(&rev);

        // You must call `.prepare` before you can use the process again, which
        // will erase configured arguments and environment. However, you can
        // still access the configuration after an `execute` and before a
        // `prepare`. This is intended for debugging and diagnostics of failed
        // processes.
        assert(process.state == .executed);
        assert(process.argv.items.len > 0);
        assert(process.envp.items.len > 0);
        log.info("process = {f}", .{process});
    }

    // Finally, `spawn` method is the most powerful, it creates a process, but
    // doesn't wait for it automatically. There can be only one process running
    // at a time. The number of instances of `Process` matches the maximum
    // possible concurrency.
    {
        process.prepare();
        try process.arguments_format("python3 -m http.server", .{});

        // Spawning a child returns its stdio file descriptors:
        const child_io = try process.spawn(.{
            .stdout = .pipe,
            .stderr = .ignore,
        });
        defer {
            process.kill(); // Kill is idempotent, and must be used after spawning.
            child_io.close_all(process.io); // Child pipes must be closed separately.
        }

        // Process always pipes stdin, so that the child process can observe
        // parent exit in a cross-platform way.
        assert(child_io.stdin != null);

        assert(child_io.stdout != null);
        assert(child_io.stderr == null);

        // File descriptors can be used to communicate with the child.
        var buffer: [u8]KiB = undefined;
        const read_size = try child_io.stdout.?.readStreaming(process.io, &.{&buffer});
        _ = read_size;

        // And you can close them one-by-one, if needed.
        child_io.close(process.io, .stdin);

        // Don't forget to check the exit status!
        const term = try process.wait();
        assert(std.meta.eql(term, .{ .exited = 0 }));
    }
}

const std = @import("std");
const stdx = @import("./stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const KiB = stdx.KiB;
const MiB = stdx.MiB;

const log = std.log.scoped(.process);

io: std.Io,
spawn_arena: std.heap.FixedBufferAllocator,
argv: std.ArrayList([]const u8),
envp: std.ArrayList(struct { key: []u8, value: []u8 }),
pgid: ?std.posix.pid_t = null,

/// Caller is responsible for closing.
working_directory: std.Io.Dir,
parent_environment: *const std.process.Environ.Map,

state: enum {
    idle,
    prepare,
    running,
    // After process is awaited for, we don't immediately reset `argv` to be
    // able to print the command line on failure.
    executed,
} = .idle,

child: ?std.process.Child,
/// For debugging purposes, we always try to capture stderr.
/// Stderr can be arbitrary large, so we capture it using ring buffer semantics.
stderr_buffer: []u8,
stderr_size: u64 = 0, // NB: Total size, `maybe(stderr_size > stderr_buffer.len)`.

/// A small utility to learn your own path, if you want to recursively execute yourself.
/// Create this in main, pass it down.
pub const SelfExe = struct {
    path: [:0]const u8,

    pub fn init(gpa: std.mem.Allocator, io: std.Io) !SelfExe {
        return .{
            .path = try std.process.executablePathAlloc(io, gpa),
        };
    }

    pub fn deinit(self: *SelfExe, gpa: std.mem.Allocator) void {
        gpa.free(self.path);
        self.* = undefined;
    }
};

pub const Term = std.process.Child.Term;
const Process = @This();

pub fn init(
    gpa: std.mem.Allocator,
    io: std.Io,
    parent_environment: *const std.process.Environ.Map,
    options: struct {
        working_directory: std.Io.Dir,
        /// This parameter accounts for memory required to hold the command line
        /// and any environmental modifications. There's no universal default here.
        /// Use 4 * KiB if you are absolutely unsure, and increase if necessary.
        spawn_size_exta: u32,
    },
) !Process {
    const spawn_size = spawn_size_minimal(parent_environment) + options.spawn_size_exta;
    const spawn_buffer = try gpa.alloc(u8, spawn_size);
    errdefer gpa.free(spawn_buffer);

    const stderr_buffer = try gpa.alloc(u8, 4 * KiB);
    errdefer gpa.free(stderr_buffer);

    return .{
        .io = io,
        .spawn_arena = .{
            .end_index = 0,
            .buffer = spawn_buffer,
        },
        .argv = .empty,
        .envp = .empty,
        .working_directory = options.working_directory,
        .parent_environment = parent_environment,
        .child = null,
        .stderr_buffer = stderr_buffer,
    };
}

/// When we spawn a subprocess, we might need to materialize the copy of our
/// environment. It's hard to predict exactly how much memory will this require,
/// but 4x the logical size of our env is a conservative guess:
/// - 1x to store it logically as a hash map,
/// - 1x to store it physically, as a contiguous block of memory,
/// - 1x to account for allocation overhead and utf16 conversion on windows,
/// - and 1x for a round number.
///
/// In general, Process is one of the few places where we have to guesstimate
/// memory usage, rather than derive it precisely from the logical limits.
fn spawn_size_minimal(parent_environment: *const std.process.Environ.Map) u32 {
    var size: usize = 0;
    for (parent_environment.keys()) |key| size += key.len;
    for (parent_environment.values()) |value| size += value.len;
    size *= 4;
    size = @max(size, 4 * KiB);
    assert(size < 128 * MiB);
    return @intCast(size);
}

pub fn deinit(process: *Process, gpa: std.mem.Allocator) void {
    assert(process.state != .running); // Caller of `.spawn` must `defer process.kill()`.
    gpa.free(process.stderr_buffer);
    gpa.free(process.spawn_arena.buffer);
    process.* = undefined;
}

/// The `run_` family of functions are shortcuts. If you need to build the
/// CLI piecemeal, use `prepare` and `execute`. You can `prepare` before run
/// to set environmental variables.
pub fn run(process: *Process, comptime fmt: []const u8, fmt_args: anytype) !void {
    process.prepare_run();
    assert(process.state == .prepare);
    try process.arguments_format(fmt, fmt_args);
    try process.execute();
}

/// Returns numeric exit code of the process. Returns an error if the process
/// is killed.
pub fn run_status(process: *Process, comptime fmt: []const u8, fmt_args: anytype) !u8 {
    process.prepare_run();
    assert(process.state == .prepare);
    try process.arguments_format(fmt, fmt_args);
    return try process.execute_status();
}

pub fn run_stdout(
    process: *Process,
    comptime fmt: []const u8,
    fmt_args: anytype,
    buffer: []u8,
) ![]u8 {
    process.prepare_run();
    assert(process.state == .prepare);
    try process.arguments_format(fmt, fmt_args);
    return try process.execute_stdout(buffer);
}

/// Reads a single output line, strips `\n`.
pub fn run_stdout_line(
    process: *Process,
    comptime fmt: []const u8,
    fmt_args: anytype,
    buffer: []u8,
) ![]u8 {
    process.prepare_run();
    assert(process.state == .prepare);
    try process.arguments_format(fmt, fmt_args);
    return try process.execute_stdout_line(buffer);
}

/// Reads exactly `buffer.len` bytes + an optional trailing `\n`.
pub fn run_stdout_line_exact(
    process: *Process,
    comptime fmt: []const u8,
    fmt_args: anytype,
    buffer: []u8,
) !void {
    process.prepare_run();
    assert(process.state == .prepare);
    try process.arguments_format(fmt, fmt_args);
    try process.execute_stdout_line_exact(buffer);
}

const StdoutAndStatus = struct {
    stdout: []u8,
    status: u8,
};
pub fn run_stdout_and_status(
    process: *Process,
    comptime fmt: []const u8,
    fmt_args: anytype,
    buffer: []u8,
) !StdoutAndStatus {
    process.prepare_run();
    assert(process.state == .prepare);
    try process.arguments_format(fmt, fmt_args);
    return try process.execute_stdout_and_status(buffer);
}

/// Stateful API: call `prepare` to start building a new invocation, then
/// continue calling `arguments` to build the command line incrementally,
/// and top it off with `run_` to `exec` and `wait`.
pub fn prepare(process: *Process) void {
    assert(process.state == .idle or process.state == .executed);
    assert(process.child == null);
    process.* = .{
        .io = process.io,
        .working_directory = process.working_directory,
        .parent_environment = process.parent_environment,

        .state = .prepare,
        .spawn_arena = .{
            .buffer = process.spawn_arena.buffer,
            .end_index = 0,
        },
        .argv = .empty,
        // NB: Intentionally reset the environment. It would be possible to treat
        // `Process` as a multishot builder and allow to configure environment once
        // for multiple invocations, but a more statless API minimizes surprises,
        // even if it is a bit more verbose: every call site needs to repeat shared
        // environment setup, and that makes it obvious which `execute`s actually
        // care about environment.
        .envp = .empty,
        .pgid = null,
        .child = null,
        .stderr_buffer = process.stderr_buffer,
    };
}

// For the `run_` family of APIs, we want to allow the caller to `prepare`
// manually in case they want to configure environment.
fn prepare_run(process: *Process) void {
    switch (process.state) {
        .idle => process.prepare(),
        .prepare => assert(process.argv.items.len == 0),
        .executed => process.prepare(),
        .running => unreachable,
    }
}

pub fn arguments_format(
    process: *Process,
    comptime fmt: []const u8,
    fmt_args: anytype,
) error{ProcessArgvLimit}!void {
    var argv: Argv = .{
        .gpa = process.spawn_arena.allocator(),
        .args = &process.argv,
    };
    expand_argv(&argv, fmt, fmt_args) catch |err|
        switch (err) {
            error.OutOfMemory => return error.ProcessArgvLimit,
        };
    assert(process.argv.items.len > 0);
}

pub fn arguments(process: *Process, values: []const []const u8) error{ProcessArgvLimit}!void {
    assert(process.state == .prepare);
    for (values) |value| try process.argument(value);
}

pub fn argument(process: *Process, value: []const u8) error{ProcessArgvLimit}!void {
    assert(process.state == .prepare);
    process.argument_alloc(value) catch |err| switch (err) {
        error.OutOfMemory => return error.ProcessArgvLimit,
    };
}

fn argument_alloc(process: *Process, arg: []const u8) !void {
    assert(process.state == .prepare);
    const arena = process.spawn_arena.allocator();
    const arg_owned = try arena.dupe(u8, arg);
    try process.argv.append(arena, arg_owned);
}

pub fn environment(
    process: *Process,
    key: []const u8,
    value: []const u8,
) error{ProcessEnvpLimit}!void {
    assert(process.state == .prepare);
    process.environment_format_alloc(key, "{s}", .{value}) catch |err| switch (err) {
        error.OutOfMemory => return error.ProcessEnvpLimit,
    };
}

pub fn environment_format(
    process: *Process,
    key: []const u8,
    comptime value_fmt: []const u8,
    value_args: anytype,
) error{ProcessEnvpLimit}!void {
    assert(process.state == .prepare);
    process.environment_format_alloc(key, value_fmt, value_args) catch |err| switch (err) {
        error.OutOfMemory => return error.ProcessEnvpLimit,
    };
}

fn environment_format_alloc(
    process: *Process,
    key: []const u8,
    comptime value_fmt: []const u8,
    value_args: anytype,
) !void {
    assert(process.state == .prepare);
    const arena = process.spawn_arena.allocator();
    const key_owned = try arena.dupe(u8, key);
    const value_owned = try std.fmt.allocPrint(arena, value_fmt, value_args);
    try process.envp.append(arena, .{ .key = key_owned, .value = value_owned });
}

pub fn execute(process: *Process) !void {
    try process.execute_inner(.{
        .capture_term = null,
        .capture_stdout = null,
    });
}

pub fn execute_term(process: *Process) !Term {
    var term: ?Term = null;
    try process.execute_inner(.{
        .capture_term = .{ .full = &term },
        .capture_stdout = null,
    });
    return term.?;
}

pub fn execute_status(process: *Process) !u8 {
    var status: ?u8 = null;
    try process.execute_inner(.{
        .capture_term = .{ .status = &status },
        .capture_stdout = null,
    });
    return status.?;
}

pub fn execute_stdout(process: *Process, output_buffer: []u8) ![]u8 {
    var result: ?[]u8 = null;
    try process.execute_inner(.{
        .capture_term = null,
        .capture_stdout = .{
            .buffer = output_buffer,
            .result = &result,
            .single_line = false,
        },
    });
    return result.?;
}

pub fn execute_stdout_line(process: *Process, output_buffer: []u8) ![]u8 {
    var result: ?[]u8 = null;
    try process.execute_inner(.{
        .capture_term = null,
        .capture_stdout = .{
            .buffer = output_buffer,
            .result = &result,
            .single_line = true,
        },
    });
    return result.?;
}

pub fn execute_stdout_line_exact(process: *Process, output_buffer: []u8) !void {
    const result = try process.execute_stdout_line(output_buffer);
    assert(result.len <= output_buffer.len);
    if (result.len < output_buffer.len) {
        return error.ProcessNotEnoughData;
    }
}

pub fn execute_stdout_and_status(process: *Process, output_buffer: []u8) !StdoutAndStatus {
    var result_stdout: ?[]u8 = null;
    var result_status: ?u8 = null;
    try process.execute_inner(.{
        .capture_term = .{ .status = &result_status },
        .capture_stdout = .{
            .buffer = output_buffer,
            .result = &result_stdout,
            .single_line = false,
        },
    });
    return .{
        .stdout = result_stdout.?,
        .status = result_status.?,
    };
}

pub const ChildStdio = struct {
    stdin: ?std.Io.File,
    stdout: ?std.Io.File,
    stderr: ?std.Io.File,

    const Stream = std.meta.FieldEnum(ChildStdio);

    pub const Behavior = enum {
        pipe,
        ignore,
        inherit, // NB: use _very_ sparingly, only for user-interactive tools.

        fn to_std(behavior: Behavior) std.process.SpawnOptions.StdIo {
            return switch (behavior) {
                inline else => |tag| @field(std.process.SpawnOptions.StdIo, @tagName(tag)),
            };
        }
    };

    pub fn close(child: *ChildStdio, io: std.Io, comptime stream: Stream) void {
        const field = @tagName(stream);
        assert(@field(child, field) != null);
        @field(child, field).?.close(io);
        @field(child, field) = null;
    }

    pub fn close_all(child: *ChildStdio, io: std.Io) void {
        inline for (comptime std.enums.values(Stream)) |stream| {
            if (@field(child, @tagName(stream)) != null) {
                child.close(io, stream);
            }
        }
    }
};

/// Spawns a concurrent process. Must be followed by `defer process.kill()`.
/// Call `process.wait()` to get the exit status.
/// The caller must close the pipes returned.
pub fn spawn(
    process: *Process,
    options: struct {
        // NB: stdin is piped unconditionally,
        // to allow the child to notice when the parent exits.

        stdout: ChildStdio.Behavior,
        stderr: ChildStdio.Behavior,
    },
) !ChildStdio {
    assert(process.state == .prepare);
    assert(process.argv.items.len > 0);
    assert(process.child == null);

    var child_environment: ?std.process.Environ.Map = try process.combine_environment();

    // Annoyingly, this will use IO's global allocator:
    // https://zsf.zulipchat.com/#narrow/channel/454371-std/topic/process.2ESpawnOptions.20allocator
    process.child = try std.process.spawn(process.io, .{
        .argv = process.argv.items,
        .cwd = .{ .dir = process.working_directory },
        .environ_map = if (child_environment) |*pointer| pointer else null,
        .pgid = process.pgid,
        .stdin = .pipe,
        .stdout = options.stdout.to_std(),
        .stderr = options.stderr.to_std(),
    });
    errdefer comptime unreachable;

    process.state = .running;

    // Steal the pipes so that they are only closed once.
    const result: ChildStdio = .{
        .stdin = process.child.?.stdin,
        .stdout = process.child.?.stdout,
        .stderr = process.child.?.stderr,
    };
    process.child.?.stdin = null;
    process.child.?.stdout = null;
    process.child.?.stderr = null;
    return result;
}

/// Terminates concurrently running child process. Idempotent.
pub fn kill(process: *Process) void {
    switch (process.state) {
        .idle, .prepare => unreachable,
        .running => {
            assert(process.child != null);
            process.child.?.kill(process.io);
            process.child = null;
            process.state = .executed;
        },
        .executed => {}, // Idempotent to simplify error handling.
    }
}

pub fn wait(process: *Process) !Term {
    assert(process.state == .running);
    assert(process.child != null);
    const term = try process.child.?.wait(process.io);
    process.child = null;
    process.state = .executed;
    return term;
}

/// Get a suffix of stderr produced, for debugging purposes.
pub fn stderr_debug(process: *Process) []const u8 {
    assert(process.state == .executed);
    const size_truncated = @min(process.stderr_size, process.stderr_buffer.len);
    return process.stderr_buffer[0..size_truncated];
}

fn execute_inner(
    process: *Process,
    options: struct {
        capture_term: ?union(enum) {
            full: *?Term,
            status: *?u8,
        },
        capture_stdout: ?struct {
            buffer: []u8,
            result: *?[]u8,
            // Single line mode expects at most one line of output, and
            // automatically strips a single trailing `\n`. This is a weirdly
            // specific toggle, but is handy to parse output of
            //    git rev-parse HEAD
            // and the like, which print fixed data, followed by a new line.
            single_line: bool,
        },
    },
) !void {
    assert(process.state == .prepare);
    assert(process.argv.items.len > 0);

    // NB: If the process is expected to exit with a non-zero status, the
    // caller must capture its term to avoid false logging.
    errdefer log.err("run {f}", .{process});

    var child_io = try process.spawn(.{
        .stdout = if (options.capture_stdout == null) .ignore else .pipe,
        .stderr = .pipe,
    });
    defer {
        process.kill();
        child_io.close_all(process.io);
    }

    assert(process.state == .running);

    var stdout_buffers: [2][]u8 = undefined;
    var stdout_buffer_eof: [2]u8 = undefined;
    var stdout_size: u64 = 0;

    // For debugging, capture&log a tail of stderr using a ring buffer.
    var stderr_buffers: [1][]u8 = undefined;
    errdefer {
        if (process.stderr_size > 0) {
            const suffix = if (process.stderr_size > process.stderr_buffer.len) " tail" else "";
            log.err("stderr{s}:\n{s}", .{ suffix, process.stderr_debug() });
        }
    }
    defer {
        if (process.stderr_size > process.stderr_buffer.len) {
            std.mem.rotate(
                u8,
                process.stderr_buffer,
                process.stderr_size % process.stderr_buffer.len,
            );
        }
    }

    var batch_storage: [2]std.Io.Operation.Storage = undefined;
    var batch = std.Io.Batch.init(&batch_storage);
    defer batch.cancel(process.io);

    var batch_count: u32 = 0;

    if (options.capture_stdout) |capture| {
        stdout_buffers = .{ capture.buffer, &stdout_buffer_eof };
        batch.addAt(0, .{ .file_read_streaming = .{
            .file = child_io.stdout.?,
            .data = &stdout_buffers,
        } });
        batch_count += 1;
    }

    stderr_buffers = .{process.stderr_buffer};
    batch.addAt(1, .{ .file_read_streaming = .{
        .file = child_io.stderr.?,
        .data = &stderr_buffers,
    } });
    batch_count += 1;

    while (batch_count > 0) {
        batch.awaitConcurrent(process.io, .none) catch |err| switch (err) {
            // The Io we are using _should_ support the `Io.Batch` API, but,
            // annoyingly, the implementation allocates internally. Long term,
            // we should use IO here instead.
            error.ConcurrencyUnavailable => @panic("execute_inner: ConcurrentError"),
            error.Canceled, // We don't cancel anything.
            error.Timeout, // Timeout is none.
            => unreachable,
        };

        while (batch.next()) |operation| {
            batch_count -= 1;
            const result: std.Io.Operation.FileReadStreaming.Result =
                operation.result.file_read_streaming;

            switch (operation.index) {
                0 => { // stdout
                    assert(options.capture_stdout != null);
                    const capture = options.capture_stdout.?;
                    const read_size = result catch |err| switch (err) {
                        error.EndOfStream => continue,
                        else => return err,
                    };
                    stdout_size += read_size;
                    stdout_buffers = .{
                        capture.buffer[@min(stdout_size, capture.buffer.len)..],
                        stdout_buffer_eof[stdout_size -| capture.buffer.len..],
                    };

                    maybe(stdout_buffers[0].len == 0);
                    if (stdout_buffers[1].len == 0) {
                        return error.ProcessTooMuchData;
                    }

                    batch.addAt(0, .{ .file_read_streaming = .{
                        .file = child_io.stdout.?,
                        .data = &stdout_buffers,
                    } });
                    batch_count += 1;
                },
                1 => { // stderr
                    const read_size = result catch |err| switch (err) {
                        error.EndOfStream => continue,
                        else => return err,
                    };
                    process.stderr_size += read_size;

                    maybe(process.stderr_size > process.stderr_buffer.len);
                    stderr_buffers = .{
                        process.stderr_buffer[process.stderr_size % process.stderr_buffer.len ..],
                    };

                    batch.addAt(1, .{ .file_read_streaming = .{
                        .file = child_io.stderr.?,
                        .data = &stderr_buffers,
                    } });
                    batch_count += 1;
                },
                else => unreachable,
            }
        }
    }

    if (options.capture_stdout) |capture| {
        assert(child_io.stdout != null);
        assert(stdout_size < capture.buffer.len + stdout_buffer_eof.len);
        assert(capture.result.* == null);
        if (capture.single_line) {
            if (stdout_size > capture.buffer.len) {
                assert(stdout_size == capture.buffer.len + 1);
                if (stdout_buffer_eof[0] == '\n') {
                    stdout_size -= 1;
                } else {
                    return error.ProcessTooMuchData;
                }
            } else {
                if (stdout_size > 0 and capture.buffer[stdout_size - 1] == '\n') {
                    stdout_size -= 1;
                }
            }
            assert(stdout_size <= capture.buffer.len);

            if (std.mem.indexOfScalar(u8, capture.buffer[0..stdout_size], '\n') != null) {
                return error.ProcessTooManyLines;
            }
        } else {
            if (stdout_size > capture.buffer.len) {
                return error.ProcessTooMuchData;
            }
            assert(stdout_size <= capture.buffer.len);
        }
        capture.result.* = capture.buffer[0..stdout_size];
    }

    const term = try process.wait();
    assert(process.state == .executed);

    errdefer log.err("term={}", .{term});

    // Design principle: unless term/status is communicated to the caller, the
    // result is checked and the error is raised automatically.
    if (options.capture_term != null and
        options.capture_term.? == .full)
    {
        assert(options.capture_term.?.full.* == null);
        options.capture_term.?.full.* = term;
        return;
    }

    const status: u8 = switch (term) {
        .exited => |status| status,
        else => return error.ProcessAbnormalTermination,
    };

    if (options.capture_term != null) {
        assert(options.capture_term.? == .status);
        assert(options.capture_term.?.status.* == null);
        options.capture_term.?.status.* = status;
        return;
    }

    if (status != 0) {
        return error.ProcessAbnormalStatus;
    }
}

fn combine_environment(process: *Process) !?std.process.Environ.Map {
    if (process.envp.items.len == 0) return null;
    const arena = process.spawn_arena.allocator();

    var result = try process.parent_environment.clone(arena);
    for (process.envp.items) |entry| {
        try result.putMove(entry.key, entry.value);
    }
    return result;
}

pub fn format(process: *const Process, writer: *std.Io.Writer) !void {
    for (process.argv.items, 0..) |arg, arg_index| {
        if (arg_index > 0) try writer.writeByte(' ');
        try quote_arg(writer, arg);
    }
}

pub fn quote_arg(writer: *std.Io.Writer, arg: []const u8) !void {
    if (std.mem.indexOfAny(u8, arg, " \t\n\"\\") == null) {
        return try writer.writeAll(arg);
    }
    try writer.writeAll("\"");
    var rest = arg;
    while (std.mem.indexOfAny(u8, rest, "\t\n\"\\")) |index| {
        try writer.writeAll(rest[0..index]);
        switch (rest[index]) {
            ' ' => unreachable,
            '\t' => try writer.writeAll("\\t"),
            '\n' => try writer.writeAll("\\n"),
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            else => unreachable,
        }
        rest = rest[index + 1 ..];
    }
    try writer.writeAll(rest);
    try writer.writeAll("\"");
}

const Argv = struct {
    gpa: std.mem.Allocator,
    args: *std.ArrayList([]const u8),

    fn slice(argv: *const Argv) []const []const u8 {
        return argv.args.items;
    }

    fn append_new_arg(argv: *Argv, comptime arg_fmt: []const u8, arg: anytype) !void {
        const arg_owned = try std.fmt.allocPrint(
            argv.gpa,
            arg_fmt,
            arg,
        );
        errdefer argv.gpa.free(arg_owned);

        try argv.args.append(argv.gpa, arg_owned);
    }

    fn extend_last_arg(argv: *Argv, comptime arg_fmt: []const u8, arg: anytype) !void {
        assert(argv.args.items.len > 0);
        const arg_allocated = try std.fmt.allocPrint(
            argv.gpa,
            "{s}" ++ arg_fmt,
            .{argv.args.items[argv.args.items.len - 1]} ++ arg,
        );
        argv.gpa.free(argv.args.items[argv.args.items.len - 1]);
        argv.args.items[argv.args.items.len - 1] = arg_allocated;
    }
};

/// Expands `cmd` into an array of command arguments, substituting values from `cmd_args`.
///
/// This avoids shell injection by construction as it doesn't concatenate strings.
fn expand_argv(argv: *Argv, comptime cmd: []const u8, cmd_args: anytype) !void {
    @setEvalBranchQuota(5_000);
    // Mostly copy-paste from std.fmt.format

    comptime var pos: usize = 0;

    // For arguments like `tigerbeetle-{version}.exe`, we want to concatenate literal suffix
    // ("tigerbeetle-") and prefix (".exe") to the value of `version` interpolated argument.
    //
    // These two variables track the spaces around `{}` syntax.
    comptime var concat_left: bool = false;
    comptime var concat_right: bool = false;

    const arg_count = std.meta.fields(@TypeOf(cmd_args)).len;
    comptime var args_used: stdx.BitSetType(arg_count) = .{};
    comptime assert(std.mem.indexOfScalar(u8, cmd, '\'') == null); // Intentionally unsupported.
    comptime assert(std.mem.indexOfScalar(u8, cmd, '"') == null);
    inline while (pos < cmd.len) {
        inline while (pos < cmd.len and (cmd[pos] == ' ' or cmd[pos] == '\n')) {
            pos += 1;
        }

        const pos_start = pos;
        inline while (pos < cmd.len) : (pos += 1) {
            switch (cmd[pos]) {
                ' ', '\n', '{' => break,
                else => {},
            }
        }

        const pos_end = pos;
        if (pos_start != pos_end) {
            if (concat_right) {
                assert(pos_start > 0 and cmd[pos_start - 1] == '}');
                try argv.extend_last_arg("{s}", .{cmd[pos_start..pos_end]});
            } else {
                try argv.append_new_arg("{s}", .{cmd[pos_start..pos_end]});
            }
        }

        concat_left = false;
        concat_right = false;

        if (pos >= cmd.len) break;
        if (cmd[pos] == ' ' or cmd[pos] == '\n') continue;

        comptime assert(cmd[pos] == '{');
        concat_left = pos > 0 and cmd[pos - 1] != ' ' and cmd[pos - 1] != '\n';
        if (concat_left) assert(argv.slice().len > 0);
        pos += 1;

        const pos_arg_start = pos;
        inline while (pos < cmd.len and cmd[pos] != '}') : (pos += 1) {}
        const pos_arg_end = pos;

        if (pos >= cmd.len) @compileError("Missing closing }");

        comptime assert(cmd[pos] == '}');
        concat_right = pos + 1 < cmd.len and cmd[pos + 1] != ' ' and cmd[pos + 1] != '\n';
        pos += 1;

        const arg_name = comptime cmd[pos_arg_start..pos_arg_end];
        const arg_or_slice = @field(cmd_args, arg_name);
        comptime args_used.set(for (std.meta.fieldNames(@TypeOf(cmd_args)), 0..) |field, index| {
            if (std.mem.eql(u8, field, arg_name)) break index;
        } else unreachable);

        const T = @TypeOf(arg_or_slice);

        if (@typeInfo(T) == .int or @typeInfo(T) == .comptime_int) {
            if (concat_left) {
                try argv.extend_last_arg("{d}", .{arg_or_slice});
            } else {
                try argv.append_new_arg("{d}", .{arg_or_slice});
            }
        } else if (std.meta.Elem(T) == u8) {
            if (concat_left) {
                try argv.extend_last_arg("{s}", .{arg_or_slice});
            } else {
                try argv.append_new_arg("{s}", .{arg_or_slice});
            }
        } else if (std.meta.Elem(T) == []const u8) {
            if (concat_left or concat_right) @compileError("Can't concatenate slices");
            for (arg_or_slice) |arg_part| {
                try argv.append_new_arg("{s}", .{arg_part});
            }
        } else {
            @compileError("Unsupported argument type");
        }
    }

    comptime if (args_used.count() != arg_count) @compileError("Unused argument");
}
