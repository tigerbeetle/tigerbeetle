//! Collection of utilities for scripting: an in-process sh+coreutils combo.
//!
//! Keep this as a single file, independent from the rest of the codebase, to make it easier to
//! re-use across different processes (eg build.zig).
//!
//! If possible, avoid shelling out to `sh` or other systems utils --- the whole purpose here is to
//! avoid any extra dependencies.
//!
//! The `exec_` family of methods provides a convenience wrapper around `std.ChildProcess`:
//!   - It allows constructing the array of arguments using convenient interpolation syntax a-la
//!     `std.fmt` (but of course no actual string concatenation happens anywhere).
//!   - `ChildProcess` is versatile and has many knobs, but they might be hard to use correctly (eg,
//!     its easy to forget to check exit status). `Shell` instead is focused on providing a set of
//!     specific narrow use-cases (eg, parsing the output of a subprocess) and takes care of setting
//!     the right defaults.

const std = @import("std");
const log = std.log;
const builtin = @import("builtin");
const assert = std.debug.assert;

const Shell = @This();

/// For internal use by the `Shell` itself.
gpa: std.mem.Allocator,

/// To improve ergonomics, any returned data is owned by the `Shell` and is stored in this arena.
/// This way, the user doesn't need to worry about deallocating each individual string, as long as
/// they don't forget to call `Shell.destroy`.
arena: std.heap.ArenaAllocator,

/// Root directory of this repository.
///
/// Prefer this to `std.fs.cwd()` if possible.
///
/// This is initialized when a shell is created. It would be more flexible to lazily initialize this
/// on the first access, but, given that we always use `Shell` in the context of our repository,
/// eager initialization is more ergonomic.
project_root: std.fs.Dir,

env: std.process.EnvMap,

pub fn create(gpa: std.mem.Allocator) !*Shell {
    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();

    var project_root = try discover_project_root();
    errdefer project_root.close();

    var result = try gpa.create(Shell);
    result.* = Shell{
        .gpa = gpa,
        .arena = arena,
        .project_root = project_root,
        .env = try std.process.getEnvMap(gpa),
    };

    return result;
}

pub fn destroy(shell: *Shell) void {
    const gpa = shell.gpa;

    shell.env.deinit();
    shell.arena.deinit();
    gpa.destroy(shell);
}

const ansi = .{
    .red = "\x1b[0;31m",
    .reset = "\x1b[0m",
};

/// Prints formatted input to stderr.
/// Newline symbol is appended automatically.
/// ANSI colors are supported via `"{ansi-red}my colored text{ansi-reset}"` syntax.
pub fn echo(shell: *Shell, comptime fmt: []const u8, fmt_args: anytype) void {
    _ = shell;

    comptime var fmt_ansi: []const u8 = "";
    comptime var pos: usize = 0;
    comptime var pos_start: usize = 0;

    comptime next_pos: while (pos < fmt.len) {
        if (fmt[pos] == '{') {
            for (std.meta.fieldNames(@TypeOf(ansi))) |field_name| {
                const tag = "{ansi-" ++ field_name ++ "}";
                if (std.mem.startsWith(u8, fmt[pos..], tag)) {
                    fmt_ansi = fmt_ansi ++ fmt[pos_start..pos] ++ @field(ansi, field_name);
                    pos += tag.len;
                    pos_start = pos;
                    continue :next_pos;
                }
            }
        }
        pos += 1;
    };
    comptime assert(pos == fmt.len);

    fmt_ansi = fmt_ansi ++ fmt[pos_start..pos] ++ "\n";

    std.debug.print(fmt_ansi, fmt_args);
}

pub fn print(shell: *Shell, comptime fmt: []const u8, fmt_args: anytype) ![]const u8 {
    return std.fmt.allocPrint(shell.arena.allocator(), fmt, fmt_args);
}

pub fn env_get(shell: *Shell, var_name: []const u8) ![]const u8 {
    errdefer {
        log.err("environment variable '{s}' not defined", .{var_name});
    }

    return try std.process.getEnvVarOwned(shell.arena.allocator(), var_name);
}

/// Checks if the path exists and is a directory.
pub fn dir_exists(shell: *Shell, path: []const u8) !bool {
    _ = shell;

    return subdir_exists(std.fs.cwd(), path);
}

fn subdir_exists(dir: std.fs.Dir, path: []const u8) !bool {
    const stat = dir.statFile(path) catch |err| switch (err) {
        error.FileNotFound => return false,
        error.IsDir => return true,
        else => return err,
    };

    return stat.kind == .directory;
}

const FindOptions = struct {
    where: []const []const u8,
    extension: ?[]const u8 = null,
    extensions: ?[]const []const u8 = null,
};

/// Analogue of the `find` utility, returns a set of paths matching filtering criteria.
///
/// Returned slice is stored in `Shell.arena`.
pub fn find(shell: *Shell, options: FindOptions) ![]const []const u8 {
    if (options.extension != null and options.extensions != null) {
        @panic("conflicting extension filters");
    }
    if (options.extension) |extension| {
        assert(extension[0] == '.');
    }
    if (options.extensions) |extensions| {
        for (extensions) |extension| {
            assert(extension[0] == '.');
        }
    }

    var result = std.ArrayList([]const u8).init(shell.arena.allocator());

    const cwd = std.fs.cwd();
    for (options.where) |base_path| {
        var base_dir = try cwd.openIterableDir(base_path, .{});
        defer base_dir.close();

        var walker = try base_dir.walk(shell.gpa);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind == .file and find_filter_path(entry.path, options)) {
                const full_path =
                    try std.fs.path.join(shell.arena.allocator(), &.{ base_path, entry.path });
                try result.append(full_path);
            }
        }
    }

    return result.items;
}

fn find_filter_path(path: []const u8, options: FindOptions) bool {
    if (options.extension == null and options.extensions == null) return true;
    if (options.extension != null and options.extensions != null) @panic("conflicting filters");

    if (options.extension) |extension| {
        return std.mem.endsWith(u8, path, extension);
    }

    if (options.extensions) |extensions| {
        for (extensions) |extension| {
            if (std.mem.endsWith(u8, path, extension)) return true;
        }
        return false;
    }

    unreachable;
}

/// Copy file, creating the destination directory as necessary.
pub fn copy_path(
    src_dir: std.fs.Dir,
    src_path: []const u8,
    dst_dir: std.fs.Dir,
    dst_path: []const u8,
) !void {
    errdefer {
        log.err("failed to copy {s} to {s}", .{ src_path, dst_path });
    }
    if (std.fs.path.dirname(dst_path)) |dir| {
        try dst_dir.makePath(dir);
    }
    try src_dir.copyFile(src_path, dst_dir, dst_path, .{});
}

/// Runs the given command with inherited stdout and stderr.
/// Returns an error if exit status is non-zero.
///
/// Supports interpolation using the following syntax:
///
/// ```
/// shell.exec("git branch {op} {branches}", .{
///     .op = "-D",
///     .branches = &.{"main", "feature"},
/// })
/// ```
pub fn exec(shell: Shell, comptime cmd: []const u8, cmd_args: anytype) !void {
    var argv = Argv.init(shell.gpa);
    defer argv.deinit();

    try expand_argv(&argv, cmd, cmd_args);

    var child = std.ChildProcess.init(argv.slice(), shell.gpa);
    child.env_map = &shell.env;
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

    echo_command(&child);
    const term = try child.spawnAndWait();

    switch (term) {
        .Exited => |code| if (code != 0) return error.NonZeroExitStatus,
        else => return error.CommandFailed,
    }
}

/// Returns `true` if the command executed successfully with a zero exit code and an empty stderr.
///
/// One intended use-case is sanity-checking that an executable is present, by running
/// `my-tool --version`.
pub fn exec_status_ok(shell: *Shell, comptime cmd: []const u8, cmd_args: anytype) !bool {
    var argv = Argv.init(shell.gpa);
    defer argv.deinit();

    try expand_argv(&argv, cmd, cmd_args);

    const res = std.ChildProcess.exec(.{
        .allocator = shell.gpa,
        .argv = argv.slice(),
    }) catch return false;
    defer shell.gpa.free(res.stderr);
    defer shell.gpa.free(res.stdout);

    return switch (res.term) {
        .Exited => |code| code == 0 and res.stderr.len == 0,
        else => false,
    };
}

/// Run the command and return its stdout.
///
/// Returns an error if the command exists with a non-zero status or a non-empty stderr.
///
/// Trims the trailing newline, if any.
pub fn exec_stdout(shell: *Shell, comptime cmd: []const u8, cmd_args: anytype) ![]const u8 {
    var argv = Argv.init(shell.gpa);
    defer argv.deinit();

    try expand_argv(&argv, cmd, cmd_args);

    const res = try std.ChildProcess.exec(.{
        .allocator = shell.arena.allocator(),
        .argv = argv.slice(),
    });
    defer shell.arena.allocator().free(res.stderr);
    errdefer shell.arena.allocator().free(res.stdout);

    switch (res.term) {
        .Exited => |code| if (code != 0) {
            std.debug.print(
                \\Debugging failure (stdout): {s}
                \\Debugging failure (stderr): {s}
                \\
            , .{
                res.stdout,
                res.stderr,
            });
            return error.NonZeroExitStatus;
        },
        else => return error.CommandFailed,
    }

    if (res.stderr.len > 0) return error.NonEmptyStderr;

    const trailing_newline = if (std.mem.indexOf(u8, res.stdout, "\n")) |first_newline|
        first_newline == res.stdout.len - 1
    else
        false;
    return res.stdout[0 .. res.stdout.len - if (trailing_newline) @as(usize, 1) else 0];
}

/// Spawns the process, piping its stdout and stderr.
///
/// The caller must `.kill()` and `.wait()` the child, to minimize the chance of process leak (
/// sadly, child will still be leaked if the parent process is killed itself, because POSIX doesn't
/// have nice APIs for structured concurrency).
pub fn spawn(shell: Shell, comptime cmd: []const u8, cmd_args: anytype) !std.ChildProcess {
    var argv = Argv.init(shell.gpa);
    defer argv.deinit();

    try expand_argv(&argv, cmd, cmd_args);

    var child = std.ChildProcess.init(argv.slice(), shell.gpa);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();

    // XXX: child.argv is undefined when we return. This is fine though, as it is only used during
    // `spawn`.
    return child;
}

/// Runs the zig compiler.
pub fn zig(shell: Shell, comptime cmd: []const u8, cmd_args: anytype) !void {
    const zig_exe = try shell.project_root.realpathAlloc(
        shell.gpa,
        comptime "zig/zig" ++ builtin.target.exeFileExt(),
    );
    defer shell.gpa.free(zig_exe);

    var argv = Argv.init(shell.gpa);
    defer argv.deinit();

    try argv.append_new_arg(zig_exe);
    try expand_argv(&argv, cmd, cmd_args);

    var child = std.ChildProcess.init(argv.slice(), shell.gpa);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

    echo_command(&child);
    const term = try child.spawnAndWait();

    switch (term) {
        .Exited => |code| if (code != 0) return error.NonZeroExitStatus,
        else => return error.CommandFailed,
    }
}

/// If we inherit `stdout` to show the output to the user, it's also helpful to echo the command
/// itself.
fn echo_command(child: *const std.ChildProcess) void {
    assert(child.stdout_behavior == .Inherit);

    std.debug.print("$ ", .{});
    for (child.argv, 0..) |arg, i| {
        if (i != 0) std.debug.print(" ", .{});
        std.debug.print("{s}", .{arg});
    }
    std.debug.print("\n", .{});
}

/// Returns current git commit hash as an ASCII string.
pub fn git_commit(shell: *Shell) ![]const u8 {
    const stdout = try shell.exec_stdout("git rev-parse --verify HEAD", .{});
    if (stdout.len != 40) return error.InvalidCommitFormat;
    return stdout;
}

/// Returns current git tag.
pub fn git_tag(shell: *Shell) ![]const u8 {
    // --always is necessary in cases where we haven't yet `git fetch`-ed.
    const stdout = try shell.exec_stdout("git describe --tags --always", .{});
    return stdout;
}

const Argv = struct {
    args: std.ArrayList([]const u8),

    fn init(gpa: std.mem.Allocator) Argv {
        return Argv{ .args = std.ArrayList([]const u8).init(gpa) };
    }

    fn deinit(argv: *Argv) void {
        for (argv.args.items) |arg| argv.args.allocator.free(arg);
        argv.args.deinit();
    }

    fn slice(argv: *Argv) []const []const u8 {
        return argv.args.items;
    }

    fn append_new_arg(argv: *Argv, arg: []const u8) !void {
        const arg_owned = try argv.args.allocator.dupe(u8, arg);
        errdefer argv.args.allocator.free(arg_owned);

        try argv.args.append(arg_owned);
    }

    fn extend_last_arg(argv: *Argv, arg: []const u8) !void {
        assert(argv.args.items.len > 0);
        const arg_allocated = try std.fmt.allocPrint(argv.args.allocator, "{s}{s}", .{
            argv.args.items[argv.args.items.len - 1],
            arg,
        });
        argv.args.allocator.free(argv.args.items[argv.args.items.len - 1]);
        argv.args.items[argv.args.items.len - 1] = arg_allocated;
    }
};

/// Expands `cmd` into an array of command arguments, substituting values from `cmd_args`.
///
/// This avoids shell injection by construction as it doesn't concatenate strings.
fn expand_argv(argv: *Argv, comptime cmd: []const u8, cmd_args: anytype) !void {
    // Mostly copy-paste from std.fmt.format

    comptime var pos: usize = 0;

    // For arguments like `tigerbeetle-{version}.exe`, we want to concatenate literal suffix
    // ("tigerbeetle-") and prefix (".exe") to the value of `version` interpolated argument.
    //
    // These two variables track the spaces around `{}` syntax.
    comptime var concat_left: bool = false;
    comptime var concat_right: bool = false;

    const arg_count = std.meta.fields(@TypeOf(cmd_args)).len;
    comptime var args_used = std.StaticBitSet(arg_count).initEmpty();
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
                try argv.extend_last_arg(cmd[pos_start..pos_end]);
            } else {
                try argv.append_new_arg(cmd[pos_start..pos_end]);
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

        if (std.meta.Elem(T) == u8) {
            if (concat_left) {
                try argv.extend_last_arg(arg_or_slice);
            } else {
                try argv.append_new_arg(arg_or_slice);
            }
        } else if (std.meta.Elem(T) == []const u8) {
            if (concat_left or concat_right) @compileError("Can't concatenate slices");
            for (arg_or_slice) |arg_part| {
                try argv.append_new_arg(arg_part);
            }
        } else {
            @compileError("Unsupported argument type");
        }
    }

    comptime if (args_used.count() != arg_count) @compileError("Unused argument");
}

const Snap = @import("./testing/snaptest.zig").Snap;
const snap = Snap.snap;

test "shell: expand_argv" {
    const T = struct {
        fn check(
            comptime cmd: []const u8,
            args: anytype,
            want: Snap,
        ) !void {
            var argv = Argv.init(std.testing.allocator);
            defer argv.deinit();

            try expand_argv(&argv, cmd, args);
            try want.diff_json(argv.slice());
        }
    };

    try T.check("zig version", .{}, snap(@src(),
        \\["zig","version"]
    ));
    try T.check("  zig  version  ", .{}, snap(@src(),
        \\["zig","version"]
    ));

    try T.check(
        "zig {version}",
        .{ .version = @as([]const u8, "version") },
        snap(@src(),
            \\["zig","version"]
        ),
    );

    try T.check(
        "zig {version}",
        .{ .version = @as([]const []const u8, &.{ "version", "--verbose" }) },
        snap(@src(),
            \\["zig","version","--verbose"]
        ),
    );
}

/// Finds the root of TigerBeetle repo.
///
/// Caller is responsible for closing the dir.
fn discover_project_root() !std.fs.Dir {
    // TODO(Zig): https://github.com/ziglang/zig/issues/16779
    const ancestors = "./" ++ "../" ** 16;

    var level: u32 = 0;
    while (level < 16) : (level += 1) {
        const ancestor = ancestors[0 .. 2 + 3 * level];
        assert(ancestor[ancestor.len - 1] == '/');

        var current = try std.fs.cwd().openDir(ancestor, .{});
        errdefer current.close();

        if (current.statFile("src/shell.zig")) |_| {
            return current;
        } else |err| switch (err) {
            error.FileNotFound => {
                current.close();
            },
            else => return err,
        }
    }

    return error.DiscoverProjectRootDepthExceeded;
}
