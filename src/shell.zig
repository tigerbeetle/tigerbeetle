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
const builtin = @import("builtin");
const assert = std.debug.assert;

/// For internal use by the `Shell` itself.
gpa: std.mem.Allocator,

/// To improve ergonomics, any returned data is owned by the `Shell` and is stored in this arena.
/// This way, the user doesn't need to worry about deallocating each individual string, as long as
/// they don't forget to call `Shell.destroy`.
arena: std.heap.ArenaAllocator,

const Shell = @This();

pub fn create(allocator: std.mem.Allocator) !*Shell {
    var result = try allocator.create(Shell);
    result.* = Shell{
        .gpa = allocator,
        .arena = std.heap.ArenaAllocator.init(allocator),
    };
    return result;
}

pub fn destroy(shell: *Shell) void {
    const allocator = shell.gpa;

    shell.arena.deinit();
    allocator.destroy(shell);
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

    next_pos: inline while (pos < fmt.len) {
        if (fmt[pos] == '{') {
            inline for (comptime std.meta.fieldNames(@TypeOf(ansi))) |field_name| {
                const tag = comptime "{ansi-" ++ field_name ++ "}";
                if (comptime std.mem.startsWith(u8, fmt[pos..], tag)) {
                    fmt_ansi = fmt_ansi ++ fmt[pos_start..pos] ++ @field(ansi, field_name);
                    pos += tag.len;
                    pos_start = pos;
                    continue :next_pos;
                }
            }
        }
        pos += 1;
    }
    fmt_ansi = fmt_ansi ++ fmt[pos_start..pos] ++ "\n";

    std.debug.print(fmt_ansi, fmt_args);
}

/// Checks if the path exists and is a directory.
pub fn dir_exists(shell: *Shell, path: []const u8) !bool {
    _ = shell;

    const stat = std.fs.cwd().statFile(path) catch |err| switch (err) {
        error.FileNotFound => return false,
        error.IsDir => return true,
        else => return err,
    };

    return stat.kind == .Directory;
}

/// Analogue of the `find` utility, returns a set of paths matching filtering criteria.
///
/// Returned slice is stored in `Shell.arena`.
pub fn find(shell: *Shell, options: struct {
    where: []const []const u8,
    ends_with: []const u8,
}) ![]const []const u8 {
    var result = std.ArrayList([]const u8).init(shell.arena.allocator());

    const cwd = std.fs.cwd();
    for (options.where) |base_path| {
        var base_dir = try cwd.openDir(base_path, .{ .iterate = true });
        defer base_dir.close();

        var walker = try base_dir.walk(shell.gpa);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (std.mem.endsWith(u8, entry.path, options.ends_with)) {
                const full_path =
                    try std.fs.path.join(shell.arena.allocator(), &.{ base_path, entry.path });
                try result.append(full_path);
            }
        }
    }

    return result.items;
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

    const child = try std.ChildProcess.init(argv.items, shell.gpa);
    defer child.deinit();

    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

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
        .argv = argv.items,
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
pub fn exec_stdout(shell: *Shell, comptime cmd: []const u8, cmd_args: anytype) ![]const u8 {
    var argv = Argv.init(shell.gpa);
    defer argv.deinit();

    try expand_argv(&argv, cmd, cmd_args);

    const res = try std.ChildProcess.exec(.{
        .allocator = shell.arena.allocator(),
        .argv = argv.items,
    });
    defer shell.arena.allocator().free(res.stderr);
    errdefer shell.arena.allocator().free(res.stdout);

    switch (res.term) {
        .Exited => |code| if (code == 0) return error.NonZeroExitStatus,
        else => return error.CommandFailed,
    }

    if (res.stderr.len > 0) return error.NonEmptyStderr;
    return res.stdout;
}

/// Returns current git commit hash as an ASCII string.
pub fn git_commit(shell: *Shell) ![40]u8 {
    const stdout = try shell.exec_stdout("git rev-parse --verify HEAD", .{});
    // +1 for trailing newline.
    if (stdout.len != 40 + 1) return error.InvalidCommitFormat;
    return stdout[0..40].*;
}

/// Returns current git tag.
pub fn git_tag(shell: *Shell) ![]const u8 {
    const stdout = try shell.exec_stdout("git describe --tags", .{});
    return stdout;
}

const Argv = std.ArrayList([]const u8);

/// Expands `cmd` into an array of command arguments, substituting values from `cmd_args`.
///
/// This avoids shell injection by construction as it doesn't concatenate strings.
fn expand_argv(argv: *Argv, comptime cmd: []const u8, cmd_args: anytype) !void {
    // Mostly copy-paste from std.fmt.format
    comptime var pos: usize = 0;

    comptime var args_used = 0;
    inline while (pos < cmd.len) {
        inline while (pos < cmd.len and cmd[pos] == ' ') {
            pos += 1;
        }

        const pos_start = pos;
        inline while (pos < cmd.len) : (pos += 1) {
            switch (cmd[pos]) {
                ' ', '{' => break,
                else => {},
            }
        }

        const pos_end = pos;
        if (pos_start != pos_end) {
            try argv.append(cmd[pos_start..pos_end]);
        }

        if (pos >= cmd.len) break;
        if (cmd[pos] == ' ') continue;

        comptime assert(cmd[pos] == '{');
        if (pos > 0 and cmd[pos - 1] != ' ') @compileError("Surround {} with spaces");
        pos += 1;

        const pos_arg_start = pos;
        inline while (pos < cmd.len and cmd[pos] != '}') : (pos += 1) {}
        const pos_arg_end = pos;

        if (pos >= cmd.len) @compileError("Missing closing }");

        comptime assert(cmd[pos] == '}');
        if (pos + 1 < cmd.len and cmd[pos + 1] != ' ') @compileError("Surround {} with spaces");
        pos += 1;

        const arg_name = comptime cmd[pos_arg_start..pos_arg_end];
        const arg_or_slice = @field(cmd_args, arg_name);
        args_used += 1;

        if (@TypeOf(arg_or_slice) == []const u8) {
            try argv.append(arg_or_slice);
        } else if (@TypeOf(arg_or_slice) == []const []const u8) {
            for (arg_or_slice) |arg_part| {
                try argv.append(arg_part);
            }
        } else {
            @compileError("Unsupported argument type");
        }
    }

    const args_provided = std.meta.fields(@TypeOf(cmd_args)).len;
    if (args_used != args_provided) @compileError("Unused argument");
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
            try want.diff_json(argv.items);
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
