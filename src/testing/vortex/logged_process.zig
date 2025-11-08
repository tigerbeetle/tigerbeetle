//! Runs a subprocess inheriting its stderr output. The processes can terminate by its own or be
//! actively terminated with a SIGKILL.
//!
//! We use this to run a cluster of replicas and the workload.
//!
//! NOTE: some duplication exists between this and `tmp_tigerbeetle.zig` that could perhaps be
//! unified.
const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");

const assert = std.debug.assert;
const log = std.log.scoped(.logged_process);

const LoggedProcess = @This();

const Options = struct {
    stdout_behavior: std.process.Child.StdIo = .Ignore,
};

child: std.process.Child,
state: enum { running, paused, terminated },

pub fn spawn(
    allocator: std.mem.Allocator,
    argv: []const []const u8,
    options: Options,
) !*LoggedProcess {
    const self = try allocator.create(LoggedProcess);
    errdefer allocator.destroy(self);

    self.* = .{
        .state = .running,
        .child = std.process.Child.init(argv, allocator),
    };

    self.child.stdin_behavior = .Ignore;
    self.child.stdout_behavior = options.stdout_behavior;
    self.child.stderr_behavior = .Inherit;

    try self.child.spawn();
    errdefer _ = self.child.kill() catch {};

    return self;
}

pub fn destroy(self: *LoggedProcess, allocator: std.mem.Allocator) void {
    assert(self.state == .terminated);
    allocator.destroy(self);
}

pub fn pause(
    self: *LoggedProcess,
) !void {
    comptime assert(builtin.os.tag != .windows);
    assert(self.state == .running);

    try std.posix.kill(self.child.id, std.posix.SIG.STOP);
    self.state = .paused;
}

pub fn unpause(
    self: *LoggedProcess,
) !void {
    comptime assert(builtin.os.tag != .windows);
    assert(self.state == .paused);

    try std.posix.kill(self.child.id, std.posix.SIG.CONT);
    self.state = .running;
}

pub fn terminate(
    self: *LoggedProcess,
) !std.process.Child.Term {
    self.expect_state_in(.{ .running, .paused });
    defer self.expect_state_in(.{.terminated});

    // Terminate the process.
    //
    // Uses the same method as `src/testing/tmp_tigerbeetle.zig`.
    // See: https://github.com/ziglang/zig/issues/16820
    _ = kill: {
        if (builtin.os.tag == .windows) {
            const exit_code = 1;
            break :kill std.os.windows.TerminateProcess(self.child.id, exit_code);
        } else {
            break :kill std.posix.kill(self.child.id, std.posix.SIG.KILL);
        }
    } catch |err| {
        log.err(
            "failed to kill process {d}: {any}\n",
            .{ self.child.id, err },
        );
    };

    const term = try self.child.wait();
    self.state = .terminated;
    return term;
}

pub fn wait(
    self: *LoggedProcess,
) !std.process.Child.Term {
    self.expect_state_in(.{ .running, .terminated });
    defer self.expect_state_in(.{.terminated});

    const term = try self.child.wait();
    self.state = .terminated;
    return term;
}

/// If the process has exited, reap it and return the exit code.
/// Otherwise, return null.
pub fn wait_nonblocking(self: *LoggedProcess) ?std.process.Child.Term {
    self.expect_state_in(.{ .running, .paused });

    const result = std.posix.waitpid(self.child.id, std.posix.W.NOHANG);
    if (result.pid == 0) return null;
    assert(result.pid == self.child.id);

    self.state = .terminated;
    return stdx.term_from_status(result.status);
}

fn expect_state_in(self: *LoggedProcess, comptime valid_states: anytype) void {
    inline for (valid_states) |valid| {
        if (self.state == valid) return;
    }

    std.debug.panic("expected state in {any} but actual state is {s}", .{
        valid_states,
        @tagName(self.state),
    });
}

// The test for LoggedProcess needs an executable that runs forever which is solved (without
// depending on system executables) by using the Zig compiler to build this very file as an
// executable in a temporary directory.
// For production builds, don't include the main function.
// This is `if __name__ == "__main__":` at comptime!
pub const main =
    if (@import("root") != @This()) {} else struct {
        fn main() !void {
            while (true) {
                try std.io.getStdOut().writeAll("yep\n");
            }
        }
    }.main;

test "LoggedProcess: starts and stops" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer assert(gpa.deinit() == .ok);

    const zig_exe = try std.process.getEnvVarOwned(allocator, "ZIG_EXE"); // Set by build.zig
    defer allocator.free(zig_exe);

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_dir_path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        &tmp_dir.sub_path,
    });
    defer allocator.free(tmp_dir_path);

    const test_exe_buf = try allocator.create([std.fs.max_path_bytes]u8);
    defer allocator.destroy(test_exe_buf);

    { // Compile this file as an executable!
        const module_path = "src";
        const path_relative = try std.fs.path.join(allocator, &.{
            module_path,
            @src().file,
        });
        defer allocator.free(path_relative);
        const this_file = try std.fs.cwd().realpath(
            path_relative,
            test_exe_buf,
        );
        const argv = [_][]const u8{ zig_exe, "build-exe", this_file };
        const exec_result = try std.process.Child.run(.{
            .allocator = allocator,
            .argv = &argv,
            .cwd = tmp_dir_path,
        });
        defer allocator.free(exec_result.stdout);
        defer allocator.free(exec_result.stderr);

        if (exec_result.term.Exited != 0) {
            std.debug.print("{s}{s}", .{ exec_result.stdout, exec_result.stderr });
            return error.FailedToCompile;
        }
    }

    const test_exe = try tmp_dir.dir.realpath(
        "logged_process" ++ comptime builtin.target.exeFileExt(),
        test_exe_buf,
    );

    const argv: []const []const u8 = &.{test_exe};

    var process = try LoggedProcess.spawn(allocator, argv, .{});
    defer process.destroy(allocator);

    std.Thread.sleep(10 * std.time.ns_per_ms);
    _ = try process.terminate();
}
