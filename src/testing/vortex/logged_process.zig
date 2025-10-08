//! Runs a subprocess inheriting its stderr output. The processes can terminate by its own or be
//! actively terminated with a SIGKILL.
//!
//! We use this to run a cluster of replicas and the workload.
//!
//! NOTE: some duplication exists between this and `tmp_tigerbeetle.zig` that could perhaps be
//! unified.
const std = @import("std");
const builtin = @import("builtin");

const log = std.log.scoped(.logged_process);

const assert = std.debug.assert;

const LoggedProcess = @This();

pub const State = enum(u8) { running, stopped, terminated };
const AtomicState = std.atomic.Value(State);

const Options = struct {
    stdout_behavior: std.process.Child.StdIo = .Ignore,
};

// Allocated by init
child: std.process.Child,
stdin_thread: std.Thread,
stderr_thread: std.Thread,

// Lifecycle state
current_state: AtomicState,

pub fn spawn(
    allocator: std.mem.Allocator,
    argv: []const []const u8,
    options: Options,
) !*LoggedProcess {
    const self = try allocator.create(LoggedProcess);
    errdefer allocator.destroy(self);

    self.* = .{
        .current_state = AtomicState.init(.running),
        .child = std.process.Child.init(argv, allocator),
        .stdin_thread = undefined,
        .stderr_thread = undefined,
    };

    self.child.stdin_behavior = .Pipe;
    self.child.stdout_behavior = options.stdout_behavior;
    self.child.stderr_behavior = .Inherit;

    try self.child.spawn();

    errdefer {
        _ = self.child.kill() catch {};
    }

    // Zig doesn't have non-blocking version of child.wait, so we use `BrokenPipe`
    // on writing to child's stdin to detect if a child is dead in a non-blocking
    // manner. Checks once a second in a separate thread.
    _ = try std.posix.fcntl(
        self.child.stdin.?.handle,
        std.posix.F.SETFL,
        @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true })),
    );
    self.stdin_thread = try std.Thread.spawn(
        .{},
        struct {
            fn poll_broken_pipe(stdin: std.fs.File, process: *LoggedProcess) void {
                while (process.state() == .running) {
                    std.time.sleep(1 * std.time.ns_per_s);
                    _ = stdin.write(&.{1}) catch |err| {
                        switch (err) {
                            error.WouldBlock => {}, // still running
                            error.BrokenPipe,
                            error.NotOpenForWriting,
                            => {
                                // Only write the state variable in case the process is still
                                // considered running. If it was actively terminated, we don't
                                // want to overwrite that state.
                                _ = process.current_state.cmpxchgStrong(
                                    .running,
                                    .terminated,
                                    .seq_cst,
                                    .seq_cst,
                                );
                                break;
                            },
                            else => @panic(@errorName(err)),
                        }
                    };
                }
            }
        }.poll_broken_pipe,
        .{ self.child.stdin.?, self },
    );

    return self;
}

pub fn destroy(self: *LoggedProcess, allocator: std.mem.Allocator) void {
    assert(self.state() == .terminated);
    allocator.destroy(self);
}

pub fn state(self: *LoggedProcess) State {
    return self.current_state.load(.seq_cst);
}

pub fn stop(
    self: *LoggedProcess,
) !void {
    assert(builtin.os.tag != .windows);
    try std.posix.kill(self.child.id, std.posix.SIG.STOP);
    self.current_state.store(.stopped, .seq_cst);
}

pub fn cont(
    self: *LoggedProcess,
) !void {
    assert(builtin.os.tag != .windows);
    try std.posix.kill(self.child.id, std.posix.SIG.CONT);
    self.current_state.store(.running, .seq_cst);
}

pub fn terminate(
    self: *LoggedProcess,
) !std.process.Child.Term {
    self.expect_state_in(.{ .running, .stopped });
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

    // Await thread.
    self.stdin_thread.join();

    // Await the terminated process.
    const term = self.child.wait() catch unreachable;

    self.current_state.store(.terminated, .seq_cst);

    return term;
}

pub fn wait(
    self: *LoggedProcess,
) !std.process.Child.Term {
    self.expect_state_in(.{ .running, .terminated });
    defer self.expect_state_in(.{.terminated});

    // Wait until the process runs to completion.
    const term = self.child.wait();

    if (self.state() == .running) {
        // Await thread in case this process is still running.
        self.stdin_thread.join();
    }

    return term;
}

fn expect_state_in(self: *LoggedProcess, comptime valid_states: anytype) void {
    const actual_state = self.state();

    inline for (valid_states) |valid| {
        if (actual_state == valid) return;
    }

    log.err("expected state in {any} but actual state is {s}", .{
        valid_states,
        @tagName(actual_state),
    });
    unreachable;
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

    std.time.sleep(10 * std.time.ns_per_ms);
    _ = try process.terminate();
}

/// Formats the ports as comma-separated. Caller owns slice after successful return.
pub fn comma_separate_ports(allocator: std.mem.Allocator, ports: []const u16) ![]const u8 {
    assert(ports.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    errdefer out.deinit();

    const writer = out.writer();

    try std.fmt.format(writer, "{d}", .{ports[0]});
    for (ports[1..]) |port| {
        try writer.writeByte(',');
        try std.fmt.format(writer, "{d}", .{port});
    }

    return out.toOwnedSlice();
}

test comma_separate_ports {
    const formatted = try comma_separate_ports(std.testing.allocator, &.{ 3000, 3001, 3002 });
    defer std.testing.allocator.free(formatted);

    try std.testing.expectEqualStrings("3000,3001,3002", formatted);
}
