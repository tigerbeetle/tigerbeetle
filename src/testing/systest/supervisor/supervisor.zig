const std = @import("std");
const builtin = @import("builtin");
const flags = @import("../../../flags.zig");
const Shell = @import("../../../shell.zig");

const assert = std.debug.assert;

const replica_count = 3;
const replica_ports = [replica_count]u16{ 3000, 3001, 3002 };

const run_time_ns = 30 * std.time.ns_per_s;
const tick_ns = 50 * std.time.ns_per_ms;
const target_tick_count = @divFloor(run_time_ns, tick_ns);

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
};

const Replica = struct {
    name: []const u8,
    process: *LoggedProcess,
};

pub fn main(shell: *Shell, allocator: std.mem.Allocator, args: CLIArgs) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteDir(tmp_dir) catch {};

    var replicas: [replica_count]Replica = undefined;
    for (0..replica_count) |i| {
        const name = try shell.fmt("replica {d}", .{i});
        const datafile = try shell.fmt("{s}/1_{d}.tigerbeetle", .{ tmp_dir, i });

        // Format datafile
        try shell.exec(
            \\{tigerbeetle} format 
            \\  --cluster=1
            \\  --replica={index}
            \\  --replica-count={replica_count} 
            \\  {datafile}
        , .{
            .tigerbeetle = args.tigerbeetle_executable,
            .index = i,
            .replica_count = replica_count,
            .datafile = datafile,
        });

        // Start replica
        const addresses = try shell.fmt("--addresses={s}", .{
            try comma_separate_ports(shell.arena.allocator(), &replica_ports),
        });
        const argv = try shell.arena.allocator().dupe([]const u8, &.{
            args.tigerbeetle_executable,
            "start",
            addresses,
            datafile,
        });

        var process = try LoggedProcess.init(allocator, name, argv, .{});
        replicas[i] = .{ .name = name, .process = process };
        try process.start();
    }

    // Start workload
    const workload = try start_workload(shell, allocator);

    // Let it finish by itself, or kill it after `target_tick_count` ticks.
    const workload_result = term: {
        for (0..target_tick_count) |tick| {
            const duration_ns = tick * tick_ns;
            if (@rem(duration_ns, std.time.ns_per_s) == 0) {
                // std.debug.print("supervisor: waited for {d}\n", .{@divExact(duration_ns, std.time.ns_per_s)});
            }
            if (workload.state == .completed) {
                break :term try workload.wait();
            }
            std.time.sleep(tick_ns);
        }

        break :term try workload.terminate();
    };

    workload.deinit();

    for (replicas) |replica| {
        _ = try replica.process.terminate();
        replica.process.deinit();
    }

    switch (workload_result) {
        .Exited => |code| {
            if (code == 128 + std.posix.SIG.TERM) {
                std.debug.print("workload terminated as requested\n", .{});
            } else if (code >= 128) {
                const signal = @mod(code, 128);
                std.debug.print("workload terminated after signal {d}\n", .{signal});
            } else {
                std.debug.print("workload exited unexpectedly with code {d}\n", .{code});
                std.process.exit(1);
            }
        },
        else => {},
    }
}

const LoggedProcess = struct {
    const Self = @This();
    const State = enum { initial, running, terminated, completed };
    const Options = struct { env: ?*const std.process.EnvMap = null };

    // Passed in to init
    allocator: std.mem.Allocator,
    name: []const u8,
    argv: []const []const u8,
    options: Options,

    // Allocated by init
    arena: std.heap.ArenaAllocator,
    cwd: []const u8,

    // Lifecycle state
    child: ?std.process.Child = null,
    stdin_thread: ?std.Thread = null,
    stderr_thread: ?std.Thread = null,
    state: State, // TODO: atomic

    fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        argv: []const []const u8,
        options: Options,
    ) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);

        const cwd = try std.process.getCwdAlloc(arena.allocator());

        const process = try allocator.create(Self);
        process.* = .{
            .allocator = allocator,
            .arena = arena,
            .name = name,
            .cwd = cwd,
            .argv = argv,
            .options = options,
            .state = .initial,
        };
        return process;
    }

    fn deinit(self: *Self) void {
        const allocator = self.allocator;
        self.arena.deinit();
        allocator.destroy(self);
    }

    fn start(
        self: *Self,
    ) !void {
        assert(self.state != .running);
        defer assert(self.state == .running);

        var child = std.process.Child.init(self.argv, self.allocator);

        child.cwd = self.cwd;
        child.env_map = self.options.env;
        child.stdin_behavior = .Pipe;
        child.stdout_behavior = .Ignore;
        child.stderr_behavior = .Pipe;

        try child.spawn();

        std.debug.print(
            "{s}: {s}\n",
            .{ self.name, try format_argv(self.arena.allocator(), self.argv) },
        );

        // Zig doesn't have non-blocking version of child.wait, so we use `BrokenPipe`
        // on writing to child's stdin to detect if a child is dead in a non-blocking
        // manner. Checks once a second second in a separate thread.
        _ = try std.posix.fcntl(
            child.stdin.?.handle,
            std.posix.F.SETFL,
            @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true })),
        );
        self.stdin_thread = try std.Thread.spawn(
            .{},
            struct {
                fn poll_broken_pipe(stdin: std.fs.File, process: *Self) void {
                    while (true) {
                        std.time.sleep(1 * std.time.ns_per_s);
                        _ = stdin.write(&.{1}) catch |err| {
                            switch (err) {
                                error.WouldBlock => {}, // still running
                                error.BrokenPipe,
                                error.NotOpenForWriting,
                                => {
                                    process.state = .completed;
                                    break;
                                },
                                else => @panic(@errorName(err)),
                            }
                        };
                    }
                }
            }.poll_broken_pipe,
            .{ child.stdin.?, self },
        );

        // The child process' stderr is echoed to stderr with a name prefix
        self.stderr_thread = try std.Thread.spawn(
            .{},
            struct {
                fn log(stderr: std.fs.File, process: *Self) void {
                    while (true) {
                        var buf: [1024]u8 = undefined;
                        const line_opt = stderr.reader().readUntilDelimiterOrEof(&buf, '\n') catch |err| {
                            std.debug.print("{s}: failed reading stderr: {any}\n", .{ process.name, err });
                            break;
                        };
                        if (line_opt) |line| {
                            std.debug.print("{s}: {s}\n", .{ process.name, line });
                        } else {
                            break;
                        }
                    }
                }
            }.log,
            .{ child.stderr.?, self },
        );

        self.child = child;
        self.state = .running;
    }

    fn terminate(
        self: *Self,
    ) !std.process.Child.Term {
        assert(self.state == .running);
        defer assert(self.state == .terminated);

        std.debug.print("{s}: terminating\n", .{self.name});

        var child = self.child.?;
        const stdin_thread = self.stdin_thread.?;
        const stderr_thread = self.stderr_thread.?;

        // Terminate the process
        //
        // Uses the same method as `src/testing/tmp_tigerbeetle.zig`.
        // See: https://github.com/ziglang/zig/issues/16820
        _ = kill: {
            if (builtin.os.tag == .windows) {
                const exit_code = 1;
                break :kill std.os.windows.TerminateProcess(child.id, exit_code);
            } else {
                break :kill std.posix.kill(child.id, std.posix.SIG.TERM);
            }
        } catch |err| {
            std.debug.print(
                "{s}: failed to kill process: {any}\n",
                .{ self.name, err },
            );
        };

        // Await threads
        stdin_thread.join();
        stderr_thread.join();

        // Await the terminated process
        const term = child.wait() catch unreachable;

        std.debug.print("{s}: terminated\n", .{self.name});

        self.child = null;
        self.stderr_thread = null;
        self.state = .terminated;

        return term;
    }

    fn wait(
        self: *Self,
    ) !std.process.Child.Term {
        assert(self.state == .running or self.state == .completed);
        defer assert(self.state == .completed);

        std.debug.print("{s}: awaiting\n", .{self.name});

        var child = self.child.?;
        const stdin_thread = self.stdin_thread.?;
        const stderr_thread = self.stderr_thread.?;

        // Wait until the process runs to completion
        const term = child.wait();

        // Await threads
        stdin_thread.join();
        stderr_thread.join();

        std.debug.print("{s}: awaited\n", .{self.name});

        self.child = null;
        self.stderr_thread = null;
        self.state = .completed;

        return term;
    }
};

fn start_workload(shell: *Shell, allocator: std.mem.Allocator) !*LoggedProcess {
    const name = "workload";
    const client_jar = "src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar";
    const workload_jar = "src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar";

    const class_path = try shell.fmt("{s}:{s}", .{ client_jar, workload_jar });
    const argv = try shell.arena.allocator().dupe([]const u8, &.{
        "java",
        "-ea",
        "-cp",
        class_path,
        "Main",
    });

    var env = try std.process.getEnvMap(shell.arena.allocator());
    try env.put("REPLICAS", try comma_separate_ports(shell.arena.allocator(), &replica_ports));

    var process = try LoggedProcess.init(allocator, name, argv, .{ .env = &env });
    try process.start();
    return process;
}

fn format_argv(allocator: std.mem.Allocator, argv: []const []const u8) ![]const u8 {
    assert(argv.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    const writer = out.writer();

    try writer.writeAll("$");
    for (argv) |arg| {
        try writer.writeByte(' ');
        try writer.writeAll(arg);
    }

    return try out.toOwnedSlice();
}

fn comma_separate_ports(allocator: std.mem.Allocator, ports: []const u16) ![]const u8 {
    assert(ports.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    const writer = out.writer();

    try std.fmt.format(writer, "{d}", .{ports[0]});
    for (ports[1..]) |port| {
        try writer.writeByte(',');
        try std.fmt.format(writer, "{d}", .{port});
    }

    return out.toOwnedSlice();
}

test "LoggedProcess: starts and stops" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer assert(gpa.deinit() == .ok);

    const argv: []const []const u8 = &.{
        "awk",
        \\ BEGIN { for (i = 0; i < 10; i++) { print i > "/dev/fd/2"; } }
        ,
    };

    const name = "test program";
    var replica = try LoggedProcess.init(allocator, name, argv, .{});
    defer replica.deinit();

    // start & stop
    try replica.start();
    std.time.sleep(10 * std.time.ns_per_ms);
    _ = try replica.terminate();

    std.time.sleep(10 * std.time.ns_per_ms);

    // restart & stop
    try replica.start();
    std.time.sleep(10 * std.time.ns_per_ms);
    _ = try replica.terminate();
}

test "format_argv: space-separates slice as a prompt" {
    const formatted = try format_argv(std.testing.allocator, &.{ "foo", "bar", "baz" });
    defer std.testing.allocator.free(formatted);

    try std.testing.expectEqualStrings("$ foo bar baz", formatted);
}

test "comma-separates ports" {
    const formatted = try comma_separate_ports(std.testing.allocator, &.{ 3000, 3001, 3002 });
    defer std.testing.allocator.free(formatted);

    try std.testing.expectEqualStrings("3000,3001,3002", formatted);
}
