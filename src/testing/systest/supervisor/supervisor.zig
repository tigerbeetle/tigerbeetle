const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const tigerbeetle = @import("tigerbeetle");

const replica_count = 3;

const Args = struct {
    tigerbeetle_executable: []const u8,
    client_command: []const u8,
};

const Replica = struct {
    name: []const u8,
    process: *LoggedProcess,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer assert(gpa.deinit() == .ok);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var args = try std.process.argsWithAllocator(arena.allocator());

    const cli_args = tigerbeetle.flags.parse(&args, Args);
    const ports = .{ 3000, 3001, 3002 };

    var replicas: [replica_count]Replica = undefined;
    for (0..replica_count) |i| {
        const name = try std.fmt.allocPrint(arena.allocator(), "replica {d}", .{i});
        const datafile = try std.fmt.allocPrint(
            arena.allocator(),
            "{s}/0_{d}.tigerbeetle",
            .{ "/tmp/systest", i },
        );
        const addresses = try std.fmt.allocPrint(
            arena.allocator(),
            "--addresses={s}",
            .{try comma_separate_ports(arena.allocator(), &ports)},
        );
        const argv = try arena.allocator().dupe([]const u8, &.{
            cli_args.tigerbeetle_executable,
            "start",
            addresses,
            datafile,
        });
        var process = try LoggedProcess.init(arena.allocator(), name, argv);
        replicas[i] = .{ .name = name, .process = process };
        try process.start();
    }

    std.time.sleep(5 * std.time.ns_per_s);

    for (replicas) |replica| {
        try replica.process.stop();
        replica.process.deinit();
    }
}

const LoggedProcess = struct {
    const Self = @This();
    const State = enum { initial, running, stopped };

    // Passed in to init
    allocator: std.mem.Allocator,
    name: []const u8,
    argv: []const []const u8,

    // Allocated by init
    arena: std.heap.ArenaAllocator,
    cwd: []const u8,

    // Lifecycle state
    child: ?std.process.Child = null,
    stderr_thread: ?std.Thread = null,
    state: State,

    fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        argv: []const []const u8,
    ) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);

        const cwd = try std.process.getCwdAlloc(arena.allocator());

        const replica = try allocator.create(Self);
        replica.* = .{
            .allocator = allocator,
            .arena = arena,
            .name = name,
            .cwd = cwd,
            .argv = argv,
            .state = .initial,
        };
        return replica;
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
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Ignore;
        child.stderr_behavior = .Pipe;

        try child.spawn();

        std.debug.print(
            "{s}: {s}\n",
            .{ self.name, try format_argv(self.arena.allocator(), self.argv) },
        );

        self.stderr_thread = try std.Thread.spawn(
            .{},
            struct {
                fn log(stderr: std.fs.File, replica: *Self) void {
                    while (true) {
                        var buf: [1024]u8 = undefined;
                        const line_opt = stderr.reader().readUntilDelimiterOrEof(&buf, '\n') catch |err| {
                            std.debug.print("{s}: failed reading stderr: {any}\n", .{ replica.name, err });
                            break;
                        };
                        if (line_opt) |line| {
                            std.debug.print("{s}: {s}\n", .{ replica.name, line });
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

    fn stop(
        self: *Self,
    ) !void {
        assert(self.state == .running);
        defer assert(self.state == .stopped);

        std.debug.print("{s}: stopping\n", .{self.name});

        var child = self.child.?;
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

        // Stop the logging thread
        stderr_thread.join();

        // Await the terminated process
        _ = child.wait() catch unreachable;

        std.debug.print("{s}: stopped\n", .{self.name});

        self.child = null;
        self.stderr_thread = null;
        self.state = .stopped;
    }
};

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
    var replica = try LoggedProcess.init(allocator, name, argv);
    defer replica.deinit();

    // start & stop
    try replica.start();
    std.time.sleep(10 * std.time.ns_per_ms);
    try replica.stop();

    std.time.sleep(10 * std.time.ns_per_ms);

    // restart & stop
    try replica.start();
    std.time.sleep(10 * std.time.ns_per_ms);
    try replica.stop();
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
