const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const tigerbeetle = @import("tigerbeetle");

const replica_count = 3;

const Args = struct {
    tigerbeetle_executable: []const u8,
    client_command: []const u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const cli_args = tigerbeetle.flags.parse(&args, Args);

    var replicas: [replica_count]*Replica = undefined;
    for (0..replica_count) |i| {
        replicas[i] = try Replica.init(allocator, .{
            .tigerbeetle_executable = cli_args.tigerbeetle_executable,
            .ports = &.{ 3000, 3001, 3002 },
            .temp_dir = "/tmp/systest",
            .index = @intCast(i),
        });
        try replicas[i].start();
    }

    std.time.sleep(10 * std.time.ns_per_s);

    for (0..replica_count) |i| {
        try replicas[i].stop();
    }
}

fn start_client(
    allocator: std.mem.Allocator,
    options: struct { client_command: []const u8 },
) !void {
    const argv = std.ArrayList([]const u8).init();
    for (std.mem.split(u8, options.client_command, " ")) |arg| {
        argv.append(arg);
    }

    const cwd = try std.process.getCwdAlloc(allocator);
    defer allocator.free(cwd);

    var child = std.process.Child.init(argv, allocator);
    child.cwd = cwd;
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;
}

const State = enum { initial, running, stopped };

const Replica = struct {
    const Self = @This();
    const Options = struct {
        tigerbeetle_executable: []const u8,
        ports: []const u16,
        temp_dir: []const u8,
        index: u8,
    };

    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    options: Options,
    cwd: []const u8,
    argv: []const []const u8,

    child: ?std.process.Child = null,
    stderr_thread: ?std.Thread = null,
    state: State,

    fn init(
        allocator: std.mem.Allocator,
        options: Options,
    ) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        const cwd = try std.process.getCwdAlloc(arena.allocator());

        const argv: []const []const u8 = &.{
            options.tigerbeetle_executable,
            "start",
            try std.fmt.allocPrint(
                arena.allocator(),
                "--addresses={s}",
                .{try comma_separate_ports(arena.allocator(), options.ports)},
            ),
            try std.fmt.allocPrint(
                arena.allocator(),
                "{s}/0_{d}.tigerbeetle",
                .{ options.temp_dir, options.index },
            ),
        };

        const replica = try allocator.create(Self);
        replica.* = .{
            .allocator = allocator,
            .arena = arena,
            .options = options,
            .cwd = cwd,
            .argv = try arena.allocator().dupe([]const u8, argv),
            .state = .initial,
        };
        return replica;
    }

    fn deinit(self: *Self) void {
        self.arena.deinit();
        self.allocator.destroy(self);
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

        // std.debug.print(
        //     "replica {d}: {s}\n",
        //     .{ self.options.index, try format_argv(self.arena.allocator(), self.argv) },
        // );
        try child.spawn();

        self.stderr_thread = try std.Thread.spawn(
            .{},
            struct {
                fn log(stderr: std.fs.File, replica: *Self) void {
                    while (true) {
                        var buf: [1024]u8 = undefined;
                        const line_opt = stderr.reader().readUntilDelimiterOrEof(&buf, '\n') catch |err| {
                            std.debug.print("replica {d}: failed reading stderr: {any}\n", .{ replica.options.index, err });
                            break;
                        };
                        if (line_opt) |line| {
                            std.debug.print("replica {d}: {s}\n", .{ replica.options.index, line });
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

        std.debug.print("replica {d}: stopping\n", .{self.options.index});

        var child = self.child.?;
        const stderr_thread = self.stderr_thread.?;

        // Terminate the process
        _ = kill: {
            if (builtin.os.tag == .windows) {
                const exit_code = 1;
                break :kill std.os.windows.TerminateProcess(child.id, exit_code);
            } else {
                break :kill std.posix.kill(child.id, std.posix.SIG.TERM);
            }
        } catch |err| {
            std.debug.print(
                "replica {d}: failed to kill process: {any}\n",
                .{ self.options.index, err },
            );
        };

        // Stop the logging thread
        stderr_thread.join();

        // Await the terminated process
        _ = child.wait() catch unreachable;

        std.debug.print("replica {d}: stopped\n", .{self.options.index});

        self.child = null;
        self.stderr_thread = null;
        self.state = .stopped;
    }
};

fn format_argv(allocator: std.mem.Allocator, argv: []const []const u8) ![]const u8 {
    assert(argv.len > 0);

    var segments = std.ArrayList([]const u8).init(allocator);
    defer segments.deinit();

    try segments.append("$");
    for (argv) |arg| {
        try segments.append(" ");
        try segments.append(arg);
    }

    return try std.mem.concat(allocator, u8, segments.items);
}

fn comma_separate_ports(allocator: std.mem.Allocator, ports: []const u16) ![]const u8 {
    assert(ports.len > 0);

    var segments = std.ArrayList([]const u8).init(allocator);
    defer segments.deinit();

    try segments.append(try std.fmt.allocPrint(allocator, "{d}", .{ports[0]}));
    for (ports[1..]) |port| {
        try segments.append(",");
        try segments.append(try std.fmt.allocPrint(allocator, "{d}", .{port}));
    }

    return try std.mem.concat(allocator, u8, segments.items);
}

test "replica: starts and stops" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer assert(gpa.deinit() == .ok);

    var replica = try Replica.init(allocator, .{
        .tigerbeetle_executable = "./tigerbeetle",
        .ports = &.{3000},
        .temp_dir = "/tmp/systest-unit",
        .index = 0,
    });
    defer replica.deinit();

    // start & stop
    try replica.start();
    std.time.sleep(1 * std.time.ns_per_s);
    try replica.stop();

    // restart & stop
    try replica.start();
    std.time.sleep(1 * std.time.ns_per_s);
    try replica.stop();
}

test "format_argv: space-separates slice as a prompt" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const formatted = try format_argv(allocator, &.{ "foo", "bar", "baz" });

    try std.testing.expectEqualStrings("$ foo bar baz", formatted);
}

test "comma-separates ports" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const ports = try comma_separate_ports(allocator, &.{ 3000, 3001, 3002 });

    try std.testing.expectEqualStrings("3000,3001,3002", ports);
}
