const std = @import("std");
const assert = std.debug.assert;
const tigerbeetle = @import("tigerbeetle");

const Args = struct {
    tigerbeetle_executable: []const u8,
    client_command: []const u8,
};

pub fn main() !void {
    // TODO(owickstrom): arena?
    const allocator = std.heap.page_allocator;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const cli_args = tigerbeetle.flags.parse(&args, Args);

    var replica = try Replica.init(allocator, .{
        .tigerbeetle_executable = cli_args.tigerbeetle_executable,
        .port = 3000,
        .datafile = "/tmp/systest.3000.tigerbeetle",
    });
    try replica.start();
    std.time.sleep(5 * std.time.ns_per_s);
    try replica.stop();
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
    const Options = struct { tigerbeetle_executable: []const u8, port: u16, datafile: []const u8 };

    allocator: std.mem.Allocator,
    options: Options,
    cwd: []const u8,
    addresses: []const u8,
    argv: []const []const u8,

    child: ?*std.process.Child = null,
    state: State,

    fn init(
        allocator: std.mem.Allocator,
        options: Options,
    ) !*Self {
        const cwd = try std.process.getCwdAlloc(allocator);

        const addresses = try std.fmt.allocPrint(allocator, "--addresses={d}", .{options.port});

        const argv: []const []const u8 = &.{
            options.tigerbeetle_executable,
            "start",
            addresses,
            options.datafile,
        };
        const argv_copy = try allocator.dupe([]const u8, argv);
        // errdefer allocator.free(argv_copy);

        var replica = try allocator.create(Self);
        replica.allocator = allocator;
        replica.options = options;
        replica.cwd = cwd;
        replica.addresses = addresses;
        replica.argv = argv_copy;
        replica.state = .initial;

        return replica;
    }

    fn deinit(self: *Self) void {
        self.allocator.free(self.cwd);
        self.allocator.free(self.addresses);
        self.allocator.free(self.argv);
        self.allocator.destroy(self);
    }

    fn start(
        self: *Self,
    ) !void {
        assert(self.state != .running);
        defer assert(self.state == .running);

        var child = std.process.Child.init(self.argv, self.allocator);

        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Inherit;
        child.stderr_behavior = .Inherit;

        try child.spawn();

        self.child = &child;
        self.state = .running;
    }

    fn stop(
        self: *Self,
    ) !void {
        assert(self.state == .running);
        defer assert(self.state == .stopped);

        if (self.child) |child| {
            _ = try child.kill();
        }
        self.child = null;
        self.state = .stopped;
    }
};

test "replica: starts and stops" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var replica = try Replica.init(allocator, .{
        .tigerbeetle_executable = "./tigerbeetle",
        .port = 3000,
        .datafile = "/tmp/systest.3000.tigerbeetle",
    });
    defer {
        replica.deinit();
        assert(gpa.deinit() == .ok);
    }

    try replica.start();
    std.time.sleep(3 * std.time.ns_per_s);
    try replica.stop();
}
