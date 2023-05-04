//
// This script spins up a TigerBeetle server on the first available
// TCP port and references a brand new data file. Then it runs the
// command passed to it and shuts down the server. It cleans up all
// resources unless told not to. It fails if the command passed to it
// fails. It could have been a bash script except for that it works on
// Windows as well.
//
// Example: (run from the repo root)
//   ./scripts/build.sh run_with_tb -- node myscript.js
//

const std = @import("std");
const builtin = @import("builtin");

const run = @import("./shutil.zig").run;
const run_with_env = @import("./shutil.zig").run_with_env;
const TmpDir = @import("./shutil.zig").TmpDir;
const git_root = @import("./shutil.zig").git_root;
const path_exists = @import("./shutil.zig").path_exists;
const script_filename = @import("./shutil.zig").script_filename;
const binary_filename = @import("./shutil.zig").binary_filename;
const file_or_directory_exists = @import("./shutil.zig").file_or_directory_exists;

fn free_port() !u16 {
    var port: u16 = 1025;
    while (true) {
        // Address.resolveIp doesn't end up working on Windows. It's
        // ok because this is always going to be an IPv4 address anyway.
        const self_addr = try std.net.Address.parseIp4("127.0.0.1", port);
        var listener = std.net.StreamServer.init(.{});
        defer listener.close();
        listener.listen(self_addr) catch {
            port += 1;
            continue;
        };

        return port;
    }
}

pub fn run_with_tb(arena: *std.heap.ArenaAllocator, commands: []const []const u8, cwd: []const u8) !void {
    const root = try git_root(arena);

    std.debug.print("Moved to git root: {s}\n", .{root});
    try std.os.chdir(root);
    var tb_binary = try binary_filename(arena, &[_][]const u8{"tigerbeetle"});

    // Build TigerBeetle
    if (!file_or_directory_exists(arena, tb_binary)) {
        std.debug.print("Building TigerBeetle server\n", .{});
        try run(arena, &[_][]const u8{
            try script_filename(arena, &[_][]const u8{ "scripts", "install" }),
        });
    }

    std.debug.assert(file_or_directory_exists(arena, tb_binary));

    var tmpdir = try TmpDir.init(arena);
    defer tmpdir.deinit();
    const wrk_dir = tmpdir.path;

    const data_file = try std.fmt.allocPrint(
        arena.allocator(),
        "{s}/0_0.tigerbeetle",
        .{wrk_dir},
    );

    std.debug.print("Formatting data file: {s}\n", .{data_file});
    _ = try run(arena, &[_][]const u8{
        tb_binary,
        "format",
        "--cluster=0",
        "--replica=0",
        "--replica-count=1",
        data_file,
    });

    const port = free_port();

    const start_args = &[_][]const u8{
        tb_binary,
        "start",
        try std.fmt.allocPrint(arena.allocator(), "--addresses={}", .{port}),
        data_file,
    };
    std.debug.print("Starting TigerBeetle server: {s}\n", .{start_args});
    const cp = try std.ChildProcess.init(
        start_args,
        arena.allocator(),
    );
    defer {
        _ = cp.kill() catch {};
        cp.deinit();
    }
    try cp.spawn();

    std.debug.print("Running commands: {s}\n", .{commands});

    try std.os.chdir(cwd);

    try run_with_env(arena, commands, &[_][]const u8{
        "TB_ADDRESS",
        try std.fmt.allocPrint(arena.allocator(), "{}", .{port}),
    });
}

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const cwd = std.process.getEnvVarOwned(allocator, "R_CWD") catch ".";

    var collected_args = std.ArrayList([]const u8).init(allocator);
    var args = std.process.args();
    // Skip first arg, this process's name
    _ = args.next(allocator);
    while (args.next(allocator)) |arg_or_err| {
        const arg = arg_or_err catch {
            std.debug.print("Could not parse all arguments.\n", .{});
            return error.CouldNotParseArguments;
        };

        try collected_args.append(arg);
    }

    try run_with_tb(&arena, collected_args.items, cwd);
}

// Returning errors in main produces useless traces, at least for some
// known errors. But using errors allows defers to run. So wrap the
// main but don't pass the error back to main. Just exit(1) on
// failure.
pub fn main() !void {
    if (error_main()) {
        // fine
    } else |err| switch (err) {
        error.RunCommandFailed => std.os.exit(1),
        else => return err,
    }
}
