//
// This script wraps additional commands around a TigerBeetle server
// running against formatted data in a random temporary directory.
//
// Ex:
//   zig build-exe run_with_tb.zig
//   ./run_with_tb bash -c "npm install tigerbeetle-node && node myscript.js"
//

const std = @import("std");
const builtin = @import("builtin");

const run = @import("./docs_generate.zig").run;
const TmpDir = @import("./docs_generate.zig").TmpDir;
const git_root = @import("./docs_generate.zig").git_root;
const path_exists = @import("./docs_generate.zig").path_exists;

pub fn file_or_directory_exists(arena: *std.heap.ArenaAllocator, f_or_d: []const u8) bool {
    _ = std.fs.cwd().realpathAlloc(arena.allocator(), f_or_d) catch {
        return false;
    };

    return true;
}

// Makes sure a local script name has the platform-appropriate folder
// character and extension. This is particularly important in Windows
// when you are calling a program. arg0 must be a well-formed path
// otherwise you have to pass it through powershell -c '...' and that's a
// waste.
pub fn script_filename(arena: *std.heap.ArenaAllocator, parts: []const []const u8) ![]const u8 {
    var file_name = std.ArrayList(u8).init(arena.allocator());
    const sep = if (builtin.os.tag == .windows) '\\' else '/';
    _ = try file_name.append('.');
    for (parts) |part| {
        _ = try file_name.append(sep);
        _ = try file_name.appendSlice(part);
    }

    _ = try file_name.appendSlice(if (builtin.os.tag == .windows) ".bat" else ".sh");
    return file_name.items;
}

pub fn binary_filename(arena: *std.heap.ArenaAllocator, parts: []const []const u8) ![]const u8 {
    var file_name = std.ArrayList(u8).init(arena.allocator());
    const sep = if (builtin.os.tag == .windows) '\\' else '/';
    _ = try file_name.append('.');
    for (parts) |part| {
        _ = try file_name.append(sep);
        _ = try file_name.appendSlice(part);
    }

    _ = try file_name.appendSlice(if (builtin.os.tag == .windows) ".exe" else "");
    return file_name.items;
}

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
    const cmdCp = try std.ChildProcess.init(
        commands,
        arena.allocator(),
    );
    var env = try std.process.getEnvMap(arena.allocator());
    try env.put(
        "TB_PORT",
        try std.fmt.allocPrint(arena.allocator(), "{}", .{port}),
    );
    cmdCp.env_map = &env;
    cmdCp.cwd = cwd;

    var res = try cmdCp.spawnAndWait();
    switch (res) {
        .Exited => |code| {
            if (code != 0) {
                return error.ProxiedCommandFailed;
            }
        },

        else => return error.UnknownCase,
    }
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
        error.ProxiedCommandFailed => std.os.exit(1),
        else => return err,
    }
}
