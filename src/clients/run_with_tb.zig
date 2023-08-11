//! This script spins up a TigerBeetle server on an available
//! TCP port and references a brand new data file. Then it runs the
//! command passed to it and shuts down the server. It cleans up all
//! resources unless told not to. It fails if the command passed to it
//! fails. It could have been a bash script except for that it works on
//! Windows as well.
//!
//! Example: (run from the repo root)
//!   ./zig/zig build run_with_tb -- node myscript.js
//!

const std = @import("std");
const builtin = @import("builtin");
const os = std.os;

const run = @import("./shutil.zig").run;
const run_with_env = @import("./shutil.zig").run_with_env;
const TmpDir = @import("./shutil.zig").TmpDir;
const git_root = @import("./shutil.zig").git_root;
const path_exists = @import("./shutil.zig").path_exists;
const script_filename = @import("./shutil.zig").script_filename;
const binary_filename = @import("./shutil.zig").binary_filename;
const file_or_directory_exists = @import("./shutil.zig").file_or_directory_exists;
const TmpTigerBeetle = @import("../testing/tmp_tigerbeetle.zig");

pub fn run_with_tb(arena: *std.heap.ArenaAllocator, commands: []const []const u8, cwd: []const u8) !void {
    var tb = try TmpTigerBeetle.init(arena.allocator());
    defer tb.deinit();

    std.debug.print("Running commands: {s}\n", .{commands});

    try std.os.chdir(cwd);
    try run_with_env(arena, commands, &[_][]const u8{ "TB_ADDRESS", tb.port_str.slice() });
}

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const cwd = std.process.getEnvVarOwned(allocator, "R_CWD") catch ".";

    var collected_args = std.ArrayList([]const u8).init(allocator);
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip first arg, this process's name
    std.debug.assert(args.skip());
    while (args.next()) |arg| {
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
