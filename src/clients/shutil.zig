const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

pub fn exec(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
) !std.ChildProcess.ExecResult {
    var res = try std.ChildProcess.exec(.{
        .allocator = arena.allocator(),
        .argv = cmd,
    });
    switch (res.term) {
        .Exited => |code| {
            if (code != 0) {
                return error.ExecCommandFailed;
            }
        },

        else => return error.UnknownCase,
    }

    return res;
}

pub fn git_root(arena: *std.heap.ArenaAllocator) ![]const u8 {
    const exec_result = try exec(arena, &.{ "git", "rev-parse", "--show-toplevel" });
    // As conventional, output includes a trailing `\n` which we need to strip away.
    assert(!std.mem.endsWith(u8, exec_result.stdout, "\r\n"));
    assert(std.mem.endsWith(u8, exec_result.stdout, "\n"));
    return exec_result.stdout[0 .. exec_result.stdout.len - 1];
}

pub fn file_or_directory_exists(f_or_d: []const u8) bool {
    var file = std.fs.cwd().openFile(f_or_d, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => return false,
            error.IsDir => return true,
            else => std.debug.panic(
                "unexpected error while checking file_or_directory_exists({s}): {s}",
                .{ f_or_d, @errorName(err) },
            ),
        }
    };
    file.close();

    return true;
}
