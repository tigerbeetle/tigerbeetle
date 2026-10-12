//! Like stdx, but for build helpers.
const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log;

pub fn exec_ok(arena: Allocator, io: std.Io, argv: []const []const u8) bool {
    assert(argv.len > 0);
    const result = std.process.run(arena, io, .{ .argv = argv }) catch return false;
    return result.term == .exited and result.term.exited == 0;
}

pub fn exec(arena: Allocator, io: std.Io, argv: []const []const u8) ![]const u8 {
    assert(argv.len > 0);
    const result = std.process.run(arena, io, .{ .argv = argv }) catch |err| {
        const argv_flat = std.mem.join(arena, " ", argv) catch "<oom>";
        log.err("running {s}: {}", .{ argv_flat, err });
        return err;
    };
    if (!(result.term == .exited and result.term.exited == 0)) {
        const argv_flat = std.mem.join(arena, " ", argv) catch "<oom>";
        log.err("running {s}: {}\n{s}", .{ argv_flat, result.term, result.stderr });
        return error.Exec;
    }
    if (std.mem.indexOfScalar(u8, result.stdout, '\n')) |first_newline| {
        if (first_newline + 1 == result.stdout.len) {
            return result.stdout[0 .. result.stdout.len - 1];
        }
    }
    return result.stdout;
}
