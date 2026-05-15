//! Like stdx, but for build helpers.
const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log;

pub fn exec_ok(arena: Allocator, argv: []const []const u8) bool {
    assert(argv.len > 0);
    const result = std.process.Child.run(.{ .allocator = arena, .argv = argv }) catch return false;
    return result.term == .Exited and result.term.Exited == 0;
}

pub fn exec(arena: Allocator, argv: []const []const u8) ![]const u8 {
    assert(argv.len > 0);
    const result = std.process.Child.run(.{ .allocator = arena, .argv = argv }) catch |err| {
        log.err("running {s}: {}", .{ argv, err });
        return err;
    };
    if (!(result.term == .Exited and result.term.Exited == 0)) {
        log.err("running {s}: {}\n{s}", .{ argv, result.term, result.stderr });
        return error.Exec;
    }
    if (std.mem.indexOfScalar(u8, result.stdout, '\n')) |first_newline| {
        if (first_newline + 1 == result.stdout.len) {
            return result.stdout[0 .. result.stdout.len - 1];
        }
    }
    return result.stdout;
}
