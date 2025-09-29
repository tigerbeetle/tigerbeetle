const std = @import("std");

const Shell = @import("../../shell.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    _ = gpa;
    try shell.exec("cargo test --all", .{});
    try shell.exec("cargo fmt --check", .{});
    try shell.exec("cargo clippy -- -D clippy::all", .{});
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    _ = shell;
    _ = gpa;
    _ = options;
    // todo
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    _ = shell; // autofix
    return "unimplemented";
}
