//! Deployment script for our systest (src/testing/systest).

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

pub const CLIArgs = struct {
    tag: []const u8,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    // if (builtin.os.tag == .windows) {
    //     return error.NotSupported;
    // }
    //
    _ = gpa;

    assert(try shell.exec_status_ok("docker --version", .{}));

    // Docker tag to build and push
    const tag = std.mem.trim(u8, cli_args.tag, &std.ascii.whitespace);
    assert(tag.len > 0);

    try shell.zig("build -Drelease", .{});

    // Build Java client library
    {
        try shell.pushd("./src/clients/java");
        defer shell.popd();

        try shell.exec("mvn clean install --batch-mode --quiet -Dmaven.test.skip", .{});
    }

    // Build workload
    {
        try shell.pushd("./src/testing/systest/workload");
        defer shell.popd();

        try shell.exec("mvn clean package --batch-mode --quiet", .{});
    }

    try shell.exec(
        \\docker build --file=./src/testing/systest/configuration.Dockerfile
        \\  --build-arg TAG={tag}
        \\ --tag=config:{tag}
        \\ .
    , .{ .tag = tag });

    try shell.exec(
        \\docker build --file=./src/testing/systest/replica.Dockerfile
        \\ --tag=config:{tag}
        \\ .
    , .{ .tag = tag });

    try shell.exec(
        \\docker build --file=./src/testing/systest/workload.Dockerfile
        \\ --tag=config:{tag}
        \\ .
    , .{ .tag = tag });
}
