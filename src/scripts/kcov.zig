//! Runs a subset of TigerBeetle tests with kcov. While we don't rigorously track coverage
//! information, this is useful to run as a one-off sanity check.
//!
//! # Usage
//!
//! This script is run by CI for each merge to the main branch. That CI jobs uploads the results
//! as GitHub Actions artifact. To view the results, open the CI run in the browser, find the link
//! to the artifact, download `code-coverage-report.zig` file, unpack it, and open the `index.html`
//! in the browser.
const std = @import("std");

const Shell = @import("../shell.zig");

const log = std.log;

pub const CliArgs = struct {};

pub fn main(shell: *Shell, _: std.mem.Allocator, _: CliArgs) !void {
    const kcov_version = shell.exec_stdout("kcov --version", .{}) catch {
        log.err("can't find kcov", .{});
        std.process.exit(1);
    };
    log.info("kcov version {s}", .{kcov_version});

    try shell.zig("build test:unit:build", .{});
    try shell.zig("build vopr:build", .{});
    try shell.zig("build fuzz:build", .{});

    try shell.project_root.deleteTree("./zig-out/kcov");
    try shell.project_root.makePath("./zig-out/kcov");

    const kcov: []const []const u8 = &.{ "kcov", "--include-path=./src", "./zig-out/kcov" };
    try shell.exec("{kcov} ./zig-out/bin/test", .{ .kcov = kcov });
    try shell.exec("{kcov} ./zig-out/bin/fuzz lsm_tree 92", .{ .kcov = kcov });
    try shell.exec("{kcov} ./zig-out/bin/fuzz lsm_forest 92", .{ .kcov = kcov });
    try shell.exec("{kcov} ./zig-out/bin/vopr 92", .{ .kcov = kcov });
}
