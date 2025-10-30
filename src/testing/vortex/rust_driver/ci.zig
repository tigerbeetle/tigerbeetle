const builtin = @import("builtin");
const std = @import("std");
const log = std.log;

const Shell = @import("../../../shell.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    _ = gpa;

    try shell.exec("cargo build", .{});
    try shell.exec("cargo fmt --check", .{});
    try shell.exec("cargo clippy -- -D clippy::all", .{});

    // NB: This expects the vortex bin to be available.
    if (builtin.target.os.tag == .linux) {
        const base_path = "../../../../";
        const vortex_bin = base_path ++ "zig-out/bin/vortex";
        const driver_command = base_path ++
            "src/testing/vortex/rust_driver/target/debug/vortex-driver-rust";
        try shell.exec(
            "{vortex_bin} " ++
                "supervisor --driver-command={driver_command} " ++
                "--replica-count=1 " ++
                "--disable-faults " ++
                "--test-duration=1s",
            .{
                .vortex_bin = vortex_bin,
                .driver_command = driver_command,
            },
        );
    } else {
        log.warn("Not testing vortex rust driver on OS {}", .{builtin.target.os.tag});
    }
}
