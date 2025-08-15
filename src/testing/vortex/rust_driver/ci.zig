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
        const tigerbeetle_bin = base_path ++ "zig-out/bin/tigerbeetle";
        const vortex_bin = base_path ++ "zig-out/bin/vortex";
        const driver_command = base_path ++
            "src/testing/vortex/rust_driver/target/debug/vortex-driver-rust";
        const vortex_out_dir = try shell.create_tmp_dir();
        defer shell.cwd.deleteTree(vortex_out_dir) catch {};
        try shell.exec(
            vortex_bin ++ " " ++
                "run --driver-command={driver_command} " ++
                "--tigerbeetle-executable={tigerbeetle_bin} " ++
                "--output-directory={vortex_out_dir} " ++
                "--disable-faults " ++
                "--test-duration-seconds=10",
            .{
                .driver_command = driver_command,
                .tigerbeetle_bin = tigerbeetle_bin,
                .vortex_out_dir = vortex_out_dir,
            },
        );
    } else {
        log.warn("Not testing vortex java on OS {}", .{builtin.target.os.tag});
    }
}
