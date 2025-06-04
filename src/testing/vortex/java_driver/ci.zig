const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

const Shell = @import("../../../shell.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    _ = gpa;

    assert(shell.file_exists("pom.xml"));

    // NB: This expects the TB java driver to have been installed with `mvn install`.
    try shell.exec("mvn --batch-mode --file pom.xml --quiet package", .{});

    // NB: This expects the vortex bin to be available.
    if (builtin.target.os.tag == .linux) {
        const base_path = "../../../../";
        const tigerbeetle_bin = base_path ++ "zig-out/bin/tigerbeetle";
        const vortex_bin = base_path ++ "zig-out/bin/vortex";
        const class_path_driver = base_path ++
            "src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar";
        const class_path = class_path_driver ++ ":" ++ base_path ++
            "src/testing/vortex/java_driver/target/vortex-driver-java-0.0.1-SNAPSHOT.jar";
        const driver_command = "java -cp " ++ class_path ++ " Main";
        try shell.exec("rm -rf ./vortex-out", .{});
        try shell.exec("mkdir ./vortex-out", .{});
        defer shell.exec("rm -rf ./vortex-out", .{}) catch {
            @panic("unable to remove vortex work dir");
        };
        try shell.exec(
            vortex_bin ++ " " ++
                "run --driver-command={driver_command} " ++
                "--tigerbeetle-executable={tigerbeetle_bin} " ++
                "--output-directory=./vortex-out " ++
                "--disable-faults " ++
                "--test-duration-seconds=10",
            .{
                .driver_command = driver_command,
                .tigerbeetle_bin = tigerbeetle_bin,
            },
        );
    }
}
