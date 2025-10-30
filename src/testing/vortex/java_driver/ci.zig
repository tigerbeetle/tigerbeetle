const builtin = @import("builtin");
const std = @import("std");
const log = std.log;
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
        const vortex_bin = base_path ++ "zig-out/bin/vortex";
        const class_path_driver = base_path ++
            "src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar";
        const class_path = class_path_driver ++ ":" ++ base_path ++
            "src/testing/vortex/java_driver/target/vortex-driver-java-0.0.1-SNAPSHOT.jar";
        const driver_command = "java -cp " ++ class_path ++ " Main";
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
        log.warn("Not testing vortex java on OS {}", .{builtin.target.os.tag});
    }
}
