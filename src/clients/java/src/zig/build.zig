const std = @import("std");
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;

pub fn build(b: *std.build.Builder) void {
    const mode = b.standardReleaseOptions();

    // Zig cross-target x java os-arch names
    const platforms = .{
        .{ "x86_64-linux-gnu", "linux-x86_64" },
        .{ "x86_64-macos", "macos-x86_64" },
        .{ "aarch64-linux-gnu", "linux-aarch_64" },
        .{ "aarch64-macos", "macos-aarch64" },
        .{ "x86_64-windows", "win-x86_64" },
    };

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;

        const lib = b.addSharedLibrary("tb_jniclient", "src/client.zig", .unversioned);
        lib.addPackagePath("jui", "lib/jui/src/jui.zig");
        lib.addPackagePath("tigerbeetle", "tb_client.zig");
        lib.setOutputDir("../tigerbeetle-java/src/main/resources/lib/" ++ platform[1]);
        lib.setTarget(cross_target);
        lib.setBuildMode(mode);

        if (cross_target.os_tag.? == .windows) {

            // The linker cannot resolve these dependencies from tb_client
            // So we have to insert them manually here, or we are going to receive a "lld-link: error: undefined symbol"
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        } else {
            lib.linkLibC();
        }

        lib.install();
    }
}
