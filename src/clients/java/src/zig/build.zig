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
    } ++
        // This is a workarround
        // Cross compiling to windows is failing
        // tb_client.obj: not an ELF file error: LLDReportedFailure
        switch (builtin.target.os.tag) {
        .windows => .{
            .{ "x86_64-windows-gnu", "win-x86_64" },
        },
        else => .{},
    };

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;

        const tb_client = b.addStaticLibrary("tb_client", "lib/tigerbeetle/src/c/tb_client.zig");
        tb_client.setMainPkgPath("lib/tigerbeetle/src");
        tb_client.setTarget(cross_target);
        tb_client.setBuildMode(mode);

        const lib = b.addSharedLibrary("tb_jniclient", "src/client.zig", .unversioned);
        lib.addPackagePath("jui", "lib/jui/src/jui.zig");
        lib.setOutputDir("../tigerbeetle-java/src/main/resources/lib/" ++ platform[1]);
        lib.setTarget(cross_target);
        lib.setBuildMode(mode);

        if (cross_target.os_tag.? == .windows) {

            // This is another workarround
            // Compiling on Windows, the linker cannot resolve those dependencies from tb_client static library
            // So we have to insert them manually here, or we are going to receive a "lld-link: error: undefined symbol"
            if (builtin.target.os.tag == .windows) {
                lib.linkSystemLibrary("ws2_32");
                lib.linkSystemLibrary("advapi32");
            }
        } else {
            tb_client.linkLibC();
            lib.linkLibC();
        }

        lib.linkLibrary(tb_client);
        lib.install();
    }
}
