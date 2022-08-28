const std = @import("std");
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;

pub fn build(b: *std.build.Builder) void {

    const mode = b.standardReleaseOptions();
    const tb_client = b.step("tb_client", "Build tb_client shared libs");

    // Zig cross-target x folder names
    const platforms = .{
        .{ "x86_64-linux-gnu", "linux-x64" },
        .{ "x86_64-windows-gnu", "win-x64" },
        .{ "x86_64-macos", "osx-x64" },
    };

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;

        const lib = b.addSharedLibrary("tb_client", "tigerbeetle/src/c/tb_client.zig", .unversioned);
        lib.setMainPkgPath("tigerbeetle/src");
        lib.setOutputDir(b.pathJoin(&.{ "src/TigerBeetle/native/", platform[1] }));
        lib.setTarget(cross_target);
        lib.setBuildMode(mode);
        lib.bundle_compiler_rt = true;           
        
        const os_tag = cross_target.os_tag orelse builtin.target.os.tag;
        if (os_tag != .windows) {
            lib.linkLibC();
        }           

        tb_client.dependOn(&lib.step);
    }
}
