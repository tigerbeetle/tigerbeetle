const std = @import("std");
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;

pub fn build(b: *std.build.Builder) void {

    const mode = b.standardReleaseOptions();
    const tb_client_dyn = b.step("tb_client_dyn", "Build tb_client dynamic libs");

    // Zig cross-target x folder names
    const platforms = .{
        .{ "x86_64-linux-gnu", "linux-x64" },
        .{ "x86_64-windows-gnu", "win-x64" },
        .{ "x86_64-macos", "osx-x64" },
    };

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;

        const dynamic_lib = b.addStaticLibrary("tb_client", "tigerbeetle/src/c/tb_client.zig");
        dynamic_lib.setMainPkgPath("tigerbeetle/src");
        dynamic_lib.setTarget(cross_target);
        dynamic_lib.setBuildMode(mode);
        dynamic_lib.setOutputDir(b.pathJoin(&.{ "src/TigerBeetle/native/", platform[1] }));
        dynamic_lib.bundle_compiler_rt = true;
        dynamic_lib.linkage = .dynamic;            
        
        const os_tag = cross_target.os_tag orelse builtin.target.os.tag;
        if (os_tag != .windows) {
            dynamic_lib.linkLibC();
        }           

        tb_client_dyn.dependOn(&dynamic_lib.step);
    }
}
