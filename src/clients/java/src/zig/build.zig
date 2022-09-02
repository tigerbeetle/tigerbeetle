const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    const tb_client = b.addStaticLibrary("tb_client", "lib/tigerbeetle/src/c/tb_client.zig");
    tb_client.setMainPkgPath("lib/tigerbeetle/src");
    tb_client.setTarget(target);
    tb_client.setBuildMode(mode);

    const lib = b.addSharedLibrary("tb_jniclient", "src/client.zig", .unversioned);
    lib.addPackagePath("jui", "lib/jui/src/jui.zig");
    lib.linkLibrary(tb_client);

    const os_tag = target.os_tag orelse builtin.target.os.tag;
    if (os_tag == .windows) {

        // This is a workarround
        // The linker cannot resolve those dependencies from tb_client static library
        // So we have to insert them manually here, or we are going to receive a "lld-link: error: undefined symbol"
        lib.linkSystemLibrary("ws2_32");
        lib.linkSystemLibrary("advapi32");

    } else{
        tb_client.linkLibC();
        lib.linkLibC();
    }

    lib.setTarget(target);
    lib.setBuildMode(mode);
    lib.install();
}
