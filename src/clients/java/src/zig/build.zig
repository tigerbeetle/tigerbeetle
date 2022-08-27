const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    const lib = b.addSharedLibrary("tb_jniclient", "src/client.zig", .unversioned);

    lib.addPackagePath("jui", "libs/jui/src/jui.zig");
    lib.setBuildMode(mode);
    lib.install();
}
