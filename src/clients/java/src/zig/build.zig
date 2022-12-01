const std = @import("std");
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;
const Pkg = std.build.Pkg;

pub fn build(b: *std.build.Builder) void {
    const mode = b.standardReleaseOptions();

    // Zig cross-targets
    const platforms = .{
        "x86_64-linux-gnu",
        "x86_64-linux-musl",
        "x86_64-macos",
        "aarch64-linux-gnu",
        "aarch64-linux-musl",
        "aarch64-macos",
        "x86_64-windows",
    };

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform, .cpu_features = "baseline" }) catch unreachable;

        const lib = b.addSharedLibrary("tb_jniclient", "src/client.zig", .unversioned);

        // TODO(batiati)
        // This is a hack used to work around the absence of tigerbeetle_build_options.
        // This should be removed once the client is built using the root `build.zig`.
        {
            const ConfigBase = enum { production, development, test_min, default };
            const TracerBackend = enum { none, perfetto, tracy };
            const dummy_options = b.addOptions();
            dummy_options.addOption(ConfigBase, "config_base", .default);
            dummy_options.addOption(TracerBackend, "tracer_backend", .none);
            var tigerbeetle_build_options = dummy_options.getPackage("tigerbeetle_build_options");

            lib.addPackage(Pkg{
                .name = "tigerbeetle",
                .path = .{ .path = "tb_client.zig" },
                .dependencies = &[_]Pkg{tigerbeetle_build_options},
            });
        }

        lib.addPackagePath("jui", "lib/jui/src/jui.zig");
        lib.setOutputDir("../tigerbeetle-java/src/main/resources/lib/" ++ platform);
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

        // Hit some issue with the build cache between cross compilations:
        // - From Linux, it runs fine
        // - From Windows it fails on libc "invalid object"
        // - From MacOS, similar to https://github.com/ziglang/zig/issues/9711
        // Workarround: Just setting a different cache folder for each platform and an isolated global cache.
        b.cache_root = "zig-cache/" ++ platform;
        b.global_cache_root = "zig-cache/global-" ++ platform;

        lib.install();
    }
}
