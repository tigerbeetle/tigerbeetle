const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    {
        const tigerbeetle = b.addExecutable("tigerbeetle", "src/main.zig");
        tigerbeetle.setTarget(target);
        tigerbeetle.setBuildMode(mode);
        tigerbeetle.install();

        // The "tigerbeetle version" command includes the build-time commit hash.
        const options = b.addOptions();
        if (git_commit(allocator)) |commit| {
            options.addOption(?[]const u8, "git_commit", commit[0..]);
        } else {
            options.addOption(?[]const u8, "git_commit", null);
        }
        tigerbeetle.addOptions("tigerbeetle_build_options", options);

        const run_cmd = tigerbeetle.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run TigerBeetle");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const lint = b.addExecutable("lint", "scripts/lint.zig");
        lint.setTarget(target);
        lint.setBuildMode(mode);

        const run_cmd = lint.run();
        if (b.args) |args| {
            run_cmd.addArgs(args);
        } else {
            run_cmd.addArg("src");
        }

        const lint_step = b.step("lint", "Run the linter on src/");
        lint_step.dependOn(&run_cmd.step);
    }

    {
        const unit_tests = b.addTest("src/unit_tests.zig");
        unit_tests.setTarget(target);
        unit_tests.setBuildMode(mode);

        // for src/c/tb_client_header_test.zig to use cImport on tb_client.h
        unit_tests.linkLibC();
        unit_tests.addIncludeDir("src/c/");

        const test_step = b.step("test", "Run the unit tests");
        test_step.dependOn(&unit_tests.step);
    }

    {
        const tb_client = b.addStaticLibrary("tb_client", "src/c/tb_client.zig");
        tb_client.setMainPkgPath("src");
        tb_client.setTarget(target);
        tb_client.setBuildMode(mode);
        tb_client.setOutputDir("zig-out");
        tb_client.pie = true;
        tb_client.bundle_compiler_rt = true;

        const os_tag = target.os_tag orelse builtin.target.os.tag;
        if (os_tag != .windows) {
            tb_client.linkLibC();
        }

        const build_step = b.step("tb_client", "Build C client shared library");
        build_step.dependOn(&tb_client.step);
    }

    {
        const simulator = b.addExecutable("simulator", "src/simulator.zig");
        simulator.setTarget(target);
        // Ensure that we get stack traces even in release builds.
        simulator.omit_frame_pointer = false;

        const run_cmd = simulator.run();

        if (b.args) |args| {
            run_cmd.addArgs(args);
            simulator.setBuildMode(mode);
        } else {
            simulator.setBuildMode(.ReleaseSafe);
        }

        const step = b.step("simulator", "Run the Simulator");
        step.dependOn(&run_cmd.step);
    }

    {
        const vopr = b.addExecutable("vopr", "src/vopr.zig");
        vopr.setTarget(target);
        // Ensure that we get stack traces even in release builds.
        vopr.omit_frame_pointer = false;

        const run_cmd = vopr.run();

        if (b.args) |args| {
            run_cmd.addArgs(args);
            vopr.setBuildMode(mode);
        } else {
            vopr.setBuildMode(.ReleaseSafe);
        }

        const step = b.step("vopr", "Run the VOPR");
        step.dependOn(&run_cmd.step);
    }

    {
        const fuzz_lsm_forest = b.addExecutable("fuzz_lsm_forest", "src/lsm/forest_fuzz.zig");
        fuzz_lsm_forest.setMainPkgPath("src");
        fuzz_lsm_forest.setTarget(target);
        fuzz_lsm_forest.setBuildMode(mode);
        // Ensure that we get stack traces even in release builds.
        fuzz_lsm_forest.omit_frame_pointer = false;

        const run_cmd = fuzz_lsm_forest.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("fuzz_lsm_forest", "Fuzz the LSM forest. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fuzz_lsm_manifest_log = b.addExecutable(
            "fuzz_lsm_manifest_log",
            "src/lsm/manifest_log_fuzz.zig",
        );
        fuzz_lsm_manifest_log.setMainPkgPath("src");
        fuzz_lsm_manifest_log.setTarget(target);
        fuzz_lsm_manifest_log.setBuildMode(mode);
        fuzz_lsm_manifest_log.omit_frame_pointer = false;

        const run_cmd = fuzz_lsm_manifest_log.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("fuzz_lsm_manifest_log", "Fuzz the ManifestLog. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fuzz_lsm_tree = b.addExecutable("fuzz_lsm_tree", "src/lsm/tree_fuzz.zig");
        fuzz_lsm_tree.setMainPkgPath("src");
        fuzz_lsm_tree.setTarget(target);
        fuzz_lsm_tree.setBuildMode(mode);
        // Ensure that we get stack traces even in release builds.
        fuzz_lsm_tree.omit_frame_pointer = false;

        const run_cmd = fuzz_lsm_tree.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("fuzz_lsm_tree", "Fuzz the LSM tree. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fuzz_lsm_segmented_array = b.addExecutable(
            "fuzz_lsm_segmented_array",
            "src/lsm/segmented_array_fuzz.zig",
        );
        fuzz_lsm_segmented_array.setMainPkgPath("src");
        fuzz_lsm_segmented_array.setTarget(target);
        fuzz_lsm_segmented_array.setBuildMode(mode);
        // Ensure that we get stack traces even in release builds.
        fuzz_lsm_segmented_array.omit_frame_pointer = false;

        const run_cmd = fuzz_lsm_segmented_array.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("fuzz_lsm_segmented_array", "Fuzz the LSM segmented array. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fuzz_vsr_superblock = b.addExecutable(
            "fuzz_vsr_superblock",
            "src/vsr/superblock_fuzz.zig",
        );
        fuzz_vsr_superblock.setMainPkgPath("src");
        fuzz_vsr_superblock.setTarget(target);
        fuzz_vsr_superblock.setBuildMode(mode);
        fuzz_vsr_superblock.omit_frame_pointer = false;

        const run_cmd = fuzz_vsr_superblock.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("fuzz_vsr_superblock", "Fuzz the SuperBlock. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fuzz_vsr_superblock_quorums = b.addExecutable(
            "fuzz_vsr_superblock_quorums",
            "src/vsr/superblock_quorums_fuzz.zig",
        );
        fuzz_vsr_superblock_quorums.setMainPkgPath("src");
        fuzz_vsr_superblock_quorums.setTarget(target);
        fuzz_vsr_superblock_quorums.setBuildMode(mode);
        fuzz_vsr_superblock_quorums.omit_frame_pointer = false;

        const run_cmd = fuzz_vsr_superblock_quorums.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step(
            "fuzz_vsr_superblock_quorums",
            "Fuzz the SuperBlock Quorums. Args: [--seed <seed>]",
        );
        run_step.dependOn(&run_cmd.step);
    }

    inline for (.{
        .{
            .name = "benchmark_ewah",
            .file = "src/ewah_benchmark.zig",
            .description = "EWAH codec",
        },
        .{
            .name = "benchmark_eytzinger",
            .file = "src/lsm/eytzinger_benchmark.zig",
            .description = "array search",
        },
        .{
            .name = "benchmark_segmented_array",
            .file = "src/lsm/segmented_array_benchmark.zig",
            .description = "SegmentedArray search",
        },
    }) |benchmark| {
        const exe = b.addExecutable(benchmark.name, benchmark.file);
        exe.setTarget(target);
        exe.setBuildMode(.ReleaseSafe);
        exe.setMainPkgPath("src");

        const build_step = b.step("build_" ++ benchmark.name, "Build " ++ benchmark.description ++ " benchmark");
        build_step.dependOn(&exe.step);

        const run_cmd = exe.run();
        const step = b.step(benchmark.name, "Benchmark " ++ benchmark.description);
        step.dependOn(&run_cmd.step);
    }
}

fn git_commit(allocator: std.mem.Allocator) ?[40]u8 {
    const exec_result = std.ChildProcess.exec(.{
        .allocator = allocator,
        .argv = &.{ "git", "rev-parse", "--verify", "HEAD" },
    }) catch return null;
    defer allocator.free(exec_result.stdout);
    defer allocator.free(exec_result.stderr);

    // +1 for trailing newline.
    if (exec_result.stdout.len != 40 + 1) return null;
    if (exec_result.stderr.len != 0) return null;

    var output: [40]u8 = undefined;
    std.mem.copy(u8, &output, exec_result.stdout[0..40]);
    return output;
}
