const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    {
        const tigerbeetle = b.addExecutable("tigerbeetle", "src/main.zig");
        tigerbeetle.setTarget(target);
        tigerbeetle.setBuildMode(mode);
        tigerbeetle.install();

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
        const benchmark = b.addExecutable("eytzinger_benchmark", "src/eytzinger_benchmark.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(.ReleaseSafe);
        const run_cmd = benchmark.run();

        const step = b.step("eytzinger_benchmark", "Benchmark array search");
        step.dependOn(&run_cmd.step);
    }

    {
        const benchmark = b.addExecutable("benchmark_ewah", "src/ewah_benchmark.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(.ReleaseSafe);
        const run_cmd = benchmark.run();

        const step = b.step("benchmark_ewah", "Benchmark EWAH codec");
        step.dependOn(&run_cmd.step);
    }

    {
        const benchmark = b.addExecutable(
            "benchmark_segmented_array",
            "src/lsm/segmented_array_benchmark.zig",
        );
        benchmark.setTarget(target);
        benchmark.setBuildMode(.ReleaseSafe);
        benchmark.setMainPkgPath("src/");
        const run_cmd = benchmark.run();

        const step = b.step("benchmark_segmented_array", "Benchmark SegmentedArray search");
        step.dependOn(&run_cmd.step);
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
        const lsm_forest_fuzz = b.addExecutable("lsm_forest_fuzz", "src/lsm/forest_fuzz.zig");
        lsm_forest_fuzz.setMainPkgPath("src");
        lsm_forest_fuzz.setTarget(target);
        lsm_forest_fuzz.setBuildMode(mode);
        // Ensure that we get stack traces even in release builds.
        lsm_forest_fuzz.omit_frame_pointer = false;

        const run_cmd = lsm_forest_fuzz.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("lsm_forest_fuzz", "Fuzz the LSM forest. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const lsm_tree_fuzz = b.addExecutable("lsm_tree_fuzz", "src/lsm/tree_fuzz.zig");
        lsm_tree_fuzz.setMainPkgPath("src");
        lsm_tree_fuzz.setTarget(target);
        lsm_tree_fuzz.setBuildMode(mode);
        // Ensure that we get stack traces even in release builds.
        lsm_tree_fuzz.omit_frame_pointer = false;

        const run_cmd = lsm_tree_fuzz.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("lsm_tree_fuzz", "Fuzz the LSM tree. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const lsm_segmented_array_fuzz = b.addExecutable("lsm_segmented_array_fuzz", "src/lsm/segmented_array_fuzz.zig");
        lsm_segmented_array_fuzz.setMainPkgPath("src");
        lsm_segmented_array_fuzz.setTarget(target);
        lsm_segmented_array_fuzz.setBuildMode(mode);
        // Ensure that we get stack traces even in release builds.
        lsm_segmented_array_fuzz.omit_frame_pointer = false;

        const run_cmd = lsm_segmented_array_fuzz.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("lsm_segmented_array_fuzz", "Fuzz the LSM segmented array. Args: [--seed <seed>]");
        run_step.dependOn(&run_cmd.step);
    }
}
