const std = @import("std");
const builtin = @import("builtin");

const config = @import("./src/config.zig");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const options = b.addOptions();

    // The "tigerbeetle version" command includes the build-time commit hash.
    if (git_commit(allocator)) |commit| {
        options.addOption(?[]const u8, "git_commit", commit[0..]);
    } else {
        options.addOption(?[]const u8, "git_commit", null);
    }

    options.addOption(
        config.ConfigBase,
        "config_base",
        b.option(config.ConfigBase, "config", "Base configuration.") orelse .default,
    );

    options.addOption(
        config.StateMachine,
        "config_cluster_state_machine",
        b.option(
            config.StateMachine,
            "config-cluster-state-machine",
            "State machine.",
        ) orelse .accounting,
    );

    const tracer_backend = b.option(
        config.TracerBackend,
        "tracer-backend",
        "Which backend to use for tracing.",
    ) orelse .none;
    options.addOption(config.TracerBackend, "tracer_backend", tracer_backend);

    {
        const tigerbeetle = b.addExecutable("tigerbeetle", "src/main.zig");
        tigerbeetle.setTarget(target);
        tigerbeetle.setBuildMode(mode);
        tigerbeetle.install();
        // Ensure that we get stack traces even in release builds.
        tigerbeetle.omit_frame_pointer = false;
        tigerbeetle.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(tigerbeetle, tracer_backend, target);

        const run_cmd = tigerbeetle.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run TigerBeetle");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const benchmark = b.addExecutable("benchmark", "src/benchmark.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(mode);
        benchmark.install();
        benchmark.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(benchmark, tracer_backend, target);

        const run_cmd = benchmark.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("benchmark", "Run TigerBeetle benchmark");
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

    // Executable which generates src/c/tb_client.h
    const tb_client_header_generate = blk: {
        const tb_client_header = b.addExecutable("tb_client_header", "src/c/tb_client_header.zig");
        tb_client_header.addOptions("tigerbeetle_build_options", options);
        tb_client_header.setMainPkgPath("src");
        break :blk tb_client_header.run();
    };

    {
        const unit_tests = b.addTest("src/unit_tests.zig");
        unit_tests.setTarget(target);
        unit_tests.setBuildMode(mode);
        unit_tests.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(unit_tests, tracer_backend, target);

        // for src/c/tb_client_header_test.zig to use cImport on tb_client.h
        unit_tests.linkLibC();
        unit_tests.addIncludeDir("src/c/");

        const test_step = b.step("test", "Run the unit tests");
        test_step.dependOn(&unit_tests.step);
        test_step.dependOn(&tb_client_header_generate.step);
    }

    {
        const tb_client = b.addStaticLibrary("tb_client", "src/c/tb_client.zig");
        tb_client.setMainPkgPath("src");
        tb_client.setTarget(target);
        tb_client.setBuildMode(mode);
        tb_client.addOptions("tigerbeetle_build_options", options);
        tb_client.setOutputDir("zig-out");
        tb_client.pie = true;
        tb_client.bundle_compiler_rt = true;

        tb_client.linkLibC();

        const build_step = b.step("tb_client", "Build C client shared library");
        build_step.dependOn(&tb_client.step);
        build_step.dependOn(&tb_client_header_generate.step);
    }

    {
        const simulator = b.addExecutable("simulator", "src/simulator.zig");
        simulator.setTarget(target);
        simulator.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(simulator, tracer_backend, target);

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
        vopr.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(vopr, tracer_backend, target);

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

    inline for (.{
        .{
            .name = "fuzz_ewah",
            .file = "src/ewah_fuzz.zig",
            .description = "Fuzz EWAH codec. Args: [--seed <seed>]",
        },
        .{
            .name = "fuzz_lsm_forest",
            .file = "src/lsm/forest_fuzz.zig",
            .description = "Fuzz the LSM forest. Args: [--seed <seed>] [--events-max <count>]",
        },
        .{
            .name = "fuzz_lsm_manifest_log",
            .file = "src/lsm/manifest_log_fuzz.zig",
            .description = "Fuzz the ManifestLog. Args: [--seed <seed>] [--events-max <count>]",
        },
        .{
            .name = "fuzz_lsm_tree",
            .file = "src/lsm/tree_fuzz.zig",
            .description = "Fuzz the LSM tree. Args: [--seed <seed>] [--events-max <count>]",
        },
        .{
            .name = "fuzz_lsm_segmented_array",
            .file = "src/lsm/segmented_array_fuzz.zig",
            .description = "Fuzz the LSM segmented array. Args: [--seed <seed>]",
        },
        .{
            .name = "fuzz_vsr_journal_format",
            .file = "src/vsr/journal_format_fuzz.zig",
            .description = "Fuzz the WAL format. Args: [--seed <seed>]",
        },
        .{
            .name = "fuzz_vsr_superblock",
            .file = "src/vsr/superblock_fuzz.zig",
            .description = "Fuzz the SuperBlock. Args: [--seed <seed>] [--events-max <count>]",
        },
        .{
            .name = "fuzz_vsr_superblock_free_set",
            .file = "src/vsr/superblock_free_set_fuzz.zig",
            .description = "Fuzz the SuperBlock FreeSet. Args: [--seed <seed>]",
        },
        .{
            .name = "fuzz_vsr_superblock_quorums",
            .file = "src/vsr/superblock_quorums_fuzz.zig",
            .description = "Fuzz the SuperBlock Quorums. Args: [--seed <seed>]",
        },
    }) |fuzzer| {
        const exe = b.addExecutable(fuzzer.name, fuzzer.file);
        exe.setMainPkgPath("src");
        exe.setTarget(target);
        exe.setBuildMode(mode);
        exe.omit_frame_pointer = false;
        exe.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(exe, tracer_backend, target);

        const run_cmd = exe.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step(fuzzer.name, fuzzer.description);
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
        exe.addOptions("tigerbeetle_build_options", options);
        link_tracer_backend(exe, tracer_backend, target);

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

fn link_tracer_backend(
    exe: *std.build.LibExeObjStep,
    tracer_backend: config.TracerBackend,
    target: std.zig.CrossTarget,
) void {
    switch (tracer_backend) {
        .none, .perfetto => {},
        .tracy => {
            // Code here is based on
            // https://github.com/ziglang/zig/blob/a660df4900520c505a0865707552dcc777f4b791/build.zig#L382

            // On mingw, we need to opt into windows 7+ to get some features required by tracy.
            const tracy_c_flags: []const []const u8 = if (target.isWindows() and target.getAbi() == .gnu)
                &[_][]const u8{
                    "-DTRACY_ENABLE=1",
                    "-DTRACY_FIBERS=1",
                    "-fno-sanitize=undefined",
                    "-D_WIN32_WINNT=0x601",
                }
            else
                &[_][]const u8{
                    "-DTRACY_ENABLE=1",
                    "-DTRACY_FIBERS=1",
                    "-fno-sanitize=undefined",
                };

            exe.addIncludeDir("./tools/tracy/public/tracy");
            exe.addCSourceFile("./tools/tracy/public/TracyClient.cpp", tracy_c_flags);
            exe.linkLibC();
            exe.linkSystemLibraryName("c++");

            if (target.isWindows()) {
                exe.linkSystemLibrary("dbghelp");
                exe.linkSystemLibrary("ws2_32");
            }
        },
    }
}
