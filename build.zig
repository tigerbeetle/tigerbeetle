const std = @import("std");
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;
const Mode = std.builtin.Mode;

const config = @import("./src/config.zig");
const Shell = @import("./src/shell.zig");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    const options = b.addOptions();

    var shell = Shell.create(b.allocator) catch unreachable;
    defer shell.destroy();

    // The "tigerbeetle version" command includes the build-time commit hash.
    options.addOption(
        ?[]const u8,
        "git_commit",
        if (shell.git_commit()) |commit| @as([]const u8, &commit) else |_| null,
    );
    options.addOption(?[]const u8, "git_tag", shell.git_tag() catch null);

    options.addOption(
        config.ConfigBase,
        "config_base",
        b.option(config.ConfigBase, "config", "Base configuration.") orelse .default,
    );

    options.addOption(
        std.log.Level,
        "config_log_level",
        b.option(std.log.Level, "config-log-level", "Log level.") orelse .info,
    );

    const tracer_backend = b.option(
        config.TracerBackend,
        "tracer-backend",
        "Which backend to use for tracing.",
    ) orelse .none;
    options.addOption(config.TracerBackend, "tracer_backend", tracer_backend);
    const git_clone_tracy = GitCloneStep.add(b, .{
        .repo = "https://github.com/wolfpld/tracy.git",
        .tag = "v0.9.1", // unrelated to Zig 0.9.1
        .path = "tools/tracy",
    });

    const aof_record_enable = b.option(bool, "config-aof-record", "Enable AOF Recording.") orelse false;
    const aof_recovery_enable = b.option(bool, "config-aof-recovery", "Enable AOF Recovery mode.") orelse false;
    options.addOption(bool, "config_aof_record", aof_record_enable);
    options.addOption(bool, "config_aof_recovery", aof_recovery_enable);

    const hash_log_mode = b.option(
        config.HashLogMode,
        "hash-log-mode",
        "Log hashes (used for debugging non-deterministic executions).",
    ) orelse .none;
    options.addOption(config.HashLogMode, "hash_log_mode", hash_log_mode);

    const vsr_package = std.build.Pkg{
        .name = "vsr",
        .path = .{ .path = "src/vsr.zig" },
        .dependencies = &.{options.getPackage("vsr_options")},
    };

    const tigerbeetle = b.addExecutable("tigerbeetle", "src/tigerbeetle/main.zig");
    tigerbeetle.setTarget(target);
    tigerbeetle.setBuildMode(mode);
    tigerbeetle.addPackage(vsr_package);
    tigerbeetle.install();
    // Ensure that we get stack traces even in release builds.
    tigerbeetle.omit_frame_pointer = false;
    tigerbeetle.addOptions("vsr_options", options);
    link_tracer_backend(tigerbeetle, git_clone_tracy, tracer_backend, target);

    {
        const run_cmd = tigerbeetle.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run TigerBeetle");
        run_step.dependOn(&run_cmd.step);
    }

    {
        // "zig build install" moves the server executable to the root folder:
        const move_cmd = b.addInstallBinFile(
            tigerbeetle.getOutputSource(),
            b.pathJoin(&.{ "../../", tigerbeetle.out_filename }),
        );
        move_cmd.step.dependOn(&tigerbeetle.step);

        const install_step = b.getInstallStep();
        install_step.dependOn(&move_cmd.step);
    }

    {
        const benchmark = b.addExecutable("benchmark", "src/benchmark.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(mode);
        benchmark.addPackage(vsr_package);
        benchmark.addOptions("vsr_options", options);
        link_tracer_backend(benchmark, git_clone_tracy, tracer_backend, target);

        const run_cmd = benchmark.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("benchmark", "Run TigerBeetle benchmark");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const aof = b.addExecutable("aof", "src/aof.zig");
        aof.setTarget(target);
        aof.setBuildMode(mode);
        aof.addOptions("vsr_options", options);
        link_tracer_backend(aof, git_clone_tracy, tracer_backend, target);

        const run_cmd = aof.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("aof", "Run TigerBeetle AOF Utility");
        run_step.dependOn(&run_cmd.step);
    }

    // Linting targets
    // We currently have: lint_zig_fmt, lint_tigerstyle, lint_shellcheck, lint_validate_docs.
    // The meta-target lint runs them all
    {
        // lint_zig_fmt
        const lint_zig_fmt = b.addSystemCommand(&.{ b.zig_exe, "fmt", "--check", "." });
        const lint_zig_fmt_step = b.step("lint_zig_fmt", "Run zig fmt");
        lint_zig_fmt_step.dependOn(&lint_zig_fmt.step);

        // lint_tigerstyle
        const lint_tigerstyle = b.addExecutable("lint_tigerstyle", "scripts/lint_tigerstyle.zig");
        lint_tigerstyle.setTarget(target);
        lint_tigerstyle.setBuildMode(mode);

        const run_cmd = lint_tigerstyle.run();
        if (b.args) |args| {
            run_cmd.addArgs(args);
        } else {
            run_cmd.addArg("src");
        }

        const lint_tigerstyle_step = b.step("lint_tigerstyle", "Run the linter on src/");
        lint_tigerstyle_step.dependOn(&run_cmd.step);

        // lint_shellcheck
        const lint_shellcheck = ShellcheckStep.add(b);
        const lint_shellcheck_step = b.step("lint_shellcheck", "Run shellcheck on **.sh");
        lint_shellcheck_step.dependOn(&lint_shellcheck.step);

        // lint_validate_docs
        const lint_validate_docs = b.addSystemCommand(&.{"scripts/validate_docs.sh"});
        const lint_validate_docs_step = b.step("lint_validate_docs", "Validate docs");
        lint_validate_docs_step.dependOn(&lint_validate_docs.step);

        // TODO: Iterate above? Make it impossible to neglect to add somehow?
        // lint
        const lint_step = b.step("lint", "Run all defined linters");
        lint_step.dependOn(lint_tigerstyle_step);
        lint_step.dependOn(lint_zig_fmt_step);
        lint_step.dependOn(lint_shellcheck_step);
        lint_step.dependOn(lint_validate_docs_step);
    }

    // Executable which generates src/clients/c/tb_client.h
    const tb_client_header_generate = blk: {
        const tb_client_header = b.addExecutable("tb_client_header", "src/clients/c/tb_client_header.zig");
        tb_client_header.addOptions("vsr_options", options);
        tb_client_header.setMainPkgPath("src");
        tb_client_header.setTarget(target);
        break :blk tb_client_header.run();
    };

    {
        const test_filter = b.option(
            []const u8,
            "test-filter",
            "Skip tests that do not match filter",
        );

        const unit_tests = b.addTest("src/unit_tests.zig");
        unit_tests.setTarget(target);
        unit_tests.setBuildMode(mode);
        unit_tests.addOptions("vsr_options", options);
        unit_tests.step.dependOn(&tb_client_header_generate.step);
        unit_tests.setFilter(test_filter);
        link_tracer_backend(unit_tests, git_clone_tracy, tracer_backend, target);

        // for src/clients/c/tb_client_header_test.zig to use cImport on tb_client.h
        unit_tests.linkLibC();
        unit_tests.addIncludeDir("src/clients/c/");

        const unit_tests_step = b.step("test:unit", "Run the unit tests");
        unit_tests_step.dependOn(&unit_tests.step);

        const test_step = b.step("test", "Run the unit tests");
        test_step.dependOn(&unit_tests.step);
        if (test_filter == null) {
            // Test that our demos compile, but don't run them.
            inline for (.{
                "demo_01_create_accounts",
                "demo_02_lookup_accounts",
                "demo_03_create_transfers",
                "demo_04_create_pending_transfers",
                "demo_05_post_pending_transfers",
                "demo_06_void_pending_transfers",
                "demo_07_lookup_transfers",
            }) |demo| {
                const demo_exe = b.addExecutable(demo, "src/demos/" ++ demo ++ ".zig");
                demo_exe.addPackage(vsr_package);
                demo_exe.setTarget(target);
                test_step.dependOn(&demo_exe.step);
            }
        }

        const unit_tests_exe = b.addTestExe("tests", "src/unit_tests.zig");
        unit_tests_exe.setTarget(target);
        unit_tests_exe.setBuildMode(mode);
        unit_tests_exe.addOptions("vsr_options", options);
        unit_tests_exe.step.dependOn(&tb_client_header_generate.step);
        unit_tests_exe.setFilter(test_filter);
        link_tracer_backend(unit_tests_exe, git_clone_tracy, tracer_backend, target);

        // for src/clients/c/tb_client_header_test.zig to use cImport on tb_client.h
        unit_tests_exe.linkLibC();
        unit_tests_exe.addIncludeDir("src/clients/c/");

        const unit_tests_exe_step = b.step("test:build", "Build the unit tests");
        const install_unit_tests_exe = b.addInstallArtifact(unit_tests_exe);
        unit_tests_exe_step.dependOn(&install_unit_tests_exe.step);
    }

    // Clients build:
    {
        var install_step = b.addInstallArtifact(tigerbeetle);

        go_client(
            b,
            mode,
            &.{ &install_step.step, &tb_client_header_generate.step },
            target,
            options,
            git_clone_tracy,
            tracer_backend,
        );
        java_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
            git_clone_tracy,
            tracer_backend,
        );
        dotnet_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
            git_clone_tracy,
            tracer_backend,
        );
        node_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
            git_clone_tracy,
            tracer_backend,
        );
        c_client(
            b,
            mode,
            &.{ &install_step.step, &tb_client_header_generate.step },
            options,
            git_clone_tracy,
            tracer_backend,
        );
        c_client_sample(
            b,
            mode,
            target,
            &.{ &install_step.step, &tb_client_header_generate.step },
            options,
            git_clone_tracy,
            tracer_backend,
        );
        run_with_tb(
            b.allocator,
            b,
            mode,
            target,
        );
        client_integration(
            b.allocator,
            b,
            mode,
            target,
        );
        client_docs(
            b.allocator,
            b,
            mode,
            target,
        );
    }

    {
        const jni_tests_step = b.step("test:jni", "Run the JNI tests");
        const jni_tests = JniTestStep.add(b, mode, target);
        jni_tests_step.dependOn(&jni_tests.step);
    }

    {
        const simulator_options = b.addOptions();

        // When running without a SEED, default to release.
        const simulator_mode = if (b.args == null) .ReleaseSafe else mode;

        const StateMachine = enum { testing, accounting };
        simulator_options.addOption(
            StateMachine,
            "state_machine",
            b.option(
                StateMachine,
                "simulator-state-machine",
                "State machine.",
            ) orelse .accounting,
        );

        const SimulatorLog = enum { full, short };
        const default_simulator_log =
            if (simulator_mode == .ReleaseSafe) SimulatorLog.short else .full;
        simulator_options.addOption(
            SimulatorLog,
            "log",
            b.option(
                SimulatorLog,
                "simulator-log",
                "Log only state transitions (short) or everything (full).",
            ) orelse default_simulator_log,
        );

        const simulator = b.addExecutable("simulator", "src/simulator.zig");
        simulator.setTarget(target);
        simulator.setBuildMode(simulator_mode);
        // Ensure that we get stack traces even in release builds.
        simulator.omit_frame_pointer = false;
        simulator.addOptions("vsr_options", options);
        simulator.addOptions("vsr_simulator_options", simulator_options);
        link_tracer_backend(simulator, git_clone_tracy, tracer_backend, target);

        const run_cmd = simulator.run();

        if (b.args) |args| run_cmd.addArgs(args);

        const install_step = b.addInstallArtifact(simulator);
        const build_step = b.step("simulator", "Build the Simulator");
        build_step.dependOn(&install_step.step);

        const run_step = b.step("simulator_run", "Run the Simulator");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const vopr = b.addExecutable("vopr", "src/vopr.zig");
        vopr.setTarget(target);
        // Ensure that we get stack traces even in release builds.
        vopr.omit_frame_pointer = false;
        vopr.addOptions("vsr_options", options);
        link_tracer_backend(vopr, git_clone_tracy, tracer_backend, target);

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
            .name = "fuzz_lsm_manifest_level",
            .file = "src/lsm/manifest_level_fuzz.zig",
            .description = "Fuzz the ManifestLevel. Args: [--seed <seed>] [--events-max <count>]",
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
        exe.addOptions("vsr_options", options);
        link_tracer_backend(exe, git_clone_tracy, tracer_backend, target);
        const install_step = b.addInstallArtifact(exe);
        const build_step = b.step("build_" ++ fuzzer.name, fuzzer.description);
        build_step.dependOn(&install_step.step);

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
        exe.addOptions("vsr_options", options);
        link_tracer_backend(exe, git_clone_tracy, tracer_backend, target);

        const build_step = b.step(
            "build_" ++ benchmark.name,
            "Build " ++ benchmark.description ++ " benchmark",
        );
        build_step.dependOn(&exe.step);

        const run_cmd = exe.run();
        const step = b.step(benchmark.name, "Benchmark " ++ benchmark.description);
        step.dependOn(&run_cmd.step);
    }
}

fn link_tracer_backend(
    exe: *std.build.LibExeObjStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
    target: std.zig.CrossTarget,
) void {
    switch (tracer_backend) {
        .none => {},
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
            exe.step.dependOn(&git_clone_tracy.step);
        },
    }
}

// Zig cross-targets plus Dotnet RID (Runtime Identifier):
const platforms = .{
    .{ "x86_64-linux-gnu", "linux-x64" },
    .{ "x86_64-linux-musl", "linux-musl-x64" },
    .{ "x86_64-macos", "osx-x64" },
    .{ "aarch64-linux-gnu", "linux-arm64" },
    .{ "aarch64-linux-musl", "linux-musl-arm64" },
    .{ "aarch64-macos", "osx-arm64" },
    .{ "x86_64-windows", "win-x64" },
};

fn go_client(
    b: *std.build.Builder,
    mode: Mode,
    dependencies: []const *std.build.Step,
    target: CrossTarget,
    options: *std.build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("go_client", "Build Go client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    // Updates the generated header file:
    const install_header = b.addInstallFile(
        .{ .path = "src/clients/c/tb_client.h" },
        "../src/clients/go/pkg/native/tb_client.h",
    );

    const bindings = b.addExecutable("go_bindings", "src/clients/go/go_bindings.zig");
    bindings.addOptions("vsr_options", options);
    bindings.setTarget(target);
    bindings.setMainPkgPath("src");
    const bindings_step = bindings.run();

    inline for (platforms) |platform| {
        const name = if (comptime std.mem.eql(u8, platform[0], "x86_64-linux-musl"))
            "x86_64-linux"
        else if (comptime std.mem.eql(u8, platform[0], "aarch64-linux-musl"))
            "aarch64-linux"
        else
            platform[0];

        // We don't need the linux-gnu builds.
        if (comptime !std.mem.endsWith(u8, platform[0], "linux-gnu")) {
            const cross_target = CrossTarget.parse(.{ .arch_os_abi = name, .cpu_features = "baseline" }) catch unreachable;
            var b_isolated = builder_with_isolated_cache(b, cross_target);

            const lib = b_isolated.addStaticLibrary("tb_client", "src/clients/c/tb_client.zig");
            lib.setMainPkgPath("src");
            lib.setTarget(cross_target);
            lib.setBuildMode(mode);
            lib.linkLibC();
            lib.pie = true;
            lib.bundle_compiler_rt = true;

            lib.setOutputDir("src/clients/go/pkg/native/" ++ name);

            lib.addOptions("vsr_options", options);
            link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

            lib.step.dependOn(&install_header.step);
            lib.step.dependOn(&bindings_step.step);
            build_step.dependOn(&lib.step);
        }
    }
}

fn java_client(
    b: *std.build.Builder,
    mode: Mode,
    dependencies: []const *std.build.Step,
    target: CrossTarget,
    options: *std.build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("java_client", "Build Java client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable("java_bindings", "src/clients/java/java_bindings.zig");
    bindings.addOptions("vsr_options", options);
    bindings.setTarget(target);
    bindings.setMainPkgPath("src");
    const bindings_step = bindings.run();

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const lib = b_isolated.addSharedLibrary("tb_jniclient", "src/clients/java/src/client.zig", .unversioned);
        lib.setMainPkgPath("src");
        lib.setOutputDir("src/clients/java/src/main/resources/lib/" ++ platform[0]);
        lib.setTarget(cross_target);
        lib.setBuildMode(mode);
        lib.linkLibC();

        if (cross_target.os_tag.? == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

        lib.step.dependOn(&bindings_step.step);
        build_step.dependOn(&lib.step);
    }
}

fn dotnet_client(
    b: *std.build.Builder,
    mode: Mode,
    dependencies: []const *std.build.Step,
    target: CrossTarget,
    options: *std.build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("dotnet_client", "Build dotnet client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable("dotnet_bindings", "src/clients/dotnet/dotnet_bindings.zig");
    bindings.addOptions("vsr_options", options);
    bindings.setTarget(target);
    bindings.setMainPkgPath("src");
    const bindings_step = bindings.run();

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const lib = b_isolated.addSharedLibrary("tb_client", "src/clients/c/tb_client.zig", .unversioned);
        lib.setMainPkgPath("src");
        lib.setOutputDir("src/clients/dotnet/TigerBeetle/runtimes/" ++ platform[1] ++ "/native");
        lib.setTarget(cross_target);
        lib.setBuildMode(mode);
        lib.linkLibC();

        if (cross_target.os_tag.? == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

        lib.step.dependOn(&bindings_step.step);
        build_step.dependOn(&lib.step);
    }
}

fn node_client(
    b: *std.build.Builder,
    mode: Mode,
    dependencies: []const *std.build.Step,
    target: CrossTarget,
    options: *std.build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("node_client", "Build Node client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable("node_bindings", "src/clients/node/node_bindings.zig");
    bindings.addOptions("vsr_options", options);
    bindings.setTarget(target);
    bindings.setMainPkgPath("src");
    const bindings_step = bindings.run();

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        if (cross_target.os_tag.? == .windows) {
            // No Windows support just yet. We need to be on a version with https://github.com/ziglang/zig/commit/b97a68c529b5db15705f4d542d8ead616d27c880
        } else {
            const lib = b_isolated.addSharedLibrary("tb_nodeclient", "src/clients/node/src/node.zig", .unversioned);
            lib.setMainPkgPath("src");
            lib.setOutputDir("src/clients/node/dist/bin/" ++ platform[0]);

            // This is provided by the node-api-headers package; make sure to run `npm install` under `src/clients/node`
            // if you're running zig build node_client manually.
            lib.addSystemIncludeDir("src/clients/node/node_modules/node-api-headers/include");
            lib.setTarget(cross_target);
            lib.setBuildMode(mode);
            lib.linkLibC();
            lib.linker_allow_shlib_undefined = true;

            if (cross_target.os_tag.? == .windows) {
                lib.linkSystemLibrary("ws2_32");
                lib.linkSystemLibrary("advapi32");
            }

            lib.addOptions("vsr_options", options);
            link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

            lib.step.dependOn(&bindings_step.step);
            build_step.dependOn(&lib.step);
        }
    }
}

fn c_client(
    b: *std.build.Builder,
    mode: Mode,
    dependencies: []const *std.build.Step,
    options: *std.build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("c_client", "Build C client library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    // Updates the generated header file:
    const install_header = b.addInstallFile(
        .{ .path = "src/clients/c/tb_client.h" },
        "../src/clients/c/lib/include/tb_client.h",
    );

    build_step.dependOn(&install_header.step);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const shared_lib = b_isolated.addSharedLibrary("tb_client", "src/clients/c/tb_client.zig", .unversioned);
        const static_lib = b_isolated.addStaticLibrary("tb_client", "src/clients/c/tb_client.zig");
        static_lib.bundle_compiler_rt = true;
        static_lib.pie = true;

        for ([_]*std.build.LibExeObjStep{ shared_lib, static_lib }) |lib| {
            lib.setMainPkgPath("src");
            lib.setOutputDir("src/clients/c/lib/" ++ platform[0]);
            lib.setTarget(cross_target);
            lib.setBuildMode(mode);
            lib.linkLibC();

            if (cross_target.os_tag.? == .windows) {
                lib.linkSystemLibrary("ws2_32");
                lib.linkSystemLibrary("advapi32");
            }

            lib.addOptions("vsr_options", options);
            link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

            build_step.dependOn(&lib.step);
        }
    }
}

fn c_client_sample(
    b: *std.build.Builder,
    mode: Mode,
    target: CrossTarget,
    dependencies: []const *std.build.Step,
    options: *std.build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const c_sample_build = b.step("c_sample", "Build the C client sample");
    for (dependencies) |dependency| {
        c_sample_build.dependOn(dependency);
    }

    const static_lib = b.addStaticLibrary("tb_client", "src/clients/c/tb_client.zig");
    static_lib.setMainPkgPath("src");
    static_lib.linkage = .static;
    static_lib.linkLibC();
    static_lib.setBuildMode(mode);
    static_lib.setTarget(target);
    static_lib.pie = true;
    static_lib.bundle_compiler_rt = true;
    static_lib.addOptions("vsr_options", options);
    link_tracer_backend(static_lib, git_clone_tracy, tracer_backend, target);
    c_sample_build.dependOn(&static_lib.step);

    const sample = b.addExecutable("c_sample", "src/clients/c/samples/main.c");
    sample.setBuildMode(mode);
    sample.setTarget(target);
    sample.linkLibrary(static_lib);
    sample.linkLibC();

    if (target.isWindows()) {
        static_lib.linkSystemLibrary("ws2_32");
        static_lib.linkSystemLibrary("advapi32");

        // TODO: Illegal instruction on Windows:
        sample.disable_sanitize_c = true;
    }

    const install_step = b.addInstallArtifact(sample);
    c_sample_build.dependOn(&install_step.step);
}

// Allows a build step to run the command it builds after it builds it if the user passes --.
// e.g.: ./scripts/build.sh docs_generate --
// Whereas `./scripts/build.sh docs_generate` would not run the command.
fn maybe_execute(
    b: *std.build.Builder,
    allocator: std.mem.Allocator,
    step: *std.build.Step,
    binary_name: []const u8,
) void {
    var to_run = std.ArrayList([]const u8).init(allocator);
    const sep = if (builtin.os.tag == .windows) "\\" else "/";
    const ext = if (builtin.os.tag == .windows) ".exe" else "";
    to_run.append(
        std.fmt.allocPrint(
            allocator,
            ".{s}zig-out{s}bin{s}{s}{s}",
            .{
                sep,
                sep,
                sep,
                binary_name,
                ext,
            },
        ) catch unreachable,
    ) catch unreachable;

    var args = std.process.args();
    var build_and_run = false;
    while (args.next(allocator)) |arg_or_err| {
        const arg = arg_or_err catch unreachable;

        if (std.mem.eql(u8, arg, "--")) {
            build_and_run = true;
            continue;
        }

        if (build_and_run) {
            to_run.append(arg) catch unreachable;
        }
    }

    if (build_and_run) {
        const run = b.addSystemCommand(to_run.items);
        step.dependOn(&run.step);
    }
}

// See src/clients/README.md for documentation.
fn run_with_tb(
    allocator: std.mem.Allocator,
    b: *std.build.Builder,
    mode: Mode,
    target: CrossTarget,
) void {
    const run_with_tb_build = b.step("run_with_tb", "Build the run_with_tb helper");
    const binary = b.addExecutable("run_with_tb", "src/clients/run_with_tb.zig");
    binary.setBuildMode(mode);
    binary.setTarget(target);
    run_with_tb_build.dependOn(&binary.step);

    const install_step = b.addInstallArtifact(binary);
    run_with_tb_build.dependOn(&install_step.step);

    maybe_execute(b, allocator, run_with_tb_build, "run_with_tb");
}

// See src/clients/README.md for documentation.
fn client_integration(
    allocator: std.mem.Allocator,
    b: *std.build.Builder,
    mode: Mode,
    target: CrossTarget,
) void {
    const client_integration_build = b.step("client_integration", "Run sample integration tests for a client library");
    const binary = b.addExecutable("client_integration", "src/clients/integration.zig");
    binary.setBuildMode(mode);
    binary.setTarget(target);
    client_integration_build.dependOn(&binary.step);

    const install_step = b.addInstallArtifact(binary);
    client_integration_build.dependOn(&install_step.step);

    maybe_execute(b, allocator, client_integration_build, "client_integration");
}

// See src/clients/README.md for documentation.
fn client_docs(
    allocator: std.mem.Allocator,
    b: *std.build.Builder,
    mode: Mode,
    target: CrossTarget,
) void {
    const client_docs_build = b.step("client_docs", "Run sample integration tests for a client library");
    const binary = b.addExecutable("client_docs", "src/clients/docs_generate.zig");
    binary.setBuildMode(mode);
    binary.setTarget(target);
    client_docs_build.dependOn(&binary.step);

    const install_step = b.addInstallArtifact(binary);
    client_docs_build.dependOn(&install_step.step);

    maybe_execute(b, allocator, client_docs_build, "client_docs");
}

/// Detects the system's JVM.
/// JNI tests requires the Java SDK installed and
/// JAVA_HOME environment variable set.
const JniTestStep = struct {
    step: std.build.Step,
    tests: *std.build.LibExeObjStep,

    fn add(b: *std.build.Builder, mode: Mode, target: CrossTarget) *std.build.LibExeObjStep {
        const tests = b.addTest("src/clients/java/src/jni_tests.zig");
        tests.linkSystemLibrary("jvm");
        tests.linkLibC();
        tests.setTarget(target);
        if (builtin.os.tag == .windows) {
            // TODO(zig): The function `JNI_CreateJavaVM` tries to detect
            // the stack size and causes a SEGV that is handled by Zig's panic handler.
            // https://bugzilla.redhat.com/show_bug.cgi?id=1572811#c7
            //
            // The workaround is run the tests in "ReleaseFast" mode.
            tests.setBuildMode(.ReleaseFast);
        } else {
            tests.setBuildMode(mode);
        }

        var jvm_check_step = b.allocator.create(JniTestStep) catch unreachable;
        jvm_check_step.* = .{
            .step = std.build.Step.init(.custom, "jni test", b.allocator, JniTestStep.make),
            .tests = tests,
        };

        tests.step.dependOn(&jvm_check_step.step);
        return tests;
    }

    fn make(step: *std.build.Step) anyerror!void {
        const self = @fieldParentPtr(JniTestStep, "step", step);
        const builder: *std.build.Builder = self.tests.builder;

        const java_home = builder.env_map.get("JAVA_HOME") orelse {
            std.log.err(
                "JNI tests require the Java SDK installed and JAVA_HOME environment variable set.",
                .{},
            );
            return error.JavaHomeNotSet;
        };

        const libjvm_path = builder.pathJoin(&.{
            java_home,
            if (builtin.os.tag == .windows) "/lib" else "/lib/server",
        });
        self.tests.addLibPath(libjvm_path);

        switch (builtin.os.tag) {
            .windows => set_windows_dll(builder.allocator, java_home),
            .macos => try builder.env_map.put("DYLD_LIBRARY_PATH", libjvm_path),
            .linux => {
                // On Linux, detects the abi by calling `ldd` to check if
                // the libjvm.so is linked against libc or musl.
                // It's reasonable to assume that ldd will be present.
                const ldd_result = try builder.exec(&.{
                    "ldd",
                    builder.pathJoin(&.{ libjvm_path, "libjvm.so" }),
                });
                self.tests.target.abi = if (std.mem.indexOf(u8, ldd_result, "musl") != null)
                    .musl
                else if (std.mem.indexOf(u8, ldd_result, "libc") != null)
                    .gnu
                else {
                    std.log.err("{s}", .{ldd_result});
                    return error.JavaAbiUnrecognized;
                };
            },
            else => unreachable,
        }
    }

    /// Set the JVM DLL directory on Windows.
    fn set_windows_dll(allocator: std.mem.Allocator, java_home: []const u8) void {
        comptime std.debug.assert(builtin.os.tag == .windows);
        const set_dll_directory = struct {
            pub extern "kernel32" fn SetDllDirectoryA(path: [*:0]const u8) callconv(.C) std.os.windows.BOOL;
        }.SetDllDirectoryA;

        var java_bin_path = std.fs.path.joinZ(
            allocator,
            &.{ java_home, "\\bin" },
        ) catch unreachable;
        _ = set_dll_directory(java_bin_path);

        var java_bin_server_path = std.fs.path.joinZ(
            allocator,
            &.{ java_home, "\\bin\\server" },
        ) catch unreachable;
        _ = set_dll_directory(java_bin_server_path);
    }
};

const ShellcheckStep = struct {
    step: std.build.Step,
    gpa: std.mem.Allocator,

    fn add(b: *std.build.Builder) *ShellcheckStep {
        var result = b.allocator.create(ShellcheckStep) catch unreachable;
        result.* = .{
            .step = std.build.Step.init(.custom, "run shellcheck", b.allocator, ShellcheckStep.make),
            .gpa = b.allocator,
        };
        return result;
    }

    fn make(step: *std.build.Step) anyerror!void {
        const self = @fieldParentPtr(ShellcheckStep, "step", step);

        var shell = try Shell.create(self.gpa);
        defer shell.destroy();

        if (!try shell.exec_status_ok("shellcheck --version", .{})) {
            shell.echo(
                "{ansi-red}Please install shellcheck - https://www.shellcheck.net/{ansi-reset}",
                .{},
            );
            return error.NoShellcheck;
        }

        const scripts = try shell.find(.{
            .where = &.{ "src", "scripts", ".github" },
            .ends_with = ".sh",
        });

        try shell.exec("shellcheck {scripts}", .{
            .scripts = scripts,
        });
    }
};

/// Every large project contains its own bespoke implementation of `git submodule`, this is ours.
/// We use `GitCloneStep` to lazily download build-time dependencies when we need them.
const GitCloneStep = struct {
    step: std.build.Step,
    gpa: std.mem.Allocator,
    options: Options,

    const Options = struct {
        repo: []const u8,
        tag: []const u8,
        path: []const u8,
    };

    fn add(b: *std.build.Builder, options: Options) *GitCloneStep {
        var result = b.allocator.create(GitCloneStep) catch unreachable;
        result.* = .{
            .step = std.build.Step.init(.custom, "run git clone", b.allocator, GitCloneStep.make),
            .gpa = b.allocator,
            .options = options,
        };
        return result;
    }

    fn make(step: *std.build.Step) anyerror!void {
        const self = @fieldParentPtr(GitCloneStep, "step", step);

        var shell = try Shell.create(self.gpa);
        defer shell.destroy();

        if (try shell.dir_exists(self.options.path)) return;
        try shell.exec("git clone --branch {tag} {repo} {path}", self.options);
    }
};

/// Creates a new Builder, with isolated cache for each platform.
/// Hit some issues with the build cache between cross compilations:
/// - From Linux, it runs fine.
/// - From Windows it fails on libc "invalid object".
/// - From MacOS, similar to https://github.com/ziglang/zig/issues/9711#issuecomment-1090071087.
/// Workaround: Just setting different cache folders for each platform.
fn builder_with_isolated_cache(
    b: *std.build.Builder,
    target: CrossTarget,
) *std.build.Builder {
    // This workaround isn't necessary when cross-compiling from Linux.
    if (builtin.os.tag == .linux) return b;

    // If not cross-compiling, we can return the current *Builder in order
    // to reuse the same cache from other artifacts.
    if (target.cpu_arch.? == builtin.cpu.arch and
        target.os_tag.? == builtin.os.tag)
        return b;

    // Generating isolated cache and global_cache dirs for each cpu/os:
    const cache_root = b.pathJoin(&.{
        b.cache_root,
        @tagName(target.cpu_arch.?),
        @tagName(target.os_tag.?),
    });
    const global_cache_root = b.pathJoin(&.{
        b.global_cache_root,
        @tagName(target.cpu_arch.?),
        @tagName(target.os_tag.?),
    });

    // Note, this builder leaks memory, since there is no way to deinit it.
    return std.build.Builder.create(
        b.allocator,
        b.zig_exe,
        b.build_root,
        cache_root,
        global_cache_root,
    ) catch unreachable;
}
