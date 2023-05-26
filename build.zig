const std = @import("std");
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;
const Mode = std.builtin.Mode;

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
        ?[]const u8,
        "git_tag",
        if (git_tag(allocator)) |tag| tag.constSlice() else null,
    );

    options.addOption(
        config.ConfigBase,
        "config_base",
        b.option(config.ConfigBase, "config", "Base configuration.") orelse .default,
    );

    const tracer_backend = b.option(
        config.TracerBackend,
        "tracer-backend",
        "Which backend to use for tracing.",
    ) orelse .none;
    options.addOption(config.TracerBackend, "tracer_backend", tracer_backend);

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
    link_tracer_backend(tigerbeetle, tracer_backend, target);

    {
        const run_cmd = tigerbeetle.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run TigerBeetle");
        run_step.dependOn(&run_cmd.step);
    }

    {
        // "zig build install" moves the server executable to the root folder:
        const move_cmd = b.addInstallBinFile(tigerbeetle.getOutputSource(), b.pathJoin(&.{ "../../", tigerbeetle.out_filename }));
        move_cmd.step.dependOn(&tigerbeetle.step);

        var install_step = b.getInstallStep();
        install_step.dependOn(&move_cmd.step);
    }

    {
        const benchmark = b.addExecutable("benchmark", "src/benchmark.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(mode);
        benchmark.addPackage(vsr_package);
        benchmark.install();
        benchmark.addOptions("vsr_options", options);
        link_tracer_backend(benchmark, tracer_backend, target);

        const run_cmd = benchmark.run();
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("benchmark", "Run TigerBeetle benchmark");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const aof = b.addExecutable("aof", "src/aof.zig");
        aof.setTarget(target);
        aof.setBuildMode(mode);
        aof.install();
        aof.addOptions("vsr_options", options);
        link_tracer_backend(aof, tracer_backend, target);

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
        // TODO: Better way of running zig fmt?
        const lint_zig_fmt = b.addSystemCommand(&.{ b.zig_exe, "fmt", "--check", "src/" });
        const lint_zig_fmt_step = b.step("lint_zig_fmt", "Run zig fmt on src/");
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
        const lint_shellcheck = b.addSystemCommand(&.{ "sh", "-c", "command -v shellcheck >/dev/null" ++
            " || (echo -e '\\033[0;31mPlease install shellcheck - https://www.shellcheck.net/\\033[0m' && exit 1)" ++
            " && shellcheck $(find ./src ./scripts -type f -name '*.sh')" });
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
        link_tracer_backend(unit_tests, tracer_backend, target);

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
            tracer_backend,
        );
        java_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
            tracer_backend,
        );
        dotnet_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
            tracer_backend,
        );
        node_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
            tracer_backend,
        );
        c_client(
            b,
            mode,
            &.{ &install_step.step, &tb_client_header_generate.step },
            options,
            tracer_backend,
        );
        c_client_sample(
            b,
            mode,
            target,
            &.{ &install_step.step, &tb_client_header_generate.step },
            options,
            tracer_backend,
        );
        run_with_tb(
            allocator,
            b,
            mode,
            target,
        );
        client_integration(
            allocator,
            b,
            mode,
            target,
        );
        client_docs(
            allocator,
            b,
            mode,
            target,
        );
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
        const default_simulator_log = if (simulator_mode == .ReleaseSafe) SimulatorLog.short else .full;
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
        link_tracer_backend(simulator, tracer_backend, target);

        const run_cmd = simulator.run();

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

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
        link_tracer_backend(exe, tracer_backend, target);
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
        .{
            .name = "benchmark_table_mutable",
            .file = "src/lsm/table_mutable_benchmark.zig",
            .description = "TableMutable accesses",
        },
    }) |benchmark| {
        const exe = b.addExecutable(benchmark.name, benchmark.file);
        exe.setTarget(target);
        exe.setBuildMode(.ReleaseSafe);
        exe.setMainPkgPath("src");
        exe.addOptions("vsr_options", options);
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

fn git_tag(allocator: std.mem.Allocator) ?std.BoundedArray(u8, 100) {
    const exec_result = std.ChildProcess.exec(.{
        .allocator = allocator,
        .argv = &.{ "git", "describe", "--tags" },
    }) catch return null;
    defer allocator.free(exec_result.stdout);
    defer allocator.free(exec_result.stderr);

    if (exec_result.stderr.len != 0) return null;

    var tag = std.BoundedArray(u8, 100){ .buffer = undefined };
    tag.appendSlice(exec_result.stdout) catch {
        std.debug.print("`git describe --tags` output too long.\n", .{});
        return null;
    };
    return tag;
}

fn link_tracer_backend(
    exe: *std.build.LibExeObjStep,
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

            const lib = b.addStaticLibrary("tb_client", "src/clients/c/tb_client.zig");
            lib.setMainPkgPath("src");
            lib.setTarget(cross_target);
            lib.setBuildMode(mode);
            lib.linkLibC();
            lib.pie = true;
            lib.bundle_compiler_rt = true;

            lib.setOutputDir("src/clients/go/pkg/native/" ++ name);

            lib.addOptions("vsr_options", options);
            link_tracer_backend(lib, tracer_backend, cross_target);

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

        const lib = b.addSharedLibrary("tb_jniclient", "src/clients/java/src/client.zig", .unversioned);
        lib.setMainPkgPath("src");
        lib.addPackagePath("jui", "src/clients/java/lib/jui/src/jui.zig");
        lib.setOutputDir("src/clients/java/src/main/resources/lib/" ++ platform[0]);
        lib.setTarget(cross_target);
        lib.setBuildMode(mode);
        lib.linkLibC();

        if (cross_target.os_tag.? == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, tracer_backend, cross_target);

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

        const lib = b.addSharedLibrary("tb_client", "src/clients/c/tb_client.zig", .unversioned);
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
        link_tracer_backend(lib, tracer_backend, cross_target);

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

        if (cross_target.os_tag.? == .windows) {
            // No Windows support just yet. We need to be on a version with https://github.com/ziglang/zig/commit/b97a68c529b5db15705f4d542d8ead616d27c880
        } else {
            const lib = b.addSharedLibrary("tb_nodeclient", "src/clients/node/src/node.zig", .unversioned);
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
            link_tracer_backend(lib, tracer_backend, cross_target);

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

        const shared_lib = b.addSharedLibrary("tb_client", "src/clients/c/tb_client.zig", .unversioned);
        const static_lib = b.addStaticLibrary("tb_client", "src/clients/c/tb_client.zig");

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
            link_tracer_backend(lib, tracer_backend, cross_target);

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
    link_tracer_backend(static_lib, tracer_backend, target);
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
        }

        if (!build_and_run) continue;

        to_run.append(arg) catch unreachable;
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
