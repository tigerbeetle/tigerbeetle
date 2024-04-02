const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;
const Mode = std.builtin.Mode;

const config = @import("./src/config.zig");
const Shell = @import("./src/shell.zig");

// TigerBeetle binary requires certain CPU feature and supports a closed set of CPUs. Here, we
// specify exactly which features the binary needs. Client shared libraries might be more lax with
// CPU features required.
const supported_targets: []const CrossTarget = supported_targets: {
    @setEvalBranchQuota(100_000);
    var result: []const CrossTarget = &.{};
    const triples = .{
        "aarch64-linux",
        "aarch64-macos",
        "x86_64-linux",
        "x86_64-macos",
        "x86_64-windows",
    };
    const cpus = .{
        "baseline+aes+neon",
        "baseline+aes+neon",
        "x86_64_v3+aes",
        "x86_64_v3+aes",
        "x86_64_v3+aes",
    };
    for (triples, cpus) |triple, cpu| {
        result = result ++ .{CrossTarget.parse(.{
            .arch_os_abi = triple,
            .cpu_features = cpu,
        }) catch unreachable};
    }
    break :supported_targets result;
};

pub fn build(b: *std.Build) !void {
    // A compile error stack trace of 10 is arbitrary in size but helps with debugging.
    b.reference_trace = 10;

    var target = b.standardTargetOptions(.{});
    // Patch the target to use the right CPU. This is a somewhat hacky way to do this, but the core
    // idea here is to keep this file as the source of truth for what we need from the CPU.
    for (supported_targets) |supported_target| {
        if (target.getCpuArch() == supported_target.getCpuArch()) {
            target.cpu_model = supported_target.cpu_model;
            target.cpu_features_add = supported_target.cpu_features_add;
            target.cpu_features_sub = supported_target.cpu_features_sub;
            break;
        }
    } else @panic("error: unsupported target");

    const mode = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });
    const emit_llvm_ir = b.option(bool, "emit-llvm-ir", "Emit LLVM IR (.ll file)") orelse false;

    const options = b.addOptions();

    var shell = Shell.create(b.allocator) catch unreachable;
    defer shell.destroy();

    // The "tigerbeetle version" command includes the build-time commit hash.
    const git_commit = b.option(
        []const u8,
        "git-commit",
        "The git commit revision of the source code.",
    ) orelse try shell.git_commit();
    options.addOption([]const u8, "git_commit", git_commit);

    const release_version_string = b.env_map.get("TIGERBEETLE_RELEASE");
    options.addOption([]const u8, "version", release_version_string orelse try shell.git_tag());
    options.addOption([]const u8, "release", release_version_string orelse "0.0.1");

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

    const vsr_options_module = options.createModule();
    const vsr_module = b.addModule("vsr", .{
        .source_file = .{ .path = "src/vsr.zig" },
        .dependencies = &.{.{
            .name = "vsr_options",
            .module = vsr_options_module,
        }},
    });

    {
        // Run a tigerbeetle build without running codegen and waiting for llvm
        // see <https://github.com/ziglang/zig/commit/5c0181841081170a118d8e50af2a09f5006f59e1>
        // how it's supposed to work.
        // In short, codegen only runs if zig build sees a dependency on the binary output of
        // the step. So we duplicate the build definition so that it doesn't get polluted by
        // b.installArtifact.
        // TODO(zig): https://github.com/ziglang/zig/issues/18877
        const tigerbeetle = b.addExecutable(.{
            .name = "tigerbeetle",
            .root_source_file = .{ .path = "src/tigerbeetle/main.zig" },
            .target = target,
            .optimize = mode,
        });
        tigerbeetle.addModule("vsr", vsr_module);
        tigerbeetle.addModule("vsr_options", vsr_options_module);

        const check = b.step("check", "Check if Tigerbeetle compiles");
        check.dependOn(&tigerbeetle.step);
    }

    const tigerbeetle = b.addExecutable(.{
        .name = "tigerbeetle",
        .root_source_file = .{ .path = "src/tigerbeetle/main.zig" },
        .target = target,
        .optimize = mode,
    });
    if (mode == .ReleaseSafe) {
        tigerbeetle.strip = true;
    }
    if (emit_llvm_ir) {
        _ = tigerbeetle.getEmittedLlvmIr();
    }

    {
        const zap = b.dependency("zap", .{
            .target = target,
            .optimize = mode,
            .openssl = false, // set to true to enable TLS support
        });
        tigerbeetle.addModule("zap", zap.module("zap"));
        tigerbeetle.linkLibrary(zap.artifact("facil.io"));
    }

    tigerbeetle.addModule("vsr", vsr_module);
    tigerbeetle.addModule("vsr_options", vsr_options_module);
    b.installArtifact(tigerbeetle);
    // Ensure that we get stack traces even in release builds.
    tigerbeetle.omit_frame_pointer = false;
    link_tracer_backend(tigerbeetle, git_clone_tracy, tracer_backend, target);

    {
        const run_cmd = b.addRunArtifact(tigerbeetle);
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
        const aof = b.addExecutable(.{
            .name = "aof",
            .root_source_file = .{ .path = "src/aof.zig" },
            .target = target,
            .optimize = mode,
        });
        aof.addOptions("vsr_options", options);
        link_tracer_backend(aof, git_clone_tracy, tracer_backend, target);

        const run_cmd = b.addRunArtifact(aof);
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("aof", "Run TigerBeetle AOF Utility");
        run_step.dependOn(&run_cmd.step);
    }

    // Linting targets
    // We currently have: lint_zig_fmt, lint_shellcheck.
    // The meta-target lint runs them all
    {
        // lint_zig_fmt
        const lint_zig_fmt = b.addFmt(.{ .paths = &.{"."}, .check = true });
        const lint_zig_fmt_step = b.step("lint_zig_fmt", "Run zig fmt");
        lint_zig_fmt_step.dependOn(&lint_zig_fmt.step);

        // lint_shellcheck
        const lint_shellcheck = ShellcheckStep.add(b);
        const lint_shellcheck_step = b.step("lint_shellcheck", "Run shellcheck on **.sh");
        lint_shellcheck_step.dependOn(&lint_shellcheck.step);

        // lint
        const lint_step = b.step("lint", "Run all defined linters");
        lint_step.dependOn(lint_zig_fmt_step);
        lint_step.dependOn(lint_shellcheck_step);
    }

    // Executable which generates src/clients/c/tb_client.h
    const tb_client_header_generate = blk: {
        const tb_client_header = b.addExecutable(.{
            .name = "tb_client_header",
            .root_source_file = .{ .path = "src/clients/c/tb_client_header.zig" },
            .target = target,
            .main_pkg_path = .{ .path = "src" },
        });

        tb_client_header.addOptions("vsr_options", options);
        break :blk b.addRunArtifact(tb_client_header);
    };

    {
        const test_filter: ?[]const u8 =
            if (b.args != null and b.args.?.len == 1) b.args.?[0] else null;

        const unit_tests = b.addTest(.{
            .root_source_file = .{ .path = "src/unit_tests.zig" },
            .target = target,
            .optimize = mode,
            .filter = test_filter,
        });
        unit_tests.addModule("vsr_options", vsr_options_module);
        unit_tests.step.dependOn(&tb_client_header_generate.step);
        link_tracer_backend(unit_tests, git_clone_tracy, tracer_backend, target);

        // for src/clients/c/tb_client_header_test.zig to use cImport on tb_client.h
        unit_tests.linkLibC();
        unit_tests.addIncludePath(.{ .path = "src/clients/c/" });

        const unit_tests_exe_step = b.step("test:build", "Build the unit tests");
        const install_unit_tests_exe = b.addInstallArtifact(unit_tests, .{});
        unit_tests_exe_step.dependOn(&install_unit_tests_exe.step);

        const run_unit_tests = b.addRunArtifact(unit_tests);
        run_unit_tests.setEnvironmentVariable("ZIG_EXE", b.zig_exe);
        const unit_tests_step = b.step("test:unit", "Run the unit tests");
        unit_tests_step.dependOn(&run_unit_tests.step);

        const integration_tests = b.addTest(.{
            .root_source_file = .{ .path = "src/integration_tests.zig" },
            .target = target,
            .optimize = mode,
        });
        const run_integration_tests = b.addRunArtifact(integration_tests);
        // Ensure integration test have tigerbeetle binary.
        run_integration_tests.step.dependOn(b.getInstallStep());
        const integration_tests_step = b.step("test:integration", "Run the integration tests");
        integration_tests_step.dependOn(&run_integration_tests.step);

        const test_step = b.step("test", "Run the unit tests");
        test_step.dependOn(&run_unit_tests.step);

        if (test_filter == null) {
            test_step.dependOn(&run_integration_tests.step);

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
                const demo_exe = b.addExecutable(.{
                    .name = demo,
                    .root_source_file = .{ .path = "src/demos/" ++ demo ++ ".zig" },
                    .target = target,
                });
                demo_exe.addModule("vsr", vsr_module);
                demo_exe.addModule("vsr_options", vsr_options_module);
                test_step.dependOn(&demo_exe.step);
            }
        }
    }

    // Clients build:
    {
        var install_step = b.addInstallArtifact(tigerbeetle, .{});

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
    }

    {
        const jni_tests_step = b.step("test:jni", "Run the JNI tests");

        // We need libjvm.so both at build time and at a runtime, so use `FailStep` when that is not
        // available.
        if (b.env_map.get("JAVA_HOME")) |java_home| {
            const libjvm_path = b.pathJoin(&.{
                java_home,
                if (builtin.os.tag == .windows) "/lib" else "/lib/server",
            });

            const tests = b.addTest(.{
                .root_source_file = .{ .path = "src/clients/java/src/jni_tests.zig" },
                .target = target,
                // TODO(zig): The function `JNI_CreateJavaVM` tries to detect
                // the stack size and causes a SEGV that is handled by Zig's panic handler.
                // https://bugzilla.redhat.com/show_bug.cgi?id=1572811#c7
                //
                // The workaround is run the tests in "ReleaseFast" mode.
                .optimize = if (builtin.os.tag == .windows) .ReleaseFast else mode,
            });
            tests.linkLibC();

            tests.linkSystemLibrary("jvm");
            tests.addLibraryPath(.{ .path = libjvm_path });
            if (builtin.os.tag == .linux) {
                // On Linux, detects the abi by calling `ldd` to check if
                // the libjvm.so is linked against libc or musl.
                // It's reasonable to assume that ldd will be present.
                var exit_code: u8 = undefined;
                const stderr_behavior = .Ignore;
                const ldd_result = try b.execAllowFail(
                    &.{ "ldd", b.pathJoin(&.{ libjvm_path, "libjvm.so" }) },
                    &exit_code,
                    stderr_behavior,
                );
                tests.target.abi = if (std.mem.indexOf(u8, ldd_result, "musl") != null)
                    .musl
                else if (std.mem.indexOf(u8, ldd_result, "libc") != null)
                    .gnu
                else {
                    std.log.err("{s}", .{ldd_result});
                    return error.JavaAbiUnrecognized;
                };
            }

            switch (builtin.os.tag) {
                .windows => set_windows_dll(b.allocator, java_home),
                .macos => try b.env_map.put("DYLD_LIBRARY_PATH", libjvm_path),
                .linux => try b.env_map.put("LD_LIBRARY_PATH", libjvm_path),
                else => unreachable,
            }

            const tests_run = b.addRunArtifact(tests);
            jni_tests_step.dependOn(&tests_run.step);
        } else {
            const fail_step = FailStep.add(
                b,
                "can't build jni tests tests, JAVA_HOME is not set",
            );
            jni_tests_step.dependOn(&fail_step.step);
        }
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
        simulator_options.addOption(
            SimulatorLog,
            "log",
            b.option(
                SimulatorLog,
                "simulator-log",
                "Log only state transitions (short) or everything (full).",
            ) orelse .short,
        );

        const simulator = b.addExecutable(.{
            .name = "simulator",
            .root_source_file = .{ .path = "src/simulator.zig" },
            .target = target,
            .optimize = simulator_mode,
        });
        // Ensure that we get stack traces even in release builds.
        simulator.omit_frame_pointer = false;
        simulator.addOptions("vsr_options", options);
        simulator.addOptions("vsr_simulator_options", simulator_options);
        link_tracer_backend(simulator, git_clone_tracy, tracer_backend, target);

        const run_cmd = b.addRunArtifact(simulator);

        if (b.args) |args| run_cmd.addArgs(args);

        const install_step = b.addInstallArtifact(simulator, .{});
        const build_step = b.step("simulator", "Build the Simulator");
        build_step.dependOn(&install_step.step);

        const run_step = b.step("simulator_run", "Run the Simulator");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const vopr = b.addExecutable(.{
            .name = "vopr",
            .root_source_file = .{ .path = "src/vopr.zig" },
            .target = target,
            .optimize = if (b.args != null) mode else .ReleaseSafe,
        });
        // Ensure that we get stack traces even in release builds.
        vopr.omit_frame_pointer = false;
        vopr.addOptions("vsr_options", options);
        link_tracer_backend(vopr, git_clone_tracy, tracer_backend, target);

        const run_cmd = b.addRunArtifact(vopr);
        if (b.args) |args| run_cmd.addArgs(args);

        const step = b.step("vopr", "Run the VOPR");
        step.dependOn(&run_cmd.step);
    }

    { // Fuzzers: zig build fuzz -- lsm_tree --seed=92 --events-max=100
        const fuzz_exe = b.addExecutable(.{
            .name = "fuzz",
            .root_source_file = .{ .path = "src/fuzz_tests.zig" },
            .target = target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });
        fuzz_exe.omit_frame_pointer = false;
        fuzz_exe.addOptions("vsr_options", options);
        link_tracer_backend(fuzz_exe, git_clone_tracy, tracer_backend, target);

        const fuzz_run = b.addRunArtifact(fuzz_exe);
        if (b.args) |args| fuzz_run.addArgs(args);

        const fuzz_step = b.step("fuzz", "Run the specified fuzzer");
        fuzz_step.dependOn(&fuzz_run.step);

        const fuzz_install_step = b.addInstallArtifact(fuzz_exe, .{});
        const fuzz_build_step = b.step("build_fuzz", "Build fuzzers");
        fuzz_build_step.dependOn(&fuzz_install_step.step);
    }

    inline for (.{
        .{
            .name = "benchmark_ewah",
            .file = "src/ewah_benchmark.zig",
            .description = "EWAH codec",
        },
        .{
            .name = "benchmark_binary_search",
            .file = "src/lsm/binary_search_benchmark.zig",
            .description = "Array search",
        },
        .{
            .name = "benchmark_segmented_array",
            .file = "src/lsm/segmented_array_benchmark.zig",
            .description = "SegmentedArray search",
        },
    }) |benchmark| {
        const exe = b.addExecutable(.{
            .name = benchmark.name,
            .root_source_file = .{ .path = benchmark.file },
            .target = target,
            .optimize = .ReleaseSafe,
            .main_pkg_path = .{ .path = "src" },
        });
        exe.addOptions("vsr_options", options);
        link_tracer_backend(exe, git_clone_tracy, tracer_backend, target);

        const build_step = b.step(
            "build_" ++ benchmark.name,
            "Build " ++ benchmark.description ++ " benchmark",
        );
        build_step.dependOn(&exe.step);

        const run_cmd = b.addRunArtifact(exe);
        const step = b.step(benchmark.name, "Benchmark " ++ benchmark.description);
        step.dependOn(&run_cmd.step);
    }

    { // Free-form automation: `zig build scripts -- ci --language=java`
        const scripts_exe = b.addExecutable(.{
            .name = "scripts",
            .root_source_file = .{ .path = "src/scripts/main.zig" },
            .target = target,
            .main_pkg_path = .{ .path = "src" },
        });
        const scripts_run = b.addRunArtifact(scripts_exe);
        if (b.args) |args| scripts_run.addArgs(args);
        const scripts_step = b.step("scripts", "Run automation scripts");
        scripts_step.dependOn(&scripts_run.step);
    }
}

fn link_tracer_backend(
    exe: *std.Build.LibExeObjStep,
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

            exe.addIncludePath(.{ .path = "./tools/tracy/public/tracy" });
            exe.addCSourceFile(.{
                .file = .{ .path = "./tools/tracy/public/TracyClient.cpp" },
                .flags = tracy_c_flags,
            });
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
    .{ "x86_64-linux-gnu.2.27", "linux-x64" },
    .{ "x86_64-linux-musl", "linux-musl-x64" },
    .{ "x86_64-macos", "osx-x64" },
    .{ "aarch64-linux-gnu.2.27", "linux-arm64" },
    .{ "aarch64-linux-musl", "linux-musl-arm64" },
    .{ "aarch64-macos", "osx-arm64" },
    .{ "x86_64-windows", "win-x64" },
};

fn strip_glibc_version(triple: []const u8) []const u8 {
    if (std.mem.endsWith(u8, triple, "gnu.2.27")) {
        return triple[0 .. triple.len - ".2.27".len];
    }
    assert(std.mem.indexOf(u8, triple, "gnu") == null);
    return triple;
}

fn go_client(
    b: *std.Build,
    mode: Mode,
    dependencies: []const *std.Build.Step,
    target: CrossTarget,
    options: *std.Build.OptionsStep,
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

    const bindings = b.addExecutable(.{
        .name = "go_bindings",
        .root_source_file = .{ .path = "src/clients/go/go_bindings.zig" },
        .target = target,
        .main_pkg_path = .{ .path = "src" },
    });
    bindings.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    inline for (platforms) |platform| {
        // We don't need the linux-gnu builds.
        if (comptime std.mem.indexOf(u8, platform[0], "linux-gnu") != null) continue;

        const name = if (comptime std.mem.eql(u8, platform[0], "x86_64-linux-musl"))
            "x86_64-linux"
        else if (comptime std.mem.eql(u8, platform[0], "aarch64-linux-musl"))
            "aarch64-linux"
        else
            platform[0];

        const cross_target = CrossTarget.parse(.{ .arch_os_abi = name, .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const lib = b_isolated.addStaticLibrary(.{
            .name = "tb_client",
            .root_source_file = .{ .path = "src/clients/c/tb_client_exports.zig" },
            .target = cross_target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });
        lib.linkLibC();
        lib.pie = true;
        lib.bundle_compiler_rt = true;
        lib.stack_protector = false;

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

        lib.step.dependOn(&install_header.step);
        lib.step.dependOn(&bindings_step.step);

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/.
        const lib_install = b.addInstallArtifact(lib, .{});
        lib_install.dest_dir = .{ .custom = "../src/clients/go/pkg/native/" ++ name };
        build_step.dependOn(&lib_install.step);
    }
}

fn java_client(
    b: *std.Build,
    mode: Mode,
    dependencies: []const *std.Build.Step,
    target: CrossTarget,
    options: *std.Build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("java_client", "Build Java client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable(.{
        .name = "java_bindings",
        .root_source_file = .{ .path = "src/clients/java/java_bindings.zig" },
        .target = target,
        .main_pkg_path = .{ .path = "src" },
    });
    bindings.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const lib = b_isolated.addSharedLibrary(.{
            .name = "tb_jniclient",
            .root_source_file = .{ .path = "src/clients/java/src/client.zig" },
            .target = cross_target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });
        lib.linkLibC();

        if (cross_target.os_tag.? == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

        lib.step.dependOn(&bindings_step.step);

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/.
        const lib_install = b.addInstallArtifact(lib, .{});
        lib_install.dest_dir = .{
            .custom = "../src/clients/java/src/main/resources/lib/" ++
                comptime strip_glibc_version(platform[0]),
        };
        build_step.dependOn(&lib_install.step);
    }
}

fn dotnet_client(
    b: *std.Build,
    mode: Mode,
    dependencies: []const *std.Build.Step,
    target: CrossTarget,
    options: *std.Build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("dotnet_client", "Build dotnet client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable(.{
        .name = "dotnet_bindings",
        .root_source_file = .{ .path = "src/clients/dotnet/dotnet_bindings.zig" },
        .target = target,
        .main_pkg_path = .{ .path = "src" },
    });
    bindings.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const lib = b_isolated.addSharedLibrary(.{
            .name = "tb_client",
            .root_source_file = .{ .path = "src/clients/c/tb_client_exports.zig" },
            .target = cross_target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });
        lib.linkLibC();

        if (cross_target.os_tag.? == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

        lib.step.dependOn(&bindings_step.step);

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/
        const lib_install = b.addInstallArtifact(lib, .{});
        lib_install.dest_dir = .{
            .custom = "../src/clients/dotnet/TigerBeetle/runtimes/" ++ platform[1] ++ "/native",
        };
        build_step.dependOn(&lib_install.step);
    }
}

fn node_client(
    b: *std.Build,
    mode: Mode,
    dependencies: []const *std.Build.Step,
    target: CrossTarget,
    options: *std.Build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const build_step = b.step("node_client", "Build Node client shared library");
    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable(.{
        .name = "node_bindings",
        .root_source_file = .{ .path = "src/clients/node/node_bindings.zig" },
        .target = target,
        .main_pkg_path = .{ .path = "src" },
    });
    bindings.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        var b_isolated = builder_with_isolated_cache(b, cross_target);

        const lib = b_isolated.addSharedLibrary(.{
            .name = "tb_nodeclient",
            .root_source_file = .{ .path = "src/clients/node/src/node.zig" },
            .target = cross_target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });
        lib.linkLibC();

        // This is provided by the node-api-headers package; make sure to run `npm install` under `src/clients/node`
        // if you're running zig build node_client manually.
        lib.addSystemIncludePath(.{ .path = "src/clients/node/node_modules/node-api-headers/include" });
        lib.linker_allow_shlib_undefined = true;

        if (cross_target.os_tag.? == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");

            lib.addLibraryPath(.{ .path = "src/clients/node" });
            lib.linkSystemLibrary("node");
        }

        lib.addOptions("vsr_options", options);
        link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

        lib.step.dependOn(&bindings_step.step);

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/
        const lib_install = b.addInstallArtifact(lib, .{});
        lib_install.dest_dir = .{
            .custom = "../src/clients/node/dist/bin/" ++ comptime strip_glibc_version(platform[0]),
        };
        build_step.dependOn(&lib_install.step);
    }
}

fn c_client(
    b: *std.Build,
    mode: Mode,
    dependencies: []const *std.Build.Step,
    options: *std.Build.OptionsStep,
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

        const shared_lib = b_isolated.addSharedLibrary(.{
            .name = "tb_client",
            .root_source_file = .{ .path = "src/clients/c/tb_client_exports.zig" },
            .target = cross_target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });
        const static_lib = b_isolated.addStaticLibrary(.{
            .name = "tb_client",
            .root_source_file = .{ .path = "src/clients/c/tb_client_exports.zig" },
            .target = cross_target,
            .optimize = mode,
            .main_pkg_path = .{ .path = "src" },
        });

        static_lib.bundle_compiler_rt = true;
        static_lib.pie = true;

        for ([_]*std.Build.Step.Compile{ shared_lib, static_lib }) |lib| {
            lib.linkLibC();

            if (cross_target.os_tag.? == .windows) {
                lib.linkSystemLibrary("ws2_32");
                lib.linkSystemLibrary("advapi32");
            }

            lib.addOptions("vsr_options", options);
            link_tracer_backend(lib, git_clone_tracy, tracer_backend, cross_target);

            // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/
            const lib_install = b.addInstallArtifact(lib, .{});
            lib_install.dest_dir = .{
                .custom = "../src/clients/c/lib/" ++ comptime strip_glibc_version(platform[0]),
            };
            build_step.dependOn(&lib_install.step);
        }
    }
}

fn c_client_sample(
    b: *std.Build,
    mode: Mode,
    target: CrossTarget,
    dependencies: []const *std.Build.Step,
    options: *std.Build.OptionsStep,
    git_clone_tracy: *GitCloneStep,
    tracer_backend: config.TracerBackend,
) void {
    const c_sample_build = b.step("c_sample", "Build the C client sample");
    for (dependencies) |dependency| {
        c_sample_build.dependOn(dependency);
    }

    const static_lib = b.addStaticLibrary(.{
        .name = "tb_client",
        .root_source_file = .{ .path = "src/clients/c/tb_client_exports.zig" },
        .target = target,
        .optimize = mode,
        .main_pkg_path = .{ .path = "src" },
    });
    static_lib.linkLibC();
    static_lib.pie = true;
    static_lib.bundle_compiler_rt = true;
    static_lib.addOptions("vsr_options", options);
    link_tracer_backend(static_lib, git_clone_tracy, tracer_backend, target);
    c_sample_build.dependOn(&static_lib.step);

    const sample = b.addExecutable(.{
        .name = "c_sample",
        .root_source_file = .{ .path = "src/clients/c/samples/main.c" },
        .target = target,
        .optimize = mode,
    });
    sample.linkLibrary(static_lib);
    sample.linkLibC();

    if (target.isWindows()) {
        static_lib.linkSystemLibrary("ws2_32");
        static_lib.linkSystemLibrary("advapi32");

        // TODO: Illegal instruction on Windows:
        sample.disable_sanitize_c = true;
    }

    const install_step = b.addInstallArtifact(sample, .{});
    c_sample_build.dependOn(&install_step.step);
}

/// Steps which unconditionally fails with a message.
///
/// This is useful for cases where at configuration time you can determine that a certain step
/// can't succeeded (e.g., a system library is not preset on the host system), but you only want
/// to fail the step once the user tries to run it. That is, you don't want to fail the whole build,
/// as other steps might run fine.
const FailStep = struct {
    step: std.build.Step,
    message: []const u8,

    fn add(b: *std.Build, message: []const u8) *FailStep {
        const result = b.allocator.create(FailStep) catch unreachable;
        result.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "failure",
                .owner = b,
                .makeFn = FailStep.make,
            }),
            .message = message,
        };
        return result;
    }

    fn make(step: *std.Build.Step, _: *std.Progress.Node) anyerror!void {
        const self = @fieldParentPtr(FailStep, "step", step);
        std.log.err("{s}", .{self.message});
        return error.FailStep;
    }
};

const ShellcheckStep = struct {
    step: std.Build.Step,
    gpa: std.mem.Allocator,

    fn add(b: *std.Build) *ShellcheckStep {
        const result = b.allocator.create(ShellcheckStep) catch unreachable;
        result.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "run shellcheck",
                .owner = b,
                .makeFn = ShellcheckStep.make,
            }),
            .gpa = b.allocator,
        };
        return result;
    }

    fn make(step: *std.Build.Step, _: *std.Progress.Node) anyerror!void {
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
            .extension = ".sh",
        });

        try shell.exec("shellcheck {scripts}", .{ .scripts = scripts });
        try shell.exec("shellcheck ./bootstrap.sh", .{});
    }
};

/// Every large project contains its own bespoke implementation of `git submodule`, this is ours.
/// We use `GitCloneStep` to lazily download build-time dependencies when we need them.
const GitCloneStep = struct {
    step: std.Build.Step,
    gpa: std.mem.Allocator,
    options: Options,

    const Options = struct {
        repo: []const u8,
        tag: []const u8,
        path: []const u8,
    };

    fn add(b: *std.Build, options: Options) *GitCloneStep {
        const result = b.allocator.create(GitCloneStep) catch unreachable;
        result.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "run git clone",
                .owner = b,
                .makeFn = GitCloneStep.make,
            }),
            .gpa = b.allocator,
            .options = options,
        };
        return result;
    }

    fn make(step: *std.Build.Step, _: *std.Progress.Node) anyerror!void {
        const self = @fieldParentPtr(GitCloneStep, "step", step);

        var shell = try Shell.create(self.gpa);
        defer shell.destroy();

        if (try shell.dir_exists(self.options.path)) return;
        try shell.exec("git clone --branch {tag} {repo} {path}", self.options);
    }
};

/// Set the JVM DLL directory on Windows.
fn set_windows_dll(allocator: std.mem.Allocator, java_home: []const u8) void {
    comptime std.debug.assert(builtin.os.tag == .windows);
    const set_dll_directory = struct {
        pub extern "kernel32" fn SetDllDirectoryA(path: [*:0]const u8) callconv(.C) std.os.windows.BOOL;
    }.SetDllDirectoryA;

    const java_bin_path = std.fs.path.joinZ(
        allocator,
        &.{ java_home, "\\bin" },
    ) catch unreachable;
    _ = set_dll_directory(java_bin_path);

    const java_bin_server_path = std.fs.path.joinZ(
        allocator,
        &.{ java_home, "\\bin\\server" },
    ) catch unreachable;
    _ = set_dll_directory(java_bin_server_path);
}

/// Creates a new Builder, with isolated cache for each platform.
/// Hit some issues with the build cache between cross compilations:
/// - From Linux, it runs fine.
/// - From Windows it fails on libc "invalid object".
/// - From MacOS, similar to https://github.com/ziglang/zig/issues/9711#issuecomment-1090071087.
/// Workaround: Just setting different cache folders for each platform.
fn builder_with_isolated_cache(
    b: *std.Build,
    target: CrossTarget,
) *std.Build {
    // This workaround isn't necessary when cross-compiling from Linux.
    if (builtin.os.tag == .linux) return b;

    // If not cross-compiling, we can return the current *Builder in order
    // to reuse the same cache from other artifacts.
    if (target.cpu_arch.? == builtin.cpu.arch and
        target.os_tag.? == builtin.os.tag)
        return b;

    // Generating isolated cache and global_cache dirs for each cpu/os:
    const cache_root = create_cache_directory(b.pathJoin(&.{
        b.cache_root.path.?,
        @tagName(target.cpu_arch.?),
        @tagName(target.os_tag.?),
    }));
    const global_cache_root = create_cache_directory(b.pathJoin(&.{
        b.global_cache_root.path.?,
        @tagName(target.cpu_arch.?),
        @tagName(target.os_tag.?),
    }));

    // Need to create a custom cache as the local_cache_root changes.
    // See: https://github.com/ziglang/zig/blob/0.11.0/lib/build_runner.zig#L68
    const cache = b.allocator.create(std.Build.Cache) catch unreachable;
    cache.* = .{
        .gpa = b.allocator,
        .manifest_dir = cache_root.handle.makeOpenPath("h", .{}) catch unreachable,
    };
    cache.addPrefix(.{ .path = null, .handle = std.fs.cwd() });
    cache.addPrefix(b.build_root);
    cache.addPrefix(cache_root);
    cache.addPrefix(global_cache_root);
    cache.hash.addBytes(builtin.zig_version_string);

    // Note, this builder leaks memory, since there is no way to deinit it.
    return std.Build.create(
        b.allocator,
        b.zig_exe,
        b.build_root,
        cache_root,
        global_cache_root,
        std.zig.system.NativeTargetInfo.detect(target) catch unreachable,
        cache,
    ) catch unreachable;
}

fn create_cache_directory(path: []const u8) std.Build.Cache.Directory {
    std.fs.cwd().makePath(path) catch unreachable;
    return .{
        .path = path,
        .handle = std.fs.cwd().openDir(path, .{}) catch unreachable,
    };
}
