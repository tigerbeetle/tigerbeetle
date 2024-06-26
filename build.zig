const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const CrossTarget = std.zig.CrossTarget;
const Mode = std.builtin.Mode;

const config = @import("./src/config.zig");

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
        if (target.result.cpu.arch == supported_target.cpu_arch) {
            // CPU model detection from: https://github.com/ziglang/zig/blob/0.13.0/lib/std/zig/system.zig#L320
            target.result.cpu.model = switch (supported_target.cpu_model) {
                .native => @panic("pre-defined supported target assumed runtime-detected cpu model"),
                .baseline, .determined_by_cpu_arch => std.Target.Cpu.baseline(supported_target.cpu_arch.?).model,
                .explicit => |model| model,
            };
            target.result.cpu.features.addFeatureSet(supported_target.cpu_features_add);
            target.result.cpu.features.removeFeatureSet(supported_target.cpu_features_sub);
            break;
        }
    } else @panic("error: unsupported target");

    const mode = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });
    const emit_llvm_ir = b.option(bool, "emit-llvm-ir", "Emit LLVM IR (.ll file)") orelse false;

    const options = b.addOptions();

    // The "tigerbeetle version" command includes the build-time commit hash.
    const git_commit = b.option(
        []const u8,
        "git-commit",
        "The git commit revision of the source code.",
    ) orelse std.mem.trimRight(u8, b.run(&.{ "git", "rev-parse", "--verify", "HEAD" }), "\n");
    assert(git_commit.len == 40);
    options.addOption(?[40]u8, "git_commit", git_commit[0..40].*);

    options.addOption(
        ?[]const u8,
        "release",
        b.option([]const u8, "config-release", "Release triple."),
    );

    options.addOption(
        ?[]const u8,
        "release_client_min",
        b.option([]const u8, "config-release-client-min", "Minimum client release triple."),
    );

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
        .root_source_file = b.path("src/vsr.zig"),
    });
    vsr_module.addImport("vsr_options", vsr_options_module);
    switch (tracer_backend) {
        .none => {},
        .tracy => {
            // Code here is based on
            // https://github.com/ziglang/zig/blob/a660df4900520c505a0865707552dcc777f4b791/build.zig#L382

            // On mingw, we need to opt into windows 7+ to get some features required by tracy.
            const tracy_c_flags: []const []const u8 = if (target.result.os.tag == .windows and target.result.abi == .gnu)
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

            const tracy = b.addSystemCommand(&.{
                "git",
                "clone",
                "--branch=v0.9.1",
                "https://github.com/wolfpld/tracy.git",
            }).addOutputDirectoryArg("tracy");

            vsr_module.addCSourceFile(.{
                .file = tracy.path(b, "./public/TracyClient.cpp"),
                .flags = tracy_c_flags,
            });
            vsr_module.addIncludePath(tracy.path(b, "./public/tracy"));
            vsr_module.link_libc = true;
            vsr_module.link_libcpp = true;

            if (target.result.os.tag == .windows) {
                vsr_module.linkSystemLibrary("dbghelp", .{});
                vsr_module.linkSystemLibrary("ws2_32", .{});
            }
        },
    }

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
            .root_source_file = b.path("src/tigerbeetle/main.zig"),
            .target = target,
            .optimize = mode,
        });
        tigerbeetle.root_module.addImport("vsr", vsr_module);
        tigerbeetle.root_module.addImport("vsr_options", vsr_options_module);

        const check = b.step("check", "Check if Tigerbeetle compiles");
        check.dependOn(&tigerbeetle.step);
    }

    const tigerbeetle = b.addExecutable(.{
        .name = "tigerbeetle",
        .root_source_file = b.path("src/tigerbeetle/main.zig"),
        .target = target,
        .optimize = mode,
    });
    if (mode == .ReleaseSafe) {
        tigerbeetle.root_module.strip = tracer_backend == .none;
    }
    if (emit_llvm_ir) {
        _ = tigerbeetle.getEmittedLlvmIr();
    }
    tigerbeetle.root_module.addImport("vsr", vsr_module);
    tigerbeetle.root_module.addImport("vsr_options", vsr_options_module);
    b.installArtifact(tigerbeetle);
    // Ensure that we get stack traces even in release builds.
    tigerbeetle.root_module.omit_frame_pointer = false;

    {
        const run_cmd = b.addRunArtifact(tigerbeetle);
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run TigerBeetle");
        run_step.dependOn(&run_cmd.step);
    }

    {
        // "zig build install" moves the server executable to the root folder:
        const move_cmd = b.addInstallBinFile(
            tigerbeetle.getEmittedBin(),
            b.pathJoin(&.{ "../../", tigerbeetle.out_filename }),
        );
        move_cmd.step.dependOn(&tigerbeetle.step);

        const install_step = b.getInstallStep();
        install_step.dependOn(&move_cmd.step);
    }

    {
        const aof = b.addExecutable(.{
            .name = "aof",
            .root_source_file = b.path("src/aof.zig"),
            .target = target,
            .optimize = mode,
        });
        aof.root_module.addOptions("vsr_options", options);

        const run_cmd = b.addRunArtifact(aof);
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("aof", "Run TigerBeetle AOF Utility");
        run_step.dependOn(&run_cmd.step);
    }

    const tb_client_header = blk: {
        const tb_client_header = b.addExecutable(.{
            .name = "tb_client_header",
            .root_source_file = b.path("src/clients/c/tb_client_header.zig"),
            .target = target,
        });
        tb_client_header.root_module.addImport("vsr", vsr_module);
        tb_client_header.root_module.addOptions("vsr_options", options);

        const run = b.addRunArtifact(tb_client_header);
        const out_file = run.addOutputFileArg("tb_client.h");

        const install = InstallFile.create(b, out_file, b.pathFromRoot("src/clients/c/tb_client.h"));
        break :blk install.getDest();
    };

    {
        const test_filter: ?[]const u8 =
            if (b.args != null and b.args.?.len == 1) b.args.?[0] else null;

        const unit_tests = b.addTest(.{
            .root_source_file = b.path("src/unit_tests.zig"),
            .target = target,
            .optimize = mode,
            .filter = test_filter,
        });
        unit_tests.root_module.addImport("vsr_options", vsr_options_module);

        // for src/clients/c/tb_client_header_test.zig to use cImport on tb_client.h
        unit_tests.linkLibC();
        unit_tests.addIncludePath(tb_client_header.dirname());

        const unit_tests_exe_step = b.step("test:build", "Build the unit tests");
        const install_unit_tests_exe = b.addInstallArtifact(unit_tests, .{});
        unit_tests_exe_step.dependOn(&install_unit_tests_exe.step);

        const run_unit_tests = b.addRunArtifact(unit_tests);
        run_unit_tests.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);
        const unit_tests_step = b.step("test:unit", "Run the unit tests");
        unit_tests_step.dependOn(&run_unit_tests.step);

        const integration_tests = b.addTest(.{
            .root_source_file = b.path("src/integration_tests.zig"),
            .target = target,
            .optimize = mode,
        });
        const run_integration_tests = b.addRunArtifact(integration_tests);
        // Ensure integration test have tigerbeetle binary.
        run_integration_tests.step.dependOn(b.getInstallStep());
        const integration_tests_step = b.step("test:integration", "Run the integration tests");
        integration_tests_step.dependOn(&run_integration_tests.step);

        const run_fmt = b.addFmt(.{ .paths = &.{"."}, .check = true });
        const fmt_test_step = b.step("test:fmt", "Check formatting");
        fmt_test_step.dependOn(&run_fmt.step);

        const test_step = b.step("test", "Run the unit tests");
        test_step.dependOn(&run_unit_tests.step);
        if (test_filter == null) {
            test_step.dependOn(&run_integration_tests.step);
            test_step.dependOn(&run_fmt.step);
        }
    }

    // Clients build:
    {
        var install_step = b.addInstallArtifact(tigerbeetle, .{});

        go_client(
            b,
            mode,
            &.{&install_step.step},
            tb_client_header,
            target,
            options,
        );
        java_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            vsr_module,
            options,
        );
        dotnet_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
        );
        node_client(
            b,
            mode,
            &.{&install_step.step},
            target,
            options,
        );
        c_client(
            b,
            mode,
            &.{&install_step.step},
            tb_client_header,
            options,
        );
        c_client_sample(
            b,
            mode,
            target,
            &.{&install_step.step},
            options,
        );
    }

    {
        const jni_tests_step = b.step("test:jni", "Run the JNI tests");

        // We need libjvm.so both at build time and at a runtime, so use `FailStep` when that is not
        // available.
        if (b.graph.env_map.get("JAVA_HOME")) |java_home| {
            const libjvm_path = b.pathJoin(&.{
                java_home,
                if (builtin.os.tag == .windows) "/lib" else "/lib/server",
            });

            const tests = b.addTest(.{
                .root_source_file = b.path("src/clients/java/src/jni_tests.zig"),
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
            tests.addLibraryPath(.{ .cwd_relative = libjvm_path });
            if (builtin.os.tag == .linux) {
                // On Linux, detects the abi by calling `ldd` to check if
                // the libjvm.so is linked against libc or musl.
                // It's reasonable to assume that ldd will be present.
                var exit_code: u8 = undefined;
                const stderr_behavior = .Ignore;
                const ldd_result = try b.runAllowFail(
                    &.{ "ldd", b.pathJoin(&.{ libjvm_path, "libjvm.so" }) },
                    &exit_code,
                    stderr_behavior,
                );
                tests.root_module.resolved_target.?.result.abi = if (std.mem.indexOf(u8, ldd_result, "musl") != null)
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
                .macos => try b.graph.env_map.put("DYLD_LIBRARY_PATH", libjvm_path),
                .linux => try b.graph.env_map.put("LD_LIBRARY_PATH", libjvm_path),
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
            .root_source_file = b.path("src/simulator.zig"),
            .target = target,
            .optimize = simulator_mode,
        });
        // Ensure that we get stack traces even in release builds.
        simulator.root_module.omit_frame_pointer = false;
        simulator.root_module.addOptions("vsr_options", options);
        simulator.root_module.addOptions("vsr_simulator_options", simulator_options);

        const run_cmd = b.addRunArtifact(simulator);

        if (b.args) |args| run_cmd.addArgs(args);

        const install_step = b.addInstallArtifact(simulator, .{});
        const build_step = b.step("simulator", "Build the Simulator");
        build_step.dependOn(&install_step.step);

        const run_step = b.step("simulator_run", "Run the Simulator");
        run_step.dependOn(&run_cmd.step);
    }

    { // Fuzzers: zig build fuzz -- --events-max=100 lsm_tree 123
        const fuzz_exe = b.addExecutable(.{
            .name = "fuzz",
            .root_source_file = b.path("src/fuzz_tests.zig"),
            .target = target,
            .optimize = mode,
        });
        fuzz_exe.root_module.omit_frame_pointer = false;
        fuzz_exe.root_module.addOptions("vsr_options", options);

        const fuzz_run = b.addRunArtifact(fuzz_exe);
        if (b.args) |args| fuzz_run.addArgs(args);

        const fuzz_step = b.step("fuzz", "Run the specified fuzzer");
        fuzz_step.dependOn(&fuzz_run.step);

        const fuzz_install_step = b.addInstallArtifact(fuzz_exe, .{});
        const fuzz_build_step = b.step("build_fuzz", "Build fuzzers");
        fuzz_build_step.dependOn(&fuzz_install_step.step);
    }

    { // Free-form automation: `zig build scripts -- ci --language=java`
        const scripts_exe = b.addExecutable(.{
            .name = "scripts",
            .root_source_file = b.path("src/scripts.zig"),
            .target = target,
            .optimize = mode,
        });
        const scripts_run = b.addRunArtifact(scripts_exe);
        scripts_run.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);
        if (b.args) |args| scripts_run.addArgs(args);
        const scripts_step = b.step("scripts", "Run automation scripts");
        scripts_step.dependOn(&scripts_run.step);
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
    tb_client_header: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    options: *std.Build.Step.Options,
) void {
    const build_step = b.step("go_client", "Build Go client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    // Updates the generated header file:
    const install_header = InstallFile.create(
        b,
        tb_client_header,
        b.pathFromRoot("src/clients/go/pkg/native/tb_client.h"),
    );
    build_step.dependOn(&install_header.step);

    const bindings = b.addExecutable(.{
        .name = "go_bindings",
        .root_source_file = b.path("src/go_bindings.zig"),
        .target = target,
    });
    bindings.root_module.addOptions("vsr_options", options);
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
        const resolved_target = b.resolveTargetQuery(cross_target);

        const lib = b.addStaticLibrary(.{
            .name = "tb_client",
            .root_source_file = b.path("src/tb_client_exports.zig"),
            .target = resolved_target,
            .optimize = mode,
        });
        lib.linkLibC();
        lib.pie = true;
        lib.bundle_compiler_rt = true;
        lib.root_module.stack_protector = false;
        lib.root_module.addOptions("vsr_options", options);

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
    target: std.Build.ResolvedTarget,
    vsr_module: *std.Build.Module,
    options: *std.Build.Step.Options,
) void {
    const build_step = b.step("java_client", "Build Java client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable(.{
        .name = "java_bindings",
        .root_source_file = b.path("src/java_bindings.zig"),
        .target = target,
    });
    bindings.root_module.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(cross_target);

        const lib = b.addSharedLibrary(.{
            .name = "tb_jniclient",
            .root_source_file = b.path("src/clients/java/src/client.zig"),
            .target = resolved_target,
            .optimize = mode,
        });
        lib.linkLibC();

        if (resolved_target.result.os.tag == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.root_module.addImport("vsr", vsr_module);
        lib.root_module.addOptions("vsr_options", options);

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
    target: std.Build.ResolvedTarget,
    options: *std.Build.Step.Options,
) void {
    const build_step = b.step("dotnet_client", "Build dotnet client shared library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable(.{
        .name = "dotnet_bindings",
        .root_source_file = b.path("src/dotnet_bindings.zig"),
        .target = target,
    });
    bindings.root_module.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(cross_target);

        const lib = b.addSharedLibrary(.{
            .name = "tb_client",
            .root_source_file = b.path("src/tb_client_exports.zig"),
            .target = resolved_target,
            .optimize = mode,
        });
        lib.linkLibC();

        if (resolved_target.result.os.tag == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }

        lib.root_module.addOptions("vsr_options", options);

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
    target: std.Build.ResolvedTarget,
    options: *std.Build.Step.Options,
) void {
    const build_step = b.step("node_client", "Build Node client shared library");
    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    const bindings = b.addExecutable(.{
        .name = "node_bindings",
        .root_source_file = b.path("src/node_bindings.zig"),
        .target = target,
    });
    bindings.root_module.addOptions("vsr_options", options);
    const bindings_step = b.addRunArtifact(bindings);

    // Run `npm install` to get access to node headers.
    var npm_install = b.addSystemCommand(&.{ "npm", "install" });
    npm_install.cwd = b.path("./src/clients/node");

    // For windows, compile a set of all symbols that could be exported by node and write it to a
    // `.def` file for `zig dlltool` to generate a `.lib` file from.
    var write_def_file = b.addSystemCommand(&.{
        "node", "--eval",
        \\const headers = require('node-api-headers')
        \\
        \\const allSymbols = new Set()
        \\for (const ver of Object.values(headers.symbols)) {
        \\    for (const sym of ver.node_api_symbols) {
        \\        allSymbols.add(sym)
        \\    }
        \\    for (const sym of ver.js_native_api_symbols) {
        \\        allSymbols.add(sym)
        \\    }
        \\}
        \\
        \\fs.writeFileSync('./node.def', 'EXPORTS\n    ' + Array.from(allSymbols).join('\n    '))
    });
    write_def_file.cwd = b.path("./src/clients/node");
    write_def_file.step.dependOn(&npm_install.step);

    var run_dll_tool = b.addSystemCommand(&.{
        b.graph.zig_exe, "dlltool",
        "-m",            "i386:x86-64",
        "-D",            "node.exe",
        "-d",            "node.def",
        "-l",            "node.lib",
    });
    run_dll_tool.cwd = b.path("./src/clients/node");
    run_dll_tool.step.dependOn(&write_def_file.step);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(cross_target);

        const lib = b.addSharedLibrary(.{
            .name = "tb_nodeclient",
            .root_source_file = b.path("src/node.zig"),
            .target = resolved_target,
            .optimize = mode,
        });
        lib.linkLibC();

        lib.step.dependOn(&npm_install.step);
        lib.addSystemIncludePath(b.path("src/clients/node/node_modules/node-api-headers/include"));
        lib.linker_allow_shlib_undefined = true;

        if (resolved_target.result.os.tag == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");

            lib.step.dependOn(&run_dll_tool.step);
            lib.addLibraryPath(b.path("src/clients/node"));
            lib.linkSystemLibrary("node");
        }

        lib.root_module.addOptions("vsr_options", options);

        lib.step.dependOn(&bindings_step.step);

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/
        const lib_install = b.addInstallFile(
            lib.getEmittedBin(),
            "../src/clients/node/dist/bin/" ++
                comptime strip_glibc_version(platform[0]) ++
                "/client.node",
        );
        build_step.dependOn(&lib_install.step);
    }
}

fn c_client(
    b: *std.Build,
    mode: Mode,
    dependencies: []const *std.Build.Step,
    tb_client_header: std.Build.LazyPath,
    options: *std.Build.Step.Options,
) void {
    const build_step = b.step("c_client", "Build C client library");

    for (dependencies) |dependency| {
        build_step.dependOn(dependency);
    }

    // Updates the generated header file:
    const install_header = InstallFile.create(
        b,
        tb_client_header,
        b.pathFromRoot("src/clients/c/lib/include/tb_client.h"),
    );

    build_step.dependOn(&install_header.step);

    inline for (platforms) |platform| {
        const cross_target = CrossTarget.parse(.{ .arch_os_abi = platform[0], .cpu_features = "baseline" }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(cross_target);

        const shared_lib = b.addSharedLibrary(.{
            .name = "tb_client",
            .root_source_file = b.path("src/tb_client_exports.zig"),
            .target = resolved_target,
            .optimize = mode,
        });
        const static_lib = b.addStaticLibrary(.{
            .name = "tb_client",
            .root_source_file = b.path("src/tb_client_exports.zig"),
            .target = resolved_target,
            .optimize = mode,
        });

        static_lib.bundle_compiler_rt = true;
        static_lib.pie = true;

        for ([_]*std.Build.Step.Compile{ shared_lib, static_lib }) |lib| {
            lib.linkLibC();

            if (resolved_target.result.os.tag == .windows) {
                lib.linkSystemLibrary("ws2_32");
                lib.linkSystemLibrary("advapi32");
            }

            lib.root_module.addOptions("vsr_options", options);

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
    target: std.Build.ResolvedTarget,
    dependencies: []const *std.Build.Step,
    options: *std.Build.Step.Options,
) void {
    const c_sample_build = b.step("c_sample", "Build the C client sample");
    for (dependencies) |dependency| {
        c_sample_build.dependOn(dependency);
    }

    const static_lib = b.addStaticLibrary(.{
        .name = "tb_client",
        .root_source_file = b.path("src/tb_client_exports.zig"),
        .target = target,
        .optimize = mode,
    });
    static_lib.linkLibC();
    static_lib.pie = true;
    static_lib.bundle_compiler_rt = true;
    static_lib.root_module.addOptions("vsr_options", options);
    c_sample_build.dependOn(&static_lib.step);

    const sample = b.addExecutable(.{
        .name = "c_sample",
        .target = target,
        .optimize = mode,
    });
    sample.addCSourceFile(.{
        .file = b.path("src/clients/c/samples/main.c"),
    });
    sample.linkLibrary(static_lib);
    sample.linkLibC();

    if (target.result.os.tag == .windows) {
        static_lib.linkSystemLibrary("ws2_32");
        static_lib.linkSystemLibrary("advapi32");

        // TODO: Illegal instruction on Windows:
        sample.root_module.sanitize_c = false;
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
    step: std.Build.Step,
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

    fn make(step: *std.Build.Step, _: std.Progress.Node) anyerror!void {
        const self: *FailStep = @fieldParentPtr("step", step);
        std.log.err("{s}", .{self.message});
        return error.FailStep;
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

const InstallFile = struct {
    step: std.Build.Step,
    source: std.Build.LazyPath,
    dest_path: []const u8,
    generated: std.Build.GeneratedFile,

    pub fn create(
        owner: *std.Build,
        source: std.Build.LazyPath,
        dest_path: []const u8,
    ) *InstallFile {
        assert(dest_path.len != 0);
        const install = owner.allocator.create(InstallFile) catch @panic("OOM");
        install.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = owner.fmt(
                    "install {s} to {s}",
                    .{ source.getDisplayName(), dest_path },
                ),
                .owner = owner,
                .makeFn = make,
            }),
            .source = source.dupe(owner),
            .dest_path = dest_path,
            .generated = .{
                .step = &install.step,
                .path = dest_path,
            },
        };
        source.addStepDependencies(&install.step);
        return install;
    }

    pub fn getDest(self: *InstallFile) std.Build.LazyPath {
        return .{ .generated = .{ .file = &self.generated } };
    }

    fn make(step: *std.Build.Step, prog_node: std.Progress.Node) !void {
        _ = prog_node;
        const b = step.owner;
        const install: *InstallFile = @fieldParentPtr("step", step);
        const full_src_path = install.source.getPath2(b, step);
        const cwd = std.fs.cwd();
        const prev = std.fs.Dir.updateFile(cwd, full_src_path, cwd, install.dest_path, .{}) catch |err| {
            return step.fail("unable to update file from '{s}' to '{s}': {s}", .{
                full_src_path, install.dest_path, @errorName(err),
            });
        };
        step.result_cached = prev == .fresh;
    }
};
