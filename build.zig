const std = @import("std");
const builtin = @import("builtin");
// NB: Don't import anything from `./src` to keep compile times low.

const assert = std.debug.assert;
const Query = std.Target.Query;

const VoprStateMachine = enum { testing, accounting };
const VoprLog = enum { short, full };

// TigerBeetle binary requires certain CPU feature and supports a closed set of CPUs. Here, we
// specify exactly which features the binary needs.
fn resolve_target(b: *std.Build, target_requested: ?[]const u8) !std.Build.ResolvedTarget {
    const target_host = @tagName(builtin.target.cpu.arch) ++ "-" ++ @tagName(builtin.target.os.tag);
    const target = target_requested orelse target_host;
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

    const arch_os, const cpu = inline for (triples, cpus) |triple, cpu| {
        if (std.mem.eql(u8, target, triple)) break .{ triple, cpu };
    } else {
        std.log.err("unsupported target: '{s}'", .{target});
        return error.UnsupportedTarget;
    };
    const query = try Query.parse(.{
        .arch_os_abi = arch_os,
        .cpu_features = cpu,
    });
    return b.resolveTargetQuery(query);
}

const zig_version = std.SemanticVersion{
    .major = 0,
    .minor = 14,
    .patch = 1,
};

comptime {
    // Compare versions while allowing different pre/patch metadata.
    const zig_version_eq = zig_version.major == builtin.zig_version.major and
        zig_version.minor == builtin.zig_version.minor and
        (zig_version.patch == builtin.zig_version.patch);
    if (!zig_version_eq) {
        @compileError(std.fmt.comptimePrint(
            "unsupported zig version: expected {}, found {}",
            .{ zig_version, builtin.zig_version },
        ));
    }
}

pub fn build(b: *std.Build) !void {
    // A compile error stack trace of 10 is arbitrary in size but helps with debugging.
    b.reference_trace = 10;

    // Top-level steps you can invoke on the command line.
    const build_steps = .{
        .aof = b.step("aof", "Run TigerBeetle AOF Utility"),
        .check = b.step("check", "Check if TigerBeetle compiles"),
        .clients_c = b.step("clients:c", "Build C client library"),
        .clients_c_sample = b.step("clients:c:sample", "Build C client sample"),
        .clients_dotnet = b.step("clients:dotnet", "Build dotnet client shared library"),
        .clients_rust = b.step("clients:rust", "Build Rust client shared library"),
        .clients_go = b.step("clients:go", "Build Go client shared library"),
        .clients_java = b.step("clients:java", "Build Java client shared library"),
        .clients_node = b.step("clients:node", "Build Node client shared library"),
        .clients_python = b.step("clients:python", "Build Python client library"),
        .docs = b.step("docs", "Build docs"),
        .fuzz = b.step("fuzz", "Run non-VOPR fuzzers"),
        .fuzz_build = b.step("fuzz:build", "Build non-VOPR fuzzers"),
        .run = b.step("run", "Run TigerBeetle"),
        .ci = b.step("ci", "Run the full suite of CI checks"),
        .scripts = b.step("scripts", "Free form automation scripts"),
        .scripts_build = b.step("scripts:build", "Build automation scripts"),
        .vortex = b.step("vortex", "Full system tests with pluggable client drivers"),
        .vortex_build = b.step("vortex:build", "Build the Vortex"),
        .@"test" = b.step("test", "Run all tests"),
        .test_fmt = b.step("test:fmt", "Check formatting"),
        .test_integration = b.step("test:integration", "Run integration tests"),
        .test_integration_build = b.step("test:integration:build", "Build integration tests"),
        .test_unit = b.step("test:unit", "Run unit tests"),
        .test_unit_build = b.step("test:unit:build", "Build unit tests"),
        .test_jni = b.step("test:jni", "Run Java JNI tests"),
        .vopr = b.step("vopr", "Run the VOPR"),
        .vopr_build = b.step("vopr:build", "Build the VOPR"),
    };

    const mode = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });

    // Build options passed with `-D` flags.
    const build_options = .{
        .target = b.option([]const u8, "target", "The CPU architecture and OS to build for"),
        .multiversion = b.option(
            []const u8,
            "multiversion",
            "Past version to include for upgrades (\"latest\" or \"x.y.z\")",
        ),
        .multiversion_file = b.option(
            []const u8,
            "multiversion-file",
            "Past version to include for upgrades (local binary file)",
        ),
        .config_verify = b.option(bool, "config_verify", "Enable extra assertions.") orelse
            // If `config_verify` isn't set, disable it for `release` builds; otherwise, enable it.
            (mode == .Debug),
        .config_aof_recovery = b.option(
            bool,
            "config-aof-recovery",
            "Enable AOF Recovery mode.",
        ) orelse false,
        .config_release = b.option([]const u8, "config-release", "Release triple."),
        .config_release_client_min = b.option(
            []const u8,
            "config-release-client-min",
            "Minimum client release triple.",
        ),
        .emit_llvm_ir = b.option(bool, "emit-llvm-ir", "Emit LLVM IR (.ll file)") orelse false,
        // The "tigerbeetle version" command includes the build-time commit hash.
        .git_commit = b.option(
            []const u8,
            "git-commit",
            "The git commit revision of the source code.",
        ) orelse std.mem.trimRight(u8, b.run(&.{ "git", "rev-parse", "--verify", "HEAD" }), "\n"),
        .vopr_state_machine = b.option(
            VoprStateMachine,
            "vopr-state-machine",
            "State machine.",
        ) orelse .accounting,
        .vopr_log = b.option(
            VoprLog,
            "vopr-log",
            "Log only state transitions (short) or everything (full).",
        ) orelse .short,
        .llvm_objcopy = b.option(
            []const u8,
            "llvm-objcopy",
            "Use this llvm-objcopy instead of downloading one",
        ),
        .print_exe = b.option(
            bool,
            "print-exe",
            "Build tasks print the path of the executable",
        ) orelse false,
    };

    const target = try resolve_target(b, build_options.target);
    const stdx_module = b.addModule("stdx", .{ .root_source_file = b.path("src/stdx/stdx.zig") });

    assert(build_options.git_commit.len == 40);
    const vsr_options, const vsr_module = build_vsr_module(b, .{
        .stdx_module = stdx_module,
        .git_commit = build_options.git_commit[0..40].*,
        .config_verify = build_options.config_verify,
        .config_release = build_options.config_release,
        .config_release_client_min = build_options.config_release_client_min,
        .config_aof_recovery = build_options.config_aof_recovery,
    });

    const tb_client_header = blk: {
        const tb_client_header_generator = b.addExecutable(.{
            .name = "tb_client_header",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/clients/c/tb_client_header.zig"),
                .target = b.graph.host,
            }),
        });
        tb_client_header_generator.root_module.addImport("vsr", vsr_module);
        tb_client_header_generator.root_module.addOptions("vsr_options", vsr_options);
        break :blk Generated.file(b, .{
            .generator = tb_client_header_generator,
            .path = "./src/clients/c/tb_client.h",
        });
    };

    // zig build check
    build_check(b, build_steps.check, .{
        .stdx_module = stdx_module,
        .vsr_module = vsr_module,
        .target = target,
        .mode = mode,
    });

    // zig build, zig build run
    build_tigerbeetle(b, .{
        .run = build_steps.run,
        .install = b.getInstallStep(),
    }, .{
        .stdx_module = stdx_module,
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .llvm_objcopy = build_options.llvm_objcopy,
        .target = target,
        .mode = mode,
        .emit_llvm_ir = build_options.emit_llvm_ir,
        .multiversion = build_options.multiversion,
        .multiversion_file = build_options.multiversion_file,
    });

    // zig build aof
    build_aof(b, build_steps.aof, .{
        .stdx_module = stdx_module,
        .vsr_options = vsr_options,
        .target = target,
        .mode = mode,
    });

    // zig build test -- "test filter"
    try build_test(b, .{
        .test_unit = build_steps.test_unit,
        .test_unit_build = build_steps.test_unit_build,
        .test_integration = build_steps.test_integration,
        .test_integration_build = build_steps.test_integration_build,
        .test_fmt = build_steps.test_fmt,
        .@"test" = build_steps.@"test",
    }, .{
        .stdx_module = stdx_module,
        .vsr_options = vsr_options,
        .llvm_objcopy = build_options.llvm_objcopy,
        .tb_client_header = tb_client_header,
        .target = target,
        .mode = mode,
    });

    // zig build test:jni
    try build_test_jni(b, build_steps.test_jni, .{
        .target = target,
        .mode = mode,
    });

    // zig build vopr -- 42
    build_vopr(b, .{
        .vopr_build = build_steps.vopr_build,
        .vopr_run = build_steps.vopr,
    }, .{
        .stdx_module = stdx_module,
        .vsr_options = vsr_options,
        .target = target,
        .mode = mode,
        .print_exe = build_options.print_exe,
        .vopr_state_machine = build_options.vopr_state_machine,
        .vopr_log = build_options.vopr_log,
    });

    // zig build fuzz -- --events-max=100 lsm_tree 123
    build_fuzz(b, .{
        .fuzz = build_steps.fuzz,
        .fuzz_build = build_steps.fuzz_build,
    }, .{
        .stdx_module = stdx_module,
        .vsr_options = vsr_options,
        .target = target,
        .mode = mode,
        .print_exe = build_options.print_exe,
    });

    // zig build scripts -- ci --language=java
    const scripts = build_scripts(b, .{
        .scripts = build_steps.scripts,
        .scripts_build = build_steps.scripts_build,
    }, .{
        .stdx_module = stdx_module,
        .vsr_options = vsr_options,
        .target = target,
    });

    // zig build vortex
    build_vortex(b, .{
        .vortex_build = build_steps.vortex_build,
        .vortex_run = build_steps.vortex,
    }, .{
        .stdx_module = stdx_module,
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .target = target,
        .mode = mode,
        .tb_client_header = tb_client_header.path,
        .print_exe = build_options.print_exe,
    });

    // zig build clients:$lang
    build_rust_client(b, build_steps.clients_rust, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .tb_client_header = tb_client_header.path,
        .mode = mode,
    });
    build_go_client(b, build_steps.clients_go, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .tb_client_header = tb_client_header.path,
        .mode = mode,
    });
    build_java_client(b, build_steps.clients_java, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .mode = mode,
    });
    build_dotnet_client(b, build_steps.clients_dotnet, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .mode = mode,
    });
    build_node_client(b, build_steps.clients_node, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .mode = mode,
    });
    build_python_client(b, build_steps.clients_python, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .tb_client_header = tb_client_header.path,
        .mode = mode,
    });
    build_c_client(b, build_steps.clients_c, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .tb_client_header = tb_client_header,
        .mode = mode,
    });

    // zig build clients:c:sample
    build_clients_c_sample(b, build_steps.clients_c_sample, .{
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .target = target,
        .mode = mode,
    });

    // zig build docs
    build_steps.docs.dependOn(blk: {
        const nested_build = b.addSystemCommand(&.{ b.graph.zig_exe, "build" });
        nested_build.setCwd(b.path("./src/docs_website/"));
        break :blk &nested_build.step;
    });

    // zig build ci
    build_ci(b, build_steps.ci, .{
        .scripts = scripts,
        .git_commit = build_options.git_commit,
    });
}

fn build_vsr_module(b: *std.Build, options: struct {
    stdx_module: *std.Build.Module,
    git_commit: [40]u8,
    config_verify: bool,
    config_release: ?[]const u8,
    config_release_client_min: ?[]const u8,
    config_aof_recovery: bool,
}) struct { *std.Build.Step.Options, *std.Build.Module } {
    // Ideally, we would return _just_ the module here, and keep options an implementation detail.
    // However, currently Zig makes it awkward to provide multiple entry points for a module:
    // https://ziggit.dev/t/suggested-project-layout-for-multiple-entry-point-for-zig-0-12/4219
    //
    // For this reason, we have to return options as well, so that other entry points can
    // essentially re-create identical module.
    const vsr_options = b.addOptions();
    vsr_options.addOption(?[40]u8, "git_commit", options.git_commit[0..40].*);
    vsr_options.addOption(bool, "config_verify", options.config_verify);
    vsr_options.addOption(?[]const u8, "release", options.config_release);
    vsr_options.addOption(
        ?[]const u8,
        "release_client_min",
        options.config_release_client_min,
    );
    vsr_options.addOption(bool, "config_aof_recovery", options.config_aof_recovery);

    const vsr_module = b.createModule(.{
        .root_source_file = b.path("src/vsr.zig"),
    });
    vsr_module.addImport("stdx", options.stdx_module);
    vsr_module.addOptions("vsr_options", vsr_options);

    return .{ vsr_options, vsr_module };
}

/// This is what is called by CI infrastructure, but you can also use it locally. In particular,
///
///     ./zig/zig build ci
///
/// is useful to run locally to get a set of somewhat comprehensive checks without needing many
/// external dependencies.
///
/// Various CI machines pass filters to select a subset of checks:
///
///     ./zig/zig build ci -- all
fn build_ci(
    b: *std.Build,
    step_ci: *std.Build.Step,
    options: struct {
        scripts: *std.Build.Step.Compile,
        git_commit: []const u8,
    },
) void {
    const CIMode = enum {
        smoke, // Quickly check formatting and such.
        @"test", // Main test suite, excluding VOPR and clients.
        fuzz, // Smoke tests for fuzzers and VOPR.
        aof, // Dedicated test for AOF, which is somewhat slow to run.

        clients, // Tests for all language clients below.
        dotnet,
        go,
        rust,
        java,
        node,
        python,

        devhub, // Things that run on known-good commit on main branch after merge.
        @"devhub-dry-run",
        amqp,
        default, // smoke + test + building Zig parts of clients.
        all,
    };

    const mode: CIMode = if (b.args) |args| mode: {
        if (args.len != 1) {
            step_ci.dependOn(&b.addFail("invalid CIMode").step);
            return;
        }
        if (std.meta.stringToEnum(CIMode, args[0])) |m| {
            break :mode m;
        } else {
            step_ci.dependOn(&b.addFail("invalid CIMode").step);
            return;
        }
    } else .default;

    const all = mode == .all;
    const default = all or mode == .default;

    if (default or mode == .smoke) {
        build_ci_step(b, step_ci, .{"test:fmt"});
        build_ci_step(b, step_ci, .{"check"});

        const build_docs = b.addSystemCommand(&.{ b.graph.zig_exe, "build" });
        build_docs.has_side_effects = true;
        build_docs.cwd = b.path("./src/docs_website");
        step_ci.dependOn(&build_docs.step);
    }
    if (default or mode == .@"test") {
        build_ci_step(b, step_ci, .{"test"});
        build_ci_step(b, step_ci, .{ "fuzz", "--", "smoke" });
        build_ci_step(b, step_ci, .{"clients:c:sample"});
        build_ci_script(b, step_ci, options.scripts, &.{"--help"});
    }
    if (default or mode == .fuzz) {
        build_ci_step(b, step_ci, .{ "fuzz", "--", "smoke" });
        inline for (.{ "testing", "accounting" }) |state_machine| {
            build_ci_step(b, step_ci, .{
                "vopr",
                "-Dvopr-state-machine=" ++ state_machine,
                "-Drelease",
                "--",
                options.git_commit,
            });
        }
    }
    if (default or mode == .amqp) {
        // Smoke test the AMQP integration.
        build_ci_script(b, step_ci, options.scripts, &.{
            "amqp",
            "--transfer-count=100",
        });
    }

    if (all or mode == .aof) {
        const aof = b.addSystemCommand(&.{"./.github/ci/test_aof.sh"});
        hide_stderr(aof);
        step_ci.dependOn(&aof.step);
    }
    inline for (&.{ CIMode.dotnet, .go, .rust, .java, .node, .python }) |language| {
        if (default or mode == .clients or mode == language) {
            // Client tests expect vortex to exist.
            build_ci_step(b, step_ci, .{"vortex:build"});
            build_ci_step(b, step_ci, .{"clients:" ++ @tagName(language)});
        }
        if (all or mode == .clients or mode == language) {
            build_ci_script(b, step_ci, options.scripts, &.{
                "ci",
                "--language=" ++ @tagName(language),
            });
        }
    }

    if (all or mode == .@"devhub-dry-run") {
        build_ci_script(b, step_ci, options.scripts, &.{
            "devhub",
            b.fmt("--sha={s}", .{options.git_commit}),
            "--skip-kcov",
        });
    }
    if (mode == .devhub) {
        build_ci_script(b, step_ci, options.scripts, &.{
            "devhub",
            b.fmt("--sha={s}", .{options.git_commit}),
        });
    }
}

fn build_ci_step(
    b: *std.Build,
    step_ci: *std.Build.Step,
    command: anytype,
) void {
    const argv = .{ b.graph.zig_exe, "build" } ++ command;
    const system_command = b.addSystemCommand(&argv);
    const name = std.mem.join(b.allocator, " ", &command) catch @panic("OOM");
    system_command.setName(name);
    hide_stderr(system_command);
    step_ci.dependOn(&system_command.step);
}

fn build_ci_script(
    b: *std.Build,
    step_ci: *std.Build.Step,
    scripts: *std.Build.Step.Compile,
    argv: []const []const u8,
) void {
    const run_artifact = b.addRunArtifact(scripts);
    run_artifact.addArgs(argv);
    run_artifact.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);
    hide_stderr(run_artifact);
    step_ci.dependOn(&run_artifact.step);
}

// Hide step's stderr unless it fails, to prevent zig build ci output being dominated by VOPR logs.
// Sadly, this requires "overriding" Build.Step.Run make function.
fn hide_stderr(run: *std.Build.Step.Run) void {
    const b = run.step.owner;

    run.addCheck(.{ .expect_term = .{ .Exited = 0 } });
    run.has_side_effects = true;

    const override = struct {
        var global_map: std.AutoHashMapUnmanaged(usize, std.Build.Step.MakeFn) = .{};

        fn make(step: *std.Build.Step, options: std.Build.Step.MakeOptions) anyerror!void {
            const original = global_map.get(@intFromPtr(step)).?;
            try original(step, options);
            assert(step.result_error_msgs.items.len == 0);
            step.result_stderr = "";
        }
    };

    const original = run.step.makeFn;
    override.global_map.put(b.allocator, @intFromPtr(&run.step), original) catch @panic("OOM");
    run.step.makeFn = &override.make;
}

// Run a tigerbeetle build without running codegen and waiting for llvm
// see <https://github.com/ziglang/zig/commit/5c0181841081170a118d8e50af2a09f5006f59e1>
// how it's supposed to work.
// In short, codegen only runs if zig build sees a dependency on the binary output of
// the step. So we duplicate the build definition so that it doesn't get polluted by
// b.installArtifact.
// TODO(zig): https://github.com/ziglang/zig/issues/18877
fn build_check(
    b: *std.Build,
    step_check: *std.Build.Step,
    options: struct {
        stdx_module: *std.Build.Module,
        vsr_module: *std.Build.Module,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const tigerbeetle = b.addExecutable(.{
        .name = "tigerbeetle",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/main.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    tigerbeetle.root_module.addImport("stdx", options.stdx_module);
    tigerbeetle.root_module.addImport("vsr", options.vsr_module);
    step_check.dependOn(&tigerbeetle.step);
}

fn build_tigerbeetle(
    b: *std.Build,
    steps: struct {
        run: *std.Build.Step,
        install: *std.Build.Step,
    },
    options: struct {
        stdx_module: *std.Build.Module,
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        llvm_objcopy: ?[]const u8,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
        multiversion: ?[]const u8,
        multiversion_file: ?[]const u8,
        emit_llvm_ir: bool,
    },
) void {
    const multiversion_file: ?std.Build.LazyPath = if (options.multiversion_file) |path|
        .{ .cwd_relative = path }
    else if (options.multiversion) |version_past|
        download_release(b, version_past, options.target, options.mode)
    else
        null;

    const tigerbeetle_bin = if (multiversion_file) |multiversion_lazy_path| bin: {
        assert(!options.emit_llvm_ir);
        break :bin build_tigerbeetle_executable_multiversion(b, .{
            .stdx_module = options.stdx_module,
            .vsr_module = options.vsr_module,
            .vsr_options = options.vsr_options,
            .llvm_objcopy = options.llvm_objcopy,
            .tigerbeetle_previous = multiversion_lazy_path,
            .target = options.target,
            .mode = options.mode,
        });
    } else bin: {
        const tigerbeetle_exe = build_tigerbeetle_executable(b, .{
            .vsr_module = options.vsr_module,
            .vsr_options = options.vsr_options,
            .target = options.target,
            .mode = options.mode,
        });
        if (options.emit_llvm_ir) {
            steps.install.dependOn(&b.addInstallBinFile(
                tigerbeetle_exe.getEmittedLlvmIr(),
                "tigerbeetle.ll",
            ).step);
        }
        break :bin tigerbeetle_exe.getEmittedBin();
    };

    const out_filename = if (options.target.result.os.tag == .windows)
        "tigerbeetle.exe"
    else
        "tigerbeetle";

    steps.install.dependOn(&b.addInstallBinFile(tigerbeetle_bin, out_filename).step);
    // "zig build install" moves the server executable to the root folder:
    steps.install.dependOn(&b.addInstallFile(
        tigerbeetle_bin,
        b.pathJoin(&.{ "../", out_filename }),
    ).step);

    const run_cmd = std.Build.Step.Run.create(b, b.fmt("run tigerbeetle", .{}));
    run_cmd.addFileArg(tigerbeetle_bin);
    if (b.args) |args| run_cmd.addArgs(args);
    steps.run.dependOn(&run_cmd.step);
}

fn build_tigerbeetle_executable(b: *std.Build, options: struct {
    vsr_module: *std.Build.Module,
    vsr_options: *std.Build.Step.Options,
    target: std.Build.ResolvedTarget,
    mode: std.builtin.OptimizeMode,
}) *std.Build.Step.Compile {
    const root_module = b.createModule(.{
        .root_source_file = b.path("src/tigerbeetle/main.zig"),
        .target = options.target,
        .optimize = options.mode,
    });
    root_module.addImport("vsr", options.vsr_module);
    root_module.addOptions("vsr_options", options.vsr_options);
    if (options.mode == .ReleaseSafe) strip_root_module(root_module);

    const tigerbeetle = b.addExecutable(.{
        .name = "tigerbeetle",
        .root_module = root_module,
    });

    return tigerbeetle;
}

fn build_tigerbeetle_executable_multiversion(b: *std.Build, options: struct {
    stdx_module: *std.Build.Module,
    vsr_module: *std.Build.Module,
    vsr_options: *std.Build.Step.Options,
    llvm_objcopy: ?[]const u8,
    tigerbeetle_previous: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    mode: std.builtin.OptimizeMode,
}) std.Build.LazyPath {
    // build_multiversion a custom step that would take care of packing several releases into one
    const build_multiversion_exe = b.addExecutable(.{
        .name = "build_multiversion",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/build_multiversion.zig"),
            // Enable aes extensions for vsr.checksum on the host.
            .target = resolve_target(b, null) catch @panic("unsupported host"),
        }),
    });
    build_multiversion_exe.root_module.addImport("stdx", options.stdx_module);
    // Ideally, we should pass `vsr_options` here at runtime. Making them comptime
    // parameters is inelegant, but practical!
    build_multiversion_exe.root_module.addOptions("vsr_options", options.vsr_options);

    const build_multiversion = b.addRunArtifact(build_multiversion_exe);
    if (options.llvm_objcopy) |path| {
        build_multiversion.addArg(b.fmt("--llvm-objcopy={s}", .{path}));
    } else {
        build_multiversion.addPrefixedFileArg(
            "--llvm-objcopy=",
            build_tigerbeetle_executable_get_objcopy(b),
        );
    }
    if (options.target.result.os.tag == .macos) {
        build_multiversion.addArg("--target=macos");
        inline for (.{ "x86_64", "aarch64" }, .{ "x86-64", "aarch64" }) |arch, flag| {
            build_multiversion.addPrefixedFileArg(
                "--tigerbeetle-current-" ++ flag ++ "=",
                build_tigerbeetle_executable(b, .{
                    .vsr_module = options.vsr_module,
                    .vsr_options = options.vsr_options,
                    .target = resolve_target(b, arch ++ "-macos") catch unreachable,
                    .mode = options.mode,
                }).getEmittedBin(),
            );
        }
    } else {
        build_multiversion.addArg(b.fmt("--target={s}-{s}", .{
            @tagName(options.target.result.cpu.arch),
            @tagName(options.target.result.os.tag),
        }));
        build_multiversion.addPrefixedFileArg(
            "--tigerbeetle-current=",
            build_tigerbeetle_executable(b, .{
                .vsr_module = options.vsr_module,
                .vsr_options = options.vsr_options,
                .target = options.target,
                .mode = options.mode,
            }).getEmittedBin(),
        );
    }

    if (options.mode == .Debug) {
        build_multiversion.addArg("--debug");
    }

    build_multiversion.addPrefixedFileArg("--tigerbeetle-past=", options.tigerbeetle_previous);
    build_multiversion.addArg(b.fmt(
        "--tmp={s}",
        .{b.cache_root.join(b.allocator, &.{"tmp"}) catch @panic("OOM")},
    ));
    const basename = if (options.target.result.os.tag == .windows)
        "tigerbeetle.exe"
    else
        "tigerbeetle";
    return build_multiversion.addPrefixedOutputFileArg("--output=", basename);
}

// Downloads a pre-build llvm-objcopy from <https://github.com/tigerbeetle/dependencies>.
fn build_tigerbeetle_executable_get_objcopy(b: *std.Build) std.Build.LazyPath {
    switch (b.graph.host.result.os.tag) {
        .linux => {
            switch (b.graph.host.result.cpu.arch) {
                .x86_64 => {
                    return fetch(b, .{
                        .url = "https://github.com/tigerbeetle/dependencies/releases/download/" ++
                            "18.1.8/llvm-objcopy-x86_64-linux.zip",
                        .file_name = "llvm-objcopy",
                        .hash = "N-V-__8AAFCWcgAxBPUOMe_uJrFGfQ2Ri_SsbNp77pPYZdAe",
                    });
                },
                .aarch64 => {
                    return fetch(b, .{
                        .url = "https://github.com/tigerbeetle/dependencies/releases/download/" ++
                            "18.1.8/llvm-objcopy-aarch64-linux.zip",
                        .file_name = "llvm-objcopy",
                        .hash = "N-V-__8AAIgJcQAG--KvT2zb1yNrlRtNEo3pW3aIgoppmbT-",
                    });
                },
                else => @panic("unsupported arch"),
            }
        },
        .windows => {
            assert(b.graph.host.result.cpu.arch == .x86_64);
            return fetch(b, .{
                .url = "https://github.com/tigerbeetle/dependencies/releases/download/" ++
                    "18.1.8/llvm-objcopy-x86_64-windows.zip",
                .file_name = "llvm-objcopy.exe",
                .hash = "N-V-__8AAADuPABpdHRgl3oetSEQ6yq8i5kq9XJC73JDFtMH",
            });
        },
        .macos => {
            // TODO: this assert triggers, but the macOS tests on x86_64 work...?
            // assert(b.graph.host.result.cpu.arch == .aarch64);
            return fetch(b, .{
                .url = "https://github.com/tigerbeetle/dependencies/releases/download/" ++
                    "18.1.8/llvm-objcopy-aarch64-macos.zip",
                .file_name = "llvm-objcopy",
                .hash = "N-V-__8AAFAsVgArdRpU50gjJhqaAUSXsTemKo2A9rCaewUV",
            });
        },
        else => @panic("unsupported host"),
    }
}

fn build_aof(
    b: *std.Build,
    step_aof: *std.Build.Step,
    options: struct {
        stdx_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const aof = b.addExecutable(.{
        .name = "aof",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/aof.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    aof.root_module.addImport("stdx", options.stdx_module);
    aof.root_module.addOptions("vsr_options", options.vsr_options);
    const run_cmd = b.addRunArtifact(aof);
    if (b.args) |args| run_cmd.addArgs(args);
    step_aof.dependOn(&run_cmd.step);
}

fn build_test(
    b: *std.Build,
    steps: struct {
        test_unit: *std.Build.Step,
        test_unit_build: *std.Build.Step,
        test_integration: *std.Build.Step,
        test_integration_build: *std.Build.Step,
        test_fmt: *std.Build.Step,
        @"test": *std.Build.Step,
    },
    options: struct {
        llvm_objcopy: ?[]const u8,
        stdx_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        tb_client_header: *Generated,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) !void {
    const test_options = b.addOptions();
    // Benchmark run in two modes.
    // - ./zig/zig build test
    // - ./zig/zig build -Drelease test -- "benchmark: name"
    // The former uses small parameter values and is silent.
    // The latter is the real benchmark, which prints the output.
    test_options.addOption(bool, "benchmark", for (b.args orelse &.{}) |arg| {
        if (std.mem.indexOf(u8, arg, "benchmark") != null) break true;
    } else false);

    const stdx_unit_tests = b.addTest(.{
        .name = "test-stdx",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/stdx/stdx.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
        .filters = b.args orelse &.{},
    });
    const unit_tests = b.addTest(.{
        .name = "test-unit",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/unit_tests.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
        .filters = b.args orelse &.{},
    });
    unit_tests.root_module.addImport("stdx", options.stdx_module);
    unit_tests.root_module.addOptions("vsr_options", options.vsr_options);
    unit_tests.root_module.addOptions("test_options", test_options);

    steps.test_unit_build.dependOn(&b.addInstallArtifact(stdx_unit_tests, .{}).step);
    steps.test_unit_build.dependOn(&b.addInstallArtifact(unit_tests, .{}).step);

    const run_stdx_unit_tests = b.addRunArtifact(stdx_unit_tests);
    const run_unit_tests = b.addRunArtifact(unit_tests);
    run_stdx_unit_tests.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);
    run_unit_tests.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);
    if (b.args != null) { // Don't cache test results if running a specific test.
        run_stdx_unit_tests.has_side_effects = true;
        run_unit_tests.has_side_effects = true;
    }
    steps.test_unit.dependOn(&run_stdx_unit_tests.step);
    steps.test_unit.dependOn(&run_unit_tests.step);

    run_unit_tests.setCwd(b.path("."));

    build_test_integration(b, .{
        .test_integration = steps.test_integration,
        .test_integration_build = steps.test_integration_build,
    }, .{
        .tb_client_header = options.tb_client_header.path,
        .llvm_objcopy = options.llvm_objcopy,
        .stdx_module = options.stdx_module,
        .target = options.target,
        .mode = options.mode,
    });

    const run_fmt = b.addFmt(.{ .paths = &.{"."}, .check = true });
    steps.test_fmt.dependOn(&run_fmt.step);

    steps.@"test".dependOn(&run_stdx_unit_tests.step);
    steps.@"test".dependOn(&run_unit_tests.step);
    if (b.args == null) {
        steps.@"test".dependOn(steps.test_integration);
        steps.@"test".dependOn(steps.test_fmt);
    }
}

fn build_test_integration(
    b: *std.Build,
    steps: struct {
        test_integration: *std.Build.Step,
        test_integration_build: *std.Build.Step,
    },
    options: struct {
        tb_client_header: std.Build.LazyPath,
        llvm_objcopy: ?[]const u8,
        stdx_module: *std.Build.Module,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) void {
    // For integration tests, we build an independent copy of TigerBeetle with "real" config and
    // multiversioning.
    const vsr_options, const vsr_module = build_vsr_module(b, .{
        .stdx_module = options.stdx_module,
        .git_commit = "bee71e0000000000000000000000000000bee71e".*, // Beetle-hash!
        .config_verify = true,
        .config_release = "65535.0.0",
        .config_release_client_min = "0.16.4",
        .config_aof_recovery = false,
    });
    const tigerbeetle_previous = download_release(b, "latest", options.target, options.mode);
    const tigerbeetle = build_tigerbeetle_executable_multiversion(b, .{
        .stdx_module = options.stdx_module,
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .llvm_objcopy = options.llvm_objcopy,
        .tigerbeetle_previous = tigerbeetle_previous,
        .target = options.target,
        .mode = options.mode,
    });

    const vortex = build_vortex_executable(b, .{
        .tb_client_header = options.tb_client_header,
        .stdx_module = options.stdx_module,
        .vsr_module = vsr_module,
        .vsr_options = vsr_options,
        .target = options.target,
        .mode = options.mode,
    });
    const vortex_artifact = b.addInstallArtifact(vortex, .{});

    const integration_tests_options = b.addOptions();
    integration_tests_options.addOptionPath("tigerbeetle_exe", tigerbeetle);
    integration_tests_options.addOptionPath("tigerbeetle_exe_past", tigerbeetle_previous);
    integration_tests_options.addOptionPath("vortex_exe", vortex_artifact.emitted_bin.?);
    const integration_tests = b.addTest(.{
        .name = "test-integration",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration_tests.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
        .filters = b.args orelse &.{},
    });
    integration_tests.root_module.addImport("stdx", options.stdx_module);
    integration_tests.root_module.addOptions("vsr_options", vsr_options);
    integration_tests.root_module.addOptions("test_options", integration_tests_options);
    integration_tests.addIncludePath(options.tb_client_header.dirname());
    steps.test_integration_build.dependOn(&b.addInstallArtifact(integration_tests, .{}).step);

    const run_integration_tests = b.addRunArtifact(integration_tests);
    if (b.args != null) { // Don't cache test results if running a specific test.
        run_integration_tests.has_side_effects = true;
    }
    run_integration_tests.has_side_effects = true;
    steps.test_integration.dependOn(&run_integration_tests.step);
}

fn build_test_jni(
    b: *std.Build,
    step_test_jni: *std.Build.Step,
    options: struct {
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) !void {
    const java_home = b.graph.env_map.get("JAVA_HOME") orelse {
        step_test_jni.dependOn(&b.addFail(
            "can't build jni tests tests, JAVA_HOME is not set",
        ).step);
        return;
    };

    // JNI test require JVM to be present, and are _not_ run as a part of `zig build test`.
    // We need libjvm.so both at build time and at a runtime, so use `FailStep` when that is not
    // available.
    const libjvm_path = b.pathJoin(&.{
        java_home,
        if (builtin.os.tag == .windows) "/lib" else "/lib/server",
    });

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/java/src/jni_tests.zig"),
            .target = options.target,
            // TODO(zig): The function `JNI_CreateJavaVM` tries to detect
            // the stack size and causes a SEGV that is handled by Zig's panic handler.
            // https://bugzilla.redhat.com/show_bug.cgi?id=1572811#c7
            //
            // The workaround is to run the tests in "ReleaseFast" mode.
            .optimize = if (builtin.os.tag == .windows) .ReleaseFast else options.mode,
        }),
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

        if (std.mem.indexOf(u8, ldd_result, "musl") != null) {
            tests.root_module.resolved_target.?.query.abi = .musl;
            tests.root_module.resolved_target.?.result.abi = .musl;
        } else if (std.mem.indexOf(u8, ldd_result, "libc") != null) {
            tests.root_module.resolved_target.?.query.abi = .gnu;
            tests.root_module.resolved_target.?.result.abi = .gnu;
        } else {
            std.log.err("{s}", .{ldd_result});
            return error.JavaAbiUnrecognized;
        }
    }

    switch (builtin.os.tag) {
        .windows => set_windows_dll(b.allocator, java_home),
        .macos => try b.graph.env_map.put("DYLD_LIBRARY_PATH", libjvm_path),
        .linux => try b.graph.env_map.put("LD_LIBRARY_PATH", libjvm_path),
        else => unreachable,
    }

    step_test_jni.dependOn(&b.addRunArtifact(tests).step);
}

fn build_vopr(
    b: *std.Build,
    steps: struct {
        vopr_build: *std.Build.Step,
        vopr_run: *std.Build.Step,
    },
    options: struct {
        stdx_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
        print_exe: bool,
        vopr_state_machine: VoprStateMachine,
        vopr_log: VoprLog,
    },
) void {
    const vopr_options = b.addOptions();

    vopr_options.addOption(VoprStateMachine, "state_machine", options.vopr_state_machine);
    vopr_options.addOption(VoprLog, "log", options.vopr_log);

    const vopr = b.addExecutable(.{
        .name = "vopr",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/vopr.zig"),
            .target = options.target,
            // When running without a SEED, default to release.
            .optimize = if (b.args == null) .ReleaseSafe else options.mode,
        }),
    });
    vopr.stack_size = 4 * MiB;
    vopr.root_module.addImport("stdx", options.stdx_module);
    vopr.root_module.addOptions("vsr_options", options.vsr_options);
    vopr.root_module.addOptions("vsr_vopr_options", vopr_options);
    // Ensure that we get stack traces even in release builds.
    vopr.root_module.omit_frame_pointer = false;
    steps.vopr_build.dependOn(print_or_install(b, vopr, options.print_exe));

    const run_cmd = b.addRunArtifact(vopr);
    if (b.args) |args| run_cmd.addArgs(args);
    steps.vopr_run.dependOn(&run_cmd.step);
}

fn build_fuzz(
    b: *std.Build,
    steps: struct {
        fuzz: *std.Build.Step,
        fuzz_build: *std.Build.Step,
    },
    options: struct {
        stdx_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
        print_exe: bool,
    },
) void {
    const fuzz_exe = b.addExecutable(.{
        .name = "fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz_tests.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    fuzz_exe.stack_size = 4 * MiB;
    fuzz_exe.root_module.addImport("stdx", options.stdx_module);
    fuzz_exe.root_module.addOptions("vsr_options", options.vsr_options);
    fuzz_exe.root_module.omit_frame_pointer = false;
    steps.fuzz_build.dependOn(print_or_install(b, fuzz_exe, options.print_exe));

    const fuzz_run = b.addRunArtifact(fuzz_exe);
    if (b.args) |args| fuzz_run.addArgs(args);
    steps.fuzz.dependOn(&fuzz_run.step);
}

fn build_scripts(
    b: *std.Build,
    steps: struct {
        scripts: *std.Build.Step,
        scripts_build: *std.Build.Step,
    },
    options: struct {
        stdx_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
    },
) *std.Build.Step.Compile {
    const scripts_exe = b.addExecutable(.{
        .name = "scripts",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/scripts.zig"),
            .target = options.target,
            .optimize = .Debug,
        }),
    });
    scripts_exe.root_module.addImport("stdx", options.stdx_module);
    scripts_exe.root_module.addOptions("vsr_options", options.vsr_options);
    steps.scripts_build.dependOn(
        &b.addInstallArtifact(scripts_exe, .{}).step,
    );

    const scripts_run = b.addRunArtifact(scripts_exe);
    scripts_run.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);
    if (b.args) |args| scripts_run.addArgs(args);
    steps.scripts.dependOn(&scripts_run.step);

    return scripts_exe;
}

fn build_vortex(
    b: *std.Build,
    steps: struct {
        vortex_build: *std.Build.Step,
        vortex_run: *std.Build.Step,
    },
    options: struct {
        tb_client_header: std.Build.LazyPath,
        stdx_module: *std.Build.Module,
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
        print_exe: bool,
    },
) void {
    const vortex = build_vortex_executable(b, .{
        .tb_client_header = options.tb_client_header,
        .stdx_module = options.stdx_module,
        .vsr_module = options.vsr_module,
        .vsr_options = options.vsr_options,
        .target = options.target,
        .mode = options.mode,
    });

    const install_step = print_or_install(b, vortex, options.print_exe);
    steps.vortex_build.dependOn(install_step);

    const run_cmd = b.addRunArtifact(vortex);
    if (b.args) |args| run_cmd.addArgs(args);
    steps.vortex_run.dependOn(&run_cmd.step);
}

fn build_vortex_executable(
    b: *std.Build,
    options: struct {
        tb_client_header: std.Build.LazyPath,
        stdx_module: *std.Build.Module,
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) *std.Build.Step.Compile {
    const tb_client = b.addLibrary(.{
        .name = "tb_client",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    tb_client.linkLibC();
    tb_client.pie = true;
    tb_client.bundle_compiler_rt = true;
    tb_client.root_module.addImport("vsr", options.vsr_module);
    tb_client.root_module.addOptions("vsr_options", options.vsr_options);
    if (options.target.result.os.tag == .windows) {
        tb_client.linkSystemLibrary("ws2_32");
        tb_client.linkSystemLibrary("advapi32");
    }

    const tigerbeetle = build_tigerbeetle_executable(b, .{
        .vsr_module = options.vsr_module,
        .vsr_options = options.vsr_options,
        .target = options.target,
        .mode = options.mode,
    });

    const vortex_options = b.addOptions();
    vortex_options.addOptionPath("tigerbeetle_exe", tigerbeetle.getEmittedBin());

    const vortex = b.addExecutable(.{
        .name = "vortex",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/vortex.zig"),
            .omit_frame_pointer = false,
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    vortex.root_module.addImport("stdx", options.stdx_module);
    vortex.linkLibC();
    vortex.linkLibrary(tb_client);
    vortex.addIncludePath(options.tb_client_header.dirname());
    vortex.root_module.addOptions("vsr_options", options.vsr_options);
    vortex.root_module.addOptions("vortex_options", vortex_options);
    return vortex;
}

// Zig cross-targets, Dotnet RID (Runtime Identifier), CPU features.
const platforms = .{
    .{ "x86_64-linux-gnu.2.27", "linux-x64", "x86_64_v3+aes" },
    .{ "x86_64-linux-musl", "linux-musl-x64", "x86_64_v3+aes" },
    .{ "x86_64-macos", "osx-x64", "x86_64_v3+aes" },
    .{ "aarch64-linux-gnu.2.27", "linux-arm64", "baseline+aes+neon" },
    .{ "aarch64-linux-musl", "linux-musl-arm64", "baseline+aes+neon" },
    .{ "aarch64-macos", "osx-arm64", "baseline+aes+neon" },
    .{ "x86_64-windows", "win-x64", "x86_64_v3+aes" },
};

fn strip_glibc_version(triple: []const u8) []const u8 {
    if (std.mem.endsWith(u8, triple, "gnu.2.27")) {
        return triple[0 .. triple.len - ".2.27".len];
    }
    assert(std.mem.indexOf(u8, triple, "gnu") == null);
    return triple;
}

fn build_rust_client(
    b: *std.Build,
    step_clients_rust: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        tb_client_header: std.Build.LazyPath,
        mode: std.builtin.OptimizeMode,
    },
) void {
    // The Rust test suite runs tigerbeetle directly. This ensures it is available.
    step_clients_rust.dependOn(b.getInstallStep());

    // Copy the generated header file to the Rust client assets directory:
    const tb_client_header_copy = Generated.file_copy(b, .{
        .from = options.tb_client_header,
        .path = "./src/clients/rust/assets/tb_client.h",
    });
    step_clients_rust.dependOn(&tb_client_header_copy.step);

    inline for (platforms) |platform| {
        const query = Query.parse(.{
            .arch_os_abi = platform[0],
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = resolved_target,
            .optimize = options.mode,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const static_lib = b.addLibrary(.{
            .name = "tb_client",
            .linkage = .static,
            .root_module = root_module,
        });
        static_lib.bundle_compiler_rt = true;
        static_lib.pie = true;
        static_lib.linkLibC();

        step_clients_rust.dependOn(&b.addInstallFile(static_lib.getEmittedBin(), b.pathJoin(&.{
            "../src/clients/rust/assets/lib/",
            platform[0],
            static_lib.out_filename,
        })).step);
    }

    const rust_bindings_generator = b.addExecutable(.{
        .name = "rust_bindings",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/rust/rust_bindings.zig"),
            .target = b.graph.host,
        }),
    });
    rust_bindings_generator.root_module.addImport("vsr", options.vsr_module);
    rust_bindings_generator.root_module.addOptions("vsr_options", options.vsr_options);
    const bindings = Generated.file(b, .{
        .generator = rust_bindings_generator,
        .path = "./src/clients/rust/src/tb_client.rs",
    });

    step_clients_rust.dependOn(&bindings.step);
}

fn build_go_client(
    b: *std.Build,
    step_clients_go: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        tb_client_header: std.Build.LazyPath,
        mode: std.builtin.OptimizeMode,
    },
) void {
    // Updates the generated header file:
    const tb_client_header_copy = Generated.file_copy(b, .{
        .from = options.tb_client_header,
        .path = "./src/clients/go/pkg/native/tb_client.h",
    });

    const go_bindings_generator = b.addExecutable(.{
        .name = "go_bindings",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/go/go_bindings.zig"),
            .target = b.graph.host,
        }),
    });
    go_bindings_generator.root_module.addImport("vsr", options.vsr_module);
    go_bindings_generator.root_module.addOptions("vsr_options", options.vsr_options);
    go_bindings_generator.step.dependOn(&tb_client_header_copy.step);
    const bindings = Generated.file(b, .{
        .generator = go_bindings_generator,
        .path = "./src/clients/go/pkg/types/bindings.go",
    });

    inline for (platforms) |platform| {
        // We don't need the linux-gnu builds.
        if (comptime std.mem.indexOf(u8, platform[0], "linux-gnu") != null) continue;

        const platform_name = if (comptime std.mem.eql(u8, platform[0], "x86_64-linux-musl"))
            "x86_64-linux"
        else if (comptime std.mem.eql(u8, platform[0], "aarch64-linux-musl"))
            "aarch64-linux"
        else
            platform[0];

        const query = Query.parse(.{
            .arch_os_abi = platform_name,
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = resolved_target,
            .optimize = options.mode,
            .stack_protector = false,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const lib = b.addLibrary(.{
            .name = "tb_client",
            .linkage = .static,
            .root_module = root_module,
        });
        lib.linkLibC();
        lib.pie = true;
        lib.bundle_compiler_rt = true;
        lib.step.dependOn(&bindings.step);

        const file_name: []const u8, const extension: []const u8 = cut: {
            assert(std.mem.count(u8, lib.out_lib_filename, ".") == 1);
            var it = std.mem.splitScalar(u8, lib.out_lib_filename, '.');
            defer assert(it.next() == null);
            break :cut .{ it.next().?, it.next().? };
        };

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/.
        step_clients_go.dependOn(&b.addInstallFile(
            lib.getEmittedBin(),
            b.fmt("../src/clients/go/pkg/native/{s}_{s}.{s}", .{
                file_name,
                platform_name,
                extension,
            }),
        ).step);
    }
}

fn build_java_client(
    b: *std.Build,
    step_clients_java: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const java_bindings_generator = b.addExecutable(.{
        .name = "java_bindings",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/java/java_bindings.zig"),
            .target = b.graph.host,
        }),
    });
    java_bindings_generator.root_module.addImport("vsr", options.vsr_module);
    java_bindings_generator.root_module.addOptions("vsr_options", options.vsr_options);
    const bindings = Generated.directory(b, .{
        .generator = java_bindings_generator,
        .path = "./src/clients/java/src/main/java/com/tigerbeetle/",
    });

    inline for (platforms) |platform| {
        const query = Query.parse(.{
            .arch_os_abi = platform[0],
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/java/src/client.zig"),
            .target = resolved_target,
            .optimize = options.mode,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const lib = b.addLibrary(.{
            .name = "tb_jniclient",
            .linkage = .dynamic,
            .root_module = root_module,
        });
        lib.linkLibC();
        if (resolved_target.result.os.tag == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }
        lib.step.dependOn(&bindings.step);

        // NB: New way to do lib.setOutputDir(). The ../ is important to escape zig-cache/.
        step_clients_java.dependOn(&b.addInstallFile(lib.getEmittedBin(), b.pathJoin(&.{
            "../src/clients/java/src/main/resources/lib/",
            strip_glibc_version(platform[0]),
            lib.out_filename,
        })).step);
    }
}

fn build_dotnet_client(
    b: *std.Build,
    step_clients_dotnet: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const dotnet_bindings_generator = b.addExecutable(.{
        .name = "dotnet_bindings",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/dotnet/dotnet_bindings.zig"),
            .target = b.graph.host,
        }),
    });
    dotnet_bindings_generator.root_module.addImport("vsr", options.vsr_module);
    dotnet_bindings_generator.root_module.addOptions("vsr_options", options.vsr_options);
    const bindings = Generated.file(b, .{
        .generator = dotnet_bindings_generator,
        .path = "./src/clients/dotnet/TigerBeetle/Bindings.cs",
    });

    inline for (platforms) |platform| {
        const query = Query.parse(.{
            .arch_os_abi = platform[0],
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = resolved_target,
            .optimize = options.mode,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const lib = b.addLibrary(.{
            .name = "tb_client",
            .linkage = .dynamic,
            .root_module = root_module,
        });
        lib.linkLibC();
        if (resolved_target.result.os.tag == .windows) {
            lib.linkSystemLibrary("ws2_32");
            lib.linkSystemLibrary("advapi32");
        }
        lib.step.dependOn(&bindings.step);

        step_clients_dotnet.dependOn(&b.addInstallFile(lib.getEmittedBin(), b.pathJoin(&.{
            "../src/clients/dotnet/TigerBeetle/runtimes/",
            platform[1],
            "native",
            lib.out_filename,
        })).step);
    }
}

fn build_node_client(
    b: *std.Build,
    step_clients_node: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const node_bindings_generator = b.addExecutable(.{
        .name = "node_bindings",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/node/node_bindings.zig"),
            .target = b.graph.host,
        }),
    });
    node_bindings_generator.root_module.addImport("vsr", options.vsr_module);
    node_bindings_generator.root_module.addOptions("vsr_options", options.vsr_options);
    const bindings = Generated.file(b, .{
        .generator = node_bindings_generator,
        .path = "./src/clients/node/src/bindings.ts",
    });

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
        \\process.stdout.write('EXPORTS\n    ' + Array.from(allSymbols).join('\n    '))
    });
    write_def_file.cwd = b.path("./src/clients/node");
    write_def_file.step.dependOn(&npm_install.step);

    var run_dll_tool = b.addSystemCommand(&.{
        b.graph.zig_exe, "dlltool",
        "-m",            "i386:x86-64",
        "-D",            "node.exe",
        "-l",            "node.lib",
        "-d",
    });
    run_dll_tool.addFileArg(write_def_file.captureStdOut());
    run_dll_tool.cwd = b.path("./src/clients/node");

    inline for (platforms) |platform| {
        const query = Query.parse(.{
            .arch_os_abi = platform[0],
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/node/node.zig"),
            .target = resolved_target,
            .optimize = options.mode,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const lib = b.addLibrary(.{
            .name = "tb_nodeclient",
            .linkage = .dynamic,
            .root_module = root_module,
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

        lib.step.dependOn(&bindings.step);
        step_clients_node.dependOn(&b.addInstallFile(lib.getEmittedBin(), b.pathJoin(&.{
            "../src/clients/node/dist/bin",
            strip_glibc_version(platform[0]),
            "/client.node",
        })).step);
    }
}

fn build_python_client(
    b: *std.Build,
    step_clients_python: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        tb_client_header: std.Build.LazyPath,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const python_bindings_generator = b.addExecutable(.{
        .name = "python_bindings",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clients/python/python_bindings.zig"),
            .target = b.graph.host,
        }),
    });
    python_bindings_generator.root_module.addImport("vsr", options.vsr_module);
    python_bindings_generator.root_module.addOptions("vsr_options", options.vsr_options);
    const bindings = Generated.file(b, .{
        .generator = python_bindings_generator,
        .path = "./src/clients/python/src/tigerbeetle/bindings.py",
    });

    inline for (platforms) |platform| {
        const query = Query.parse(.{
            .arch_os_abi = platform[0],
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = resolved_target,
            .optimize = options.mode,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const shared_lib = b.addLibrary(.{
            .name = "tb_client",
            .linkage = .dynamic,
            .root_module = root_module,
        });
        shared_lib.linkLibC();
        if (resolved_target.result.os.tag == .windows) {
            shared_lib.linkSystemLibrary("ws2_32");
            shared_lib.linkSystemLibrary("advapi32");
        }

        step_clients_python.dependOn(&b.addInstallFile(
            shared_lib.getEmittedBin(),
            b.pathJoin(&.{
                "../src/clients/python/src/tigerbeetle/lib/",
                platform[0],
                shared_lib.out_filename,
            }),
        ).step);
    }

    step_clients_python.dependOn(&bindings.step);
}

fn build_c_client(
    b: *std.Build,
    step_clients_c: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        tb_client_header: *Generated,
        mode: std.builtin.OptimizeMode,
    },
) void {
    step_clients_c.dependOn(&options.tb_client_header.step);

    inline for (platforms) |platform| {
        const query = Query.parse(.{
            .arch_os_abi = platform[0],
            .cpu_features = platform[2],
        }) catch unreachable;
        const resolved_target = b.resolveTargetQuery(query);

        const root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = resolved_target,
            .optimize = options.mode,
        });
        root_module.addImport("vsr", options.vsr_module);
        root_module.addOptions("vsr_options", options.vsr_options);
        if (options.mode == .ReleaseSafe) strip_root_module(root_module);

        const shared_lib = b.addLibrary(.{
            .name = "tb_client",
            .linkage = .dynamic,
            .root_module = root_module,
        });

        const static_lib = b.addLibrary(.{
            .name = "tb_client",
            .linkage = .static,
            .root_module = root_module,
        });
        static_lib.bundle_compiler_rt = true;
        static_lib.pie = true;

        for ([_]*std.Build.Step.Compile{ shared_lib, static_lib }) |lib| {
            lib.linkLibC();
            if (resolved_target.result.os.tag == .windows) {
                lib.linkSystemLibrary("ws2_32");
                lib.linkSystemLibrary("advapi32");
            }

            step_clients_c.dependOn(&b.addInstallFile(lib.getEmittedBin(), b.pathJoin(&.{
                "../src/clients/c/lib/",
                platform[0],
                lib.out_filename,
            })).step);
        }
    }
}

fn build_clients_c_sample(
    b: *std.Build,
    step_clients_c_sample: *std.Build.Step,
    options: struct {
        vsr_module: *std.Build.Module,
        vsr_options: *std.Build.Step.Options,
        target: std.Build.ResolvedTarget,
        mode: std.builtin.OptimizeMode,
    },
) void {
    const static_lib = b.addLibrary(.{
        .name = "tb_client",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tigerbeetle/libtb_client.zig"),
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    static_lib.linkLibC();
    static_lib.pie = true;
    static_lib.bundle_compiler_rt = true;
    static_lib.root_module.addImport("vsr", options.vsr_module);
    static_lib.root_module.addOptions("vsr_options", options.vsr_options);
    step_clients_c_sample.dependOn(&static_lib.step);

    const sample = b.addExecutable(.{
        .name = "c_sample",
        .root_module = b.createModule(.{
            .target = options.target,
            .optimize = options.mode,
        }),
    });
    sample.root_module.addCSourceFile(.{
        .file = b.path("src/clients/c/samples/main.c"),
    });
    sample.linkLibrary(static_lib);
    sample.linkLibC();

    if (options.target.result.os.tag == .windows) {
        static_lib.linkSystemLibrary("ws2_32");
        static_lib.linkSystemLibrary("advapi32");

        // TODO: Illegal instruction on Windows:
        sample.root_module.sanitize_c = false;
    }

    const install_step = b.addInstallArtifact(sample, .{});
    step_clients_c_sample.dependOn(&install_step.step);
}

fn strip_root_module(root_module: *std.Build.Module) void {
    root_module.strip = true;
    // Ensure that we get stack traces even in release builds.
    root_module.omit_frame_pointer = false;
    root_module.unwind_tables = .none;
}

/// Set the JVM DLL directory on Windows.
fn set_windows_dll(allocator: std.mem.Allocator, java_home: []const u8) void {
    comptime assert(builtin.os.tag == .windows);

    const java_bin_path = std.fs.path.joinZ(
        allocator,
        &.{ java_home, "\\bin" },
    ) catch unreachable;
    _ = SetDllDirectoryA(java_bin_path);

    const java_bin_server_path = std.fs.path.joinZ(
        allocator,
        &.{ java_home, "\\bin\\server" },
    ) catch unreachable;
    _ = SetDllDirectoryA(java_bin_server_path);
}

extern "kernel32" fn SetDllDirectoryA(path: [*:0]const u8) callconv(.c) std.os.windows.BOOL;

fn print_or_install(b: *std.Build, compile: *std.Build.Step.Compile, print: bool) *std.Build.Step {
    const PrintStep = struct {
        step: std.Build.Step,
        compile: *std.Build.Step.Compile,

        fn make(step: *std.Build.Step, _: std.Build.Step.MakeOptions) !void {
            const print_step: *@This() = @fieldParentPtr("step", step);
            const path = print_step.compile.getEmittedBin().getPath2(step.owner, step);
            try std.io.getStdOut().writer().print("{s}\n", .{path});
        }
    };

    if (print) {
        const print_step = b.allocator.create(PrintStep) catch @panic("OOM");
        print_step.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "print exe",
                .owner = b,
                .makeFn = PrintStep.make,
            }),
            .compile = compile,
        };
        print_step.step.dependOn(&print_step.compile.step);
        return &print_step.step;
    } else {
        return &b.addInstallArtifact(compile, .{}).step;
    }
}

/// Code generation for files which must also be committed to the repository.
///
/// Runs the generator program to produce a file or a directory and copies the result to the
/// destination directory within the source tree.
///
/// On CI (when CI env var is set), the files are not updated, and merely checked for freshness.
const Generated = struct {
    step: std.Build.Step,
    path: std.Build.LazyPath,

    destination: []const u8,
    generated_file: std.Build.GeneratedFile,
    source: std.Build.LazyPath,
    mode: enum { file, directory },

    /// The `generator` program prints the file to stdout.
    pub fn file(b: *std.Build, options: struct {
        generator: *std.Build.Step.Compile,
        path: []const u8,
    }) *Generated {
        return create(b, options.path, .{
            .file = options.generator,
        });
    }

    pub fn file_copy(b: *std.Build, options: struct {
        from: std.Build.LazyPath,
        path: []const u8,
    }) *Generated {
        return create(b, options.path, .{
            .copy = options.from,
        });
    }

    /// The `generator` program creates several files in the output directory, which is passed in
    /// as an argument.
    ///
    /// NB: there's no check that there aren't extra file at the destination. In other words, this
    /// API can be used for mixing generated and hand-written files in a single directory.
    pub fn directory(b: *std.Build, options: struct {
        generator: *std.Build.Step.Compile,
        path: []const u8,
    }) *Generated {
        return create(b, options.path, .{
            .directory = options.generator,
        });
    }

    fn create(b: *std.Build, destination: []const u8, generator: union(enum) {
        file: *std.Build.Step.Compile,
        directory: *std.Build.Step.Compile,
        copy: std.Build.LazyPath,
    }) *Generated {
        assert(std.mem.startsWith(u8, destination, "./src"));
        const result = b.allocator.create(Generated) catch @panic("OOM");
        result.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = b.fmt("generate {s}", .{std.fs.path.basename(destination)}),
                .owner = b,
                .makeFn = make,
            }),
            .path = .{ .generated = .{ .file = &result.generated_file } },

            .destination = destination,
            .generated_file = .{ .step = &result.step },
            .source = switch (generator) {
                .file => |compile| b.addRunArtifact(compile).captureStdOut(),
                .directory => |compile| b.addRunArtifact(compile).addOutputDirectoryArg("out"),
                .copy => |lazy_path| lazy_path,
            },
            .mode = switch (generator) {
                .file, .copy => .file,
                .directory => .directory,
            },
        };
        result.source.addStepDependencies(&result.step);

        return result;
    }

    fn make(step: *std.Build.Step, _: std.Build.Step.MakeOptions) !void {
        const b = step.owner;
        const generated: *Generated = @fieldParentPtr("step", step);
        const ci = try std.process.hasEnvVar(b.allocator, "CI");
        const source_path = generated.source.getPath2(b, step);

        if (ci) {
            const fresh = switch (generated.mode) {
                .file => file_fresh(b, source_path, generated.destination),
                .directory => directory_fresh(b, source_path, generated.destination),
            } catch |err| {
                return step.fail("unable to check '{s}': {s}", .{
                    generated.destination, @errorName(err),
                });
            };

            if (!fresh) {
                return step.fail("file '{s}' is outdated", .{
                    generated.destination,
                });
            }
            step.result_cached = true;
        } else {
            const prev = switch (generated.mode) {
                .file => file_update(b, source_path, generated.destination),
                .directory => directory_update(b, source_path, generated.destination),
            } catch |err| {
                return step.fail("unable to update '{s}': {s}", .{
                    generated.destination, @errorName(err),
                });
            };
            step.result_cached = prev == .fresh;
        }

        generated.generated_file.path = generated.destination;
    }

    fn file_fresh(
        b: *std.Build,
        source_path: []const u8,
        target_path: []const u8,
    ) !bool {
        const want = try b.build_root.handle.readFileAlloc(
            b.allocator,
            source_path,
            std.math.maxInt(usize),
        );
        defer b.allocator.free(want);

        const got = b.build_root.handle.readFileAlloc(
            b.allocator,
            target_path,
            std.math.maxInt(usize),
        ) catch return false;
        defer b.allocator.free(got);

        return std.mem.eql(u8, want, got);
    }

    fn file_update(
        b: *std.Build,
        source_path: []const u8,
        target_path: []const u8,
    ) !std.fs.Dir.PrevStatus {
        return std.fs.Dir.updateFile(
            b.build_root.handle,
            source_path,
            b.build_root.handle,
            target_path,
            .{},
        );
    }

    fn directory_fresh(
        b: *std.Build,
        source_path: []const u8,
        target_path: []const u8,
    ) !bool {
        var source_dir = try b.build_root.handle.openDir(source_path, .{ .iterate = true });
        defer source_dir.close();

        var target_dir = b.build_root.handle.openDir(target_path, .{}) catch return false;
        defer target_dir.close();

        var source_iter = source_dir.iterate();
        while (try source_iter.next()) |entry| {
            assert(entry.kind == .file);
            const want = try source_dir.readFileAlloc(
                b.allocator,
                entry.name,
                std.math.maxInt(usize),
            );
            defer b.allocator.free(want);

            const got = target_dir.readFileAlloc(
                b.allocator,
                entry.name,
                std.math.maxInt(usize),
            ) catch return false;
            defer b.allocator.free(got);

            if (!std.mem.eql(u8, want, got)) return false;
        }

        return true;
    }

    fn directory_update(
        b: *std.Build,
        source_path: []const u8,
        target_path: []const u8,
    ) !std.fs.Dir.PrevStatus {
        var result: std.fs.Dir.PrevStatus = .fresh;
        var source_dir = try b.build_root.handle.openDir(source_path, .{ .iterate = true });
        defer source_dir.close();

        var target_dir = try b.build_root.handle.makeOpenPath(target_path, .{});
        defer target_dir.close();

        var source_iter = source_dir.iterate();
        while (try source_iter.next()) |entry| {
            assert(entry.kind == .file);
            const status = try std.fs.Dir.updateFile(
                source_dir,
                entry.name,
                target_dir,
                entry.name,
                .{},
            );
            if (status == .stale) result = .stale;
        }

        return result;
    }
};

fn download_release(
    b: *std.Build,
    version_or_latest: []const u8,
    target: std.Build.ResolvedTarget,
    mode: std.builtin.OptimizeMode,
) std.Build.LazyPath {
    const release_slug = if (std.mem.eql(u8, version_or_latest, "latest"))
        "latest/download"
    else
        b.fmt("download/{s}", .{version_or_latest});

    const arch = if (target.result.os.tag == .macos)
        "universal"
    else switch (target.result.cpu.arch) {
        .x86_64 => "x86_64",
        .aarch64 => "aarch64",
        else => @panic("unsupported CPU"),
    };

    const os = switch (target.result.os.tag) {
        .windows => "windows",
        .linux => "linux",
        .macos => "macos",
        else => @panic("unsupported OS"),
    };

    const debug = switch (mode) {
        .ReleaseSafe => "",
        .Debug => "-debug",
        else => @panic("unsupported mode"),
    };

    const url = b.fmt(
        "https://github.com/tigerbeetle/tigerbeetle" ++
            "/releases/{s}/tigerbeetle-{s}-{s}{s}.zip",
        .{ release_slug, arch, os, debug },
    );

    return fetch(b, .{
        .url = url,
        .file_name = if (target.result.os.tag == .windows) "tigerbeetle.exe" else "tigerbeetle",
        .hash = null,
    });
}

// Use 'zig fetch' to download and unpack the specified URL, optionally verifying the checksum.
fn fetch(b: *std.Build, options: struct {
    url: []const u8,
    file_name: []const u8,
    hash: ?[]const u8,
}) std.Build.LazyPath {
    const copy_from_cache = b.addRunArtifact(b.addExecutable(.{
        .name = "copy-from-cache",
        .root_module = b.createModule(.{
            .root_source_file = b.addWriteFiles().add("main.zig",
                \\const builtin = @import("builtin");
                \\const std = @import("std");
                \\const assert = std.debug.assert;
                \\
                \\pub fn main() !void {
                \\    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                \\    const allocator = arena.allocator();
                \\    const args = try std.process.argsAlloc(allocator);
                \\    assert(args.len == 5 or args.len == 6);
                \\
                \\    const hash_and_newline = try std.fs.cwd().readFileAlloc(allocator, args[2], 128);
                \\    assert(hash_and_newline[hash_and_newline.len - 1] == '\n');
                \\    const hash = hash_and_newline[0 .. hash_and_newline.len - 1];
                \\    if (args.len == 6 and !std.mem.eql(u8, args[5], hash)) {
                \\        std.debug.panic(
                \\            \\bad hash
                \\            \\specified:  {s}
                \\            \\downloaded: {s}
                \\            \\
                \\        , .{ args[5], hash });
                \\    }
                \\
                \\    const source_path = try std.fs.path.join(allocator, &.{ args[1], hash, args[3] });
                \\    try std.fs.cwd().copyFile(
                \\        source_path,
                \\        std.fs.cwd(),
                \\        args[4],
                \\        .{},
                \\    );
                \\}
            ),
            .target = b.graph.host,
        }),
    }));
    copy_from_cache.addArg(
        b.graph.global_cache_root.join(b.allocator, &.{"p"}) catch @panic("OOM"),
    );
    copy_from_cache.addFileArg(
        b.addSystemCommand(&.{ b.graph.zig_exe, "fetch", options.url }).captureStdOut(),
    );
    copy_from_cache.addArg(options.file_name);
    const result = copy_from_cache.addOutputFileArg(options.file_name);
    if (options.hash) |hash| {
        copy_from_cache.addArg(hash);
    }
    return result;
}

const MiB = 1024 * 1024;
