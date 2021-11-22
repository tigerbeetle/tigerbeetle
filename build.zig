const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const target = fixedStandardTargetOptions(b);
    const mode = b.standardReleaseOptions();

    {
        const tigerbeetle = b.addExecutable("tigerbeetle", "src/main.zig");
        tigerbeetle.setTarget(target);
        tigerbeetle.setBuildMode(mode);
        tigerbeetle.install();

        const run_cmd = tigerbeetle.run();
        run_cmd.step.dependOn(b.getInstallStep());
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run TigerBeetle");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const lint = b.addExecutable("lint", "scripts/lint.zig");
        lint.setTarget(target);
        lint.setBuildMode(mode);

        const run_cmd = lint.run();
        run_cmd.step.dependOn(&lint.step);
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

        const test_step = b.step("test", "Run the unit tests");
        test_step.dependOn(&unit_tests.step);
    }

    {
        const benchmark = b.addExecutable("eytzinger_benchmark", "src/eytzinger_benchmark.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(mode);
        const run_cmd = benchmark.run();

        const step = b.step("eytzinger_benchmark", "Benchmark array search");
        step.dependOn(&run_cmd.step);
    }
}

// A patched version of std.build.Builder.standardTargetOptions() to backport the fix
// from https://github.com/ziglang/zig/pull/9817.
fn fixedStandardTargetOptions(self: *std.build.Builder) std.zig.CrossTarget {
    const maybe_triple = self.option(
        []const u8,
        "target",
        "The CPU architecture, OS, and ABI to build for",
    );
    const mcpu = self.option([]const u8, "cpu", "Target CPU");

    if (maybe_triple == null and mcpu == null) {
        return .{};
    }

    const triple = maybe_triple orelse "native";

    var diags: std.zig.CrossTarget.ParseOptions.Diagnostics = .{};
    var selected_target = std.zig.CrossTarget.parse(.{
        .arch_os_abi = triple,
        .cpu_features = mcpu,
        .diagnostics = &diags,
    }) catch |err| switch (err) {
        error.UnknownCpuModel => {
            std.debug.print("Unknown CPU: '{s}'\nAvailable CPUs for architecture '{s}':\n", .{
                diags.cpu_name.?,
                @tagName(diags.arch.?),
            });
            for (diags.arch.?.allCpuModels()) |cpu| {
                std.debug.print(" {s}\n", .{cpu.name});
            }
            std.debug.print("\n", .{});
            self.invalid_user_input = true;
            return .{};
        },
        error.UnknownCpuFeature => {
            std.debug.print(
                \\Unknown CPU feature: '{s}'
                \\Available CPU features for architecture '{s}':
                \\
            , .{
                diags.unknown_feature_name,
                @tagName(diags.arch.?),
            });
            for (diags.arch.?.allFeaturesList()) |feature| {
                std.debug.print(" {s}: {s}\n", .{ feature.name, feature.description });
            }
            std.debug.print("\n", .{});
            self.invalid_user_input = true;
            return .{};
        },
        error.UnknownOperatingSystem => {
            std.debug.print(
                \\Unknown OS: '{s}'
                \\Available operating systems:
                \\
            , .{diags.os_name});
            inline for (std.meta.fields(std.Target.Os.Tag)) |field| {
                std.debug.print(" {s}\n", .{field.name});
            }
            std.debug.print("\n", .{});
            self.invalid_user_input = true;
            return .{};
        },
        else => |e| {
            std.debug.print("Unable to parse target '{s}': {s}\n\n", .{ triple, @errorName(e) });
            self.invalid_user_input = true;
            return .{};
        },
    };

    // Work around LibExeObjStep.make() explicitly omitting -mcpu=baseline
    // even when the target arch is native. The proper fix is in https://github.com/ziglang/zig/pull/9817
    // but we can work around this by providing an explicit arch if the baseline cpu and native arch
    // were requested.
    const cpu = selected_target.getCpu();
    if (selected_target.cpu_arch == null and cpu.model == std.Target.Cpu.baseline(cpu.arch).model) {
        const native_target_info = std.zig.system.NativeTargetInfo.detect(self.allocator, .{}) catch {
            @panic("failed to detect native target info");
        };
        selected_target.cpu_arch = native_target_info.target.cpu.arch;
    }

    return selected_target;
}
