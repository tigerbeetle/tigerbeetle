const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
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
        const benchmark = b.addExecutable("benchmark_array_search", "src/benchmark_array_search.zig");
        benchmark.setTarget(target);
        benchmark.setBuildMode(mode);
        //const run_cmd = tigerbeetle.run();
        const run_cmd = benchmark.run();

        //const test_step = b.step("test", "Run the unit tests");
        const step = b.step("benchmark_array_search", "Benchmark array search");
        step.dependOn(&run_cmd.step);
    }
}
