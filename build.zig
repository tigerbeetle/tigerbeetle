const std = @import("std");

pub fn build(b: *std.Build) void {
    const consensus = b: {
        const module = b.createModule(.{
            .root_source_file = b.path("./src/main.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        });
        const exe = b.addExecutable(.{
            .name = "cst",
            .root_module = module,
        });
        break :b exe;
    };

    const checker = b: {
        const module = b.createModule(.{
            .root_source_file = b.path("./src/checker.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        });
        const exe = b.addExecutable(.{
            .name = "cst",
            .root_module = module,
        });
        break :b exe;
    };

    const run = b.addRunArtifact(checker);
    run.addArtifactArg(consensus);
    run.has_side_effects = true;
    b.default_step = &run.step;
}
