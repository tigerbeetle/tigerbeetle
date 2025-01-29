const std = @import("std");
const Build = std.Build;

pub const exclude_extensions = [_][]const u8{
    ".DS_Store",
};

pub fn install(b: *Build, options: struct {
    source: []const u8,
    target: []const u8,
}) *Build.Step.InstallDir {
    return b.addInstallDirectory(.{
        .source_dir = b.path(options.source),
        .install_dir = .prefix,
        .install_subdir = options.target,
        .exclude_extensions = &exclude_extensions,
    });
}
