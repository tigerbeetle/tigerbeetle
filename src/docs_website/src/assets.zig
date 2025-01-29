const std = @import("std");
const Build = std.Build;

const FileType = struct {
    extension: []const u8,
    is_binary: bool,
};

pub const supported_file_types = [_]FileType{
    .{ .extension = ".avif", .is_binary = true },
    .{ .extension = ".css", .is_binary = false },
    .{ .extension = ".gif", .is_binary = true },
    .{ .extension = ".html", .is_binary = false },
    .{ .extension = ".jpg", .is_binary = true },
    .{ .extension = ".js", .is_binary = false },
    .{ .extension = ".json", .is_binary = false },
    .{ .extension = ".png", .is_binary = true },
    .{ .extension = ".svg", .is_binary = false },
    .{ .extension = ".ttf", .is_binary = true },
    .{ .extension = ".webp", .is_binary = true },
    .{ .extension = ".woff2", .is_binary = true },
    .{ .extension = ".xml", .is_binary = false },
};

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
