const std = @import("std");
const Build = std.Build;

const extensions_text = .{ ".css", ".html", ".js", ".json", ".svg", ".xml" };
const extensions_binary = .{ ".avif", ".gif", ".jpg", ".png", ".ttf", ".webp", ".woff2" };

const FileType = struct {
    extension: []const u8,
    is_binary: bool,
};

pub const supported_file_types: []const FileType = blk: {
    var result: []const FileType = &.{};
    for (extensions_text) |ext| {
        result = result ++ .{.{ .extension = ext, .is_binary = false }};
    }
    for (extensions_binary) |ext| {
        result = result ++ .{.{ .extension = ext, .is_binary = true }};
    }
    break :blk result;
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
