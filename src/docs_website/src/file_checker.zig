//! Sanity checks that all the generated files look reasonable.

const std = @import("std");
const log = std.log.scoped(.validate);
const assert = std.debug.assert;

pub const file_size_max = 900 * 1024;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    var args = std.process.args();
    _ = args.skip();
    const path = args.next().?;
    assert(args.next() == null);
    try validate_dir(allocator, path);
}

fn classify_file(path: []const u8) enum { text, binary, exception, unexpected } {
    const text: []const []const u8 =
        &.{ ".css", ".html", ".js", ".json", ".svg", ".xml" };
    const binary: []const []const u8 =
        &.{ ".avif", ".gif", ".jpg", ".png", ".ttf", ".webp", ".woff2" };
    const exceptions: []const []const u8 =
        &.{ "CNAME", ".nojekyll" };

    const extension = std.fs.path.extension(path);
    for (text) |text_extension| {
        if (std.mem.eql(u8, extension, text_extension)) return .text;
    }

    for (binary) |binary_extension| {
        if (std.mem.eql(u8, extension, binary_extension)) return .binary;
    }

    for (exceptions) |exception| {
        if (std.mem.eql(u8, exception, path)) return .exception;
    }

    return .unexpected;
}

fn validate_dir(arena: std.mem.Allocator, path: []const u8) !void {
    var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();

    var walker = try dir.walk(arena);
    while (try walker.next()) |entry| switch (entry.kind) {
        .file => try validate_file(arena, dir, entry.path),
        .directory => {},
        else => {
            log.err("unexpected file type: '{s}'", .{
                try dir.realpathAlloc(arena, entry.path),
            });
            return error.UnsupportedFileType;
        },
    };
}

fn validate_file(arena: std.mem.Allocator, dir: std.fs.Dir, path: []const u8) !void {
    const stat = dir.statFile(path) catch |err| {
        log.err("unable to stat file '{s}': {s}", .{
            try dir.realpathAlloc(arena, path),
            @errorName(err),
        });
        return err;
    };
    if (stat.size > file_size_max) {
        log.err("file '{s}' with size {:.2} exceeds max file size of {:.2}", .{
            try dir.realpathAlloc(arena, path),
            std.fmt.fmtIntSizeBin(stat.size),
            std.fmt.fmtIntSizeBin(file_size_max),
        });
        return error.FileSizeExceeded;
    }

    switch (classify_file(path)) {
        .text => try validate_text_file(arena, dir, path),
        .binary => {}, // Nothing to validate.
        .exception => {}, // Nothing to validate.
        .unexpected => {
            log.err("file '{s}' has unsupported type '{s}'", .{
                try dir.realpathAlloc(arena, path),
                std.fs.path.extension(path),
            });
            return error.UnsupportedFileType;
        },
    }
}

fn validate_text_file(arena: std.mem.Allocator, dir: std.fs.Dir, path: []const u8) !void {
    assert(classify_file(path) == .text);

    const file = try dir.openFile(path, .{});
    defer file.close();

    try file.seekFromEnd(-1);
    const last_byte = try file.reader().readByte();
    if (last_byte != '\n') {
        log.err("file '{s}' doesn't end with a newline", .{try dir.realpathAlloc(arena, path)});
        return error.MissingNewline;
    }
}
