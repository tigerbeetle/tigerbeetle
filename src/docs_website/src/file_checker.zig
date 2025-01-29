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

    if (std.mem.endsWith(u8, path, ".html")) {
        try check_links(arena, dir, path);
    }
}

// These links don't work with https.
const https_exceptions = std.StaticStringMap(void).initComptime(.{
    .{"http://www.bailis.org/blog/linearizability-versus-serializability/"},
});

fn check_links(arena: std.mem.Allocator, dir: std.fs.Dir, html_path: []const u8) !void {
    const html = try dir.readFileAlloc(arena, html_path, file_size_max);

    var line_it = std.mem.tokenizeScalar(u8, html, '\n');
    var line_number: usize = 1;
    while (line_it.next()) |line| {
        defer line_number += 1;
        errdefer log.err("[link checker] error in {s}:{}", .{ html_path, line_number });

        var link_it = std.mem.tokenizeSequence(u8, line, "href=\"");
        if (!std.mem.startsWith(u8, line, "href=\"")) _ = link_it.next(); // Skip prefix
        while (link_it.next()) |chunk| {
            var url = std.mem.sliceTo(chunk, '"');

            // Strip anchor
            if (std.mem.lastIndexOfScalar(u8, url, '#')) |i| url = url[0..i];
            if (url.len == 0) continue;

            if (std.mem.startsWith(u8, url, "http://")) {
                if (https_exceptions.has(url)) continue;
                log.err("Found insecure link: {s}", .{url});
                return error.InsecureLink;
            }

            // Skip external link
            if (std.mem.startsWith(u8, url, "https://")) continue;
            if (std.mem.startsWith(u8, url, "mailto:")) continue;

            const target = if (url[0] == '/')
                url[1..]
            else if (std.fs.path.dirname(html_path)) |dirname|
                try std.fs.path.join(arena, &.{ dirname, url })
            else
                url;
            if (target.len == 0) continue;

            if (!try path_exists(dir, target)) {
                log.err("Link (\"{s}\") target not found: {s}", .{ url, target });
                return error.TargetNotFound;
            }
        }
    }
}

fn path_exists(dir: std.fs.Dir, path: []const u8) !bool {
    dir.access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
