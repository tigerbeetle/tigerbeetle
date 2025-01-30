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

const FileValidationConext = struct {
    arena: std.mem.Allocator,
    dir: std.fs.Dir,
    path: []const u8,
};

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
const http_exceptions = std.StaticStringMap(void).initComptime(.{
    .{"http://www.bailis.org/blog/linearizability-versus-serializability/"},
});

fn check_links(arena: std.mem.Allocator, dir: std.fs.Dir, html_path: []const u8) !void {
    const html = try dir.readFileAlloc(arena, html_path, file_size_max);

    var link_iterator = find_links(html);
    errdefer log.err("[link checker] error in {s}:{}", .{
        dir.realpathAlloc(arena, html_path) catch unreachable,
        link_iterator.line_number,
    });

    while (link_iterator.next()) |link| {
        try check_link(arena, dir, html_path, link);
    }
}

fn check_link(arena: std.mem.Allocator, dir: std.fs.Dir, html_path: []const u8, link: Link) !void {
    // Check schema.
    {
        if (std.mem.startsWith(u8, link.base, "mailto:")) return;

        if (std.mem.startsWith(u8, link.base, "https://")) return check_external_link(link);

        if (std.mem.startsWith(u8, link.base, "http://")) {
            if (!http_exceptions.has(link.base)) {
                log.err("Found insecure link: {s}", .{link.base});
                return error.InsecureLink;
            }

            return check_external_link(link);
        }
    }

    var target = link.base;
    const is_absolute = target.len > 0 and target[0] == '/';
    if (is_absolute) {
        target = target[1..];
    } else if (std.fs.path.dirname(html_path)) |dirname| {
        target = try std.fs.path.join(arena, &.{ dirname, target });
    }

    const is_directory = std.fs.path.extension(target).len == 0;
    if (is_directory) {
        target = try std.fs.path.join(arena, &.{ target, "index.html" });
    }

    if (!try path_exists(dir, target)) {
        log.err("Link target not found: {s}", .{target});
        return error.TargetNotFound;
    }

    if (link.fragment) |anchor| {
        try check_anchor(target, anchor);
    }
}

fn check_external_link(link: Link) !void {
    // TODO: use http client
    _ = link;
}

fn check_anchor(target_path: []const u8, anchor: []const u8) !void {
    // TODO
    log.info("anchor {s} {s}", .{ target_path, anchor });
}

fn find_links(html: []const u8) LinkIterator {
    return LinkIterator.init(html);
}

const Link = struct {
    // line_number: u32,
    base: []const u8,
    fragment: ?[]const u8 = null,

    fn parse(text: []const u8) Link {
        if (std.mem.lastIndexOfScalar(u8, text, '#')) |index| {
            return .{
                .base = text[0..index],
                .fragment = text[index + 1 ..],
            };
        }
        return .{ .base = text };
    }
};

const LinkIterator = struct {
    line_number: u32 = 0,
    line_iterator: std.mem.TokenIterator(u8, .scalar),
    href_iterator: std.mem.TokenIterator(u8, .sequence),

    const href_prefix = "href=\"";

    fn init(html: []const u8) LinkIterator {
        return .{
            .line_iterator = std.mem.tokenizeScalar(u8, html, '\n'),
            .href_iterator = std.mem.tokenizeSequence(u8, "", href_prefix),
        };
    }

    fn next(self: *LinkIterator) ?Link {
        const href = self.next_href() orelse return null;
        return Link.parse(href);
    }

    fn next_href(self: *LinkIterator) ?[]const u8 {
        while (true) {
            if (self.href_iterator.next()) |href| {
                return std.mem.sliceTo(href, '"');
            } else {
                const line = self.next_line() orelse return null;

                self.href_iterator = std.mem.tokenizeSequence(u8, line, href_prefix);
                if (!std.mem.startsWith(u8, line, href_prefix)) {
                    _ = self.href_iterator.next(); // Skip
                }
            }
        }
    }

    fn next_line(self: *LinkIterator) ?[]const u8 {
        const line = self.line_iterator.next() orelse return null;
        self.line_number += 1;
        return line;
    }
};

fn path_exists(dir: std.fs.Dir, path: []const u8) !bool {
    dir.access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
