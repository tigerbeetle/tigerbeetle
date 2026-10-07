//! Sanity checks that all the generated files look reasonable.

const std = @import("std");
const log = std.log.scoped(.validate);
const assert = std.debug.assert;

const file_size_max = 166 * 1024;
const search_index_size_max = 2000 * 1024;
const single_page_size_max = 2000 * 1024;

// If this is set to true, we check if we get a 200 response for any external links.
const check_links_external: bool = false;

var file_cache: std.StringHashMap([]const u8) = undefined;

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    file_cache = std.StringHashMap([]const u8).init(allocator);
    var args = init.minimal.args.iterate();
    _ = args.skip();
    const path = args.next().?;
    assert(args.next() == null);
    try validate_dir(allocator, init.io, path);
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

fn validate_dir(arena: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);

    var walker = try dir.walk(arena);
    defer walker.deinit();

    while (try walker.next(io)) |entry| switch (entry.kind) {
        .file => try validate_file(.{ .arena = arena, .io = io, .dir = dir, .path = entry.path }),
        .directory => {},
        else => {
            log.err("unexpected file type: '{s}'", .{
                try dir.realPathFileAlloc(io, entry.path, arena),
            });
            return error.UnsupportedFileType;
        },
    };
}

const FileValidationContext = struct {
    arena: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
};

fn validate_file(context: FileValidationContext) !void {
    const stat = context.dir.statFile(context.io, context.path, .{}) catch |err| {
        log.err("unable to stat file '{s}': {s}", .{
            try context.dir.realPathFileAlloc(context.io, context.path, context.arena),
            @errorName(err),
        });
        return err;
    };
    const size_max: u64 = if (std.mem.eql(u8, context.path, "search-index.json"))
        search_index_size_max
    else if (std.mem.eql(u8, context.path, "single-page/index.html"))
        single_page_size_max
    else
        file_size_max;
    if (stat.size > size_max) {
        log.err("file '{s}' with size {Bi:.2} exceeds max file size of {Bi:.2}", .{
            try context.dir.realPathFileAlloc(context.io, context.path, context.arena),
            stat.size,
            size_max,
        });
        return error.FileSizeExceeded;
    }

    switch (classify_file(context.path)) {
        .text => try validate_text_file(context),
        .binary => {}, // Nothing to validate.
        .exception => {}, // Nothing to validate.
        .unexpected => {
            log.err("file '{s}' has unsupported type '{s}'", .{
                try context.dir.realPathFileAlloc(context.io, context.path, context.arena),
                std.fs.path.extension(context.path),
            });
            return error.UnsupportedFileType;
        },
    }
}

fn validate_text_file(context: FileValidationContext) !void {
    assert(classify_file(context.path) == .text);

    const file = try context.dir.openFile(context.io, context.path, .{});
    defer file.close(context.io);

    const stat = try file.stat(context.io);
    var last_byte: [1]u8 = undefined;
    _ = try file.readPositionalAll(context.io, &last_byte, stat.size - 1);
    if (last_byte[0] != '\n') {
        log.err("file '{s}' doesn't end with a newline", .{
            try context.dir.realPathFileAlloc(context.io, context.path, context.arena),
        });
        return error.MissingNewline;
    }

    if (std.mem.endsWith(u8, context.path, ".html")) {
        try check_links(context);
    }
}

fn read_file_cached(
    arena: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
) ![]const u8 {
    if (file_cache.get(path)) |content| return content;

    const content = try dir.readFileAlloc(io, path, arena, .limited(2 * 1024 * 1024));
    try file_cache.put(try arena.dupe(u8, path), content);

    return content;
}

// These links don't work with https.
const http_exceptions = std.StaticStringMap(void).initComptime(.{
    .{"http://www.bailis.org/blog/linearizability-versus-serializability/"},
    .{"http://pmg.csail.mit.edu/papers/vr-revisited.pdf"},
});

// These links cause TLS errors with std.http.Client.
const https_exceptions = std.StaticStringMap(void).initComptime(.{
    .{"https://www.eecg.utoronto.ca/~yuan/papers/failure_analysis_osdi14.pdf"},
    .{"https://pmg.csail.mit.edu/papers/vr.pdf"},
    .{"https://www.infoq.com/presentations/LMAX/"},
    .{"https://kernel.dk/io_uring.pdf"},
    .{"https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf"},
    .{"https://security.googleblog.com/2023/06/learnings-from-kctf-vrps-42-linux.html"},
});

fn check_links(context: FileValidationContext) !void {
    const html = try read_file_cached(context.arena, context.io, context.dir, context.path);

    var link_iterator = LinkIterator.init(html);
    errdefer log.err("[link checker] error in {s}:{}", .{
        context.dir.realPathFileAlloc(context.io, context.path, context.arena) catch unreachable,
        link_iterator.line_number,
    });

    while (link_iterator.next()) |link| {
        try check_link(context, link);
    }
}

fn check_link(context: FileValidationContext, link: Link) !void {
    // Check schema.
    {
        if (std.mem.startsWith(u8, link.base, "mailto:")) {
            return; // Ignore.
        }

        if (std.mem.startsWith(u8, link.base, "https://")) {
            return check_link_external(context.arena, context.io, link);
        }

        if (std.mem.startsWith(u8, link.base, "http://")) {
            if (http_exceptions.has(link.base)) {
                return check_link_external(context.arena, context.io, link);
            }

            log.err("found insecure link: '{s}'", .{link.base});
            return error.InsecureLink;
        }
    }

    if (std.mem.indexOf(u8, link.base, "//") != null or
        std.mem.indexOf(u8, link.base, "/./") != null)
    {
        log.err("redundant slash: '{s}'", .{link.base});
        return error.RedundantSlash;
    }

    // Locate local link target.
    var target = link.base;
    const is_absolute = target.len > 0 and target[0] == '/';
    if (is_absolute) {
        target = target[1..];
    } else if (std.fs.path.dirname(context.path)) |dirname| {
        target = try std.fs.path.join(context.arena, &.{ dirname, target });
    }

    const is_directory = std.fs.path.extension(target).len == 0;
    if (is_directory) {
        target = try std.fs.path.join(context.arena, &.{ target, "index.html" });
    }

    if (!try path_exists(context.io, context.dir, target)) {
        log.err("link target not found: '{s}'", .{target});
        return error.TargetNotFound;
    }

    if (link.fragment) |fragment| {
        try check_link_fragment(context, target, fragment);
    }
}

fn check_link_external(arena: std.mem.Allocator, io: std.Io, link: Link) !void {
    if (!check_links_external) return;
    if (https_exceptions.has(link.base)) return;

    errdefer |err| log.err("got {} while checking external link '{s}'", .{ err, link.base });

    log.info("checking external link '{s}'", .{link.base});

    var client = std.http.Client{ .allocator = arena, .io = io };
    defer client.deinit();

    var discarding = std.Io.Writer.Discarding.init(&.{});
    const result = try client.fetch(.{
        .location = .{ .url = link.base },
        .method = .GET,
        .response_writer = &discarding.writer,
    });

    if (result.status != std.http.Status.ok) {
        return error.WrongStatusResponse;
    }
}

fn check_link_fragment(
    context: FileValidationContext,
    target_path: []const u8,
    fragment: []const u8,
) !void {
    assert(std.mem.endsWith(u8, target_path, ".html"));

    const html = try read_file_cached(context.arena, context.io, context.dir, target_path);
    const needle = try std.mem.concat(context.arena, u8, &.{ "id=\"", fragment, "\"" });
    if (std.mem.indexOf(u8, html, needle) == null) {
        log.err("link target '{s}' does not contain anchor: '{s}'", .{ target_path, fragment });
        return error.AnchorNotFound;
    }
}

const Link = struct {
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
    line_number: u32 = 1,
    remaining: []const u8,

    const href_prefix = "href=\"";

    fn init(html: []const u8) LinkIterator {
        return .{ .remaining = html };
    }

    fn next(self: *LinkIterator) ?Link {
        const index = std.mem.indexOf(u8, self.remaining, href_prefix) orelse
            return null;
        const uri_start = index + href_prefix.len;
        const uri_len = std.mem.indexOfScalar(u8, self.remaining[uri_start..], '"') orelse
            return null;
        const uri_end = uri_start + uri_len;
        const uri_text = self.remaining[uri_start..][0..uri_len];

        for (self.remaining[0..uri_start]) |c| {
            if (c == '\n') self.line_number += 1;
        }
        self.remaining = self.remaining[uri_end..];

        return Link.parse(uri_text);
    }
};

fn path_exists(io: std.Io, dir: std.Io.Dir, path: []const u8) !bool {
    dir.access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
