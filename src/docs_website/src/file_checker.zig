//! Sanity checks that all the generated files look reasonable.

const std = @import("std");
const log = std.log.scoped(.validate);
const assert = std.debug.assert;

const file_size_max = 166 * 1024;
const search_index_size_max = 950 * 1024;
const single_page_size_max = 950 * 1024;

// If this is set to true, we check if we get a 200 response for any external links.
const check_links_external: bool = false;

var file_cache: std.StringHashMap([]const u8) = undefined;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    file_cache = std.StringHashMap([]const u8).init(allocator);
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
    defer walker.deinit();

    while (try walker.next()) |entry| switch (entry.kind) {
        .file => try validate_file(.{ .arena = arena, .dir = dir, .path = entry.path }),
        .directory => {},
        else => {
            log.err("unexpected file type: '{s}'", .{
                try dir.realpathAlloc(arena, entry.path),
            });
            return error.UnsupportedFileType;
        },
    };
}

const FileValidationContext = struct {
    arena: std.mem.Allocator,
    dir: std.fs.Dir,
    path: []const u8,
};

fn validate_file(context: FileValidationContext) !void {
    const stat = context.dir.statFile(context.path) catch |err| {
        log.err("unable to stat file '{s}': {s}", .{
            try context.dir.realpathAlloc(context.arena, context.path),
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
        log.err("file '{s}' with size {:.2} exceeds max file size of {:.2}", .{
            try context.dir.realpathAlloc(context.arena, context.path),
            std.fmt.fmtIntSizeBin(stat.size),
            std.fmt.fmtIntSizeBin(size_max),
        });
        return error.FileSizeExceeded;
    }

    switch (classify_file(context.path)) {
        .text => try validate_text_file(context),
        .binary => {}, // Nothing to validate.
        .exception => {}, // Nothing to validate.
        .unexpected => {
            log.err("file '{s}' has unsupported type '{s}'", .{
                try context.dir.realpathAlloc(context.arena, context.path),
                std.fs.path.extension(context.path),
            });
            return error.UnsupportedFileType;
        },
    }
}

fn validate_text_file(context: FileValidationContext) !void {
    assert(classify_file(context.path) == .text);

    const file = try context.dir.openFile(context.path, .{});
    defer file.close();

    try file.seekFromEnd(-1);
    const last_byte = try file.reader().readByte();
    if (last_byte != '\n') {
        log.err("file '{s}' doesn't end with a newline", .{
            try context.dir.realpathAlloc(context.arena, context.path),
        });
        return error.MissingNewline;
    }

    if (std.mem.endsWith(u8, context.path, ".html")) {
        try check_links(context);
    }
}

fn read_file_cached(arena: std.mem.Allocator, dir: std.fs.Dir, path: []const u8) ![]const u8 {
    if (file_cache.get(path)) |content| return content;

    const content = try dir.readFileAlloc(arena, path, 1 * 1024 * 1024);
    try file_cache.put(try arena.dupe(u8, path), content);

    return content;
}

// These links don't work with https.
const http_exceptions = std.StaticStringMap(void).initComptime(.{
    .{"http://www.bailis.org/blog/linearizability-versus-serializability/"},
});

// These links cause TLS errors with std.http.Client.
const https_exceptions = std.StaticStringMap(void).initComptime(.{
    .{"https://www.eecg.utoronto.ca/~yuan/papers/failure_analysis_osdi14.pdf"},
    .{"https://pmg.csail.mit.edu/papers/vr-revisited.pdf"},
    .{"https://pmg.csail.mit.edu/papers/vr.pdf"},
    .{"https://www.infoq.com/presentations/LMAX/"},
    .{"https://kernel.dk/io_uring.pdf"},
    .{"https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf"},
    .{"https://security.googleblog.com/2023/06/learnings-from-kctf-vrps-42-linux.html"},
});

fn check_links(context: FileValidationContext) !void {
    const html = try read_file_cached(context.arena, context.dir, context.path);

    var link_iterator = LinkIterator.init(html);
    errdefer log.err("[link checker] error in {s}:{}", .{
        context.dir.realpathAlloc(context.arena, context.path) catch unreachable,
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
            return check_link_external(context.arena, link);
        }

        if (std.mem.startsWith(u8, link.base, "http://")) {
            if (http_exceptions.has(link.base)) {
                return check_link_external(context.arena, link);
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

    if (!try path_exists(context.dir, target)) {
        log.err("link target not found: '{s}'", .{target});
        return error.TargetNotFound;
    }

    if (link.fragment) |fragment| {
        try check_link_fragment(context, target, fragment);
    }
}

fn check_link_external(arena: std.mem.Allocator, link: Link) !void {
    if (!check_links_external) return;
    if (https_exceptions.has(link.base)) return;

    errdefer |err| log.err("got {} while checking external link '{s}'", .{ err, link.base });

    log.info("checking external link '{s}'", .{link.base});

    var client = std.http.Client{ .allocator = arena };
    defer client.deinit();

    const uri = try std.Uri.parse(link.base);
    var header_buffer: [512 * 1024]u8 = undefined;
    var request = try client.open(.GET, uri, .{ .server_header_buffer = &header_buffer });
    defer request.deinit();

    try request.send();
    try request.finish();
    try request.wait();

    if (request.response.status != std.http.Status.ok) {
        return error.WrongStatusResponse;
    }
}

fn check_link_fragment(
    context: FileValidationContext,
    target_path: []const u8,
    fragment: []const u8,
) !void {
    assert(std.mem.endsWith(u8, target_path, ".html"));

    const html = try read_file_cached(context.arena, context.dir, target_path);
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

fn path_exists(dir: std.fs.Dir, path: []const u8) !bool {
    dir.access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
