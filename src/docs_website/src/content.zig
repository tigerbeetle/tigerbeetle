//! Walk the `/docs` directory to build table of contents by parsing links to child pages from
//! the READMEs.
const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const log = std.log.scoped(.content);
const cut_prefix = @import("./docs.zig").cut_prefix;
const cut = @import("./docs.zig").cut;

pub const Page = struct {
    content: PageContent,
    path: []const u8, // Always ends in .md
    children: []Page,
};

// Parsed content of a single .md file.
const PageContent = struct {
    title: []const u8,
    children: []Child,

    const Child = struct {
        title: []const u8,
        path: []const u8,
    };
};

pub fn load(arena: Allocator, base: std.fs.Dir, page_buffer: []u8) !Page {
    return load_page(arena, base, "./", page_buffer);
}

fn load_page(arena: Allocator, base: std.fs.Dir, path: []const u8, page_buffer: []u8) !Page {
    errdefer log.err("error while loading '{s}'", .{path});
    const is_dir = std.mem.endsWith(u8, path, "/");
    const is_client = std.mem.indexOf(u8, path, "src/clients") != null;

    const file_path = if (is_dir) try std.fs.path.join(arena, &.{ path, "README.md" }) else path;
    if (!std.mem.endsWith(u8, file_path, ".md")) {
        return error.InvalidPath;
    }

    const text = try read_file(base, file_path, page_buffer);
    const content = try parse_page_content(arena, text, .{
        .parse_children = is_dir and !is_client,
    });

    var children: std.ArrayListUnmanaged(Page) = .{};
    for (content.children) |child| {
        assert(is_dir);
        const child_path = if (std.mem.startsWith(u8, child.path, "/src/clients"))
            try std.fs.path.join(arena, &.{
                "..",
                child.path,
            })
        else
            try std.fs.path.join(arena, &.{
                path,
                cut_prefix(child.path, "./") orelse child.path,
            });
        const child_page = try load_page(arena, base, child_path, page_buffer);
        try children.append(arena, child_page);
    }

    if (is_dir and !is_client) {
        var dir = try base.openDir(path, .{ .iterate = true });
        defer dir.close();

        var dir_iterator = dir.iterate();
        while (try dir_iterator.next()) |entry| {
            if (std.mem.eql(u8, entry.name, "README.md")) continue;
            if (std.mem.eql(u8, entry.name, "internals")) continue;
            if (std.mem.eql(u8, entry.name, "TIGER_STYLE.md")) continue;
            for (content.children) |child| {
                const name = std.mem.trimRight(
                    u8,
                    cut_prefix(child.path, "./") orelse child.path,
                    "/",
                );
                if (std.mem.eql(u8, name, entry.name)) break;
            } else {
                log.err("orphaned page: {s}{s}", .{ path, entry.name });
                return error.OrphanedPage;
            }
        }
    }

    return .{
        .content = content,
        .path = file_path,
        .children = children.items,
    };
}

fn parse_page_content(arena: Allocator, text: []const u8, options: struct {
    parse_children: bool,
}) !PageContent {
    var line_iterator = std.mem.tokenizeScalar(u8, text, '\n');
    var title_line = line_iterator.next() orelse return error.TitleInvalid;
    if (std.mem.startsWith(u8, title_line, "<!--")) {
        // Clients' readmes start with auto-generated comment, skip over it.
        title_line = line_iterator.next() orelse return error.TitleInvalid;
    }

    var title = cut_prefix(title_line, "# ") orelse return error.TitleInvalid;
    title = std.mem.trim(u8, title, "`");
    if (title.len < 3) return error.TitleInvalid;

    var children: std.ArrayListUnmanaged(PageContent.Child) = .{};

    if (options.parse_children) {
        while (line_iterator.next()) |line| {
            if (try parse_page_child(line)) |child| {
                try children.append(arena, .{
                    .title = try arena.dupe(u8, child.title),
                    .path = try arena.dupe(u8, child.path),
                });
            }
        }
    }

    return .{
        .title = try arena.dupe(u8, title),
        .children = children.items,
    };
}

fn parse_page_child(line: []const u8) !?PageContent.Child {
    errdefer log.err("error while parsing '{s}'", .{line});

    var rest = cut_prefix(line, "- [") orelse return null;
    const title, rest = cut(rest, "](") orelse return error.InvalidLink;
    const path, _ = cut(rest, ")") orelse return error.InvalidLink;
    if (!std.mem.startsWith(u8, path, "./")) {
        if (std.mem.startsWith(u8, path, "/src/clients/")) {
            // Special case.
        } else {
            log.err("ToC links must start with './'", .{});
            return error.InvalidLink;
        }
    }
    if (!std.mem.endsWith(u8, path, ".md") and !std.mem.endsWith(u8, path, "/")) {
        log.err("ToC links must end with '/' or '.md'", .{});
        return error.InvalidLink;
    }
    return .{
        .title = std.mem.trim(u8, title, "`"),
        .path = path,
    };
}

fn read_file(dir: std.fs.Dir, path: []const u8, page_buffer: []u8) ![]const u8 {
    const result = try dir.readFile(path, page_buffer);
    if (result.len == page_buffer.len) return error.FileToLarge;
    return result;
}
