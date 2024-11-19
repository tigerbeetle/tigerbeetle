const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.docs);
const Website = @import("website.zig").Website;
const assets = @import("assets.zig");
const Html = @import("html.zig").Html;

const enforce_readme_md = false;

pub fn build(
    b: *std.Build,
    website: Website,
    source_dir: []const u8,
) !std.Build.LazyPath {
    const arena = b.allocator;
    const docs = b.addWriteFiles();

    const menu = try DocPage.find_all(arena, "TigerBeetle Docs", source_dir);

    var nav_html = try Html.create(arena);
    try menu.write_links(nav_html);
    _ = docs.add("nav.html", nav_html.string());

    for (menu.pages, 0..) |page, i| {
        const page_prev = if (i < menu.pages.len - 1) menu.pages[i + 1] else null;
        const page_next = if (i > 0) menu.pages[i - 1] else null;
        try page.install(b, website, docs, page_prev, page_next);
    }

    return docs.getDirectory();
}

const Menu = struct {
    title: []const u8,
    path: []const u8,
    index_page: ?DocPage,
    menus: []Menu,
    pages: []DocPage,

    fn asc(context: void, lhs: Menu, rhs: Menu) bool {
        _ = context;
        assert(!std.mem.eql(u8, lhs.title, rhs.title));
        return std.mem.lessThan(u8, lhs.title, rhs.title);
    }

    fn write_links(self: Menu, html: *Html) !void {
        try html.write("<ul>", .{});
        for (self.menus) |menu| {
            try html.write("<li><details open>", .{});
            if (menu.index_page) |_| {
                try html.write("<summary><a href=\"$url\">$title</a></summary>", .{
                    .url = menu.path,
                    .title = try html.from_md(menu.title),
                });
            } else {
                try html.write("<summary>$title</summary>", .{
                    .title = try html.from_md(menu.title),
                });
            }
            try menu.write_links(html);
            try html.write("</details></li>", .{});
        }
        for (self.pages) |page| {
            try html.write("<li><a href=\"$url\">$title</a></li>", .{
                .url = page.path,
                .title = try html.from_md(page.title),
            });
        }
        try html.write("</ul>", .{});
    }
};

const DocPage = struct {
    path: []const u8,

    // Derived from path name.
    id: []const u8,

    // Parsed from Markdown content.
    title: []const u8,
    content: []const u8,

    fn init(arena: Allocator, path: []const u8) !DocPage {
        assert(std.mem.endsWith(u8, path, ".md"));
        const name = std.fs.path.basename(path);
        const id = name[0 .. name.len - ".md".len];
        var post: DocPage = .{
            .path = path,
            .id = id,
            .title = undefined,
            .content = undefined,
        };
        try post.load(arena);
        return post;
    }

    fn load(self: *DocPage, arena: Allocator) !void {
        errdefer log.err("error while loading '{s}'", .{self.path});

        const source = try std.fs.cwd().readFileAlloc(arena, self.path, Website.file_size_max);
        var line_it = std.mem.tokenizeScalar(u8, source, '\n');

        const title_line = line_it.next().?;
        if (title_line.len < 3 or !std.mem.eql(u8, title_line[0..2], "# ")) {
            return error.TitleInvalid;
        }
        self.title = title_line[2..];

        // Cut off title line.
        self.content = source[line_it.index..];
    }

    fn asc(context: void, lhs: DocPage, rhs: DocPage) bool {
        _ = context;
        assert(!std.mem.eql(u8, lhs.title, rhs.title));
        return std.mem.lessThan(u8, lhs.title, rhs.title);
    }

    fn find_all(arena: Allocator, title: []const u8, path: []const u8) !Menu {
        var index_page: ?DocPage = null;
        var pages = std.ArrayList(DocPage).init(arena);
        var menus = std.ArrayList(Menu).init(arena);

        var dir = std.fs.cwd().openDir(path, .{ .iterate = true }) catch |err| {
            log.err("unable to open path '{s}'", .{path});
            return err;
        };
        defer dir.close();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                const page_path = try std.fs.path.join(arena, &.{ path, entry.name });
                const page = try DocPage.init(arena, page_path);
                if (std.mem.eql(u8, entry.name, "README.md")) {
                    assert(index_page == null);
                    index_page = page;
                } else {
                    try pages.append(page);
                }
            } else if (entry.kind == .directory) {
                const menu_path = try std.fs.path.join(arena, &.{ path, entry.name });
                const menu_title = try make_title(arena, entry.name);
                try menus.append(try find_all(arena, menu_title, menu_path));
            }
        }

        if (enforce_readme_md and index_page == null) {
            log.err("README.md not found in '{s}'", .{path});
            return error.MissingReadmeMd;
        }

        std.mem.sort(DocPage, pages.items, {}, DocPage.asc);
        std.mem.sort(Menu, menus.items, {}, Menu.asc);

        return .{
            .title = title,
            .path = path,
            .index_page = index_page,
            .menus = menus.items,
            .pages = pages.items,
        };
    }

    fn install(
        page: DocPage,
        b: *std.Build,
        website: Website,
        blog: *std.Build.Step.WriteFile,
        page_prev: ?DocPage,
        page_next: ?DocPage,
    ) !void {
        const arena = b.allocator;
        const temp = b.addWriteFiles();
        const header_path = temp.add("header.html", try page.header(arena, website.url_prefix));
        const content_path = temp.add("content.md", page.content);
        const footer_path = temp.add("footer.html", try page.footer(
            arena,
            page_prev,
            page_next,
        ));

        const pandoc_step = std.Build.Step.Run.create(b, "run pandoc");
        pandoc_step.addFileArg(website.pandoc_bin);
        pandoc_step.addArgs(&.{ "--from", "gfm", "--to", "html5" });
        pandoc_step.addArg("--lua-filter");
        pandoc_step.addFileArg(b.path("pandoc/anchor-links.lua"));
        pandoc_step.addArg("--output");
        const pandoc_out = pandoc_step.addOutputFileArg("pandoc-out.html");
        pandoc_step.addFileArg(content_path);

        const cat_step = b.addSystemCommand(&.{"cat"});
        cat_step.addFileArg(header_path);
        cat_step.addFileArg(pandoc_out);
        cat_step.addFileArg(footer_path);
        const combined = cat_step.captureStdOut();

        const page_path = website.write_page(.{
            .title = page.title,
            .content = combined,
        });
        _ = blog.addCopyFile(page_path, b.pathJoin(&.{ page.id, "index.html" }));

        // If it exists, copy the page's asset directory.
        const page_dir = page.path[0 .. page.path.len - ".md".len];
        if (try path_exists(b.pathFromRoot(page_dir))) {
            _ = blog.addCopyDirectory(b.path(page_dir), page.id, .{
                .include_extensions = &assets.supported_file_types,
            });
        }
    }

    fn header(self: DocPage, arena: Allocator, url_prefix: []const u8) ![]const u8 {
        _ = url_prefix;
        errdefer log.err("error while rendering '{s}' header", .{self.path});

        var html = try Html.create(arena);
        try html.write(
            \\<article>
            \\  <div class="header">
            \\    <h1>$title</h1>
            \\  </div>
            \\  <div class="content">
        , .{
            .title = self.title,
        });

        return html.string();
    }

    fn footer(
        self: DocPage,
        arena: Allocator,
        post_prev: ?DocPage,
        post_next: ?DocPage,
    ) ![]const u8 {
        errdefer log.err("error while rendering '{s}' footer", .{self.path});

        var html = try Html.create(arena);
        try html.write(
            \\  </div>
            \\</article>
            \\<nav id="navigation-buttons">
            \\  $button_next
            \\  $button_prev
            \\</nav>
        , .{
            .button_next = if (post_next) |post| try nav_button(
                post,
                try html.child(),
                "← Next",
            ) else "",
            .button_prev = if (post_prev) |post| try nav_button(
                post,
                try html.child(),
                "Previous →",
            ) else "",
        });

        return html.string();
    }

    fn nav_button(self: DocPage, html: *Html, label: []const u8) ![]const u8 {
        try html.write(
            \\<a class="button" href="../$id">
            \\  <div class="vstack">
            \\    <p class="label">$label</p>
            \\    <h2>$title</h2>
            \\  </div>
            \\</a>
        , .{
            .id = self.id,
            .label = label,
            .title = self.title,
        });
        return html.string();
    }
};

fn make_title(arena: Allocator, input: []const u8) ![]const u8 {
    const output = try arena.dupe(u8, input);
    var needs_upper = true;
    for (output) |*c| {
        if (needs_upper) {
            c.* = std.ascii.toUpper(c.*);
            needs_upper = false;
        }
        switch (c.*) {
            ' ' => needs_upper = true,
            else => {},
        }
    }
    return output;
}

fn path_exists(path: []const u8) !bool {
    std.fs.cwd().access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
