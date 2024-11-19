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

    const menu = try DocPage.find_all(arena, "TigerBeetle Docs", source_dir, source_dir);

    var nav_html = try Html.create(arena);
    try menu.write_links(nav_html);
    _ = docs.add("nav.html", nav_html.string());

    try menu.install(b, website, docs);

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
            if (menu.index_page) |page| {
                try html.write("<summary><a href=\"$url\">$title</a></summary>", .{
                    .url = page.path_target,
                    .title = try html.from_md(menu.title), // Fabio: index page titles are too long
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
                .url = page.path_target,
                .title = try html.from_md(page.title),
            });
        }
        try html.write("</ul>", .{});
    }

    fn install(
        self: Menu,
        b: *std.Build,
        website: Website,
        docs: *std.Build.Step.WriteFile,
    ) !void {
        if (self.index_page) |index_page| {
            try index_page.install(b, website, docs, null, null);
        }
        for (self.menus) |menu| {
            try menu.install(b, website, docs);
        }
        for (self.pages, 0..) |page, i| {
            const page_prev = if (i < self.pages.len - 1) self.pages[i + 1] else null;
            const page_next = if (i > 0) self.pages[i - 1] else null;
            try page.install(b, website, docs, page_prev, page_next);
        }
    }
};

const DocPage = struct {
    path_source: []const u8,
    path_target: []const u8,

    // Parsed from Markdown content.
    title: []const u8,
    content: []const u8,

    fn init(arena: Allocator, base_path: []const u8, path_source: []const u8) !DocPage {
        assert(std.mem.endsWith(u8, path_source, ".md"));
        var path_target = path_source[base_path.len + 1 ..];
        if (std.mem.endsWith(u8, path_target, "/README.md")) {
            path_target = path_target[0 .. path_target.len - "/README.md".len];
        } else {
            path_target = path_target[0 .. path_target.len - ".md".len];
        }
        var post: DocPage = .{
            .path_source = path_source,
            .path_target = path_target,
            .title = undefined,
            .content = undefined,
        };
        try post.load(arena);
        return post;
    }

    fn load(self: *DocPage, arena: Allocator) !void {
        errdefer log.err("error while loading '{s}'", .{self.path_source});

        const source = try std.fs.cwd().readFileAlloc(arena, self.path_source, Website.file_size_max);
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

    fn find_all(arena: Allocator, title: []const u8, base_path: []const u8, path: []const u8) !Menu {
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
                const page = try DocPage.init(arena, base_path, page_path);
                if (std.mem.eql(u8, entry.name, "README.md")) {
                    assert(index_page == null);
                    index_page = page;
                } else {
                    try pages.append(page);
                }
            } else if (entry.kind == .directory) {
                const menu_path = try std.fs.path.join(arena, &.{ path, entry.name });
                const menu_title = try make_title(arena, entry.name);
                try menus.append(try find_all(arena, menu_title, base_path, menu_path));
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
        self: DocPage,
        b: *std.Build,
        website: Website,
        docs: *std.Build.Step.WriteFile,
        page_prev: ?DocPage,
        page_next: ?DocPage,
    ) !void {
        const arena = b.allocator;
        const temp = b.addWriteFiles();
        const header_path = temp.add("header.html", try self.header(arena, website.url_prefix));
        const content_path = temp.add("content.md", self.content);
        const footer_path = temp.add("footer.html", try self.footer(
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
            .title = self.title,
            .content = combined,
        });
        _ = docs.addCopyFile(page_path, b.pathJoin(&.{ self.path_target, "index.html" }));

        // If it exists, copy the page's asset directory.
        const page_dir = self.path_source[0 .. self.path_source.len - ".md".len];
        if (try path_exists(b.pathFromRoot(page_dir))) {
            _ = docs.addCopyDirectory(b.path(page_dir), self.path_target, .{
                .include_extensions = &assets.supported_file_types,
            });
        }
    }

    fn header(self: DocPage, arena: Allocator, url_prefix: []const u8) ![]const u8 {
        _ = url_prefix;
        errdefer log.err("error while rendering '{s}' header", .{self.path_target});

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
        errdefer log.err("error while rendering '{s}' footer", .{self.path_target});

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
            \\<a class="button" href="../$url">
            \\  <div class="vstack">
            \\    <p class="label">$label</p>
            \\    <h2>$title</h2>
            \\  </div>
            \\</a>
        , .{
            .url = self.path_target,
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
