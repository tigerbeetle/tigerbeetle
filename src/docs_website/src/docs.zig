const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.blog);
const Website = @import("website.zig").Website;
const assets = @import("assets.zig");
const Html = @import("html.zig").Html;

const Redirect = struct {
    old: []const u8,
    new: []const u8,
};
const redirects = [_]Redirect{};

pub fn build(
    b: *std.Build,
    website: Website,
    source_dir: []const u8,
) !std.Build.LazyPath {
    const arena = b.allocator;
    const blog = b.addWriteFiles();

    const posts = try DocPage.find_all(arena, source_dir);
    std.mem.sort(DocPage, posts, {}, DocPage.desc);

    for (posts, 0..) |post, i| {
        const post_prev = if (i < posts.len - 1) posts[i + 1] else null;
        const post_next = if (i > 0) posts[i - 1] else null;
        try post.install(b, website, blog, post_prev, post_next);
    }

    // Handle redirects by writing a redirect HTML file to the old directory name.
    for (redirects) |redirect| {
        const redirect_target_exists = for (posts) |post| {
            if (std.mem.eql(u8, post.id, redirect.new)) break true;
        } else false;
        assert(redirect_target_exists);

        const url_new = try std.mem.concat(arena, u8, &.{
            website.url_prefix,
            "/blog/",
            redirect.new,
        });
        const path_old = b.pathJoin(&.{ redirect.old, "index.html" });
        _ = blog.add(path_old, try Html.redirect(arena, url_new));
    }

    return blog.getDirectory();
}

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

        // Cut off title and "by" line.
        self.content = source[line_it.index..];
    }

    fn desc(context: void, lhs: DocPage, rhs: DocPage) bool {
        _ = context;
        assert(!std.mem.eql(u8, lhs.title, rhs.title));
        return !std.mem.lessThan(u8, lhs.title, rhs.title);
    }

    fn find_all(arena: Allocator, base_path: []const u8) ![]DocPage {
        var doc_pages = std.ArrayList(DocPage).init(arena);

        var dir = std.fs.cwd().openDir(base_path, .{ .iterate = true }) catch |err| {
            log.err("unable to open path '{s}'", .{base_path});
            return err;
        };
        defer dir.close();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                const path = try std.fs.path.join(arena, &.{ base_path, entry.name });
                const doc_page = try DocPage.init(arena, path);
                try doc_pages.append(doc_page);
            }
        }

        return doc_pages.items;
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

    fn header(post: DocPage, arena: Allocator, url_prefix: []const u8) ![]const u8 {
        _ = url_prefix;
        errdefer log.err("error while rendering '{s}' header", .{post.path});

        var html = try Html.create(arena);
        try html.write(
            \\<article id="DocPage">
            \\  <div class="header">
            \\    <h1>$title</h1>
            \\  </div>
            \\  <div class="content">
        , .{
            .title = post.title,
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

    fn nav_button(post: DocPage, html: *Html, label: []const u8) ![]const u8 {
        try html.write(
            \\<a class="button" href="../$id">
            \\  <div class="vstack">
            \\    <p class="label">$label</p>
            \\    <h2>$title</h2>
            \\  </div>
            \\</a>
        , .{
            .id = post.id,
            .label = label,
            .title = post.title,
        });
        return html.string();
    }
};

fn path_exists(path: []const u8) !bool {
    std.fs.cwd().access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
