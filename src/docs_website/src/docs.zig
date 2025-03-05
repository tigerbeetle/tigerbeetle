const std = @import("std");
const Allocator = std.mem.Allocator;
const LazyPath = std.Build.LazyPath;
const Website = @import("website.zig").Website;
const Html = @import("html.zig").Html;
const content = @import("./content.zig");

const base_path = "../../docs";

const SearchIndexEntry = struct {
    page_path: []const u8,
    html_path: LazyPath,
};
const SearchIndex = std.ArrayList(SearchIndexEntry);

pub fn build(
    b: *std.Build,
    output: *std.Build.Step.WriteFile,
    website: Website,
) !void {
    const arena = b.allocator;

    var search_index = SearchIndex.init(arena);

    var page_buffer: [1 << 16]u8 = undefined;
    var base = try b.build_root.handle.openDir(base_path, .{});
    defer base.close();
    const root_page = try content.load(arena, base, &page_buffer);

    try tree_install(b, website, output, &search_index, root_page, root_page);

    const run_search_index_writer = b.addRunArtifact(b.addExecutable(.{
        .name = "search_index_writer",
        .root_source_file = b.path("src/search_index_writer.zig"),
        .target = b.graph.host,
    }));
    for (search_index.items) |entry| {
        run_search_index_writer.addArg(entry.page_path);
        run_search_index_writer.addFileArg(entry.html_path);
    }
    _ = output.addCopyFile(
        run_search_index_writer.captureStdOut(),
        "search-index.json",
    );

    try write_single_page(b, website, &search_index, root_page, output);

    try write_404_page(b, website, output);
}

fn tree_install(
    b: *std.Build,
    website: Website,
    output: *std.Build.Step.WriteFile,
    search_index: *SearchIndex,
    root: content.Page,
    page: content.Page,
) !void {
    try page_install(b, website, output, search_index, root, page);
    for (page.children) |child| try tree_install(b, website, output, search_index, root, child);
}

fn page_install(
    b: *std.Build,
    website: Website,
    output: *std.Build.Step.WriteFile,
    search_index: *SearchIndex,
    root: content.Page,
    page: content.Page,
) !void {
    const page_html = run_pandoc(b, website.pandoc_bin, page.path);

    const page_path = page_url(b.allocator, page);

    try search_index.append(.{
        .page_path = page_path,
        .html_path = page_html,
    });

    const nav_html = try Html.create(b.allocator);
    try nav_fill(website, nav_html, root, .{ .target = page });

    try nav_html.write(
        @embedFile("html/single-page-link.html"),
        .{ .url_prefix = website.url_prefix },
    );

    // Add trailing slash.
    const page_path_canonical = if (page_path.len == 0) "" else b.fmt("{s}/", .{page_path});

    const page_out = website.write_page(.{
        .title = page.content.title,
        .page_path = page_path_canonical,
        .nav = nav_html.string(),
        .content = page_html,
    });
    _ = output.addCopyFile(page_out, b.pathJoin(&.{ page_path, "index.html" }));
}

fn page_url(arena: Allocator, page: content.Page) []const u8 {
    const url = cut_suffix(page.path, "/README.md") orelse cut_suffix(page.path, ".md").?;
    if (cut_prefix(url, "../src/clients/")) |client| {
        // Special case: docs for clients are in `/src/clients/$lang`, not under `/docs`.
        return std.mem.concat(arena, u8, &.{ "coding/clients/", client }) catch @panic("OOM");
    }

    if (url.len == 1 and url[0] == '.') return "";
    return cut_prefix(url, "./").?;
}

fn url2slug(arena: Allocator, url: []const u8) []const u8 {
    const slug = arena.dupe(u8, url) catch @panic("OOM");
    std.mem.replaceScalar(u8, slug, '/', '-');
    return std.mem.concat(arena, u8, &.{ "#", slug }) catch @panic("OOM");
}

fn nav_fill(website: Website, html: *Html, node: content.Page, options: struct {
    target: content.Page,
    single_page: bool = false,
}) !void {
    try html.write("<ol>\n", .{});
    for (node.children, node.content.children) |node_child, content_child| {
        var url = page_url(html.arena, node_child);
        if (options.single_page) {
            url = url2slug(html.arena, url);
        } else {
            url = try std.fmt.allocPrint(html.arena, "{s}/{s}/", .{ website.url_prefix, url });
        }

        if (node_child.children.len > 0) {
            try html.write("<li>\n<details", .{});
            if (nav_contains(node_child, options.target)) try html.write(" open", .{});
            try html.write("><summary class=\"item\">", .{});
            try html.write(
                \\<a href="$url">$title</a>
            , .{
                .url = url,
                // Fabio: index page titles are too long
                .title = content_child.title,
            });
            try html.write("</summary>\n", .{});
            try nav_fill(website, html, node_child, options);
            try html.write("</details></li>\n", .{});
        } else {
            try html.write(
                \\<li class="item"><a href="$url"$class>$title</a></li>
                \\
            , .{
                .url = url,
                .class = if (nav_same_page(node_child, options.target)) " class=\"target\"" else "",
                .title = content_child.title,
            });
        }
    }
    try html.write("</ol>\n", .{});
}

fn nav_contains(node: content.Page, target: content.Page) bool {
    if (nav_same_page(node, target)) return true;
    for (node.children) |child| {
        if (nav_contains(child, target)) return true;
    }
    return false;
}

fn nav_same_page(a: content.Page, b: content.Page) bool {
    return std.mem.eql(u8, a.path, b.path);
}

fn run_pandoc(
    b: *std.Build,
    pandoc_bin: std.Build.LazyPath,
    source: []const u8,
) std.Build.LazyPath {
    const pandoc_step = std.Build.Step.Run.create(b, "run pandoc");
    pandoc_step.addFileArg(pandoc_bin);
    pandoc_step.addArgs(&.{ "--from", "gfm+smart", "--to", "html5" });
    pandoc_step.addPrefixedFileArg("--lua-filter=", b.path("pandoc/markdown-links.lua"));
    pandoc_step.addPrefixedFileArg("--lua-filter=", b.path("pandoc/anchor-links.lua"));
    pandoc_step.addPrefixedFileArg("--lua-filter=", b.path("pandoc/table-wrapper.lua"));
    pandoc_step.addPrefixedFileArg("--lua-filter=", b.path("pandoc/code-block-buttons.lua"));
    pandoc_step.addPrefixedFileArg("--lua-filter=", b.path("pandoc/edit-link-footer.lua"));
    pandoc_step.addArg("--reference-location=section");
    const result = pandoc_step.addPrefixedOutputFileArg("--output=", "pandoc-out.html");
    pandoc_step.addFileArg(b.path(base_path).path(b, source));
    return result;
}

fn write_single_page(
    b: *std.Build,
    website: Website,
    search_index: *const SearchIndex,
    root: content.Page,
    docs: *std.Build.Step.WriteFile,
) !void {
    const run_single_page_writer = b.addRunArtifact(b.addExecutable(.{
        .name = "single_page_writer",
        .root_source_file = b.path("src/single_page_writer.zig"),
        .target = b.graph.host,
    }));
    for (search_index.items) |entry| {
        run_single_page_writer.addArg(entry.page_path);
        run_single_page_writer.addFileArg(entry.html_path);
    }
    const nav_html = try Html.create(b.allocator);
    try nav_fill(website, nav_html, root, .{ .target = root, .single_page = true });

    const single_page = website.write_page(.{
        .page_path = "single-page/",
        .include_search = false,
        .nav = nav_html.string(),
        .content = run_single_page_writer.captureStdOut(),
    });

    _ = docs.addCopyFile(single_page, "single-page/index.html");
}

fn write_404_page(
    b: *std.Build,
    website: Website,
    docs: *std.Build.Step.WriteFile,
) !void {
    const template = @embedFile("html/404.html");
    var html = try Html.create(b.allocator);
    try html.write(template, .{
        .url_prefix = website.url_prefix,
        .title = "Page not found",
        .author = "TigerBeetle Team",
    });
    _ = docs.add("404.html", html.string());
}

pub fn cut(haystack: []const u8, needle: []const u8) ?struct { []const u8, []const u8 } {
    const index = std.mem.indexOf(u8, haystack, needle) orelse return null;

    return .{ haystack[0..index], haystack[index + needle.len ..] };
}

pub fn cut_prefix(text: []const u8, comptime prefix: []const u8) ?[]const u8 {
    return if (std.mem.startsWith(u8, text, prefix))
        text[prefix.len..]
    else
        null;
}

pub fn cut_suffix(text: []const u8, comptime suffix: []const u8) ?[]const u8 {
    return if (std.mem.endsWith(u8, text, suffix))
        text[0 .. text.len - suffix.len]
    else
        null;
}
