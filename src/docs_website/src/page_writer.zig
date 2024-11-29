const std = @import("std");
const assert = std.debug.assert;
const Website = @import("website.zig").Website;
const Html = @import("html.zig").Html;

const page_template = @embedFile("html/page.html");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    const args = try std.process.argsAlloc(allocator);
    assert(args.len == 7);
    const title = args[1];
    const author = args[2];
    const url_prefix = args[3];
    const nav_file_path = args[4];
    const source_file_path = args[5];
    const target_file_path = args[6];

    const nav = try std.fs.cwd().readFileAlloc(
        allocator,
        nav_file_path,
        Website.file_size_max,
    );
    const content = try std.fs.cwd().readFileAlloc(
        allocator,
        source_file_path,
        Website.file_size_max,
    );
    var html = try Html.create(allocator);
    try html.write(page_template, .{
        .title = title,
        .author = author,
        .url_prefix = url_prefix,
        .nav = nav,
        .content = content,
    });
    try std.fs.cwd().writeFile(.{ .sub_path = target_file_path, .data = html.string() });
}
