const std = @import("std");
const assert = std.debug.assert;
const Website = @import("website.zig").Website;
const Html = @import("html.zig").Html;

const page_template = @embedFile("html/page.html");
const search_box_template = @embedFile("html/search-box.html");
const search_results_template = @embedFile("html/search-results.html");
const search_script_template = "<script src=\"$url_prefix/js/search.js\"></script>";
const page_script = @embedFile("js/page-script.js");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    const args = try std.process.argsAlloc(allocator);
    assert(args.len == 9);
    const title = args[1];
    const author = args[2];
    const url_prefix = args[3];
    const page_path = args[4];
    const include_search = std.mem.eql(u8, args[5], "true");
    const nav = args[6];
    const source_file_path = args[7];
    const target_file_path = args[8];

    var script = try Html.create(allocator);
    try script.write(page_script, .{ .url_prefix = url_prefix });

    var script_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(script.string(), &script_hash, .{});
    const b64_encoder = std.base64.standard.Encoder;
    var script_hash_b64_buf: [b64_encoder.calcSize(script_hash.len)]u8 = undefined;
    const script_hash_b64 = b64_encoder.encode(&script_hash_b64_buf, &script_hash);

    const content = try std.fs.cwd().readFileAlloc(
        allocator,
        source_file_path,
        Website.file_size_max,
    );
    var html = try Html.create(allocator);
    var search_box = try html.child();
    var search_results = try html.child();
    var search_script = try html.child();
    if (include_search) {
        try search_box.write(search_box_template, .{});
        try search_results.write(search_results_template, .{ .url_prefix = url_prefix });
        try search_script.write(search_script_template, .{ .url_prefix = url_prefix });
    }
    try html.write(page_template, .{
        .page_script_hash = script_hash_b64,
        .title = title,
        .author = author,
        .url_prefix = url_prefix,
        .page_path = page_path,
        .nav = nav,
        .search_box = search_box,
        .search_results = search_results,
        .content = content,
        .page_script = script,
        .search_script = search_script,
    });
    try std.fs.cwd().writeFile(.{ .sub_path = target_file_path, .data = html.string() });
}
