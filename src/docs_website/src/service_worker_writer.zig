const std = @import("std");
const Html = @import("html.zig").Html;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    var args = std.process.args();
    _ = args.skip();
    const url_prefix = args.next().?;
    const cache_name = args.next().?;
    const search_path = args.next().?;

    const file_paths = try collect_files(allocator, url_prefix, search_path);
    try write_service_worker(allocator, cache_name, file_paths);
}

fn collect_files(
    arena: std.mem.Allocator,
    url_prefix: []const u8,
    search_path: []const u8,
) ![]const []const u8 {
    var file_paths = std.ArrayList([]const u8).init(arena);

    var dir = try std.fs.cwd().openDir(search_path, .{ .iterate = true });
    defer dir.close();

    var walker = try dir.walk(arena);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind == .file) {
            // Normalize requests by using directory with trailing slash instead of index.html.
            if (std.mem.endsWith(u8, entry.path, "index.html")) {
                const stripped = entry.path[0 .. entry.path.len - "index.html".len];
                try file_paths.append(try std.mem.join(arena, "/", &.{ url_prefix, stripped }));
            } else {
                try file_paths.append(try std.mem.join(arena, "/", &.{ url_prefix, entry.path }));
            }
        }
    }

    return file_paths.toOwnedSlice();
}

fn write_service_worker(
    arena: std.mem.Allocator,
    cache_name: []const u8,
    file_paths: []const []const u8,
) !void {
    const template = @embedFile("js/service-worker.js");

    const file_paths_json = try std.json.stringifyAlloc(arena, file_paths, .{});

    var html = try Html.create(arena);
    try html.write(template, .{
        .cache_name = cache_name,
        .files_to_cache = file_paths_json,
    });

    try std.io.getStdOut().writer().print("{s}", .{html.string()});
}
