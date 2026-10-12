const std = @import("std");
const Html = @import("html.zig").Html;

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    var args = init.minimal.args.iterate();
    _ = args.skip();
    const url_prefix = args.next().?;
    const cache_name = args.next().?;
    const search_path = args.next().?;

    const file_paths = try collect_files(allocator, init.io, url_prefix, search_path);
    try write_service_worker(allocator, init.io, cache_name, file_paths);
}

fn collect_files(
    arena: std.mem.Allocator,
    io: std.Io,
    url_prefix: []const u8,
    search_path: []const u8,
) ![]const []const u8 {
    var file_paths: std.ArrayList([]const u8) = .empty;

    var dir = try std.Io.Dir.cwd().openDir(io, search_path, .{ .iterate = true });
    defer dir.close(io);

    var walker = try dir.walk(arena);
    defer walker.deinit();

    while (try walker.next(io)) |entry| {
        if (entry.kind == .file) {
            // Normalize requests by using directory with trailing slash instead of index.html.
            if (std.mem.endsWith(u8, entry.path, "index.html")) {
                const stripped = entry.path[0 .. entry.path.len - "index.html".len];
                const path = try std.mem.join(arena, "/", &.{ url_prefix, stripped });
                try file_paths.append(arena, path);
            } else {
                const path = try std.mem.join(arena, "/", &.{ url_prefix, entry.path });
                try file_paths.append(arena, path);
            }
        }
    }

    return file_paths.toOwnedSlice(arena);
}

fn write_service_worker(
    arena: std.mem.Allocator,
    io: std.Io,
    cache_name: []const u8,
    file_paths: []const []const u8,
) !void {
    const template = @embedFile("js/service-worker.js");

    const file_paths_json = try std.json.Stringify.valueAlloc(arena, file_paths, .{});

    var html = try Html.create(arena);
    try html.write(template, .{
        .cache_name = cache_name,
        .files_to_cache = file_paths_json,
    });

    try std.Io.File.stdout().writeStreamingAll(io, html.string());
}
