const std = @import("std");
const Website = @import("website.zig").Website;

const Entry = struct {
    path: []const u8,
    html: []const u8,
};

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();

    var args = init.minimal.args.iterate();
    _ = args.skip();

    var entries: std.ArrayList(Entry) = .empty;
    while (args.next()) |path| {
        const html = args.next().?;
        const entry = Entry{
            .path = path,
            .html = try std.Io.Dir.cwd().readFileAlloc(
                init.io,
                html,
                allocator,
                .limited(Website.file_size_max),
            ),
        };
        try entries.append(allocator, entry);
    }

    const json_string = try std.json.Stringify.valueAlloc(allocator, entries.items, .{});
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &.{});
    try stdout.interface.print("{s}\n", .{json_string});
}
