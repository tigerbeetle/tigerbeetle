const std = @import("std");
const Website = @import("website.zig").Website;

const Entry = struct {
    path: []const u8,
    html: []const u8,
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();

    var args = std.process.args();
    _ = args.skip();

    var entries = std.ArrayList(Entry).init(allocator);
    while (args.next()) |path| {
        const html = args.next().?;
        const entry = Entry{
            .path = path,
            .html = try std.fs.cwd().readFileAlloc(allocator, html, Website.file_size_max),
        };
        try entries.append(entry);
    }

    const json_string = try std.json.stringifyAlloc(allocator, entries.items, .{});
    try std.io.getStdOut().writer().print("{s}\n", .{json_string});
}
