const std = @import("std");
const log = std.log.scoped(.file_checker);
const assets = @import("assets.zig");

pub const file_size_max = 900 << 10;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    var args = std.process.args();
    _ = args.skip();
    while (args.next()) |arg| {
        const path = arg;
        try check_files(allocator, path);
    }
}

fn check_files(arena: std.mem.Allocator, path: []const u8) !void {
    const dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    var walker = try dir.walk(arena);
    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;

        const stat = dir.statFile(entry.path) catch |err| {
            log.err("unable to stat file '{s}': {s}", .{
                try dir.realpathAlloc(arena, entry.path),
                @errorName(err),
            });
            return err;
        };
        if (stat.size > file_size_max) {
            log.err("file '{s}' with size {:.2} exceeds max file size of {:.2}", .{
                try dir.realpathAlloc(arena, entry.path),
                std.fmt.fmtIntSizeBin(stat.size),
                std.fmt.fmtIntSizeBin(file_size_max),
            });
            return error.FileSizeExceeded;
        }

        const file_type = for (assets.supported_file_types) |file_type| {
            if (std.mem.endsWith(u8, entry.path, file_type.extension)) break file_type;
        } else {
            log.err("file '{s}' has unsupported type '{s}'", .{
                try dir.realpathAlloc(arena, entry.path),
                std.fs.path.extension(entry.path),
            });
            return error.UnsupportedFileType;
        };

        if (!file_type.is_binary) {
            const file = try dir.openFile(entry.path, .{});
            defer file.close();
            try file.seekFromEnd(-1);
            const last_byte = try file.reader().readByte();
            if (last_byte != '\n') {
                log.err("file '{s}' doesn't end with a newline", .{
                    try dir.realpathAlloc(arena, entry.path),
                });
                return error.MissingNewline;
            }
        }
    }
}
