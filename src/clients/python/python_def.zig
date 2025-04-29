const std = @import("std");
const assert = std.debug.assert;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    const args = try std.process.argsAlloc(allocator);
    assert(args.len == 2);

    std.log.info("WTF: {s}", .{args[1]});
    const python3_def = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024);
    const stdout = std.io.getStdOut().writer();

    try stdout.print("EXPORTS\n", .{});

    var iterator_line = std.mem.splitScalar(u8, python3_def, '\n');

    while (iterator_line.next()) |line| {
        // All the actual definitions start with a double space.
        if (!std.mem.startsWith(u8, line, "  ")) {
            continue;
        }

        var iterator_equals = std.mem.splitScalar(u8, line, '=');
        const symbol = iterator_equals.next().?;
        try stdout.print("  {s}", .{symbol});

        var iterator_space = std.mem.splitScalar(u8, iterator_equals.next().?, ' ');
        _ = iterator_space.next();

        if (iterator_space.next()) |data| {
            assert(std.mem.eql(u8, data, "DATA"));
            try stdout.print(" DATA", .{});
        }

        try stdout.print("\n", .{});

        assert(iterator_equals.next() == null);
    }
}
