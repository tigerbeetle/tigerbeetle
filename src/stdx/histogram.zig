//! Integer-only vertical histogram formatting.
//! The caller provides the buckets and counts.
const std = @import("std");
const assert = std.debug.assert;

counts: []const u64,
max_count: u64,
height: u16 = 8,
show_counts: bool = true,

const Histogram = @This();

pub fn format(self: Histogram, writer: *std.Io.Writer) !void {
    assert(self.counts.len > 0);
    assert(self.height > 0);
    assert(self.max_count > 0);

    for (self.counts) |count| assert(count <= self.max_count);

    const levels = [_][]const u8{ " ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█" };

    try writer.print("count (max {})\n", .{self.max_count});

    var row = self.height;
    while (row > 0) {
        row -= 1;
        try writer.writeAll("|");

        for (self.counts, 0..) |count, index| {
            if (index > 0) try writer.writeAll(" ");
            // Floor to eighths of a cell without overflowing even for maxInt(u64) counts.
            const eighths = @as(u128, count) * self.height * 8 / self.max_count;
            const below = @as(u64, row) * 8;
            const level = @min(eighths -| below, 8);
            try writer.writeAll(levels[@intCast(level)]);
        }
        try writer.writeAll("|");
        if (row > 0 or self.show_counts) try writer.writeAll("\n");
    }

    if (self.show_counts) try writer.print("counts: {any}", .{self.counts});
}

test "histogram renders empty, fractional, full, and multirow bars" {
    const output = try std.fmt.allocPrint(std.testing.allocator, "{f}", .{Histogram{
        .counts = &.{ 0, 1, 8, 9, 16 },
        .max_count = 16,
        .height = 2,
    }});
    defer std.testing.allocator.free(output);

    try std.testing.expectEqualStrings(
        \\count (max 16)
        \\|      ▁ █|
        \\|  ▁ █ █ █|
        \\counts: { 0, 1, 8, 9, 16 }
    , output);
}

test "histogram handles u64 limits and optionally omits counts" {
    const output = try std.fmt.allocPrint(std.testing.allocator, "{f}", .{Histogram{
        .counts = &.{ 0, std.math.maxInt(u64) },
        .max_count = std.math.maxInt(u64),
        .height = 2,
        .show_counts = false,
    }});
    defer std.testing.allocator.free(output);

    try std.testing.expectEqualStrings(
        \\count (max 18446744073709551615)
        \\|  █|
        \\|  █|
    , output);
}
