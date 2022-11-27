const std = @import("std");
const assert = std.debug.assert;

/// Negative values for unsigned integers are interpreted as `maxInt(Int) - ...`.
// TODO(Zig): Change this to a a purely comptime function (no allocator) returning a slice
// once Zig's "runtime value cannot be passed to comptime arg" bugs are fixed.
pub fn parse(
    allocator: std.mem.Allocator,
    comptime Row: type,
    comptime table_string: []const u8,
) !std.ArrayList(Row) {
    var rows = std.ArrayList(Row).init(allocator);
    errdefer rows.deinit();

    var row_strings = std.mem.tokenize(u8, table_string, "\n");
    while (row_strings.next()) |row_string| {
        // Ignore blank line.
        if (row_string.len == 0) continue;

        var columns = std.mem.tokenize(u8, row_string, " ");
        try rows.append(parse_data(Row, &columns));

        // Ignore trailing line comment.
        if (columns.next()) |last| assert(std.mem.eql(u8, last, "//"));
    }
    return rows;
}

fn parse_data(comptime Data: type, tokens: *std.mem.TokenIterator(u8)) Data {
    return switch (@typeInfo(Data)) {
        .Optional => |info| parse_data(info.child, tokens),
        .Enum => field(Data, tokens.next().?),
        .Void => assert(tokens.next() == null),
        .Bool => {
            const token = tokens.next().?;
            inline for (.{ "0", "false", "F", "N" }) |t| {
                if (std.mem.eql(u8, token, t)) return false;
            }
            inline for (.{ "1", "true", "T", "Y" }) |t| {
                if (std.mem.eql(u8, token, t)) return true;
            }
            std.debug.panic("Unknown boolean: {s}", .{token});
        },
        .Int => |info| {
            const max = std.math.maxInt(Data);
            const token = tokens.next().?;
            //if (std.mem.eql(u8, token, "max")) return max;
            const offset: usize = if (std.ascii.isAlpha(token[0])) 1 else 0;
            if (info.signedness == .unsigned and token[offset] == '-') {
                return max - (std.fmt.parseInt(Data, token[offset + 1 ..], 10) catch unreachable);
            }
            return std.fmt.parseInt(Data, token[offset..], 10) catch unreachable;
        },
        .Struct => {
            var data: Data = undefined;
            inline for (std.meta.fields(Data)) |value_field| {
                // The repeated else branch seems to be necessary to keep Zig from complaining:
                //   control flow attempts to use compile-time variable at runtime
                if (value_field.default_value != null) {
                    if (take(tokens, "_")) {
                        @field(data, value_field.name) = value_field.default_value.?;
                    } else {
                        @field(data, value_field.name) = parse_data(value_field.field_type, tokens);
                    }
                } else {
                    @field(data, value_field.name) = parse_data(value_field.field_type, tokens);
                }
            }
            return data;
        },
        .Union => |info| {
            const variant_string = tokens.next().?;
            inline for (info.fields) |variant_field| {
                if (std.mem.eql(u8, variant_field.name, variant_string)) {
                    return @unionInit(
                        Data,
                        variant_field.name,
                        parse_data(variant_field.field_type, tokens),
                    );
                }
            }
            std.debug.panic("Unknown union variant: {s}", .{variant_string});
        },
        else => @compileError("Unimplemented column type: " ++ @typeName(Data)),
    };
}

fn take(tokens: *std.mem.TokenIterator(u8), token: []const u8) bool {
    var index_before = tokens.index;
    if (std.mem.eql(u8, tokens.next().?, token)) return true;
    tokens.index = index_before;
    return false;
}

/// TODO This function is a workaround for a comptime bug:
///   error: unable to evaluate constant expression
///   .Enum => @field(Column, column_string),
fn field(comptime Enum: type, name: []const u8) Enum {
    inline for (std.meta.fields(Enum)) |variant| {
        if (std.mem.eql(u8, variant.name, name)) {
            return @field(Enum, variant.name);
        }
    }
    std.debug.panic("Unkown field name={s} for type={}", .{ name, Enum });
}

test "comment" {
    const rows = try parse(std.testing.allocator, struct {
        a: u8,
    },
        \\
        \\ 1 // Comment
        \\
    );
    defer rows.deinit();

    try std.testing.expectEqual(rows.items.len, 1);
    try std.testing.expectEqual(rows.items[0], .{ .a = 1 });
}

test "int" {
    const rows = try parse(std.testing.allocator, struct { i: usize },
        \\ 1
        \\ 2
        \\ -3
        \\ A4
        \\ a5
        \\ -0
    );
    defer rows.deinit();

    try std.testing.expectEqual(rows.items.len, 6);
    try std.testing.expectEqual(rows.items[0], .{ .i = 1 });
    try std.testing.expectEqual(rows.items[1], .{ .i = 2 });
    try std.testing.expectEqual(rows.items[2], .{ .i = std.math.maxInt(usize) - 3 });
    try std.testing.expectEqual(rows.items[3], .{ .i = 4 });
    try std.testing.expectEqual(rows.items[4], .{ .i = 5 });
    try std.testing.expectEqual(rows.items[5], .{ .i = std.math.maxInt(usize) });
}

test "bool" {
    const rows = try parse(std.testing.allocator, struct { i: bool },
        \\ 0
        \\ 1
        \\ false
        \\ true
        \\ F
        \\ T
        \\ N
        \\ Y
    );
    defer rows.deinit();

    try std.testing.expectEqual(rows.items.len, 8);
    try std.testing.expectEqual(rows.items[0], .{ .i = false });
    try std.testing.expectEqual(rows.items[1], .{ .i = true });
    try std.testing.expectEqual(rows.items[2], .{ .i = false });
    try std.testing.expectEqual(rows.items[3], .{ .i = true });
    try std.testing.expectEqual(rows.items[4], .{ .i = false });
    try std.testing.expectEqual(rows.items[5], .{ .i = true });
    try std.testing.expectEqual(rows.items[6], .{ .i = false });
    try std.testing.expectEqual(rows.items[7], .{ .i = true });
}

test "struct" {
    const rows = try parse(std.testing.allocator, struct {
        c1: enum { a, b, c, d },
        c2: u8,
        c3: u16 = 30,
        c4: ?u32 = null,
        c5: bool = false,
    },
        \\ a 1 10 1000 1
        \\ b 2 20    _ T
        \\ c 3  _    _ Y
        \\ d 4  _    _ _
    );
    defer rows.deinit();

    try std.testing.expectEqual(rows.items.len, 4);
    try std.testing.expectEqual(rows.items[0], .{ .c1 = .a, .c2 = 1, .c3 = 10, .c4 = 1000, .c5 = true });
    try std.testing.expectEqual(rows.items[1], .{ .c1 = .b, .c2 = 2, .c3 = 20, .c4 = null, .c5 = true });
    try std.testing.expectEqual(rows.items[2], .{ .c1 = .c, .c2 = 3, .c3 = 30, .c4 = null, .c5 = true });
    try std.testing.expectEqual(rows.items[3], .{ .c1 = .d, .c2 = 4, .c3 = 30, .c4 = null, .c5 = false });
}

test "struct (nested)" {
    const rows = try parse(std.testing.allocator, struct {
        a: u32,
        b: struct {
            b1: u8,
            b2: u8,
        },
        c: u32,
    },
        \\ 1 2 3 4
        \\ 5 6 7 8
    );
    defer rows.deinit();

    try std.testing.expectEqual(rows.items.len, 2);
    try std.testing.expectEqual(rows.items[0], .{ .a = 1, .b = .{ .b1 = 2, .b2 = 3 }, .c = 4 });
    try std.testing.expectEqual(rows.items[1], .{ .a = 5, .b = .{ .b1 = 6, .b2 = 7 }, .c = 8 });
}

test "union" {
    const rows = try parse(std.testing.allocator, union(enum) {
        a: struct { b: u8, c: i8 },
        d: u8,
        e: void,
    },
        \\a 1 -2
        \\d 3
        \\e
    );
    defer rows.deinit();

    try std.testing.expectEqual(rows.items.len, 3);
    try std.testing.expectEqual(rows.items[0], .{ .a = .{ .b = 1, .c = -2 } });
    try std.testing.expectEqual(rows.items[1], .{ .d = 3 });
    try std.testing.expectEqual(rows.items[2], .{ .e = {} });
}
