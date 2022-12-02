const std = @import("std");
const assert = std.debug.assert;

/// Parse a "table" of data with the specified schema.
/// See test cases for example usage.
// TODO(Zig): Change this to a a purely comptime function returning a slice
// once Zig's "runtime value cannot be passed to comptime arg" bugs are fixed.
pub fn parse(comptime Row: type, comptime table_string: []const u8) !std.BoundedArray(Row, 64) {
    var rows = std.BoundedArray(Row, 64).init(0) catch unreachable;
    var row_strings = std.mem.tokenize(u8, table_string, "\n");
    while (row_strings.next()) |row_string| {
        // Ignore blank line.
        if (row_string.len == 0) continue;

        var columns = std.mem.tokenize(u8, row_string, " ");
        const row = parse_data(Row, &columns);
        rows.appendAssumeCapacity(row);

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
            inline for (.{ "0", "false", "F" }) |t| {
                if (std.mem.eql(u8, token, t)) return false;
            }
            inline for (.{ "1", "true", "T" }) |t| {
                if (std.mem.eql(u8, token, t)) return true;
            }
            std.debug.panic("Unknown boolean: {s}", .{token});
        },
        .Int => |info| {
            const max = std.math.maxInt(Data);
            const token = tokens.next().?;
            // If the first character is a letter ("a-zA-Z"), ignore it. (For example, "A1" → 1).
            // This serves as a comment, to help visually disambiguate sequential integer columns.
            const offset: usize = if (std.ascii.isAlpha(token[0])) 1 else 0;
            // Negative unsigned values are computed relative to the maxInt.
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
                    if (eat(tokens, "_")) {
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

fn eat(tokens: *std.mem.TokenIterator(u8), token: []const u8) bool {
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
    const rows = try parse(struct {
        a: u8,
    },
        \\
        \\ 1 // Comment
        \\
    );

    try std.testing.expectEqual(rows.len, 1);
    try std.testing.expectEqual(rows.get(0), .{ .a = 1 });
}

test "enum" {
    const rows = try parse(enum { a, b, c },
        \\ c
        \\ b
        \\ a
    );

    try std.testing.expectEqual(rows.len, 3);
    try std.testing.expectEqual(rows.get(0), .c);
    try std.testing.expectEqual(rows.get(1), .b);
    try std.testing.expectEqual(rows.get(2), .a);
}

test "bool" {
    const rows = try parse(struct { i: bool },
        \\ 0
        \\ 1
        \\ false
        \\ true
        \\ F
        \\ T
    );

    try std.testing.expectEqual(rows.len, 6);
    try std.testing.expectEqual(rows.get(0), .{ .i = false });
    try std.testing.expectEqual(rows.get(1), .{ .i = true });
    try std.testing.expectEqual(rows.get(2), .{ .i = false });
    try std.testing.expectEqual(rows.get(3), .{ .i = true });
    try std.testing.expectEqual(rows.get(4), .{ .i = false });
    try std.testing.expectEqual(rows.get(5), .{ .i = true });
}

test "int" {
    const rows = try parse(struct { i: usize },
        \\ 1
        \\ 2
        \\ A3
        \\ a4
        // For unsigned integers, `-n` is interpreted as `maxInt(Int) - n`.
        \\ -5
        \\ -0
    );

    try std.testing.expectEqual(rows.len, 6);
    try std.testing.expectEqual(rows.get(0), .{ .i = 1 });
    try std.testing.expectEqual(rows.get(1), .{ .i = 2 });
    try std.testing.expectEqual(rows.get(2), .{ .i = 3 });
    try std.testing.expectEqual(rows.get(3), .{ .i = 4 });
    try std.testing.expectEqual(rows.get(4), .{ .i = std.math.maxInt(usize) - 5 });
    try std.testing.expectEqual(rows.get(5), .{ .i = std.math.maxInt(usize) });
}

test "struct" {
    const rows = try parse(struct {
        c1: enum { a, b, c, d },
        c2: u8,
        c3: u16 = 30,
        c4: ?u32 = null,
        c5: bool = false,
    },
        \\ a 1 10 1000 1
        \\ b 2 20    _ T
        \\ c 3  _    _ F
        \\ d 4  _    _ _
    );

    try std.testing.expectEqual(rows.len, 4);
    try std.testing.expectEqual(rows.get(0), .{ .c1 = .a, .c2 = 1, .c3 = 10, .c4 = 1000, .c5 = true });
    try std.testing.expectEqual(rows.get(1), .{ .c1 = .b, .c2 = 2, .c3 = 20, .c4 = null, .c5 = true });
    try std.testing.expectEqual(rows.get(2), .{ .c1 = .c, .c2 = 3, .c3 = 30, .c4 = null, .c5 = false });
    try std.testing.expectEqual(rows.get(3), .{ .c1 = .d, .c2 = 4, .c3 = 30, .c4 = null, .c5 = false });
}

test "struct (nested)" {
    const rows = try parse(struct {
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

    try std.testing.expectEqual(rows.len, 2);
    try std.testing.expectEqual(rows.get(0), .{ .a = 1, .b = .{ .b1 = 2, .b2 = 3 }, .c = 4 });
    try std.testing.expectEqual(rows.get(1), .{ .a = 5, .b = .{ .b1 = 6, .b2 = 7 }, .c = 8 });
}

test "union" {
    const rows = try parse(union(enum) {
        a: struct { b: u8, c: i8 },
        d: u8,
        e: void,
    },
        \\a 1 -2
        \\d 3
        \\e
    );

    try std.testing.expectEqual(rows.len, 3);
    try std.testing.expectEqual(rows.get(0), .{ .a = .{ .b = 1, .c = -2 } });
    try std.testing.expectEqual(rows.get(1), .{ .d = 3 });
    try std.testing.expectEqual(rows.get(2), .{ .e = {} });
}
