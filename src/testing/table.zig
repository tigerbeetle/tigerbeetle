const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");

/// Parse a "table" of data with the specified schema.
/// See test cases for example usage.
pub fn parse(comptime Row: type, table_string: []const u8) stdx.BoundedArray(Row, 128) {
    var rows = stdx.BoundedArray(Row, 128){};
    var row_strings = std.mem.tokenizeAny(u8, table_string, "\n");
    while (row_strings.next()) |row_string| {
        // Ignore blank line.
        if (row_string.len == 0) continue;

        var columns = std.mem.tokenizeAny(u8, row_string, " ");
        const row = parse_data(Row, &columns);
        rows.append_assume_capacity(row);

        // Ignore trailing line comment.
        if (columns.next()) |last| assert(std.mem.eql(u8, last, "//"));
    }
    return rows;
}

fn parse_data(comptime Data: type, tokens: *std.mem.TokenIterator(u8, .any)) Data {
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
            const offset: usize = if (std.ascii.isAlphabetic(token[0])) 1 else 0;
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
                const Field = value_field.type;
                if (value_field.default_value) |ptr| {
                    if (eat(tokens, "_")) {
                        const value_ptr: *const Field = @ptrCast(@alignCast(ptr));
                        @field(data, value_field.name) = value_ptr.*;
                    } else {
                        @field(data, value_field.name) = parse_data(Field, tokens);
                    }
                } else {
                    @field(data, value_field.name) = parse_data(Field, tokens);
                }
            }
            return data;
        },
        .Array => |info| {
            var values: Data = undefined;
            for (values[0..]) |*value| {
                value.* = parse_data(info.child, tokens);
            }
            return values;
        },
        .Union => |info| {
            const variant_string = tokens.next().?;
            inline for (info.fields) |variant_field| {
                if (std.mem.eql(u8, variant_field.name, variant_string)) {
                    return @unionInit(
                        Data,
                        variant_field.name,
                        parse_data(variant_field.type, tokens),
                    );
                }
            }
            std.debug.panic("Unknown union variant: {s}", .{variant_string});
        },
        else => @compileError("Unimplemented column type: " ++ @typeName(Data)),
    };
}

fn eat(tokens: *std.mem.TokenIterator(u8, .any), token: []const u8) bool {
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
    std.debug.panic("Unknown field name={s} for type={}", .{ name, Enum });
}

fn test_parse(
    comptime Row: type,
    comptime rows_expect: []const Row,
    comptime string: []const u8,
) !void {
    const rows_actual = parse(Row, string).const_slice();
    try std.testing.expectEqual(rows_expect.len, rows_actual.len);

    for (rows_expect, 0..) |row, i| {
        try std.testing.expectEqual(row, rows_actual[i]);
    }
}

test "comment" {
    try test_parse(struct {
        a: u8,
    }, &.{
        .{ .a = 1 },
    },
        \\
        \\ 1 // Comment
        \\
    );
}

test "enum" {
    try test_parse(enum { a, b, c }, &.{ .c, .b, .a },
        \\ c
        \\ b
        \\ a
    );
}

test "bool" {
    try test_parse(struct { i: bool }, &.{
        .{ .i = false },
        .{ .i = true },
        .{ .i = false },
        .{ .i = true },
        .{ .i = false },
        .{ .i = true },
    },
        \\ 0
        \\ 1
        \\ false
        \\ true
        \\ F
        \\ T
    );
}

test "int" {
    try test_parse(struct { i: usize }, &.{
        .{ .i = 1 },
        .{ .i = 2 },
        .{ .i = 3 },
        .{ .i = 4 },
        .{ .i = std.math.maxInt(usize) - 5 },
        .{ .i = std.math.maxInt(usize) },
    },
        \\ 1
        \\ 2
        \\ A3
        \\ a4
        // For unsigned integers, `-n` is interpreted as `maxInt(Int) - n`.
        \\ -5
        \\ -0
    );
}

test "struct" {
    try test_parse(struct {
        c1: enum { a, b, c, d },
        c2: u8,
        c3: u16 = 30,
        c4: ?u32 = null,
        c5: bool = false,
    }, &.{
        .{ .c1 = .a, .c2 = 1, .c3 = 10, .c4 = 1000, .c5 = true },
        .{ .c1 = .b, .c2 = 2, .c3 = 20, .c4 = null, .c5 = true },
        .{ .c1 = .c, .c2 = 3, .c3 = 30, .c4 = null, .c5 = false },
        .{ .c1 = .d, .c2 = 4, .c3 = 30, .c4 = null, .c5 = false },
    },
        \\ a 1 10 1000 1
        \\ b 2 20    _ T
        \\ c 3  _    _ F
        \\ d 4  _    _ _
    );
}

test "struct (nested)" {
    try test_parse(struct {
        a: u32,
        b: struct {
            b1: u8,
            b2: u8,
        },
        c: u32,
    }, &.{
        .{ .a = 1, .b = .{ .b1 = 2, .b2 = 3 }, .c = 4 },
        .{ .a = 5, .b = .{ .b1 = 6, .b2 = 7 }, .c = 8 },
    },
        \\ 1 2 3 4
        \\ 5 6 7 8
    );
}

test "array" {
    try test_parse(struct {
        a: u32,
        b: [2]u32,
        c: u32,
    }, &.{
        .{ .a = 1, .b = .{ 2, 3 }, .c = 4 },
        .{ .a = 5, .b = .{ 6, 7 }, .c = 8 },
    },
        \\ 1 2 3 4
        \\ 5 6 7 8
    );
}

test "union" {
    try test_parse(union(enum) {
        a: struct { b: u8, c: i8 },
        d: u8,
        e: void,
    }, &.{
        .{ .a = .{ .b = 1, .c = -2 } },
        .{ .d = 3 },
        .{ .e = {} },
    },
        \\a 1 -2
        \\d 3
        \\e
    );
}
