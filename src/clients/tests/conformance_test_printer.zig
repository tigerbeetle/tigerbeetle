const std = @import("std");
const assert = std.debug.assert;

const ast = @import("conformance_test_ast.zig");
const Arena = @import("conformance_test_arena.zig").Arena;

pub const Printer = struct {
    // For GO* see: https://github.com/golang/go/wiki/CodeReviewComments#initialisms
    const Casing = enum {
        snake_case,
        camelCase,
        PascalCase,
        UPPER_CASE,
        GOPascalCase,
        GOCamelCase,
    };

    pub const Options = struct {
        indent_width: u32,
        indent_char: u8 = ' ',
        start_level: u32 = 0,
        comment_marker: []const u8 = "//",
    };

    arena: *Arena,
    writer: *std.Io.Writer,
    indent_width: u32,
    indent_char: u8 = ' ',
    // Start of outermost generated declaration. `reset_indent` returns to this.
    start_level: u32,
    level: u32,
    comment_marker: []const u8 = "//",

    pub fn init(arena: *Arena, writer: *std.Io.Writer, options: Options) Printer {
        return .{
            .arena = arena,
            .writer = writer,
            .indent_width = options.indent_width,
            .indent_char = options.indent_char,
            .start_level = options.start_level,
            .level = options.start_level,
            .comment_marker = options.comment_marker,
        };
    }

    pub fn indent(printer: *Printer) void {
        printer.level += 1;
    }

    pub fn dedent(printer: *Printer) void {
        assert(printer.level > printer.start_level);
        printer.level -= 1;
    }

    pub fn reset_indent(printer: *Printer) void {
        printer.level = printer.start_level;
    }

    // Starts a line the caller has to finish.
    pub fn write_indent(printer: Printer) !void {
        const width = printer.level * printer.indent_width;
        try printer.writer.splatByteAll(printer.indent_char, width);
    }

    pub fn write_padding(printer: Printer, count: usize) !void {
        try printer.writer.splatByteAll(' ', count);
    }

    pub fn write(printer: Printer, bytes: []const u8) !void {
        try printer.writer.writeAll(bytes);
    }

    pub fn print(printer: Printer, comptime fmt: []const u8, args: anytype) !void {
        try printer.writer.print(fmt, args);
    }

    pub fn write_empty_line(printer: Printer) !void {
        try printer.write("\n");
    }

    pub fn write_indented(printer: Printer, bytes: []const u8) !void {
        try printer.write_indent();
        try printer.write(bytes);
        try printer.write("\n");
    }

    pub fn print_indented(printer: Printer, comptime fmt: []const u8, args: anytype) !void {
        try printer.write_indent();
        try printer.print(fmt ++ "\n", args);
    }

    pub fn write_omission(printer: Printer, case: ast.Case) !void {
        try printer.print_indented("{s} Omitted: \"{s}\"", .{
            printer.comment_marker, case.description,
        });
        try printer.write_indent();
        try printer.print("{s} Reason: ", .{printer.comment_marker});
        for (@tagName(case.requirement.?)) |char| {
            try printer.writer.writeByte(if (char == '_') ' ' else char);
        }
        try printer.write("\n");
    }

    pub fn string_alloc(printer: *Printer, comptime fmt: []const u8, args: anytype) ![]const u8 {
        return std.fmt.allocPrint(printer.arena.allocator(), fmt, args);
    }

    pub fn to_case_alloc(printer: *Printer, casing: Casing, input: []const u8) ![]const u8 {
        var result = try std.ArrayList(u8).initCapacity(printer.arena.allocator(), input.len);
        var words = Words.init(input);
        while (words.next()) |word| {
            const first = result.items.len == 0;
            switch (casing) {
                .snake_case, .UPPER_CASE => {
                    if (!first) result.appendAssumeCapacity('_');
                    for (word) |char| result.appendAssumeCapacity(
                        if (casing == .UPPER_CASE) std.ascii.toUpper(char) else char,
                    );
                },
                .camelCase, .PascalCase, .GOPascalCase, .GOCamelCase => {
                    const go = casing == .GOPascalCase or casing == .GOCamelCase;
                    const lowercase = first and (casing == .camelCase or casing == .GOCamelCase);
                    if (go and is_go_initialism(word)) {
                        for (word) |char| result.appendAssumeCapacity(
                            if (lowercase) std.ascii.toLower(char) else std.ascii.toUpper(char),
                        );
                    } else {
                        const initial = if (lowercase)
                            std.ascii.toLower(word[0])
                        else
                            std.ascii.toUpper(word[0]);
                        result.appendAssumeCapacity(initial);
                        result.appendSliceAssumeCapacity(word[1..]);
                    }
                },
            }
        }
        return result.items;
    }
};

// Any run of characters that isn't alphanumeric is a word break for conversion purposes. With
// `include_underscore`, `_` is part of a word.
pub const Words = struct {
    input: []const u8,
    index: usize = 0,
    include_underscore: bool = false,

    fn init(input: []const u8) Words {
        return .{ .input = input };
    }

    pub fn next(words: *Words) ?[]const u8 {
        var start: ?usize = null;
        while (words.index < words.input.len) : (words.index += 1) {
            if (words.is_word_char(words.input[words.index])) {
                if (start == null) start = words.index;
            } else if (start != null) {
                break;
            }
        }
        const word_start = start orelse return null;
        return words.input[word_start..words.index];
    }

    fn is_word_char(words: *const Words, char: u8) bool {
        return std.ascii.isAlphanumeric(char) or (char == '_' and words.include_underscore);
    }
};

fn is_go_initialism(word: []const u8) bool {
    return std.ascii.eqlIgnoreCase(word, "id") or std.ascii.eqlIgnoreCase(word, "ok");
}
