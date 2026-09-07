const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

const MiB = stdx.MiB;

const ast = @import("conformance_test_ast.zig");

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

    // What a generator needs to build a whole file. Callers are free to pass a different size.
    pub const default_memory_size = 2 * MiB;

    fba: std.heap.FixedBufferAllocator,
    writer: *std.Io.Writer,
    indent_width: u32,
    indent_char: u8 = ' ',
    // Start of outermost generated declaration. `reset_indent` returns to this.
    start_level: u32,
    level: u32,
    comment_marker: []const u8 = "//",

    pub fn init(memory: []u8, writer: *std.Io.Writer, options: Options) Printer {
        return .{
            .fba = std.heap.FixedBufferAllocator.init(memory),
            .writer = writer,
            .indent_width = options.indent_width,
            .indent_char = options.indent_char,
            .start_level = options.start_level,
            .level = options.start_level,
            .comment_marker = options.comment_marker,
        };
    }

    pub fn arena(printer: *Printer) std.mem.Allocator {
        return printer.fba.allocator();
    }

    // The strings each case builds are temporary: the generators mark the current arena
    // emit a case, then release the memory back.
    pub fn mark(printer: *const Printer) usize {
        return printer.fba.end_index;
    }

    pub fn release(printer: *Printer, marked: usize) void {
        assert(marked <= printer.fba.end_index);
        printer.fba.end_index = marked;
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
        return std.fmt.allocPrint(printer.arena(), fmt, args);
    }

    pub fn to_case_alloc(printer: *Printer, casing: Casing, input: []const u8) ![]const u8 {
        var result = try std.ArrayList(u8).initCapacity(printer.arena(), input.len);
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

// Any run of characters that isn't alphanumeric is a word break for conversion purposes.
const Words = struct {
    input: []const u8,
    index: usize = 0,

    fn init(input: []const u8) Words {
        return .{ .input = input };
    }

    fn next(words: *Words) ?[]const u8 {
        _ = words.take(.separators);
        return words.take(.word);
    }

    // Consumes the run of `chars` under the cursor and returns it, or null if there are none.
    fn take(words: *Words, chars: enum { word, separators }) ?[]const u8 {
        const alphanumeric = chars == .word;
        const start = words.index;
        while (words.index < words.input.len and
            std.ascii.isAlphanumeric(words.input[words.index]) == alphanumeric)
        {
            words.index += 1;
        }
        return if (words.index > start) words.input[start..words.index] else null;
    }
};

fn is_go_initialism(word: []const u8) bool {
    return std.ascii.eqlIgnoreCase(word, "id") or std.ascii.eqlIgnoreCase(word, "ok");
}
