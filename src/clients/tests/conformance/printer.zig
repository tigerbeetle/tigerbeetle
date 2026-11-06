//! This file defines a printer for the conformance test emitters.
//!
//! It encapsulates information regarding indents and comments, and exposes
//! methods for indenting and dedenting so users of the printer never need to
//! manually track the indent level.

const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");

const Arena = @import("arena.zig").Arena;

pub const Printer = struct {
    pub const Options = struct {
        indent_width: u32,
        indent_char: u8,
        comment_marker: []const u8,
        start_level: u32 = 0,
        /// List of words that must be fully capitalized when casing as
        /// PascalCase or camelCase.
        ///
        /// For example, "xml" and "http" are initialisms, and would be
        /// printed as `XMLHTTPRequest` in PascalCase and `xmlHTTPRequest` in
        /// camelCase.
        initialisms: ?[]const []const u8 = null,
    };

    arena: *Arena,
    writer: *std.Io.Writer,
    options: Options,
    level: u32,

    pub fn init(arena: *Arena, writer: *std.Io.Writer, options: Options) Printer {
        return .{
            .arena = arena,
            .writer = writer,
            .options = options,
            .level = options.start_level,
        };
    }

    pub fn indent(printer: *Printer) void {
        printer.level += 1;
        assert(printer.level > printer.options.start_level);
    }

    pub fn dedent(printer: *Printer) void {
        assert(printer.level > printer.options.start_level);
        printer.level -= 1;
    }

    pub fn reset_indent(printer: *Printer) void {
        assert(printer.level >= printer.options.start_level);
        printer.level = printer.options.start_level;
    }

    // Starts a line the caller has to finish.
    pub fn write_indent(printer: Printer) !void {
        const width = printer.level * printer.options.indent_width;
        try printer.writer.splatByteAll(printer.options.indent_char, width);
    }

    // Used for alignment, not indenting.
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

    pub fn string_alloc(printer: *Printer, comptime fmt: []const u8, args: anytype) ![]const u8 {
        return std.fmt.allocPrint(printer.arena.allocator(), fmt, args);
    }

    pub fn to_case_alloc(printer: *Printer, casing: stdx.Casing, input: []const u8) ![]const u8 {
        var result = try std.ArrayList(u8).initCapacity(printer.arena.allocator(), input.len);
        var words = Words.init(input);
        while (words.next()) |word| {
            const first = result.items.len == 0;
            switch (casing) {
                .snake_case, .upper_case => {
                    if (!first) result.appendAssumeCapacity('_');
                    for (word) |char| result.appendAssumeCapacity(
                        if (casing == .upper_case) std.ascii.toUpper(char) else char,
                    );
                },
                .camel_case, .pascal_case => {
                    const lowercase = first and casing == .camel_case;
                    if (printer.is_initialism(word)) {
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
                .kebab_case => unreachable,
            }
        }
        return result.items;
    }

    fn is_initialism(printer: *const Printer, word: []const u8) bool {
        if (printer.options.initialisms) |initialisms| {
            for (initialisms) |match| {
                if (std.ascii.eqlIgnoreCase(word, match)) {
                    return true;
                }
            }
        }
        return false;
    }
};

// Any run of characters that isn't alphanumeric is a word break for
// conversion purposes. With `include_underscore`, `_` is part of a word.
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
