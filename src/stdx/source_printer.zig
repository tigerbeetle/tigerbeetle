//! This file defines a printer for source code emitters.
//!
//! It encapsulates information regarding indents and comments, and exposes
//! methods for indenting and dedenting so users of the printer never need to
//! manually track the indent level.

const std = @import("std");
const assert = std.debug.assert;

const Casing = @import("stdx.zig").Casing;

pub const SourcePrinter = struct {
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

    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: Options,
    level: u32,

    pub fn init(
        allocator: std.mem.Allocator,
        writer: *std.Io.Writer,
        options: Options,
    ) SourcePrinter {
        return .{
            .allocator = allocator,
            .writer = writer,
            .options = options,
            .level = options.start_level,
        };
    }

    pub fn indent(printer: *SourcePrinter) void {
        printer.level += 1;
        assert(printer.level > printer.options.start_level);
    }

    pub fn dedent(printer: *SourcePrinter) void {
        assert(printer.level > printer.options.start_level);
        printer.level -= 1;
    }

    pub fn reset_indent(printer: *SourcePrinter) void {
        assert(printer.level >= printer.options.start_level);
        printer.level = printer.options.start_level;
    }

    // Starts a line the caller has to finish.
    pub fn write_indent(printer: SourcePrinter) !void {
        const width = printer.level * printer.options.indent_width;
        try printer.writer.splatByteAll(printer.options.indent_char, width);
    }

    // Used for alignment, not indenting.
    pub fn write_padding(printer: SourcePrinter, count: usize) !void {
        try printer.writer.splatByteAll(' ', count);
    }

    pub fn write(printer: SourcePrinter, bytes: []const u8) !void {
        try printer.writer.writeAll(bytes);
    }

    pub fn print(printer: SourcePrinter, comptime fmt: []const u8, args: anytype) !void {
        try printer.writer.print(fmt, args);
    }

    pub fn write_empty_line(printer: SourcePrinter) !void {
        try printer.write("\n");
    }

    pub fn write_indented(printer: SourcePrinter, bytes: []const u8) !void {
        try printer.write_indent();
        try printer.write(bytes);
        try printer.write("\n");
    }

    pub fn print_indented(printer: SourcePrinter, comptime fmt: []const u8, args: anytype) !void {
        try printer.write_indent();
        try printer.print(fmt ++ "\n", args);
    }

    pub fn string_alloc(
        printer: *SourcePrinter,
        comptime fmt: []const u8,
        args: anytype,
    ) ![]const u8 {
        return std.fmt.allocPrint(printer.allocator, fmt, args);
    }

    /// Converts `input` to `casing` at comptime, without initialisms.
    pub fn to_case(comptime input: []const u8, comptime casing: Casing) []const u8 {
        return to_case_initialisms(input, casing, &.{});
    }

    /// Like `to_case`, but words matching one of `initialisms` are fully capitalized when casing
    /// as PascalCase or camelCase.
    pub fn to_case_initialisms(
        comptime input: []const u8,
        comptime casing: Casing,
        comptime initialisms: []const []const u8,
    ) []const u8 {
        return comptime blk: {
            var output: [input.len * 2]u8 = undefined;
            const len = write_case(&output, input, casing, initialisms);
            const result = output[0..len].*;
            break :blk &result;
        };
    }

    pub fn to_case_alloc(printer: *SourcePrinter, casing: Casing, input: []const u8) ![]const u8 {
        const output = try printer.allocator.alloc(u8, input.len * 2);
        const len = write_case(output, input, casing, printer.options.initialisms orelse &.{});
        return output[0..len];
    }

    fn write_case(
        output: []u8,
        input: []const u8,
        casing: Casing,
        initialisms: []const []const u8,
    ) usize {
        assert(output.len >= input.len * 2);

        var len: usize = 0;
        var first = true;
        var words = Words.init(input);
        while (words.next()) |word| {
            switch (casing) {
                .snake_case, .upper_case => {
                    if (!first) {
                        output[len] = '_';
                        len += 1;
                    }
                    for (word) |char| {
                        output[len] = if (casing == .upper_case)
                            std.ascii.toUpper(char)
                        else
                            std.ascii.toLower(char);
                        len += 1;
                    }
                },
                .camel_case, .pascal_case => {
                    const lowercase = first and casing == .camel_case;
                    if (is_initialism(initialisms, word)) {
                        for (word) |char| {
                            output[len] = if (lowercase)
                                std.ascii.toLower(char)
                            else
                                std.ascii.toUpper(char);
                            len += 1;
                        }
                    } else {
                        output[len] = if (lowercase)
                            std.ascii.toLower(word[0])
                        else
                            std.ascii.toUpper(word[0]);
                        @memcpy(output[len + 1 ..][0 .. word.len - 1], word[1..]);
                        len += word.len;
                    }
                },
            }
            first = false;
        }
        return len;
    }

    fn is_initialism(initialisms: []const []const u8, word: []const u8) bool {
        for (initialisms) |match| {
            if (std.ascii.eqlIgnoreCase(word, match)) {
                return true;
            }
        }
        return false;
    }
};

// Any run of characters that isn't alphanumeric is a word break for
// conversion purposes, as is a lowercase letter or digit followed by an
// uppercase letter.  With `include_underscore`, `_` is part of a word.
pub const Words = struct {
    input: []const u8,
    index: usize = 0,
    include_underscore: bool = false,

    fn init(input: []const u8) Words {
        return .{ .input = input };
    }

    pub fn next(words: *Words) ?[]const u8 {
        var start = words.index;
        while (start < words.input.len and !words.is_word_char(words.input[start])) start += 1;
        if (start == words.input.len) {
            words.index = start;
            return null;
        }

        var end = start + 1;
        while (end < words.input.len and words.is_word_char(words.input[end]) and
            !is_camel_boundary(words.input[end - 1], words.input[end])) end += 1;
        words.index = end;
        return words.input[start..end];
    }

    fn is_word_char(words: *const Words, char: u8) bool {
        return std.ascii.isAlphanumeric(char) or (char == '_' and words.include_underscore);
    }

    fn is_camel_boundary(previous: u8, current: u8) bool {
        return (std.ascii.isLower(previous) or std.ascii.isDigit(previous)) and
            std.ascii.isUpper(current);
    }
};

test "to_case" {
    try std.testing.expectEqualStrings("createAccounts", comptime SourcePrinter.to_case(
        "create_accounts",
        .camel_case,
    ));
    try std.testing.expectEqualStrings("CreateTransfers", comptime SourcePrinter.to_case(
        "create_transfers",
        .pascal_case,
    ));
    try std.testing.expectEqualStrings("GET_ACCOUNT_BALANCES", comptime SourcePrinter.to_case(
        "get_account_balances",
        .upper_case,
    ));
    try std.testing.expectEqualStrings("create_account_status", comptime SourcePrinter.to_case(
        "CreateAccountStatus",
        .snake_case,
    ));
    try std.testing.expectEqualStrings("tb_uint128_range_t", comptime SourcePrinter.to_case(
        "TbUint128Range_t",
        .snake_case,
    ));
}

test "to_case_initialisms" {
    const initialisms: []const []const u8 = &.{ "id", "ok" };
    try std.testing.expectEqualStrings("UserID", comptime SourcePrinter.to_case_initialisms(
        "user_id",
        .pascal_case,
        initialisms,
    ));
    try std.testing.expectEqualStrings("idOK", comptime SourcePrinter.to_case_initialisms(
        "id_ok",
        .camel_case,
        initialisms,
    ));
}

test "to_case_alloc" {
    var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_instance.deinit();

    var printer = SourcePrinter.init(arena_instance.allocator(), undefined, .{
        .indent_width = 1,
        .indent_char = ' ',
        .comment_marker = "//",
        .initialisms = &.{"id"},
    });
    try std.testing.expectEqualStrings(
        "created_by_id",
        try printer.to_case_alloc(.snake_case, "createdById"),
    );
}
