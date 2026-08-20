const std = @import("std");
const types = @import("conformance_test_types.zig");

pub const Formatter = struct {
    // For GO* see: https://github.com/golang/go/wiki/CodeReviewComments#initialisms
    pub const Casing = enum {
        snake_case,
        camelCase,
        PascalCase,
        UPPER_CASE,
        GOPascalCase,
        GOCamelCase,
    };

    arena: std.mem.Allocator,
    indent_width: u32,
    indent_char: u8 = ' ',
    // The outermost generated declaration. Statements sit one level deeper.
    start_level: u32,
    // Derived, so it has no default. Use `init` to build a Formatter.
    statement_level: u32,
    comment_marker: []const u8 = "//",

    pub fn init(arena: std.mem.Allocator, options: struct {
        indent_width: u32,
        indent_char: u8 = ' ',
        start_level: u32 = 1,
        comment_marker: []const u8 = "//",
    }) Formatter {
        return .{
            .arena = arena,
            .indent_width = options.indent_width,
            .indent_char = options.indent_char,
            .start_level = options.start_level,
            .statement_level = options.start_level + 1,
            .comment_marker = options.comment_marker,
        };
    }

    pub fn write_indent(
        formatter: Formatter,
        writer: *std.Io.Writer,
        options: struct { level: ?u32 = null },
    ) !void {
        const level = options.level orelse formatter.start_level;
        try writer.splatByteAll(formatter.indent_char, level * formatter.indent_width);
    }

    pub fn indent(formatter: Formatter) ![]const u8 {
        const text = try formatter.arena.alloc(
            u8,
            formatter.start_level * formatter.indent_width,
        );
        @memset(text, formatter.indent_char);
        return text;
    }

    pub fn generate_omission(formatter: Formatter, case: types.Case) ![]const u8 {
        const comment_start = try std.fmt.allocPrint(formatter.arena, "{s}{s} ", .{
            try formatter.indent(),
            formatter.comment_marker,
        });
        const reason = try formatter.arena.dupe(u8, @tagName(case.requirement.?));
        std.mem.replaceScalar(u8, reason, '_', ' ');
        return std.fmt.allocPrint(formatter.arena, "{s}Omitted: \"{s}\"\n{s}Reason: {s}\n", .{
            comment_start,
            case.description,
            comment_start,
            reason,
        });
    }

    // Any run of characters that isn't alphanumeric is a word break for conversion purposes.
    pub fn to_case(formatter: Formatter, casing: Casing, input: []const u8) ![]const u8 {
        var result = try std.ArrayList(u8).initCapacity(formatter.arena, input.len);
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
