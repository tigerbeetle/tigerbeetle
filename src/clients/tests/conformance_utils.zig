const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

// For GO* see: https://github.com/golang/go/wiki/CodeReviewComments#initialisms
pub const Case = enum { camelCase, PascalCase, UPPER_CASE, GOPascalCase, GOCamelCase };

// NOTE: This may eventually move to stdx. I did not feel like touching that during the
// initial PoC phase.
pub fn to_case(arena: std.mem.Allocator, case: Case, snake_case: []const u8) ![]const u8 {
    switch (case) {
        .UPPER_CASE => {
            const result = try arena.alloc(u8, snake_case.len);
            return std.ascii.upperString(result, snake_case);
        },
        .camelCase, .PascalCase, .GOPascalCase, .GOCamelCase => {
            const go = case == .GOPascalCase or case == .GOCamelCase;
            var text: std.ArrayList(u8) = .empty;
            var words = std.mem.tokenizeScalar(u8, snake_case, '_');
            while (words.next()) |word| {
                if (go and is_go_initialism(word)) {
                    for (word) |char| try text.append(arena, std.ascii.toUpper(char));
                    continue;
                }
                try text.append(arena, std.ascii.toUpper(word[0]));
                try text.appendSlice(arena, word[1..]);
            }
            if (case == .camelCase or case == .GOCamelCase) {
                const lead = if (go and is_go_initialism(first_word(snake_case)))
                    first_word(snake_case).len
                else
                    1;
                for (text.items[0..lead]) |*char| char.* = std.ascii.toLower(char.*);
            }
            return text.items;
        },
    }
}

fn first_word(snake_case: []const u8) []const u8 {
    const parts = stdx.cut(snake_case, "_") orelse return snake_case;
    assert(parts[0].len > 0);
    return parts[0];
}

fn is_go_initialism(word: []const u8) bool {
    return std.ascii.eqlIgnoreCase(word, "id") or std.ascii.eqlIgnoreCase(word, "ok");
}

test to_case {
    var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_instance.deinit();
    const arena = arena_instance.allocator();
    const expect = std.testing.expectEqualStrings;
    try expect("createAccounts", try to_case(arena, .camelCase, "create_accounts"));
    try expect("CreateTransfers", try to_case(arena, .PascalCase, "create_transfers"));
    try expect("GET_ACCOUNT_BALANCES", try to_case(arena, .UPPER_CASE, "get_account_balances"));
    try expect("IdFoo", try to_case(arena, .PascalCase, "id_foo"));
    try expect("IDFoo", try to_case(arena, .GOPascalCase, "id_foo"));
    try expect("idFoo", try to_case(arena, .GOCamelCase, "id_foo"));
    try expect("FooID", try to_case(arena, .GOPascalCase, "foo_id"));
}
