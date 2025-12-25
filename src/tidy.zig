//! Checks for various non-functional properties of the code itself.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const fs = std.fs;
const mem = std.mem;
const Ast = std.zig.Ast;

const stdx = @import("stdx");
const Shell = @import("./shell.zig");

const Snap = stdx.Snap;
const module_path = "src";
const snap = Snap.snap_fn(module_path);

const MiB = stdx.MiB;

test "tidy" {
    const gpa = std.testing.allocator;

    var errors: Errors = .{};

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var counter: IdentifierCounter = try .init(gpa);
    defer counter.deinit(gpa);

    var dead_files_detector = DeadFilesDetector.init(gpa);
    defer dead_files_detector.deinit(gpa);

    // NB: all checks are intentionally implemented in a streaming fashion,
    // such that we only need to read the files once.
    const file_buffer = try gpa.alloc(u8, 1 * MiB);
    defer gpa.free(file_buffer);

    const paths = try list_file_paths(shell);
    for (paths) |file_path| {
        const bytes_read = (try std.fs.cwd().readFile(file_path, file_buffer)).len;
        if (bytes_read >= file_buffer.len - 1) return error.FileTooLong;
        file_buffer[bytes_read] = 0;

        const source_file = SourceFile{ .path = file_path, .text = file_buffer[0..bytes_read :0] };
        try tidy_file(gpa, &counter, source_file, &errors);

        if (source_file.has_extension(".zig")) {
            try dead_files_detector.visit(source_file);
        }
    }

    dead_files_detector.finish(&errors);

    if (errors.count > 0) return error.Untidy;
    assert(errors.count == 0);
}

const Errors = struct {
    count: u32 = 0,
    captured: ?std.ArrayListUnmanaged(u8) = null, // For tests.

    pub fn add_control_character(
        errors: *Errors,
        file: SourceFile,
        offset: usize,
        character: u8,
    ) void {
        errors.emit(
            "{s}:{d}: error: control character code={}\n",
            .{ file.path, file.line_number(offset), character },
        );
    }

    pub fn add_banned(
        errors: *Errors,
        file: SourceFile,
        offset: usize,
        banned_item: []const u8,
        replacement: []const u8,
    ) void {
        errors.emit(
            "{s}:{d}: error: {s} is banned, use {s}\n",
            .{ file.path, file.line_number(offset), banned_item, replacement },
        );
    }

    pub fn add_banned_reminder(
        errors: *Errors,
        file: SourceFile,
        offset: usize,
        banned_item: []const u8,
    ) void {
        errors.emit(
            "{s}:{d}: error: leftover {s}, remove before merge\n",
            .{ file.path, file.line_number(offset), banned_item },
        );
    }

    pub fn add_long_line(errors: *Errors, file: SourceFile, line_index: usize) void {
        const line_number = line_index + 1;
        errors.emit(
            "{s}:{d}: error: line exceeds 100 columns\n",
            .{ file.path, line_number },
        );
    }

    pub fn add_bad_type_function_name(
        errors: *Errors,
        file: SourceFile,
        line_index: usize,
        function_name: []const u8,
    ) void {
        const line_number = line_index + 1;
        errors.emit(
            "{s}:{d}: error: type function name '{s}' should end in 'Type'\n",
            .{ file.path, line_number, function_name },
        );
    }

    pub fn add_long_function(errors: *Errors, file: SourceFile, line_index: usize) void {
        const line_number = line_index + 1;
        errors.emit(
            "{s}:{d}: error: functions exceeds 70 lines\n",
            .{ file.path, line_number },
        );
    }

    pub fn add_ambiguous_precedence(errors: *Errors, file: SourceFile, line_index: usize) void {
        const line_number = line_index + 1;
        errors.emit(
            "{s}:{d}: error: ambiguous operator precedence, add parenthesis\n",
            .{ file.path, line_number },
        );
    }

    pub fn add_dead_declaration(errors: *Errors, file: SourceFile, declaration: []const u8) void {
        errors.emit("{s}: error: '{s}' is dead code\n", .{ file.path, declaration });
    }

    pub fn add_invalid_markdown_title(errors: *Errors, file: SourceFile) void {
        errors.emit(
            "{s}: error: document should have exactly one top-level '# Title'\n",
            .{file.path},
        );
    }

    pub fn add_file_untracked(errors: *Errors, file: []const u8) void {
        errors.emit(
            "{s}: error: imported file untracked by git\n",
            .{file},
        );
    }

    pub fn add_file_dead(errors: *Errors, file: []const u8) void {
        errors.emit(
            "{s}: error: file never imported\n",
            .{file},
        );
    }

    fn emit(errors: *Errors, comptime fmt: []const u8, args: anytype) void {
        comptime assert(fmt[fmt.len - 1] == '\n');
        errors.count += 1;
        if (errors.captured) |*captured| {
            captured.writer(std.testing.allocator).print(fmt, args) catch @panic("OOM");
        } else {
            std.debug.print(fmt, args);
        }
    }
};

const SourceFile = struct {
    path: []const u8,
    text: [:0]const u8,

    fn has_extension(file: SourceFile, extension: []const u8) bool {
        assert(extension.len > 0);
        assert(extension[0] == '.');
        return std.mem.endsWith(u8, file.path, extension);
    }

    // O(1), but only invoked on the cold path (when there are errors).
    fn line_number(file: SourceFile, offset: usize) usize {
        assert(offset <= file.text.len);
        // +1: Line _index_ is zero-based, line _number_ is one-based.
        return std.mem.count(u8, file.text[0..offset], "\n") + 1;
    }
};

fn tidy_file(
    gpa: Allocator,
    counter: *IdentifierCounter,
    file: SourceFile,
    errors: *Errors,
) Allocator.Error!void {
    tidy_control_characters(file, errors);
    if (file.has_extension(".zig")) {
        tidy_banned(file, errors);
        tidy_lines(file, errors);
        tidy_type_functions(file, errors);

        var tree = try std.zig.Ast.parse(gpa, file.text, .zig);
        defer tree.deinit(gpa);

        tidy_dead_declarations(file, &tree, counter, errors);
        tidy_ast(file, &tree, errors);
    }
    if (file.has_extension(".md")) {
        tidy_markdown_title(file, errors);
    }
}

fn check_tidy_file(file_path: []const u8, file_text: [:0]const u8, want: Snap) !void {
    const gpa = std.testing.allocator;

    var counter: IdentifierCounter = try .init(gpa);
    defer counter.deinit(gpa);

    var errors: Errors = .{ .captured = .{} };
    defer errors.captured.?.deinit(std.testing.allocator);

    try tidy_file(gpa, &counter, .{ .path = file_path, .text = file_text }, &errors);
    const got = errors.captured.?.items;

    try want.diff(got);
    assert(errors.count == std.mem.count(u8, got, "\n"));
}

fn tidy_control_characters(file: SourceFile, errors: *Errors) void {
    const binary_file_extensions: []const []const u8 = &.{ ".ico", ".png", ".webp" };
    for (binary_file_extensions) |extension| {
        if (file.has_extension(extension)) return;
    }

    const allowed = .{
        .@"\r" = file.has_extension(".bat"),

        // Visual Studio insists on \t, taking the best from `make`.
        // Go uses tabs.
        .@"\t" = file.has_extension(".sln") or
            (file.has_extension(".go") or
                (file.has_extension(".md") and mem.indexOf(u8, file.text, "```go") != null)),
    };

    var remaining = file.text;
    while (mem.indexOfAny(u8, remaining, "\r\t")) |index| {
        const offset = index + (file.text.len - remaining.len);
        inline for (comptime std.meta.fieldNames(@TypeOf(allowed))) |field| {
            if (remaining[index] == field[0]) {
                if (!@field(allowed, field)) {
                    errors.add_control_character(file, offset, field[0]);
                }
                break;
            }
        } else unreachable;

        remaining = remaining[index + 1 ..];
    }
}

test tidy_control_characters {
    try check_tidy_file(
        "hello.txt",
        "Hello\t\nWorld\r\n",
        snap(@src(),
            \\hello.txt:1: error: control character code=9
            \\hello.txt:2: error: control character code=13
            \\
        ),
    );
}

fn tidy_banned(file: SourceFile, errors: *Errors) void {
    // Vendored code is exempt from bans.
    if (std.mem.eql(u8, file.path, "src/stdx/vendored/aegis.zig")) return;
    // Don't ban ourselves!
    if (std.mem.eql(u8, file.path, "src/tidy.zig")) return;

    const ban_list: []const struct { []const u8, []const u8 } = &.{
        // Functionality provided by stdx:
        .{ "std.BoundedArray", "stdx.BoundedArrayType" },
        .{ "StaticBitSet", "stdx.BitSetType" },
        .{ "std.time.Duration", "stdx.Duration" },
        .{ "std.time.Instant", "stdx.Instant" },
        .{ "hasUniqueRepresentation", "stdx.has_unique_representation" },
        .{ "@memcpy(", "stdx.copy_disjoint" },
        .{ "mem.copyForwards(", "stdx.copy_left" },
        .{ "mem.copyBackwards(", "stdx.copy_right" },
        .{ "uintLessThan", "stdx.PRNG" },
        .{ "intRangeLessThan", "stdx.PRNG" },
        .{ "intRangeAtMost", "stdx.PRNG" },
        .{ "intRangeAtMostBiased", "stdx.PRNG" },

        // Library footguns:
        .{ "unexpectedErrno", "stdx.unexpected_errno" },
        .{ "posix.send(", "posix.sendto to avoid connection race condition" },

        // Language footguns:
        .{ "== error.", "switch to avoid silent anyerror upcast" },
        .{ "!= error.", "switch to avoid silent anyerror upcast" },

        // Everything else:
        .{ "debug.assert(", "unqualified assert" },
        .{ "Self = @This()", "proper type name" },
        .{ "!comptime", "! inside comptime" },
        .{ "usingnamespace", "something else" },
    };

    for (ban_list) |ban_item| {
        const banned, const replacement = ban_item;
        if (std.mem.indexOf(u8, file.text, banned)) |offset| {
            errors.add_banned(file, offset, banned, replacement);
        }
    }

    // Reminders:
    // Do use FIXME comments proactively while iterating on the code when you want to make sure
    // something is revisited before getting into the main branch.
    inline for (.{ "FIXME", "dbg(" }) |banned| {
        if (std.mem.indexOf(u8, file.text, banned)) |offset| {
            if (std.mem.startsWith(u8, file.text[offset..], "dbg(prefix: []const u8")) {
                // Allow fn dbg( function definition.

            } else {
                errors.add_banned_reminder(file, offset, banned);
            }
        }
    }
}

test tidy_banned {
    try check_tidy_file(
        \\banned.zig
    ,
        \\//FIXME: use copy_disjoint:
        \\@memcpy(foo, bar)
    ,
        snap(@src(),
            \\banned.zig:2: error: @memcpy( is banned, use stdx.copy_disjoint
            \\banned.zig:1: error: leftover FIXME, remove before merge
            \\
        ),
    );
}

fn tidy_lines(file: SourceFile, errors: *Errors) void {
    if (std.mem.endsWith(u8, file.path, "low_level_hash_vectors.zig")) return;

    var line_iterator = mem.splitScalar(u8, file.text, '\n');
    var line_index: u32 = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        tidy_line(file, line, line_index, errors);
    }
}

fn tidy_line(file: SourceFile, line: []const u8, line_index: usize, errors: *Errors) void {
    const line_length = tidy_line_length(line);
    if (line_length <= 100) return;

    if (tidy_line_link(line)) return;

    // Journal recovery table
    if (std.mem.indexOf(u8, line, "Case.init(") != null) return;

    // For multiline strings, we care that the _result_ fits 100 characters,
    // but we don't mind indentation in the source.
    if (tidy_line_raw_literal(line)) |string_value| {
        const string_value_length = tidy_line_length(string_value);
        if (string_value_length <= 100) return;

        if (std.mem.endsWith(u8, file.path, "state_machine_tests.zig") and
            (std.mem.startsWith(u8, string_value, " account A") or
                std.mem.startsWith(u8, string_value, " transfer T") or
                std.mem.startsWith(u8, string_value, " transfer   ")))
        {
            // Table tests from state_machine.zig. They are intentionally wide.
            return;
        }

        // vsr.zig's Checkpoint ops diagram.
        if (std.mem.endsWith(u8, file.path, "vsr.zig") and
            std.mem.startsWith(u8, string_value, "OPS: ")) return;

        // trace.zig's JSON snapshot test.
        if (std.mem.endsWith(u8, file.path, "trace.zig") and
            std.mem.startsWith(u8, string_value, "{\"pid\":1,\"tid\":")) return;

        // AMQP JSON snapshot test.
        if (std.mem.endsWith(u8, file.path, "cdc/runner.zig") and
            std.mem.startsWith(u8, string_value, "{\"timestamp\":")) return;

        // Message fromatting tests.
        if (std.mem.endsWith(u8, file.path, "message_header.zig") and
            std.mem.startsWith(u8, string_value, "Prepare{")) return;
    }

    errors.add_long_line(file, line_index);
}

fn tidy_line_length(line: []const u8) usize {
    // Count codepoints for simplicity, even if it is wrong.
    return std.unicode.utf8CountCodepoints(line) catch @panic("invalid utf-8");
}

/// Heuristically checks if a `line` contains an URL.
fn tidy_line_link(line: []const u8) bool {
    return std.mem.indexOf(u8, line, "https://") != null;
}

/// If a line is a `\\` string literal, extract its value.
fn tidy_line_raw_literal(line: []const u8) ?[]const u8 {
    const indentation, const value = stdx.cut(line, "\\\\") orelse return null;
    for (indentation) |c| if (c != ' ') return null;
    return value;
}

test tidy_lines {
    try check_tidy_file(
        \\lines.zig
    ,
        "" ++
            "pub const x = 92;\n" ++
            "pub const x = " ++ ("9" ** 199) ++ ";\n" ++
            "pub const url = \"https://example." ++ ("0" ** 199) ++ " \";\n" ++
            "        \\\\" ++ ("9" ** 99) ++ "\n" ++
            "        \"" ++ ("9" ** 99) ++ "\"\n",
        snap(@src(),
            \\lines.zig:2: error: line exceeds 100 columns
            \\lines.zig:5: error: line exceeds 100 columns
            \\
        ),
    );
}

/// All functions using the `CamelCase` naming convention return a type,
/// so we enforce that the function name also ends with the `Type` suffix.
fn tidy_type_functions(file: SourceFile, errors: *Errors) void {
    var line_index: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');
    while (it.next()) |line| : (line_index += 1) {
        // Zig fmt enforces that the pattern `fn Foo(` is not split across multiple lines.

        const prefix, const suffix = stdx.cut(line, "fn ") orelse continue;
        // Not all `fn ` tokens are functions, some may be `callback_fn` for example.
        // Functions appear at the beginning of a line or after a whitespace.
        if (prefix.len > 0 and prefix[prefix.len - 1] != ' ') continue;
        const function_name, _ = stdx.cut(suffix, "(") orelse continue;
        if (function_name.len == 0) continue; // E.g: `*const fn (*anyopaque) void`.
        assert(function_name.len > 0);

        // Skipping naming convention that requires upper-case functions.
        if (std.mem.startsWith(u8, function_name, "JNI_")) continue;
        // Windows use CamelCase functions.
        if (std.mem.indexOf(u8, line, "extern \"kernel32\"") != null) continue;

        if (std.ascii.isUpper(function_name[0])) {
            if (!std.mem.endsWith(u8, function_name, "Type")) {
                errors.add_bad_type_function_name(file, line_index, function_name);
            }
        }
    }
}

test tidy_type_functions {
    try check_tidy_file(
        \\type_functions.zig
    ,
        \\pub fn MyArrayType() type { }
    ++ "\npub fn" ++ " MyArray() type { }" ++ "\n" ++
        \\ pub const callback = *const fn (*anyopaque) void;
    ,
        snap(@src(),
            \\type_functions.zig:2: error: type function name 'MyArray' should end in 'Type'
            \\
        ),
    );
}

const IdentifierCounter = struct {
    const file_identifier_count_max = 100_000;

    map: std.StringHashMapUnmanaged(struct { count: u32, offset: u32 }) = .{},

    pub fn init(gpa: Allocator) !IdentifierCounter {
        var counter: IdentifierCounter = .{};
        try counter.map.ensureTotalCapacity(gpa, file_identifier_count_max + 1);
        return counter;
    }

    pub fn deinit(counter: *IdentifierCounter, gpa: Allocator) void {
        counter.map.deinit(gpa);
        counter.* = undefined;
    }

    pub fn empty(counter: *const IdentifierCounter) bool {
        return counter.map.count() == 0;
    }

    pub fn clear(counter: *IdentifierCounter) void {
        counter.map.clearRetainingCapacity();
    }

    pub fn record(
        counter: *IdentifierCounter,
        tree: *const Ast,
        token_text: []const u8,
        token_offset: u32,
    ) void {
        const gop = counter.map.getOrPutAssumeCapacity(token_text);
        if (counter.map.count() > file_identifier_count_max) @panic("file too large");

        if (gop.found_existing) {
            // Count occurrences on a single line as one, as a special case for imports:
            // const foo = std.foo;
            const between_tokens_text = tree.source[gop.value_ptr.offset..token_offset];
            const same_line_occurrence = mem.indexOfScalar(u8, between_tokens_text, '\n') == null;
            if (same_line_occurrence) return;
        }

        if (!gop.found_existing) gop.value_ptr.* = .{ .count = 0, .offset = 0 };
        gop.value_ptr.count += 1;
        gop.value_ptr.offset = token_offset;
    }

    pub fn get(counter: *const IdentifierCounter, token_text: []const u8) u32 {
        return counter.map.get(token_text).?.count;
    }
};

/// Detects unused constants and functions.
///
/// This is a one-side heuristic: there might be false negatives, but no false positives.
///
/// Current algorithm:
/// - Two passes.
/// - Pass 1: count how many times each identifier is mentioned in the file.
/// - Pass 2: warn about any unique identifier which is a non-public declaration.
///
/// At the moment, this is implemented using only the lexer, without looking at the AST, as that
/// seemed simpler.
fn tidy_dead_declarations(
    file: SourceFile,
    tree: *const Ast,
    counter: *IdentifierCounter,
    errors: *Errors,
) void {
    assert(counter.empty());
    defer counter.clear();

    var identifier_start: ?Ast.ByteOffset = 0;
    inline for (.{ .fill, .check }) |phase| {
        next_token: for (
            tree.tokens.items(.tag),
            tree.tokens.items(.start),
            0..,
        ) |tag, start, index_usize| {
            const index: Ast.TokenIndex = @intCast(index_usize);
            const identifier_start_previous = identifier_start;
            identifier_start = switch (tag) {
                .identifier => start,
                else => null,
            };

            const start_previous = identifier_start_previous orelse continue :next_token;
            const token_text = std.mem.trim(
                u8,
                tree.source[start_previous..start],
                &std.ascii.whitespace,
            );

            switch (phase) {
                .fill => counter.record(tree, token_text, start),
                .check => {
                    const usages = counter.get(token_text);
                    assert(usages >= 1);
                    if (usages == 1) {
                        if (tidy_dead_declarations_is_private_declaration(tree, index - 1)) {
                            errors.add_dead_declaration(file, token_text);
                        }
                    }
                },
                else => comptime unreachable,
            }
        }
    }
}

// Checks if the given identifier token refers to non-public declaration.
fn tidy_dead_declarations_is_private_declaration(
    tree: *const Ast,
    token_index: Ast.TokenIndex,
) bool {
    assert(tree.tokens.items(.tag)[token_index] == .identifier);
    var declaration_keyword = false;
    for (0..4) |context_offset| {
        const context_tag = if (token_index - context_offset < 1)
            .eof
        else
            tree.tokens.get(token_index - context_offset - 1).tag;

        if (!declaration_keyword) {
            switch (context_tag) {
                .keyword_fn, .keyword_const => declaration_keyword = true,
                // Not a declaration.
                else => return false,
            }
        } else {
            switch (context_tag) {
                .keyword_inline, .keyword_extern, .string_literal => {},
                // Public declaration can be used in a different file.
                .keyword_pub, .keyword_export => return false,
                // []const u8 or *const u8, not a declaration.
                .r_bracket, .asterisk => return false,
                // Non public declarations, never used.
                else => return true,
            }
        }
    } else unreachable;
}

test tidy_dead_declarations {
    try check_tidy_file(
        \\dead.zig
    ,
        \\ const std = @import("std");
        \\ const import_unused = std.import_unused;
        \\ pub fn public_used() void { private_used(); }
        \\ fn private_used() void {}
        \\ fn private_unused() void {}
    ,
        snap(@src(),
            \\dead.zig: error: 'import_unused' is dead code
            \\dead.zig: error: 'private_unused' is dead code
            \\
        ),
    );
}

fn tidy_ast(
    file: SourceFile,
    tree: *const Ast,
    errors: *Errors,
) void {
    if (std.mem.eql(u8, file.path, "build.zig")) return;
    if (std.mem.endsWith(u8, file.path, "build_multiversion.zig")) return;
    if (std.mem.endsWith(u8, file.path, "bindings.zig")) return;

    const tags = tree.nodes.items(.tag);
    const datas = tree.nodes.items(.data);
    // We can implement this in a streaming fashion, but its more convenient to materialize all
    // functions. 1k functions per file should be enough even for TigerBeetle!
    var functions: [1024]struct {
        line_opening: usize,
        line_closing: usize,
    } = undefined;
    var functions_count: u32 = 0;

    for (tags, datas, 0..) |tag, data, node| {
        if (tag == .fn_decl) { // Check function length.
            const node_body = data.rhs;

            const token_opening = tree.firstToken(@intCast(node));
            const token_closing = tree.lastToken(@intCast(node_body));

            const line_opening = tree.tokenLocation(0, token_opening).line;
            const line_closing = tree.tokenLocation(0, token_closing).line;

            functions[functions_count] = .{
                .line_opening = line_opening,
                .line_closing = line_closing,
            };
            functions_count += 1;
        }
        if (is_bin_op(tag)) { // Forbid mixing bitops and arithmetics without parentheses.
            inline for (.{ data.lhs, data.rhs }) |child| {
                const tag_child = tags[child];
                if ((is_bin_op_bitwise(tag) and is_bin_op_arithmetic(tag_child)) or
                    (is_bin_op_arithmetic(tag) and is_bin_op_bitwise(tag_child)))
                {
                    const token_opening = tree.firstToken(@intCast(node));
                    const line_opening = tree.tokenLocation(0, token_opening).line;
                    errors.add_ambiguous_precedence(file, line_opening);
                }
            }
        }
    }

    // We ratchet 70-lines-per-function TigerStyle rule from the bottom up. Some functions want
    // to be really long, and that is big. The most values is in preventing originally small
    // functions to grow over time.
    const function_length_red_zone = .{
        .min = 70, // NB: both are exclusive, so red zone is intentionally empty to start!
        .max = 70,
    };

    for (functions[0..functions_count], 0..) |f, index| {
        // Functions are sorted by the start line.
        if (index > 0) assert(functions[index - 1].line_opening < f.line_opening);

        if (index == functions_count - 1 or
            functions[index + 1].line_opening > f.line_closing)
        {
            const function_length = f.line_closing - f.line_opening + 1;
            if (function_length_red_zone.min < function_length and
                function_length < function_length_red_zone.max)
            {
                errors.add_long_function(file, f.line_opening);
            }
        }
    }
}

fn is_bin_op(tag: Ast.Node.Tag) bool {
    return is_bin_op_bitwise(tag) or is_bin_op_arithmetic(tag);
}

fn is_bin_op_bitwise(tag: Ast.Node.Tag) bool {
    return switch (tag) {
        .shl, .shl_sat => true,
        .shr => true,
        .bit_xor, .bit_or, .bit_and => true,
        else => false,
    };
}

fn is_bin_op_arithmetic(tag: Ast.Node.Tag) bool {
    return switch (tag) {
        .add, .add_sat, .add_wrap => true,
        .sub, .sub_sat, .sub_wrap => true,
        .mul, .mul_sat, .mul_wrap => true,
        .div, .mod => true,
        else => false,
    };
}

test tidy_ast {
    try check_tidy_file(
        \\precedence.zig
    ,
        \\ pub const confusing = 1 + foo << 3;
        \\ pub const ok = 1 + (foo << 3);
    ,
        snap(@src(),
            \\precedence.zig:1: error: ambiguous operator precedence, add parenthesis
            \\
        ),
    );
}

/// Checks that each markdown document has exactly one h1.
///
/// There are two schools of thought regarding largest (`# War and Peace`)
/// headings in markdown. One school says that they are _section_ titles, so
/// you could have multiple #'s in the document. But another option is to
/// say that a single # signifies document _title_, and there should be only
/// one in a document.
///
/// We use markdown to create HTML, so # turns into h1. MDN recommends that
/// there's only a single h1 in a page:
///
/// <https://developer.mozilla.org/en-US/docs/Web/HTML/Element/Heading_Elements#avoid_using_multiple_h1_elements_on_one_page>
///
/// For this reason, we follow the second convention.
fn tidy_markdown_title(file: SourceFile, errors: *Errors) void {
    var fenced_block = false; // Avoid interpreting `# ` shell comments as titles.
    var heading_count: u32 = 0;
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');
    while (it.next()) |line| {
        line_count += 1;
        if (mem.startsWith(u8, line, "```")) fenced_block = !fenced_block;
        if (!fenced_block and mem.startsWith(u8, line, "# ")) heading_count += 1;
    }
    assert(!fenced_block);
    switch (heading_count) {
        // No need for a title for a short note.
        0 => if (line_count > 2) errors.add_invalid_markdown_title(file),
        1 => {},
        else => errors.add_invalid_markdown_title(file),
    }
}

test tidy_markdown_title {
    try check_tidy_file(
        \\ok.md
    ,
        \\# TigerStyle
        \\
        \\Style applies everywhere!
        \\For example, we check that markdowns contains exactly one top-level title:
        \\
        \\```
        \\# Good Document
        \\```
    ,
        snap(@src(),
            \\
        ),
    );
    try check_tidy_file(
        \\bad.md
    ,
        \\# Top Level Header
        \\
        \\Lorem Ipsum
        \\
        \\# And Another Top Level Header
    ,
        snap(@src(),
            \\bad.md: error: document should have exactly one top-level '# Title'
            \\
        ),
    );
}

// Zig's lazy compilation model makes it too easy to forget to include a file into the build --- if
// nothing imports a file, compiler just doesn't see it and can't flag it as unused.
//
// DeadFilesDetector implements heuristic detection of unused files, by "grepping" for import
// statements and flagging file which are never imported. This gives false negatives for unreachable
// cycles of files, as well as for identically-named files, but it should be good enough in
// practice.
const DeadFilesDetector = struct {
    const FileName = [64]u8;
    const FileState = struct { import_count: u32, definition_count: u32 };
    const FileMap = std.AutoArrayHashMap(FileName, FileState);

    files: FileMap,

    fn init(gpa: Allocator) DeadFilesDetector {
        return .{ .files = FileMap.init(gpa) };
    }

    fn deinit(detector: *DeadFilesDetector, _: Allocator) void {
        detector.files.deinit();
    }

    fn visit(detector: *DeadFilesDetector, file: SourceFile) Allocator.Error!void {
        assert(file.has_extension(".zig"));
        (try detector.file_state(file.path)).definition_count += 1;

        var rest: []const u8 = file.text;
        for (0..1024) |_| {
            _, rest = stdx.cut(rest, "@import(\"") orelse break;
            const import_path, rest = stdx.cut(rest, "\")").?;
            if (std.mem.endsWith(u8, import_path, ".zig")) {
                (try detector.file_state(import_path)).import_count += 1;
            }
        } else {
            std.debug.panic("file with more than 1024 imports: {s}", .{file.path});
        }
    }

    fn finish(detector: *DeadFilesDetector, errors: *Errors) void {
        defer detector.files.clearRetainingCapacity();

        for (detector.files.keys(), detector.files.values()) |name, state| {
            if (state.definition_count == 0) {
                errors.add_file_untracked(&name);
            }
            if (state.import_count == 0 and !is_entry_point(name)) {
                errors.add_file_dead(&name);
            }
        }
    }

    fn file_state(detector: *DeadFilesDetector, path: []const u8) !*FileState {
        const gop = try detector.files.getOrPut(path_to_name(path));
        if (!gop.found_existing) gop.value_ptr.* = .{ .import_count = 0, .definition_count = 0 };
        return gop.value_ptr;
    }

    fn path_to_name(path: []const u8) FileName {
        assert(std.mem.endsWith(u8, path, ".zig"));
        const basename = std.fs.path.basename(path);
        var file_name: FileName = @splat(0);
        assert(basename.len <= file_name.len);
        stdx.copy_disjoint(.inexact, u8, &file_name, basename);
        return file_name;
    }

    fn is_entry_point(file: FileName) bool {
        const entry_points: []const []const u8 = &.{
            "build_multiversion.zig",
            "build.zig",
            "dotnet_bindings.zig",
            "file_checker.zig",
            "fuzz_tests.zig",
            "go_bindings.zig",
            "integration_tests.zig",
            "java_bindings.zig",
            "jni_tests.zig",
            "libtb_client.zig",
            "main.zig",
            "node_bindings.zig",
            "node.zig",
            "page_writer.zig",
            "python_bindings.zig",
            "scripts.zig",
            "search_index_writer.zig",
            "service_worker_writer.zig",
            "rust_bindings.zig",
            "single_page_writer.zig",
            "tb_client_header.zig",
            "unit_tests.zig",
            "vopr.zig",
            "vortex.zig",
        };
        for (entry_points) |entry_point| {
            if (std.mem.startsWith(u8, &file, entry_point)) return true;
        }
        return false;
    }
};

test "tidy changelog" {
    const allocator = std.testing.allocator;

    const changelog_size_max = 1 * MiB;
    const changelog = try fs.cwd().readFileAlloc(allocator, "CHANGELOG.md", changelog_size_max);
    defer allocator.free(changelog);

    var line_iterator = mem.splitScalar(u8, changelog, '\n');
    var line_index: usize = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        if (std.mem.endsWith(u8, line, " ")) {
            std.debug.print("CHANGELOG.md:{d} trailing whitespace", .{line_index + 1});
            return error.TrailingWhitespace;
        }
        const line_length = tidy_line_length(line);
        if (line_length > 100 and !tidy_line_link(line)) {
            std.debug.print("CHANGELOG.md:{d} line exceeds 100 columns\n", .{line_index + 1});
            return error.LineTooLong;
        }
    }
}

test "tidy no large blobs" {
    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    // Run `git rev-list | git cat-file` to find large blobs. This is better than looking at the
    // files in the working tree, because it catches the cases where a large file is "removed" by
    // reverting the commit.
    //
    // Zig's std doesn't provide a cross platform abstraction for piping two commands together, so
    // we begrudgingly pass the data through this intermediary process.
    const shallow = try shell.exec_stdout("git rev-parse --is-shallow-repository", .{});
    if (!std.mem.eql(u8, shallow, "false")) {
        return error.ShallowRepository;
    }

    const rev_list = try shell.exec_stdout("git rev-list --objects HEAD", .{});
    const objects = try shell.exec_stdout_options(
        .{ .stdin_slice = rev_list },
        "git cat-file --batch-check={format}",
        .{ .format = "%(objecttype) %(objectsize) %(rest)" },
    );

    var has_large_blobs = false;
    var lines = std.mem.splitScalar(u8, objects, '\n');
    while (lines.next()) |line| {
        // Parsing lines like
        //     blob 1032 client/package.json
        const blob = stdx.cut_prefix(line, "blob ") orelse continue;

        const size_string, const path = stdx.cut(blob, " ").?;
        const size = try std.fmt.parseInt(u64, size_string, 10);

        if (std.mem.eql(u8, path, "src/vsr/replica.zig")) continue; // :-)
        if (std.mem.eql(u8, path, "src/state_machine.zig")) continue; // :-|
        if (std.mem.eql(u8, path, "src/docs_website/package-lock.json")) continue; // :-(
        if (size > @divExact(MiB, 4)) {
            has_large_blobs = true;
            std.debug.print("{s}\n", .{line});
        }
    }
    if (has_large_blobs) return error.HasLargeBlobs;
}

test "tidy unix permissions" {
    const executable_files = [_][]const u8{
        "zig/download.ps1",
        "zig/download.sh",
        ".github/ci/test_aof.sh",
        "src/scripts/cfo_supervisor.sh",
    };

    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const files = try shell.exec_stdout("git ls-files -z --format {format}", .{
        .format = "%(objectmode) %(path)",
    });
    assert(files[files.len - 1] == 0);
    var lines = std.mem.splitScalar(u8, files[0 .. files.len - 1], 0);
    while (lines.next()) |line| {
        const mode, const path = stdx.cut(line, " ").?;
        errdefer std.debug.print("{s}: error: unexpected mode={s}\n", .{ path, mode });

        if (std.mem.eql(u8, mode, "100644")) {
            // Expected for most files.
        } else if (std.mem.eql(u8, mode, "100755")) {
            const expected = for (executable_files) |executable_file| {
                if (std.mem.eql(u8, path, executable_file)) break true;
            } else false;

            if (!expected) return error.UnexpectedExecutable;
        } else {
            return error.UnexpectedMode;
        }
    }
}

// Sanity check for "unexpected" files in the repository.
test "tidy extensions" {
    const allowed_extensions = std.StaticStringMap(void).initComptime(.{
        .{".c"},    .{".cs"},      .{".csproj"}, .{".css"},   .{".go"},
        .{".h"},    .{".hcl"},     .{".html"},   .{".java"},  .{".js"},
        .{".json"}, .{".md"},      .{".mod"},    .{".props"}, .{".py"},
        .{".rs"},   .{".service"}, .{".sln"},    .{".sum"},   .{".svg"},
        .{".toml"}, .{".ts"},      .{".txt"},    .{".xml"},   .{".yml"},
        .{".zig"},  .{".zon"},
    });

    const exceptions = std.StaticStringMap(void).initComptime(.{
        .{".editorconfig"},
        .{".gitignore"},
        .{".nojekyll"},
        .{"CNAME"},
        .{"exclude-pmd.properties"},
        .{"favicon.png"},
        .{"notfound-light.webp"},
        .{"notfound-dark.webp"},
        .{"preview.webp"},
        .{"LICENSE"},
        .{"module-info.test"},
        .{"anchor-links.lua"},
        .{"markdown-links.lua"},
        .{"table-wrapper.lua"},
        .{"code-block-buttons.lua"},
        .{"edit-link-footer.lua"},
        .{"src/docs_website/.vale.ini"},
        .{"zig/download.sh"},
        .{"zig/download.ps1"},
        .{"zig/download.win.ps1"},
        .{"src/scripts/cfo_supervisor.sh"},
        .{".github/ci/test_aof.sh"},
        .{"src/clients/python/pyproject.toml"},
        .{"src/clients/python/src/tigerbeetle/py.typed"},
    });

    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const paths = try list_file_paths(shell);

    for (exceptions.keys()) |exception| {
        for (paths) |path| {
            const basename = std.fs.path.basename(path);
            if (std.mem.eql(u8, exception, basename) or std.mem.eql(u8, exception, path)) {
                break;
            }
        } else {
            std.debug.panic("exception (or basename) doesn't exist: {s} ({s})", .{
                exception,
                std.fs.path.basename(exception),
            });
        }
    }

    var bad_extension = false;
    for (paths) |path| {
        if (path.len == 0) continue;
        const extension = std.fs.path.extension(path);
        if (!allowed_extensions.has(extension)) {
            const basename = std.fs.path.basename(path);
            if (!exceptions.has(basename) and !exceptions.has(path)) {
                std.debug.print("bad extension: {s}\n", .{path});
                bad_extension = true;
            }
        }
    }
    if (bad_extension) return error.BadExtension;
}

/// Lists all files in the repository.
fn list_file_paths(shell: *Shell) ![]const []const u8 {
    var result = std.ArrayList([]const u8).init(shell.arena.allocator());

    const files = try shell.exec_stdout("git ls-files -z", .{});
    assert(files.len > 0);
    assert(files[files.len - 1] == 0);
    var lines = std.mem.splitScalar(u8, files[0 .. files.len - 1], 0);
    while (lines.next()) |line| {
        assert(line.len > 0);
        try result.append(line);
    }

    return result.items;
}
