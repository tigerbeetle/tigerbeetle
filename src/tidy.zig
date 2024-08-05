//! Checks for various non-functional properties of the code itself.

const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const mem = std.mem;
const math = std.math;

const stdx = @import("./stdx.zig");
const Shell = @import("./shell.zig");

test "tidy" {
    const allocator = std.testing.allocator;

    const buffer_size = 1024 * 1024;
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    var src_dir = try fs.cwd().openDir("./src", .{ .iterate = true });
    defer src_dir.close();

    var walker = try src_dir.walk(allocator);
    defer walker.deinit();

    var dead_detector = DeadDetector.init(allocator);
    defer dead_detector.deinit();

    var function_line_count_longest: usize = 0;

    // NB: all checks are intentionally implemented in a streaming fashion, such that we only need
    // to read the files once.
    while (try walker.next()) |entry| {
        if (entry.kind == .file and mem.endsWith(u8, entry.path, ".zig")) {
            const file = try entry.dir.openFile(entry.basename, .{});
            defer file.close();

            const bytes_read = try file.readAll(buffer);
            if (bytes_read == buffer.len - 1) return error.FileTooLong;
            buffer[bytes_read] = 0;

            const source_file = SourceFile{ .path = entry.path, .text = buffer[0..bytes_read :0] };
            try tidy_banned(source_file);
            try tidy_long_line(source_file);

            function_line_count_longest = @max(
                function_line_count_longest,
                (try tidy_long_functions(source_file)).function_line_count_longest,
            );

            try dead_detector.visit(source_file);
        }
    }

    try dead_detector.finish();

    if (function_line_count_longest < function_line_count_max) {
        std.debug.print("error: `function_line_count_max` must be updated to {d}\n", .{
            function_line_count_longest,
        });
    }
}

const SourceFile = struct { path: []const u8, text: [:0]const u8 };

fn tidy_banned(file: SourceFile) !void {
    if (banned(file.text)) |ban| {
        std.debug.print(
            "{s}: banned: {s}\n",
            .{ file.path, ban },
        );
        return error.Banned;
    }
}

fn tidy_long_line(file: SourceFile) !void {
    if (std.mem.endsWith(u8, file.path, "low_level_hash_vectors.zig")) return;
    const long_line = try find_long_line(file.text);
    if (long_line) |line_index| {
        std.debug.print(
            "{s}:{d} error: line exceeds 100 columns\n",
            .{ file.path, line_index + 1 },
        );
        return error.LineTooLong;
    }
}

/// As we trim our functions, make sure to update this constant; tidy will error if you do not.
const function_line_count_max = 361; // build_tigerbeetle

fn tidy_long_functions(
    file: SourceFile,
) !struct {
    function_line_count_longest: usize,
} {
    const allocator = std.testing.allocator;

    if (std.mem.endsWith(u8, file.path, "client_readmes.zig")) {
        // This file is essentially a template to generate a markdown file, so it
        // intentionally has giant functions.
        return .{ .function_line_count_longest = 0 };
    }

    const Function = struct {
        fn_decl_line: usize,
        first_token_location: std.zig.Ast.Location,
        last_token_location: std.zig.Ast.Location,
        /// Functions that are not "innermost," meaning that they have other functions
        /// inside of them (such as functions that return `type`s) are not checked as
        /// it is normal for them to be very lengthy.
        is_innermost: bool,

        fn is_parent_of(a: @This(), b: @This()) bool {
            return a.first_token_location.line_start < b.first_token_location.line_start and
                a.last_token_location.line_end > b.last_token_location.line_end;
        }

        fn get_and_check_line_count(
            function: @This(),
            file_of_function: SourceFile,
        ) usize {
            const function_line_count =
                function.last_token_location.line -
                function.first_token_location.line;

            if (function_line_count > function_line_count_max) {
                std.debug.print(
                    "{s}:{d} error: above function line count max with {d} lines\n",
                    .{
                        file_of_function.path,
                        function.fn_decl_line + 1,
                        function_line_count,
                    },
                );
            }

            return function_line_count;
        }
    };

    var function_stack = stdx.BoundedArray(Function, 32).from_slice(&.{}) catch unreachable;

    var tree = try std.zig.Ast.parse(allocator, file.text, .zig);
    defer tree.deinit(allocator);

    const tags = tree.nodes.items(.tag);
    const datas = tree.nodes.items(.data);

    var function_line_count_longest: usize = 0;

    for (tags, datas, 0..) |tag, data, function_decl_node| {
        if (tag != .fn_decl) continue;

        const function_body_node = data.rhs;

        const function_decl_first_token = tree.firstToken(@intCast(function_decl_node));
        const function_body_first_token = tree.firstToken(@intCast(function_body_node));
        const function_body_last_token = tree.lastToken(@intCast(function_body_node));

        const innermost_function = .{
            .fn_decl_line = tree.tokenLocation(0, function_decl_first_token).line,
            .first_token_location = tree.tokenLocation(0, function_body_first_token),
            .last_token_location = tree.tokenLocation(0, function_body_last_token),
            .is_innermost = true,
        };

        while (function_stack.count() > 0) {
            const last_function = function_stack.get(function_stack.count() - 1);

            if (!last_function.is_parent_of(innermost_function)) {
                if (last_function.is_innermost) {
                    const line_count = last_function.get_and_check_line_count(file);
                    function_line_count_longest = @max(function_line_count_longest, line_count);
                }
                _ = function_stack.pop();
            } else {
                break;
            }
        }

        if (function_stack.count() > 0) {
            const last_function = &function_stack.slice()[function_stack.count() - 1];

            assert(last_function.is_parent_of(innermost_function));
            last_function.is_innermost = false;
        }

        function_stack.append_assume_capacity(innermost_function);
    }

    if (function_stack.count() > 0) {
        const last_function = function_stack.get(function_stack.count() - 1);

        if (last_function.is_innermost) {
            const line_count = last_function.get_and_check_line_count(file);
            function_line_count_longest = @max(function_line_count_longest, line_count);
        }
    }

    return .{
        .function_line_count_longest = function_line_count_longest,
    };
}

// Zig's lazy compilation model makes it too easy to forget to include a file into the build --- if
// nothing imports a file, compiler just doesn't see it and can't flag it as unused.
//
// DeadDetector implements heuristic detection of unused files, by "grepping" for import statements
// and flagging file which are never imported. This gives false negatives for unreachable cycles of
// files, as well as for identically-named files, but it should be good enough in practice.
const DeadDetector = struct {
    const FileName = [64]u8;
    const FileState = struct { import_count: u32, definition_count: u32 };
    const FileMap = std.AutoArrayHashMap(FileName, FileState);

    files: FileMap,

    fn init(allocator: std.mem.Allocator) DeadDetector {
        return .{ .files = FileMap.init(allocator) };
    }

    fn deinit(detector: *DeadDetector) void {
        detector.files.deinit();
    }

    fn visit(detector: *DeadDetector, file: SourceFile) !void {
        (try detector.file_state(file.path)).definition_count += 1;

        var text: []const u8 = file.text;
        for (0..1024) |_| {
            const cut = stdx.cut(text, "@import(\"") orelse break;
            text = cut.suffix;
            const import_path = stdx.cut(text, "\")").?.prefix;
            if (std.mem.endsWith(u8, import_path, ".zig")) {
                (try detector.file_state(import_path)).import_count += 1;
            }
        } else {
            std.debug.panic("file with more than 1024 imports: {s}", .{file.path});
        }
    }

    fn finish(detector: *DeadDetector) !void {
        defer detector.files.clearRetainingCapacity();

        for (detector.files.keys(), detector.files.values()) |name, state| {
            assert(state.definition_count > 0);
            if (state.import_count == 0 and !is_entry_point(name)) {
                std.debug.print("file never imported: {s}\n", .{name});
                return error.DeadFile;
            }
        }
    }

    fn file_state(detector: *DeadDetector, path: []const u8) !*FileState {
        const gop = try detector.files.getOrPut(path_to_name(path));
        if (!gop.found_existing) gop.value_ptr.* = .{ .import_count = 0, .definition_count = 0 };
        return gop.value_ptr;
    }

    fn path_to_name(path: []const u8) FileName {
        assert(std.mem.endsWith(u8, path, ".zig"));
        const basename = std.fs.path.basename(path);
        var file_name: FileName = .{0} ** 64;
        assert(basename.len <= file_name.len);
        stdx.copy_disjoint(.inexact, u8, &file_name, basename);
        return file_name;
    }

    fn is_entry_point(file: FileName) bool {
        const entry_points: []const []const u8 = &.{
            "fuzz_tests.zig",
            "integration_tests.zig",
            "jni_tests.zig",
            "main.zig",
            "node.zig",
            "vopr.zig",
            "tb_client_header.zig",
            "unit_tests.zig",
            "scripts.zig",
            "tb_client_exports.zig",
            "dotnet_bindings.zig",
            "go_bindings.zig",
            "node_bindings.zig",
            "java_bindings.zig",
        };
        for (entry_points) |entry_point| {
            if (std.mem.startsWith(u8, &file, entry_point)) return true;
        }
        return false;
    }
};

test "tidy changelog" {
    const allocator = std.testing.allocator;

    const changelog_size_max = 1024 * 1024;
    const changelog = try fs.cwd().readFileAlloc(allocator, "CHANGELOG.md", changelog_size_max);
    defer allocator.free(changelog);

    var line_iterator = mem.split(u8, changelog, "\n");
    var line_index: usize = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        if (std.mem.endsWith(u8, line, " ")) {
            std.debug.print("CHANGELOG.md:{d} trailing whitespace", .{line_index + 1});
            return error.TrailingWhitespace;
        }
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100 and !has_link(line)) {
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

    const MiB = 1024 * 1024;
    const rev_list = try shell.exec_stdout_options(
        .{ .max_output_bytes = 50 * MiB },
        "git rev-list --objects HEAD",
        .{},
    );
    const objects = try shell.exec_stdout_options(
        .{ .max_output_bytes = 50 * MiB, .stdin_slice = rev_list },
        "git cat-file --batch-check={format}",
        .{ .format = "%(objecttype) %(objectsize) %(rest)" },
    );

    var has_large_blobs = false;
    var lines = std.mem.split(u8, objects, "\n");
    while (lines.next()) |line| {
        // Parsing lines like
        //     blob 1032 client/package.json
        const blob = stdx.cut_prefix(line, "blob ") orelse continue;

        const cut = stdx.cut(blob, " ").?;
        const size = try std.fmt.parseInt(u64, cut.prefix, 10);
        const path = cut.suffix;

        if (std.mem.eql(u8, path, "src/vsr/replica.zig")) continue; // :-)
        if (std.mem.eql(u8, path, "src/docs_website/package-lock.json")) continue; // :-(
        if (size > @divExact(MiB, 4)) {
            has_large_blobs = true;
            std.debug.print("{s}\n", .{line});
        }
    }
    if (has_large_blobs) return error.HasLargeBlobs;
}

// Sanity check for "unexpected" files in the repository.
test "tidy extensions" {
    const allowed_extensions = std.StaticStringMap(void).initComptime(.{
        .{".bat"}, .{".c"},     .{".cs"},   .{".csproj"},  .{".css"},  .{".go"},
        .{".h"},   .{".hcl"},   .{".java"}, .{".js"},      .{".json"}, .{".md"},
        .{".mod"}, .{".props"}, .{".ps1"},  .{".service"}, .{".sln"},  .{".sum"},
        .{".ts"},  .{".txt"},   .{".xml"},  .{".yml"},     .{".zig"},
    });

    const exceptions = std.StaticStringMap(void).initComptime(.{
        .{".editorconfig"},                          .{".gitattributes"},
        .{".gitignore"},                             .{".nojekyll"},
        .{"CNAME"},                                  .{"Dockerfile"},
        .{"exclude-pmd.properties"},                 .{"favicon.ico"},
        .{"favicon.png"},                            .{"LICENSE"},
        .{"module-info.test"},                       .{"index.html"},
        .{"logo.svg"},                               .{"logo-white.svg"},
        .{"logo-with-text-white.svg"},               .{"zig/download.sh"},
        .{"src/scripts/cfo_supervisor.sh"},          .{"src/docs_website/scripts/build.sh"},
        .{".github/ci/docs_check.sh"},               .{".github/ci/test_aof.sh"},
        .{"tools/systemd/tigerbeetle-pre-start.sh"}, .{"tools/vscode/format_debug_server.sh"},
    });

    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const files = try shell.exec_stdout("git ls-files", .{});
    var lines = std.mem.split(u8, files, "\n");
    var bad_extension = false;
    while (lines.next()) |path| {
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

fn banned(source: []const u8) ?[]const u8 {
    // Note: must avoid banning ourselves!
    if (std.mem.indexOf(u8, source, "std." ++ "BoundedArray") != null) {
        return "use stdx." ++ "BoundedArray instead of std version";
    }

    if (std.mem.indexOf(u8, source, "trait." ++ "hasUniqueRepresentation") != null) {
        return "use stdx." ++ "has_unique_representation instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copy(") != null) {
        return "use stdx." ++ "copy_disjoint instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copyForwards(") != null) {
        return "use stdx." ++ "copy_left instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copyBackwards(") != null) {
        return "use stdx." ++ "copy_right instead of std version";
    }

    // Ban "fixme" comments. This allows using fixme as reminders with teeth --- when working on
    // larger pull requests, it is often helpful to leave fixme comments as a reminder to oneself.
    // This tidy rule ensures that the reminder is acted upon before code gets into main. That is:
    // - use fixme for issues to be fixed in the same pull request,
    // - use todo as general-purpose long-term remainders without enforcement.
    if (std.mem.indexOf(u8, source, "FIX" ++ "ME") != null) {
        return "FIX" ++ "ME comments must be addressed before getting to main";
    }

    return null;
}

fn find_long_line(file_text: []const u8) !?usize {
    var line_iterator = mem.split(u8, file_text, "\n");
    var line_index: usize = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100) {
            if (has_link(line)) continue;

            // Journal recovery table
            if (std.mem.indexOf(u8, line, "Case.init(") != null) continue;

            // For multiline strings, we care that the _result_ fits 100 characters,
            // but we don't mind indentation in the source.
            if (parse_multiline_string(line)) |string_value| {
                const string_value_length = try std.unicode.utf8CountCodepoints(string_value);
                if (string_value_length <= 100) continue;

                if (std.mem.startsWith(u8, string_value, " account A") or
                    std.mem.startsWith(u8, string_value, " transfer T") or
                    std.mem.startsWith(u8, string_value, " transfer   "))
                {
                    // Table tests from state_machine.zig. They are intentionally wide.
                    continue;
                }

                // vsr.zig's Checkpoint ops diagram.
                if (std.mem.startsWith(u8, string_value, "OPS: ")) continue;
            }

            return line_index;
        }
    }
    return null;
}

/// Heuristically checks if a `line` contains an URL.
fn has_link(line: []const u8) bool {
    return std.mem.indexOf(u8, line, "https://") != null;
}

/// If a line is a `\\` string literal, extract its value.
fn parse_multiline_string(line: []const u8) ?[]const u8 {
    const cut = stdx.cut(line, "\\\\") orelse return null;
    for (cut.prefix) |c| if (c != ' ') return null;
    return cut.suffix;
}
