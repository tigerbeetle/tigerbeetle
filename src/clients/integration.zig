//
// This script wraps ./run_with_tb.zig with setup necessary for the
// sample code in src/clients/$lang/samples/$sample. It builds and
// runs one sample, failing if the sample fails. It could have been a
// bash script except for that it works on Windows as well.
//
// Example: (run from the repo root)
//   ./scripts/build.sh client_integration -- --language java --sample basic
//

const std = @import("std");
const builtin = @import("builtin");

const java_docs = @import("./java/docs.zig").JavaDocs;
const go_docs = @import("./go/docs.zig").GoDocs;
const node_docs = @import("./node/docs.zig").NodeDocs;
const Docs = @import("./docs_types.zig").Docs;
const TmpDir = @import("./shutil.zig").TmpDir;
const git_root = @import("./shutil.zig").git_root;
const run_shell = @import("./shutil.zig").run_shell;
const prepare_directory_and_integrate = @import("./docs_generate.zig").prepare_directory_and_integrate;

fn copy_into_tmp_dir(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
) !TmpDir {
    var t = try TmpDir.init(arena);

    try run_shell(
        arena,
        // Should actually work on Windows as well!
        try std.fmt.allocPrint(
            arena.allocator(),
            "cp -r {s}/* {s}/",
            .{ sample_dir, t.path },
        ),
    );

    return t;
}

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const root = try git_root(&arena);

    std.debug.print("Moved to git root: {s}\n", .{root});
    try std.os.chdir(root);

    var args = std.process.args();
    _ = args.next(allocator);
    var language: ?Docs = null;
    var sample: []const u8 = "";
    var keep_tmp = false;
    while (args.next(allocator)) |arg_or_err| {
        const arg = arg_or_err catch {
            std.debug.print("Could not parse all arguments.\n", .{});
            return error.CouldNotParseArguments;
        };

        if (std.mem.eql(u8, arg, "--language")) {
            const next = try args.next(allocator) orelse "";

            if (std.mem.eql(u8, next, "java")) {
                language = java_docs;
            } else if (std.mem.eql(u8, next, "node")) {
                language = node_docs;
            } else if (std.mem.eql(u8, next, "go")) {
                language = go_docs;
            } else {
                std.debug.print("Unknown language: {s}.\n", .{next});
                return error.UnknownLanguage;
            }
        }

        if (std.mem.eql(u8, arg, "--keep-tmp")) {
            keep_tmp = true;
        }

        if (std.mem.eql(u8, arg, "--sample")) {
            const next = try args.next(allocator) orelse "";

            sample = next;
        }
    }

    if (sample.len == 0) {
        std.debug.print("--sample not set.\n", .{});
        return error.SampleNotSet;
    }

    if (language == null) {
        std.debug.print("--language not set.\n", .{});
        return error.LanguageNotSet;
    }

    // Copy the sample into a temporary directory.
    const sample_dir = try std.fmt.allocPrint(
        allocator,
        "{s}/src/clients/{s}/samples/{s}",
        .{
            root,
            language.?.directory,
            sample,
        },
    );
    var tmp_copy = try copy_into_tmp_dir(&arena, sample_dir);

    // Not a great hack. The integration code depends on being able to
    // init go.mod. This is reasonable in most cases but breaks
    // integration testing of the sample code which do already including
    // go.mod.
    if (std.mem.eql(u8, language.?.markdown_name, "go")) {
        try std.os.unlink(
            try std.fmt.allocPrint(
                arena.allocator(),
                "{s}/go.mod",
                .{tmp_copy.path},
            ),
        );
    }

    defer {
        if (!keep_tmp) {
            tmp_copy.deinit();
        }
    }

    try prepare_directory_and_integrate(&arena, language.?, tmp_copy.path, true);
}

// Returning errors in main produces useless traces, at least for some
// known errors. But using errors allows defers to run. So wrap the
// main but don't pass the error back to main. Just exit(1) on
// failure.
pub fn main() !void {
    if (error_main()) {
        // fine
    } else |err| switch (err) {
        error.RunCommandFailed => std.os.exit(1),
        else => return err,
    }
}
