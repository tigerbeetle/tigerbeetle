//
// This script wraps ./run_with_tb.zig with setup necessary for the
// sample code in src/clients/$lang/samples/$sample. It builds and
// runs one sample, failing if the sample fails. It could have been a
// bash script except for that it works on Windows as well.
//
// Example: (run from the repo root)
//   ./zig/zig build client_integration -- --language=java --sample=basic
//

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const flags = @import("../flags.zig");

const Docs = @import("./docs_types.zig").Docs;
const TmpDir = @import("./shutil.zig").TmpDir;
const git_root = @import("./shutil.zig").git_root;
const run_shell = @import("./shutil.zig").run_shell;
const prepare_directory = @import("./docs_generate.zig").prepare_directory;
const integrate = @import("./docs_generate.zig").integrate;

const CliArgs = struct {
    language: enum { dotnet, go, java, node },
    sample: []const u8,
    keep_tmp: bool = false,
};

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip the first argument of the exe.
    assert(args.skip());

    const cli_args = flags.parse_flags(&args, CliArgs);

    const root = try git_root(&arena);

    std.debug.print("Moved to git root: {s}\n", .{root});
    try std.os.chdir(root);

    const language: Docs = switch (cli_args.language) {
        .dotnet => @import("./dotnet/docs.zig").DotnetDocs,
        .go => @import("./go/docs.zig").GoDocs,
        .java => @import("./java/docs.zig").JavaDocs,
        .node => @import("./node/docs.zig").NodeDocs,
    };

    var tmp_copy = try TmpDir.init(&arena);
    defer if (!cli_args.keep_tmp) tmp_copy.deinit();

    try prepare_directory(&arena, language, tmp_copy.path);

    // Copy the sample into a temporary directory.
    try run_shell(
        &arena,
        try std.fmt.allocPrint(
            arena.allocator(),
            // Works on Windows as well.
            "cp -r {s}/* {s}/",
            .{
                // Full path of sample directory.
                try std.fmt.allocPrint(allocator, "{s}/src/clients/{s}/samples/{s}", .{
                    root,
                    language.directory,
                    cli_args.sample,
                }),
                tmp_copy.path,
            },
        ),
    );

    try integrate(&arena, language, tmp_copy.path, true);
}

// Returning errors in main produces useless traces, at least for some
// known errors. But using errors allows defers to run. So wrap the
// main but don't pass the error back to main. Just exit(1) on
// failure.
pub fn main() !void {
    error_main() catch |err| switch (err) {
        error.RunCommandFailed => std.os.exit(1),
        else => return err,
    };
}
