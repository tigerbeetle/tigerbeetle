//! Various CI checks that go beyond `zig build test`. Notably, at the moment this script includes:
//!
//! - Testing all language clients.
//! - Building and link-checking docs.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");
const TmpTigerBeetle = @import("../testing/tmp_tigerbeetle.zig");

const Language = std.meta.FieldEnum(@TypeOf(LanguageCI));
const LanguageCI = .{
    .dotnet = @import("../clients/dotnet/ci.zig"),
    .go = @import("../clients/go/ci.zig"),
    .java = @import("../clients/java/ci.zig"),
    .node = @import("../clients/node/ci.zig"),
};

const CliArgs = struct {
    language: ?Language = null,
    verify_release: bool = false,
};

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => fatal("memory leak", .{}),
    };

    const gpa = gpa_allocator.allocator();
    var arena_allocator = std.heap.ArenaAllocator.init(gpa);
    defer arena_allocator.deinit();

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    assert(args.skip());
    const cli_args = flags.parse_flags(&args, CliArgs);

    inline for (comptime std.enums.values(Language)) |language| {
        if (cli_args.language == language or cli_args.language == null) {
            const ci = @field(LanguageCI, @tagName(language));
            if (cli_args.verify_release) {
                var tmp_dir = std.testing.tmpDir(.{});
                defer tmp_dir.cleanup();

                try tmp_dir.dir.setAsCwd();

                try ci.verify_release(shell, gpa, tmp_dir.dir);
            } else {
                var section = try shell.open_section(@tagName(language) ++ " ci");
                defer section.close();

                {
                    try shell.pushd("./src/clients/" ++ @tagName(language));
                    defer shell.popd();

                    try ci.tests(shell, gpa);
                }

                // Piggy back on node client testing to verify our docs, as we use node to generate
                // them anyway.
                if (language == .node and builtin.os.tag == .linux) {
                    const node_version = try shell.exec_stdout("node --version", .{});
                    if (std.mem.startsWith(u8, node_version, "v14")) {
                        log.warn("skip building documentation on old Node.js", .{});
                    } else {
                        try build_docs(shell);
                    }
                }
            }
        }
    }
}

fn build_docs(shell: *Shell) !void {
    try shell.pushd("./src/docs_website");
    defer shell.popd();

    try shell.exec("npm install", .{});
    try shell.exec("npm run build", .{});
}
