const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");
const TmpTigerBeetle = @import("../testing/tmp_tigerbeetle.zig");

const Languages = .{
    .dotnet = @import("./dotnet/ci.zig"),
    .go = @import("./go/ci.zig"),
    .java = @import("./java/ci.zig"),
    .node = @import("./node/ci.zig"),
};

const CliArgs = struct {
    language: std.meta.FieldEnum(@TypeOf(Languages)),
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

    switch (cli_args.language) {
        inline else => |language| {
            const ci = @field(Languages, @tagName(language));

            if (cli_args.verify_release) {
                var tmp_dir = std.testing.tmpDir(.{});
                defer tmp_dir.cleanup();

                try tmp_dir.dir.setAsCwd();

                try ci.verify_release(shell, gpa, tmp_dir.dir);
            } else {
                var client_src_dir = try shell.project_root.openDir(
                    "src/clients/" ++ @tagName(language),
                    .{},
                );
                defer client_src_dir.close();

                try client_src_dir.setAsCwd();

                try ci.tests(shell, gpa);
            }
        },
    }
}
