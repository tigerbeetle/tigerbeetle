//! Grab bag of automation scripts around TigerBeetle.
//!
//! Design rationale:
//! - Bash is not cross platform, suffers from high accidental complexity, and is a second language.
//!   We strive to centralize on Zig for all of the things.
//! - While build.zig is great for _building_ software using a graph of tasks with dependency
//!   tracking, higher-level orchestration is easier if you just write direct imperative code.
//! - To minimize the number of things that need compiling and improve link times, all scripts are
//!   subcommands of a single binary.
//!
//!   This is a special case of the following rule-of-thumb: length of `build.zig` should be O(1).
const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");
const flags = @import("flags.zig");
const fatal = flags.fatal;
const Shell = @import("shell.zig");

const cfo = @import("./scripts/cfo.zig");
const ci = @import("./scripts/ci.zig");
const release = @import("./scripts/release.zig");
const devhub = @import("./scripts/devhub.zig");
const changelog = @import("./scripts/changelog.zig");

const CLIArgs = union(enum) {
    cfo: cfo.CLIArgs,
    ci: ci.CLIArgs,
    release: release.CLIArgs,
    devhub: devhub.CLIArgs,
    changelog: void,

    pub const help =
        \\Usage:
        \\
        \\  zig build scripts -- [-h | --help]
        \\
        \\  zig build scripts -- changelog
        \\
        \\  zig build scripts -- cfo [--budget-minutes=<n>] [--hang-minutes=<n>] [--concurrency=<n>]
        \\
        \\  zig build scripts -- ci [--language=<dotnet|go|java|node>] [--validate-release]
        \\
        \\  zig build scripts -- devhub --sha=<commit>
        \\
        \\  zig build scripts -- release --run-number=<run> --sha=<commit>
        \\
        \\Options:
        \\
        \\  -h, --help
        \\        Print this help message and exit.
        \\
        \\Options (release):
        \\
        \\  --language=<dotnet|go|java|node|zig|docker>
        \\        Build/publish only the specified language.
        \\        (If not set, cover all languages in sequence.)
        \\
        \\  --build
        \\        Build the packages.
        \\
        \\  --publish
        \\        Publish the packages.
        \\
    ;
};

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => @panic("memory leak"),
    };

    const gpa = gpa_allocator.allocator();

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli_args = flags.parse(&args, CLIArgs);

    switch (cli_args) {
        .cfo => |args_cfo| try cfo.main(shell, gpa, args_cfo),
        .ci => |args_ci| try ci.main(shell, gpa, args_ci),
        .release => |args_release| try release.main(shell, gpa, args_release),
        .devhub => |args_devhub| try devhub.main(shell, gpa, args_devhub),
        .changelog => try changelog.main(shell, gpa),
    }
}
