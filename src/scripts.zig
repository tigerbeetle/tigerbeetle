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
const kcov = @import("./scripts/kcov.zig");
const changelog = @import("./scripts/changelog.zig");

const CliArgs = union(enum) {
    cfo: cfo.CliArgs,
    ci: ci.CliArgs,
    release: release.CliArgs,
    devhub: devhub.CliArgs,
    kcov: kcov.CliArgs,
    changelog: void,
};

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => fatal("memory leak", .{}),
    };

    const gpa = gpa_allocator.allocator();

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli_args = flags.parse(&args, CliArgs);

    switch (cli_args) {
        .cfo => |args_cfo| try cfo.main(shell, gpa, args_cfo),
        .ci => |args_ci| try ci.main(shell, gpa, args_ci),
        .release => |args_release| try release.main(shell, gpa, args_release),
        .devhub => |args_devhub| try devhub.main(shell, gpa, args_devhub),
        .kcov => |args_kcov| try kcov.main(shell, gpa, args_kcov),
        .changelog => try changelog.main(shell, gpa),
    }
}
