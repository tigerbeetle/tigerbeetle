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
const inspect = @import("./scripts/inspect.zig");

const CliArgs = union(enum) {
    cfo: cfo.CliArgs,
    ci: ci.CliArgs,
    release: release.CliArgs,
    devhub: devhub.CliArgs,
    kcov: kcov.CliArgs,
    changelog: void,
    inspect: inspect.CliArgs,

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
        \\  zig build scripts -- inspect <superblock|wal|replies|grid|manifest|tables> <path>
        \\
        \\  zig build scripts -- release --run-number=<run> --sha=<commit>
        \\
        \\Options:
        \\
        \\  -h, --help
        \\        Print this help message and exit.
        \\
        \\Options (inspect):
        \\
        \\  When `--superblock-copy` is set, use the trailer referenced by that superblock copy.
        \\  Otherwise, copy=0 will be used by default.
        \\
        \\  superblock
        \\        Inspect the superblock header copies.
        \\        In the left column of the output, "|" denotes which copies have a particular value.
        \\        "||||" means that all four superblock copies are in agreement.
        \\        "| | " means that the value matches in copies 0/2, but differs from copies 1/3.
        \\
        \\  wal
        \\        Inspect the WAL headers and prepares.
        \\        In the left column of the output, "|" denotes which set of headers has each value.
        \\        "||" denotes that the prepare and the redundant header match.
        \\        "| " is the redundant header.
        \\        " |" is the prepare's header.
        \\
        \\  wal --slot=<slot>
        \\        Inspect the WAL header/prepare in the given slot.
        \\
        \\  replies [--superblock-copy=<copy>]
        \\        Inspect the client reply headers and session numbers.
        \\
        \\  replies --slot=<slot> [--superblock-copy=<copy>]
        \\        Inspect a particular client reply.
        \\        "||" denotes that the client session header and reply header match.
        \\        "| " is the client session header.
        \\        " |" is the client reply's header.
        \\
        \\  grid [--superblock-copy=<copy>]
        \\        Inspect the free set.
        \\
        \\  grid --block=<address>
        \\        Inspect the block at the given address.
        \\
        \\  manifest [--superblock-copy=<copy>]
        \\        Inspect the LSM manifest.
        \\
        \\  tables --tree=<name|id> [--level=<integer>] [--superblock-copy=<copy>]
        \\        List the tables matching the given tree/level.
        \\        Example tree names: "transfers" (object table), "transfers.amount" (index table).
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
        .inspect => |args_inspect| try inspect.main(gpa, args_inspect),
    }
}
