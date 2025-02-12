//! git-review --- a tool for offline-first, git-native code reviews.
//!
//! A review is the last commit on a branch, whose diff contains exclusively special review
//! comments:
//!
//!   // r cb22:
//!   // Have you considered using stdx.BoundedArray here?
//!   // r matklad:
//!   // Oh, great suggestion, thanks!
//!   // r resolved.
//!
//! Both reviewer and author amend the review commit and push --force-with-lease it to GitHub.
//! By convention, only the author can change the code, and they should rebase the review commit.
//!
//! When all comments are resolved, a reverting commit is added (to preserve review itself in git
//! history)
//!
//! Why:
//! - GitHub web interface is slow, even comment text area lags!
//! - We want to encourage deeper reviews, where you play with code locally, run the fuzzers to
//!   gauge coverage, etc.
//! - Not a strong reason, but keeping review data in repository itself reduced vendor lock-in.

const std = @import("std");
const Shell = @import("./shell.zig");
const flags = @import("./flags.zig");

const log = std.log;

const CLIArgs = union(enum) {
    pub const help =
        \\Usage:
        \\
        \\  git review --help
        \\
        \\  git review status
        \\        Check validity of the current review.
        \\
        \\  git review new
        \\        Add and push an empty review commit.
        \\
        \\  git review find
        \\        Find and checkout a PR for review.
        \\
        \\  git review pull [PR]
        \\        Synchronize review state with remote.
        \\
        \\  git review push
        \\        Add new comments to the review.
        \\
        \\  git review resolve
        \\        Assert that all comments are resolved and revert the review commit.
        \\
        \\  git review diff-in-place
        \\        Reset branch to the merge base.
        \\
    ;
};

pub fn main() !void {
    var gpa_allocator: std.heap.GeneralPurposeAllocator(.{}) = .{};
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

    switch (cli_args) {}
}
