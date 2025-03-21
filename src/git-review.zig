//! git-review --- a tool for offline-first, git-native code reviews.
//!
//! A review is the last commit on a branch, whose diff contains exclusively special review
//! comments:
//!
//!   //? cb22: Have you considered using stdx.BoundedArray here?
//!   //? matklad: Oh, great suggestion, thanks!
//!   //? resolved.
//!
//! Both reviewer and author amend the review commit and push --force-with-lease it to GitHub.
//! By convention, only the author can change the code, and they should rebase the review commit.
//!
//! A review commit also adds REVIEW_SUMMARY.md, which is both a cover letter and a marker for the
//! review.
//!
//! When all comments are resolved, a reverting commit is added (to preserve review itself in git
//! history).
//!
//! Why:
//! - GitHub web interface is slow, even comment text area lags!
//! - We want to encourage deeper reviews, where you play with code locally, run the fuzzers to
//!   gauge coverage, etc.
//! - Not a strong reason, but keeping review data in repository itself reduced vendor lock-in.
//!
//! ## Tool Status
//!
//! The tool is MVP. It can validate the state of review, but it lacks helper command for
//! manipulating review. It's on you to use `git push --force-with-lease`, `git stash`, `git reset`,
//! `git reflog`, `git rebase -i` and friends to construct the review state.

const std = @import("std");
const stdx = @import("./stdx.zig");
const Shell = @import("./shell.zig");
const flags = @import("./flags.zig");

const log = std.log;
const assert = std.debug.assert;

pub const std_options: std.Options = .{
    .logFn = log_fn,
};

fn log_fn(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    assert(scope == .default);
    const prefix = comptime if (level == .info) "" else level.asText() ++ ": ";
    std.debug.print(prefix ++ format ++ "\n", args);
}

const CLIArgs = union(enum) {
    new,
    status,
    lgtm,
    pub const help =
        \\Usage:
        \\
        \\  git review --help
        \\
        \\  git review new
        \\        Add an empty review commit.
        \\
        \\  git review status
        \\        Check validity of the current review.
        \\
        \\  git review lgtm
        \\        Assert that all comments are resolved, revert the review commit, push to remote.
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

    switch (cli_args) {
        .status => _ = try review_status(shell),
        .new => try review_new(shell),
        .lgtm => try review_lgtm(shell),
    }
}

fn has_review(shell: *Shell) !bool {
    const has_summary = shell.file_exists("REVIEW_SUMMARY.md");
    const has_commit = has_commit: {
        const top_commit = try shell.exec_stdout("git rev-parse HEAD", .{});
        const commit_message = try shell.exec_stdout("git log -1 --format=%B {commit}", .{
            .commit = top_commit,
        });
        break :has_commit std.mem.eql(u8, commit_message, "review\n\n");
    };
    if (has_summary and !has_commit) {
        log.err("REVIEW_SUMMARY.md present, but top-level commit is not a review", .{});
        return error.InvalidReviewState;
    }
    if (!has_summary and has_commit) {
        log.err("no REVIEW_SUMMARY.md present, but top-level commit is a review", .{});
        return error.InvalidReviewState;
    }
    return has_summary;
}

fn review_status(shell: *Shell) !enum { resolved, unresolved } {
    if (!try has_review(shell)) {
        log.info("no review", .{});
        return error.NoReview;
    }

    const diff_review = try shell.exec_stdout("git diff HEAD~ HEAD", .{});
    const stats = try parse_diff(diff_review);
    const unresolved = stats.comments_total - stats.comments_resolved;

    const merge_base = try shell.exec_stdout("git merge-base origin/main HEAD~", .{});
    const all_commits = try shell.exec_stdout("git log --format=%H {merge_base}..HEAD~", .{
        .merge_base = merge_base,
    });

    var line_iterator = std.mem.tokenizeScalar(u8, all_commits, '\n');
    while (line_iterator.next()) |commit| {
        const commit_diff = try shell.exec_stdout("git show {commit}", .{ .commit = commit });
        if (std.mem.indexOf(u8, commit_diff, "//?")) |bad_index| {
            if (std.mem.indexOf(u8, commit_diff, "git-review.zig") != null) {
                continue; // HACK: It's ok to have review markers in this file :-)
            }

            const bad_line = blk: {
                var line_start = bad_index;
                while (line_start > 0 and commit_diff[line_start] != '\n') {
                    line_start -= 1;
                }
                if (commit_diff[line_start] == '\n') line_start += 1;
                var line_end = bad_index;
                while (line_end < commit_diff.len and commit_diff[line_end] != '\n') {
                    line_end += 1;
                }
                break :blk commit_diff[line_start..line_end];
            };

            log.err("non-review commit contains review marker '//?'", .{});
            log.err("commit: {s}", .{commit});
            log.err("diff line: {s}", .{bad_line});
            return error.InvalidCommit;
        }
    }

    log.info("comments:   {}", .{stats.comments_total});
    log.info("unresolved: {}", .{unresolved});

    return if (unresolved == 0) .resolved else .unresolved;
}

fn review_new(shell: *Shell) !void {
    if (try has_review(shell)) {
        log.err("review already exists", .{});
        return error.ReviewExists;
    }

    const summary =
        \\# Review Summary
        \\
        \\ Use this for review cover letter, if needed, or to leave review-wide comments.
    ;
    try shell.cwd.writeFile(.{ .sub_path = "REVIEW_SUMMARY.md", .data = summary });

    try shell.exec("git add REVIEW_SUMMARY.md", .{});
    try shell.exec("git commit --message review", .{});
}

fn review_lgtm(shell: *Shell) !void {
    switch (try review_status(shell)) {
        .unresolved => {
            log.err("there are unresolved comments", .{});
            return error.ReviewUnresolved;
        },
        .resolved => {},
    }
    const commit = try shell.exec_stdout("git rev-parse HEAD", .{});
    try shell.exec("git revert --no-commit {commit}", .{ .commit = commit });
    try shell.exec("git commit --message {message}", .{ .message = "review revert" });
    try shell.exec("git push --force-with-lease", .{});
}

const ParseDiffResult = struct {
    comments_total: u32,
    comments_resolved: u32,
};

fn parse_diff(diff: []const u8) !ParseDiffResult {
    var result: ParseDiffResult = .{
        .comments_total = 0,
        .comments_resolved = 0,
    };
    var file_name: []const u8 = "";
    var line_iterator = std.mem.tokenizeScalar(u8, diff, '\n');
    var comment_start: ?u32 = null;
    var line_index: u32 = 0;
    while (line_iterator.next()) |line| {
        defer line_index += 1;

        if (stdx.cut_prefix(line, "diff --git a/")) |suffix| {
            _, file_name = stdx.cut(suffix, " ").?.unpack();
            continue;
        }
        assert(file_name.len > 0);
        if (std.mem.eql(u8, file_name, "b/REVIEW_SUMMARY.md")) continue;
        errdefer log.err("invalid review in '{s}':\n{s}", .{ file_name, line });

        if (std.mem.startsWith(u8, line, "- ")) {
            return error.InvalidDiff;
        }

        if (stdx.cut_prefix(line, "+")) |line_added| {
            if (std.mem.startsWith(u8, line_added, "++")) continue;
            const comment = std.mem.trimLeft(u8, line_added, " ");
            if (!std.mem.startsWith(u8, comment, "//?")) {
                return error.InvalidDiff;
            }

            if (comment_start == null) {
                comment_start = line_index;
                result.comments_total += 1;
            } else {
                if (std.mem.eql(u8, comment, "//? resolved.")) {
                    assert(comment_start != null);
                    comment_start = null;
                    result.comments_resolved += 1;
                }
            }
        } else {
            comment_start = null;
        }
    }
    assert(result.comments_total >= result.comments_resolved);
    return result;
}
