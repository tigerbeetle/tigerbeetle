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
//!
//! # Workflow for Author
//!
//! - Submit a PR normally through GitHub.
//! - Run `git review new`.
//! - Optionally:
//!   - do a self-review by adding `// r yourname:` comments or editing REVIEW_SUMMARY.md
//!   - submit self-review via `git review push`
//! - To get new review comments, run `git review pull`
//! - To address code changes, add or change commits below the review comment.
//! - To respond to comments, add more `// r yourname:` lines.
//! - Add `// r resolved.` if you consider a particular thread resolved. Both author and reviewer
//!   can resolve threads and there's no preference to either.
//! - To upload review, run `git review push`.
//!
//! # Workflow for Review
//!
//! - Ideally, switch to a dedicated review work-tree.
//! - Run `git review find` to use fzf to checkout PR interactively.
//! - Or run `git review pull $pr` if you already know the number of the pull request.
//! - If there's no REVIEW_SUMMARY file, run `git review new`.
//! - Add your review comments using `// r yourname:` syntax.
//! - To upload review, run `git review push`.
//! - To fetch response, run `git review pull` without an argument.
//! - Once you are satisfied with review, run `git review resolve`.
//! - Approve PR on GitHub (if the PR small&doesn't need any changes, you can skip the review dance
//!   and approve directly on GitHub).

const std = @import("std");
const stdx = @import("./stdx.zig");
const Shell = @import("./shell.zig");
const flags = @import("./flags.zig");

const Allocator = std.mem.Allocator;
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
    status,
    new,
    find,
    pull: struct {
        positional: struct { pr: ?u32 = null },
    },
    push,
    lgtm,
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
        \\        Find and then pull a PR for review.
        \\
        \\  git review pull [PR]
        \\        Synchronize review state with remote.
        \\
        \\  git review push
        \\        Add new comments to the review.
        \\
        \\  git review lgtm
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

    switch (cli_args) {
        .status => _ = try review_status(shell),
        .new => try review_new(shell),
        .find => try review_find(shell),
        .push => try review_push(shell),
        .pull => |pull| try review_pull(shell, pull.positional.pr),
        .lgtm => try review_lgtm(shell),
    }
}

fn review_status(shell: *Shell) !enum { resolved, unresolved } {
    if (!shell.file_exists("REVIEW_SUMMARY.md")) {
        log.info("no review", .{});
        return error.NoReview;
    }

    const commit = try shell.exec_stdout("git rev-parse HEAD", .{});
    const commit_message = try shell.exec_stdout("git log -1 --format=%B {commit}", .{
        .commit = commit,
    });
    if (!std.mem.eql(u8, commit_message, "review\n\n")) {
        log.err("REVIEW_SUMMARY.md file present, but the top commit is not a review:{s}", .{
            commit_message,
        });
        return error.InvalidCommit;
    }

    defer {
        shell.exec("git reset {commit}", .{ .commit = commit }) catch {};
    }
    try shell.exec("git add .", .{});
    try shell.exec("git commit --amend --no-edit", .{});

    const diff = try shell.exec_stdout("git diff HEAD~ HEAD", .{});
    const stats = try parse_diff(diff);
    const unresolved = stats.comments_total - stats.comments_resolved;
    log.info("comments:   {}", .{stats.comments_total});
    log.info("unresolved: {}", .{unresolved});
    return if (unresolved == 0) .resolved else .unresolved;
}

fn review_new(shell: *Shell) !void {
    if (try git_has_changes(shell)) {
        log.err("working tree is dirty", .{});
        return error.DirtyWorkingTree;
    }
    try shell.cwd.writeFile(.{ .sub_path = "REVIEW_SUMMARY.md", .data =
    \\# Review Summary
    \\
    });

    try shell.exec("git add REVIEW_SUMMARY.md", .{});
    try shell.exec("git commit -m review", .{});
}

fn review_find(shell: *Shell) !void {
    if (try git_has_changes(shell)) {
        log.err("working tree is dirty", .{});
        return error.DirtyWorkingTree;
    }

    const pull_requsts = try shell.exec_stdout("gh pr list --assignee=@me", .{});
    const selected = try shell.exec_stdout_options(.{
        .stdin_slice = pull_requsts,
    }, "fzf --info=inline --height=~100% --reverse", .{});
    const pr_number_string, _ =
        (stdx.cut(selected, "\t") orelse return error.NoSelection).unpack();
    const pr_number = try std.fmt.parseInt(u32, pr_number_string, 10);

    try review_pull(shell, pr_number);
}

fn review_pull(shell: *Shell, pull_request: ?u32) !void {
    if (try git_has_changes(shell)) {
        log.err("working tree is dirty", .{});
        return error.DirtyWorkingTree;
    }

    if (pull_request) |number| {
        try shell.exec("gh pr checkout --branch review-{number} {number}", .{
            .number = number,
        });
        _ = try review_status(shell);
    } else {
        // No `pull_request` means we want to sync our local state with remote state.
        const branch_full = try shell.exec_stdout(
            "git rev-parse --abbrev-ref --symbolic-full-name {ref}",
            .{ .ref = "@{u}" },
        );
        const branch = stdx.cut_prefix(branch_full, "origin/").?;
        try shell.exec("git fetch origin {branch}", .{ .branch = branch });
        try shell.exec("git reset --hard origin/{branch}", .{ .branch = branch });
    }
}

fn review_push(shell: *Shell) !void {
    _ = try review_status(shell);
    try shell.exec("git add .", .{});
    try shell.exec("git commit --amend --no-edit", .{});
    try git_push(shell);
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
    try shell.exec("git commit -m {message}", .{ .message = "review revert" });
    try git_push(shell);
}

fn files_with_review_comments(shell: *Shell) []const []const u8 {
    const committed = shell.exec_stdout("git show --name-only --pretty=format:");
    const pending = shell.exec("git status --short", .{});

    var result: std.StringArrayHashMapUnmanaged(void) = .{};

    for (.{ committed, pending }) |lines| {
        var line_iterator = std.mem.tokenizeScalar(u8, lines, '\n');
        while (line_iterator.next()) |line| {
            try result.put(shell.arena.allocator(), line, {});
        }
    }

    return result.keys();
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
        errdefer log.err("invalid review in '{s}':\n{s}", .{ file_name, line });

        if (std.mem.startsWith(u8, line, "- ")) {
            return error.InvalidDiff;
        }

        if (stdx.cut_prefix(line, "+ ")) |line_added| {
            const indent, const comment_content =
                (stdx.cut(line_added, "//") orelse return error.InvalidDiff).unpack();
            if (!is_blank(indent)) return error.InvalidDiff;

            const command = parse_command(comment_content);
            if (comment_start == null) {
                if (command == null or command.? != .author) return error.InvalidDiff;

                comment_start = line_index;
                result.comments_total += 1;
            } else {
                if (command != null and command.? == .resolved) {
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

fn parse_command(comment_content: []const u8) ?union(enum) { author: []const u8, resolved } {
    const command_suffix = stdx.cut_prefix(comment_content, " r ") orelse return null;
    if (std.mem.eql(u8, command_suffix, "resolved.")) return .resolved;
    const author = stdx.cut_suffix(command_suffix, ":") orelse return null;
    return .{ .author = author };
}

fn is_blank(text: []const u8) bool {
    for (text) |c| if (c != ' ') return false;
    return true;
}

fn git_has_changes(shell: *Shell) !bool {
    const output = try shell.exec_stdout("git status --short", .{});
    return output.len > 0;
}

const dry_run = false;

fn git_push(shell: *Shell) !void {
    if (dry_run) return;

    try shell.exec("git push --force-with-lease", .{});
}
