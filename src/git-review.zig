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
//! A review commit also adds REVIEW.md, which is both a cover letter and a marker for the
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
    split,
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
        \\  git review split
        \\        Splits mixed review commit into code changes and review proper.
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
        .split => try review_split(shell),
        .lgtm => try review_lgtm(shell),
    }
}

fn has_review(shell: *Shell) !bool {
    const has_summary = shell.file_exists("REVIEW.md");
    const has_commit = has_commit: {
        const top_commit = try shell.exec_stdout("git rev-parse HEAD", .{});
        const commit_message = try shell.exec_stdout("git log -1 --format=%B {commit}", .{
            .commit = top_commit,
        });
        break :has_commit std.mem.eql(u8, commit_message, "review\n\n");
    };
    if (has_summary and !has_commit) {
        log.err("REVIEW.md present, but top-level commit is not a review", .{});
        return error.InvalidReviewState;
    }
    if (!has_summary and has_commit) {
        log.err("no REVIEW.md present, but top-level commit is a review", .{});
        return error.InvalidReviewState;
    }
    return has_summary;
}

fn review_status(shell: *Shell) !enum { resolved, unresolved } {
    if (!try has_review(shell)) {
        log.info("no review", .{});
        return error.NoReview;
    }

    const diff_review = try shell.exec_stdout("git diff --unified=0 HEAD~ HEAD", .{});
    const review = try Review.parse(shell.arena.allocator(), diff_review);

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

    try std.io.getStdErr().writer().print("{}", .{review});

    return if (review.unresolved_count == 0) .resolved else .unresolved;
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
    try shell.cwd.writeFile(.{ .sub_path = "REVIEW.md", .data = summary });

    try shell.exec("git add REVIEW.md", .{});
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

fn review_split(shell: *Shell) !void {
    if (!try has_review(shell)) {
        log.err("no review in progress", .{});
        return error.NoReview;
    }
    const commit = try shell.exec_stdout("git rev-parse HEAD", .{});
    const changed_files = try shell.exec_stdout("git diff --name-only HEAD~ HEAD", .{});

    shell.exec("git diff --quiet --cached", .{}) catch {
        log.err("git index is dirty", .{});
        return error.DirtyIndex;
    };

    var it = std.mem.tokenizeScalar(u8, changed_files, '\n');
    while (it.next()) |file| {
        shell.exec("git diff --quiet {file}", .{ .file = file }) catch {
            log.err("working tree is dirty: {s}", .{file});
            return error.DirtyWorkingTree;
        };
    }

    log.info("splitting commit {s}", .{commit});

    try shell.exec("git reset HEAD~", .{});
    it.reset();
    while (it.next()) |file| {
        if (std.mem.eql(u8, file, "REVIEW.md")) continue;
        const text_before = try shell.cwd.readFileAlloc(
            shell.arena.allocator(),
            file,
            1 * 1024 * 1024,
        );

        const fd = try shell.cwd.createFile(file, .{ .truncate = true });
        defer fd.close();

        var fbs = std.io.fixedBufferStream(text_before);
        try remove_review_comments(fbs.reader().any(), fd.writer().any());
        try shell.exec("git add {file}", .{ .file = file });
    }
    try shell.exec("git commit -m {message}", .{ .message = "review changes" });

    it.reset();
    while (it.next()) |file| {
        try shell.exec("git restore {file} --source {commit}", .{
            .file = file,
            .commit = commit,
        });
        try shell.exec("git add {file}", .{ .file = file });
    }
    try shell.exec("git commit -m review", .{});
    log.info("done", .{});
}

fn remove_review_comments(reader: std.io.AnyReader, writer: std.io.AnyWriter) !void {
    while (true) {
        var line_buffer: [4096]u8 = undefined;
        const line = try reader.readUntilDelimiterOrEof(&line_buffer, '\n') orelse return;
        const line_no_indent = std.mem.trimLeft(u8, line, " ");
        if (std.mem.startsWith(u8, line_no_indent, "//?")) {
            // Skip this line.
        } else {
            try writer.writeAll(line);
            try writer.writeAll("\n");
        }
    }
}

const Review = struct {
    comments: []const Comment,
    resolved_count: u32,
    unresolved_count: u32,

    const Comment = struct {
        location: Location,
        snippet: []const u8,
        resolved: bool,
    };

    const Location = struct {
        file: []const u8,
        line: u32, // 1-based display number.

        pub fn format(
            location: Location,
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;
            try writer.print("{s}:{d}", .{ location.file, location.line });
        }
    };

    pub fn parse(arena: std.mem.Allocator, diff: []const u8) !Review {
        var comments: std.ArrayListUnmanaged(Comment) = .{};
        var unresolved_count: u32 = 0;
        var resolved_count: u32 = 0;

        var file_name: []const u8 = "";
        var hunk_line: ?u32 = null;
        var line_iterator = std.mem.tokenizeScalar(u8, diff, '\n');
        var in_comment = false;
        while (line_iterator.next()) |line| {
            if (stdx.cut_prefix(line, "diff --git a/")) |suffix| {
                _, file_name = stdx.cut(suffix, " b/").?;
                hunk_line = null;
                continue;
            }
            assert(file_name.len > 0);
            if (std.mem.eql(u8, file_name, "REVIEW.md")) continue;
            errdefer log.err("invalid review in '{s}':\n{s}", .{ file_name, line });

            if (std.mem.startsWith(u8, line, "@@")) {
                // Extract '380' from
                // @@ -379,0 +380,2 @@
                _, const added = stdx.cut(line, " +").?;
                const hunk_line_str, _ = stdx.cut(added, ",").?;
                hunk_line = std.fmt.parseInt(u32, hunk_line_str, 10) catch unreachable;
            }

            if (std.mem.startsWith(u8, line, "- ")) {
                return error.InvalidDiff;
            }

            if (stdx.cut_prefix(line, "+")) |line_added| {
                if (std.mem.startsWith(u8, line_added, "++")) continue;
                assert(hunk_line != null);

                const comment = std.mem.trimLeft(u8, line_added, " ");
                if (!std.mem.startsWith(u8, comment, "//?")) {
                    return error.InvalidDiff;
                }

                if (!in_comment) {
                    in_comment = true;
                    try comments.append(arena, .{
                        .location = .{
                            .file = file_name,
                            .line = hunk_line.?,
                        },
                        .snippet = stdx.cut_prefix(comment, "//?").?,
                        .resolved = false,
                    });
                    unresolved_count += 1;
                } else {
                    if (std.mem.eql(u8, comment, "//? resolved.")) {
                        assert(comments.items.len > 0);
                        assert(!comments.items[comments.items.len - 1].resolved);
                        comments.items[comments.items.len - 1].resolved = true;
                        in_comment = false;
                        unresolved_count -= 1;
                        resolved_count += 1;
                    }
                }
            } else {
                in_comment = false;
            }
        }

        return .{
            .comments = comments.items,
            .resolved_count = resolved_count,
            .unresolved_count = unresolved_count,
        };
    }

    pub fn format(
        review: Review,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        for (review.comments) |comment| {
            if (!comment.resolved) {
                try writer.print("{s}: {s}\n", .{ comment.location, comment.snippet });
            }
        }
        if (review.unresolved_count > 0) {
            try writer.print("\n{d} resolved {d} unresolved\n", .{
                review.resolved_count,
                review.unresolved_count,
            });
        } else {
            try writer.print("{d} resolved\n", .{review.resolved_count});
        }
    }
};

const Snap = @import("./testing/snaptest.zig").Snap;
const snap = Snap.snap;

test Review {
    const diff =
        \\diff --git a/REVIEW.md b/REVIEW.md
        \\new file mode 100644
        \\index 000000000..6b5e74701
        \\--- /dev/null
        \\+++ b/REVIEW.md
        \\@@ -0,0 +1,3 @@
        \\+# Review Summary
        \\+
        \\+(Work in progress review so far.)
        \\diff --git a/src/vsr/message_header.zig b/src/vsr/message_header.zig
        \\index b2b176e5f..fd7d420c1 100644
        \\--- a/src/vsr/message_header.zig
        \\+++ b/src/vsr/message_header.zig
        \\@@ -379,0 +380,2 @@ pub const Header = extern struct {
        \\+        //? dj: Positioning the field here is nice for alignment, but makes the upgrade
        \\+        //? more complicated than if we just appended the field after release_count.
        \\+        //? matklad: I want to fix this in a follow up!
        \\+        //? resolved.
        \\@@ -427,0 +432,2 @@ pub fn op_next_hop(routing: *const Routing, op: u64) NextHop {
        \\+        //? dj: What do you think of referring to this as a replica_position,
        \\+        //? since replica_index is also what `routing.replica` is.
        \\
    ;
    try test_review_case(diff, snap(@src(),
        \\src/vsr/message_header.zig:432:  dj: What do you think of referring to this as a replica_position,
        \\
        \\1 resolved 1 unresolved
        \\
    ));
}

fn test_review_case(diff: []const u8, want: Snap) !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_instance.deinit();

    const arena = arena_instance.allocator();

    const review = try Review.parse(arena, diff);
    try want.diff_fmt("{}", .{review});
}
