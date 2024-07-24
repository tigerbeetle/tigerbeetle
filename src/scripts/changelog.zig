const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");

const log = std.log;

pub fn main(shell: *Shell, gpa: std.mem.Allocator) !void {
    _ = gpa;

    const today = try date_iso(shell);

    try shell.exec("git fetch origin --quiet", .{});
    try shell.exec("git switch --create release-{today} origin/main", .{ .today = today });

    const merges = try shell.exec_stdout(
        \\git log --merges --first-parent origin/release..origin/main
    , .{});
    try shell.project_root.makePath("./.zig-cache");
    try shell.project_root.writeFile(.{ .sub_path = "./.zig-cache/merges.txt", .data = merges });
    log.info("merged PRs: ./.zig-cache/merges.txt", .{});

    const max_bytes = 10 * 1024 * 1024;
    const changelog_current = try shell.project_root.readFileAlloc(
        shell.arena.allocator(),
        "./CHANGELOG.md",
        max_bytes,
    );

    var changelog_new = std.ArrayList(u8).init(shell.arena.allocator());
    try format_changelog(changelog_new.writer(), .{
        .changelog_current = changelog_current,
        .merges = merges,
        .today = today,
    });

    try shell.project_root.writeFile(.{ .sub_path = "CHANGELOG.md", .data = changelog_new.items });

    log.info("don't forget to update ./CHANGELOG.md", .{});
}

fn format_changelog(buffer: std.ArrayList(u8).Writer, options: struct {
    changelog_current: []const u8,
    merges: []const u8,
    today: []const u8,
}) !void {
    if (std.mem.indexOf(u8, options.changelog_current, options.today) != null) {
        return error.ChangelogAlreadyUpdated;
    }

    try buffer.print(
        \\# TigerBeetle Changelog
        \\
        \\## {s}
        \\
        \\
    , .{options.today});

    var merges_left = options.merges;
    // TODO Shrink this down after we release again.
    for (0..512) |_| {
        const merge = try format_changelog_cut_single_merge(&merges_left) orelse break;

        try buffer.print(
            \\- [#{d}](https://github.com/tigerbeetle/tigerbeetle/pull/{d})
            \\      {s}
            \\
        , .{ merge.pr, merge.pr, merge.summary });
    } else @panic("suspiciously many PRs merged");
    assert(std.mem.indexOf(u8, merges_left, "commit") == null);

    try buffer.print(
        \\
        \\### Safety And Performance
        \\
        \\-
        \\
        \\### Features
        \\
        \\-
        \\
        \\### Internals
        \\
        \\-
        \\
        \\### TigerTracks 🎧
        \\
        \\- []()
        \\
        \\
    , .{});

    const changelog_rest = stdx.cut(
        options.changelog_current,
        "# TigerBeetle Changelog\n\n",
    ) orelse return error.ParseChangelog;

    try buffer.writeAll(changelog_rest.suffix);
}

fn format_changelog_cut_single_merge(merges_left: *[]const u8) !?struct {
    pr: u16,
    summary: []const u8,
} {
    errdefer {
        log.err("failed to parse:\n{s}", .{merges_left.*});
    }

    // This is what we are parsing here:
    //
    //    commit 02650cd67da855609cc41196e0d6f639b870ccf5
    //    Merge: b7c2fcda 4bb433ce
    //    Author: protty <45520026+kprotty@users.noreply.github.com>
    //    Date:   Fri Feb 9 18:37:04 2024 +0000
    //
    //    Merge pull request #1523 from tigerbeetle/king/client-uid
    //
    //    Client: add ULID helper functions

    var cut = stdx.cut(merges_left.*, "Merge pull request #") orelse return null;
    merges_left.* = cut.suffix;

    cut = stdx.cut(merges_left.*, " from ") orelse return error.ParseMergeLog;
    const pr = try std.fmt.parseInt(u16, cut.prefix, 10);
    merges_left.* = cut.suffix;

    cut = stdx.cut(merges_left.*, "\n    \n    ") orelse return error.ParseMergeLog;
    merges_left.* = cut.suffix;

    cut = stdx.cut(merges_left.*, "\n") orelse return error.ParseMergeLog;
    const summary = cut.prefix;
    merges_left.* = cut.suffix;

    return .{ .pr = pr, .summary = summary };
}

fn date_iso(shell: *Shell) ![]const u8 {
    return try shell.exec_stdout("date --iso", .{});
}
