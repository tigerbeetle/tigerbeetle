const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = @import("../shell.zig");

const Release = @import("../multiversion.zig").Release;
const ReleaseTriple = @import("../multiversion.zig").ReleaseTriple;

const MiB = stdx.MiB;

const log = std.log;

const changelog_bytes_max = 10 * MiB;

pub fn main(shell: *Shell, gpa: std.mem.Allocator) !void {
    _ = gpa;

    const now_utc = stdx.DateTimeUTC.now();
    const today = try shell.fmt(
        "{:0>4}-{:0>2}-{:0>2}",
        .{ now_utc.year, now_utc.month, now_utc.day },
    );

    try shell.exec("git fetch origin --quiet", .{});
    try shell.exec("git switch --create release-{today} origin/main", .{ .today = today });

    const merges = try shell.exec_stdout(
        \\git log --merges --first-parent origin/release..origin/main
    , .{});

    const changelog_current = try shell.project_root.readFileAlloc(
        shell.arena.allocator(),
        "./CHANGELOG.md",
        changelog_bytes_max,
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

    var it = ChangelogIterator.init(options.changelog_current);
    const last_changelog_entry = it.next_changelog().?;

    try buffer.print(
        \\# Changelog
        \\
        \\Subscribe to the [tracking issue #2231](https://github.com/tigerbeetle/tigerbeetle/issues/2231)
        \\to receive notifications about breaking changes!
        \\
        \\
    , .{});

    if (last_changelog_entry.release) |release| {
        const release_next = Release.from(.{
            .major = release.triple().major,
            .minor = release.triple().minor,
            .patch = release.triple().patch + 1,
        });
        try buffer.print("## TigerBeetle {}\n\n", .{release_next});
    } else {
        try buffer.print("## TigerBeetle (unreleased)\n\n", .{});
    }
    try buffer.print("Released: {s}\n\n", .{options.today});

    var merges_left = options.merges;
    for (0..128) |_| {
        const merge = try format_changelog_cut_single_merge(&merges_left) orelse break;

        try buffer.print(
            \\- [#{d}](https://github.com/tigerbeetle/tigerbeetle/pull/{d})
            \\
            \\  {s}
            \\
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

    try buffer.writeAll(it.all_entries);
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

    _, merges_left.* = stdx.cut(merges_left.*, "Merge pull request #") orelse return null;

    const pr_string, merges_left.* = stdx.cut(merges_left.*, " from ") orelse
        return error.ParseMergeLog;
    const pr = try std.fmt.parseInt(u16, pr_string, 10);

    _, merges_left.* = stdx.cut(merges_left.*, "\n    \n    ") orelse return error.ParseMergeLog;

    const summary, merges_left.* = stdx.cut(merges_left.*, "\n") orelse return error.ParseMergeLog;

    return .{ .pr = pr, .summary = summary };
}

pub const ChangelogIterator = struct {
    const Entry = struct {
        release: ?Release,
        text_full: []const u8,
        text_body: []const u8,
    };

    // Immutable suffix of the changelog, used to prepend a new entry in front.
    all_entries: []const u8,

    // Mutable suffix of what's yet to be iterated.
    rest: []const u8,

    release_previous_iteration: ?Release = null,

    pub fn init(changelog: []const u8) ChangelogIterator {
        var rest = stdx.cut_prefix(changelog, "# Changelog\n\n").?;
        const start_index = std.mem.indexOf(u8, rest, "##").?;
        assert(rest[start_index - 1] == '\n');
        rest = rest[start_index..];
        assert(std.mem.startsWith(u8, rest, "## TigerBeetle"));

        return .{
            .all_entries = rest,
            .rest = rest,
        };
    }

    pub fn next_changelog(it: *ChangelogIterator) ?Entry {
        if (it.done()) return null;
        assert(std.mem.startsWith(u8, it.rest, "## TigerBeetle"));
        const entry_end_index = std.mem.indexOf(u8, it.rest[2..], "\n\n## ").? + 2;
        const text_full = it.rest[0 .. entry_end_index + 1];
        it.rest = it.rest[entry_end_index + 2 ..];
        const entry = parse_entry(text_full);

        if (it.release_previous_iteration != null and entry.release != null) {
            // The changelog is ordered from newest to oldest, and that's how it's iterated. The
            // current iteration's release is thus expected to be less than the previous iteration's
            // release.
            assert(Release.less_than({}, entry.release.?, it.release_previous_iteration.?));
        }
        if (entry.release != null) {
            it.release_previous_iteration = entry.release;
        }

        return entry;
    }

    fn done(it: *const ChangelogIterator) bool {
        // First old-style release.
        return std.mem.startsWith(u8, it.rest, "## 2024-08-05");
    }

    fn parse_entry(text_full: []const u8) Entry {
        assert(std.mem.startsWith(u8, text_full, "## TigerBeetle"));
        assert(std.mem.endsWith(u8, text_full, "\n"));
        assert(!std.mem.endsWith(u8, text_full, "\n\n"));

        const first_line, var body = stdx.cut(text_full, "\n").?;
        const release = if (std.mem.eql(u8, first_line, "## TigerBeetle (unreleased)"))
            null
        else
            Release.parse(stdx.cut_prefix(first_line, "## TigerBeetle ").?) catch
                @panic("invalid changelog");

        body = stdx.cut_prefix(body, "\nReleased:").?;
        _, body = stdx.cut(body, "\n").?;
        return .{ .release = release, .text_full = text_full, .text_body = body };
    }
};

test ChangelogIterator {
    const changelog =
        \\# Changelog
        \\
        \\Some preamble here
        \\
        \\## TigerBeetle 1.2.3
        \\
        \\Released: 2024-10-23
        \\
        \\This is the start of the changelog.
        \\
        \\### Features
        \\
        \\- a cool PR
        \\
        \\## TigerBeetle 1.2.2
        \\
        \\Released: 2024-10-16
        \\
        \\ The beginning.
        \\
        \\## 2024-08-05 (prehistory)
        \\
        \\
    ;

    var it = ChangelogIterator.init(changelog);

    var entry = it.next_changelog().?;
    try std.testing.expectEqual(entry.release.?.triple(), ReleaseTriple{
        .major = 1,
        .minor = 2,
        .patch = 3,
    });
    try std.testing.expectEqualStrings(entry.text_full,
        \\## TigerBeetle 1.2.3
        \\
        \\Released: 2024-10-23
        \\
        \\This is the start of the changelog.
        \\
        \\### Features
        \\
        \\- a cool PR
        \\
    );
    try std.testing.expectEqualStrings(entry.text_body,
        \\
        \\This is the start of the changelog.
        \\
        \\### Features
        \\
        \\- a cool PR
        \\
    );

    entry = it.next_changelog().?;
    try std.testing.expectEqual(entry.release.?.triple(), ReleaseTriple{
        .major = 1,
        .minor = 2,
        .patch = 2,
    });

    try std.testing.expectEqual(it.next_changelog(), null);
}

test "current changelog" {
    const allocator = std.testing.allocator;

    const changelog_text = try std.fs.cwd().readFileAlloc(
        allocator,
        "./CHANGELOG.md",
        changelog_bytes_max,
    );
    defer allocator.free(changelog_text);

    var it = ChangelogIterator.init(changelog_text);
    while (it.next_changelog()) |_| {}
}
