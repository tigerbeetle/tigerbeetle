const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");

const Release = @import("../multiversion.zig").Release;
const ReleaseTriple = @import("../multiversion.zig").ReleaseTriple;

const MiB = stdx.MiB;

const changelog_bytes_max = 10 * MiB;

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

    try std.testing.expect(it.next_changelog() == null);
}

test "current changelog" {
    const allocator = std.testing.allocator;

    const changelog_text = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "./CHANGELOG.md",
        allocator,
        .limited(changelog_bytes_max),
    );
    defer allocator.free(changelog_text);

    var it = ChangelogIterator.init(changelog_text);
    while (it.next_changelog()) |_| {}
}
