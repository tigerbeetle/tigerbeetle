//! A tiny pattern/library for testing with expectations ([1], [2]).
//!
//! On a high level, this is a replacement for `std.testing.expectEqual` which:
//!
//!   - is less cumbersome to use for complex types,
//!   - gives somewhat more useful feedback on a test failure without much investment,
//!   - drastically reduces the time to update the tests after refactors,
//!   - encourages creation of reusable visualizations for data structures.
//!
//! Implementation-wise, `snaptest` provides a `Snap` type, which can be thought of as a Zig string
//! literal which also remembers its location in the source file, can be diffed with other strings,
//! and, crucially, can _update its own source code_ to match the expected value.
//!
//! Example usage:
//!
//! ```
//! const Snap = @import("snaptest.zig").Snap;
//! const snap = Snap.snap;
//!
//! fn check_addition(x: u32, y: u32, want: Snap) !void {
//!     const got = x + y;
//!     try want.diff_fmt("{}", .{got});
//! }
//!
//! test "addition" {
//!     try check_addition(2, 2, snap(@src(),
//!         \\8
//!     ));
//! }
//! ```
//!
//! Running this test fails, printing the diff between actual result (`4`) and what's specified in
//! the source code.
//!
//! Re-running the test with `SNAP_UPDATE=1` environmental variable auto-magically updates the
//! source code to say `\\4`. Alternatively, you can use `snap(...).update()` to auto-update just a
//! single test.
//!
//! Note the `@src()` argument passed to the `snap(...)` invocation --- that's how it knows which
//! lines to update.
//!
//! Snapshots can use `<snap:ignore>` marker to ignore part of input:
//!
//! ```
//! test "time" {
//!     var buf: [32]u8 = undefined;
//!     const time = try std.fmt.bufPrint(&buf, "it's {}ms", .{
//!         std.time.milliTimestamp(),
//!     });
//!     try Snap.snap(@src(),
//!         \\it's <snap:ignore>ms
//!     ).diff(time);
//! }
//! ```
//!
//! TODO:
//!   - This doesn't actually `diff` things yet :o) But running with `SNAP_UPDATE=1` and then using
//!     `git diff` is a workable substitute.
//!   - Only one test can be updated at a time. To update several, we need to return
//!     `error.SkipZigTest` on mismatch and adjust offsets appropriately.
//!
//! [1]: https://blog.janestreet.com/using-ascii-waveforms-to-test-hardware-designs/
//! [2]: https://ianthehenry.com/posts/my-kind-of-repl/
const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const SourceLocation = std.builtin.SourceLocation;

const stdx = @import("../stdx.zig");

comptime {
    assert(builtin.is_test);
}

// Set to `true` to update all snapshots.
const update_all: bool = false;

pub const Snap = struct {
    location: SourceLocation,
    text: []const u8,
    update_this: bool = false,

    /// Creates a new Snap.
    ///
    /// For the update logic to work, *must* be formatted as:
    ///
    /// ```
    /// snap(@src(),
    ///     \\Text of the snapshot.
    /// )
    /// ```
    pub fn snap(location: SourceLocation, text: []const u8) Snap {
        return Snap{ .location = location, .text = text };
    }

    /// Builder-lite method to update just this particular snapshot.
    pub fn update(snapshot: *const Snap) Snap {
        return Snap{
            .location = snapshot.location,
            .text = snapshot.text,
            .update_this = true,
        };
    }

    /// To update a snapshot, use whichever you prefer:
    ///   - `.update()` method on a particular snap,
    ///   - `update_all` const in this file,
    ///   - `SNAP_UPDATE` env var.
    fn should_update(snapshot: *const Snap) bool {
        return snapshot.update_this or update_all or
            std.process.hasEnvVarConstant("SNAP_UPDATE");
    }

    // Compare the snapshot with a formatted string.
    pub fn diff_fmt(snapshot: *const Snap, comptime fmt: []const u8, fmt_args: anytype) !void {
        const got = try std.fmt.allocPrint(std.testing.allocator, fmt, fmt_args);
        defer std.testing.allocator.free(got);

        try snapshot.diff(got);
    }

    // Compare the snapshot with the json serialization of a `value`.
    pub fn diff_json(snapshot: *const Snap, value: anytype) !void {
        var got = std.ArrayList(u8).init(std.testing.allocator);
        defer got.deinit();

        try std.json.stringify(value, .{}, got.writer());
        try snapshot.diff(got.items);
    }

    // Compare the snapshot with a given string.
    pub fn diff(snapshot: *const Snap, got: []const u8) !void {
        if (equal_excluding_ignored(got, snapshot.text)) return;

        std.debug.print(
            \\Snapshot differs.
            \\Want:
            \\----
            \\{s}
            \\----
            \\Got:
            \\----
            \\{s}
            \\----
            \\
        ,
            .{
                snapshot.text,
                got,
            },
        );

        if (!snapshot.should_update()) {
            std.debug.print(
                "Rerun with SNAP_UPDATE=1 environmental variable to update the snapshot.\n",
                .{},
            );
            return error.SnapDiff;
        }

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const allocator = arena.allocator();

        const file_text =
            try std.fs.cwd().readFileAlloc(allocator, snapshot.location.file, 1024 * 1024);
        var file_text_updated = try std.ArrayList(u8).initCapacity(allocator, file_text.len);

        const line_zero_based = snapshot.location.line - 1;
        const range = snap_range(file_text, line_zero_based);

        const snapshot_prefix = file_text[0..range.start];
        const snapshot_text = file_text[range.start..range.end];
        const snapshot_suffix = file_text[range.end..];

        const indent = get_indent(snapshot_text);

        try file_text_updated.appendSlice(snapshot_prefix);
        {
            var lines = std.mem.split(u8, got, "\n");
            while (lines.next()) |line| {
                try file_text_updated.writer().print("{s}\\\\{s}\n", .{ indent, line });
            }
        }
        try file_text_updated.appendSlice(snapshot_suffix);

        try std.fs.cwd().writeFile(snapshot.location.file, file_text_updated.items);

        std.debug.print("Updated {s}\n", .{snapshot.location.file});
        return error.SnapUpdated;
    }
};

fn equal_excluding_ignored(got: []const u8, snapshot: []const u8) bool {
    var got_rest = got;
    var snapshot_rest = snapshot;

    // Don't allow ignoring suffixes and prefixes, as that makes it easy to miss trailing or leading
    // data.
    assert(!std.mem.startsWith(u8, snapshot, "<snap:ignore>"));
    assert(!std.mem.endsWith(u8, snapshot, "<snap:ignore>"));

    for (0..10) |_| {
        // Cut the part before the first ignore, it should be equal between two strings...
        const snapshot_cut = stdx.cut(snapshot_rest, "<snap:ignore>") orelse break;
        const got_cut = stdx.cut(got_rest, snapshot_cut.prefix) orelse return false;
        if (got_cut.prefix.len != 0) return false;
        got_rest = got_cut.suffix;
        snapshot_rest = snapshot_cut.suffix;

        // ...then find the next part that should match, and cut up to that.
        const next_match = if (stdx.cut(snapshot_rest, "<snap:ignore>")) |snapshot_cut_next|
            snapshot_cut_next.prefix
        else
            snapshot_rest;
        assert(next_match.len > 0);
        snapshot_rest = stdx.cut(snapshot_rest, next_match).?.suffix;

        const got_cut_next = stdx.cut(got_rest, next_match) orelse return false;
        const ignored = got_cut_next.prefix;
        // If <snap:ignore> matched an empty string, or several lines, report it as an error.
        if (ignored.len == 0) return false;
        if (std.mem.indexOf(u8, ignored, "\n") != null) return false;
        got_rest = got_cut_next.suffix;
    } else @panic("more than 10 ignores");

    return std.mem.eql(u8, got_rest, snapshot_rest);
}

test equal_excluding_ignored {
    const TestCase = struct { got: []const u8, snapshot: []const u8 };

    const cases_ok: []const TestCase = &.{
        .{ .got = "ABA", .snapshot = "ABA" },
        .{ .got = "ABBA", .snapshot = "A<snap:ignore>A" },
        .{ .got = "ABBACABA", .snapshot = "AB<snap:ignore>CA<snap:ignore>A" },
    };
    for (cases_ok) |case| {
        try std.testing.expect(equal_excluding_ignored(case.got, case.snapshot));
    }

    const cases_err: []const TestCase = &.{
        .{ .got = "ABA", .snapshot = "ACA" },
        .{ .got = "ABBA", .snapshot = "A<snap:ignore>C" },
        .{ .got = "ABBACABA", .snapshot = "AB<snap:ignore>DA<snap:ignore>BA" },
        .{ .got = "ABBACABA", .snapshot = "AB<snap:ignore>BA<snap:ignore>DA" },
        .{ .got = "ABA", .snapshot = "AB<snap:ignore>A" },
        .{ .got = "A\nB\nA", .snapshot = "A<snap:ignore>A" },
    };
    for (cases_err) |case| {
        try std.testing.expect(!equal_excluding_ignored(case.got, case.snapshot));
    }
}

const Range = struct { start: usize, end: usize };

/// Extracts the range of the snapshot. Assumes that the snapshot is formatted as
///
/// ```
/// snap(@src(),
///     \\first line
///     \\second line
/// )
/// ```
///
/// We could make this more robust by using `std.zig.Ast`, but sticking to manual string processing
/// is simpler, and enforced consistent style of snapshots is a good thing.
///
/// While we expect to find a snapshot after a given line, this is not guaranteed (the file could
/// have been modified between compilation and running the test), but should be rare enough to
/// just fail with an assertion.
fn snap_range(text: []const u8, src_line: u32) Range {
    var offset: usize = 0;
    var line_number: u32 = 0;

    var lines = std.mem.split(u8, text, "\n");
    const snap_start = while (lines.next()) |line| : (line_number += 1) {
        if (line_number == src_line) {
            assert(std.mem.indexOf(u8, line, "@src()") != null);
        }
        if (line_number == src_line + 1) {
            assert(is_multiline_string(line));
            break offset;
        }
        offset += line.len + 1; // 1 for \n
    } else unreachable;

    lines = std.mem.split(u8, text[snap_start..], "\n");
    const snap_end = while (lines.next()) |line| {
        if (!is_multiline_string(line)) {
            break offset;
        }
        offset += line.len + 1; // 1 for \n
    } else unreachable;

    return Range{ .start = snap_start, .end = snap_end };
}

fn is_multiline_string(line: []const u8) bool {
    for (line, 0..) |c, i| {
        switch (c) {
            ' ' => {},
            '\\' => return (i + 1 < line.len and line[i + 1] == '\\'),
            else => return false,
        }
    }
    return false;
}

fn get_indent(line: []const u8) []const u8 {
    for (line, 0..) |c, i| {
        if (c != ' ') return line[0..i];
    }
    return line;
}
