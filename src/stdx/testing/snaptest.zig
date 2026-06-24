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

const MiB = stdx.MiB;

pub const Snap = struct {
    comptime {
        assert(builtin.is_test);
    }

    // Set to `true` to update all snapshots.
    pub var update_all: bool = false;

    module_path: []const u8,
    location: SourceLocation,
    text: []const u8,
    update_this: bool = false,

    const SnapFn = fn (location: SourceLocation, text: []const u8) Snap;

    /// Takes the path to the root source file of the current module and creates a snap function.
    ///
    /// ```
    /// const snap = Snap.snap_fn("src");
    /// const snap = Snap.snap_fn("src/stdx");
    /// ```
    ///
    /// For the update logic to work, usage *must* be formatted as:
    ///
    /// ```
    /// snap(@src(),
    ///     \\Text of the snapshot.
    /// )
    /// ```
    pub fn snap_fn(comptime module_path: []const u8) SnapFn {
        return struct {
            fn snap(location: SourceLocation, text: []const u8) Snap {
                return init(module_path, location, text);
            }
        }.snap;
    }

    /// Creates a new Snap.
    fn init(module_path: []const u8, location: SourceLocation, text: []const u8) Snap {
        return Snap{ .module_path = module_path, .location = location, .text = text };
    }

    /// Builder-lite method to update just this particular snapshot.
    pub fn update(snapshot: *const Snap) Snap {
        return Snap{
            .module_path = snapshot.module_path,
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

    // Compare the snapshot with the zon json serialization of a `value`.
    pub fn diff_zon(
        snapshot: *const Snap,
        value: anytype,
    ) !void {
        var got: std.ArrayListUnmanaged(u8) = .empty;
        defer got.deinit(std.testing.allocator);

        try std.zon.stringify.serialize(value, .{}, got.writer(std.testing.allocator));
        try snapshot.diff(got.items);
    }

    pub fn diff_hex(snapshot: *const Snap, value: []const u8) !void {
        var buffer: std.ArrayListUnmanaged(u8) = .empty;
        defer buffer.deinit(std.testing.allocator);

        try hexdump(value, buffer.writer(std.testing.allocator).any());
        try snapshot.diff(buffer.items);
    }

    // Compare the snapshot with a given string.
    pub fn diff(snapshot: *const Snap, got: []const u8) !void {
        if (equal_excluding_ignored(got, snapshot.text)) return;

        std.debug.print("Snapshot differs.\n", .{});
        try print_diff(snapshot.text, got);

        if (!snapshot.should_update()) {
            std.debug.print(
                "Rerun with SNAP_UPDATE=1 environmental variable to update the snapshot.\n",
                .{},
            );
            return error.SnapDiff;
        }

        var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena_instance.deinit();

        const arena = arena_instance.allocator();
        const file_path_relative = try std.fs.path.join(
            arena,
            // The file location is relative to the module root path.
            &.{ snapshot.module_path, snapshot.location.file },
        );

        const file_text = try std.fs.cwd().readFileAlloc(arena, file_path_relative, 1 * MiB);
        var file_text_updated = try std.ArrayList(u8).initCapacity(arena, file_text.len);

        const line_zero_based = snapshot.location.line - 1;
        const range = snap_range(file_text, line_zero_based);

        const snapshot_prefix = file_text[0..range.start];
        const snapshot_text = file_text[range.start..range.end];
        const snapshot_suffix = file_text[range.end..];

        const indent = get_indent(snapshot_text);

        try file_text_updated.appendSlice(snapshot_prefix);
        {
            var lines = std.mem.splitScalar(u8, got, '\n');
            while (lines.next()) |line| {
                try file_text_updated.writer().print("{s}\\\\{s}\n", .{ indent, line });
            }
        }
        try file_text_updated.appendSlice(snapshot_suffix);

        try std.fs.cwd().writeFile(.{
            .sub_path = file_path_relative,
            .data = file_text_updated.items,
        });

        std.debug.print("Updated {s}\n", .{file_path_relative});
        return error.SnapUpdated;
    }
};

fn print_diff(want: []const u8, got: []const u8) !void {
    const allocator = std.testing.allocator;
    const want_lines = try split_lines(allocator, want);
    defer allocator.free(want_lines);
    const got_lines = try split_lines(allocator, got);
    defer allocator.free(got_lines);

    const prefix = lines_common_prefix(want_lines, got_lines);
    const suffix = lines_common_suffix(want_lines, got_lines, prefix);

    const context = 3;
    const hunk_start = prefix - @min(prefix, context);
    const want_change_end = want_lines.len - suffix;
    const got_change_end = got_lines.len - suffix;
    const want_hunk_end = @min(want_lines.len, want_change_end + context);
    const got_hunk_end = @min(got_lines.len, got_change_end + context);

    std.debug.print(
        \\Diff:
        \\----
        \\--- want
        \\+++ got
        \\@@ -{},{} +{},{} @@
        \\
    ,
        .{
            hunk_start + 1,
            want_hunk_end - hunk_start,
            hunk_start + 1,
            got_hunk_end - hunk_start,
        },
    );

    for (want_lines[hunk_start..prefix]) |line| {
        std.debug.print(" {s}\n", .{line});
    }

    if (want_change_end - prefix == got_change_end - prefix) {
        for (
            want_lines[prefix..want_change_end],
            got_lines[prefix..got_change_end],
        ) |want_line, got_line| {
            if (lines_match(want_line, got_line)) {
                std.debug.print(" {s}\n", .{want_line});
            } else {
                std.debug.print("-{s}\n", .{want_line});
                std.debug.print("+{s}\n", .{got_line});
            }
        }
    } else {
        for (want_lines[prefix..want_change_end]) |line| {
            std.debug.print("-{s}\n", .{line});
        }
        for (got_lines[prefix..got_change_end]) |line| {
            std.debug.print("+{s}\n", .{line});
        }
    }

    for (
        want_lines[want_change_end..want_hunk_end],
        got_lines[got_change_end..got_hunk_end],
    ) |want_line, got_line| {
        assert(lines_match(want_line, got_line));
        std.debug.print(" {s}\n", .{want_line});
    }

    std.debug.print("----\n", .{});
}

fn split_lines(allocator: std.mem.Allocator, text: []const u8) ![][]const u8 {
    var lines: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer lines.deinit(allocator);

    var rest = text;
    while (stdx.cut(rest, "\n")) |cut| {
        const line, const rest_next = cut;
        try lines.append(allocator, line);
        rest = rest_next;
    }
    if (rest.len > 0) {
        try lines.append(allocator, rest);
    }

    return try lines.toOwnedSlice(allocator);
}

fn lines_common_prefix(a: []const []const u8, b: []const []const u8) usize {
    for (0..@min(a.len, b.len)) |index| {
        if (!lines_match(a[index], b[index])) return index;
    }
    return @min(a.len, b.len);
}

fn lines_common_suffix(
    a: []const []const u8,
    b: []const []const u8,
    prefix: usize,
) usize {
    assert(prefix <= @min(a.len, b.len));

    const suffix_max = @min(a.len, b.len) - prefix;
    for (0..suffix_max) |suffix| {
        const a_line = a[a.len - suffix - 1];
        const b_line = b[b.len - suffix - 1];
        if (!lines_match(a_line, b_line)) return suffix;
    }
    return suffix_max;
}

fn lines_match(want: []const u8, got: []const u8) bool {
    return equal_excluding_ignored(got, want);
}

test "diff lines handle ignored markers" {
    const want = [_][]const u8{
        "{",
        "  \"id\": \"5\",",
        "  \"timestamp\": \"<snap:ignore>\",",
        "}",
    };
    const got = [_][]const u8{
        "{",
        "  \"id\": \"1\",",
        "  \"timestamp\": \"1782292370439176883\",",
        "}",
    };

    const prefix = lines_common_prefix(&want, &got);
    try std.testing.expectEqual(@as(usize, 1), prefix);
    try std.testing.expectEqual(@as(usize, 2), lines_common_suffix(&want, &got, prefix));
}

fn equal_excluding_ignored(got: []const u8, snapshot: []const u8) bool {
    // Don't allow ignoring suffixes and prefixes, as that makes it easy to miss trailing or leading
    // data.
    assert(!std.mem.startsWith(u8, snapshot, "<snap:ignore>"));
    assert(!std.mem.endsWith(u8, snapshot, "<snap:ignore>"));

    var got_rest = got;
    var snapshot_rest = snapshot;
    for (0..10) |_| {
        // Cut the part before the first ignore, it should be equal between two strings...
        const common_prefix, snapshot_rest = stdx.cut(snapshot_rest, "<snap:ignore>") orelse break;
        got_rest = stdx.cut_prefix(got_rest, common_prefix) orelse return false;

        // ...then find the next part that should match, and cut up to that.
        const common_middle, _ =
            stdx.cut(snapshot_rest, "<snap:ignore>") orelse .{ snapshot_rest, "" };
        assert(common_middle.len > 0);
        snapshot_rest = stdx.cut_prefix(snapshot_rest, common_middle).?;

        const ignored, got_rest = stdx.cut(got_rest, common_middle) orelse return false;
        // If <snap:ignore> matched an empty string, or several lines, report it as an error.
        if (ignored.len == 0) return false;
        if (std.mem.indexOfScalar(u8, ignored, '\n') != null) return false;
    } else @panic("more than 10 ignores");

    return std.mem.eql(u8, got_rest, snapshot_rest);
}

test equal_excluding_ignored {
    try equal_excluding_ignored_case("ABA", "ABA", true);
    try equal_excluding_ignored_case("ABBA", "A<snap:ignore>A", true);
    try equal_excluding_ignored_case("ABBACABA", "AB<snap:ignore>CA<snap:ignore>A", true);

    try equal_excluding_ignored_case("ABA", "ACA", false);
    try equal_excluding_ignored_case("ABBA", "A<snap:ignore>C", false);
    try equal_excluding_ignored_case("ABBACABA", "AB<snap:ignore>DA<snap:ignore>BA", false);
    try equal_excluding_ignored_case("ABBACABA", "AB<snap:ignore>BA<snap:ignore>DA", false);
    try equal_excluding_ignored_case("ABA", "AB<snap:ignore>A", false);
    try equal_excluding_ignored_case("A\nB\nA", "A<snap:ignore>A", false);
}

fn equal_excluding_ignored_case(got: []const u8, snapshot: []const u8, ok: bool) !void {
    try std.testing.expectEqual(equal_excluding_ignored(got, snapshot), ok);
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

    var lines = std.mem.splitScalar(u8, text, '\n');
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

    lines = std.mem.splitScalar(u8, text[snap_start..], '\n');
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

fn hexdump(bytes: []const u8, writer: std.io.AnyWriter) !void {
    for (bytes, 0..) |byte, index| {
        if (index > 0) {
            const space = if (index % 16 == 0) "\n" else if (index % 8 == 0) "  " else " ";
            try writer.writeAll(space);
        }
        try writer.print("{x:02}", .{byte});
    }
}

test hexdump {
    const snap = Snap.snap_fn("./src/stdx");

    try snap(@src(),
        \\68 65 6c 6c 6f 2c 20 77  6f 72 6c 64 0a 00 01 02
        \\03 fd fe ff
    ).diff_hex("hello, world\n" ++ .{ 0, 1, 2, 3, 253, 254, 255 });
}

test "Snap update disabled" {
    assert(!Snap.update_all); // Forgot to flip this back to false after updating snapshots?
}
