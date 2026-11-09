const std = @import("std");
const stdx = @import("stdx");

const KiB = stdx.KiB;
const MiB = stdx.MiB;

// Writes an explicit list of git-tracked files to files.txt, to make it harder
// to accidentally commit something untoward, and to add an extra friction point
// for "do we really need this" in review?
test "files" {
    const io_std = std.testing.io;
    const gpa = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const files_txt = "src/testing/files.txt";

    var environment = try std.testing.environ.createMap(gpa);
    defer environment.deinit();

    const buffer = try gpa.alloc(u8, MiB);
    defer gpa.free(buffer);

    var process = try stdx.Process.init(gpa, io_std, &environment, .{
        .working_directory = cwd,
        .spawn_size_exta = 4 * KiB,
    });
    defer process.deinit(gpa);

    const files_got = try process.run_stdout("git ls-files", .{}, buffer);
    const files_want = try cwd.readFileAlloc(
        io_std,
        files_txt,
        gpa,
        .limited(1 * MiB),
    );
    defer gpa.free(files_want);

    if (std.mem.eql(u8, files_got, files_want)) {
        return; // File list up-to-date.
    } else {
        try cwd.writeFile(io_std, .{
            .sub_path = files_txt,
            .data = files_got,
        });
        if (find_different_line(files_got, files_want)) |line| {
            const side = switch (line.side) {
                .left => "is missing",
                .right => "has extra",
            };
            std.log.err("files.txt {s} '{s}'", .{ side, line.text });
        }
        std.log.err("files.txt updated, commit it", .{});
        return error.TestFiles;
    }
}

const DifferentLine = struct { text: []const u8, side: enum { left, right } };

fn find_different_line(a: []const u8, b: []const u8) ?DifferentLine {
    var a_lines = std.mem.tokenizeScalar(u8, a, '\n');
    while (a_lines.next()) |a_line| {
        if (std.mem.indexOf(u8, b, a_line) == null) {
            return .{ .text = a_line, .side = .left };
        }
    }

    var b_lines = std.mem.tokenizeScalar(u8, b, '\n');
    while (b_lines.next()) |b_line| {
        if (std.mem.indexOf(u8, a, b_line) == null) {
            return .{ .text = b_line, .side = .right };
        }
    }

    return null;
}
