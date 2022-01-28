const std = @import("std");
const fs = std.fs;
const math = std.math;
const mem = std.mem;

const whitelist = std.ComptimeStringMap([]const u32, .{
    .{ "src/cli.zig", &.{ 35, 39 } },
});

fn whitelisted(path: []const u8, line: u32) bool {
    const lines = whitelist.get(path) orelse return false;
    return mem.indexOfScalar(u32, lines, line) != null;
}

const Stats = struct {
    path: []const u8,
    assert_count: u32,
    function_count: u32,
    ratio: f64,
};

var file_stats = std.ArrayListUnmanaged(Stats){};
var seen = std.AutoArrayHashMapUnmanaged(fs.File.INode, void){};

var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
const gpa = general_purpose_allocator.allocator();

pub fn main() !void {
    const argv = std.os.argv;
    for (argv[1..]) |raw_path| {
        const path = mem.span(raw_path);
        lint_file(path, fs.cwd(), path) catch |err| switch (err) {
            error.IsDir, error.AccessDenied => try lint_dir(path, fs.cwd(), path),
            else => return err,
        };
    }

    var max_path_len: usize = "total:".len;
    var total_assert_count: usize = 0;
    var total_function_count: usize = 0;
    for (file_stats.items) |stats| {
        max_path_len = math.max(max_path_len, stats.path.len);
        total_assert_count += stats.assert_count;
        total_function_count += stats.function_count;
    }

    std.sort.sort(Stats, file_stats.items, {}, struct {
        fn less_than(_: void, a: Stats, b: Stats) bool {
            return a.ratio > b.ratio;
        }
    }.less_than);

    var buffered_writer = std.io.bufferedWriter(std.io.getStdOut().writer());
    const stdout = buffered_writer.writer();

    try stdout.writeAll("\npath");
    try stdout.writeByteNTimes(' ', max_path_len - "path".len);
    try stdout.writeAll(" asserts functions ratio\n");

    for (file_stats.items) |stats| {
        try stdout.writeAll(stats.path);
        try stdout.writeByteNTimes(' ', max_path_len - stats.path.len);
        try stdout.print(" {d: >7} {d: >9} {d: >5.2}\n", .{
            stats.assert_count,
            stats.function_count,
            stats.ratio,
        });
    }

    try stdout.writeByteNTimes(' ', max_path_len - "total:".len);
    try stdout.print("total: {d: >7} {d: >9} {d: >5.2}\n", .{
        total_assert_count,
        total_function_count,
        @intToFloat(f64, total_assert_count) / @intToFloat(f64, total_function_count),
    });
    try buffered_writer.flush();
}

const LintError = error{
    OutOfMemory,
    ParseError,
    NotUtf8,
} || fs.File.OpenError || fs.File.ReadError || fs.File.WriteError;

fn lint_dir(file_path: []const u8, parent_dir: fs.Dir, parent_sub_path: []const u8) LintError!void {
    var dir = try parent_dir.openDir(parent_sub_path, .{ .iterate = true });
    defer dir.close();

    const stat = try dir.stat();
    if (try seen.fetchPut(gpa, stat.inode, {})) |_| return;

    var dir_it = dir.iterate();
    while (try dir_it.next()) |entry| {
        const is_dir = entry.kind == .Directory;

        if (is_dir and std.mem.eql(u8, entry.name, "zig-cache")) continue;

        if (is_dir or mem.endsWith(u8, entry.name, ".zig")) {
            const full_path = try fs.path.join(gpa, &[_][]const u8{ file_path, entry.name });
            defer gpa.free(full_path);

            if (is_dir) {
                try lint_dir(full_path, dir, entry.name);
            } else {
                try lint_file(full_path, dir, entry.name);
            }
        }
    }
}

fn lint_file(file_path: []const u8, dir: fs.Dir, sub_path: []const u8) LintError!void {
    const source_file = try dir.openFile(sub_path, .{});
    defer source_file.close();

    const stat = try source_file.stat();

    if (stat.kind == .Directory) return error.IsDir;

    // Add to set after no longer possible to get error.IsDir.
    if (try seen.fetchPut(gpa, stat.inode, {})) |_| return;

    const source = try source_file.readToEndAllocOptions(
        gpa,
        math.maxInt(usize),
        null,
        @alignOf(u8),
        0,
    );
    try check_line_length(source, file_path);

    var tree = try std.zig.parse(gpa, source);
    defer tree.deinit(gpa);

    if (tree.errors.len != 0) return error.ParseError;

    const node_tags = tree.nodes.items(.tag);
    const main_tokens = tree.nodes.items(.main_token);
    const node_datas = tree.nodes.items(.data);

    var function_count: u32 = 0;
    var assert_count: u32 = 0;
    for (node_tags) |tag, node| {
        switch (tag) {
            .fn_decl => {
                function_count += 1;
                const body = node_datas[node].rhs;
                const body_start = tree.tokenLocation(0, tree.firstToken(body));
                const body_end = tree.tokenLocation(0, tree.lastToken(body));
                // Add 1 as the count returned by tokenLocation() is
                // 0-indexed while most editors start at 1.
                const line = @intCast(u32, body_start.line + 1);
                const body_lines = body_end.line - body_start.line;
                if (body_lines > 70 and !whitelisted(file_path, line)) {
                    const stderr = std.io.getStdErr().writer();
                    try stderr.print("{s}:{d} function body exceeds 70 lines ({d} lines)\n", .{
                        file_path,
                        line,
                        body_lines,
                    });
                }
            },

            .call_one, .call_one_comma => {
                const lhs = node_datas[node].lhs;
                if (node_tags[lhs] == .identifier and
                    mem.eql(u8, "assert", tree.tokenSlice(main_tokens[lhs])))
                {
                    assert_count += 1;
                }
            },

            .unreachable_literal => assert_count += 1,

            else => {},
        }
    }

    try file_stats.append(gpa, .{
        .path = try gpa.dupe(u8, file_path),
        .assert_count = assert_count,
        .function_count = function_count,
        .ratio = @intToFloat(f64, assert_count) / @intToFloat(f64, function_count),
    });
}

fn check_line_length(source: []const u8, path: []const u8) !void {
    var i: usize = 0;
    var line: u32 = 1;
    while (mem.indexOfScalar(u8, source[i..], '\n')) |newline| : (line += 1) {
        const line_length = std.unicode.utf8CountCodepoints(
            source[i..][0..newline],
        ) catch return error.NotUtf8;
        if (line_length > 100 and !whitelisted(path, line)) {
            const stderr = std.io.getStdErr().writer();
            try stderr.print("{s}:{d} line exceeds 100 columns\n", .{ path, line });
        }
        i += newline + 1;
    }
}
