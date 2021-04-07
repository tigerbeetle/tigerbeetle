const std = @import("std");
const fs = std.fs;
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

var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
const gpa = &general_purpose_allocator.allocator;

pub fn main() !void {
    const argv = std.os.argv;
    for (argv[1..]) |raw_path| {
        const path = mem.span(raw_path);
        if (fs.walkPath(gpa, path)) |*walker| {
            defer walker.deinit();
            while (try walker.next()) |entry| {
                if (entry.kind == .File and
                    mem.eql(u8, ".zig", entry.basename[entry.basename.len - 4 ..]))
                {
                    const file = try entry.dir.openFile(entry.basename, .{});
                    defer file.close();
                    const source = try file.readToEndAlloc(gpa, std.math.maxInt(usize));
                    defer gpa.free(source);
                    try lint(source, entry.path);
                }
            }
        } else |err| switch (err) {
            error.NotDir => {
                const file = try fs.cwd().openFile(path, .{});
                defer file.close();
                const source = try file.readToEndAlloc(gpa, std.math.maxInt(usize));
                defer gpa.free(source);
                try lint(source, path);
            },
            else => return err,
        }
    }

    var max_path_len: usize = 0;
    var total_assert_count: usize = 0;
    var total_function_count: usize = 0;
    for (file_stats.items) |stats| {
        max_path_len = std.math.max(max_path_len, stats.path.len);
        total_assert_count += stats.assert_count;
        total_function_count += stats.function_count;
    }

    std.sort.sort(Stats, file_stats.items, {}, struct {
        fn less_than(_: void, a: Stats, b: Stats) bool {
            return a.ratio < b.ratio;
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

fn lint(source: []const u8, path: []const u8) !void {
    try check_line_length(source, path);

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
                if (body_lines > 70 and !whitelisted(path, line)) {
                    const stderr = std.io.getStdErr().writer();
                    try stderr.print("{s}:{d} function body exceeds 70 lines ({d} lines)\n", .{
                        path,
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

            else => {},
        }
    }

    try file_stats.append(gpa, .{
        .path = try gpa.dupe(u8, path),
        .assert_count = assert_count,
        .function_count = function_count,
        .ratio = @intToFloat(f64, assert_count) / @intToFloat(f64, function_count),
    });
}

fn check_line_length(source: []const u8, path: []const u8) !void {
    var i: usize = 0;
    var line: u32 = 1;
    while (mem.indexOfScalar(u8, source[i..], '\n')) |newline| : (line += 1) {
        const line_length = try std.unicode.utf8CountCodepoints(source[i..][0..newline]);
        if (line_length > 100 and !whitelisted(path, line)) {
            const stderr = std.io.getStdErr().writer();
            try stderr.print("{s}:{d} line exceeds 100 columns\n", .{ path, line });
        }
        i += newline + 1;
    }
}
