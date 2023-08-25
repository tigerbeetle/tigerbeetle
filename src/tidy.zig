//! Checks for various non-functional properties of the code itself.

const std = @import("std");
const stdx = @import("./stdx.zig");
const fs = std.fs;
const mem = std.mem;
const math = std.math;

test "tidy" {
    const allocator = std.testing.allocator;

    const buffer_size = 1024 * 1024;
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    var src_dir = try fs.cwd().openIterableDir("./src", .{});
    defer src_dir.close();

    var walker = try src_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind == .file and mem.endsWith(u8, entry.path, ".zig")) {
            const source_file = try entry.dir.openFile(entry.basename, .{});
            defer source_file.close();

            const bytes_read = try source_file.readAll(buffer);
            if (bytes_read == buffer.len) return error.FileTooLong;
            const source_file_text = buffer[0..bytes_read];

            if (banned(source_file_text)) |ban| {
                std.debug.print(
                    "{s}: banned: {s}\n",
                    .{ entry.path, ban },
                );
                return error.Banned;
            }

            const long_line = try find_long_line(source_file_text);
            if (long_line) |line_number| {
                if (!is_naughty(entry.path)) {
                    std.debug.print(
                        "{s}:{d} error: line exceeds 100 columns\n",
                        .{ entry.path, line_number + 1 },
                    );
                    return error.LineTooLong;
                }
            } else {
                if (is_naughty(entry.path)) {
                    std.debug.print(
                        "{s}: error: no longer contains long lines, " ++
                            "remove from the `naughty_list`\n",
                        .{entry.path},
                    );
                    return error.OutdatedNaughtyList;
                }
            }
        }
    }
}

fn banned(source: []const u8) ?[]const u8 {
    // Note: must avoid banning ourselves!
    if (std.mem.indexOf(u8, source, "std." ++ "BoundedArray") != null) {
        return "use stdx." ++ "BoundedArray instead of std version";
    }
    return null;
}

fn is_naughty(path: []const u8) bool {
    for (naughty_list) |naughty_path| {
        // Separator-agnostic path comparison.
        if (naughty_path.len == path.len) {
            var equal_paths = true;
            for (naughty_path, 0..) |c, i| {
                equal_paths = equal_paths and
                    (path[i] == c or (path[i] == fs.path.sep and c == fs.path.sep_posix));
            }
            if (equal_paths) return true;
        }
    }
    return false;
}

fn find_long_line(file_text: []const u8) !?usize {
    var line_iterator = mem.split(u8, file_text, "\n");
    var line_number: usize = 0;
    while (line_iterator.next()) |line| : (line_number += 1) {
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100) {
            if (is_url(line)) continue;
            // For multiline strings, we care that the _result_ fits 100 characters,
            // but we don't mind indentation in the source.
            if (parse_multiline_string(line)) |string_value| {
                const string_value_length = try std.unicode.utf8CountCodepoints(string_value);
                if (string_value_length <= 100) continue;
            }
            return line_number;
        }
    }
    return null;
}

/// Heuristically checks if a `line` is a comment with an URL.
fn is_url(line: []const u8) bool {
    const cut = stdx.cut(line, "// https://") orelse return false;
    for (cut.prefix) |c| if (!(c == ' ' or c == '/')) return false;
    for (cut.suffix) |c| if (c == ' ') return false;
    return true;
}

/// If a line is a `\\` string literal, extract its value.
fn parse_multiline_string(line: []const u8) ?[]const u8 {
    const cut = stdx.cut(line, "\\") orelse return null;
    for (cut.prefix) |c| if (c != ' ') return null;
    return cut.suffix;
}

const naughty_list = [_][]const u8{
    "clients/c/tb_client_header_test.zig",
    "clients/c/tb_client.zig",
    "clients/c/tb_client/context.zig",
    "clients/c/tb_client/signal.zig",
    "clients/c/test.zig",
    "clients/docs_types.zig",
    "clients/dotnet/docs.zig",
    "clients/dotnet/dotnet_bindings.zig",
    "clients/go/docs.zig",
    "clients/go/go_bindings.zig",
    "clients/java/docs.zig",
    "clients/java/java_bindings.zig",
    "clients/java/src/client.zig",
    "clients/java/src/jni_tests.zig",
    "clients/node/docs.zig",
    "clients/node/node_bindings.zig",
    "clients/node/src/node.zig",
    "clients/node/src/translate.zig",
    "config.zig",
    "constants.zig",
    "ewah_benchmark.zig",
    "ewah.zig",
    "io/benchmark.zig",
    "io/darwin.zig",
    "io/linux.zig",
    "io/test.zig",
    "io/windows.zig",
    "lsm/binary_search.zig",
    "lsm/compaction.zig",
    "lsm/binary_search_benchmark.zig",
    "lsm/forest_fuzz.zig",
    "lsm/groove.zig",
    "lsm/level_data_iterator.zig",
    "lsm/level_index_iterator.zig",
    "lsm/manifest_level.zig",
    "lsm/merge_iterator.zig",
    "lsm/posted_groove.zig",
    "lsm/segmented_array_benchmark.zig",
    "lsm/segmented_array.zig",
    "lsm/set_associative_cache.zig",
    "lsm/table_data_iterator.zig",
    "lsm/table_mutable.zig",
    "lsm/table.zig",
    "lsm/tree_fuzz.zig",
    "message_bus.zig",
    "message_pool.zig",
    "ring_buffer.zig",
    "simulator.zig",
    "state_machine.zig",
    "state_machine/auditor.zig",
    "state_machine/workload.zig",
    "statsd.zig",
    "storage.zig",
    "test/cluster.zig",
    "test/cluster/network.zig",
    "test/cluster/state_checker.zig",
    "test/conductor.zig",
    "test/network.zig",
    "test/packet_simulator.zig",
    "test/priority_queue.zig",
    "test/storage.zig",
    "test/time.zig",
    "testing/aof.zig",
    "testing/cluster.zig",
    "testing/cluster/network.zig",
    "testing/cluster/state_checker.zig",
    "testing/hash_log.zig",
    "testing/low_level_hash_vectors.zig",
    "testing/packet_simulator.zig",
    "testing/priority_queue.zig",
    "testing/state_machine.zig",
    "testing/storage.zig",
    "testing/time.zig",
    "tigerbeetle/main.zig",
    "time.zig",
    "tracer.zig",
    "vsr.zig",
    "vsr/client_replies.zig",
    "vsr/client.zig",
    "vsr/clock.zig",
    "vsr/grid.zig",
    "vsr/journal.zig",
    "vsr/replica_test.zig",
    "vsr/replica.zig",
    "vsr/superblock_client_sessions.zig",
    "vsr/superblock_client_table.zig",
    "vsr/superblock_free_set.zig",
    "vsr/superblock_quorums.zig",
    "vsr/superblock.zig",
    "vsr/sync.zig",
};
