comptime {
    _ = @import("aof.zig");
    _ = @import("cdc/amqp.zig");
    _ = @import("cdc/amqp/protocol.zig");
    _ = @import("cdc/runner.zig");
    _ = @import("clients/c/tb_client.zig");
    _ = @import("clients/c/tb_client/context.zig");
    _ = @import("clients/c/tb_client/signal.zig");
    _ = @import("clients/c/test.zig");
    _ = @import("copyhound.zig");
    _ = @import("ewah.zig");
    _ = @import("ewah_benchmark.zig");
    _ = @import("io/test.zig");
    _ = @import("iops.zig");
    _ = @import("list.zig");
    _ = @import("lsm/binary_search.zig");
    _ = @import("lsm/binary_search_benchmark.zig");
    _ = @import("lsm/cache_map.zig");
    _ = @import("lsm/composite_key.zig");
    _ = @import("lsm/forest.zig");
    _ = @import("lsm/forest_table_iterator.zig");
    _ = @import("lsm/k_way_merge.zig");
    _ = @import("lsm/k_way_merge_benchmark.zig");
    _ = @import("lsm/manifest_level.zig");
    _ = @import("lsm/node_pool.zig");
    _ = @import("lsm/scratch_memory.zig");
    _ = @import("lsm/segmented_array.zig");
    _ = @import("lsm/segmented_array_benchmark.zig");
    _ = @import("lsm/set_associative_cache.zig");
    _ = @import("lsm/table.zig");
    _ = @import("lsm/table_memory.zig");
    _ = @import("lsm/tree.zig");
    _ = @import("lsm/zig_zag_merge.zig");
    _ = @import("message_buffer.zig");
    _ = @import("multiversion.zig");
    _ = @import("queue.zig");
    _ = @import("repl/completion.zig");
    _ = @import("repl/parser.zig");
    _ = @import("repl/terminal.zig");
    _ = @import("scripts/cfo.zig");
    _ = @import("scripts/changelog.zig");
    _ = @import("shell.zig");
    _ = @import("stack.zig");
    _ = @import("state_machine.zig");
    _ = @import("state_machine_fuzz.zig");
    _ = @import("state_machine_tests.zig");
    _ = @import("testing/bench.zig");
    _ = @import("testing/exhaustigen.zig");
    _ = @import("testing/id.zig");
    _ = @import("testing/marks.zig");
    _ = @import("testing/table.zig");
    _ = @import("testing/vortex/logged_process.zig");
    _ = @import("testing/vortex/supervisor.zig");
    _ = @import("tidy.zig");
    _ = @import("time.zig");
    _ = @import("trace.zig");
    _ = @import("trace/event.zig");
    _ = @import("vsr.zig");
    _ = @import("vsr/checksum.zig");
    _ = @import("vsr/clock.zig");
    _ = @import("vsr/free_set.zig");
    _ = @import("vsr/grid_scrubber.zig");
    _ = @import("vsr/journal.zig");
    _ = @import("vsr/marzullo.zig");
    _ = @import("vsr/message_header.zig");
    _ = @import("vsr/multi_batch.zig");
    _ = @import("vsr/replica_format.zig");
    _ = @import("vsr/replica_test.zig");
    _ = @import("vsr/routing.zig");
    _ = @import("vsr/superblock.zig");
    _ = @import("vsr/superblock_quorums.zig");
}

const quine =
    \\const std = @import("std");
    \\const stdx = @import("stdx");
    \\const builtin = @import("builtin");
    \\const assert = std.debug.assert;
    \\
    \\const MiB = stdx.MiB;
    \\
    \\test quine {
    \\    var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
    \\    defer arena_instance.deinit();
    \\    const arena = arena_instance.allocator();
    \\
    \\    // build.zig runs this in the root dir.
    \\    var src_dir = try std.fs.cwd().openDir("src", .{
    \\        .access_sub_paths = true,
    \\        .iterate = true,
    \\    });
    \\
    \\    var unit_tests_contents = std.ArrayList(u8).init(arena);
    \\    const writer = unit_tests_contents.writer();
    \\    try writer.writeAll("comptime {\n");
    \\
    \\    for (try unit_test_files(arena, src_dir)) |unit_test_file| {
    \\        try writer.print("    _ = @import(\"{s}\");\n", .{unit_test_file});
    \\    }
    \\
    \\    try writer.writeAll("}\n\n");
    \\
    \\    var quine_lines = std.mem.splitScalar(u8, quine, '\n');
    \\    try writer.writeAll("const quine =\n");
    \\    while (quine_lines.next()) |line| {
    \\        try writer.print("    \\\\{s}\n", .{line});
    \\    }
    \\    try writer.writeAll(";\n\n");
    \\
    \\    try writer.writeAll(quine);
    \\
    \\    assert(std.mem.eql(u8, @src().file, "unit_tests.zig"));
    \\    const unit_tests_contents_disk = try src_dir.readFileAlloc(arena, @src().file, 1 * MiB);
    \\    assert(std.mem.startsWith(u8, unit_tests_contents_disk, "comptime {"));
    \\    assert(std.mem.endsWith(u8, unit_tests_contents.items, "}\n"));
    \\
    \\    const unit_tests_needs_update = !std.mem.startsWith(
    \\        u8,
    \\        unit_tests_contents_disk,
    \\        unit_tests_contents.items,
    \\    );
    \\
    \\    if (unit_tests_needs_update) {
    \\        if (std.process.hasEnvVarConstant("SNAP_UPDATE")) {
    \\            // Add the rest of the real file on disk to the generated in-memory file.
    \\            try src_dir.writeFile(.{
    \\                .sub_path = "unit_tests.zig",
    \\                .data = unit_tests_contents.items,
    \\                .flags = .{ .exclusive = false, .truncate = true },
    \\            });
    \\        } else {
    \\            std.debug.print("unit_tests.zig needs updating.\n", .{});
    \\            std.debug.print(
    \\                "Rerun with SNAP_UPDATE=1 environmental variable to update the contents.\n",
    \\                .{},
    \\            );
    \\            assert(false);
    \\        }
    \\    }
    \\}
    \\
    \\fn unit_test_files(arena: std.mem.Allocator, src_dir: std.fs.Dir) ![]const []const u8 {
    \\    // Different platforms can walk the directory in different orders.
    \\    // Store the paths and sort them to ensure consistency.
    \\    var result = std.ArrayList([]const u8).init(arena);
    \\
    \\    var src_walker = try src_dir.walk(arena);
    \\    defer src_walker.deinit();
    \\
    \\    while (try src_walker.next()) |entry| {
    \\        if (entry.kind != .file) continue;
    \\
    \\        const entry_path = try arena.dupe(u8, entry.path);
    \\
    \\        // Replace the path separator to be Unix-style, for consistency on Windows.
    \\        // Don't use entry.path directly!
    \\        if (builtin.os.tag == .windows) {
    \\            std.mem.replaceScalar(u8, entry_path, '\\', '/');
    \\        }
    \\
    \\        if (!std.mem.endsWith(u8, entry_path, ".zig")) continue;
    \\
    \\        if (std.mem.eql(u8, entry_path, "unit_tests.zig")) continue;
    \\        if (std.mem.eql(u8, entry_path, "integration_tests.zig")) continue;
    \\        if (std.mem.startsWith(u8, entry_path, "stdx/")) continue;
    \\        if (std.mem.startsWith(u8, entry_path, "clients/") and
    \\            !std.mem.startsWith(u8, entry_path, "clients/c")) continue;
    \\        if (std.mem.eql(u8, entry_path, "clients/c/tb_client_header_test.zig")) continue;
    \\        if (std.mem.eql(u8, entry_path, "tigerbeetle/libtb_client.zig")) continue;
    \\
    \\        const contents = try src_dir.readFileAlloc(arena, entry_path, 1 * MiB);
    \\        var line_iterator = std.mem.splitScalar(u8, contents, '\n');
    \\        while (line_iterator.next()) |line| {
    \\            const line_trimmed = std.mem.trimLeft(u8, line, " ");
    \\            if (std.mem.startsWith(u8, line_trimmed, "test ")) {
    \\                try result.append(entry_path);
    \\                break;
    \\            }
    \\        }
    \\    }
    \\
    \\    std.mem.sort(
    \\        []const u8,
    \\        result.items,
    \\        {},
    \\        struct {
    \\            fn less_than_fn(_: void, a: []const u8, b: []const u8) bool {
    \\                return std.mem.order(u8, a, b) == .lt;
    \\            }
    \\        }.less_than_fn,
    \\    );
    \\
    \\    return result.items;
    \\}
    \\
;

const std = @import("std");
const stdx = @import("stdx");
const builtin = @import("builtin");
const assert = std.debug.assert;

const MiB = stdx.MiB;

test quine {
    var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_instance.deinit();
    const arena = arena_instance.allocator();

    // build.zig runs this in the root dir.
    var src_dir = try std.fs.cwd().openDir("src", .{
        .access_sub_paths = true,
        .iterate = true,
    });

    var unit_tests_contents = std.ArrayList(u8).init(arena);
    const writer = unit_tests_contents.writer();
    try writer.writeAll("comptime {\n");

    for (try unit_test_files(arena, src_dir)) |unit_test_file| {
        try writer.print("    _ = @import(\"{s}\");\n", .{unit_test_file});
    }

    try writer.writeAll("}\n\n");

    var quine_lines = std.mem.splitScalar(u8, quine, '\n');
    try writer.writeAll("const quine =\n");
    while (quine_lines.next()) |line| {
        try writer.print("    \\\\{s}\n", .{line});
    }
    try writer.writeAll(";\n\n");

    try writer.writeAll(quine);

    assert(std.mem.eql(u8, @src().file, "unit_tests.zig"));
    const unit_tests_contents_disk = try src_dir.readFileAlloc(arena, @src().file, 1 * MiB);
    assert(std.mem.startsWith(u8, unit_tests_contents_disk, "comptime {"));
    assert(std.mem.endsWith(u8, unit_tests_contents.items, "}\n"));

    const unit_tests_needs_update = !std.mem.startsWith(
        u8,
        unit_tests_contents_disk,
        unit_tests_contents.items,
    );

    if (unit_tests_needs_update) {
        if (std.process.hasEnvVarConstant("SNAP_UPDATE")) {
            // Add the rest of the real file on disk to the generated in-memory file.
            try src_dir.writeFile(.{
                .sub_path = "unit_tests.zig",
                .data = unit_tests_contents.items,
                .flags = .{ .exclusive = false, .truncate = true },
            });
        } else {
            std.debug.print("unit_tests.zig needs updating.\n", .{});
            std.debug.print(
                "Rerun with SNAP_UPDATE=1 environmental variable to update the contents.\n",
                .{},
            );
            assert(false);
        }
    }
}

fn unit_test_files(arena: std.mem.Allocator, src_dir: std.fs.Dir) ![]const []const u8 {
    // Different platforms can walk the directory in different orders.
    // Store the paths and sort them to ensure consistency.
    var result = std.ArrayList([]const u8).init(arena);

    var src_walker = try src_dir.walk(arena);
    defer src_walker.deinit();

    while (try src_walker.next()) |entry| {
        if (entry.kind != .file) continue;

        const entry_path = try arena.dupe(u8, entry.path);

        // Replace the path separator to be Unix-style, for consistency on Windows.
        // Don't use entry.path directly!
        if (builtin.os.tag == .windows) {
            std.mem.replaceScalar(u8, entry_path, '\\', '/');
        }

        if (!std.mem.endsWith(u8, entry_path, ".zig")) continue;

        if (std.mem.eql(u8, entry_path, "unit_tests.zig")) continue;
        if (std.mem.eql(u8, entry_path, "integration_tests.zig")) continue;
        if (std.mem.startsWith(u8, entry_path, "stdx/")) continue;
        if (std.mem.startsWith(u8, entry_path, "clients/") and
            !std.mem.startsWith(u8, entry_path, "clients/c")) continue;
        if (std.mem.eql(u8, entry_path, "clients/c/tb_client_header_test.zig")) continue;
        if (std.mem.eql(u8, entry_path, "tigerbeetle/libtb_client.zig")) continue;

        const contents = try src_dir.readFileAlloc(arena, entry_path, 1 * MiB);
        var line_iterator = std.mem.splitScalar(u8, contents, '\n');
        while (line_iterator.next()) |line| {
            const line_trimmed = std.mem.trimLeft(u8, line, " ");
            if (std.mem.startsWith(u8, line_trimmed, "test ")) {
                try result.append(entry_path);
                break;
            }
        }
    }

    std.mem.sort(
        []const u8,
        result.items,
        {},
        struct {
            fn less_than_fn(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less_than_fn,
    );

    return result.items;
}
