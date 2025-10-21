const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("pyproject.toml"));

    // Integration tests.

    // `python3 -m build` won't compile the native library automatically, we need to do that
    // ourselves.
    try shell.exec_zig("build clients:python -Drelease", .{});

    // Only to test the build process - the samples below run directly from the src/ directory.
    try shell.exec("python3 -m build .", .{});

    const path_relative = try std.fs.path.join(shell.arena.allocator(), &.{
        "src",
        @src().file,
    });
    const python_path_relative = try std.fs.path.join(shell.arena.allocator(), &.{
        std.fs.path.dirname(path_relative).?,
        "src",
    });

    const python_path = try shell.project_root.realpathAlloc(
        shell.arena.allocator(),
        python_path_relative,
    );

    try shell.env.put("PYTHONPATH", python_path);

    {
        log.info("running pytest", .{});
        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        const tigerbeetle_exe = comptime "tigerbeetle" ++ builtin.target.exeFileExt();
        const tigerbeetle_path = try shell.project_root.realpathAlloc(
            shell.arena.allocator(),
            tigerbeetle_exe,
        );
        try shell.env.put("TIGERBEETLE_BINARY", tigerbeetle_path);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("python3 -m pytest tests/", .{});
    }

    inline for ([_][]const u8{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        log.info("testing sample '{s}'", .{sample});

        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("python3 main.py", .{});
    }

    // We are checking type annotations of the entire package.
    try shell.exec("python3 -m mypy . --strict", .{});
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    try shell.exec("python3 -m venv {tmp_dir}", .{ .tmp_dir = tmp_dir });

    for (0..9) |_| {
        if (shell.exec("{tmp_dir}/bin/pip install tigerbeetle=={version}", .{
            .tmp_dir = tmp_dir,
            .version = options.version,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear in PyPi", .{
                options.version,
            });
            std.time.sleep(5 * std.time.ns_per_min);
        }
    } else {
        shell.exec("{tmp_dir}/bin/pip install tigerbeetle=={version}", .{
            .tmp_dir = tmp_dir,
            .version = options.version,
        }) catch |err| {
            log.err("package is not available in PyPi", .{});
            return err;
        };
    }

    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .development = true,
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);

    try Shell.copy_path(
        shell.project_root,
        "src/clients/python/samples/basic/main.py",
        shell.cwd,
        "main.py",
    );
    try shell.exec("{tmp_dir}/bin/python3 main.py", .{ .tmp_dir = tmp_dir });
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    const output = try shell.exec_stdout("python3 -m pip index versions tigerbeetle", .{});
    const version_start = std.mem.indexOf(u8, output, "(").? + 1;
    const version_end = std.mem.indexOf(u8, output, ")").?;

    return output[version_start..version_end];
}
