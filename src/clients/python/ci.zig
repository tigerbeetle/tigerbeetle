const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = stdx.Shell;
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");
const wheel = @import("wheel.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("pyproject.toml"));

    // Integration tests.

    // Build the native libraries.
    try shell.exec_zig("build clients:python -Drelease", .{});

    // Only to test the build process - the samples below run directly from the src/ directory.
    try wheel.make(shell, "0.0.1", stdx.InstantUnix.now(), "tigerbeetle-0.0.1-py3-none-any.whl");

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

pub fn validate_release_package(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    release: []const u8,
}) !void {
    const PyPIPackage = struct {
        urls: []const struct {
            filename: []const u8,
            url: []const u8,
        },
    };

    const response_body = try shell.http_get(try shell.fmt(
        "https://pypi.org/pypi/tigerbeetle/{s}/json",
        .{options.release},
    ), .{});
    const pypi_package = try std.json.parseFromSliceLeaky(
        PyPIPackage,
        shell.arena.allocator(),
        response_body,
        .{ .ignore_unknown_fields = true },
    );

    assert(pypi_package.urls.len == 1);

    const whl_size_max = 8 * stdx.MiB;
    const whl_filename = try shell.fmt("tigerbeetle-{s}-py3-none-any.whl", .{options.release});
    assert(std.mem.eql(u8, pypi_package.urls[0].filename, whl_filename));
    const whl_url = pypi_package.urls[0].url;

    const whl_published = try shell.http_get(whl_url, .{ .response_body_size_max = whl_size_max });
    const whl_local = try shell.project_root.readFileAlloc(
        gpa,
        try shell.fmt("zig-out/dist/python/{s}", .{whl_filename}),
        whl_size_max,
    );
    defer gpa.free(whl_local);

    if (!std.mem.eql(u8, whl_published, whl_local)) {
        std.debug.panic("tigerbeetle python package doesn't match local build", .{});
    }
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    release: []const u8,
    tigerbeetle: []const u8,
}) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    try shell.exec("python3 -m venv {tmp_dir}", .{ .tmp_dir = tmp_dir });

    for (0..9) |_| {
        if (shell.exec("{tmp_dir}/bin/pip install tigerbeetle=={release}", .{
            .tmp_dir = tmp_dir,
            .release = options.release,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear in PyPi", .{
                options.release,
            });
            std.time.sleep(5 * std.time.ns_per_min);
        }
    } else {
        shell.exec("{tmp_dir}/bin/pip install tigerbeetle=={release}", .{
            .tmp_dir = tmp_dir,
            .release = options.release,
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
