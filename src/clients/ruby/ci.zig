const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("tigerbeetle.gemspec"));

    // Integration tests.

    try shell.exec_zig("build clients:ruby -Drelease", .{});

    // Only to test the build process - the samples below run directly from the src/ directory.
    try shell.exec("gem build tigerbeetle.gemspec", .{});

    const path_relative = try std.fs.path.join(shell.arena.allocator(), &.{
        "src",
        @src().file,
    });
    const ruby_path_relative = try std.fs.path.join(shell.arena.allocator(), &.{
        std.fs.path.dirname(path_relative).?,
        "src",
    });

    const ruby_path = try shell.project_root.realpathAlloc(
        shell.arena.allocator(),
        ruby_path_relative,
    );

    try shell.env.put("RUBYLIB", ruby_path);

    {
        log.info("running tests", .{});
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
        try shell.exec("ruby tests/test_basic.rb", .{});
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
        try shell.exec("ruby main.rb", .{});
    }
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    try shell.env.put("GEM_HOME", tmp_dir);
    try shell.env.put("GEM_PATH", tmp_dir);

    for (0..9) |_| {
        if (shell.exec("gem install tigerbeetle -v {version}", .{
            .version = options.version,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear in RubyGems", .{
                options.version,
            });
            std.time.sleep(5 * std.time.ns_per_min);
        }
    } else {
        shell.exec("gem install tigerbeetle -v {version}", .{
            .version = options.version,
        }) catch |err| {
            log.err("package is not available in RubyGems", .{});
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
        "src/clients/ruby/samples/basic/main.rb",
        shell.cwd,
        "main.rb",
    );
    try shell.exec("ruby main.rb", .{});
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    const output = try shell.exec_stdout("gem search --exact --versions tigerbeetle", .{});
    const version_start = std.mem.indexOf(u8, output, "(").? + 1;
    const version_end = std.mem.indexOf(u8, output, ")").?;

    return output[version_start..version_end];
}
