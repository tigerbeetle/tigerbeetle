const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("stdx").Shell;
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    tigerbeetle: []const u8,
}) !void {
    assert(shell.file_exists("tigerbeetle.gemspec"));

    // Integration tests.

    try shell.exec_zig("build clients:ruby -Drelease", .{});

    // Only to test the build process - the samples below run directly from the src/ directory.
    try shell.exec("gem build tigerbeetle.gemspec", .{});

    {
        log.info("running tests", .{});
        var tmp_beetle = try TmpTigerBeetle.init(gpa, shell.io, &shell.env, .{
            .development = true,
            .prebuilt = options.tigerbeetle,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("rake test:unit", .{});
        try shell.exec("rake test:integration", .{});
    }

    inline for ([_][]const u8{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        log.info("testing sample '{s}'", .{sample});

        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, shell.io, &shell.env, .{
            .development = true,
            .prebuilt = options.tigerbeetle,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("ruby -I ../../src -I ../../src/ext main.rb", .{});
    }
}

pub fn validate_release_package(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    release: []const u8,
}) !void {
    _ = shell;
    _ = gpa;
    _ = options;
}

pub fn validate_release_sample(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    release: []const u8,
    tigerbeetle: []const u8,
}) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(shell.io, tmp_dir) catch {};

    try shell.env.put("GEM_HOME", tmp_dir);
    try shell.env.put("GEM_PATH", tmp_dir);

    for (0..9) |_| {
        if (shell.exec("gem install tigerbeetle -v {release}", .{
            .release = options.release,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear in RubyGems", .{
                options.release,
            });
            try std.Io.sleep(shell.io, .fromSeconds(5 * std.time.s_per_min), .awake);
        }
    } else {
        shell.exec("gem install tigerbeetle -v {release}", .{
            .release = options.release,
        }) catch |err| {
            log.err("package is not available in RubyGems", .{});
            return err;
        };
    }

    var tmp_beetle = try TmpTigerBeetle.init(gpa, shell.io, &shell.env, .{
        .development = true,
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);

    try shell.copy_path(
        shell.cwd,
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
