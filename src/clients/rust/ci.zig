const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("stdx").Shell;
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    try shell.exec_zig("build clients:rust -Drelease", .{});
    try shell.exec("cargo test --all", .{});
    try shell.exec("cargo fmt --check", .{});
    try shell.exec("cargo clippy -- -D clippy::all", .{});

    inline for (.{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        try shell.exec("cargo fmt --check", .{});
        try shell.exec("cargo clippy -- -D clippy::all", .{});
        assert(try file_contains(shell, gpa, "Cargo.toml", .{
            .needle = "overflow-checks = true",
            .line_length_max = 100,
        }));

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("cargo run", .{});
    }
}

fn file_contains(
    shell: *Shell,
    gpa: std.mem.Allocator,
    path: []const u8,
    options: struct {
        needle: []const u8,
        line_length_max: u32 = 100,
    },
) !bool {
    const file = try shell.cwd.openFile(path, .{});
    defer file.close();

    const line_buffer = try gpa.alloc(u8, options.line_length_max + 1);
    defer gpa.free(line_buffer);

    const reader = file.reader();
    while (try reader.readUntilDelimiterOrEof(line_buffer, '\n')) |line| {
        if (std.mem.indexOf(u8, line, options.needle) != null) return true;
    }

    return false;
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
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    const base_dir = shell.cwd;
    try shell.pushd(tmp_dir);
    defer shell.popd();

    try shell.exec("cargo init --name test_tigerbeetle", .{});

    for (0..9) |_| {
        if (shell.exec("cargo add tigerbeetle@{release}", .{
            .release = options.release,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear on crates.io", .{
                options.release,
            });
            std.time.sleep(5 * std.time.ns_per_min);
        }
    } else {
        shell.exec("cargo add tigerbeetle@{release}", .{
            .release = options.release,
        }) catch |err| {
            log.err("package is not available on crates.io", .{});
            return err;
        };
    }

    try shell.exec("cargo add futures@0.3", .{});

    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .development = true,
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);

    try Shell.copy_path(
        base_dir,
        "src/clients/rust/samples/basic/src/main.rs",
        shell.cwd,
        "src/main.rs",
    );
    try shell.exec("cargo run", .{});
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    const CratesResponse = struct {
        crate: struct {
            max_version: []const u8,
        },
    };

    const response_body = try shell.http_get(
        "https://crates.io/api/v1/crates/tigerbeetle",
        .{},
    );

    const crates_result = try std.json.parseFromSliceLeaky(
        CratesResponse,
        shell.arena.allocator(),
        response_body,
        .{ .ignore_unknown_fields = true },
    );

    return crates_result.crate.max_version;
}
