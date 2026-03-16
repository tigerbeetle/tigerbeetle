const std = @import("std");
const log = std.log;

const Shell = @import("../../shell.zig");
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

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("cargo run", .{});
    }
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    if (true) return; // TODO: remove once Rust client is published.

    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    try shell.pushd(tmp_dir);
    defer shell.popd();

    try shell.exec("cargo init --name test_tigerbeetle", .{});

    for (0..9) |_| {
        if (shell.exec("cargo add tigerbeetle@{version}", .{
            .version = options.version,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear on crates.io", .{
                options.version,
            });
            std.time.sleep(5 * std.time.ns_per_min);
        }
    } else {
        shell.exec("cargo add tigerbeetle@{version}", .{
            .version = options.version,
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
        shell.project_root,
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
