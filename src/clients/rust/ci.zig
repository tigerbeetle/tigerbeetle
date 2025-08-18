const std = @import("std");

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
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

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("cargo run", .{});
    }
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    _ = version;
    _ = tigerbeetle;
}
