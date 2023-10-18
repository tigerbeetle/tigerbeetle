const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    // We have some unit-tests for node, but they are likely bitrotted, as they are not run on CI.

    // Integration tests.

    // We need to build the tigerbeetle-node library manually for samples to be able to pick it up.
    try shell.exec("npm install", .{});
    try shell.exec("npm pack --quiet", .{});

    inline for (.{ "basic", "two-phase", "two-phase-many" }) |sample| {
        var sample_dir = try shell.project_root.openDir("src/clients/node/samples/" ++ sample, .{});
        defer sample_dir.close();

        try sample_dir.setAsCwd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("npm install", .{});
        try shell.exec("node main.js", .{});
    }
}

pub fn verify_release(shell: *Shell, gpa: std.mem.Allocator, tmp_dir: std.fs.Dir) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
    defer tmp_beetle.deinit(gpa);

    try shell.exec("npm install tigerbeetle-node", .{});

    try Shell.copy_path(
        shell.project_root,
        "src/clients/node/samples/basic/main.js",
        tmp_dir,
        "main.js",
    );
    try shell.exec("node main.js", .{});
}
