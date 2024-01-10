const builtin = @import("builtin");
const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("go.mod"));

    // `go build`  won't compile the native library automatically, we need to do that ourselves.
    try shell.zig("build go_client -Drelease -Dconfig=production", .{});

    // Although we have compiled the TigerBeetle client library, we still need `cgo` to link it with
    // our resulting Go binary. Strictly speaking, `CC` is controlled by the users of TigerBeetle,
    // so ideally we should test common flavors of gcc. For simplicity, we:
    //   - use `zig cc` on Windows, as that doesn't have `gcc` out of the box
    //   - use `zig cc` on Linux. It might or might not have `gcc`, but `zig cc` makes our CI more
    //     reproducible
    //   - (implicitly) use `gcc` on Mac, as `zig cc` doesn't work there:
    //     <https://github.com/ziglang/zig/issues/15438>
    const zig_cc = if (builtin.os.tag == .macos) void else cc: {
        const zig_exe = try shell.project_root.realpathAlloc(
            shell.arena.allocator(),
            comptime "zig/zig" ++ builtin.target.exeFileExt(),
        );
        break :cc try shell.print("{s} cc", .{zig_exe});
    };

    // Building the server before running the integrated tests:
    try shell.zig("build install -Drelease -Dconfig=production", .{});
    try shell.exec("go test", .{});

    inline for (.{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        if (builtin.os.tag != .macos) {
            try shell.env.put("CC", zig_cc);
        }
        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("go run main.go", .{});
    }
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());

    try shell.exec("go mod init tbtest", .{});
    try shell.exec("go get github.com/tigerbeetle/tigerbeetle-go@v{version}", .{
        .version = options.version,
    });

    try Shell.copy_path(
        shell.project_root,
        "src/clients/go/samples/basic/main.go",
        shell.cwd,
        "main.go",
    );
    const zig_exe = try shell.project_root.realpathAlloc(
        shell.arena.allocator(),
        comptime "zig/zig" ++ builtin.target.exeFileExt(),
    );
    const zig_cc = try shell.print("{s} cc", .{zig_exe});

    try shell.env.put("CC", zig_cc);
    try shell.exec("go run main.go", .{});
}
