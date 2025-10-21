const builtin = @import("builtin");
const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("go.mod"));

    const bad_formatting = try shell.exec_stdout("gofmt -l .", .{});
    if (!std.mem.eql(u8, bad_formatting, "")) {
        log.err("these go files need formatting:\n'{s}'", .{bad_formatting});
        return error.GoFmt;
    }

    try shell.exec("go vet", .{});

    // `go build`  won't compile the native library automatically, we need to do that ourselves.
    try shell.exec_zig("build clients:go -Drelease", .{});
    try shell.exec_zig("build -Drelease", .{});

    // Although we have compiled the TigerBeetle client library, we still need `cgo` to link it with
    // our resulting Go binary. Strictly speaking, `CC` is controlled by the users of TigerBeetle,
    // so ideally we should test common flavors of gcc. For simplicity, we:
    //   - use `zig cc` on Windows, as that doesn't have `gcc` out of the box
    //   - use `zig cc` on Linux. It might or might not have `gcc`, but `zig cc` makes our CI more
    //     reproducible
    //   - (implicitly) use `gcc` on Mac, as `zig cc` doesn't work there:
    //     <https://github.com/ziglang/zig/issues/15438>
    switch (builtin.os.tag) {
        .linux, .windows => {
            const zig_cc = try shell.fmt("{s} cc", .{shell.zig_exe.?});
            try shell.env.put("CC", zig_cc);
        },
        .macos => {},
        else => unreachable,
    }

    try shell.exec("go test", .{});
    {
        log.info("testing `types` package helpers", .{});

        try shell.pushd("./pkg/types");
        defer shell.popd();

        try shell.exec("go test", .{});
    }

    inline for (.{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        log.info("testing sample '{s}'", .{sample});

        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("go build main.go", .{});
        try shell.exec("./main" ++ builtin.target.exeFileExt(), .{});
    }
}

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .development = true,
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);

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
    const zig_cc = try shell.fmt("{s} cc", .{shell.zig_exe.?});

    try shell.env.put("CC", zig_cc);
    try shell.exec("go run main.go", .{});
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    // Example output:
    //  github.com/tigerbeetle/tigerbeetle-go v0.9.149 v0.13.56 v0.13.57
    const output = try shell.exec_stdout(
        "go list -m -versions github.com/tigerbeetle/tigerbeetle-go",
        .{},
    );
    const last_version = std.mem.lastIndexOf(u8, output, " v").?;

    return output[last_version + 2 ..];
}
