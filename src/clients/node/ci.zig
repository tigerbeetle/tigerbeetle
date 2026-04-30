const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("package.json"));

    try shell.exec_zig("build clients:node -Drelease", .{});

    // Integration tests.

    // We need to build the tigerbeetle-node library manually for samples/testers to work.
    try shell.exec("npm install", .{});

    for ([_][]const u8{ "test", "benchmark" }) |tester| {
        log.info("testing {s}s", .{tester});

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("node ./dist/{tester}", .{ .tester = tester });
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
        try shell.exec("npm install", .{});
        try shell.exec("node main.js", .{});
    }

    // Container smoke tests.
    if (builtin.target.os.tag == .linux) {
        try shell.exec("npm pack --quiet", .{});

        for ([_][]const u8{ "node:18", "node:18-alpine" }) |image| {
            log.info("testing docker image: '{s}'", .{image});

            try shell.exec(
                \\docker run
                \\--security-opt seccomp=unconfined
                \\--volume ./:/host
                \\{image}
                \\sh
                \\-c {script}
            , .{
                .image = image,
                .script =
                \\set -ex
                \\mkdir test-project && cd test-project
                \\npm install /host/tigerbeetle-node-*.tgz
                \\node -e 'require("tigerbeetle-node"); console.log("SUCCESS!")'
                ,
            });
        }
    }
}

pub fn validate_release_package(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
}) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    const published_url = try shell.fmt(
        "https://registry.npmjs.org/tigerbeetle-node/-/tigerbeetle-node-{s}.tgz",
        .{options.version},
    );
    const published_tgz = try shell.fmt("{s}/published.tgz", .{tmp_dir});
    const published_dir = try shell.fmt("{s}/published", .{tmp_dir});
    try shell.cwd.makePath(published_dir);

    log.info("validating node package {s}", .{published_url});

    // Multiple attempts in case of network errors.
    const attempts_max = 5;
    for (0..attempts_max) |attempt_index| {
        const result = try shell.exec_raw(
            "wget --quiet --output-document={out} {url}",
            .{
                .out = published_tgz,
                .url = published_url,
            },
        );
        switch (result.term) {
            .Exited => |code| if (code == 0) break,
            else => {},
        }

        const attempt = attempt_index + 1;
        log.warn("node package download failed. Attempt={}", .{attempt});
        if (attempt == attempts_max) {
            return error.DownloadAttemptsExceeded;
        }
    }

    const local_path_relative = try shell.fmt(
        "zig-out/dist/node/tigerbeetle-node-{s}.tgz",
        .{options.version},
    );
    const local_tgz = try shell.project_root.realpathAlloc(
        shell.arena.allocator(),
        local_path_relative,
    );
    const local_dir = try shell.fmt("{s}/local", .{tmp_dir});
    try shell.cwd.makePath(local_dir);

    // npm repacks the tarball on publish with a different compression, so we extract and diff.
    try shell.exec(
        "tar --extract --file {tgz} --directory {dir}",
        .{ .tgz = published_tgz, .dir = published_dir },
    );
    try shell.exec(
        "tar --extract --file {tgz} --directory {dir}",
        .{ .tgz = local_tgz, .dir = local_dir },
    );
    try shell.exec(
        "diff --recursive {published} {local}",
        .{ .published = published_dir, .local = local_dir },
    );

    const package_json = try shell.fmt("{s}/package/package.json", .{published_dir});
    try validate_npm_metadata(shell, gpa, package_json);
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

    try shell.exec("npm install tigerbeetle-node@{version}", .{
        .version = options.version,
    });

    try Shell.copy_path(
        shell.project_root,
        "src/clients/node/samples/basic/main.js",
        shell.cwd,
        "main.js",
    );
    try shell.exec("node main.js", .{});
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    return try shell.exec_stdout("npm view tigerbeetle-node version", .{});
}

fn validate_npm_metadata(
    shell: *Shell,
    gpa: std.mem.Allocator,
    package_json_path: []const u8,
) !void {
    const package_json = try shell.cwd.readFileAlloc(gpa, package_json_path, 4 * 1024);
    defer gpa.free(package_json);

    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        shell.arena.allocator(),
        package_json,
        .{},
    );
    defer parsed.deinit();

    // Verify no runtime dependencies were introduced.
    if (parsed.value.object.get("dependencies")) |deps| {
        if (deps == .object and deps.object.count() > 0) {
            std.debug.panic("unexpected dependencies in tigerbeetle-node", .{});
        }
    }

    // Verify no install-time hooks that could run arbitrary code.
    if (parsed.value.object.get("scripts")) |scripts| {
        if (scripts == .object) {
            for ([_][]const u8{ "install", "preinstall", "postinstall" }) |hook| {
                if (scripts.object.get(hook) != null) {
                    std.debug.panic("unexpected '{s}' script in tigerbeetle-node", .{hook});
                }
            }
        }
    }
}
