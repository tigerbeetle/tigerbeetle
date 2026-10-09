const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = stdx.Shell;
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

const MiB = stdx.MiB;

// crates.io rejects uploads larger than this.
const crates_io_upload_size_max = 10 * MiB;

pub fn tests(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    tigerbeetle: []const u8,
}) !void {
    try shell.exec_zig("build clients:rust -Drelease", .{});
    try shell.exec("cargo test --all", .{});
    try shell.exec("cargo fmt --check", .{});
    try shell.exec("cargo clippy -- -D clippy::all", .{});

    inline for (.{
        "basic",
        "two-phase",
        "two-phase-many",
        "walkthrough",
        "zero-allocation",
    }) |sample| {
        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        try shell.exec("cargo fmt --check", .{});
        try shell.exec("cargo clippy -- -D clippy::all", .{});
        assert(try file_contains(shell, gpa, "Cargo.toml", .{
            .needle = "overflow-checks = true",
            .line_length_max = 100,
        }));

        var tmp_beetle = try TmpTigerBeetle.init(gpa, shell.io, &shell.env, .{
            .development = true,
            .prebuilt = options.tigerbeetle,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
        try shell.exec("cargo run", .{});
    }

    // verify that packaging works and result fits in crates.io limit,
    // also apply `--allow-dirty` like we do in release.zig
    try shell.exec("cargo package --allow-dirty", .{});
    const crate_path = "target/package/tigerbeetle-0.0.0.crate";
    const crate_size = (try shell.cwd.statFile(shell.io, crate_path, .{})).size;
    if (crate_size > crates_io_upload_size_max) {
        log.err("crate too large for crates.io: size={} max={}", .{
            crate_size,
            crates_io_upload_size_max,
        });
        return error.CrateTooLarge;
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
    const file = try shell.cwd.openFile(shell.io, path, .{});
    defer file.close(shell.io);

    const line_buffer = try gpa.alloc(u8, options.line_length_max + 1);
    defer gpa.free(line_buffer);

    var file_reader = file.reader(shell.io, line_buffer);
    while (try file_reader.interface.takeDelimiter('\n')) |line| {
        if (std.mem.indexOf(u8, line, options.needle) != null) return true;
    }

    return false;
}

pub fn validate_release_package(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    release: []const u8,
}) !void {
    _ = gpa;

    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(shell.io, tmp_dir) catch {};

    const published_url = try shell.fmt(
        "https://static.crates.io/crates/tigerbeetle/tigerbeetle-{s}.crate",
        .{options.release},
    );
    const published_crate = try shell.fmt("{s}/published.crate", .{tmp_dir});
    const published_dir = try shell.fmt("{s}/published", .{tmp_dir});
    try shell.cwd.createDirPath(shell.io, published_dir);

    // Multiple attempts in case of network errors.
    const attempts_max = 5;
    const crate_published = for (0..attempts_max) |attempt| {
        break shell.http_get(
            published_url,
            .{ .response_body_size_max = crates_io_upload_size_max },
        ) catch |err| {
            log.warn("crate package download failed. Attempt={}, {}", .{ attempt, err });
            continue;
        };
    } else return error.DownloadAttemptsExceeded;

    try shell.cwd.writeFile(shell.io, .{
        .sub_path = published_crate,
        .data = crate_published,
    });

    const local_path_relative = try shell.fmt(
        "zig-out/dist/rust/tigerbeetle-{s}.crate",
        .{options.release},
    );
    const local_crate = try shell.cwd.realPathFileAlloc(
        shell.io,
        local_path_relative,
        shell.arena.allocator(),
    );
    const local_dir = try shell.fmt("{s}/local", .{tmp_dir});
    try shell.cwd.createDirPath(shell.io, local_dir);

    // Archive metadata might differ, so we extract and diff the files themselves.
    try shell.exec(
        "tar --extract --gzip --file {crate} --directory {dir}",
        .{ .crate = published_crate, .dir = published_dir },
    );
    try shell.exec(
        "tar --extract --gzip --file {crate} --directory {dir}",
        .{ .crate = local_crate, .dir = local_dir },
    );

    // - `.cargo_vcs_info.json` records the git state of the checkout used for packaging
    // - `Cargo.lock` depends on the local registry state while packaging and is ignored by users
    try shell.exec(
        \\diff --recursive
        \\  --exclude=.cargo_vcs_info.json
        \\  --exclude=Cargo.lock
        \\  {published} {local}
    , .{ .published = published_dir, .local = local_dir });
}

pub fn validate_release_sample(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    release: []const u8,
    tigerbeetle: []const u8,
}) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(shell.io, tmp_dir) catch {};

    const base_dir = shell.cwd;
    try shell.pushd(tmp_dir);
    defer shell.popd();

    try shell.exec("cargo init --name test_tigerbeetle", .{});

    for (0..9) |_| {
        if (shell.exec("cargo add tigerbeetle@={release}", .{
            .release = options.release,
        })) {
            break;
        } else |_| {
            log.warn("waiting for 5 minutes for the {s} version to appear on crates.io", .{
                options.release,
            });
            try std.Io.sleep(shell.io, .fromSeconds(5 * std.time.s_per_min), .awake);
        }
    } else {
        shell.exec("cargo add tigerbeetle@={release}", .{
            .release = options.release,
        }) catch |err| {
            log.err("package is not available on crates.io", .{});
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
