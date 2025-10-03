//! Various CI checks that go beyond `zig build test`. Notably, at the moment this script includes:
//!
//! - Testing all language clients.
//! - Building and link-checking docs.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = @import("../shell.zig");

const client_readmes = @import("./client_readmes.zig");

pub const Language = std.meta.FieldEnum(@TypeOf(LanguageCI));
const LanguageCI = .{
    .dotnet = @import("../clients/dotnet/ci.zig"),
    .go = @import("../clients/go/ci.zig"),
    .rust = @import("../clients/rust/ci.zig"),
    .java = @import("../clients/java/ci.zig"),
    .node = @import("../clients/node/ci.zig"),
    .python = @import("../clients/python/ci.zig"),
};

const LanguageCIVortex = .{
    .rust = @import("../testing/vortex/rust_driver/ci.zig"),
    .java = @import("../testing/vortex/java_driver/ci.zig"),
};

pub const CLIArgs = struct {
    language: ?Language = null,
    validate_release: bool = false,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    if (cli_args.validate_release) {
        try validate_release(shell, gpa, cli_args.language);
    } else {
        try generate_readmes(shell, gpa, cli_args.language);
        try run_tests(shell, gpa, cli_args.language);
    }
}

fn generate_readmes(shell: *Shell, gpa: std.mem.Allocator, language_requested: ?Language) !void {
    inline for (comptime std.enums.values(Language)) |language| {
        if (language_requested == language or language_requested == null) {
            try shell.pushd("./src/clients/" ++ @tagName(language));
            defer shell.popd();

            try client_readmes.test_freshness(shell, gpa, language);
        }
    }
}

fn run_tests(shell: *Shell, gpa: std.mem.Allocator, language_requested: ?Language) !void {
    inline for (comptime std.enums.values(Language)) |language| {
        if (language_requested == language or language_requested == null) {
            {
                const ci = @field(LanguageCI, @tagName(language));
                var section = try shell.open_section(@tagName(language) ++ " ci");
                defer section.close();

                {
                    try shell.pushd("./src/clients/" ++ @tagName(language));
                    defer shell.popd();

                    try ci.tests(shell, gpa);
                }
            }

            // Test the vortex drivers.
            // These may expect the above driver tests to have run,
            // in order to build the driver.
            // They expect the vortex and tigerbeetle drivers to be built.
            if (@hasField(@TypeOf(LanguageCIVortex), @tagName(language))) {
                const ci = @field(LanguageCIVortex, @tagName(language));
                var section = try shell.open_section(@tagName(language) ++ " vortex ci");
                defer section.close();

                {
                    try shell.pushd("./src/testing/vortex/" ++ @tagName(language) ++ "_driver");
                    defer shell.popd();

                    try ci.tests(shell, gpa);
                }
            }
        }
    }
}

fn validate_release(shell: *Shell, gpa: std.mem.Allocator, language_requested: ?Language) !void {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    try shell.pushd_dir(tmp_dir.dir);
    defer shell.popd();

    const release_info = try shell.exec_stdout(
        "gh release --repo tigerbeetle/tigerbeetle list --limit 1",
        .{},
    );
    const tag, _ = stdx.cut(release_info, "\t").?;
    log.info("validating release {s}", .{tag});

    try shell.exec(
        "gh release --repo tigerbeetle/tigerbeetle download {tag}",
        .{ .tag = tag },
    );

    if (builtin.os.tag != .linux) {
        log.warn("skip release verification for platforms other than Linux", .{});
    }

    // Note: when updating the list of artifacts, don't forget to check for any external links.
    //
    // At minimum, `installation.md` requires an update.
    const artifacts = [_][]const u8{
        "tigerbeetle-aarch64-linux-debug.zip",
        "tigerbeetle-aarch64-linux.zip",
        "tigerbeetle-universal-macos-debug.zip",
        "tigerbeetle-universal-macos.zip",
        "tigerbeetle-x86_64-linux-debug.zip",
        "tigerbeetle-x86_64-linux.zip",
        "tigerbeetle-x86_64-windows-debug.zip",
        "tigerbeetle-x86_64-windows.zip",
    };
    for (artifacts) |artifact| {
        assert(shell.file_exists(artifact));
    }

    const git_sha = try shell.exec_stdout("git rev-parse HEAD", .{});
    try shell.exec_zig_options(.{ .timeout = .minutes(20) }, "build scripts -- release --build " ++
        "--sha={git_sha} --language=zig", .{
        .git_sha = git_sha,
    });
    for (artifacts) |artifact| {
        // Zig only guarantees release builds to be deterministic.
        if (std.mem.indexOf(u8, artifact, "-debug.zip") != null) continue;

        const checksum_downloaded = try shell.sha256sum(artifact);

        shell.popd();
        const checksum_built = try shell.sha256sum(try shell.fmt(
            "zig-out/dist/tigerbeetle/{s}",
            .{
                artifact,
            },
        ));
        try shell.pushd_dir(tmp_dir.dir);

        if (checksum_downloaded != checksum_built) {
            std.debug.panic("checksum mismatch - {s}: downloaded {x}, built {x}", .{
                artifact,
                checksum_downloaded,
                checksum_built,
            });
        }
    }

    try shell.unzip_executable("tigerbeetle-x86_64-linux.zip", "tigerbeetle");

    const version = try shell.exec_stdout("./tigerbeetle version --verbose", .{});
    assert(std.mem.indexOf(u8, version, tag) != null);
    assert(std.mem.indexOf(u8, version, "ReleaseSafe") != null);

    const tigerbeetle_absolute_path = try shell.cwd.realpathAlloc(gpa, "tigerbeetle");
    defer gpa.free(tigerbeetle_absolute_path);

    inline for (comptime std.enums.values(Language)) |language| {
        if (language_requested == language or language_requested == null) {
            const ci = @field(LanguageCI, @tagName(language));
            try ci.validate_release(shell, gpa, .{
                .tigerbeetle = tigerbeetle_absolute_path,
                .version = tag,
            });
        }
    }

    // Check all the client releases to ensure the latest published release is what it should be.
    inline for (comptime std.enums.values(Language)) |language| {
        if ((language == language_requested or language_requested == null) and
            language != .rust) // Rust isn't published yet.
        {
            const ci = @field(LanguageCI, @tagName(language));
            const release_published_latest = try ci.release_published_latest(shell);

            if (!std.mem.eql(u8, release_published_latest, tag)) {
                std.debug.panic("version mismatch - {s}: latest published {s}, expected {s}", .{
                    @tagName(language),
                    release_published_latest,
                    tag,
                });
            }
        }
    }

    // Check that the docker tag for latest is the same as the docker tag for the release. Docker's
    // APIs make it much harder to do a release_published_latest() style check as above.
    const docker_digest_latest = try docker_digest(shell, "latest");
    const docker_digest_tagged = try docker_digest(shell, tag);

    if (!std.mem.eql(u8, docker_digest_latest, docker_digest_tagged)) {
        std.debug.panic("version mismatch - docker: latest published {s}, expected {s}", .{
            docker_digest_latest,
            docker_digest_tagged,
        });
    }

    const docker_version = try shell.exec_stdout(
        \\docker run ghcr.io/tigerbeetle/tigerbeetle:{version} version --verbose
    , .{ .version = tag });
    assert(std.mem.indexOf(u8, docker_version, tag) != null);
    assert(std.mem.indexOf(u8, docker_version, "ReleaseSafe") != null);
}

fn docker_digest(shell: *Shell, version: []const u8) ![]const u8 {
    try shell.exec(
        \\docker pull ghcr.io/tigerbeetle/tigerbeetle:{version}
    , .{ .version = version });

    return try shell.exec_stdout("docker inspect --format={format} " ++
        "ghcr.io/tigerbeetle/tigerbeetle:{version}", .{
        .format = "{{index .RepoDigests 0}}",
        .version = version,
    });
}
