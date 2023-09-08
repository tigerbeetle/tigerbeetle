//! Orchestrates building and publishing a distribution of tigerbeetle --- a collection of (source
//! and binary) artifacts which constitutes a release and which we upload to various registries.
//!
//! Concretely, the artifacts are:
//!
//! - TigerBeetle binary build for all supported architectures
//! - TigerBeetle clients build for all supported languages
//!
//! This is implemented as a standalone zig script, rather as a step in build.zig, because this is
//! a "meta" build system --- we need to orchestrate `zig build`, `go build`, `npm publish` and
//! friends, and treat them as peers.
//!
//! Note on verbosity: to ease debugging, try to keep the output to O(1) lines per command. The idea
//! here is that, if something goes wrong, you can see _what_ goes wrong and easily copy-paste
//! specific commands to your local terminal, but, at the same time, you don't want to sift through
//! megabytes of info-level noise first.

const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

const Language = enum { dotnet, go, java, node, zig };
const LanguageSet = std.enums.EnumSet(Language);
const CliArgs = struct {
    version: []const u8,
    sha: []const u8,
    language: ?Language = null,
    build: bool = false,
    publish: bool = false,
};

const VersionInfo = struct {
    version: []const u8,
    sha: []const u8,
};

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => fatal("memory leak", .{}),
    };

    const gpa = gpa_allocator.allocator();
    var arena_allocator = std.heap.ArenaAllocator.init(gpa);
    defer arena_allocator.deinit();

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    // Discard executable name.
    _ = args.next().?;

    const cli_args = flags.parse_flags(&args, CliArgs);

    const languages = if (cli_args.language) |language|
        LanguageSet.initOne(language)
    else
        LanguageSet.initFull();

    const version_info = VersionInfo{
        .version = cli_args.version,
        .sha = cli_args.sha,
    };

    if (cli_args.build) {
        try build(shell, languages, version_info);
    }

    if (cli_args.publish) {
        try publish(shell, languages, version_info);
    }
}

fn build(shell: *Shell, languages: LanguageSet, info: VersionInfo) !void {
    try shell.project_root.deleteTree("dist");
    var dist_dir = try shell.project_root.makeOpenPath("dist", .{});
    defer dist_dir.close();

    var timer_total = try std.time.Timer.start();
    log.info("building TigerBeetle distribution into {s}", .{
        try dist_dir.realpathAlloc(shell.arena.allocator(), "."),
    });

    var timer_per_step = try std.time.Timer.start();

    if (languages.contains(.zig)) {
        var dist_dir_tigerbeetle = try dist_dir.makeOpenPath("tigerbeetle", .{});
        defer dist_dir_tigerbeetle.close();

        try build_tigerbeetle(shell, info, dist_dir_tigerbeetle);
    }
    const elapsed_tigerbeetle_ns = timer_per_step.lap();

    if (languages.contains(.dotnet)) {
        var dist_dir_dotnet = try dist_dir.makeOpenPath("dotnet", .{});
        defer dist_dir_dotnet.close();

        try build_dotnet(shell, info, dist_dir_dotnet);
    }
    const elapsed_dotnet_ns = timer_per_step.lap();

    if (languages.contains(.go)) {
        var dist_dir_go = try dist_dir.makeOpenPath("go", .{});
        defer dist_dir_go.close();

        try build_go(shell, info, dist_dir_go);
    }
    const elapsed_go_ns = timer_per_step.lap();

    if (languages.contains(.java)) {
        var dist_dir_java = try dist_dir.makeOpenPath("java", .{});
        defer dist_dir_java.close();

        try build_java(shell, info, dist_dir_java);
    }
    const elapsed_java_ns = timer_per_step.lap();

    if (languages.contains(.node)) {
        var dist_dir_node = try dist_dir.makeOpenPath("node", .{});
        defer dist_dir_node.close();

        try build_node(shell, info, dist_dir_node);
    }
    const elapsed_node_ns = timer_per_step.lap();

    const elapsed_total_ns = timer_total.lap();
    log.info(
        \\build distribution in {d}
        \\  tigerbeetle {d}
        \\  dotnet      {d}
        \\  go          {d}
        \\  java        {d}
        \\  node        {d}
    , .{
        std.fmt.fmtDuration(elapsed_total_ns),
        std.fmt.fmtDuration(elapsed_tigerbeetle_ns),
        std.fmt.fmtDuration(elapsed_dotnet_ns),
        std.fmt.fmtDuration(elapsed_go_ns),
        std.fmt.fmtDuration(elapsed_java_ns),
        std.fmt.fmtDuration(elapsed_node_ns),
    });
}

fn build_tigerbeetle(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    log.info("building tigerbeetle", .{});
    try shell.project_root.setAsCwd();

    const llvm_lipo_version = shell.exec_stdout("llvm-lipo -version", .{}) catch {
        fatal("can't find llvm-lipo", .{});
    };
    log.info("llvm-lipo version {s}", .{llvm_lipo_version});

    // We shell out to `zip` for creating archives, so we need an absolute path here.
    const dist_dir_path = try dist_dir.realpathAlloc(shell.arena.allocator(), ".");

    const targets = .{
        "aarch64-linux",
        "x86_64-linux",
        "x86_64-windows",
        "aarch64-macos",
        "x86_64-macos",
    };
    const cpus = .{
        "baseline+aes+neon",
        "x86_64_v3+aes",
        "x86_64_v3+aes",
        "baseline+aes+neon",
        "x86_64_v3+aes",
    };
    comptime assert(targets.len == cpus.len);

    // Build tigerbeetle binary for all OS/CPU combinations we support and copy the result to
    // `dist`. MacOS is special cased --- we use an extra step to merge x86 and arm binaries into
    // one.
    //TODO: use std.Target here
    inline for (.{ false, true }) |debug| {
        inline for (targets, cpus) |target, cpu| {
            try shell.zig(
                \\build install
                \\    -Dtarget={target} -Dcpu={cpu}
                \\    -Doptimize={mode}
                \\    -Dversion={version}
            , .{
                .target = target,
                .cpu = cpu,
                .mode = if (debug) "Debug" else "ReleaseSafe",
                .version = info.version,
            });

            const windows = comptime std.mem.indexOf(u8, target, "windows") != null;
            const macos = comptime std.mem.indexOf(u8, target, "macos") != null;

            if (macos) {
                try Shell.copy_path(
                    shell.project_root,
                    "tigerbeetle",
                    shell.project_root,
                    "tigerbeetle-" ++ target,
                );
            } else {
                const zip_name = "tigerbeetle-" ++ target ++ if (debug) "-debug" else "" ++ ".zip";
                try shell.exec("zip -9 {zip_path} {exe_name}", .{
                    .zip_path = try shell.print("{s}/{s}", .{ dist_dir_path, zip_name }),
                    .exe_name = "tigerbeetle" ++ if (windows) ".exe" else "",
                });
            }
        }

        try shell.exec(
            \\llvm-lipo
            \\  tigerbeetle-aarch64-macos tigerbeetle-x86_64-macos
            \\  -create -output tigerbeetle
        , .{});
        try shell.project_root.deleteFile("tigerbeetle-aarch64-macos");
        try shell.project_root.deleteFile("tigerbeetle-x86_64-macos");
        const zip_name = "tigerbeetle-universal-macos" ++ if (debug) "-debug" else "" ++ ".zip";
        try shell.exec("zip -9 {zip_path} {exe_name}", .{
            .zip_path = try shell.print("{s}/{s}", .{ dist_dir_path, zip_name }),
            .exe_name = "tigerbeetle",
        });
    }
}

fn build_dotnet(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    log.info("building dotnet client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/dotnet", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const dotnet_version = shell.exec_stdout("dotnet --version", .{}) catch {
        fatal("can't find dotnet", .{});
    };
    log.info("dotnet version {s}", .{dotnet_version});

    try shell.exec(
        \\dotnet pack TigerBeetle --configuration Release
        \\/p:AssemblyVersion={version} /p:Version={version}
    , .{ .version = info.version });

    try Shell.copy_path(
        client_src_dir,
        try shell.print("TigerBeetle/bin/Release/tigerbeetle.{s}.nupkg", .{info.version}),
        dist_dir,
        try shell.print("tigerbeetle.{s}.nupkg", .{info.version}),
    );
}

fn build_go(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    log.info("building go client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/go", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    try shell.zig("build go_client -Doptimize=ReleaseSafe -Dconfig=production", .{});

    const files = try shell.exec_stdout("git ls-files", .{});
    var files_lines = std.mem.tokenize(u8, files, "\n");
    var copied_count: u32 = 0;
    while (files_lines.next()) |file| {
        assert(file.len > 3);
        try Shell.copy_path(client_src_dir, file, dist_dir, file);
        copied_count += 1;
    }
    assert(copied_count >= 10);

    const native_files = try shell.find(.{ .where = &.{"."}, .extensions = &.{ ".a", ".lib" } });
    copied_count = 0;
    for (native_files) |native_file| {
        try Shell.copy_path(client_src_dir, native_file, dist_dir, native_file);
        copied_count += 1;
    }
    // 5 = 3 + 2
    //     3 = x86_64 for mac, windows and linux
    //         2 = aarch64 for mac and linux
    assert(copied_count == 5);

    const readme = try shell.print(
        \\# tigerbeetle-go
        \\This repo has been automatically generated from
        \\[tigerbeetle/tigerbeetle@{[sha]s}](https://github.com/tigerbeetle/tigerbeetle/commit/{[sha]s})
        \\to keep binary blobs out of the monorepo.
        \\Please see
        \\<https://github.com/tigerbeetle/tigerbeetle/tree/main/src/clients/go>
        \\for documentation and contributions.
    , .{ .sha = info.sha });
    try dist_dir.writeFile("README.md", readme);
}

fn build_java(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    log.info("building java client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/java", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const java_version = shell.exec_stdout("java --version", .{}) catch {
        fatal("can't find java", .{});
    };
    log.info("java version {s}", .{java_version});

    try backup_create(client_src_dir, "pom.xml");
    defer backup_restore(client_src_dir, "pom.xml");

    try shell.exec(
        \\mvn --batch-mode --quiet --file pom.xml
        \\versions:set -DnewVersion={version}
    , .{ .version = info.version });

    try shell.exec(
        \\mvn --batch-mode --quiet --file pom.xml
        \\  -Dmaven.test.skip -Djacoco.skip
        \\  package
    , .{});

    try Shell.copy_path(
        client_src_dir,
        try shell.print("target/tigerbeetle-java-{s}.jar", .{info.version}),
        dist_dir,
        try shell.print("tigerbeetle-java-{s}.jar", .{info.version}),
    );
}

fn build_node(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    log.info("building node client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/node", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const node_version = shell.exec_stdout("node --version", .{}) catch {
        fatal("can't find nodejs", .{});
    };
    log.info("node version {s}", .{node_version});

    try backup_create(client_src_dir, "package.json");
    defer backup_restore(client_src_dir, "package.json");

    try backup_create(client_src_dir, "package-lock.json");
    defer backup_restore(client_src_dir, "package-lock.json");

    try shell.exec("npm version --no-git-tag-version {version}", .{ .version = info.version });
    try shell.exec("npm pack --quiet", .{});

    try Shell.copy_path(
        client_src_dir,
        try shell.print("tigerbeetle-node-{s}.tgz", .{info.version}),
        dist_dir,
        try shell.print("tigerbeetle-node-{s}.tgz", .{info.version}),
    );
}

fn publish(shell: *Shell, languages: LanguageSet, info: VersionInfo) !void {
    try shell.project_root.setAsCwd();
    assert(try shell.dir_exists("dist"));

    if (languages.contains(.zig)) {
        _ = try shell.env_get("GITHUB_TOKEN");
        const gh_version = shell.exec_stdout("gh --version", .{}) catch {
            fatal("can't find gh", .{});
        };
        log.info("gh version {s}", .{gh_version});

        const notes = try shell.print(
            \\{[version]s}
            \\
            \\**Automated build. Do not use in production.**
            \\
            \\**NOTE**: You must run the same version of server and client. We do
            \\not yet follow semantic versioning where all patch releases are
            \\interchangeable.
            \\
            \\## Server
            \\
            \\* Binary: Download the zip for your OS and architecture from this page and unzip.
            \\* Docker: `docker pull ghcr.io/tigerbeetle/tigerbeetle:{[version]s}`
            \\* Docker (debug image): \`docker pull ghcr.io/tigerbeetle/tigerbeetle:{[version]s}-debug`
            \\
            \\## Clients
            \\
            \\**NOTE**: Because of package manager caching, it may take a few
            \\  minutes after the release for this version to appear in the package
            \\  manager.
            \\
            \\* .NET: `dotnet add package tigerbeetle --version {[version]s}`
            \\* Go: `go mod edit -require github.com/tigerbeetle/tigerbeetle-go@v{[version]s}`
            \\* Java: Update the version of `com.tigerbeetle.tigerbeetle-java` in `pom.xml` to `{[version]s}`.
            \\* Node.js: `npm install tigerbeetle-node@{[version]s}`
        , .{ .version = info.version });

        try shell.exec(
            \\gh release create --draft --prerelease
            \\  --notes {notes}
            \\  {tag}
        , .{
            .tag = info.version,
            .notes = notes,
        });

        // Here and elsewhere for publishing we explicitly spell out the files we are uploading
        // instead of using a for loop to double-check the logic in `build`.
        const artifacts: []const []const u8 = &.{
            "dist/tigerbeetle/tigerbeetle-aarch64-linux-debug.zip",
            "dist/tigerbeetle/tigerbeetle-aarch64-linux.zip",
            "dist/tigerbeetle/tigerbeetle-universal-macos-debug.zip",
            "dist/tigerbeetle/tigerbeetle-universal-macos.zip",
            "dist/tigerbeetle/tigerbeetle-x86_64-linux-debug.zip",
            "dist/tigerbeetle/tigerbeetle-x86_64-linux.zip",
            "dist/tigerbeetle/tigerbeetle-x86_64-windows-debug.zip",
            "dist/tigerbeetle/tigerbeetle-x86_64-windows.zip",
        };
        try shell.exec("gh release upload {tag} {artifacts}", .{
            .tag = info.version,
            .artifacts = artifacts,
        });
    }

    if (languages.contains(.dotnet)) try publish_dotnet(shell, info);
    if (languages.contains(.go)) try publish_go(shell, info);
    if (languages.contains(.java)) try publish_java(shell, info);
    if (languages.contains(.node)) try publish_node(shell, info);

    if (languages.contains(.zig)) {
        try shell.exec(
            \\gh release edit --draft=false
            \\  {tag}
        , .{ .tag = info.version });
    }
}

fn publish_dotnet(shell: *Shell, info: VersionInfo) !void {
    try shell.project_root.setAsCwd();
    assert(try shell.dir_exists("dist/dotnet"));

    const nuget_key = try shell.env_get("NUGET_KEY");
    try shell.exec(
        \\dotnet nuget push
        \\    --api-key {nuget_key}
        \\    --source https://api.nuget.org/v3/index.json
        \\    {package}
    , .{
        .nuget_key = nuget_key,
        .package = try shell.print("dist/dotnet/tigerbeetle.{s}.nupkg", .{info.version}),
    });
}

fn publish_go(shell: *Shell, info: VersionInfo) !void {
    try shell.project_root.setAsCwd();
    assert(try shell.dir_exists("dist/go"));

    const token = try shell.env_get("TIGERBEETLE_GO_GITHUB_TOKEN");
    try shell.exec(
        \\git clone --no-checkout --depth 1
        \\  https://{token}@github.com/matklad/tigerbeetle-go.git tigerbeetle-go
    , .{ .token = token });
    defer {
        shell.project_root.deleteTree("tigerbeetle-go") catch {};
    }

    const dist_files = try shell.find(.{ .where = &.{"dist/go"} });
    for (dist_files) |file| {
        try Shell.copy_path(
            shell.project_root,
            file,
            shell.project_root,
            try std.mem.replaceOwned(
                u8,
                shell.arena.allocator(),
                file,
                "dist/go",
                "tigerbeetle-go",
            ),
        );
    }

    var tigerbeetle_go_dir = try shell.project_root.openDir("tigerbeetle-go", .{});
    defer tigerbeetle_go_dir.close();

    try tigerbeetle_go_dir.setAsCwd();

    try shell.exec("git add .", .{});
    try shell.exec("git commit --author  {author} --message {message}", .{
        .author = "TigerBeetle Bot <bot@tigerbeetle.com>",
        .message = try shell.print(
            "Autogenerated commit from tigerbeetle/tigerbeetle@{s}",
            .{info.sha},
        ),
    });

    try shell.exec("git tag tigerbeetle-{sha}", .{ .sha = info.sha });
    try shell.exec("git tag v{version}", .{ .version = info.version });

    try shell.exec("git push origin main", .{});
    try shell.exec("git push origin tigerbeetle-{sha}", .{ .sha = info.sha });
    try shell.exec("git push origin v{version}", .{ .version = info.version });
}

fn publish_java(shell: *Shell, info: VersionInfo) !void {
    try shell.project_root.setAsCwd();
    assert(try shell.dir_exists("dist/java"));

    // These variables don't have a special meaning in maven, and instead are a part of
    // settings.xml generated by GitHub actions.
    _ = try shell.env_get("MAVEN_USERNAME");
    _ = try shell.env_get("MAVEN_CENTRAL_TOKEN");
    _ = try shell.env_get("MAVEN_GPG_PASSPHRASE");

    // TODO: Maven uniquely doesn't support uploading pre-build package, so here we just rebuild
    // from source and upload a _different_ artifact. This is wrong.
    //
    // As far as I can tell, there isn't a great solution here. See, for example:
    //
    // <https://users.maven.apache.narkive.com/jQ3WocgT/mvn-deploy-without-rebuilding>
    //
    // I think what we should do here is for `build` to deploy to the local repo, and then use
    //
    // <https://gist.github.com/rishabh9/183cc0c4c3ada4f8df94d65fcd73a502>
    //
    // to move the contents of that local repo to maven central. But this is todo, just rebuild now.
    try backup_create(shell.project_root, "src/clients/java/pom.xml");
    defer backup_restore(shell.project_root, "src/clients/java/pom.xml");

    try shell.exec(
        \\mvn --batch-mode --quiet --file src/clients/java/pom.xml
        \\versions:set -DnewVersion={version}
    , .{ .version = info.version });

    try shell.exec(
        \\mvn --batch-mode --quiet --file src/clients/java/pom.xml
        \\  -Dmaven.test.skip -Djacoco.skip
        \\  deploy
    , .{});
}

fn publish_node(shell: *Shell, info: VersionInfo) !void {
    try shell.project_root.setAsCwd();
    assert(try shell.dir_exists("dist/node"));

    // `NODE_AUTH_TOKEN` env var doesn't have a special meaning in npm. It does have special meaning
    // in GitHub Actions, which adds a literal
    //
    //    //registry.npmjs.org/:_authToken=${NODE_AUTH_TOKEN}
    //
    // to the .npmrc file (that is, node config file itself supports env variables).
    _ = try shell.env_get("NODE_AUTH_TOKEN");
    try shell.exec("npm publish {package}", .{
        .package = try shell.print("dist/node/tigerbeetle-node-{s}.tgz", .{info.version}),
    });
}

fn backup_create(dir: std.fs.Dir, comptime file: []const u8) !void {
    try Shell.copy_path(dir, file, dir, file ++ ".backup");
}

fn backup_restore(dir: std.fs.Dir, comptime file: []const u8) void {
    dir.deleteFile(file) catch {};
    Shell.copy_path(dir, file ++ ".backup", dir, file) catch {};
    dir.deleteFile(file ++ ".backup") catch {};
}
