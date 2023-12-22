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

const stdx = @import("../stdx.zig");
const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

const Language = enum { dotnet, go, java, node, zig, docker };
const LanguageSet = std.enums.EnumSet(Language);
pub const CliArgs = struct {
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

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    _ = gpa;

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
    var section = try shell.open_section("build all");
    defer section.close();

    try shell.project_root.deleteTree("dist");
    var dist_dir = try shell.project_root.makeOpenPath("dist", .{});
    defer dist_dir.close();

    log.info("building TigerBeetle distribution into {s}", .{
        try dist_dir.realpathAlloc(shell.arena.allocator(), "."),
    });

    if (languages.contains(.zig)) {
        var dist_dir_tigerbeetle = try dist_dir.makeOpenPath("tigerbeetle", .{});
        defer dist_dir_tigerbeetle.close();

        try build_tigerbeetle(shell, info, dist_dir_tigerbeetle);
    }

    if (languages.contains(.dotnet)) {
        var dist_dir_dotnet = try dist_dir.makeOpenPath("dotnet", .{});
        defer dist_dir_dotnet.close();

        try build_dotnet(shell, info, dist_dir_dotnet);
    }

    if (languages.contains(.go)) {
        var dist_dir_go = try dist_dir.makeOpenPath("go", .{});
        defer dist_dir_go.close();

        try build_go(shell, info, dist_dir_go);
    }

    if (languages.contains(.java)) {
        var dist_dir_java = try dist_dir.makeOpenPath("java", .{});
        defer dist_dir_java.close();

        try build_java(shell, info, dist_dir_java);
    }

    if (languages.contains(.node)) {
        var dist_dir_node = try dist_dir.makeOpenPath("node", .{});
        defer dist_dir_node.close();

        try build_node(shell, info, dist_dir_node);
    }
}

fn build_tigerbeetle(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build tigerbeetle");
    defer section.close();

    const llvm_lipo = for (@as([2][]const u8, .{ "llvm-lipo-16", "llvm-lipo" })) |llvm_lipo| {
        if (shell.exec_stdout("{llvm_lipo} -version", .{
            .llvm_lipo = llvm_lipo,
        })) |llvm_lipo_version| {
            log.info("llvm-lipo version {s}", .{llvm_lipo_version});
            break llvm_lipo;
        } else |_| {}
    } else {
        fatal("can't find llvm-lipo", .{});
    };

    // We shell out to `zip` for creating archives, so we need an absolute path here.
    const dist_dir_path = try dist_dir.realpathAlloc(shell.arena.allocator(), ".");

    const targets = .{
        "aarch64-linux",
        "x86_64-linux",
        "x86_64-windows",
        "aarch64-macos",
        "x86_64-macos",
    };

    // Build tigerbeetle binary for all OS/CPU combinations we support and copy the result to
    // `dist`. MacOS is special cased --- we use an extra step to merge x86 and arm binaries into
    // one.
    //TODO: use std.Target here
    inline for (.{ false, true }) |debug| {
        inline for (targets) |target| {
            try shell.zig(
                \\build install
                \\    -Dtarget={target}
                \\    -Drelease={release}
                \\    -Dgit-commit={commit}
                \\    -Dversion={version}
            , .{
                .target = target,
                .release = if (debug) "false" else "true",
                .commit = info.sha,
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
            \\{llvm_lipo}
            \\  tigerbeetle-aarch64-macos tigerbeetle-x86_64-macos
            \\  -create -output tigerbeetle
        , .{ .llvm_lipo = llvm_lipo });
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
    var section = try shell.open_section("build dotnet");
    defer section.close();

    try shell.pushd("./src/clients/dotnet");
    defer shell.popd();

    const dotnet_version = shell.exec_stdout("dotnet --version", .{}) catch {
        fatal("can't find dotnet", .{});
    };
    log.info("dotnet version {s}", .{dotnet_version});

    try shell.exec(
        \\dotnet pack TigerBeetle --configuration Release
        \\/p:AssemblyVersion={version} /p:Version={version}
    , .{ .version = info.version });

    try Shell.copy_path(
        shell.cwd,
        try shell.print("TigerBeetle/bin/Release/tigerbeetle.{s}.nupkg", .{info.version}),
        dist_dir,
        try shell.print("tigerbeetle.{s}.nupkg", .{info.version}),
    );
}

fn build_go(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build go");
    defer section.close();

    try shell.pushd("./src/clients/go");
    defer shell.popd();

    try shell.zig("build go_client -Drelease -Dconfig=production", .{});

    const files = try shell.exec_stdout("git ls-files", .{});
    var files_lines = std.mem.tokenize(u8, files, "\n");
    var copied_count: u32 = 0;
    while (files_lines.next()) |file| {
        assert(file.len > 3);
        try Shell.copy_path(shell.cwd, file, dist_dir, file);
        copied_count += 1;
    }
    assert(copied_count >= 10);

    const native_files = try shell.find(.{ .where = &.{"."}, .extensions = &.{ ".a", ".lib" } });
    copied_count = 0;
    for (native_files) |native_file| {
        try Shell.copy_path(shell.cwd, native_file, dist_dir, native_file);
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
    var section = try shell.open_section("build java");
    defer section.close();

    try shell.pushd("./src/clients/java");
    defer shell.popd();

    const java_version = shell.exec_stdout("java --version", .{}) catch {
        fatal("can't find java", .{});
    };
    log.info("java version {s}", .{java_version});

    try backup_create(shell.cwd, "pom.xml");
    defer backup_restore(shell.cwd, "pom.xml");

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
        shell.cwd,
        try shell.print("target/tigerbeetle-java-{s}.jar", .{info.version}),
        dist_dir,
        try shell.print("tigerbeetle-java-{s}.jar", .{info.version}),
    );
}

fn build_node(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build node");
    defer section.close();

    try shell.pushd("./src/clients/node");
    defer shell.popd();

    const node_version = shell.exec_stdout("node --version", .{}) catch {
        fatal("can't find nodejs", .{});
    };
    log.info("node version {s}", .{node_version});

    try backup_create(shell.cwd, "package.json");
    defer backup_restore(shell.cwd, "package.json");

    try backup_create(shell.cwd, "package-lock.json");
    defer backup_restore(shell.cwd, "package-lock.json");

    try shell.exec("npm version --no-git-tag-version {version}", .{ .version = info.version });
    try shell.exec("npm install", .{});
    try shell.exec("npm pack --quiet", .{});

    try Shell.copy_path(
        shell.cwd,
        try shell.print("tigerbeetle-node-{s}.tgz", .{info.version}),
        dist_dir,
        try shell.print("tigerbeetle-node-{s}.tgz", .{info.version}),
    );
}

fn publish(shell: *Shell, languages: LanguageSet, info: VersionInfo) !void {
    var section = try shell.open_section("publish all");
    defer section.close();

    assert(try shell.dir_exists("dist"));

    if (languages.contains(.zig)) {
        _ = try shell.env_get("GITHUB_TOKEN");
        const gh_version = shell.exec_stdout("gh --version", .{}) catch {
            fatal("can't find gh", .{});
        };
        log.info("gh version {s}", .{gh_version});

        const full_changelog = try shell.project_root.readFileAlloc(
            shell.arena.allocator(),
            "CHANGELOG.md",
            1024 * 1024,
        );

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
            \\* Docker (debug image): `docker pull ghcr.io/tigerbeetle/tigerbeetle:{[version]s}-debug`
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
            \\
            \\## Changelog
            \\
            \\{[changelog]s}
        , .{
            .version = info.version,
            .changelog = latest_changelog_entry(full_changelog),
        });

        try shell.exec(
            \\gh release create --draft
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

    if (languages.contains(.docker)) try publish_docker(shell, info);
    if (languages.contains(.dotnet)) try publish_dotnet(shell, info);
    if (languages.contains(.go)) try publish_go(shell, info);
    if (languages.contains(.java)) try publish_java(shell, info);
    if (languages.contains(.node)) {
        try publish_node(shell, info);
        // Our docs are build with node, so publish the docs together with the node package.
        try publish_docs(shell, info);
    }

    if (languages.contains(.zig)) {
        try shell.exec(
            \\gh release edit --draft=false --latest=true
            \\  {tag}
        , .{ .tag = info.version });
    }
}

fn latest_changelog_entry(changelog: []const u8) []const u8 {
    // Extract the first entry between two `## ` headers, excluding the header itself
    const changelog_with_header = stdx.cut(stdx.cut(changelog, "\n## ").?.suffix, "\n## ").?.prefix;
    return stdx.cut(changelog_with_header, "\n\n").?.suffix;
}

test latest_changelog_entry {
    const changelog =
        \\# TigerBeetle Changelog
        \\
        \\## 2023-10-23
        \\
        \\This is the start of the changelog.
        \\
        \\### Features
        \\
        \\
        \\## 1970-01-01
        \\
        \\ The beginning.
        \\
    ;
    try std.testing.expectEqualStrings(latest_changelog_entry(changelog),
        \\This is the start of the changelog.
        \\
        \\### Features
        \\
        \\
    );
}

fn publish_dotnet(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish dotnet");
    defer section.close();

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
    var section = try shell.open_section("publish go");
    defer section.close();

    assert(try shell.dir_exists("dist/go"));

    const token = try shell.env_get("TIGERBEETLE_GO_PAT");
    try shell.exec(
        \\git clone --no-checkout --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/tigerbeetle-go.git tigerbeetle-go
    , .{ .token = token });
    defer {
        shell.project_root.deleteTree("tigerbeetle-go") catch {};
    }

    const dist_files = try shell.find(.{ .where = &.{"dist/go"} });
    assert(dist_files.len > 10);
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

    try shell.pushd("./tigerbeetle-go");
    defer shell.popd();

    try shell.exec("git add .", .{});
    // Native libraries are ignored in this repository, but we want to push them to the
    // tigerbeetle-go one!
    try shell.exec("git add --force pkg/native", .{});

    try shell.env.put("GIT_AUTHOR_NAME", "TigerBeetle Bot");
    try shell.env.put("GIT_AUTHOR_EMAIL", "bot@tigerbeetle.com");
    try shell.env.put("GIT_COMMITTER_NAME", "TigerBeetle Bot");
    try shell.env.put("GIT_COMMITTER_EMAIL", "bot@tigerbeetle.com");
    try shell.exec("git commit --message {message}", .{
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
    var section = try shell.open_section("publish java");
    defer section.close();

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
        \\  versions:set -DnewVersion={version}
    , .{ .version = info.version });

    try shell.exec(
        \\mvn --batch-mode --quiet --file src/clients/java/pom.xml
        \\  -Dmaven.test.skip -Djacoco.skip
        \\  deploy
    , .{});
}

fn publish_node(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish node");
    defer section.close();

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

fn publish_docker(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish docker");
    defer section.close();

    assert(try shell.dir_exists("dist/tigerbeetle"));

    try shell.exec(
        \\docker login --username tigerbeetle --password {password} ghcr.io
    , .{
        .password = try shell.env_get("GITHUB_TOKEN"),
    });

    try shell.exec(
        \\docker buildx create --use
    , .{});

    for ([_]bool{ true, false }) |debug| {
        const triples = [_][]const u8{ "aarch64-linux", "x86_64-linux" };
        const docker_arches = [_][]const u8{ "arm64", "amd64" };
        for (triples, docker_arches) |triple, docker_arch| {
            // We need to unzip binaries from dist. For simplicity, don't bother with a temporary
            // directory.
            shell.project_root.deleteFile("tigerbeetle") catch {};
            try shell.exec("unzip ./dist/tigerbeetle/tigerbeetle-{triple}{debug}.zip", .{
                .triple = triple,
                .debug = if (debug) "-debug" else "",
            });
            try shell.project_root.rename(
                "tigerbeetle",
                try shell.print("tigerbeetle-{s}", .{docker_arch}),
            );
        }
        try shell.exec(
            \\docker buildx build --file tools/docker/Dockerfile . --platform linux/amd64,linux/arm64
            \\   --tag ghcr.io/tigerbeetle/tigerbeetle:{version}{debug}
            \\   {tag_latest}
            \\   --push
        , .{
            .version = info.version,
            .debug = if (debug) "-debug" else "",
            .tag_latest = @as(
                []const []const u8,
                if (debug) &.{} else &.{ "--tag", "ghcr.io/tigerbeetle/tigerbeetle:latest" },
            ),
        });

        // Sadly, there isn't an easy way to locally build & test a multiplatform image without
        // pushing it out to the registry first. As docker testing isn't covered under not rocket
        // science rule, let's do a best effort after-the-fact testing here.
        const version_verbose = try shell.exec_stdout(
            \\docker run ghcr.io/tigerbeetle/tigerbeetle:{version}{debug} version --verbose
        , .{
            .version = info.version,
            .debug = if (debug) "-debug" else "",
        });
        const mode = if (debug) "Debug" else "ReleaseSafe";
        assert(std.mem.indexOf(u8, version_verbose, mode) != null);
        assert(std.mem.indexOf(u8, version_verbose, info.version) != null);
    }
}

fn publish_docs(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish docs");
    defer section.close();

    {
        try shell.pushd("./src/docs_website");
        defer shell.popd();

        try shell.exec("npm install", .{});
        try shell.exec("npm run build", .{});
    }

    const token = try shell.env_get("TIGERBEETLE_DOCS_PAT");
    try shell.exec(
        \\git clone --no-checkout --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/docs.git tigerbeetle-docs
    , .{ .token = token });
    defer {
        shell.project_root.deleteTree("tigerbeetle-docs") catch {};
    }

    const docs_files = try shell.find(.{ .where = &.{"src/docs_website/build"} });
    assert(docs_files.len > 10);
    for (docs_files) |file| {
        try Shell.copy_path(
            shell.project_root,
            file,
            shell.project_root,
            try std.mem.replaceOwned(
                u8,
                shell.arena.allocator(),
                file,
                "src/docs_website/build",
                "tigerbeetle-docs/",
            ),
        );
    }

    try shell.pushd("./tigerbeetle-docs");
    defer shell.popd();

    try shell.exec("git add .", .{});
    try shell.env.put("GIT_AUTHOR_NAME", "TigerBeetle Bot");
    try shell.env.put("GIT_AUTHOR_EMAIL", "bot@tigerbeetle.com");
    try shell.env.put("GIT_COMMITTER_NAME", "TigerBeetle Bot");
    try shell.env.put("GIT_COMMITTER_EMAIL", "bot@tigerbeetle.com");
    // We want to push a commit even if there are no changes to the docs, to make sure
    // that the latest commit message on the docs repo points to the latest tigerbeetle
    // release.
    try shell.exec("git commit --allow-empty --message {message}", .{
        .message = try shell.print(
            "Autogenerated commit from tigerbeetle/tigerbeetle@{s}",
            .{info.sha},
        ),
    });

    try shell.exec("git push origin main", .{});
}

fn backup_create(dir: std.fs.Dir, comptime file: []const u8) !void {
    try Shell.copy_path(dir, file, dir, file ++ ".backup");
}

fn backup_restore(dir: std.fs.Dir, comptime file: []const u8) void {
    dir.deleteFile(file) catch {};
    Shell.copy_path(dir, file ++ ".backup", dir, file) catch {};
    dir.deleteFile(file ++ ".backup") catch {};
}
