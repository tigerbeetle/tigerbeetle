//! Orchestrates building a distribution of tigerbeetle --- a collection of (source and binary)
//! artifacts which constitutes a release and which we upload to various registries.
//!
//! Concretely, the artifacts are:
//!
//! - TigerBeetle binary build for all supported architectures
//! - TigerBeetle clients build for all supported architectures and languages
//! - Source code archive
//! - Release meta information and changelog
//!
//! This is implemented as a stand-alone zig script, rather as a step in build.zig, because this is
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

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const gpa = gpa_allocator.allocator();
    var arena_allocator = std.heap.ArenaAllocator.init(gpa);
    defer arena_allocator.deinit();

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    try build_dist(shell);
}

fn build_dist(shell: *Shell) !void {
    try shell.project_root.deleteTree("dist");
    var dist_dir = try shell.project_root.makeOpenPath("dist", .{});
    defer dist_dir.close();

    var timer_total = try std.time.Timer.start();
    log.info("building TigerBeetle distribution into {s}", .{
        try dist_dir.realpathAlloc(shell.arena.allocator(), "."),
    });

    var dist_dir_tigerbeetle = try dist_dir.makeOpenPath("tigerbeetle", .{});
    defer dist_dir_tigerbeetle.close();

    var dist_dir_node = try dist_dir.makeOpenPath("node", .{});
    defer dist_dir_node.close();

    var dist_dir_java = try dist_dir.makeOpenPath("java", .{});
    defer dist_dir_java.close();

    var dist_dir_dotnet = try dist_dir.makeOpenPath("dotnet", .{});
    defer dist_dir_dotnet.close();

    var dist_dir_go = try dist_dir.makeOpenPath("go", .{});
    defer dist_dir_go.close();

    var timer = try std.time.Timer.start();

    try build_dist_tigerbeetle(shell, dist_dir_tigerbeetle);
    const elapsed_tigerbeetle_ns = timer.lap();

    try build_dist_node(shell, dist_dir_node);
    const elapsed_node_ns = timer.lap();

    try build_dist_java(shell, dist_dir_java);
    const elapsed_java_ns = timer.lap();

    try build_dist_dotnet(shell, dist_dir_dotnet);
    const elapsed_dotnet_ns = timer.lap();

    try build_dist_go(shell, dist_dir_go);
    const elapsed_go_ns = timer.lap();

    const elapsed_total_ns = timer_total.lap();
    log.info(
        \\build distribution in {d}
        \\  tigerbeetle {d}
        \\  node        {d}
        \\  java        {d}
        \\  dotnet      {d}
        \\  go          {d}
    , .{
        std.fmt.fmtDuration(elapsed_total_ns),
        std.fmt.fmtDuration(elapsed_tigerbeetle_ns),
        std.fmt.fmtDuration(elapsed_node_ns),
        std.fmt.fmtDuration(elapsed_java_ns),
        std.fmt.fmtDuration(elapsed_dotnet_ns),
        std.fmt.fmtDuration(elapsed_go_ns),
    });
}

fn build_dist_tigerbeetle(shell: *Shell, dist_dir: std.fs.Dir) !void {
    log.info("building tigerbeetle", .{});

    try shell.project_root.setAsCwd();

    const version = shell.exec_stdout("llvm-lipo -version", .{}) catch {
        fatal("can't find llvm-lipo", .{});
    };
    log.info("llvm-lipo version {s}", .{version});

    const targets = .{ "aarch64-linux", "x86_64-linux", "x86_64-windows" };
    const cpus = .{ "baseline+aes+neon", "x86_64_v3+aes", "x86_64_v3+aes" };
    inline for (.{ false, true }) |debug| {
        const mode = if (debug) " -Doptimize=Debug" else " -Doptimize=ReleaseSafe";
        inline for (targets, cpus) |target, cpu| {
            try shell.zig("build install -Dtarget=" ++ target ++ " -Dcpu=" ++ cpu ++ mode, .{});
            try Shell.copy_path(shell.project_root, "tigerbeetle", dist_dir, "tigerbeetle-" ++ target ++ if (debug) "-debug" else "");
        }

        try shell.zig("build install -Dtarget=aarch64-macos -Dcpu=baseline+aes+neon" ++ mode, .{});
        try Shell.copy_path(shell.project_root, "tigerbeetle", shell.project_root, "tigerbeetle-aarch64");

        try shell.zig("build install -Dtarget=x86_64-macos  -Dcpu=x86_64_v3+aes" ++ mode, .{});
        try Shell.copy_path(shell.project_root, "tigerbeetle", shell.project_root, "tigerbeetle-x86_64");

        try shell.exec("llvm-lipo tigerbeetle-aarch64 tigerbeetle-x86_64 -create -output tigerbeetle", .{});
        try shell.project_root.deleteFile("tigerbeetle-aarch64");
        try shell.project_root.deleteFile("tigerbeetle-x86_64");
        try Shell.copy_path(shell.project_root, "tigerbeetle", dist_dir, "tigerbeetle-universal-macos" ++ if (debug) "-debug" else "");
    }
}

fn build_dist_node(shell: *Shell, dist_dir: std.fs.Dir) !void {
    log.info("building node client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/node", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const version = shell.exec_stdout("node --version", .{}) catch {
        fatal("can't find nodejs", .{});
    };
    log.info("node version {s}", .{version});

    try shell.exec("npm pack --quiet", .{});
    try client_src_dir.copyFile(
        "tigerbeetle-node-0.12.0.tgz",
        dist_dir,
        "tigerbeetle-node-0.12.0.tgz",
        .{},
    );
}

fn build_dist_java(shell: *Shell, dist_dir: std.fs.Dir) !void {
    log.info("building java client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/java", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const version = shell.exec_stdout("java --version", .{}) catch {
        fatal("can't find java", .{});
    };
    log.info("java version {s}", .{version});

    try shell.exec("mvn -B package --quiet --file pom.xml", .{});
    try client_src_dir.copyFile(
        "target/tigerbeetle-java-0.0.1-SNAPSHOT.jar",
        dist_dir,
        "tigerbeetle-java-0.0.1-SNAPSHOT.jar",
        .{},
    );
}

fn build_dist_dotnet(shell: *Shell, dist_dir: std.fs.Dir) !void {
    log.info("building dotnet client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/dotnet", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const version = shell.exec_stdout("dotnet --version", .{}) catch {
        fatal("can't find dotnet", .{});
    };
    log.info("dotnet version {s}", .{version});

    try shell.exec("dotnet pack TigerBeetle --configuration Release", .{});

    try client_src_dir.copyFile(
        "TigerBeetle/bin/Release/tigerbeetle.1.0.0.nupkg",
        dist_dir,
        "tigerbeetle.1.0.0.nupkg",
        .{},
    );
}

fn build_dist_go(shell: *Shell, dist_dir: std.fs.Dir) !void {
    log.info("building go client", .{});
    var client_src_dir = try shell.project_root.openDir("src/clients/go", .{});
    defer client_src_dir.close();

    try client_src_dir.setAsCwd();

    const version = shell.exec_stdout("go version", .{}) catch {
        fatal("can't find go", .{});
    };
    log.info("go version {s}", .{version});

    try shell.zig("build go_client -Doptimize=ReleaseSafe", .{});

    const files = try shell.exec_stdout("git ls-files", .{});
    var files_lines = std.mem.tokenize(u8, files, "\n");
    var copied_count: u32 = 0;
    while (files_lines.next()) |file| {
        assert(file.len > 3);
        if (std.fs.path.dirname(file)) |dir| {
            try dist_dir.makePath(dir);
        }
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
    assert(copied_count == 5);
}
