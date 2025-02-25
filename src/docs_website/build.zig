const std = @import("std");
const Website = @import("src/website.zig").Website;
const docs = @import("src/docs.zig");
const redirects = @import("src/redirects.zig");

pub const exclude_extensions: []const []const u8 = &.{
    ".DS_Store",
};

pub fn build(b: *std.Build) !void {
    const url_prefix: []const u8 = b.option(
        []const u8,
        "url_prefix",
        "Prefix links with this string",
    ) orelse "";

    const git_commit = b.option(
        []const u8,
        "git-commit",
        "The git commit revision of the source code.",
    ) orelse std.mem.trimRight(u8, b.run(&.{ "git", "rev-parse", "--verify", "HEAD" }), "\n");

    const pandoc_bin = get_pandoc_bin(b) orelse return;
    const vale_bin = get_vale_bin(b) orelse return;

    const check_spelling = std.Build.Step.Run.create(b, "run vale");
    check_spelling.addFileArg(vale_bin);
    const md_files = try exec_stdout(b.allocator, &.{ "git", "ls-files", "../../**/*.md" });
    var md_files_iter = std.mem.tokenize(u8, md_files, "\n");
    while (md_files_iter.next()) |md_file| {
        check_spelling.addArg(md_file);
    }

    const content = b.addWriteFiles();
    { //TODO(Zig 0.14.0): https://github.com/ziglang/zig/issues/20571
        var dir = try b.build_root.handle.openDir("assets", .{ .iterate = true });
        defer dir.close();

        var walker = try dir.walk(b.allocator);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind == .file) {
                if (std.mem.eql(u8, entry.basename, ".DS_Store")) continue;
                const source = b.path("assets").path(b, entry.path);
                _ = content.addCopyFile(source, entry.path);
            }
        }
    }

    content.step.dependOn(&check_spelling.step);

    const website = Website.init(b, url_prefix, pandoc_bin);
    try docs.build(b, content, website);
    try redirects.build(b, content, website);

    const clean_zigout_step = b.addRemoveDirTree("zig-out");

    const install_content_step = b.addInstallDirectory(.{
        .source_dir = content.getDirectory(),
        .install_dir = .prefix,
        .install_subdir = ".",
    });

    install_content_step.step.dependOn(&clean_zigout_step.step);

    const service_worker_writer = b.addRunArtifact(b.addExecutable(.{
        .name = "service_worker_writer",
        .root_source_file = b.path("src/service_worker_writer.zig"),
        .target = b.graph.host,
    }));
    service_worker_writer.addArgs(&.{ url_prefix, git_commit });
    service_worker_writer.addDirectoryArg(content.getDirectory());

    const service_worker = service_worker_writer.captureStdOut();

    const file_checker = b.addRunArtifact(b.addExecutable(.{
        .name = "file_checker",
        .root_source_file = b.path("src/file_checker.zig"),
        .target = b.graph.host,
    }));
    file_checker.addArg("zig-out");

    file_checker.step.dependOn(&install_content_step.step);
    file_checker.step.dependOn(&b.addInstallFile(service_worker, "service-worker.js").step);

    b.getInstallStep().dependOn(&file_checker.step);
}

fn get_pandoc_bin(b: *std.Build) ?std.Build.LazyPath {
    const host = b.graph.host.result;
    const name = switch (host.os.tag) {
        .linux => switch (host.cpu.arch) {
            .x86_64 => "pandoc_linux_amd64",
            else => @panic("unsupported cpu arch"),
        },
        .macos => switch (host.cpu.arch) {
            .aarch64 => "pandoc_macos_arm64",
            else => @panic("unsupported cpu arch"),
        },
        else => @panic("unsupported os"),
    };
    if (b.lazyDependency(name, .{})) |dep| {
        return dep.path("bin/pandoc");
    } else {
        return null;
    }
}

fn get_vale_bin(b: *std.Build) ?std.Build.LazyPath {
    const host = b.graph.host.result;
    const name = switch (host.os.tag) {
        .linux => switch (host.cpu.arch) {
            .x86_64 => "vale_linux_amd64",
            else => @panic("unsupported cpu arch"),
        },
        .macos => switch (host.cpu.arch) {
            .aarch64 => "vale_macos_arm64",
            else => @panic("unsupported cpu arch"),
        },
        else => @panic("unsupported os"),
    };
    if (b.lazyDependency(name, .{})) |dep| {
        return dep.path("vale");
    } else {
        return null;
    }
}

fn exec_stdout(allocator: std.mem.Allocator, argv: []const []const u8) ![]const u8 {
    var child = std.process.Child.init(argv, allocator);
    child.stdout_behavior = .Pipe;

    try child.spawn();
    const output = try child.stdout.?.readToEndAlloc(allocator, 10 * 1024);
    const term = try child.wait();
    std.debug.assert(term == .Exited);

    return output;
}
