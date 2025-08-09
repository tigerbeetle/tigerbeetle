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
    hide_stdout(check_spelling);
    check_spelling.addFileArg(vale_bin);
    const md_files = b.run(&.{ "git", "ls-files", "../../**/*.md" });
    var md_files_iter = std.mem.tokenizeScalar(u8, md_files, '\n');
    while (md_files_iter.next()) |md_file| {
        check_spelling.addFileArg(b.path(md_file));
    }

    const content = b.addWriteFiles();
    { //TODO(zig): https://github.com/ziglang/zig/issues/20571
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

    const clean_zigout_step = b.addRemoveDirTree(b.path("zig-out"));

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

// Hide step's stdout unless it fails. Sadly, this requires overriding Build.Step.Run make function.
fn hide_stdout(run: *std.Build.Step.Run) void {
    _ = run.captureStdOut();

    const override = struct {
        var global_map: std.AutoHashMapUnmanaged(usize, std.Build.Step.MakeFn) = .{};

        fn make(step: *std.Build.Step, options: std.Build.Step.MakeOptions) anyerror!void {
            const original = global_map.get(@intFromPtr(step)).?;
            original(step, options) catch |err| {
                const run_step: *std.Build.Step.Run = @fieldParentPtr("step", step);
                if (run_step.captured_stdout) |output| {
                    const file = try std.fs.cwd().openFile(output.generated_file.getPath(), .{});
                    defer file.close();

                    const stdout = try file.readToEndAlloc(step.owner.allocator, 100 * 1024);
                    std.debug.print("{s}\n", .{stdout});
                }
                return err;
            };
        }
    };

    const original = run.step.makeFn;
    const b = run.step.owner;
    override.global_map.put(b.allocator, @intFromPtr(&run.step), original) catch @panic("OOM");
    run.step.makeFn = override.make;
}
