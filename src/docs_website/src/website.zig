const std = @import("std");
const LazyPath = std.Build.LazyPath;
const Compile = std.Build.Step.Compile;

pub const Website = @This();

pub const file_size_max = 1 << 20; // 1 MiB

url_prefix: []const u8,
pandoc_bin: LazyPath,

page_writer_exe: *Compile,

pub fn init(b: *std.Build, url_prefix: []const u8, pandoc_bin: LazyPath) Website {
    return .{
        .url_prefix = url_prefix,
        .pandoc_bin = pandoc_bin,
        .page_writer_exe = b.addExecutable(.{
            .name = "page_writer",
            .root_source_file = b.path("src/page_writer.zig"),
            .target = b.graph.host,
        }),
    };
}

pub fn write_page(self: Website, options: struct {
    title: []const u8 = "TigerBeetle",
    author: []const u8 = "TigerBeetle Team",
    page_path: []const u8,
    include_search: bool = true,
    nav: []const u8,
    content: LazyPath,
}) LazyPath {
    const b = self.page_writer_exe.step.owner;
    const page_writer_run = b.addRunArtifact(self.page_writer_exe);
    page_writer_run.addArgs(&.{
        options.title,
        options.author,
        self.url_prefix,
        options.page_path,
        if (options.include_search) "true" else "false",
        options.nav,
    });
    page_writer_run.addFileArg(options.content);
    return page_writer_run.addOutputFileArg("page.html");
}
