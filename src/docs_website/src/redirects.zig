const std = @import("std");

const Website = @import("website.zig").Website;

const Redirect = struct {
    old: []const u8,
    new: []const u8,

    const all: []const Redirect = &.{
        .{ .old = "quick-start/", .new = "start/" },
        .{ .old = "about/", .new = "concepts/" },
        .{ .old = "about/vopr/", .new = "concepts/safety/" },
        .{ .old = "about/oltp/", .new = "concepts/oltp/" },
    };
};

pub fn build(b: *std.Build, content: *std.Build.Step.WriteFile, website: Website) !void {
    for (Redirect.all) |redirect| {
        try build_redirect(b, content, website, redirect);
    }
}

fn build_redirect(
    b: *std.Build,
    content: *std.Build.Step.WriteFile,
    website: Website,
    redirect: Redirect,
) !void {
    const path = b.pathJoin(&.{ redirect.old, "index.html" });
    const url = b.fmt("{s}/{s}", .{ website.url_prefix, redirect.new });
    const html_redirect = b.fmt(
        \\<!DOCTYPE html>
        \\<html lang="en">
        \\  <meta charset="utf-8">
        \\  <title>Redirecting&hellip;</title>
        \\  <link rel="canonical" href="{[url]s}">
        \\  <script>location="{[url]s}"</script>
        \\  <meta http-equiv="refresh" content="0; url={[url]s}">
        \\  <meta name="robots" content="noindex">
        \\  <h1>Redirecting&hellip;</h1>
        \\  <a href="{[url]s}">Click here if you are not redirected.</a>
        \\</html>
        \\
    , .{ .url = url });

    _ = content.add(path, html_redirect);
}
