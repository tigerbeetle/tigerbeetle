const std = @import("std");

pub fn main() !void {
    var args = std.process.args();
    _ = args.skip();

    var file_buffer: [150 * 1024]u8 = undefined;
    var stdout = std.io.getStdOut().writer();

    while (args.next()) |page_path| {
        const html_path = args.next().?;
        const html = try read_file(std.fs.cwd(), html_path, &file_buffer);

        // Clear links following h1 tags.
        var clear_h1_link = false;

        var it = AttributeIterator.init(html);
        while (it.next()) |attribute| {
            try stdout.writeAll(attribute.prefix);

            if (std.mem.eql(u8, attribute.tag, "use")) {
                // Don't alter.
                try stdout.writeAll(attribute.value);
            } else {
                // Rewrite attribute values.
                const is_h1 = std.mem.eql(u8, attribute.tag, "h1");
                defer clear_h1_link = is_h1;

                const value = if (is_h1 or clear_h1_link) "" else attribute.value;
                try switch (attribute.typ) {
                    .link => rewrite_link(value, page_path, stdout),
                    .id => rewrite_id(value, page_path, stdout),
                };
            }
        }
        const remaining = it.html[it.position..];
        try stdout.print("{s}\n", .{remaining});
    }
}

fn rewrite_link(link: []const u8, page_path: []const u8, writer: anytype) !void {
    if (std.mem.startsWith(u8, link, "http") // external link.
    or std.mem.startsWith(u8, link, "mailto:") // email.
    or std.mem.startsWith(u8, link, "#cb") // code block link.
    ) {
        return try writer.writeAll(link);
    }

    var base: []const u8 = link;
    var fragment: ?[]const u8 = null;
    if (std.mem.lastIndexOfScalar(u8, link, '#')) |index| {
        base = link[0..index];
        fragment = link[index + 1 ..];
    }

    var buffer: [200]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buffer);
    const allocator = fba.allocator();

    var path = if (base.len > 0 and base[0] == '/')
        base[1..]
    else
        try std.fs.path.resolvePosix(allocator, &.{ page_path, base });
    if (std.mem.eql(u8, path, ".")) path = "";
    path = std.mem.trimRight(u8, path, "/");

    const slug = try path2slug(allocator, path);
    if (fragment) |frag| {
        if (slug.len > 0) {
            try writer.print("#{s}-{s}", .{ slug, frag });
        } else {
            try writer.print("#{s}", .{frag});
        }
    } else {
        try writer.print("#{s}", .{slug});
    }
}

fn rewrite_id(id: []const u8, page_path: []const u8, writer: anytype) !void {
    if (std.mem.startsWith(u8, id, "cb") // code block lines.
    ) {
        return try writer.writeAll(id);
    }

    if (page_path.len > 0) {
        var buffer: [200]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buffer);
        const allocator = fba.allocator();
        const slug = try path2slug(allocator, page_path);
        if (id.len > 0) {
            try writer.print("{s}-{s}", .{ slug, id });
        } else {
            try writer.writeAll(slug);
        }
    } else {
        try writer.writeAll(id);
    }
}

fn path2slug(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    const slug = try allocator.dupe(u8, path);
    std.mem.replaceScalar(u8, slug, '/', '-');
    return slug;
}

const AttributeIterator = struct {
    const Attribute = struct {
        const Type = enum { link, id };

        typ: Type,
        tag: []const u8,
        prefix: []const u8,
        value: []const u8,
    };

    const link_prefix = "href=\"";
    const id_prefix = "id=\"";

    html: []const u8,
    position: usize = 0,

    fn init(html: []const u8) AttributeIterator {
        return .{ .html = html };
    }

    fn next(self: *AttributeIterator) ?Attribute {
        var first_index: ?usize = null;
        var typ: Attribute.Type = undefined;

        // Find the first link or id attribute.
        const remaining = self.html[self.position..];
        if (std.mem.indexOf(u8, remaining, link_prefix)) |link_index| {
            first_index = link_index + link_prefix.len;
            typ = .link;
        }
        if (std.mem.indexOf(u8, remaining, id_prefix)) |id_index| {
            if (first_index) |index| {
                if (id_index < index) {
                    first_index = id_index + id_prefix.len;
                    typ = .id;
                }
            } else {
                first_index = id_index + id_prefix.len;
                typ = .id;
            }
        }

        const value_start = first_index orelse return null;
        const value_len = std.mem.indexOfScalar(u8, remaining[value_start..], '"').?;

        const prefix = remaining[0..value_start];
        const value = remaining[value_start..][0..value_len];
        self.position += value_start + value_len;

        // Find the associated tag.
        const tag_slice = self.html[0 .. self.position - value_len];
        const tag_index = std.mem.lastIndexOfScalar(u8, tag_slice, '<').? + 1;
        const tag_len = std.mem.indexOfAny(u8, tag_slice[tag_index..], " \n").?;
        const tag = tag_slice[tag_index..][0..tag_len];

        return .{ .typ = typ, .tag = tag, .prefix = prefix, .value = value };
    }
};

fn read_file(dir: std.fs.Dir, path: []const u8, page_buffer: []u8) ![]const u8 {
    const result = try dir.readFile(path, page_buffer);
    if (result.len == page_buffer.len) return error.FileToLarge;
    return result;
}
