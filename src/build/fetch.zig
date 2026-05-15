const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const stdb = @import("./stdb.zig");

const log = std.log;

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main() !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();

    const args = try std.process.argsAlloc(arena);
    assert(args.len == 6 or args.len == 7);

    _, const zig, const global_cache, const url, const file_name, const out = args[0..6].*;
    const hash_optional = if (args.len == 7) args[6] else null;
    assert(args.len <= 7);

    if (hash_optional) |hash| {
        // Fast path --- don't touch the Internet if we have the hash locally.
        const cached = path_join(arena, &.{ global_cache, "p", hash, file_name });
        if (std.fs.cwd().copyFile(cached, std.fs.cwd(), out, .{})) {
            log.debug("download skipped: cache hit", .{});
            return;
        } else |_| { // Time to ask for forgiveness!
            log.debug("download: cache miss", .{});
        }
    } else {
        log.debug("download: no hash", .{});
    }

    const hash = try fetch(arena, .{
        .zig = zig,
        .tmp = path_join(arena, &.{ global_cache, "tmp" }),
        .url = url,
    });

    if (hash_optional) |hash_specified| {
        if (!std.mem.eql(u8, hash, hash_specified)) {
            log.err(
                \\bad hash
                \\specified: {s}
                \\fetched:   {s}
                \\
            , .{ hash_specified, hash });
            return error.BadHash;
        }
    }

    const cached = path_join(arena, &.{ global_cache, "p", hash, file_name });
    errdefer log.err("copying from {s}", .{cached});

    try std.fs.cwd().copyFile(cached, std.fs.cwd(), out, .{});
}

/// If curl is available, use it for robust downloads, and then
/// `zig fetch` a local file to get the hash. Otherwise, fetch
/// the url directly.
fn fetch(arena: Allocator, options: struct {
    zig: []const u8,
    tmp: []const u8,
    url: []const u8,
}) ![]const u8 {
    if (stdb.exec_ok(arena, &.{ "curl", "--version" })) {
        log.debug("download: curl", .{});
        const url_file_name = options.url[std.mem.lastIndexOf(u8, options.url, "/").?..];
        const tmp_dir = path_join(arena, &.{
            options.tmp,
            &std.fmt.bytesToHex(std.mem.asBytes(&std.crypto.random.int(u64)), .lower),
        });
        defer std.fs.cwd().deleteTree(tmp_dir) catch {};

        try std.fs.cwd().makePath(tmp_dir);

        const curl_output = path_join(arena, &.{ tmp_dir, url_file_name });
        _ = try stdb.exec(arena, &(.{
            "curl",             "--retry-all-errors",
            "--retry",          "5",
            "--retry-max-time", "120",
            "--retry-delay",    "30",
            "--location",       options.url,
            "--output",         curl_output,
        }));
        return try stdb.exec(arena, &.{ options.zig, "fetch", curl_output });
    }
    log.debug("download: zig fetch", .{});
    return try stdb.exec(arena, &.{ options.zig, "fetch", options.url });
}

fn path_join(arena: Allocator, components: []const []const u8) []const u8 {
    return std.fs.path.join(arena, components) catch |err| oom(err);
}

pub fn oom(_: error{OutOfMemory}) noreturn {
    @panic("OOM");
}
