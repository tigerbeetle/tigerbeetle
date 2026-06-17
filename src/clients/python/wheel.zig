//! Create a tigerbeetle Python wheel.
//! cwd must be src/clients/python.

const std = @import("std");
const stdx = @import("stdx");

const file_size_max = 10 * 1024 * 1024;

const metadata_header =
    \\Metadata-Version: 2.4
    \\Name: tigerbeetle
    \\Version: {s}
    \\Summary: The TigerBeetle client for Python.
    \\Project-URL: Homepage, https://github.com/tigerbeetle/tigerbeetle
    \\Project-URL: Issues, https://github.com/tigerbeetle/tigerbeetle/issues
    \\Classifier: Development Status :: 5 - Production/Stable
    \\Classifier: License :: OSI Approved :: Apache Software License
    \\Classifier: Operating System :: MacOS :: MacOS X
    \\Classifier: Operating System :: Microsoft :: Windows
    \\Classifier: Operating System :: POSIX :: Linux
    \\Classifier: Programming Language :: Python :: 3
    \\Classifier: Topic :: Database :: Front-Ends
    \\Requires-Python: >=3.7
    \\Description-Content-Type: text/markdown
    \\
    \\
;
const readme = @embedFile("README.md");

const wheel_content =
    \\Wheel-Version: 1.0
    \\Generator: tigerbeetle/src/scripts/release.zig
    \\Root-Is-Purelib: true
    \\Tag: py3-none-any
    \\
;

// base64 of a 32 byte sha256 digest is always 43 chars.
const sha256_base64_length = 43;

const Entry = struct {
    archive_name: []const u8,
    local_header_offset: u32,
    crc32: u32,
    compressed_size: u32,
    uncompressed_size: u32,
    sha256_base64: [sha256_base64_length]u8,
};

pub fn make(
    shell: *stdx.Shell,
    tag: []const u8,
    commit_timestamp: stdx.InstantUnix,
    output_path: []const u8,
) !void {
    const arena = shell.arena.allocator();
    const dos_timestamp = stdx.Shell.unix_to_dos_timestamp(commit_timestamp);

    if (std.fs.path.dirname(output_path)) |path| try shell.cwd.makePath(path);
    const output_file = try shell.cwd.createFile(output_path, .{});
    defer output_file.close();

    var buffered_writer = std.io.bufferedWriter(output_file.writer());
    const writer = buffered_writer.writer();

    var metadata_buffer = std.ArrayList(u8).init(arena);
    try metadata_buffer.writer().print(metadata_header, .{tag});
    try metadata_buffer.appendSlice(readme);
    const metadata = metadata_buffer.items;

    var package_dir = try shell.cwd.openDir("src/tigerbeetle", .{ .iterate = true });
    defer package_dir.close();

    var walker = try package_dir.walk(arena);
    defer walker.deinit();

    var file_paths = std.ArrayList([]const u8).init(arena);
    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        try file_paths.append(try arena.dupe(u8, entry.path));
    }
    // Sort files for reproducibility.
    std.mem.sort([]const u8, file_paths.items, {}, string_less_than);

    var offset: u32 = 0;
    var entries = std.ArrayList(Entry).init(arena);

    for (file_paths.items) |relative_path| {
        const archive_name = try shell.fmt("tigerbeetle/{s}", .{relative_path});
        const data = try package_dir.readFileAlloc(arena, relative_path, file_size_max);
        try add_entry(arena, &entries, writer, &offset, archive_name, data, dos_timestamp);
    }

    const dist_info = try shell.fmt("tigerbeetle-{s}.dist-info", .{tag});

    const metadata_name = try shell.fmt("{s}/METADATA", .{dist_info});
    try add_entry(arena, &entries, writer, &offset, metadata_name, metadata, dos_timestamp);

    const wheel_name = try shell.fmt("{s}/WHEEL", .{dist_info});
    try add_entry(arena, &entries, writer, &offset, wheel_name, wheel_content, dos_timestamp);

    // Build RECORD: all prior entries with sha256 hashes, then RECORD itself with empty fields.
    const record_name = try shell.fmt("{s}/RECORD", .{dist_info});
    var record_buffer = std.ArrayList(u8).init(arena);
    for (entries.items) |entry| {
        try record_buffer.writer().print("{s},sha256={s},{d}\n", .{
            entry.archive_name, entry.sha256_base64, entry.uncompressed_size,
        });
    }
    try record_buffer.writer().print("{s},,\n", .{record_name});
    try add_entry(
        arena,
        &entries,
        writer,
        &offset,
        record_name,
        record_buffer.items,
        dos_timestamp,
    );

    // Write central directory.
    const central_directory_offset = offset;
    for (entries.items) |entry| {
        const central_directory_header: std.zip.CentralDirectoryFileHeader = .{
            .signature = std.zip.central_file_header_sig,
            .version_made_by = 0,
            .version_needed_to_extract = 20,
            .flags = @bitCast(@as(u16, 0)),
            .compression_method = .deflate,
            .last_modification_time = dos_timestamp.time,
            .last_modification_date = dos_timestamp.date,
            .crc32 = entry.crc32,
            .compressed_size = entry.compressed_size,
            .uncompressed_size = entry.uncompressed_size,
            .filename_len = @intCast(entry.archive_name.len),
            .extra_len = 0,
            .comment_len = 0,
            .disk_number = 0,
            .internal_file_attributes = 0,
            .external_file_attributes = 0,
            .local_file_header_offset = entry.local_header_offset,
        };
        try writer.writeStructEndian(central_directory_header, .little);
        try writer.writeAll(entry.archive_name);
        offset += @intCast(@sizeOf(std.zip.CentralDirectoryFileHeader) + entry.archive_name.len);
    }

    const end_record: std.zip.EndRecord = .{
        .signature = std.zip.end_record_sig,
        .disk_number = 0,
        .central_directory_disk_number = 0,
        .record_count_disk = @intCast(entries.items.len),
        .record_count_total = @intCast(entries.items.len),
        .central_directory_size = offset - central_directory_offset,
        .central_directory_offset = central_directory_offset,
        .comment_len = 0,
    };
    try writer.writeStructEndian(end_record, .little);

    try buffered_writer.flush();
}

fn add_entry(
    arena: std.mem.Allocator,
    entries: *std.ArrayList(Entry),
    writer: anytype,
    offset: *u32,
    archive_name: []const u8,
    data: []const u8,
    dos_timestamp: stdx.Shell.DOSTimestamp,
) !void {
    const crc32 = std.hash.Crc32.hash(data);

    var compressed_buffer = std.ArrayList(u8).init(arena);
    var compressor = try std.compress.flate.compressor(compressed_buffer.writer(), .{});
    try compressor.writer().writeAll(data);
    try compressor.finish();
    const compressed = compressed_buffer.items;

    var sha256_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data, &sha256_digest, .{});
    var sha256_base64_buffer: [sha256_base64_length]u8 = undefined;
    _ = std.base64.url_safe_no_pad.Encoder.encode(&sha256_base64_buffer, &sha256_digest);

    const local_header_offset = offset.*;

    const local_header: std.zip.LocalFileHeader = .{
        .signature = std.zip.local_file_header_sig,
        .version_needed_to_extract = 20,
        .flags = @bitCast(@as(u16, 0)),
        .compression_method = .deflate,
        .last_modification_time = dos_timestamp.time,
        .last_modification_date = dos_timestamp.date,
        .crc32 = crc32,
        .compressed_size = @intCast(compressed.len),
        .uncompressed_size = @intCast(data.len),
        .filename_len = @intCast(archive_name.len),
        .extra_len = 0,
    };
    try writer.writeStructEndian(local_header, .little);
    try writer.writeAll(archive_name);
    try writer.writeAll(compressed);

    offset.* += @intCast(@sizeOf(std.zip.LocalFileHeader) + archive_name.len + compressed.len);

    try entries.append(.{
        .archive_name = archive_name,
        .local_header_offset = local_header_offset,
        .crc32 = crc32,
        .compressed_size = @intCast(compressed.len),
        .uncompressed_size = @intCast(data.len),
        .sha256_base64 = sha256_base64_buffer,
    });
}

fn string_less_than(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}
