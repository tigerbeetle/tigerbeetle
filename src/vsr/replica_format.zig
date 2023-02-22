const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;
const format_wal_headers = @import("./journal.zig").format_wal_headers;
const format_wal_prepares = @import("./journal.zig").format_wal_prepares;

/// Initialize the TigerBeetle replica's data file.
pub fn format(
    comptime Storage: type,
    allocator: std.mem.Allocator,
    cluster: u32,
    replica: u8,
    storage: *Storage,
    superblock: *vsr.SuperBlockType(Storage),
) !void {
    const ReplicaFormat = ReplicaFormatType(Storage);
    var replica_format = ReplicaFormat{};

    try replica_format.format_wal(allocator, cluster, storage);
    assert(!replica_format.formatting);

    superblock.format(
        ReplicaFormat.format_superblock_callback,
        &replica_format.superblock_context,
        .{
            .cluster = cluster,
            .replica = replica,
        },
    );

    replica_format.formatting = true;
    while (replica_format.formatting) storage.tick();
}

fn ReplicaFormatType(comptime Storage: type) type {
    const SuperBlock = vsr.SuperBlockType(Storage);
    return struct {
        const Self = @This();

        formatting: bool = false,
        superblock_context: SuperBlock.Context = undefined,
        wal_write: Storage.Write = undefined,

        fn format_wal(
            self: *Self,
            allocator: std.mem.Allocator,
            cluster: u32,
            storage: *Storage,
        ) !void {
            const header_zeroes = [_]u8{0} ** @sizeOf(Header);
            const wal_write_size_max = 4 * 1024 * 1024;
            assert(wal_write_size_max % constants.sector_size == 0);

            // Direct I/O requires the buffer to be sector-aligned.
            var wal_buffer = try allocator.allocAdvanced(
                u8,
                constants.sector_size,
                wal_write_size_max,
                .exact,
            );
            defer allocator.free(wal_buffer);

            // The logical offset *within the Zone*.
            // Even though the prepare zone follows the redundant header zone, write the prepares
            // first. This allows the test Storage to check the invariant "never write the redundant
            // header before the prepare".
            var wal_offset: u64 = 0;
            while (wal_offset < constants.journal_size_prepares) {
                const size = format_wal_prepares(cluster, wal_offset, wal_buffer);
                assert(size > 0);

                for (std.mem.bytesAsSlice(Header, wal_buffer[0..size])) |*header| {
                    if (std.mem.eql(u8, std.mem.asBytes(header), &header_zeroes)) {
                        // This is the (empty) body of a reserved or root Prepare.
                    } else {
                        // This is a Prepare's header.
                        assert(header.valid_checksum());
                        if (header.op == 0) {
                            assert(header.command == .prepare);
                            assert(header.operation == .root);
                        } else {
                            assert(header.command == .reserved);
                            assert(header.operation == .reserved);
                        }
                    }
                }

                storage.write_sectors(
                    format_wal_sectors_callback,
                    &self.wal_write,
                    wal_buffer[0..size],
                    .wal_prepares,
                    wal_offset,
                );
                self.formatting = true;
                while (self.formatting) storage.tick();
                wal_offset += size;
            }
            // There are no prepares left to write.
            assert(format_wal_prepares(cluster, wal_offset, wal_buffer) == 0);

            wal_offset = 0;
            while (wal_offset < constants.journal_size_headers) {
                const size = format_wal_headers(cluster, wal_offset, wal_buffer);
                assert(size > 0);

                for (std.mem.bytesAsSlice(Header, wal_buffer[0..size])) |*header| {
                    assert(header.valid_checksum());
                    if (header.op == 0) {
                        assert(header.command == .prepare);
                        assert(header.operation == .root);
                    } else {
                        assert(header.command == .reserved);
                        assert(header.operation == .reserved);
                    }
                }

                storage.write_sectors(
                    format_wal_sectors_callback,
                    &self.wal_write,
                    wal_buffer[0..size],
                    .wal_headers,
                    wal_offset,
                );
                self.formatting = true;
                while (self.formatting) storage.tick();
                wal_offset += size;
            }
            // There are no headers left to write.
            assert(format_wal_headers(cluster, wal_offset, wal_buffer) == 0);
        }

        fn format_wal_sectors_callback(write: *Storage.Write) void {
            const self = @fieldParentPtr(Self, "wal_write", write);
            assert(self.formatting);
            self.formatting = false;
        }

        fn format_superblock_callback(superblock_context: *SuperBlock.Context) void {
            const self = @fieldParentPtr(Self, "superblock_context", superblock_context);
            assert(self.formatting);
            self.formatting = false;
        }
    };
}

test "format" {
    const superblock_zone_size = @import("./superblock.zig").superblock_zone_size;
    const data_file_size_min = @import("./superblock.zig").data_file_size_min;
    const MessagePool = @import("../message_pool.zig").MessagePool;
    const Storage = @import("../testing/storage.zig").Storage;
    const SuperBlock = vsr.SuperBlockType(Storage);
    const allocator = std.testing.allocator;
    const cluster = 0;
    const replica = 1;

    var storage = try Storage.init(
        allocator,
        superblock_zone_size + constants.journal_size_headers + constants.journal_size_prepares,
        .{
            .read_latency_min = 0,
            .read_latency_mean = 0,
            .write_latency_min = 0,
            .write_latency_mean = 0,
        },
    );
    defer storage.deinit(allocator);

    var message_pool = try MessagePool.init(allocator, .replica);
    defer message_pool.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = data_file_size_min,
        .message_pool = &message_pool,
    });
    defer superblock.deinit(allocator);

    try format(Storage, allocator, cluster, replica, &storage, &superblock);

    // Verify the superblock headers.
    var copy: u8 = 0;
    while (copy < constants.superblock_copies) : (copy += 1) {
        const superblock_header = storage.superblock_header(copy);

        try std.testing.expectEqual(superblock_header.copy, copy);
        try std.testing.expectEqual(superblock_header.replica, replica);
        try std.testing.expectEqual(superblock_header.cluster, cluster);
        try std.testing.expectEqual(superblock_header.storage_size, storage.size);
        try std.testing.expectEqual(superblock_header.sequence, 1);
        try std.testing.expectEqual(superblock_header.vsr_state.commit_min, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.commit_max, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.view, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.log_view, 0);
    }

    // Verify the WAL headers and prepares zones.
    assert(storage.wal_headers().len == storage.wal_headers().len);
    for (storage.wal_headers()) |header, slot| {
        const message = storage.wal_prepares()[slot];
        try std.testing.expect(std.meta.eql(header, message.header));

        try std.testing.expect(header.valid_checksum());
        try std.testing.expect(header.valid_checksum_body(&[0]u8{}));
        try std.testing.expectEqual(header.invalid(), null);
        try std.testing.expectEqual(header.cluster, cluster);
        try std.testing.expectEqual(header.op, slot);
        try std.testing.expectEqual(header.size, @sizeOf(vsr.Header));
        if (slot == 0) {
            try std.testing.expectEqual(header.command, .prepare);
            try std.testing.expectEqual(header.operation, .root);
        } else {
            try std.testing.expectEqual(header.command, .reserved);
        }
    }
}
