const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;
const format_wal_headers = @import("./journal.zig").format_wal_headers;
const format_wal_prepares = @import("./journal.zig").format_wal_prepares;

/// Initialize the TigerBeetle replica's data file.
pub fn format(
    comptime Storage: type,
    allocator: std.mem.Allocator,
    options: vsr.SuperBlockType(Storage).FormatOptions,
    storage: *Storage,
    superblock: *vsr.SuperBlockType(Storage),
) !void {
    const ReplicaFormat = ReplicaFormatType(Storage);
    var replica_format = ReplicaFormat{};

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    try replica_format.queue_format_wal(arena.allocator(), options.cluster, storage);
    try replica_format.queue_format_replies(arena.allocator(), storage);
    try replica_format.queue_format_grid_padding(arena.allocator(), storage);

    replica_format.format_and_tick(storage, superblock, options);

    assert(!replica_format.formatting and !replica_format.formatting_superblock);
}

/// When formatting, we write:
/// * constants.journal_slot_count many prepares,
/// * 1 write that contains all of the headers,
/// * constants.clients_max many replies,
/// * 1 block for grid padding, maybe.
pub const writes_max = constants.journal_slot_count + 1 + constants.clients_max + 1;

fn ReplicaFormatType(comptime Storage: type) type {
    const SuperBlock = vsr.SuperBlockType(Storage);
    return struct {
        const ReplicaFormat = @This();
        var formating_instance: ?*ReplicaFormat = null;

        formatting: bool = false,
        formatting_superblock: bool = false,
        superblock_context: SuperBlock.Context = undefined,

        writes: [writes_max]Storage.Write = undefined,
        writes_pending: u64 = 0,

        /// Assume no padding for the grid (hence the -1). If padding is added, this will be
        /// incremented.
        writes_expected: u64 = writes_max - 1,

        fn queue_format_wal(
            self: *ReplicaFormat,
            arena: std.mem.Allocator,
            cluster: u128,
            storage: *Storage,
        ) !void {
            assert(!self.formatting and !self.formatting_superblock);

            // The logical offset *within the Zone*.
            // Even though the prepare zone follows the redundant header zone, write the prepares
            // first. This allows the test Storage to check the invariant "never write the redundant
            // header before the prepare".
            var wal_offset: u64 = 0;
            while (wal_offset < constants.journal_size_prepares) {
                // Direct I/O requires the buffer to be sector-aligned.
                const prepare_buffer_first_sector = try arena.alignedAlloc(
                    u8,
                    constants.sector_size,
                    constants.sector_size,
                );
                // Freed when the arena is cleaned up.

                const size = format_wal_prepares(cluster, wal_offset, prepare_buffer_first_sector);
                assert(size > 0);
                assert(!stdx.zeroed(prepare_buffer_first_sector[0..@sizeOf(Header.Prepare)]));
                assert(stdx.zeroed(prepare_buffer_first_sector[@sizeOf(Header.Prepare)..]));

                const header: *Header.Prepare = @ptrCast(prepare_buffer_first_sector);
                assert(header.valid_checksum());

                if (header.op == 0) {
                    assert(header.operation == .root);
                } else {
                    assert(header.operation == .reserved);
                }

                storage.write_sectors(
                    write_sectors_callback,
                    &self.writes[self.writes_pending],
                    prepare_buffer_first_sector[0..constants.sector_size],
                    .wal_prepares,
                    wal_offset,
                );
                self.writes_pending += 1;

                // Increment the offset by a full message_size_max, rather than the sector. This
                // skips having to write out and process ~1GiB of zeroes.
                wal_offset += constants.message_size_max;
            }
            // There are no prepares left to write.
            var wal_buffer_test: [constants.sector_size]u8 = undefined;
            assert(format_wal_prepares(cluster, wal_offset, &wal_buffer_test) == 0);

            wal_offset = 0;
            while (wal_offset < constants.journal_size_headers) {
                // Direct I/O requires the buffer to be sector-aligned.
                const wal_buffer = try arena.alignedAlloc(
                    u8,
                    constants.sector_size,
                    vsr.sector_ceil(constants.journal_size_headers),
                );
                // Freed when the arena is cleaned up.

                const size = format_wal_headers(cluster, wal_offset, wal_buffer);
                assert(size > 0);

                for (std.mem.bytesAsSlice(Header.Prepare, wal_buffer[0..size])) |*header| {
                    assert(header.valid_checksum());

                    if (header.op == 0) {
                        assert(header.operation == .root);
                    } else {
                        assert(header.operation == .reserved);
                    }
                }

                storage.write_sectors(
                    write_sectors_callback,
                    &self.writes[self.writes_pending],
                    wal_buffer[0..size],
                    .wal_headers,
                    wal_offset,
                );
                self.writes_pending += 1;
                wal_offset += size;
            }
            // There are no headers left to write.
            assert(format_wal_headers(cluster, wal_offset, &wal_buffer_test) == 0);
        }

        fn queue_format_replies(
            self: *ReplicaFormat,
            arena: std.mem.Allocator,
            storage: *Storage,
        ) !void {
            assert(!self.formatting and !self.formatting_superblock);

            // Direct I/O requires the buffer to be sector-aligned.
            const message_buffer =
                try arena.alignedAlloc(
                u8,
                constants.sector_size,
                constants.message_size_max * constants.clients_max,
            );
            // Freed when the arena is cleaned up.

            @memset(message_buffer, 0);

            for (0..constants.clients_max) |slot| {
                // Line length limits:
                const message_buffer_slot = message_buffer[slot * constants.message_size_max ..];

                storage.write_sectors(
                    write_sectors_callback,
                    &self.writes[self.writes_pending],
                    message_buffer_slot[0..constants.message_size_max],
                    .client_replies,
                    slot * constants.message_size_max,
                );
                self.writes_pending += 1;
            }
        }

        fn queue_format_grid_padding(
            self: *ReplicaFormat,
            arena: std.mem.Allocator,
            storage: *Storage,
        ) !void {
            assert(!self.formatting and !self.formatting_superblock);

            const padding_size = vsr.Zone.size(.grid_padding).?;
            assert(padding_size < constants.block_size);

            if (padding_size > 0) {
                self.writes_expected += 1;

                // Direct I/O requires the buffer to be sector-aligned.
                const padding_buffer = try arena.alignedAlloc(
                    u8,
                    constants.sector_size,
                    vsr.Zone.size(.grid_padding).?,
                );
                // Freed when the arena is cleaned up.

                @memset(padding_buffer, 0);

                storage.write_sectors(
                    write_sectors_callback,
                    &self.writes[self.writes_pending],
                    padding_buffer,
                    .grid_padding,
                    0,
                );
                self.writes_pending += 1;
            }
        }

        fn format_and_tick(
            self: *ReplicaFormat,
            storage: *Storage,
            superblock: *SuperBlock,
            superblock_options: SuperBlock.FormatOptions,
        ) void {
            assert(formating_instance == null);
            formating_instance = self;

            assert(self.writes_pending == self.writes_expected);

            superblock.format(
                format_superblock_callback,
                &self.superblock_context,
                superblock_options,
            );

            self.formatting = true;
            self.formatting_superblock = true;
            while (self.formatting or self.formatting_superblock) storage.run();

            formating_instance = null;
        }

        fn write_sectors_callback(_: *Storage.Write) void {
            const self: *ReplicaFormat = formating_instance.?;
            assert(self.formatting);

            self.writes_pending -= 1;

            if (self.writes_pending == 0) {
                self.formatting = false;
            }
        }

        fn format_superblock_callback(superblock_context: *SuperBlock.Context) void {
            const self: *ReplicaFormat =
                @alignCast(@fieldParentPtr("superblock_context", superblock_context));
            assert(self.formatting_superblock);
            self.formatting_superblock = false;
        }
    };
}

test "format" {
    const data_file_size_min = @import("./superblock.zig").data_file_size_min;
    const Storage = @import("../testing/storage.zig").Storage;
    const SuperBlock = vsr.SuperBlockType(Storage);
    const allocator = std.testing.allocator;
    const cluster = 0;
    const replica = 1;
    const replica_count = 1;

    var storage = try Storage.init(
        allocator,
        data_file_size_min,
        .{
            .read_latency_min = 0,
            .read_latency_mean = 0,
            .write_latency_min = 0,
            .write_latency_mean = 0,
            .iops_write_max = writes_max,
        },
    );
    defer storage.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = data_file_size_min,
    });
    defer superblock.deinit(allocator);

    try format(Storage, allocator, .{
        .cluster = cluster,
        .release = vsr.Release.minimum,
        .replica = replica,
        .replica_count = replica_count,
    }, &storage, &superblock);

    // Verify the superblock headers.
    var copy: u8 = 0;
    while (copy < constants.superblock_copies) : (copy += 1) {
        const superblock_header = storage.superblock_header(copy);

        try std.testing.expectEqual(superblock_header.copy, copy);
        try std.testing.expectEqual(superblock_header.cluster, cluster);
        try std.testing.expectEqual(superblock_header.sequence, 1);
        try std.testing.expectEqual(
            superblock_header.vsr_state.checkpoint.storage_size,
            storage.size,
        );
        try std.testing.expectEqual(superblock_header.vsr_state.checkpoint.header.op, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.commit_max, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.view, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.log_view, 0);
        try std.testing.expectEqual(
            superblock_header.vsr_state.replica_id,
            superblock_header.vsr_state.members[replica],
        );
        try std.testing.expectEqual(superblock_header.vsr_state.replica_count, replica_count);
    }

    // Verify the WAL headers and prepares zones.
    for (storage.wal_headers(), storage.wal_prepares(), 0..) |header, *message, slot| {
        try std.testing.expect(std.meta.eql(header, message.header));

        try std.testing.expect(header.valid_checksum());
        try std.testing.expect(header.valid_checksum_body(&[0]u8{}));
        try std.testing.expectEqual(header.invalid(), null);
        try std.testing.expectEqual(header.cluster, cluster);
        try std.testing.expectEqual(header.op, slot);
        try std.testing.expectEqual(header.size, @sizeOf(vsr.Header));
        try std.testing.expectEqual(header.command, .prepare);
        if (slot == 0) {
            try std.testing.expectEqual(header.operation, .root);
        } else {
            try std.testing.expectEqual(header.operation, .reserved);
        }
    }

    // Verify client replies.
    try std.testing.expectEqual(storage.client_replies().len, constants.clients_max);
    try std.testing.expect(stdx.zeroed(
        storage.memory[vsr.Zone.client_replies.offset(0)..][0..vsr.Zone.client_replies.size().?],
    ));

    // Verify grid padding.
    const padding_size = vsr.Zone.grid_padding.size().?;
    if (padding_size > 0) {
        try std.testing.expect(stdx.zeroed(
            storage.memory[vsr.Zone.grid_padding.offset(0)..][0..vsr.Zone.grid_padding.size().?],
        ));
    }
}
