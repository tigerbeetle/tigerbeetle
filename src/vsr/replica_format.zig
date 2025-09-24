const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const stdx = @import("stdx");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;
const data_file_size_min = @import("./superblock.zig").data_file_size_min;

/// Initialize the TigerBeetle replica's data file.
pub fn format(
    comptime Storage: type,
    gpa: std.mem.Allocator,
    storage: *Storage,
    options: vsr.SuperBlockType(Storage).FormatOptions,
) !void {
    const ReplicaFormat = ReplicaFormatType(Storage);
    const SuperBlock = vsr.SuperBlockType(Storage);

    var superblock = try SuperBlock.init(gpa, storage, .{
        .storage_size_limit = data_file_size_min,
    });
    defer superblock.deinit(gpa);

    var replica_format = try ReplicaFormat.init(gpa);
    defer replica_format.deinit(gpa);

    try replica_format.queue_format_wal(options.cluster, storage);
    replica_format.format_and_tick(storage, &superblock, options);
    replica_format.verify_writes();
}

/// When formatting, we write:
/// * constants.journal_slot_count many prepares,
/// * 1 write that contains all of the headers.
pub const writes_max = constants.journal_slot_count + 1;

fn ReplicaFormatType(comptime Storage: type) type {
    const SuperBlock = vsr.SuperBlockType(Storage);
    return struct {
        const ReplicaFormat = @This();

        const Write = struct {
            write: Storage.Write = undefined,
            replica_format: *ReplicaFormat,

            issued_buffer: []const u8,
            issued_offset: u64,
        };

        formatting: bool = false,
        formatting_superblock: bool = false,
        superblock_context: SuperBlock.Context = undefined,

        writes: [writes_max]Write = undefined,
        writes_pending: u64 = 0,

        sectors_written: std.DynamicBitSetUnmanaged,
        arena: std.heap.ArenaAllocator,

        fn init(gpa: std.mem.Allocator) !ReplicaFormat {
            var sectors_written = try std.DynamicBitSetUnmanaged.initEmpty(
                gpa,
                @divExact(data_file_size_min, constants.sector_size),
            );
            errdefer sectors_written.deinit(gpa);

            var arena = std.heap.ArenaAllocator.init(gpa);
            errdefer arena.deinit();

            return .{
                .sectors_written = sectors_written,
                .arena = arena,
            };
        }

        fn deinit(self: *ReplicaFormat, gpa: std.mem.Allocator) void {
            self.arena.deinit();
            self.sectors_written.deinit(gpa);
        }

        fn queue_format_wal(
            self: *ReplicaFormat,
            cluster: u128,
            storage: *Storage,
        ) !void {
            assert(!self.formatting and !self.formatting_superblock);

            const arena = self.arena.allocator();

            // The logical offset *within the Zone*.
            // Even though the prepare zone follows the redundant header zone, write the prepares
            // first. This allows the test Storage to check the invariant "never write the redundant
            // header before the prepare".
            for (0..constants.journal_slot_count) |slot| {
                // Direct I/O requires the buffer to be sector-aligned. Allocate a buffer for each
                // sector in the arena, so they can be written concurrently.
                const header_buffer = try arena.alignedAlloc(
                    u8,
                    constants.sector_size,
                    constants.sector_size,
                );
                const header: *Header.Prepare = std.mem.bytesAsValue(
                    Header.Prepare,
                    header_buffer,
                );
                header.* = slot_header(cluster, slot);
                assert(header.valid_checksum());

                const prepare_offset = slot * constants.message_size_max;
                assert(prepare_offset <= constants.journal_size_prepares);
                assert(prepare_offset % @sizeOf(Header) == 0);
                assert(prepare_offset % constants.sector_size == 0);

                // Zero padding to produce identical checksums of an empty datafile, not because
                // it's required for correctness.
                const header_padding = header_buffer[@sizeOf(Header.Prepare)..];
                @memset(header_padding, 0);
                assert(stdx.zeroed(header_padding));

                if (header.op == 0) {
                    assert(header.operation == .root);
                } else {
                    assert(header.operation == .reserved);
                }

                self.writes[self.writes_pending] = .{
                    .replica_format = self,
                    .issued_buffer = header_buffer,
                    .issued_offset = prepare_offset,
                };

                storage.write_sectors(
                    write_sectors_callback,
                    &self.writes[self.writes_pending].write,
                    header_buffer,
                    .wal_prepares,
                    prepare_offset,
                );
                self.writes_pending += 1;
            }

            // Direct I/O requires the buffer to be sector-aligned. Unlike the Prepares above that
            // require a buffer per prepare, since they are spread out with zeros inbetween, the
            // headers zone is contiguous so a single buffer will do.
            //
            // There might be padding, so allocate []u8 instead of []Header.Prepare.
            const headers_buffer = try arena.alignedAlloc(
                u8,
                constants.sector_size,
                vsr.sector_ceil(constants.journal_size_headers),
            );

            for (0..constants.journal_slot_count) |slot| {
                const header_buffer =
                    headers_buffer[slot * @sizeOf(Header.Prepare) ..][0..@sizeOf(Header.Prepare)];
                const header: *Header.Prepare = @alignCast(
                    std.mem.bytesAsValue(Header.Prepare, header_buffer),
                );
                header.* = slot_header(cluster, slot);
                assert(header.valid_checksum());

                if (header.op == 0) {
                    assert(header.operation == .root);
                } else {
                    assert(header.operation == .reserved);
                }
            }

            // Zero padding to produce identical checksums of an empty datafile, not because it's
            // required for correctness.
            const headers_padding =
                headers_buffer[constants.journal_slot_count * @sizeOf(Header.Prepare) ..];
            @memset(headers_padding, 0);
            assert(stdx.zeroed(headers_padding));

            self.writes[self.writes_pending] = .{
                .replica_format = self,
                .issued_buffer = headers_buffer,
                .issued_offset = 0,
            };
            storage.write_sectors(
                write_sectors_callback,
                &self.writes[self.writes_pending].write,
                headers_buffer,
                .wal_headers,
                0,
            );
            self.writes_pending += 1;
        }

        fn format_and_tick(
            self: *ReplicaFormat,
            storage: *Storage,
            superblock: *SuperBlock,
            superblock_options: SuperBlock.FormatOptions,
        ) void {
            assert(self.writes_pending == writes_max);

            self.formatting = true;
            while (self.formatting) storage.run();

            self.formatting_superblock = true;
            superblock.format(
                format_superblock_callback,
                &self.superblock_context,
                superblock_options,
            );
            while (self.formatting_superblock) storage.run();
        }

        fn write_sectors_callback(storage_write: *Storage.Write) void {
            const write: *Write = @fieldParentPtr("write", storage_write);
            const self = write.replica_format;

            assert(self.formatting);
            assert(!self.formatting_superblock);

            self.writes_pending -= 1;

            const sector_offset = @divExact(
                storage_write.zone.offset(write.issued_offset),
                constants.sector_size,
            );
            const sector_count = @divExact(write.issued_buffer.len, constants.sector_size);

            for (sector_offset..sector_offset + sector_count) |sector| {
                self.sectors_written.set(sector);
            }

            if (self.writes_pending == 0) {
                self.formatting = false;
            }
        }

        fn format_superblock_callback(superblock_context: *SuperBlock.Context) void {
            const self: *ReplicaFormat =
                @alignCast(@fieldParentPtr("superblock_context", superblock_context));
            assert(!self.formatting);
            assert(self.formatting_superblock);
            self.formatting_superblock = false;
        }

        fn verify_writes(self: *ReplicaFormat) void {
            assert(!self.formatting and !self.formatting_superblock);
            assert(self.writes_pending == 0);

            assert(self.sectors_written.count() > 0);
            assert(self.sectors_written.capacity() ==
                @divExact(data_file_size_min, constants.sector_size));

            // Expect that:
            // * every sector in the wal_headers zone has been written,
            // * the first sector in every constants.message_size_max has been written,
            // * nothing else has been written.
            //
            // This might seem to miss the superblock zone, but that's handled entirely by
            // superblock.zig, which reads back the headers to validate it has been written
            // correctly.
            for (0..self.sectors_written.capacity()) |sector| {
                const sector_start = sector * constants.sector_size;

                const zone = for (std.enums.values(vsr.Zone)) |zone| {
                    if (sector_start >= zone.start() and
                        sector_start < zone.start() + zone.size().?) break zone;
                } else unreachable;

                switch (zone) {
                    // Every sector in the wal_headers zone has been written:
                    .wal_headers => assert(self.sectors_written.isSet(sector)),

                    // The first sector in every constants.message_size_max has been written:
                    .wal_prepares => {
                        if ((sector_start - zone.start()) % constants.message_size_max == 0) {
                            assert(self.sectors_written.isSet(sector));
                        } else {
                            assert(!self.sectors_written.isSet(sector));
                        }
                    },

                    // Nothing else has been written:
                    else => assert(!self.sectors_written.isSet(sector)),
                }
            }
        }
    };
}

pub fn slot_header(cluster: u128, slot: u64) Header.Prepare {
    assert(slot < constants.journal_slot_count);
    assert(slot * @sizeOf(Header.Prepare) < constants.journal_size_headers);
    assert(slot * constants.message_size_max < constants.journal_size_prepares);
    assert(@sizeOf(Header.Prepare) < constants.sector_size);

    return if (slot == 0)
        Header.Prepare.root(cluster)
    else
        Header.Prepare.reserve(cluster, slot);
}

test slot_header {
    const allocator = std.testing.allocator;

    const header_buffer = try allocator.create(Header.Prepare);
    defer allocator.destroy(header_buffer);

    for (0..constants.journal_slot_count) |slot| {
        const header = slot_header(0, slot);

        try std.testing.expect(header.valid_checksum());
        try std.testing.expect(header.valid_checksum_body(&[0]u8{}));
        try std.testing.expectEqual(header.invalid(), null);
        try std.testing.expectEqual(header.cluster, 0);
        try std.testing.expectEqual(header.op, slot);
        try std.testing.expectEqual(header.size, @sizeOf(vsr.Header));
        try std.testing.expectEqual(header.command, .prepare);
        if (slot == 0) {
            try std.testing.expectEqual(header.operation, .root);
        } else {
            try std.testing.expectEqual(header.operation, .reserved);
        }
    }
}

test "format" {
    const Storage = @import("../testing/storage.zig").Storage;
    const fixtures = @import("../testing/fixtures.zig");
    const allocator = std.testing.allocator;
    const cluster = 0;
    const replica = 1;
    const replica_count = 1;

    var storage = try fixtures.init_storage(allocator, .{
        .size = data_file_size_min,
        .iops_write_max = writes_max,
    });
    defer storage.deinit(allocator);

    try format(Storage, allocator, &storage, .{
        .cluster = cluster,
        .release = vsr.Release.minimum,
        .replica = replica,
        .replica_count = replica_count,
        .view = null,
    });

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

    // Verify client replies. The contents are not zeroed.
    try std.testing.expectEqual(storage.client_replies().len, constants.clients_max);

    // Verify grid alignment. The contents of the padding are not zeroed.
    try std.testing.expect(vsr.Zone.grid.start() % constants.sector_size == 0);

    // Explicitly zero client_replies and the grid padding. This is not required for formatting, but
    // it allows for easy checksums of the entire testing storage.
    @memset(
        storage.memory[vsr.Zone.client_replies.offset(0)..][0..vsr.Zone.client_replies.size().?],
        0,
    );
    if (vsr.Zone.grid_padding.size().? > 0) {
        @memset(
            storage.memory[vsr.Zone.grid_padding.offset(0)..][0..vsr.Zone.grid_padding.size().?],
            0,
        );
    }

    // Lastly, verify the entire storage contents against a known good checksum for the given
    // cluster, replica and replica count.
    //
    // This doesn't match the output from `tigerbeetle format ...` since the testing storage / slot
    // counts are lower.
    try std.testing.expectEqual(
        vsr.checksum(storage.memory),
        234838825150141811691318382366967529672,
    );
}
