//! Fuzz WAL formats using different write sizes.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_journal_format);

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const journal = @import("./journal.zig");
const fuzz = @import("../testing/fuzz.zig");
const allocator = fuzz.allocator;

const cluster = 0;

pub fn main(args: fuzz.FuzzArgs) !void {
    var prng = std.rand.DefaultPrng.init(args.seed);

    // +10 to occasionally test formatting into a buffer larger than the total data size.
    const write_sectors_max = @divExact(constants.journal_size_headers, constants.sector_size);
    const write_sectors = 1 + prng.random().uintLessThan(usize, write_sectors_max + 10);
    const write_size = write_sectors * constants.sector_size;

    log.info("write_size={} write_sectors={}", .{ write_size, write_sectors });

    try fuzz_format_wal_headers(write_size);
    try fuzz_format_wal_prepares(write_size);
}

pub fn fuzz_format_wal_headers(write_size_max: usize) !void {
    assert(write_size_max > 0);
    assert(write_size_max % @sizeOf(vsr.Header) == 0);
    assert(write_size_max % constants.sector_size == 0);

    const write = try allocator.alloc(u8, write_size_max);
    defer allocator.free(write);

    var offset: usize = 0;
    while (offset < constants.journal_size_headers) {
        const write_size = journal.format_wal_headers(cluster, offset, write);
        defer offset += write_size;

        const write_headers = std.mem.bytesAsSlice(vsr.Header.Prepare, write[0..write_size]);
        for (write_headers, 0..) |header, i| {
            const slot = @divExact(offset, @sizeOf(vsr.Header)) + i;
            try verify_slot_header(slot, header);
        }
    }
    assert(offset == constants.journal_size_headers);
}

pub fn fuzz_format_wal_prepares(write_size_max: usize) !void {
    assert(write_size_max > 0);
    assert(write_size_max % @sizeOf(vsr.Header) == 0);
    assert(write_size_max % constants.sector_size == 0);

    const write = try allocator.alloc(u8, write_size_max);
    defer allocator.free(write);

    var offset: usize = 0;
    while (offset < constants.journal_size_prepares) {
        const write_size = journal.format_wal_prepares(cluster, offset, write);
        defer offset += write_size;

        var offset_checked: usize = 0;
        while (offset_checked < write_size) {
            const offset_header_next = std.mem.alignForward(
                usize,
                offset + offset_checked,
                constants.message_size_max,
            ) - offset;

            if (offset_checked == offset_header_next) {
                // Message header.
                const slot = @divExact(offset + offset_checked, constants.message_size_max);
                const header_bytes = write[offset_checked..][0..@sizeOf(vsr.Header)];
                const header = std.mem.bytesToValue(vsr.Header.Prepare, header_bytes);

                try verify_slot_header(slot, header);
                offset_checked += @sizeOf(vsr.Header);
            } else {
                // Message body.
                const offset_message_end = @min(offset_header_next, write_size);
                const message_body_bytes = write[offset_checked..offset_message_end];
                var byte: usize = 0;
                for (std.mem.bytesAsSlice(usize, message_body_bytes)) |b| byte |= b;

                try std.testing.expectEqual(byte, 0);
                offset_checked = offset_message_end;
            }
        }
        assert(offset_checked == write_size);
    }
    assert(offset == constants.journal_size_prepares);
}

fn verify_slot_header(slot: usize, header: vsr.Header.Prepare) !void {
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
