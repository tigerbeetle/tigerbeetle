//! Fuzz WAL formats using different write sizes.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_journal_format);

const config = @import("../config.zig");
const util = @import("../util.zig");
const vsr = @import("../vsr.zig");
const journal = @import("./journal.zig");
const fuzz = @import("../test/fuzz.zig");

const cluster = 0;

pub fn main() !void {
    const allocator = std.testing.allocator;
    const args = try fuzz.parse_fuzz_args(allocator);
    var prng = std.rand.DefaultPrng.init(args.seed);

    // +10 to occasionally test formatting into a buffer larger than the total data size.
    const write_sectors_max = @divExact(config.journal_size_headers, config.sector_size);
    const write_sectors = 1 + prng.random().uintLessThan(usize, write_sectors_max + 10);
    const write_size = write_sectors * config.sector_size;

    log.info("write_size={} write_sectors={}", .{ write_size, write_sectors });

    try fuzz_format_wal_headers(write_size);
}

pub fn fuzz_format_wal_headers(write_size_max: usize) !void {
    assert(write_size_max > 0);
    assert(write_size_max % @sizeOf(vsr.Header) == 0);
    assert(write_size_max % config.sector_size == 0);

    const write = try std.testing.allocator.alloc(u8, write_size_max);
    defer std.testing.allocator.free(write);

    var offset: usize = 0;
    while (offset < config.journal_size_headers) {
        const write_size = journal.format_wal_headers(cluster, offset, write);
        defer offset += write_size;

        const write_headers = std.mem.bytesAsSlice(vsr.Header, write[0..write_size]);
        for (write_headers) |header, i| {
            const slot = @divExact(offset, @sizeOf(vsr.Header)) + i;
            try verify_slot_header(slot, header);
        }
    }
    assert(offset == config.journal_size_headers);
}

fn verify_slot_header(slot: usize, header: vsr.Header) !void {
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
