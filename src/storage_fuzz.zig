const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_storage);

const stdx = @import("stdx.zig");
const vsr = @import("vsr.zig");
const constants = @import("constants.zig");
const IO = @import("testing/io.zig").IO;
const Storage = @import("storage.zig").Storage(IO);
const fuzz = @import("testing/fuzz.zig");

pub fn main(args: fuzz.FuzzArgs) !void {
    const zones: []const vsr.Zone = &.{
        .superblock,
        .wal_headers,
        .wal_prepares,
        .client_replies,
    };

    const storage_size = constants.sector_size * 64;

    var storage_data_written: [storage_size]u8 align(constants.sector_size) = undefined;
    @memset(&storage_data_written, 0);
    var storage_data_stored: [storage_size]u8 align(constants.sector_size) = undefined;
    @memset(&storage_data_stored, 0);
    var storage_data_read: [storage_size]u8 align(constants.sector_size) = undefined;
    @memset(&storage_data_read, 0);

    var io = IO.init(&.{&storage_data_stored}, .{
        .seed = args.seed,
        // Probability set to 0 as the logic for "simple" read faults is
        // very obvious: zero out the returned buffer and return then.
        //
        // It seems more interesting to test the more complex
        // "larger than a single sector" case.
        .read_fault_probability = 0,
        .larger_than_logical_sector_read_fault_probability = 25,
    });

    var storage = try Storage.init(&io, 0);

    var data_prng = std.rand.DefaultPrng.init(args.seed);

    const iterations = 10_000;

    for (0..iterations) |_| {
        const zone = zones[data_prng.random().int(u2)];

        const length = std.mem.alignBackward(
            u64,
            data_prng.random().uintLessThan(u64, zone.size().? - constants.sector_size),
            constants.sector_size,
        ) + constants.sector_size;
        const offset_in_zone = std.mem.alignBackward(
            u64,
            data_prng.random().uintLessThan(u64, zone.size().? - length),
            constants.sector_size,
        );

        const write_buffer = storage_data_written[zone.start() + offset_in_zone ..][0..length];
        data_prng.fill(write_buffer);

        var write_completion: Storage.Write = undefined;

        storage.write_sectors(
            struct {
                fn callback(completion: *Storage.Write) void {
                    _ = completion;
                }
            }.callback,
            &write_completion,
            write_buffer,
            zone,
            offset_in_zone,
        );

        storage.tick();
    }

    for (zones) |zone| {
        const ReadDetail = struct {
            offset_in_zone: u64,
            read_length: u64,
        };

        var read_details: [32]ReadDetail = undefined;

        const sector_count: u64 = @divExact(zone.size().?, constants.sector_size);
        assert(sector_count <= read_details.len);

        var index: u64 = 0;
        var read_detail_length: usize = 0;

        while (index < sector_count) : (read_detail_length += 1) {
            const n_sectors = data_prng.random().intRangeAtMost(
                u64,
                1,
                @min(4, sector_count - index),
            );

            read_details[read_detail_length] = .{
                .offset_in_zone = index * constants.sector_size,
                .read_length = n_sectors * constants.sector_size,
            };

            index += n_sectors;
        }

        data_prng.random().shuffle(ReadDetail, read_details[0..read_detail_length]);

        for (read_details[0..read_detail_length]) |read_detail| {
            const sector_offset = read_detail.offset_in_zone;
            const read_length = read_detail.read_length;
            const read_buffer = storage_data_read[zone.start() + sector_offset ..][0..read_length];

            var read_completion: Storage.Read = undefined;
            storage.read_sectors(
                struct {
                    fn callback(completion: *Storage.Read) void {
                        _ = completion;
                    }
                }.callback,
                &read_completion,
                read_buffer,
                zone,
                sector_offset,
            );

            storage.tick();
        }
    }

    try std.testing.expectEqualSlices(u8, &storage_data_written, &storage_data_stored);
    try std.testing.expectEqualSlices(u8, &storage_data_stored, &storage_data_read);
}
