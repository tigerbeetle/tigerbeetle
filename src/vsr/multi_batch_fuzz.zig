const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");
const constants = vsr.constants;
const MultiBatchDecoder = vsr.multi_batch.MultiBatchDecoder;
const MultiBatchEncoder = vsr.multi_batch.MultiBatchEncoder;
const stdx = @import("stdx");
const MiB = stdx.MiB;
const fuzz = @import("../testing/fuzz.zig");

pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    var prng = stdx.PRNG.from_seed(args.seed);
    const message_body_size_min = constants.sector_size - @sizeOf(vsr.Header);
    const message_body_size_max = (1 * MiB) - @sizeOf(vsr.Header);
    const buffer_expected = try gpa.alignedAlloc(
        u8,
        @alignOf(vsr.Header),
        message_body_size_max,
    );
    defer gpa.free(buffer_expected);
    const buffer_actual = try gpa.alignedAlloc(
        u8,
        @alignOf(vsr.Header),
        message_body_size_max,
    );
    defer gpa.free(buffer_actual);

    const events_max = args.events_max orelse 1024;
    for (0..events_max) |_| {
        const buffer_size: usize = prng.range_inclusive(
            usize,
            message_body_size_min,
            message_body_size_max,
        );
        try run_fuzz(.{
            .prng = &prng,
            .buffer_expected = buffer_expected[0..buffer_size],
            .buffer_actual = buffer_actual[0..buffer_size],
        });
    }
}

fn run_fuzz(options: struct {
    prng: *stdx.PRNG,
    buffer_expected: []u8,
    buffer_actual: []u8,
}) !void {
    assert(options.buffer_expected.len == options.buffer_actual.len);
    // The end of the buffer must be aligned with the postamble.
    const postamble_alignment = options.buffer_expected.len % @sizeOf(u16);
    assert(postamble_alignment < @sizeOf(u16));
    const buffer_expected: []u8 =
        options.buffer_expected[0 .. options.buffer_expected.len - postamble_alignment];
    const buffer_actual: []u8 =
        options.buffer_actual[0 .. options.buffer_actual.len - postamble_alignment];

    // Generate the batch plan with element sizes from 2^0 to 2^8.
    const batch_element_size: u32 = std.math.pow(u32, 2, options.prng.int_inclusive(u32, 8));
    var batches = stdx.BoundedArrayType(u32, 8190){};
    const batch_count_max = options.prng.range_inclusive(
        usize,
        1,
        batches.capacity(),
    );

    // Encoder will ignore and overwrite any existing content in the target buffer,
    // at the end, both buffers must be equal.
    options.prng.fill(buffer_expected);
    options.prng.fill(buffer_actual);

    // Encode.
    var encoder = MultiBatchEncoder.init(buffer_actual, .{
        .element_size = batch_element_size,
    });
    var expect_payload_size: u32 = 0;
    var expect_trailer_size: u32 = 0;
    for (0..batch_count_max) |_| {
        assert(expect_payload_size + expect_trailer_size <= buffer_expected.len);

        const trailer_size_next = vsr.multi_batch.trailer_total_size(.{
            .batch_count = @intCast(batches.count() + 1),
            .element_size = batch_element_size,
        });
        const expect_padding: u32 = expect_payload_size % @sizeOf(u16);
        assert(expect_padding < @sizeOf(u16));
        if (buffer_expected.len < expect_payload_size + expect_padding + trailer_size_next) {
            assert(batches.count() > 0);
            assert(encoder.writable() == null);
            break;
        }

        const batch_element_count_max: u32 = @intCast(@divFloor(
            buffer_expected.len -
                (expect_payload_size + expect_padding + trailer_size_next),
            batch_element_size,
        ));
        const batch_element_count: u32 = if (batch_element_count_max > 0)
            switch (options.prng.enum_uniform(enum { zero, one, random })) {
                .zero => 0,
                .one => 1,
                .random => options.prng.range_inclusive(u32, 1, @min(
                    std.math.maxInt(u16), // Cannot encode more than `u16` elements.
                    batch_element_count_max,
                )),
            }
        else
            0;
        const batch_size: u32 = batch_element_count * batch_element_size;
        const writable: []u8 = encoder.writable().?;
        assert(batch_size <= writable.len);

        const source: []u8 = buffer_expected[expect_payload_size..][0..batch_size];
        const target: []u8 = writable[0..batch_size];
        stdx.copy_disjoint(.exact, u8, target, source);

        encoder.add(batch_size);
        expect_payload_size += batch_size;
        batches.push(batch_element_count);
        expect_trailer_size = trailer_size_next;
    }
    assert(batches.count() > 0);
    assert(batches.count() == encoder.batch_count);
    assert(expect_payload_size + expect_trailer_size <= buffer_expected.len);
    assert(expect_payload_size == encoder.buffer_index);

    const expect_padding: u32 = expect_payload_size % @sizeOf(u16);
    assert(expect_padding < @sizeOf(u16));

    const encoder_bytes_written = encoder.finish();
    assert(encoder_bytes_written > 0);
    assert(encoder_bytes_written ==
        expect_payload_size + expect_padding + expect_trailer_size);

    {
        // Decode.
        var decoder = try MultiBatchDecoder.init(
            buffer_actual[0..encoder_bytes_written],
            .{ .element_size = batch_element_size },
        );
        assert(expect_payload_size == decoder.payload.len);

        var payloads_decoded: u32 = 0;
        for (batches.const_slice()) |batch_element_count| {
            const batch_size: u32 = batch_element_count * batch_element_size;
            const expect_batch: []u8 = buffer_expected[payloads_decoded..][0..batch_size];
            payloads_decoded += batch_size;

            const decoded_batch = decoder.pop().?;
            assert(batch_size == decoded_batch.len);
            assert(std.mem.eql(u8, expect_batch, decoded_batch));
        }
        assert(payloads_decoded == decoder.payload.len);
        assert(decoder.pop() == null);
    }

    // Verify that any flipped bit mutates the results or causes a decoding error
    // (but never a panic).
    for (0..32) |_| {
        const byte_index = options.prng.int_inclusive(usize, encoder_bytes_written - 1);
        for (0..@bitSizeOf(u8)) |bit_index| {
            buffer_actual[byte_index] ^= @as(u8, 1) << @as(u3, @intCast(bit_index));
            defer buffer_actual[byte_index] ^= @as(u8, 1) << @as(u3, @intCast(bit_index));

            var decoder = MultiBatchDecoder.init(
                buffer_actual[0..encoder_bytes_written],
                .{ .element_size = batch_element_size },
            ) catch continue;

            var same: bool = true;
            var payloads_decoded: u32 = 0;
            for (batches.const_slice()) |batch_element_count| {
                const batch_size: u32 = batch_element_count * batch_element_size;
                const expect_batch: []u8 = buffer_expected[payloads_decoded..][0..batch_size];
                payloads_decoded += batch_size;

                const decoded_batch = decoder.pop().?;
                same = same and std.mem.eql(u8, expect_batch, decoded_batch);
            }
            assert(decoder.pop() == null);
            assert(!same);
        }
    }
}
