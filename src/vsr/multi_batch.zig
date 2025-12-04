///! Multi-batching consists of the application submitting multiple independent units of work of
///! the same operation (the batch payload) within a single VSR message.
///! This amortizes network and consensus costs, improving performance in scenarios where highly
///! concurrent user requests submit operations containing only a few events each, sharing the same
///! physical request.
///!
///! - Multi-batched requests use a portion at the end of the message body (the trailer) to
///!   encode batch metadata, so multi-batched requests can hold fewer events than regular ones.
///!
///! - The trailer size is always a multiple of the operation's `Event`/`Result` size to keep
///!   system invariants.
///!
///! - The batch trailer is an array of `u16` values representing the number of events in each
///!   batch, plus a "postamble" containing the total number of batches encoded.
///!
///! - The trailer has variable length, depending on the number of batches (one `u16` per batch,
///!   in multiples of the operation's `Event`/`Result` size).
///!
///! - The trailer is written from the end of the message towards the beginning. The last element
///!   of the array corresponds to the number of events in the first batch.
///!
///! - Unused elements in the trailer, required for padding, are filled with `maxInt(u16)`.
///!
///! Example: Multi-batch request containing 4 batches, with each event being 128 bytes.
///!
///!  size          message.body_used().len == 1792 bytes
///!  2048 bytes    payload == 1664 bytes                   trailer == 128 bytes
///! ┌──────┐┌───────────────────────────────────────────┐┌────────────────────────┐
///! │ VSR  ││┌──────────┐┌─────────┐┌───────┐┌─────────┐││┌───────┐┌─┐┌─┐┌─┐┌─┐┌─┐│
///! │Header│││1024 bytes││128 bytes││0 bytes││512 bytes││││padding││4││0││1││8││4││
///! │      ││└───▲──────┘└──▲──────┘└▲──────┘└──▲──────┘││└───────┘└┬┘└┬┘└┬┘└┬┘└┬┘│
///! └──────┘└────┼──────────┼────────┼──────────┼───────┘└──────────┼──┼──┼──┼──┼─┘
///!              │          │        │          │                   │  │  │  │  └ postamble
///!              │          │        │          └───────────────────┘  │  │  │    batch_count == 4
///!              │          │        │                                 │  │  │
///!              │          │        └─────────────────────────────────┘  │  │
///!              │          │                                             │  │
///!              │          └─────────────────────────────────────────────┘  │
///!              │                                                           │
///!              └───────────────────────────────────────────────────────────┘
///!
const std = @import("std");
const testing = std.testing;

const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const MiB = stdx.MiB;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const Postamble = packed struct(u16) {
    /// `maxInt(u16)` is reserved for padding.
    const batch_count_max = std.math.maxInt(u16) - 1;

    /// The number of batches in the message body.
    batch_count: u16,
    comptime {
        assert(@sizeOf(Postamble) == @sizeOf(TrailerItem));
        assert(@alignOf(Postamble) == @alignOf(TrailerItem));
    }
};

const TrailerItem = packed struct(u16) {
    const padding: TrailerItem = .{ .element_count = std.math.maxInt(u16) };

    /// The number of elements in each batch, either `Event` or `Result`.
    element_count: u16,
    comptime {
        assert(@sizeOf(TrailerItem) == @sizeOf(Postamble));
        assert(@alignOf(TrailerItem) == @alignOf(Postamble));
    }
};

/// The maximum number of batches that can be encoded, assuming the worst case single-element
/// batches with the minimum size (maybe empty).
pub fn multi_batch_count_max(options: struct {
    /// The minimum size of a single batch.
    /// May be zero if the operation allows zeroed batches.
    batch_size_min: u32,
    /// The maximum size of the message body, including the multi-batch trailer.
    batch_size_limit: u32,
}) u16 {
    assert(options.batch_size_limit > @sizeOf(Postamble));
    maybe(options.batch_size_min == 0);
    return @intCast(@min(
        @divFloor(
            options.batch_size_limit - @sizeOf(Postamble),
            options.batch_size_min + @sizeOf(TrailerItem),
        ),
        Postamble.batch_count_max,
    ));
}

/// The trailer is an array of `TrailerItem`, each containing the number of elements
/// in a batch, followed by a `Postamble` that holds the total number of batches.
/// Encoding the trailer requires `(batch_count * @sizeOf(TrailerItem)) + @sizeOf(Postamble)`
/// bytes, but the total space occupied may be larger as padding bytes might be required for
/// alignment with the operation's element size.
pub fn trailer_total_size(options: struct {
    element_size: u32,
    batch_count: u16,
}) u32 {
    assert(options.batch_count > 0);
    assert(options.batch_count <= Postamble.batch_count_max);
    // Supports zero-sized elements, or any power of two, including 2^0.
    assert(options.element_size == 0 or std.math.isPowerOfTwo(options.element_size));

    const trailer_unpadded_size: u32 =
        (@as(u32, options.batch_count) * @sizeOf(TrailerItem)) + @sizeOf(Postamble);
    if (options.element_size == 0) return trailer_unpadded_size;

    return stdx.div_ceil(
        trailer_unpadded_size,
        options.element_size,
    ) * options.element_size;
}

pub const MultiBatchDecoder = struct {
    pub const Error = error{MultiBatchInvalid};

    pub const Options = struct {
        element_size: u32,
    };

    /// The message payload, excluding the trailer.
    payload: []const u8,
    /// The batching metadata, excluding the postamble.
    trailer_items: []const TrailerItem,

    payload_index: u32,
    batch_index: u16,

    options: Options,

    pub fn init(
        /// The message body used, including the trailer.
        body: []const u8,
        options: Options,
    ) Error!MultiBatchDecoder {
        // Supports zero-sized elements, or any power of two, including 2^0.
        assert(options.element_size == 0 or std.math.isPowerOfTwo(options.element_size));

        const Parser = struct {
            buffer: []const u8,
            buffer_parsed: u32 = 0,

            fn parse_suffix(parser: *@This(), comptime T: type, count: u32) Error![]const T {
                const suffix_size = count * @sizeOf(T);
                if (parser.buffer.len < suffix_size) return error.MultiBatchInvalid;

                const suffix = parser.buffer[parser.buffer.len - suffix_size ..];
                const suffix_aligned = std.mem.isAligned(@intFromPtr(suffix.ptr), @alignOf(T));
                if (!suffix_aligned) return error.MultiBatchInvalid;

                parser.buffer = parser.buffer[0 .. parser.buffer.len - suffix_size];
                parser.buffer_parsed += suffix_size;
                return stdx.bytes_as_slice(.exact, T, suffix);
            }
        };

        var parser = Parser{ .buffer = body };
        const postamble: *const Postamble = postamble: {
            const slice = try parser.parse_suffix(Postamble, 1);
            break :postamble &slice[0];
        };
        if (postamble.batch_count == 0) return error.MultiBatchInvalid;
        if (postamble.batch_count > Postamble.batch_count_max) return error.MultiBatchInvalid;

        const trailer_size = trailer_total_size(.{
            .element_size = options.element_size,
            .batch_count = postamble.batch_count,
        });

        const trailer_items_used = try parser.parse_suffix(TrailerItem, postamble.batch_count);
        // The trailer size is a multiple of the element size.
        // Unused elements are filled with `maxInt` for padding.
        const trailer_items_padding = try parser.parse_suffix(
            u8,
            trailer_size - parser.buffer_parsed,
        );
        if (!std.mem.allEqual(u8, trailer_items_padding, std.math.maxInt(u8))) {
            return error.MultiBatchInvalid;
        }

        const events_count_total: u32 = count: {
            var count: u32 = 0;
            for (trailer_items_used) |trailer_item| {
                count += trailer_item.element_count;
            }
            break :count count;
        };
        if (options.element_size == 0 and events_count_total != 0) return error.MultiBatchInvalid;
        const payload_size: u32 = std.math.mul(
            u32,
            events_count_total,
            options.element_size,
        ) catch |err| switch (err) {
            error.Overflow => return error.MultiBatchInvalid,
        };

        // For byte-aligned elements, padding may be required between the payload and the trailer.
        const trailer_padding_size: u32 = @intCast(payload_size % @sizeOf(TrailerItem));
        assert(trailer_padding_size < @sizeOf(TrailerItem));
        assert(trailer_padding_size == 0 or options.element_size == 1);
        const trailer_padding = try parser.parse_suffix(u8, trailer_padding_size);
        if (!std.mem.allEqual(u8, trailer_padding, std.math.maxInt(u8))) {
            return error.MultiBatchInvalid;
        }

        if (payload_size != body.len - parser.buffer_parsed) return error.MultiBatchInvalid;
        assert(payload_size == parser.buffer.len);

        return .{
            .payload = parser.buffer,
            .trailer_items = trailer_items_used,
            .payload_index = 0,
            .batch_index = 0,
            .options = options,
        };
    }

    pub fn reset(
        self: *MultiBatchDecoder,
    ) void {
        self.* = .{
            .payload = self.payload,
            .trailer_items = self.trailer_items,
            .batch_index = 0,
            .payload_index = 0,
            .options = self.options,
        };
    }

    pub fn batch_count(self: *const MultiBatchDecoder) u16 {
        assert(self.trailer_items.len <= Postamble.batch_count_max);
        return @intCast(self.trailer_items.len);
    }

    pub fn pop(self: *MultiBatchDecoder) ?[]const u8 {
        assert(self.trailer_items.len > 0);
        maybe(self.payload.len == 0);

        if (self.batch_index == self.trailer_items.len) {
            assert(self.payload_index == self.payload.len);
            return null;
        }
        const batch_item: []const u8 = self.peek();
        self.batch_index += 1;
        self.payload_index += @intCast(batch_item.len);
        assert(self.batch_index <= self.trailer_items.len);
        assert(self.payload_index <= self.payload.len);
        return batch_item;
    }

    pub fn peek(self: *const MultiBatchDecoder) []const u8 {
        assert(self.trailer_items.len > 0);
        assert(self.batch_index < self.trailer_items.len);
        assert(self.payload_index <= self.payload.len);
        maybe(self.payload.len == 0);

        // Batch metadata is written from the end of the message, so the last
        // element corresponds to the first batch.
        const trailer_item: *const TrailerItem =
            &self.trailer_items[self.trailer_items.len - self.batch_index - 1];
        if (trailer_item.element_count == 0) {
            assert(self.payload_index <= self.payload.len);
            return &.{};
        } else {
            assert(self.payload_index < self.payload.len);
        }

        const batch_size = trailer_item.element_count * self.options.element_size;
        assert(self.payload_index + batch_size <= self.payload.len);

        const slice: []const u8 = self.payload[self.payload_index..][0..batch_size];
        assert(slice.len > 0);
        assert(slice.len % self.options.element_size == 0);
        return slice;
    }
};

pub const MultiBatchEncoder = struct {
    const Options = struct {
        element_size: u32,
    };

    buffer: ?[]u8,
    batch_count: u16,
    buffer_index: u32,
    options: Options,

    pub fn init(buffer: []u8, options: Options) MultiBatchEncoder {
        // Supports zero-sized elements, or any power of two, including 2^0.
        assert(options.element_size == 0 or std.math.isPowerOfTwo(options.element_size));

        // The buffer must be large enough for at least one batch.
        const trailer_size_min = trailer_total_size(.{
            .batch_count = 1,
            .element_size = options.element_size,
        });
        assert(buffer.len >= trailer_size_min);

        // The buffer size might not be a multiple of the element size.
        // Only the final encoded message after calling `finish()` will be.
        maybe(options.element_size > 0 and
            buffer.len % options.element_size != 0);

        // The end of the buffer must be aligned with the trailer.
        // If it isn't, reduce the buffer to maintain alignment.
        const aligned_len = std.mem.alignBackward(
            usize,
            buffer.len,
            @sizeOf(TrailerItem),
        );

        return .{
            .buffer = buffer[0..aligned_len],
            .batch_count = 0,
            .buffer_index = 0,
            .options = options,
        };
    }

    pub fn reset(self: *MultiBatchEncoder) void {
        assert(self.buffer != null);
        self.* = .{
            .buffer = self.buffer,
            .batch_count = 0,
            .buffer_index = 0,
            .options = self.options,
        };
    }

    /// Returns a writable slice aligned and sized appropriately for the current operation.
    /// May return `null` if there isn't enough space in the buffer to add a new element
    /// to the trailer.
    /// The returned slice may have a length of zero if the remaining buffer
    /// isn't large enough to hold at least one element of the current operation.
    pub fn writable(self: *const MultiBatchEncoder) ?[]u8 {
        if (self.batch_count == Postamble.batch_count_max) return null;
        assert(self.batch_count < Postamble.batch_count_max);
        maybe(self.batch_count == 0);

        assert(self.options.element_size > 0 or self.buffer_index == 0);
        assert(self.options.element_size == 0 or
            self.buffer_index % self.options.element_size == 0);

        // Takes into account extra trailer bytes that will need to be included.
        const trailer_size: usize = trailer_total_size(.{
            .batch_count = self.batch_count + 1,
            .element_size = self.options.element_size,
        });

        const buffer: []u8 = self.buffer.?;
        if (buffer.len < self.buffer_index + trailer_size) {
            // Insufficient space for one more batch.
            return null;
        }

        if (self.options.element_size == 0) {
            // No writable buffer for zero-size elements, as they only add to the trailer.
            return &.{};
        }

        // Get an aligned slice.
        const slice: []u8 = buffer[self.buffer_index .. buffer.len - trailer_size];
        const size: usize =
            @divFloor(slice.len, self.options.element_size) * self.options.element_size;
        return slice[0..size];
    }

    /// Records how many bytes were written in the slice previously acquired by `writable()`.
    pub fn add(self: *MultiBatchEncoder, bytes_written: u32) void {
        assert(self.batch_count < Postamble.batch_count_max);
        maybe(self.batch_count == 0);

        const written_element_count: u16 = written_element_count: {
            if (self.options.element_size == 0) {
                assert(self.buffer_index == 0);
                assert(bytes_written == 0);
                break :written_element_count 0;
            }

            const written_element_count: u16 = @intCast(@divExact(
                bytes_written,
                self.options.element_size,
            ));
            maybe(written_element_count == 0);
            break :written_element_count written_element_count;
        };

        self.batch_count += 1;
        self.buffer_index += bytes_written;

        const buffer: []u8 = self.buffer.?;
        assert(self.buffer_index < buffer.len);

        const trailer_size = trailer_total_size(.{
            .batch_count = self.batch_count,
            .element_size = self.options.element_size,
        });
        assert(self.buffer_index + trailer_size <= buffer.len);

        const trailer_items: []TrailerItem = @alignCast(std.mem.bytesAsSlice(
            TrailerItem,
            buffer[buffer.len - trailer_size .. buffer.len - @sizeOf(Postamble)],
        ));
        assert(trailer_items.len >= self.batch_count);

        // Batch metadata is stacked from the end of the message, so the first element
        // of the array corresponds to the last batch added.
        trailer_items[trailer_items.len - self.batch_count] = .{
            .element_count = written_element_count,
        };
    }

    /// Finalizes the batch by writing the trailer with proper encoding.
    /// Returns the total number of bytes written (payload + trailer).
    /// At least one batch must be inserted, and the encoder should not be used after
    /// being finished.
    pub fn finish(self: *MultiBatchEncoder) u32 {
        assert(self.batch_count > 0);
        assert(self.batch_count <= Postamble.batch_count_max);

        const buffer: []u8 = self.buffer.?;
        assert(buffer.len > self.buffer_index);
        assert(self.options.element_size > 0 or self.buffer_index == 0);
        maybe(self.buffer_index == 0);

        const trailer_size = trailer_total_size(.{
            .batch_count = self.batch_count,
            .element_size = self.options.element_size,
        });

        // For byte-aligned elements, padding may be required between the payload and the trailer.
        const padding: u32 = self.buffer_index % @sizeOf(TrailerItem);
        assert(padding < @sizeOf(TrailerItem));
        assert(padding == 0 or self.options.element_size == 1);
        assert(buffer.len >= self.buffer_index + padding + trailer_size);
        // Filling the padding with sentinels.
        @memset(buffer[self.buffer_index..][0..padding], std.math.maxInt(u8));

        // While batches are being encoded, the trailer is written at the end of the buffer.
        // Once all batches are encoded, the trailer needs to be moved closer to the last
        // element written.
        const source: []const u8 = buffer[buffer.len - trailer_size ..];
        const target: []u8 = buffer[self.buffer_index + padding ..][0..trailer_size];
        assert(source.len == target.len);
        assert(@intFromPtr(source.ptr) >= @intFromPtr(target.ptr));
        if (source.ptr != target.ptr) {
            stdx.copy_left(.exact, u8, target, source);
        }

        const trailer_items: []TrailerItem = @alignCast(std.mem.bytesAsSlice(
            TrailerItem,
            buffer[self.buffer_index + padding ..][0 .. trailer_size - @sizeOf(Postamble)],
        ));
        // Filling in the extra alignment bytes with sentinels.
        @memset(
            trailer_items[0 .. trailer_items.len - self.batch_count],
            TrailerItem.padding,
        );

        const postamble: *Postamble = @ptrCast(@alignCast(
            buffer[self.buffer_index + padding + trailer_size - @sizeOf(Postamble) ..],
        ));
        postamble.* = .{
            .batch_count = self.batch_count,
        };

        self.buffer = null;
        const bytes_written: u32 = self.buffer_index + padding + trailer_size;
        assert(self.options.element_size > 0 or bytes_written == trailer_size);
        assert(self.options.element_size == 0 or
            bytes_written % self.options.element_size == 0);

        if (constants.verify) {
            _ = MultiBatchDecoder.init(buffer[0..bytes_written], .{
                .element_size = self.options.element_size,
            }) catch |err| switch (err) {
                error.MultiBatchInvalid => unreachable,
            };
        }

        return bytes_written;
    }
};

// The maximum number of batches, all with zero elements.
test "batch: maximum batches with no elements" {
    var prng = stdx.PRNG.from_seed_testing();

    const batch_count = Postamble.batch_count_max;
    const element_size = 128;
    const buffer_size = trailer_total_size(.{
        .element_size = element_size,
        .batch_count = batch_count,
    });

    const buffer = try testing.allocator.alignedAlloc(
        u8,
        @alignOf(vsr.Header),
        buffer_size,
    );
    defer testing.allocator.free(buffer);
    const written_bytes = try TestRunner.run(.{
        .prng = &prng,
        .element_size = element_size,
        .buffer = buffer,
        .batch_count = batch_count,
        .batch_elements = 0,
    });
    try testing.expectEqual(buffer_size, written_bytes);
}

// The maximum number of batches, when each one has one single element.
test "batch: maximum batches with a single element" {
    var prng = stdx.PRNG.from_seed_testing();

    const element_size = 128;
    const buffer_size = (1 * MiB) - @sizeOf(vsr.Header); // 1MiB message.
    const batch_count_max: u16 = multi_batch_count_max(.{
        .batch_size_min = element_size,
        .batch_size_limit = buffer_size,
    });

    const buffer = try testing.allocator.alignedAlloc(u8, @alignOf(vsr.Header), buffer_size);
    defer testing.allocator.free(buffer);
    const written_bytes = try TestRunner.run(.{
        .prng = &prng,
        .element_size = element_size,
        .buffer = buffer,
        .batch_count = batch_count_max,
        .batch_elements = 1,
    });

    const written_bytes_expected: usize =
        std.math.mulWide(u16, batch_count_max, element_size) +
        std.math.mulWide(u16, batch_count_max, @sizeOf(TrailerItem)) +
        @sizeOf(Postamble);
    assert(written_bytes_expected <= buffer_size);
    try testing.expectEqual(written_bytes_expected, written_bytes);
}

// The maximum number of elements on a single batch.
test "batch: maximum elements on a single batch" {
    var prng = stdx.PRNG.from_seed_testing();

    const element_size = 128;
    const buffer_size = (1 * MiB) - @sizeOf(vsr.Header); // 1MiB message.
    const batch_size_max = 8189; // maximum number of elements in a single-batch request.
    assert(batch_size_max == @divExact(buffer_size - element_size, element_size));

    const buffer = try testing.allocator.alignedAlloc(u8, @alignOf(vsr.Header), buffer_size);
    defer testing.allocator.free(buffer);
    const written_bytes = try TestRunner.run(.{
        .prng = &prng,
        .element_size = element_size,
        .buffer = buffer,
        .batch_count = 1,
        .batch_elements = batch_size_max,
    });
    try testing.expectEqual(buffer_size, written_bytes);
}

test "batch: invalid format" {
    var prng = stdx.PRNG.from_seed_testing();

    const element_size = 128;
    const buffer_size = (1 * MiB) - @sizeOf(vsr.Header); // 1MiB message.
    const buffer = try testing.allocator.alignedAlloc(u8, @alignOf(vsr.Header), buffer_size);
    defer testing.allocator.free(buffer);

    const batch_count = 10;
    const trailer_size = trailer_total_size(.{
        .element_size = element_size,
        .batch_count = batch_count,
    });

    var encoder = MultiBatchEncoder.init(buffer, .{
        .element_size = element_size,
    });
    var event_total_count: usize = 0;
    for (0..batch_count) |_| {
        const event_count: u16 = prng.int_inclusive(u16, 100);
        const batch_size: u32 = element_size * event_count;
        const writable = encoder.writable().?;
        try testing.expect(writable.len >= batch_size);
        encoder.add(batch_size);
        event_total_count += event_count;
    }
    const bytes_written = encoder.finish();

    try testing.expect(encoder.batch_count == batch_count);
    try testing.expect(bytes_written == (element_size * event_total_count) + trailer_size);

    _ = try MultiBatchDecoder.init(
        buffer[0..bytes_written],
        .{ .element_size = element_size },
    );

    try testing.expectError(error.MultiBatchInvalid, MultiBatchDecoder.init(
        buffer[0 .. bytes_written - element_size],
        .{ .element_size = element_size },
    ));
    try testing.expectError(error.MultiBatchInvalid, MultiBatchDecoder.init(
        buffer[element_size..bytes_written],
        .{ .element_size = element_size },
    ));
    try testing.expectError(error.MultiBatchInvalid, MultiBatchDecoder.init(
        buffer[0..bytes_written],
        .{ .element_size = element_size * 2 },
    ));
    try testing.expectError(error.MultiBatchInvalid, MultiBatchDecoder.init(
        buffer[0..bytes_written],
        .{ .element_size = element_size / 2 },
    ));

    const postamble: *Postamble = @ptrCast(@alignCast(
        buffer[bytes_written - @sizeOf(Postamble) ..],
    ));
    postamble.batch_count = batch_count + 1;
    try testing.expectError(error.MultiBatchInvalid, MultiBatchDecoder.init(
        buffer[0..bytes_written],
        .{ .element_size = element_size },
    ));
    postamble.batch_count = batch_count - 1;
    try testing.expectError(error.MultiBatchInvalid, MultiBatchDecoder.init(
        buffer[0..bytes_written],
        .{ .element_size = element_size },
    ));
}

const TestRunner = struct {
    fn run(options: struct {
        prng: *stdx.PRNG,
        element_size: u32,
        buffer: []align(16) u8,
        batch_count: u16,
        batch_elements: ?u16 = null,
    }) !usize {
        const ratio = stdx.PRNG.ratio;
        const BoundedArray = stdx.BoundedArrayType(u16, std.math.maxInt(u16));
        var expected: BoundedArray = .{};

        const trailer_size = trailer_total_size(.{
            .element_size = options.element_size,
            .batch_count = options.batch_count,
        });

        // Cleaning the buffer first, so it can assert the bytes.
        options.prng.fill(options.buffer);

        var encoder = MultiBatchEncoder.init(options.buffer, .{
            .element_size = options.element_size,
        });
        for (0..options.batch_count) |index| {
            const bytes_available = options.buffer.len - encoder.buffer_index - trailer_size;

            const elements_count: u16 = if (options.batch_elements) |batch_elements|
                batch_elements
            else random: {
                if (index == options.batch_count - 1) {
                    const batch_full = options.prng.chance(ratio(30, 100));
                    if (batch_full) {
                        break :random @intCast(@divFloor(bytes_available, options.element_size));
                    }
                }

                const batch_empty = options.prng.chance(ratio(30, 100));
                if (batch_empty) break :random 0;

                break :random @intCast(@divFloor(
                    options.prng.int_inclusive(usize, bytes_available),
                    options.element_size,
                ));
            };

            const slice = encoder.writable().?;
            const bytes_written = elements_count * options.element_size;
            assert(slice.len >= bytes_written);
            try testing.expect(slice.len >= bytes_written);
            @memset(std.mem.bytesAsSlice(u16, slice[0..bytes_written]), @intCast(index));
            encoder.add(bytes_written);

            expected.push(elements_count);
        }
        const bytes_written = encoder.finish();
        try testing.expect(encoder.batch_count == options.batch_count);

        var decoder = MultiBatchDecoder.init(
            options.buffer[0..bytes_written],
            .{ .element_size = options.element_size },
        ) catch unreachable;
        assert(decoder.batch_count() == encoder.batch_count);
        var batch_read_index: usize = 0;
        while (decoder.pop()) |batch| : (batch_read_index += 1) {
            const event_count = @divExact(batch.len, options.element_size);
            try testing.expect(expected.slice()[batch_read_index] == event_count);
            try testing.expect(std.mem.allEqual(
                u16,
                @alignCast(std.mem.bytesAsSlice(u16, batch)),
                @intCast(batch_read_index),
            ));
        }
        try testing.expect(options.batch_count == batch_read_index);

        return bytes_written;
    }
};
