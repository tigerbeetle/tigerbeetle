const std = @import("std");
const testing = std.testing;

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const Postamble = packed struct(u32) {
    /// Number of batches.
    batch_count: u16,
    reserved: u16,
};

const TrailerItem = packed struct(u32) {
    /// The size in bytes of each batch.
    /// The size of each element is operation-specific, and padding bytes may be added
    /// at the beginning of the batch to maintain alignment.
    /// Use `std.mem.alignForward` to compute the correct starting position for the required
    /// alignment. Padding bytes are zeroed.
    size: u24,

    /// The batch operation.
    operation: vsr.Operation,

    comptime {
        assert(@sizeOf(TrailerItem) == @sizeOf(Postamble));
        assert(@alignOf(TrailerItem) == @alignOf(Postamble));
    }
};

/// The trailer is a `[]BatchItem` containing the operation and the number of elements
/// in each batch. To encode the trailer, `batch_count * @sizeOf(BatchItem)` bytes are
/// needed, plus a `Postamble`.
fn trailer_total_size(batch_count: u16) usize {
    return (@as(usize, batch_count) * @sizeOf(TrailerItem)) + @sizeOf(Postamble);
}

fn align_padding(target: anytype, alignment: usize) u16 {
    const address: usize = switch (@TypeOf(target)) {
        []u8, []const u8 => @intFromPtr(target.ptr),
        usize => target,
        else => unreachable,
    };
    assert(alignment > 0);

    const aligned_address: usize = std.mem.alignForward(
        usize,
        address,
        alignment,
    );
    if (aligned_address == address) return 0;
    assert(aligned_address > address);

    const padding: u16 = @intCast(aligned_address - address);
    return padding;
}

/// Calculates the total size required to encode the payload and the trailer of a batch.
fn EncoderCountingType(
    comptime Context: type,
    comptime adapter: struct {
        is_valid: fn (Context, vsr.Operation) bool,
        element_size: fn (Context, vsr.Operation) usize,
        alignment: fn (Context, vsr.Operation) usize,
    },
) type {
    return struct {
        fn total_size(options: struct {
            context: Context,
            current_payload_size: usize,
            current_batch_count: u16,
            next_operation: vsr.Operation,
            next_payload_size: usize,
        }) struct {
            payload_size: u32,
            trailer_size: u32,
        } {
            assert(adapter.is_valid(options.context, options.next_operation));
            maybe(options.current_payload_size == 0);
            maybe(options.current_batch_count == 0);
            maybe(options.next_payload_size == 0);

            const element_size = adapter.element_size(options.context, options.next_operation);
            assert(element_size > 0 or options.next_payload_size == 0);
            assert(element_size == 0 or options.next_payload_size % element_size == 0);

            const alignment: usize = adapter.alignment(options.context, options.next_operation);
            const padding: u16 = padding: {
                if (options.next_payload_size == 0) break :padding 0;
                const padding: u16 = align_padding(
                    options.current_payload_size,
                    alignment,
                );
                // Assuming aligned buffers, so the first batch will not require padding.
                assert(padding == 0 or options.current_payload_size > 0);
                assert(std.mem.isAligned(
                    options.current_payload_size + padding,
                    alignment,
                ));
                break :padding padding;
            };

            const payload_size: usize = options.current_payload_size +
                options.next_payload_size + padding;
            const trailer_padding: u16 = align_padding(payload_size, @alignOf(TrailerItem));
            const trailer_size: usize = trailer_total_size(options.current_batch_count + 1);
            return .{
                .payload_size = @intCast(payload_size),
                .trailer_size = @intCast(trailer_size + trailer_padding),
            };
        }
    };
}

pub fn BatchDecoderType(
    comptime Context: type,
    comptime adapter: struct {
        is_valid: fn (Context, vsr.Operation) bool,
        element_size: fn (Context, vsr.Operation) usize,
        alignment: fn (Context, vsr.Operation) usize,
    },
) type {
    return struct {
        const BatchDecoder = @This();

        pub const BatchItem = struct {
            operation: vsr.Operation,
            batched: []const u8,
        };

        /// The message payload, excluding the trailer.
        payload: []const u8,
        /// The batching metadata, excluding the postamble.
        trailer_items: []const TrailerItem,

        context: Context,
        batch_index: u16,
        payload_index: usize,

        /// Calculates the total size required to encode the payload and the trailer of a batch.
        pub const encoded_total_size = EncoderCountingType(Context, .{
            .is_valid = adapter.is_valid,
            .element_size = adapter.element_size,
            .alignment = adapter.alignment,
        }).total_size;

        pub fn init(
            context: Context,
            /// The message body used, including the trailer.
            body: []const u8,
        ) error{BatchInvalid}!BatchDecoder {
            if (body.len < @sizeOf(Postamble)) return error.BatchInvalid;

            if (!std.mem.isAligned(
                @intFromPtr(&body[body.len - @sizeOf(Postamble)]),
                @alignOf(Postamble),
            )) {
                return error.BatchInvalid;
            }

            const postamble: *const Postamble = @alignCast(std.mem.bytesAsValue(
                Postamble,
                body[body.len - @sizeOf(Postamble) ..],
            ));
            if (postamble.batch_count == 0) return error.BatchInvalid;
            if (postamble.reserved != 0) return error.BatchInvalid;

            const trailer_size = trailer_total_size(postamble.batch_count);
            if (body.len < trailer_size) return error.BatchInvalid;

            const payload_end = body.len - trailer_size;
            const payload: []const u8 = body[0..payload_end];
            const trailer: []const u8 = body[payload_end..];
            assert(body.len == payload.len + trailer.len);
            assert(trailer.len == trailer_size);
            maybe(payload.len == 0);
            if (!std.mem.isAligned(@intFromPtr(trailer.ptr), @alignOf(TrailerItem))) {
                return error.BatchInvalid;
            }

            const trailer_items: []const TrailerItem = @alignCast(
                std.mem.bytesAsSlice(TrailerItem, trailer[0 .. trailer.len - @sizeOf(Postamble)]),
            );
            if (trailer_items.len != postamble.batch_count) {
                return error.BatchInvalid;
            }

            var elements_size_total: usize = 0;
            for (0..trailer_items.len) |index| {
                // Batch metadata is stacked from the end of the message, so the last element
                // of the array corresponds to the first batch added.
                // Ordering is important to correctly skip padding bytes between batches.
                const trailer_item: *const TrailerItem =
                    &trailer_items[trailer_items.len - index - 1];

                // Validate `operation`:
                if (!adapter.is_valid(context, trailer_item.operation)) {
                    return error.BatchInvalid;
                }

                // Validate the batch `size`:
                maybe(trailer_item.size == 0);
                if (elements_size_total + trailer_item.size > payload.len) {
                    return error.BatchInvalid;
                }

                // Validate the batch alignment:
                const alignment = adapter.alignment(context, trailer_item.operation);
                const batch_padding: u16 = padding: {
                    if (trailer_item.size == 0) break :padding 0;
                    const batch_padding: u16 = align_padding(
                        payload[elements_size_total..],
                        alignment,
                    );
                    if (batch_padding >= trailer_item.size) {
                        return error.BatchInvalid;
                    }
                    if (payload.len <= elements_size_total + batch_padding) {
                        return error.BatchInvalid;
                    }
                    if (!std.mem.isAligned(
                        @intFromPtr(&payload[elements_size_total + batch_padding]),
                        alignment,
                    )) {
                        return error.BatchInvalid;
                    }
                    break :padding batch_padding;
                };

                // Validate the padding bytes:
                if (!stdx.zeroed(payload[elements_size_total..][0..batch_padding])) {
                    return error.BatchInvalid;
                }

                // Validate the element size:
                const element_size = adapter.element_size(context, trailer_item.operation);
                if ((trailer_item.size - batch_padding) % element_size != 0) {
                    return error.BatchInvalid;
                }

                elements_size_total += trailer_item.size;
            }
            if (elements_size_total > payload.len) {
                return error.BatchInvalid;
            }

            const trailer_padding: usize = align_padding(
                elements_size_total,
                @alignOf(TrailerItem),
            );
            if (elements_size_total + trailer_padding != payload.len) {
                return error.BatchInvalid;
            }
            if (trailer_padding > 0 and !stdx.zeroed(payload[payload.len - trailer_padding ..])) {
                return error.BatchInvalid;
            }

            return .{
                .context = context,
                .payload = payload[0 .. payload.len - trailer_padding],
                .trailer_items = trailer_items,
                .batch_index = 0,
                .payload_index = 0,
            };
        }

        pub fn reset(
            self: *BatchDecoder,
        ) void {
            self.* = .{
                .payload = self.payload,
                .trailer_items = self.trailer_items,
                .context = self.context,
                .payload_index = 0,
                .batch_index = 0,
            };
        }

        pub fn batch_count(self: *const BatchDecoder) usize {
            return self.trailer_items.len;
        }

        pub fn set_batch_index(
            self: *BatchDecoder,
            /// The batch index to move.
            batch_index: usize,
        ) void {
            assert(batch_index < self.trailer_items.len);
            assert(self.trailer_items.len > 0);
            maybe(self.payload.len == 0);
            self.reset();

            for (0..batch_index) |index| {
                assert(self.batch_index == index);
                const moved = self.move_next();
                assert(moved);
            }
            assert(self.batch_index == batch_index);
        }

        pub fn pop(self: *BatchDecoder) ?BatchItem {
            const batch_item = self.peek() orelse return null;
            const moved = self.move_next();
            assert(moved);
            return batch_item;
        }

        pub fn peek(self: *const BatchDecoder) ?BatchItem {
            if (self.batch_index == self.trailer_items.len) return null;
            assert(self.trailer_items.len > 0);
            assert(self.payload_index <= self.payload.len);
            maybe(self.payload.len == 0);

            // Batch metadata is written from the end of the message, so the last
            // element corresponds to the first batch.
            const trailer_item: *const TrailerItem =
                &self.trailer_items[self.trailer_items.len - self.batch_index - 1];
            assert(adapter.is_valid(self.context, trailer_item.operation));
            assert(trailer_item.size <= self.payload.len);
            maybe(trailer_item.size == 0);

            const slice: []const u8 = slice: {
                if (trailer_item.size == 0) {
                    assert(self.payload_index <= self.payload.len);
                    break :slice &.{};
                }

                assert(self.payload_index < self.payload.len);
                assert(self.payload_index + trailer_item.size <= self.payload.len);
                const alignment: usize = adapter.alignment(self.context, trailer_item.operation);
                const padding: u16 = align_padding(
                    self.payload[self.payload_index..],
                    alignment,
                );
                assert(padding < trailer_item.size);

                const slice: []const u8 =
                    self.payload[self.payload_index + padding ..][0 .. trailer_item.size - padding];
                assert(std.mem.isAligned(@intFromPtr(slice.ptr), alignment));

                const element_size = adapter.element_size(self.context, trailer_item.operation);
                assert(slice.len > 0);
                assert(slice.len % element_size == 0);
                break :slice slice;
            };

            return .{
                .operation = trailer_item.operation,
                .batched = slice,
            };
        }

        pub fn move_next(self: *BatchDecoder) bool {
            if (self.batch_index == self.trailer_items.len) return false;

            const trailer_item: *const TrailerItem =
                &self.trailer_items[self.trailer_items.len - self.batch_index - 1];

            self.payload_index += trailer_item.size;
            self.batch_index += 1;

            assert(self.batch_index <= self.trailer_items.len);
            assert(self.payload_index <= self.payload.len);
            return true;
        }
    };
}

pub fn BatchEncoderType(
    comptime Context: type,
    comptime adapter: struct {
        is_valid: fn (Context, vsr.Operation) bool,
        element_size: fn (Context, vsr.Operation) usize,
        alignment: fn (Context, vsr.Operation) usize,
    },
) type {
    return struct {
        const BatchEncoder = @This();

        context: Context,
        buffer: ?[]u8,

        batch_count: u16,
        buffer_index: usize,

        /// Calculates the total size required to encode the payload and the trailer of a batch.
        pub const encoded_total_size = EncoderCountingType(Context, .{
            .is_valid = adapter.is_valid,
            .element_size = adapter.element_size,
            .alignment = adapter.alignment,
        }).total_size;

        pub fn init(context: Context, buffer: []u8) BatchEncoder {
            assert(buffer.len > @sizeOf(Postamble));

            // The end of the buffer must be aligned to store the `Postamble`.
            // If not, the buffer is reduced to achieve the required alignment.
            const postamble_address: usize = @intFromPtr(&buffer[buffer.len - @sizeOf(Postamble)]);
            const aligned_address: usize = std.mem.alignBackward(
                usize,
                postamble_address,
                @alignOf(Postamble),
            );
            const trimming_bytes: usize = trimming: {
                if (aligned_address == postamble_address) break :trimming 0;
                assert(aligned_address < postamble_address);
                const bytes = postamble_address - aligned_address;
                assert(buffer.len > @sizeOf(Postamble) + bytes);
                break :trimming bytes;
            };
            assert(std.mem.isAligned(
                @intFromPtr(&buffer[buffer.len - @sizeOf(Postamble) - trimming_bytes]),
                @alignOf(Postamble),
            ));

            // The buffer must be large enough for at least one batch.
            assert(buffer.len - trimming_bytes >= trailer_total_size(1));

            return .{
                .context = context,
                .buffer = buffer[0 .. buffer.len - trimming_bytes],
                .batch_count = 0,
                .buffer_index = 0,
            };
        }

        pub fn reset(self: *BatchEncoder) void {
            assert(self.buffer != null);
            self.* = .{
                .context = self.context,
                .buffer = self.buffer,
                .batch_count = 0,
                .buffer_index = 0,
            };
        }

        /// Returns a writable slice aligned and sized appropriately for the current operation.
        /// May return `null` if there isn't enough space in the buffer to add a new element
        /// to the trailer. The returned slice may have a length of zero if the remaining buffer
        /// isn't large enough to hold at least one element of the current operation.
        pub fn writable(self: *const BatchEncoder, operation: vsr.Operation) ?[]u8 {
            maybe(self.batch_count == 0);

            // Takes into account extra trailer bytes that will need to be included.
            const trailer_size: usize = trailer_total_size(
                self.batch_count + 1,
            );

            const buffer: []u8 = self.buffer.?;
            const padding: u16 = align_padding(
                buffer[self.buffer_index..],
                adapter.alignment(self.context, operation),
            );
            // The first batch must not include padding since the buffer is expected to be aligned.
            assert(padding == 0 or self.buffer_index > 0);

            if (buffer.len < self.buffer_index + trailer_size) {
                // Insufficient space for one more batch.
                return null;
            }

            if (constants.verify) {
                assert(buffer.len >= trailer_size);
                const trailer: []u8 = buffer[buffer.len - trailer_size ..];
                const trailer_items: []TrailerItem = @alignCast(std.mem.bytesAsSlice(
                    TrailerItem,
                    trailer[0 .. trailer.len - @sizeOf(Postamble)],
                ));
                assert(trailer_items.len == self.batch_count + 1);
                trailer_items[0] = .{
                    .operation = operation, // Set the current operation for asserting later.
                    .size = 0,
                };
            }

            if (buffer.len < self.buffer_index + padding + trailer_size) {
                return &.{};
            }

            // Get an aligned slice.
            const aligned: []u8 = buffer[self.buffer_index + padding .. buffer.len - trailer_size];

            // Trim the slice to the maximum number of elements that can fit.
            const element_size = adapter.element_size(self.context, operation);
            const size: usize = @divFloor(aligned.len, element_size) * element_size;

            return aligned[0..size];
        }

        /// Records how many bytes were written in the slice previously acquired by `writable()`.
        /// The same operation must be used in both functions.
        pub fn add(self: *BatchEncoder, operation: vsr.Operation, bytes_written: usize) void {
            maybe(self.batch_count == 0);
            assert(adapter.is_valid(self.context, operation));

            const element_size = adapter.element_size(self.context, operation);
            assert(element_size > 0 or bytes_written == 0);
            assert(element_size == 0 or bytes_written % element_size == 0);

            const buffer: []u8 = self.buffer.?;
            const alignment: usize = adapter.alignment(self.context, operation);
            const padding: u16 = padding: {
                if (bytes_written == 0) break :padding 0;
                const padding: u16 = align_padding(
                    buffer[self.buffer_index..],
                    alignment,
                );
                assert(std.mem.isAligned(
                    @intFromPtr(&buffer[self.buffer_index + padding]),
                    alignment,
                ));
                break :padding padding;
            };

            @memset(buffer[self.buffer_index..][0..padding], 0);
            self.batch_count += 1;
            self.buffer_index += bytes_written + padding;

            const trailer_size = trailer_total_size(self.batch_count);
            assert(buffer.len >= self.buffer_index + trailer_size);

            const trailer: []u8 = buffer[buffer.len - trailer_size ..];
            const trailer_items: []TrailerItem = @alignCast(std.mem.bytesAsSlice(
                TrailerItem,
                trailer[0 .. trailer.len - @sizeOf(Postamble)],
            ));
            assert(trailer_items.len == self.batch_count);
            if (constants.verify) {
                assert(trailer_items[0].operation == operation);
                assert(trailer_items[0].size == 0);
            }

            // Batch metadata is stacked from the end of the message, so the first element
            // of the array corresponds to the last batch added.
            trailer_items[0] = .{
                .operation = operation,
                .size = @intCast(bytes_written + padding),
            };
        }

        /// Finalizes the batch by writing the trailer with proper encoding.
        /// Returns the total number of bytes written (payload + padding + trailer).
        /// At least one batch must be inserted, and the encoder should not be used after
        /// being finished.
        pub fn finish(self: *BatchEncoder) usize {
            assert(self.batch_count > 0);

            const buffer = self.buffer.?;
            assert(buffer.len > self.buffer_index);
            maybe(self.buffer_index == 0);

            const trailer_size = trailer_total_size(self.batch_count);
            assert(buffer.len >= self.buffer_index + trailer_size);

            // While batches are being encoded, the trailer is written at the end of the buffer.
            // Once all batches are encoded, the trailer needs to be moved closer to the last
            // element written.
            const trailer_padding: u16 = align_padding(
                buffer[self.buffer_index..],
                @alignOf(TrailerItem),
            );
            assert(buffer.len >= self.buffer_index + trailer_padding + trailer_size);
            assert(std.mem.isAligned(
                @intFromPtr(&buffer[self.buffer_index + trailer_padding]),
                @alignOf(TrailerItem),
            ));

            const trailer_source: []const u8 = buffer[buffer.len - trailer_size ..];
            const trailer_target: []u8 =
                buffer[self.buffer_index + trailer_padding ..][0..trailer_size];
            assert(trailer_target.len == trailer_source.len);
            assert(@intFromPtr(trailer_target.ptr) <= @intFromPtr(trailer_source.ptr));
            if (trailer_target.ptr != trailer_source.ptr) {
                stdx.copy_left(
                    .exact,
                    u8,
                    trailer_target,
                    trailer_source,
                );
            }
            @memset(buffer[self.buffer_index..][0..trailer_padding], 0);

            const postamble: *Postamble = @alignCast(std.mem.bytesAsValue(
                Postamble,
                trailer_target[trailer_target.len - @sizeOf(Postamble) ..],
            ));
            postamble.* = .{
                .batch_count = self.batch_count,
                .reserved = 0,
            };

            // Reset the encoder to prevent misuse.
            defer self.* = .{
                .buffer = null,
                .batch_count = 0,
                .buffer_index = 0,
                .context = self.context,
            };

            const bytes_written = self.buffer_index + trailer_size + trailer_padding;
            if (constants.verify) {
                const BatchDecoder = BatchDecoderType(Context, .{
                    .is_valid = adapter.is_valid,
                    .element_size = adapter.element_size,
                    .alignment = adapter.alignment,
                });
                assert(BatchDecoder.init(
                    self.context,
                    buffer[0..bytes_written],
                ) != error.BatchInvalid);
            }

            return bytes_written;
        }
    };
}

const test_batch = struct {
    const Context = struct {
        const empty: Context = .{};
    };
    const Operation = enum(u8) {
        size_2 = constants.vsr_operations_reserved + 1,
        size_4 = constants.vsr_operations_reserved + 2,
        size_8 = constants.vsr_operations_reserved + 3,
        size_16 = constants.vsr_operations_reserved + 4,
        size_32 = constants.vsr_operations_reserved + 5,
        size_64 = constants.vsr_operations_reserved + 6,
        size_128 = constants.vsr_operations_reserved + 7,
        size_256 = constants.vsr_operations_reserved + 8,

        fn to_vsr(operation: Operation) vsr.Operation {
            return @enumFromInt(@intFromEnum(operation));
        }

        fn from_vsr(operation: vsr.Operation) Operation {
            return @enumFromInt(@intFromEnum(operation));
        }
    };

    fn ElementType(comptime operation: Operation) type {
        const size = Adapter.element_size(
            Context.empty,
            operation.to_vsr(),
        );

        const alignment = Adapter.alignment(
            Context.empty,
            operation.to_vsr(),
        );

        const Element = extern struct {
            value: [size]u8 align(alignment),
        };
        assert(@sizeOf(Element) == size);
        assert(@alignOf(Element) == alignment);

        return Element;
    }

    const Adapter = struct {
        fn is_valid(context: Context, vsr_operation: vsr.Operation) bool {
            _ = context;
            if (vsr_operation.vsr_reserved()) return false;
            const pow = @intFromEnum(vsr_operation) - vsr.constants.vsr_operations_reserved;
            return pow >= 1 and pow <= 8; // Test sizes between 2^1 and 2^8.
        }

        fn element_size(context: Context, vsr_operation: vsr.Operation) usize {
            assert(is_valid(context, vsr_operation));
            const pow = @intFromEnum(vsr_operation) - vsr.constants.vsr_operations_reserved;
            return std.math.pow(usize, 2, pow);
        }

        fn alignment(context: Context, vsr_operation: vsr.Operation) usize {
            assert(is_valid(context, vsr_operation));
            const operation: Operation = @enumFromInt(@intFromEnum(vsr_operation));
            return switch (operation) {
                .size_2 => @alignOf(u16),
                .size_4 => @alignOf(u32),
                .size_8 => @alignOf(u64),
                .size_16 => @alignOf(u128),
                .size_32 => @alignOf(u32),
                .size_64 => @alignOf(u64),
                .size_128 => @alignOf(u128),
                .size_256 => @alignOf(u128),
            };
        }
    };

    const BatchEncoder = BatchEncoderType(Context, .{
        .is_valid = Adapter.is_valid,
        .element_size = Adapter.element_size,
        .alignment = Adapter.alignment,
    });

    const BatchDecoder = BatchDecoderType(Context, .{
        .is_valid = Adapter.is_valid,
        .element_size = Adapter.element_size,
        .alignment = Adapter.alignment,
    });

    fn run(allocator: std.mem.Allocator, options: struct {
        random: std.rand.Random,
        buffer: []u8,
    }) !void {
        // Generate the batch plan.
        const batch_count_max = options.random.intRangeAtMost(usize, 1, options.buffer.len);
        const Batch = struct { operation: Operation, size: u32 };
        var batches = stdx.BoundedArrayType(Batch, 8190){};
        var expect_payload_size: u32 = 0;
        var expect_trailer_size: u32 = 0;
        for (0..batch_count_max) |_| {
            const batch_operation = options.random.enumValue(Operation);
            const batch_element_size = Adapter.element_size(.{}, batch_operation.to_vsr());
            const batch_element_count_max: u32 = @intCast(@divFloor(
                options.buffer.len - expect_payload_size - expect_trailer_size,
                batch_element_size,
            ));
            const batch_element_count: u32 =
                switch (options.random.enumValue(enum { zero, one, random })) {
                .zero => 0,
                .one => 1,
                .random => options.random.intRangeAtMost(u32, 1, batch_element_count_max + 1),
            };

            const total_size = BatchEncoder.encoded_total_size(.{
                .context = .{},
                .current_payload_size = expect_payload_size,
                .current_batch_count = @intCast(batches.count()),
                .next_operation = batch_operation.to_vsr(),
                .next_payload_size = batch_element_count * batch_element_size,
            });

            if (total_size.payload_size + total_size.trailer_size <= options.buffer.len) {
                batches.append_assume_capacity(.{
                    .operation = batch_operation,
                    .size = @intCast(batch_element_count * batch_element_size),
                });
                expect_payload_size = total_size.payload_size;
                expect_trailer_size = total_size.trailer_size;
            } else {
                break;
            }
        }
        assert(batches.count() > 0);
        assert(expect_payload_size + expect_trailer_size <= options.buffer.len);

        var payloads_encoded: u32 = 0;
        var payloads_decoded: u32 = 0;
        var payloads_bytes = try allocator.alloc(u8, options.buffer.len);
        defer allocator.free(payloads_bytes);
        options.random.bytes(payloads_bytes);

        // Encoder will ignore and overwrite any existing content in the target buffer.
        options.random.bytes(options.buffer);

        // Encode.
        var encoder = BatchEncoder.init(.{}, options.buffer);
        for (batches.const_slice()) |batch| {
            const writable = encoder.writable(batch.operation.to_vsr()) orelse break;
            const batch_payload_target = writable[0..batch.size];
            const batch_payload_source = payloads_bytes[payloads_encoded..][0..batch.size];
            encoder.add(batch.operation.to_vsr(), batch.size);
            stdx.copy_disjoint(.exact, u8, batch_payload_target, batch_payload_source);
            payloads_encoded += batch.size;
        }
        const encoder_bytes_written = encoder.finish();
        assert(encoder_bytes_written == expect_payload_size + expect_trailer_size);

        // Decode.
        var decoder = try BatchDecoder.init(.{}, options.buffer[0..encoder_bytes_written]);
        for (batches.const_slice()) |batch| {
            const expect_batch_bytes = payloads_bytes[payloads_decoded..][0..batch.size];
            payloads_decoded += batch.size;

            const decoded_batch = decoder.pop().?;
            const decoded_batch_bytes = decoded_batch.batched;
            const batch_element_size = Adapter.element_size(.{}, batch.operation.to_vsr());
            try testing.expectEqual(batch.operation, Operation.from_vsr(decoded_batch.operation));
            try testing.expectEqual(batch.size, decoded_batch_bytes.len);
            try testing.expect(decoded_batch_bytes.len % batch_element_size == 0);
            try testing.expect(std.mem.eql(u8, expect_batch_bytes, decoded_batch_bytes));
            if (decoded_batch_bytes.len > 0) {
                try testing.expect(std.mem.isAligned(
                    @intFromPtr(decoded_batch_bytes.ptr),
                    Adapter.alignment(.{}, batch.operation.to_vsr()),
                ));
            }

            switch (batch.operation) {
                inline else => |operation_comptime| {
                    const Element = ElementType(operation_comptime);
                    const slice: []const Element = @alignCast(std.mem.bytesAsSlice(
                        Element,
                        decoded_batch_bytes,
                    ));
                    try testing.expectEqual(
                        @as(usize, @divExact(decoded_batch.batched.len, batch_element_size)),
                        slice.len,
                    );
                },
            }
        }
        assert(decoder.pop() == null);
    }
};

test "batch: encode/decode" {
    var rng = std.rand.DefaultPrng.init(42);
    const message_body_size_min = constants.sector_size - @sizeOf(vsr.Header);
    const message_body_size_max = (1024 * 1024) - @sizeOf(vsr.Header);
    const buffer = try testing.allocator.alignedAlloc(
        u8,
        @alignOf(vsr.Header),
        message_body_size_max,
    );
    defer testing.allocator.free(buffer);

    const random = rng.random();
    for (0..1024) |_| {
        const buffer_size: usize = random.intRangeAtMost(
            usize,
            message_body_size_min,
            message_body_size_max,
        );
        try test_batch.run(testing.allocator, .{
            .random = random,
            .buffer = buffer[0..buffer_size],
        });
    }
}
