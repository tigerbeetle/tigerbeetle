const stdx = @import("stdx");
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const vsr = @import("./vsr.zig");
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = vsr.Header;
const constants = vsr.constants;

/// MessageBuffer is the interface between a MessageBus and a Replica for passing batches of
/// messages while minimizing copies. It handles message framing, but doesn't do IO directly.
///
/// It is a producer-consumer ring buffer of bytes, with two twists:
/// - consumer can skip over or "suspend" certain slices, to return to them later.
/// - producer validates bytes against a checksum, and this validation is sticky: a message is
///   validated once, even if it is skipped many times.
///
/// Invariant: suspend_size ≤ process_size ≤ advance_size ≤ receive_size
pub const MessageBuffer = struct {
    /// The buffer passed to the kernel for reading into. This is Message rather than []u8 to
    /// enable zero-copy fast path. If a recv syscall reads exactly one message, no copying occurs.
    message: *Message,

    /// Suspended bytes, always a number of full messages.
    suspend_size: u32 = 0,
    /// Processed (consumed or suspended) bytes, always a number of full messages.
    process_size: u32 = 0,
    /// Bytes covered by a valid checksum, a number of full messages and maybe a header.
    advance_size: u32 = 0,
    /// The amount of bytes received from the kernel.
    receive_size: u32 = 0,

    // An error occurred, and the MessageBus should terminate connection.
    // Can be set by replica to indicate semantic errors, such as wrong cluster.
    invalid: ?InvalidReason = null,

    iterator_state: enum {
        idle,
        after_peek,
        after_consume_suspend,
    } = .idle,

    const InvalidReason = enum {
        header_checksum,
        header_size,
        header_cluster,
        body_checksum,
        misdirected,
    };

    fn invariants(buffer: *MessageBuffer) void {
        assert(buffer.suspend_size <= buffer.process_size);
        assert(buffer.process_size <= buffer.advance_size);
        assert(buffer.advance_size <= buffer.receive_size);
        if (buffer.invalid != null) {
            assert(buffer.suspend_size == 0);
            assert(buffer.process_size == 0);
            assert(buffer.advance_size == 0);
            assert(buffer.receive_size == 0);
            assert(buffer.iterator_state == .idle);
        }
    }

    pub fn init(pool: *MessagePool) MessageBuffer {
        return .{ .message = pool.get_message(null) };
    }

    pub fn deinit(buffer: *MessageBuffer, pool: *MessagePool) void {
        pool.unref(buffer.message);
        buffer.* = undefined;
    }

    /// Pass this to the kernel to read into.
    pub fn recv_slice(buffer: *MessageBuffer) []u8 {
        assert(buffer.receive_size < constants.message_size_max);
        assert(buffer.iterator_state == .idle);
        assert(buffer.invalid == null);
        return buffer.message.buffer[buffer.receive_size..];
    }

    /// When the kernel returns, informs the buffer about the read size.
    pub fn recv_advance(buffer: *MessageBuffer, size: u32) void {
        assert(buffer.iterator_state == .idle);
        assert(buffer.process_size == 0);
        assert(size > 0);
        assert(size <= constants.message_size_max);

        buffer.receive_size += size;
        assert(buffer.receive_size <= constants.message_size_max);
        buffer.advance();
    }

    pub fn invalidate(buffer: *MessageBuffer, reason: InvalidReason) void {
        assert(buffer.invalid == null);
        buffer.suspend_size = 0;
        buffer.process_size = 0;
        buffer.advance_size = 0;
        buffer.receive_size = 0;
        buffer.iterator_state = .idle;
        buffer.invalid = reason;
        buffer.invariants();
    }

    /// Advances the parsing state machine.
    /// Idempotent, but eagerly called whenever receive_size or process_size change.
    fn advance(buffer: *MessageBuffer) void {
        if (buffer.invalid == null) buffer.advance_header();
        if (buffer.invalid == null) buffer.advance_body();
        buffer.invariants();
    }

    fn advance_header(buffer: *MessageBuffer) void {
        assert(buffer.invalid == null);
        assert(buffer.advance_size <= buffer.receive_size);
        if (buffer.advance_size >= buffer.process_size + @sizeOf(Header)) {
            return; // Header is already known to be valid.
        }
        assert(buffer.advance_size == buffer.process_size);
        if (buffer.receive_size - buffer.process_size < @sizeOf(Header)) {
            return; // Header not received yet.
        }

        const header_bytes =
            buffer.message.buffer[buffer.process_size..][0..@sizeOf(Header)];

        var header: Header = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&header), header_bytes);

        if (!header.valid_checksum()) {
            buffer.invalidate(.header_checksum);
            return;
        }

        // Check that command is valid without materializing invalid Zig enum value.
        comptime assert(@sizeOf(vsr.Command) == @sizeOf(u8) and
            @FieldType(Header, "command") == vsr.Command);
        const command_raw: u8 = header_bytes[@offsetOf(Header, "command")];
        _ = std.meta.intToEnum(vsr.Command, command_raw) catch {
            vsr.fatal(
                .unknown_vsr_command,
                "unknown VSR command, crashing for safety " ++
                    "(command={d} protocol={d} replica={d} release={})",
                .{
                    command_raw,
                    header.protocol,
                    header.replica,
                    header.release,
                },
            );
        };

        if (header.size < @sizeOf(Header) or header.size > constants.message_size_max) {
            buffer.invalidate(.header_size);
            return;
        }
        assert(@sizeOf(Header) <= header.size and header.size <= constants.message_size_max);

        buffer.advance_size += @sizeOf(Header);
    }

    fn advance_body(buffer: *MessageBuffer) void {
        assert(buffer.invalid == null);
        if (buffer.advance_size < buffer.process_size + @sizeOf(Header)) {
            return; // Header not received yet.
        }

        const header = buffer.copy_header();

        if (buffer.receive_size - buffer.process_size < header.size) {
            return; // Body not received yet.
        }

        if (buffer.advance_size >= buffer.process_size + header.size) {
            return; // Body is already known to be valid.
        }

        assert(buffer.advance_size - buffer.process_size == @sizeOf(Header));
        const body = buffer.message.buffer[buffer.process_size..][@sizeOf(Header)..header.size];
        if (!header.valid_checksum_body(body)) {
            buffer.invalidate(.body_checksum);
            return;
        }
        buffer.advance_size += header.size - @sizeOf(Header);
    }

    /// Peek at the header for the incoming message. Necessitates a copy to guarantee alignment.
    fn copy_header(buffer: *const MessageBuffer) Header {
        assert(buffer.receive_size - buffer.process_size >= @sizeOf(Header));
        var header: Header = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&header),
            buffer.message.buffer[buffer.process_size..][0..@sizeOf(Header)],
        );
        return header;
    }

    pub fn has_message(buffer: *const MessageBuffer) bool {
        const valid_unprocessed = buffer.advance_size - buffer.process_size;
        if (valid_unprocessed >= @sizeOf(Header)) {
            const header = buffer.copy_header();
            if (valid_unprocessed >= header.size) {
                return true;
            }
        }
        return false;
    }

    /// MessageBuffer is also an iterator which must be driven to completion.
    /// A call to next_header must be immediately followed by a call to consume_message
    /// or suspend_message.
    pub fn next_header(buffer: *MessageBuffer) ?Header {
        maybe(buffer.invalid != null);

        switch (buffer.iterator_state) {
            .idle, .after_consume_suspend => {},
            else => unreachable,
        }

        const valid_unprocessed = buffer.advance_size - buffer.process_size;
        if (valid_unprocessed >= @sizeOf(Header)) {
            assert(buffer.invalid == null);
            const header = buffer.copy_header();
            if (valid_unprocessed >= header.size) {
                buffer.iterator_state = .after_peek;
                return header;
            }
        }

        // Move from this:
        // |  bytes  |     hole     |     bytes     |    hole    |
        //           ^suspend_size  ^process_size   ^receive_size
        //
        // To this:
        // |           bytes             |         hole          |
        // ^ suspend_size,process_size   ^ receive_size
        assert(buffer.suspend_size <= buffer.process_size);
        assert(buffer.process_size <= buffer.receive_size);

        if (buffer.suspend_size < buffer.process_size) {
            stdx.copy_left(
                .inexact,
                u8,
                buffer.message.buffer[buffer.suspend_size..],
                buffer.message.buffer[buffer.process_size..buffer.receive_size],
            );
        }
        buffer.receive_size -= (buffer.process_size - buffer.suspend_size);
        buffer.advance_size -= (buffer.process_size - buffer.suspend_size);
        buffer.suspend_size = 0;
        buffer.process_size = 0;
        buffer.iterator_state = .idle;

        // The purpose of tracking advance_size across iterations is to "cache" checksum validation.
        // As a sanity check, assert that advance-after-back-shift is indeed a no-op.
        const advance_size_idempotent = buffer.advance_size;
        buffer.advance();
        assert(buffer.advance_size == advance_size_idempotent);

        return null;
    }

    pub fn consume_message(
        buffer: *MessageBuffer,
        pool: *MessagePool,
        header: *const Header,
    ) *Message {
        assert(buffer.iterator_state == .after_peek);
        assert(buffer.advance_size - buffer.process_size >= header.size);
        assert(buffer.invalid == null);
        defer buffer.iterator_state = .after_consume_suspend;

        if (buffer.process_size == 0 and buffer.receive_size == header.size) {
            assert(buffer.message.header.checksum == header.checksum);

            assert(buffer.suspend_size == 0);
            buffer.process_size = 0;
            buffer.receive_size = 0;
            buffer.advance_size = 0;
            buffer.advance();
            assert(buffer.advance_size == 0);

            defer buffer.message = pool.get_message(null);

            return buffer.message;
        }

        const message = pool.get_message(null);
        defer pool.unref(message);

        stdx.copy_disjoint(
            .inexact,
            u8,
            message.buffer,
            buffer.message.buffer[buffer.process_size..][0..header.size],
        );
        buffer.process_size += header.size;
        assert(buffer.process_size <= buffer.receive_size);
        buffer.advance();

        assert(message.header.checksum == header.checksum);
        return message.ref();
    }

    pub fn suspend_message(buffer: *MessageBuffer, header: *const Header) void {
        assert(buffer.iterator_state == .after_peek);
        assert(buffer.advance_size - buffer.process_size >= header.size);
        assert(buffer.invalid == null);
        assert(header.size <= constants.message_size_max);
        assert(std.mem.eql(
            u8,
            std.mem.asBytes(header),
            buffer.message.buffer[buffer.process_size..][0..@sizeOf(Header)],
        ));
        assert(buffer.suspend_size <= buffer.process_size);

        defer buffer.iterator_state = .after_consume_suspend;

        if (buffer.suspend_size < buffer.process_size) {
            // Move from this:
            // |  bytes  |    hole     |    message    |     bytes    |
            //           ^suspend_size ^process_size                  ^receive_size
            //
            // To this:
            // | bytes |    message    |     hole      |     bytes    |
            //                         ^suspend_size   ^process_size  ^receive_size
            stdx.copy_left(
                .inexact,
                u8,
                buffer.message.buffer[buffer.suspend_size..],
                buffer.message.buffer[buffer.process_size..][0..header.size],
            );
        }

        buffer.suspend_size += header.size;
        buffer.process_size += header.size;
        buffer.advance();
    }
};

test "MessageBuffer fuzz" {
    // Generate a byte buffer with a bunch of prepares side-by-side.
    // Optionally corrupt a single bit in the buffer.
    // Feed the buffer in chunks of varying length to the MessageBuffer, verify that all messages
    // are received unless a fault is detected.
    const messages_max = 100;

    var prng = stdx.PRNG.from_seed_testing();
    const gpa = std.testing.allocator;

    var buffer: []u8 = try gpa.alloc(u8, 5 * constants.message_size_max);
    defer gpa.free(buffer);

    for (0..100) |_| {
        const fault = prng.boolean();
        var total_size: u32 = 0;
        var headers: stdx.BoundedArrayType(Header, messages_max) = .{};
        for (0..messages_max) |_| {
            const message_size: u32 = switch (prng.chances(.{
                .min = 10,
                .max = 10,
                .random = 80,
            })) {
                .min => @sizeOf(Header),
                .max => constants.message_size_max,
                .random => prng.range_inclusive(u32, @sizeOf(Header), constants.message_size_max),
            };

            if (total_size + message_size > buffer.len) {
                break;
            }

            var header: vsr.Header.Prepare = .{
                .cluster = 1,
                .view = 1,
                .command = .prepare,
                .parent = prng.int(u128),
                .request_checksum = prng.int(u128),
                .checkpoint_id = prng.int(u128),
                .client = 1,
                .commit = 10,
                .timestamp = 999,
                .request = 1,
                .operation = .register,
                .release = vsr.Release.minimum,
                .op = 1,
                .size = message_size,
            };
            const body = buffer[total_size..][@sizeOf(Header)..header.size];
            prng.fill(body);
            header.set_checksum_body(body);
            header.set_checksum();
            stdx.copy_disjoint(
                .exact,
                u8,
                buffer[total_size..][0..@sizeOf(Header)],
                std.mem.asBytes(&header),
            );
            total_size += header.size;
            headers.push(header.frame_const().*);
        }

        if (fault) {
            const byte_index = prng.index(buffer[0..total_size]);
            const bit_index = prng.int_inclusive(u3, 7);
            buffer[byte_index] ^= @as(u8, 1) << bit_index;
        }

        var pool = try MessagePool.init(gpa, .{ .replica = .{
            .members_count = 6,
            .pipeline_requests_limit = 1,
            .message_bus = .testing,
        } });
        defer pool.deinit(gpa);

        var message_buffer = MessageBuffer.init(&pool);
        defer message_buffer.deinit(&pool);

        var recv_size: u32 = 0;
        while (headers.count() > 0) {
            if (message_buffer.receive_size < constants.message_size_max and
                recv_size < total_size)
            {
                const recv_slice = message_buffer.recv_slice();
                const chunk_size = @min(
                    prng.range_inclusive(u32, 1, @intCast(recv_slice.len)),
                    total_size - recv_size,
                );
                stdx.copy_disjoint(
                    .exact,
                    u8,
                    recv_slice[0..chunk_size],
                    buffer[recv_size..][0..chunk_size],
                );
                message_buffer.recv_advance(chunk_size);
                recv_size += chunk_size;
            }

            var header_index: u32 = 0;
            while (message_buffer.next_header()) |header| {
                message_buffer.invariants();
                if (prng.boolean()) {
                    const message = message_buffer.consume_message(&pool, &header);
                    defer pool.unref(message);

                    assert(stdx.equal_bytes(Header, message.header, &headers.get(header_index)));
                    _ = headers.ordered_remove(header_index);
                } else {
                    message_buffer.suspend_message(&header);
                    header_index += 1;
                }
            }
            assert(message_buffer.iterator_state == .idle);
            if (message_buffer.invalid) |reason| {
                if (!fault) std.debug.panic("invalid without faults: {s}", .{@tagName(reason)});
                break;
            }
        }
        if (fault) {
            assert(message_buffer.invalid != null);
        } else {
            assert(message_buffer.invalid == null);
            assert(headers.count() == 0);
        }
    }
}
