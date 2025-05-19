const stdx = @import("./stdx.zig");
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
pub const MessageBuffer = struct {
    /// The buffer passed to the kernel for reading into. This is Message rather than []u8 to
    /// enable zero-copy fast path. If a recv syscall reads exactly one message, no copying occurs.
    message: *Message,
    /// For the case where a single recv fetched more than one message, the amount of bytes
    /// consumed from the beginning of message.buffer.
    consume_size: u32 = 0,
    /// The amount of bytes received from the kernel.
    /// Only consume_size..receive_size byte range of message.buffer is initialized.
    receive_size: u32 = 0,

    /// Which peer we are receiving from, inferred from the received messages. Set during parsing,
    /// and used by the message bus to map connections to replicas and clients.
    peer: vsr.Peer = .unknown,

    // Parsing state machine:
    // - valid_header: Header checksum is correct.
    // - valid_body: Body checksum is correct, a full message can be consumed.
    // - invalid: An error occurred, and the MessageBus should terminate connection.
    //            Can be set by replica to indicate semantic errors, such as wrong cluster.
    valid_header: bool = false,
    valid_body: bool = false,
    invalid: ?InvalidReason = null,

    const InvalidReason = enum {
        header_checksum,
        header_size,
        header_cluster,
        body_checksum,
        misdirected,
    };

    pub fn init(pool: *MessagePool) MessageBuffer {
        return .{ .message = pool.get_message(null) };
    }

    pub fn deinit(buffer: *MessageBuffer, pool: *MessagePool) void {
        pool.unref(buffer.message);
        buffer.* = undefined;
    }

    /// Pass this to the kernel to read into.
    pub fn recv_slice(buffer: *MessageBuffer) []u8 {
        assert(!buffer.valid_body);
        assert(buffer.invalid == null);
        if (buffer.consume_size > 0) {
            stdx.copy_left(.inexact, u8, buffer.message.buffer, buffer.available_slice());
            buffer.receive_size -= buffer.consume_size;
            buffer.consume_size = 0;
        }

        assert(buffer.receive_size < constants.message_size_max);
        return buffer.message.buffer[buffer.receive_size..];
    }

    /// When the kernel returns, informs the buffer about the read size.
    pub fn recv_advance(buffer: *MessageBuffer, size: u32) void {
        assert(buffer.consume_size == 0);
        assert(size <= constants.message_size_max);

        buffer.receive_size += size;
        assert(buffer.receive_size <= constants.message_size_max);
        buffer.validate();
    }

    // Received, but not yet consumed data:
    fn available_slice(buffer: *MessageBuffer) []u8 {
        return buffer.message.buffer[buffer.consume_size..buffer.receive_size];
    }

    fn available_slice_const(buffer: *const MessageBuffer) []const u8 {
        return buffer.message.buffer[buffer.consume_size..buffer.receive_size];
    }

    fn available_size(buffer: *const MessageBuffer) u32 {
        assert(buffer.consume_size <= buffer.receive_size);
        const result = buffer.receive_size - buffer.consume_size;
        assert(result <= constants.message_size_max);
        return result;
    }

    pub fn invalidate(buffer: *MessageBuffer, reason: InvalidReason) void {
        assert(buffer.invalid == null);
        buffer.receive_size = 0;
        buffer.consume_size = 0;
        buffer.valid_header = false;
        buffer.valid_body = false;
        buffer.invalid = reason;
    }

    /// Advances the parsing state machine.
    /// Idempotent, but eagerly called whenever available_slice changes.
    fn validate(buffer: *MessageBuffer) void {
        if (buffer.invalid == null) buffer.validate_header();
        if (buffer.invalid == null) buffer.validate_body();

        // Only assertions below this line:
        if (buffer.invalid != null) {
            assert(!buffer.valid_header);
            assert(!buffer.valid_body);
        }
        if (buffer.valid_body) assert(buffer.valid_header);
        assert(buffer.consume_size <= buffer.receive_size);
    }

    fn validate_header(buffer: *MessageBuffer) void {
        assert(buffer.invalid == null);
        if (buffer.valid_header) {
            assert(buffer.available_size() >= @sizeOf(Header));
            return;
        }
        if (buffer.available_size() < @sizeOf(Header)) return;

        const header_bytes = buffer.available_slice()[0..@sizeOf(Header)];

        var header: Header = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&header), header_bytes);

        if (!header.valid_checksum()) {
            buffer.invalidate(.header_checksum);
            return;
        }

        // Check that command is valid without materializing invalid Zig enum value.
        comptime assert(@sizeOf(vsr.Command) == @sizeOf(u8) and
            std.meta.FieldType(Header, .command) == vsr.Command);
        const command_raw: u8 = header_bytes[@offsetOf(Header, "command")];
        _ = std.meta.intToEnum(vsr.Command, command_raw) catch {
            vsr.fatal(
                .unknown_command,
                "unknown command, crashing for safety " ++
                    "(peer={} command={d} protocol={d} replica={d} release={})",
                .{
                    buffer.peer,
                    command_raw,
                    header.protocol,
                    header.replica,
                    header.release,
                },
            );
        };

        //? dj: Previously in the message bus we validated the checksum before validating the size.
        //? That makes more sense to me; why swap the order?
        //?
        //? matklad: huh, I remember doing this intentionally, but I don't recall _why_. The old
        //? way definitely makes more sense to me! I'll also restore the relative order of command
        //? validation. I don't _love_ materializing header with a "poisoned" command, but we should
        //? validate the checksum first. So the order is checksum, command, size, to make sure that
        //? we don't mis-interpret fields.
        //?
        //? resolved.
        if (header.size < @sizeOf(Header) or header.size > constants.message_size_max) {
            buffer.invalidate(.header_size);
            return;
        }
        assert(@sizeOf(Header) <= header.size and header.size <= constants.message_size_max);

        buffer.valid_header = true;

        // To avoid dropping outgoing messages, set the peer as soon as we can,
        // and not when we receive a full message.
        const message_peer = header.peer_type();
        if (message_peer != .unknown) {
            if (buffer.peer == .unknown) {
                buffer.peer = message_peer;
            } else {
                if (!std.meta.eql(buffer.peer, message_peer)) {
                    buffer.invalidate(.misdirected);
                    return;
                }
            }
        }
    }

    fn validate_body(buffer: *MessageBuffer) void {
        assert(buffer.invalid == null);
        if (buffer.valid_body) {
            assert(buffer.valid_header);
            assert(buffer.available_size() >= @sizeOf(Header));
            return;
        }
        if (!buffer.valid_header) return;

        const header = buffer.copy_header();
        if (buffer.available_size() < header.size) return;

        const body = buffer.available_slice()[@sizeOf(Header)..header.size];
        if (!header.valid_checksum_body(body)) {
            buffer.invalidate(.body_checksum);
            return;
        }
        buffer.valid_body = true;
    }

    /// Peek at the header for the incoming message. Necessitates a copy to guarantee alignment.
    fn copy_header(buffer: *const MessageBuffer) Header {
        assert(buffer.available_size() >= @sizeOf(Header));
        maybe(!buffer.valid_header);
        var header: Header = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&header),
            buffer.available_slice_const()[0..@sizeOf(Header)],
        );
        return header;
    }

    pub fn consume_message(buffer: *MessageBuffer, pool: *MessagePool) ?*Message {
        if (!buffer.valid_body) return null;

        assert(buffer.valid_header);
        assert(buffer.valid_body);
        assert(buffer.invalid == null);
        const header = buffer.copy_header();
        assert(buffer.available_slice().len >= header.size);
        if (buffer.consume_size == 0 and buffer.receive_size == header.size) {
            assert(buffer.available_size() == header.size);

            buffer.consume_size = 0;
            buffer.receive_size = 0;
            buffer.valid_body = false;
            buffer.valid_header = false;
            buffer.validate(); // Just to exercise asserts.

            defer buffer.message = pool.get_message(null);

            return buffer.message;
        }

        const message = pool.get_message(null);
        defer pool.unref(message);

        stdx.copy_disjoint(
            .inexact,
            u8,
            message.buffer,
            buffer.available_slice()[0..header.size],
        );
        buffer.consume_size += header.size;
        buffer.valid_header = false;
        buffer.valid_body = false;
        assert(buffer.consume_size <= buffer.receive_size);
        buffer.validate();

        assert(message.header.checksum == header.checksum);
        return message.ref();
    }
};

test "MessageBuffer fuzz" {
    // Generate a byte buffer with a bunch of prepares side-by-side.
    // Optionally corrupt a single bit in the buffer.
    // Feed the buffer in chunks of varying length to the MessageBuffer, verify that all messages
    // are received unless a fault is detected.

    var prng = stdx.PRNG.from_seed(92);
    const gpa = std.testing.allocator;

    var buffer: []u8 = try gpa.alloc(u8, 5 * constants.message_size_max);
    defer gpa.free(buffer);

    for (0..100) |_| {
        const fault = prng.boolean();
        var total_size: u32 = 0;
        var headers: stdx.BoundedArrayType(Header, 10) = .{};
        for (0..10) |_| {
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
            headers.append_assume_capacity(header.frame_const().*);
        }

        if (fault) {
            const byte_index = prng.index(buffer[0..total_size]);
            const bit_index = prng.int_inclusive(u3, 7);
            buffer[byte_index] ^= @as(u8, 1) << bit_index;
        }

        var pool = try MessagePool.init(gpa, .{ .replica = .{
            .members_count = 6,
            .pipeline_requests_limit = 1,
        } });
        defer pool.deinit(gpa);

        var message_buffer = MessageBuffer.init(&pool);
        defer message_buffer.deinit(&pool);

        var recv_size: u32 = 0;
        var recv_message_index: u32 = 0;
        while (recv_size < total_size) {
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

            while (message_buffer.consume_message(&pool)) |message| {
                defer pool.unref(message);

                assert(stdx.equal_bytes(
                    Header,
                    message.header,
                    &headers.slice()[recv_message_index],
                ));
                assert(message.header.valid_checksum_body(message.body_used()));
                recv_message_index += 1;
            }
            if (message_buffer.invalid != null) {
                assert(fault);
                break;
            }
        }
        if (fault) {
            assert(message_buffer.invalid != null);
        } else {
            assert(message_buffer.invalid == null);
            assert(recv_message_index == headers.count());
        }
    }
}
