const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const vsr = @import("vsr.zig");
const Header = vsr.Header;

comptime {
    // message_size_max must be a multiple of sector_size for Direct I/O
    assert(config.message_size_max % config.sector_size == 0);
}

/// Add an extra sector_size bytes to allow a partially received subsequent
/// message to be shifted to make space for 0 padding to vsr.sector_ceil.
const message_size_max_padded = config.message_size_max + config.sector_size;

/// The number of full-sized messages allocated at initialization by the replica message pool.
/// There must be enough messages to ensure that the replica can always progress, to avoid deadlock.
pub const messages_max_replica = messages_max: {
    var sum: usize = 0;

    sum += config.io_depth_read + config.io_depth_write; // Journal I/O
    sum += config.clients_max; // Replica.client_table
    sum += 1; // Replica.loopback_queue
    sum += config.pipeline_max; // Replica.pipeline
    // Replica.do_view_change_from_all_replicas quorum:
    // Replica.recovery_response_quorum is only used for recovery and does not increase the limit.
    // All other quorums are bitsets.
    sum += config.replicas_max;
    sum += config.connections_max; // Connection.recv_message
    sum += config.connections_max * config.connection_send_queue_max_replica; // Connection.send_queue
    sum += 1; // Handle bursts (e.g. Connection.parse_message)
    // Handle Replica.commit_op's reply:
    // (This is separate from the burst +1 because they may occur concurrently).
    sum += 1;
    sum += 20; // TODO Our network simulator allows up to 20 messages for path_capacity_max.

    break :messages_max sum;
};

/// The number of full-sized messages allocated at initialization by the client message pool.
pub const messages_max_client = messages_max: {
    var sum: usize = 0;

    sum += config.replicas_max; // Connection.recv_message
    sum += config.replicas_max * config.connection_send_queue_max_client; // Connection.send_queue
    sum += config.client_request_queue_max; // Client.request_queue
    // Handle bursts (e.g. Connection.parse_message, or sending a ping when the send queue is full).
    sum += 1;
    sum += 20; // TODO Our network simulator allows up to 20 messages for path_capacity_max.

    break :messages_max sum;
};

comptime {
    // These conditions are necessary (but not sufficient) to prevent deadlocks.
    assert(messages_max_replica > config.replicas_max);
    assert(messages_max_client > config.client_request_queue_max);
}

/// A pool of reference-counted Messages, memory for which is allocated only once during
/// initialization and reused thereafter. The messages_max values determine the size of this pool.
pub const MessagePool = struct {
    pub const Message = struct {
        // TODO: replace this with a header() function to save memory
        header: *Header,
        /// This buffer is aligned to config.sector_size and casting to that alignment in order
        /// to perform Direct I/O is safe.
        buffer: []u8,
        references: u32 = 0,
        next: ?*Message,

        /// Increment the reference count of the message and return the same pointer passed.
        pub fn ref(message: *Message) *Message {
            message.references += 1;
            return message;
        }

        pub fn body(message: *Message) []u8 {
            return message.buffer[@sizeOf(Header)..message.header.size];
        }
    };

    /// List of currently unused messages of message_size_max_padded
    free_list: ?*Message,

    pub fn init(allocator: mem.Allocator, process_type: vsr.ProcessType) error{OutOfMemory}!MessagePool {
        const messages_max: usize = switch (process_type) {
            .replica => messages_max_replica,
            .client => messages_max_client,
        };

        var ret: MessagePool = .{
            .free_list = null,
        };
        {
            var i: usize = 0;
            while (i < messages_max) : (i += 1) {
                const buffer = try allocator.allocAdvanced(
                    u8,
                    config.sector_size,
                    message_size_max_padded,
                    .exact,
                );
                const message = try allocator.create(Message);
                message.* = .{
                    .header = mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
                    .buffer = buffer,
                    .next = ret.free_list,
                };
                ret.free_list = message;
            }
        }

        return ret;
    }

    /// Get an unused message with a buffer of config.message_size_max.
    /// The returned message has exactly one reference.
    pub fn get_message(pool: *MessagePool) *Message {
        const message = pool.free_list.?;
        pool.free_list = message.next;
        message.next = null;
        assert(message.references == 0);

        message.references = 1;
        return message;
    }

    /// Decrement the reference count of the message, possibly freeing it.
    pub fn unref(pool: *MessagePool, message: *Message) void {
        message.references -= 1;
        if (message.references == 0) {
            if (builtin.mode == .Debug) mem.set(u8, message.buffer, undefined);
            message.next = pool.free_list;
            pool.free_list = message;
        }
    }
};
