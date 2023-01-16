const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("constants.zig");

const vsr = @import("vsr.zig");
const Header = vsr.Header;

comptime {
    // message_size_max must be a multiple of sector_size for Direct I/O
    assert(constants.message_size_max % constants.sector_size == 0);
}

/// Add an extra sector_size bytes to allow a partially received subsequent
/// message to be shifted to make space for 0 padding to vsr.sector_ceil.
const message_size_max_padded = constants.message_size_max + constants.sector_size;

/// The number of full-sized messages allocated at initialization by the replica message pool.
/// There must be enough messages to ensure that the replica can always progress, to avoid deadlock.
pub const messages_max_replica = messages_max: {
    var sum: usize = 0;

    sum += constants.journal_iops_read_max; // Journal reads
    sum += constants.journal_iops_write_max; // Journal writes
    sum += constants.clients_max; // SuperBlock.client_table
    sum += 1; // Replica.loopback_queue
    sum += constants.pipeline_prepare_queue_max; // Replica.Pipeline{Queue|Cache}
    sum += constants.pipeline_request_queue_max; // Replica.Pipeline{Queue|Cache}
    sum += 1; // Replica.commit_prepare
    // Replica.do_view_change_from_all_replicas quorum:
    // Replica.recovery_response_quorum is only used for recovery and does not increase the limit.
    // All other quorums are bitsets.
    sum += constants.replicas_max;
    sum += constants.connections_max; // Connection.recv_message
    sum += constants.connections_max * constants.connection_send_queue_max_replica; // Connection.send_queue
    sum += 1; // Handle bursts (e.g. Connection.parse_message)
    // Handle Replica.commit_op's reply:
    // (This is separate from the burst +1 because they may occur concurrently).
    sum += 1;

    break :messages_max sum;
};

/// The number of full-sized messages allocated at initialization by the client message pool.
pub const messages_max_client = messages_max: {
    var sum: usize = 0;

    sum += constants.replicas_max; // Connection.recv_message
    sum += constants.replicas_max * constants.connection_send_queue_max_client; // Connection.send_queue
    sum += constants.client_request_queue_max; // Client.request_queue
    // Handle bursts (e.g. Connection.parse_message, or sending a ping when the send queue is full).
    sum += 1;

    break :messages_max sum;
};

comptime {
    // These conditions are necessary (but not sufficient) to prevent deadlocks.
    assert(messages_max_replica > constants.replicas_max);
    assert(messages_max_client > constants.client_request_queue_max);
}

/// A pool of reference-counted Messages, memory for which is allocated only once during
/// initialization and reused thereafter. The messages_max values determine the size of this pool.
pub const MessagePool = struct {
    pub const Message = struct {
        // TODO: replace this with a header() function to save memory
        header: *Header,
        buffer: []align(constants.sector_size) u8,
        references: u32 = 0,
        next: ?*Message,

        /// Increment the reference count of the message and return the same pointer passed.
        pub fn ref(message: *Message) *Message {
            message.references += 1;
            return message;
        }

        pub fn body(message: *const Message) []align(@sizeOf(Header)) u8 {
            return message.buffer[@sizeOf(Header)..message.header.size];
        }
    };

    /// List of currently unused messages.
    free_list: ?*Message,

    messages_max: usize,

    pub fn init(allocator: mem.Allocator, process_type: vsr.ProcessType) error{OutOfMemory}!MessagePool {
        return MessagePool.init_capacity(allocator, switch (process_type) {
            .replica => messages_max_replica,
            .client => messages_max_client,
        });
    }

    pub fn init_capacity(allocator: mem.Allocator, messages_max: usize) error{OutOfMemory}!MessagePool {
        var pool: MessagePool = .{
            .free_list = null,
            .messages_max = messages_max,
        };
        {
            var i: usize = 0;
            while (i < messages_max) : (i += 1) {
                const buffer = try allocator.allocAdvanced(
                    u8,
                    constants.sector_size,
                    message_size_max_padded,
                    .exact,
                );
                const message = try allocator.create(Message);
                message.* = .{
                    .header = mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
                    .buffer = buffer,
                    .next = pool.free_list,
                };
                pool.free_list = message;
            }
        }

        return pool;
    }

    /// Frees all messages that were unused or returned to the pool via unref().
    pub fn deinit(pool: *MessagePool, allocator: mem.Allocator) void {
        var free_count: usize = 0;
        while (pool.free_list) |message| {
            pool.free_list = message.next;
            allocator.free(message.buffer);
            allocator.destroy(message);
            free_count += 1;
        }
        // If the MessagePool is being deinitialized, all messages should have already been
        // released to the pool.
        assert(free_count == pool.messages_max);
    }

    /// Get an unused message with a buffer of constants.message_size_max.
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
