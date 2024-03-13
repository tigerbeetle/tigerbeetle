const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const stdx = @import("stdx.zig");
const constants = @import("constants.zig");

const vsr = @import("vsr.zig");
const Header = vsr.Header;

comptime {
    // message_size_max must be a multiple of sector_size for Direct I/O
    assert(constants.message_size_max % constants.sector_size == 0);
}

/// The number of full-sized messages allocated at initialization by the replica message pool.
/// There must be enough messages to ensure that the replica can always progress, to avoid deadlock.
pub const messages_max_replica = messages_max: {
    var sum: usize = 0;

    sum += constants.journal_iops_read_max; // Journal reads
    sum += constants.journal_iops_write_max; // Journal writes
    sum += constants.client_replies_iops_read_max; // Client-reply reads
    sum += constants.client_replies_iops_write_max; // Client-reply writes
    sum += constants.grid_repair_reads_max; // Replica.grid_reads (Replica.BlockRead)
    sum += 1; // Replica.loopback_queue
    sum += constants.pipeline_prepare_queue_max; // Replica.Pipeline{Queue|Cache}
    sum += constants.pipeline_request_queue_max; // Replica.Pipeline{Queue|Cache}
    sum += 1; // Replica.commit_prepare
    // Replica.do_view_change_from_all_replicas quorum:
    // All other quorums are bitsets.
    sum += constants.replicas_max;
    sum += constants.connections_max; // Connection.recv_message
    // Connection.send_queue:
    sum += constants.connections_max * constants.connection_send_queue_max_replica;
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
    // Connection.send_queue:
    sum += constants.replicas_max * constants.connection_send_queue_max_client;
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
    pub const Message = extern struct {
        pub const Reserved = MessageType(.reserved);
        pub const Ping = MessageType(.ping);
        pub const Pong = MessageType(.pong);
        pub const PingClient = MessageType(.ping_client);
        pub const PongClient = MessageType(.pong_client);
        pub const Request = MessageType(.request);
        pub const Prepare = MessageType(.prepare);
        pub const PrepareOk = MessageType(.prepare_ok);
        pub const Reply = MessageType(.reply);
        pub const Commit = MessageType(.commit);
        pub const StartViewChange = MessageType(.start_view_change);
        pub const DoViewChange = MessageType(.do_view_change);
        pub const StartView = MessageType(.start_view);
        pub const RequestStartView = MessageType(.request_start_view);
        pub const RequestHeaders = MessageType(.request_headers);
        pub const RequestPrepare = MessageType(.request_prepare);
        pub const RequestReply = MessageType(.request_reply);
        pub const Headers = MessageType(.headers);
        pub const Eviction = MessageType(.eviction);
        pub const RequestBlocks = MessageType(.request_blocks);
        pub const Block = MessageType(.block);
        pub const RequestSyncCheckpoint = MessageType(.request_sync_checkpoint);
        pub const SyncCheckpoint = MessageType(.sync_checkpoint);

        // TODO Avoid the extra level of indirection.
        // (https://github.com/tigerbeetle/tigerbeetle/pull/1295#discussion_r1394265250)
        header: *Header,
        buffer: *align(constants.sector_size) [constants.message_size_max]u8,
        references: u32 = 0,
        next: ?*Message,

        /// Increment the reference count of the message and return the same pointer passed.
        pub fn ref(message: *Message) *Message {
            assert(message.references > 0);
            assert(message.next == null);

            message.references += 1;
            return message;
        }

        pub fn body(message: *const Message) []align(@sizeOf(Header)) u8 {
            return message.buffer[@sizeOf(Header)..message.header.size];
        }

        /// NOTE:
        /// - Does *not* alter the reference count.
        /// - Does *not* verify the command. (Use this function for constructing the message.)
        pub fn build(message: *Message, comptime command: vsr.Command) *MessageType(command) {
            return @ptrCast(message);
        }

        /// NOTE: Does *not* alter the reference count.
        pub fn into(message: *Message, comptime command: vsr.Command) ?*MessageType(command) {
            if (message.header.command != command) return null;
            return @ptrCast(message);
        }

        pub const AnyMessage = stdx.EnumUnionType(vsr.Command, MessagePointerType);

        fn MessagePointerType(comptime command: vsr.Command) type {
            return *MessageType(command);
        }

        /// NOTE: Does *not* alter the reference count.
        pub fn into_any(message: *Message) AnyMessage {
            switch (message.header.command) {
                inline else => |command| {
                    return @unionInit(AnyMessage, @tagName(command), message.into(command).?);
                },
            }
        }
    };

    /// List of currently unused messages.
    free_list: ?*Message,

    messages_max: usize,

    pub fn init(
        allocator: mem.Allocator,
        process_type: vsr.ProcessType,
    ) error{OutOfMemory}!MessagePool {
        return MessagePool.init_capacity(allocator, switch (process_type) {
            .replica => messages_max_replica,
            .client => messages_max_client,
        });
    }

    pub fn init_capacity(
        allocator: mem.Allocator,
        messages_max: usize,
    ) error{OutOfMemory}!MessagePool {
        var pool: MessagePool = .{
            .free_list = null,
            .messages_max = messages_max,
        };
        {
            for (0..messages_max) |_| {
                const buffer = try allocator.alignedAlloc(
                    u8,
                    constants.sector_size,
                    constants.message_size_max,
                );
                const message = try allocator.create(Message);
                message.* = .{
                    .header = undefined,
                    .buffer = buffer[0..constants.message_size_max],
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

    pub fn GetMessageType(comptime command: ?vsr.Command) type {
        if (command) |c| {
            return *MessageType(c);
        } else {
            return *Message;
        }
    }

    /// Get an unused message with a buffer of constants.message_size_max.
    /// The returned message has exactly one reference.
    pub fn get_message(pool: *MessagePool, comptime command: ?vsr.Command) GetMessageType(command) {
        if (command) |c| {
            return pool.get_message_base().build(c);
        } else {
            return pool.get_message_base();
        }
    }

    fn get_message_base(pool: *MessagePool) *Message {
        const message = pool.free_list.?;
        pool.free_list = message.next;
        message.next = null;
        message.header = mem.bytesAsValue(Header, message.buffer[0..@sizeOf(Header)]);
        assert(message.references == 0);

        message.references = 1;
        return message;
    }

    /// Decrement the reference count of the message, possibly freeing it.
    ///
    /// `@TypeOf(message)` is one of:
    /// - `*Message`
    /// - `*MessageType(command)` for any `command`.
    pub fn unref(pool: *MessagePool, message: anytype) void {
        assert(@typeInfo(@TypeOf(message)) == .Pointer);
        assert(!@typeInfo(@TypeOf(message)).Pointer.is_const);

        if (@TypeOf(message) == *Message) {
            pool.unref_base(message);
        } else {
            pool.unref_base(message.base());
        }
    }

    fn unref_base(pool: *MessagePool, message: *Message) void {
        assert(message.next == null);

        message.references -= 1;
        if (message.references == 0) {
            message.header = undefined;
            if (constants.verify) {
                @memset(message.buffer, undefined);
            }
            message.next = pool.free_list;
            pool.free_list = message;
        }
    }
};

fn MessageType(comptime command: vsr.Command) type {
    return extern struct {
        const CommandMessage = @This();
        const CommandHeader = Header.Type(command);
        const Message = MessagePool.Message;

        // The underlying structure of Message and CommandMessage should be identical, so that their
        // memory can be cast back-and-forth.
        comptime {
            assert(@sizeOf(Message) == @sizeOf(CommandMessage));

            for (
                std.meta.fields(Message),
                std.meta.fields(CommandMessage),
            ) |message_field, command_message_field| {
                assert(std.mem.eql(u8, message_field.name, command_message_field.name));
                assert(@sizeOf(message_field.type) == @sizeOf(command_message_field.type));
                assert(@offsetOf(Message, message_field.name) ==
                    @offsetOf(CommandMessage, command_message_field.name));
            }
        }

        /// Points into `buffer`.
        header: *CommandHeader,
        buffer: *align(constants.sector_size) [constants.message_size_max]u8,
        references: u32,
        next: ?*Message,

        pub fn base(message: *CommandMessage) *Message {
            return @ptrCast(message);
        }

        pub fn base_const(message: *const CommandMessage) *const Message {
            return @ptrCast(message);
        }

        pub fn ref(message: *CommandMessage) *CommandMessage {
            return @ptrCast(message.base().ref());
        }

        pub fn body(message: *const CommandMessage) []align(@sizeOf(Header)) u8 {
            return message.base_const().body();
        }
    };
}
