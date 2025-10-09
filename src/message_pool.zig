const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const stdx = @import("stdx");
const constants = @import("constants.zig");

const vsr = @import("vsr.zig");
const Header = vsr.Header;
const StackType = @import("./stack.zig").StackType;

comptime {
    // message_size_max must be a multiple of sector_size for Direct I/O
    assert(constants.message_size_max % constants.sector_size == 0);
}

pub const Options = union(vsr.ProcessType) {
    replica: struct {
        members_count: u8,
        pipeline_requests_limit: u32,
        message_bus: enum { tcp, testing },
    },
    client,

    /// The number of messages allocated at initialization by the message pool.
    fn messages_max(options: *const Options) u32 {
        return switch (options.*) {
            .client => messages_max: {
                var sum: u32 = 0;

                sum += constants.replicas_max; // Connection.recv_buffer
                // Connection.send_queue:
                sum += constants.replicas_max * constants.connection_send_queue_max_client;
                sum += 1; // Client.request_inflight
                // Handle bursts.
                // (e.g. Connection.parse_message(), or sending a ping when the send queue is full).
                sum += 1;

                // This conditions is necessary (but not sufficient) to prevent deadlocks.
                assert(sum > 1);
                break :messages_max sum;
            },

            // The number of full-sized messages allocated at initialization by the replica message
            // pool. There must be enough messages to ensure that the replica can always progress,
            // to avoid deadlock.
            .replica => |*replica| messages_max: {
                assert(replica.members_count > 0);
                assert(replica.members_count <= constants.members_max);
                assert(replica.pipeline_requests_limit >= 0);
                assert(replica.pipeline_requests_limit <= constants.pipeline_request_queue_max);

                var sum: u32 = 0;

                const pipeline_limit =
                    constants.pipeline_prepare_queue_max + replica.pipeline_requests_limit;

                sum += constants.journal_iops_read_max; // Journal reads
                sum += constants.journal_iops_write_max; // Journal writes
                sum += constants.client_replies_iops_read_max; // Client-reply reads
                sum += constants.client_replies_iops_write_max; // Client-reply writes
                // Replica.grid_reads (Replica.BlockRead)
                sum += constants.grid_repair_reads_max;
                sum += 1; // Replica.loopback_queue
                sum += pipeline_limit; // Replica.Pipeline{Queue|Cache}
                sum += 1; // Replica.commit_prepare
                sum += 1; // Replica.sync_start_view
                // Replica.do_view_change_from_all_replicas quorum:
                // All other quorums are bitsets.
                //
                // This should be set to the runtime replica_count, but we don't know that precisely
                // yet, so we may guess high. (We can't differentiate between replicas and
                // standbys.)
                sum += @min(replica.members_count, constants.replicas_max);
                sum += 1; // Handle bursts (e.g. Connection.parse_message)
                // Handle Replica.commit_op's reply:
                // (This is separate from the burst +1 because they may occur concurrently).
                sum += 1;

                switch (replica.message_bus) {
                    .tcp => {
                        // The maximum number of simultaneous open connections on the server.
                        // -1 since we never connect to ourself.
                        const connections_max = replica.members_count + pipeline_limit - 1;
                        sum += connections_max; // Connection.recv_buffer
                        // Connection.send_queue:
                        sum += connections_max * constants.connection_send_queue_max_replica;
                    },
                    .testing => {},
                }

                // This conditions is necessary (but not sufficient) to prevent deadlocks.
                assert(sum > constants.replicas_max);
                break :messages_max sum;
            },
        };
    }
};

/// A pool of reference-counted Messages, memory for which is allocated only once during
/// initialization and reused thereafter. The messages_max values determine the size of this pool.
pub const MessagePool = struct {
    pub const Message = extern struct {
        pub const Reserved = CommandMessageType(.reserved);
        pub const Ping = CommandMessageType(.ping);
        pub const Pong = CommandMessageType(.pong);
        pub const PingClient = CommandMessageType(.ping_client);
        pub const PongClient = CommandMessageType(.pong_client);
        pub const Request = CommandMessageType(.request);
        pub const Prepare = CommandMessageType(.prepare);
        pub const PrepareOk = CommandMessageType(.prepare_ok);
        pub const Reply = CommandMessageType(.reply);
        pub const Commit = CommandMessageType(.commit);
        pub const StartViewChange = CommandMessageType(.start_view_change);
        pub const DoViewChange = CommandMessageType(.do_view_change);
        pub const StartView = CommandMessageType(.start_view);
        pub const RequestStartView = CommandMessageType(.request_start_view);
        pub const RequestHeaders = CommandMessageType(.request_headers);
        pub const RequestPrepare = CommandMessageType(.request_prepare);
        pub const RequestReply = CommandMessageType(.request_reply);
        pub const Headers = CommandMessageType(.headers);
        pub const Eviction = CommandMessageType(.eviction);
        pub const RequestBlocks = CommandMessageType(.request_blocks);
        pub const Block = CommandMessageType(.block);

        // TODO Avoid the extra level of indirection.
        // (https://github.com/tigerbeetle/tigerbeetle/pull/1295#discussion_r1394265250)
        header: *Header,
        buffer: *align(constants.sector_size) [constants.message_size_max]u8,
        references: u32 = 0,
        link: FreeList.Link,

        /// Increment the reference count of the message and return the same pointer passed.
        pub fn ref(message: *Message) *Message {
            assert(message.references > 0);
            assert(message.link.next == null);

            message.references += 1;
            return message;
        }

        pub fn body_used(message: *const Message) []align(@sizeOf(Header)) u8 {
            return message.buffer[@sizeOf(Header)..message.header.size];
        }

        /// NOTE:
        /// - Does *not* alter the reference count.
        /// - Does *not* verify the command. (Use this function for constructing the message.)
        pub fn build(
            message: *Message,
            comptime command: vsr.Command,
        ) *CommandMessageType(command) {
            return @ptrCast(message);
        }

        /// NOTE: Does *not* alter the reference count.
        pub fn into(
            message: *Message,
            comptime command: vsr.Command,
        ) ?*CommandMessageType(command) {
            if (message.header.command != command) return null;
            return @ptrCast(message);
        }

        pub const AnyMessage = stdx.EnumUnionType(vsr.Command, MessagePointerType);

        fn MessagePointerType(comptime command: vsr.Command) type {
            return *CommandMessageType(command);
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

    const FreeList = StackType(Message);
    /// List of currently unused messages.
    free_list: StackType(Message),

    messages_max: usize,
    messages: []Message,
    buffers: []align(constants.sector_size) [constants.message_size_max]u8,

    pub fn init(
        allocator: mem.Allocator,
        options: Options,
    ) error{OutOfMemory}!MessagePool {
        return MessagePool.init_capacity(allocator, options.messages_max());
    }

    pub fn init_capacity(
        allocator: mem.Allocator,
        messages_max: u32,
    ) error{OutOfMemory}!MessagePool {
        const buffers = try allocator.alignedAlloc(
            [constants.message_size_max]u8,
            constants.sector_size,
            messages_max,
        );
        errdefer allocator.free(buffers);

        const messages = try allocator.alloc(Message, messages_max);
        errdefer allocator.free(messages);

        var free_list = FreeList.init(.{
            .capacity = messages_max,
            .verify_push = false,
        });
        for (messages, buffers) |*message, *buffer| {
            message.* = .{ .header = undefined, .buffer = buffer, .link = .{} };
            free_list.push(message);
        }

        return .{
            .free_list = free_list,
            .messages_max = messages_max,
            .messages = messages,
            .buffers = buffers,
        };
    }

    /// Frees all messages that were unused or returned to the pool via unref().
    pub fn deinit(pool: *MessagePool, allocator: mem.Allocator) void {
        // If the MessagePool is being deinitialized, all messages should have already been
        // released to the pool.
        assert(pool.free_list.count() == pool.messages_max);
        assert(pool.messages.len == pool.messages_max);
        assert(pool.buffers.len == pool.messages_max);
        allocator.free(pool.messages);
        allocator.free(pool.buffers);
        pool.* = undefined;
    }

    pub fn GetMessageType(comptime command: ?vsr.Command) type {
        if (command) |c| {
            return *CommandMessageType(c);
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
        const message = pool.free_list.pop().?;
        assert(message.link.next == null);
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
        assert(@typeInfo(@TypeOf(message)) == .pointer);
        assert(!@typeInfo(@TypeOf(message)).pointer.is_const);

        if (@TypeOf(message) == *Message) {
            pool.unref_base(message);
        } else {
            pool.unref_base(message.base());
        }
    }

    fn unref_base(pool: *MessagePool, message: *Message) void {
        assert(message.link.next == null);

        message.references -= 1;
        if (message.references == 0) {
            message.header = undefined;
            if (constants.verify) {
                @memset(message.buffer, undefined);
            }
            pool.free_list.push(message);
        }
    }
};

fn CommandMessageType(comptime command: vsr.Command) type {
    const CommandHeaderUnified = Header.Type(command);

    return extern struct {
        const CommandMessage = @This();
        const CommandHeader = CommandHeaderUnified;
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
        link: MessagePool.FreeList.Link,

        pub fn base(message: *CommandMessage) *Message {
            return @ptrCast(message);
        }

        pub fn base_const(message: *const CommandMessage) *const Message {
            return @ptrCast(message);
        }

        pub fn ref(message: *CommandMessage) *CommandMessage {
            return @ptrCast(message.base().ref());
        }

        pub fn body_used(message: *const CommandMessage) []align(@sizeOf(Header)) u8 {
            return message.base_const().body_used();
        }
    };
}
