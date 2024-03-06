//! Replies from the cluster may arrive out-of-order; the ReplySequence reassembles them in the
//! correct order (by ascending op number).
const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const IdPermutation = @import("id.zig").IdPermutation;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Client = @import("cluster.zig").Client;
const StateMachine = @import("cluster.zig").StateMachine;
const ClusterReply = @import("cluster.zig").ClusterReply;

const PriorityQueue = std.PriorityQueue;

const PendingReplyQueue = PriorityQueue(
    ClusterReply,
    void,
    struct {
        /// `PendingReply`s are ordered by ascending reply op.
        fn compare(context: void, a: ClusterReply, b: ClusterReply) std.math.Order {
            _ = context;
            return std.math.order(a.op(), b.op());
        }
    }.compare,
);

pub const ReplySequence = struct {
    /// Reply messages (from cluster to client) may be reordered during transit.
    /// The ReplySequence must reassemble them in the original order (ascending op/commit
    /// number) before handing them off to the Workload for verification.
    ///
    /// `ReplySequence.stalled_queue` hold replies (and corresponding requests) that are
    /// waiting to be processed.
    pub const stalled_queue_capacity =
        constants.clients_max * constants.client_request_queue_max * 2;

    message_pool: MessagePool,

    /// The next op to be verified.
    /// Starts at 1, because op=0 is the root.
    stalled_op: u64 = 1,

    /// The list of messages waiting to be verified (the reply for a lower op has not yet arrived).
    /// Includes `register` messages.
    stalled_queue: PendingReplyQueue,

    pub fn init(allocator: std.mem.Allocator) !ReplySequence {
        // *2 for PendingReply.request and PendingReply.reply.
        var message_pool = try MessagePool.init_capacity(allocator, stalled_queue_capacity * 2);
        errdefer message_pool.deinit(allocator);

        var stalled_queue = PendingReplyQueue.init(allocator, {});
        errdefer stalled_queue.deinit();
        try stalled_queue.ensureTotalCapacity(stalled_queue_capacity);

        return ReplySequence{
            .message_pool = message_pool,
            .stalled_queue = stalled_queue,
        };
    }

    pub fn deinit(sequence: *ReplySequence, allocator: std.mem.Allocator) void {
        while (sequence.stalled_queue.removeOrNull()) |commit| {
            sequence.unref(commit);
        }
        sequence.stalled_queue.deinit();
        sequence.message_pool.deinit(allocator);
    }

    pub fn empty(sequence: *const ReplySequence) bool {
        return sequence.stalled_queue.len == 0;
    }

    pub fn free(sequence: ReplySequence) usize {
        return stalled_queue_capacity - sequence.stalled_queue.len;
    }

    pub fn insert(
        sequence: *ReplySequence,
        reply: ClusterReply,
    ) void {
        switch (reply) {
            .client => |client| {
                assert(client.request.header.invalid() == null);
                assert(client.request.header.command == .request);

                assert(client.reply.header.invalid() == null);
                assert(client.reply.header.request == client.request.header.request);
                assert(client.reply.header.op >= sequence.stalled_op);
                assert(client.reply.header.command == .reply);
                assert(client.reply.header.operation == client.request.header.operation);
            },
            .no_reply => |prepare| {
                assert(prepare.header.invalid() == null);
                assert(prepare.header.command == .prepare);
                assert(prepare.header.client == 0);
                assert(prepare.header.request == 0);
            },
        }

        sequence.stalled_queue.add(sequence.clone(reply)) catch unreachable;
    }

    fn clone(
        sequence: *ReplySequence,
        reply: ClusterReply,
    ) ClusterReply {
        return switch (reply) {
            .client => |client| .{
                .client = .{
                    .index = client.index,
                    .request = sequence.clone_message(client.request.base_const())
                        .into(.request).?,
                    .reply = sequence.clone_message(client.reply.base_const())
                        .into(.reply).?,
                },
            },
            .no_reply => |prepare| .{
                .no_reply = sequence.clone_message(prepare.base_const()).into(.prepare).?,
            },
        };
    }

    fn unref(sequence: *ReplySequence, reply: ClusterReply) void {
        switch (reply) {
            .client => |client| {
                sequence.message_pool.unref(client.reply);
                sequence.message_pool.unref(client.request);
            },
            .no_reply => |prepare| {
                sequence.message_pool.unref(prepare);
            },
        }
    }

    pub fn peek(sequence: *ReplySequence) ?ClusterReply {
        assert(sequence.stalled_queue.len <= stalled_queue_capacity);

        const commit = sequence.stalled_queue.peek() orelse return null;
        if (commit.op() == sequence.stalled_op) {
            return commit;
        } else {
            assert(commit.op() > sequence.stalled_op);
            return null;
        }
    }

    pub fn next(sequence: *ReplySequence) void {
        const commit = sequence.stalled_queue.remove();
        assert(commit.op() == sequence.stalled_op);

        sequence.stalled_op += 1;
        sequence.unref(commit);
    }

    /// Copy the message from a Client's MessagePool to the ReplySequence's MessagePool.
    ///
    /// The client has a finite amount of messages in its pool, and the ReplySequence needs to hold
    /// onto requests/replies until all preceeding requests/replies have arrived.
    ///
    /// Returns the ReplySequence's message.
    fn clone_message(sequence: *ReplySequence, message_client: *const Message) *Message {
        const message_sequence = sequence.message_pool.get_message(null);
        stdx.copy_disjoint(.exact, u8, message_sequence.buffer, message_client.buffer);
        return message_sequence;
    }
};
