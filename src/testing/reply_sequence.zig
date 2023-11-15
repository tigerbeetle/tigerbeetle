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

const PriorityQueue = std.PriorityQueue;

/// Both messages belong to the ReplySequence's `MessagePool`.
const PendingReply = struct {
    client_index: usize,
    request: *Message.Request,
    reply: *Message.Reply,

    /// `PendingReply`s are ordered by ascending reply op.
    fn compare(context: void, a: PendingReply, b: PendingReply) std.math.Order {
        _ = context;
        return std.math.order(a.reply.header.op, b.reply.header.op);
    }
};

const PendingReplyQueue = PriorityQueue(PendingReply, void, PendingReply.compare);

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
        while (sequence.stalled_queue.removeOrNull()) |pending| {
            sequence.message_pool.unref(pending.request);
            sequence.message_pool.unref(pending.reply);
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
        client_index: usize,
        request_message: *Message.Request,
        reply_message: *Message.Reply,
    ) void {
        assert(request_message.header.invalid() == null);
        assert(request_message.header.command == .request);

        assert(reply_message.header.invalid() == null);
        assert(reply_message.header.request == request_message.header.request);
        assert(reply_message.header.op >= sequence.stalled_op);
        assert(reply_message.header.command == .reply);
        assert(reply_message.header.operation == request_message.header.operation);

        sequence.stalled_queue.add(.{
            .client_index = client_index,
            .request = sequence.clone_message(request_message.base_const()).into(.request).?,
            .reply = sequence.clone_message(reply_message.base_const()).into(.reply).?,
        }) catch unreachable;
    }

    pub fn peek(sequence: *ReplySequence) ?PendingReply {
        assert(sequence.stalled_queue.len <= stalled_queue_capacity);

        const commit = sequence.stalled_queue.peek() orelse return null;
        if (commit.reply.header.op == sequence.stalled_op) {
            return commit;
        } else {
            assert(commit.reply.header.op > sequence.stalled_op);
            return null;
        }
    }

    pub fn next(sequence: *ReplySequence) void {
        const commit = sequence.stalled_queue.remove();
        assert(commit.reply.header.op == sequence.stalled_op);

        sequence.stalled_op += 1;
        sequence.message_pool.unref(commit.reply);
        sequence.message_pool.unref(commit.request);
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
