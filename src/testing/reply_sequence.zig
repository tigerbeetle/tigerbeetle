//! Replies from the cluster may arrive out-of-order; the ReplySequence reassembles them in the
//! correct order (by ascending op number).
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const stdx = @import("stdx");
const constants = @import("../constants.zig");
const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = MessagePool.Message;

const PriorityQueue = std.PriorityQueue;

/// Both messages belong to the ReplySequence's `MessagePool`.
const PendingReply = struct {
    /// `client_index` is null when the prepare does not originate from a client.
    client_index: ?usize,
    prepare: *Message.Prepare,
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
    /// `ReplySequence.stalled_queue` hold replies (and corresponding prepares) that are
    /// waiting to be processed.
    pub const stalled_queue_capacity =
        constants.clients_max * constants.client_request_queue_max * 2;

    message_pool: MessagePool,

    /// The list of messages waiting to be verified (the reply for a lower op has not yet arrived).
    /// Includes `register` messages.
    stalled_queue: PendingReplyQueue,

    pub fn init(allocator: std.mem.Allocator) !ReplySequence {
        // *2 for PendingReply.prepare and PendingReply.reply.
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
            sequence.message_pool.unref(pending.prepare);
            sequence.message_pool.unref(pending.reply);
        }
        sequence.stalled_queue.deinit();
        sequence.message_pool.deinit(allocator);
    }

    pub fn empty(sequence: *const ReplySequence) bool {
        return sequence.stalled_queue.count() == 0;
    }

    pub fn free(sequence: ReplySequence) usize {
        return stalled_queue_capacity - sequence.stalled_queue.count();
    }

    pub fn insert(
        sequence: *ReplySequence,
        client_index: ?usize,
        prepare_message: *const Message.Prepare,
        reply_message: *const Message.Reply,
    ) void {
        assert(sequence.stalled_queue.count() < stalled_queue_capacity);

        assert(prepare_message.header.invalid() == null);
        assert(prepare_message.header.command == .prepare);

        // The ReplySequence includes "replies" that don't actually get sent to a client (e.g.
        // upgrade/pulse replies).
        maybe(reply_message.header.invalid() == null);
        assert((reply_message.header.client == 0) == (client_index == null));
        assert(reply_message.header.client == prepare_message.header.client);
        assert(reply_message.header.request == prepare_message.header.request);
        assert(reply_message.header.command == .reply);
        assert(reply_message.header.operation == prepare_message.header.operation);
        assert(reply_message.header.op == prepare_message.header.op);

        var pending_replies = sequence.stalled_queue.iterator();
        while (pending_replies.next()) |pending| {
            assert(reply_message.header.op != pending.reply.header.op);
        }

        sequence.stalled_queue.add(.{
            .client_index = client_index,
            .prepare = sequence.clone_message(prepare_message.base_const()).into(.prepare).?,
            .reply = sequence.clone_message(reply_message.base_const()).into(.reply).?,
        }) catch unreachable;
    }

    pub fn contains(sequence: *ReplySequence, reply: *const Message.Reply) bool {
        assert(reply.header.command == .reply);

        var pending_replies = sequence.stalled_queue.iterator();
        while (pending_replies.next()) |pending| {
            if (reply.header.op == pending.reply.header.op) {
                assert(reply.header.checksum == pending.reply.header.checksum);
                return true;
            }
        }
        return false;
    }

    // TODO(Zig): This type signature could be *const once std.PriorityQueue.peek() is updated.
    pub fn peek(sequence: *ReplySequence, op: u64) ?PendingReply {
        assert(sequence.stalled_queue.count() <= stalled_queue_capacity);

        const commit = sequence.stalled_queue.peek() orelse return null;
        if (commit.reply.header.op == op) {
            return commit;
        } else {
            assert(commit.reply.header.op > op);
            return null;
        }
    }

    pub fn next(sequence: *ReplySequence) void {
        const commit = sequence.stalled_queue.remove();
        sequence.message_pool.unref(commit.reply);
        sequence.message_pool.unref(commit.prepare);
    }

    /// Copy the message from a Client's MessagePool to the ReplySequence's MessagePool.
    ///
    /// The client has a finite amount of messages in its pool, and the ReplySequence needs to hold
    /// onto prepares/replies until all preceding prepares/replies have arrived.
    ///
    /// Returns the ReplySequence's message.
    fn clone_message(sequence: *ReplySequence, message_client: *const Message) *Message {
        const message_sequence = sequence.message_pool.get_message(null);
        stdx.copy_disjoint(.exact, u8, message_sequence.buffer, message_client.buffer);
        return message_sequence;
    }
};
