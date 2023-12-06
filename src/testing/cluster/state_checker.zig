const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");

const message_pool = @import("../../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const ReplicaSet = std.StaticBitSet(constants.members_max);
const Commits = std.ArrayList(struct {
    header: vsr.Header.Prepare,
    replicas: ReplicaSet = ReplicaSet.initEmpty(),
});

const ReplicaHead = struct {
    view: u32,
    op: u64,
};

const log = std.log.scoped(.state_checker);

pub fn StateCheckerType(comptime Client: type, comptime Replica: type) type {
    return struct {
        const Self = @This();

        node_count: u8,
        replica_count: u8,

        commits: Commits,
        commit_mins: [constants.members_max]u64 = [_]u64{0} ** constants.members_max,

        replicas: []const Replica,
        clients: []const Client,

        /// The number of times the canonical state has been advanced.
        requests_committed: u64 = 0,

        /// Tracks the latest op acked by a replica across restarts.
        replica_head_max: []ReplicaHead,
        pub fn init(allocator: mem.Allocator, options: struct {
            cluster_id: u128,
            replica_count: u8,
            replicas: []const Replica,
            clients: []const Client,
        }) !Self {
            const root_prepare = vsr.Header.Prepare.root(options.cluster_id);

            var commits = Commits.init(allocator);
            errdefer commits.deinit();

            var commit_replicas = ReplicaSet.initEmpty();
            for (options.replicas, 0..) |_, i| commit_replicas.set(i);
            try commits.append(.{
                .header = root_prepare,
                .replicas = commit_replicas,
            });

            var replica_head_max = try allocator.alloc(ReplicaHead, options.replicas.len);
            errdefer allocator.free(replica_head_max);
            for (replica_head_max) |*head| head.* = .{ .view = 0, .op = 0 };

            return Self{
                .node_count = @as(u8, @intCast(options.replicas.len)),
                .replica_count = options.replica_count,
                .commits = commits,
                .replicas = options.replicas,
                .clients = options.clients,
                .replica_head_max = replica_head_max,
            };
        }

        pub fn deinit(state_checker: *Self) void {
            const allocator = state_checker.commits.allocator;
            state_checker.commits.deinit();
            allocator.free(state_checker.replica_head_max);
        }

        pub fn on_message(state_checker: *Self, message: *const Message) void {
            if (message.header.into_const(.prepare_ok)) |header| {
                const head = &state_checker.replica_head_max[header.replica];
                if (header.view > head.view or
                    (header.view == head.view and header.op > head.op))
                {
                    head.view = header.view;
                    head.op = header.op;
                }
            }
        }

        /// Returns whether the replica's state changed since the last check_state().
        pub fn check_state(state_checker: *Self, replica_index: u8) !void {
            const replica = &state_checker.replicas[replica_index];
            if (replica.syncing.target()) |sync_target| {
                // Allow a syncing replica to fast-forward its commit.
                //
                // But "fast-forwarding" may actually move commit_min slightly backwards:
                // 1. Suppose op X is a checkpoint trigger.
                // 2. We are committing op X-1 but are stuck due to a block that does not exist in
                //    the cluster anymore.
                // 3. When we sync, `commit_min` "backtracks", to `X - lsm_batch_multiple`.
                const commit_min_source = state_checker.commit_mins[replica_index];
                const commit_min_target = sync_target.checkpoint_op;
                assert(commit_min_source <= commit_min_target + constants.lsm_batch_multiple);
                state_checker.commit_mins[replica_index] = commit_min_target;
                return;
            }

            if (replica.status != .recovering_head) {
                const head_max = &state_checker.replica_head_max[replica_index];
                assert(replica.view > head_max.view or
                    (replica.view == head_max.view and replica.op >= head_max.op));
            }

            const commit_root_op = replica.superblock.working.vsr_state.checkpoint.commit_min;
            const commit_root = replica.superblock.working.vsr_state.checkpoint.commit_min_checksum;

            const commit_a = state_checker.commit_mins[replica_index];
            const commit_b = replica.commit_min;

            const header_b = replica.journal.header_with_op(replica.commit_min);
            assert(header_b != null or replica.commit_min == replica.op_checkpoint());
            assert(header_b == null or header_b.?.op == commit_b);

            const checksum_a = state_checker.commits.items[commit_a].header.checksum;
            // Even if we have header_b, if its op is commit_root_op, we can't trust it.
            // If we just finished state sync, the header in our log might not have been
            // committed (it might be left over from before sync).
            const checksum_b = if (commit_b == commit_root_op) commit_root else header_b.?.checksum;

            assert(checksum_b != commit_root or replica.commit_min == replica.superblock.working.vsr_state.checkpoint.commit_min);
            assert((commit_a == commit_b) == (checksum_a == checksum_b));

            if (checksum_a == checksum_b) return;

            assert(commit_b < commit_a or commit_a + 1 == commit_b);
            state_checker.commit_mins[replica_index] = commit_b;

            // If some other replica has already reached this state, then it will be in the commit history:
            if (replica.commit_min < state_checker.commits.items.len) {
                state_checker.commits.items[replica.commit_min].replicas.set(replica_index);

                assert(replica.commit_min < state_checker.commits.items.len);
                // A replica may transition more than once to the same state, for example, when
                // restarting after a crash and replaying the log. The more important invariant is that
                // the cluster as a whole may not transition to the same state more than once, and once
                // transitioned may not regress.
                return;
            }

            if (header_b == null) return;
            assert(header_b.?.checksum == checksum_b);
            assert(header_b.?.parent == checksum_a);
            assert(header_b.?.op > 0);
            assert(header_b.?.command == .prepare);
            assert(header_b.?.operation != .reserved);

            // The replica has transitioned to state `b` that is not yet in the commit history.
            // Check if this is a valid new state based on the originating client's inflight request.
            const client = for (state_checker.clients) |*client| {
                if (client.id == header_b.?.client) break client;
            } else unreachable;

            if (client.request_queue.empty()) {
                return error.ReplicaTransitionedToInvalidState;
            }

            const request = client.request_queue.head_ptr_const().?;
            assert(request.message.header.client == header_b.?.client);
            assert(request.message.header.checksum == header_b.?.request_checksum);
            assert(request.message.header.request == header_b.?.request);
            assert(request.message.header.command == .request);
            assert(request.message.header.operation == header_b.?.operation);
            assert(request.message.header.size == header_b.?.size);
            // `checksum_body` will not match; the leader's StateMachine updated the timestamps in the
            // prepare body's accounts/transfers.

            state_checker.requests_committed += 1;
            assert(state_checker.requests_committed == header_b.?.op);

            assert(state_checker.commits.items.len == header_b.?.op);
            state_checker.commits.append(.{ .header = header_b.?.* }) catch unreachable;
            state_checker.commits.items[header_b.?.op].replicas.set(replica_index);
        }

        pub fn replica_convergence(state_checker: *Self, replica_index: u8) bool {
            const a = state_checker.commits.items.len - 1;
            const b = state_checker.commit_mins[replica_index];
            return a == b;
        }

        pub fn assert_cluster_convergence(state_checker: *Self) void {
            for (state_checker.commits.items, 0..) |commit, i| {
                assert(commit.replicas.count() > 0);
                assert(commit.header.command == .prepare);
                assert(commit.header.op == i);
                if (i > 0) {
                    const previous = state_checker.commits.items[i - 1].header;
                    assert(commit.header.parent == previous.checksum);
                    assert(commit.header.view >= previous.view);
                }
            }
        }

        pub fn header_with_op(state_checker: *Self, op: u64) vsr.Header.Prepare {
            const commit = &state_checker.commits.items[op];
            assert(commit.header.op == op);
            assert(commit.replicas.count() > 0);
            return commit.header;
        }
    };
}
