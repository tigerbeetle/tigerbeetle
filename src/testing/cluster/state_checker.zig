const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");
const stdx = @import("stdx");
const maybe = stdx.maybe;

const message_pool = @import("../../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const ReplicaSet = stdx.BitSetType(constants.members_max);
const Commits = std.ArrayList(struct {
    header: vsr.Header.Prepare,
    // null for operation=root and operation=upgrade
    release: ?vsr.Release,
    replicas: ReplicaSet = .{},
});

const ReplicaHead = struct {
    view: u32,
    op: u64,
};

pub fn StateCheckerType(comptime Client: type, comptime Replica: type) type {
    return struct {
        const StateChecker = @This();

        node_count: u8,
        replica_count: u8,

        commits: Commits,
        commit_mins: [constants.members_max]u64 = @splat(0),

        replicas: []const Replica,
        clients: []const ?Client,
        /// Tracks the latest reply for every non-evicted client.
        client_replies: std.AutoArrayHashMapUnmanaged(u128, vsr.Header.Reply),
        clients_exhaustive: bool = true,
        clients_register_op_latest: u64 = 0,

        /// The number of times the canonical state has been advanced.
        requests_committed: u64 = 0,

        /// Tracks the latest op acked by a replica across restarts.
        replica_head_max: []ReplicaHead,

        pub fn init(allocator: mem.Allocator, options: struct {
            cluster_id: u128,
            replica_count: u8,
            replicas: []const Replica,
            clients: []const ?Client,
        }) !StateChecker {
            const root_prepare = vsr.Header.Prepare.root(options.cluster_id);

            var commits = Commits.init(allocator);
            errdefer commits.deinit();

            var commit_replicas: ReplicaSet = .{};
            for (options.replicas, 0..) |_, i| commit_replicas.set(i);
            try commits.append(.{
                .header = root_prepare,
                .release = null,
                .replicas = commit_replicas,
            });

            var client_replies: std.AutoArrayHashMapUnmanaged(u128, vsr.Header.Reply) = .{};
            try client_replies.ensureTotalCapacity(allocator, constants.clients_max);
            errdefer client_replies.deinit(allocator);

            const replica_head_max = try allocator.alloc(ReplicaHead, options.replicas.len);
            errdefer allocator.free(replica_head_max);
            for (replica_head_max) |*head| head.* = .{ .view = 0, .op = 0 };

            return StateChecker{
                .node_count = @intCast(options.replicas.len),
                .replica_count = options.replica_count,
                .commits = commits,
                .replicas = options.replicas,
                .clients = options.clients,
                .client_replies = client_replies,
                .replica_head_max = replica_head_max,
            };
        }

        pub fn deinit(state_checker: *StateChecker) void {
            const allocator = state_checker.commits.allocator;

            allocator.free(state_checker.replica_head_max);
            state_checker.client_replies.deinit(allocator);
            state_checker.commits.deinit();
        }

        pub fn on_client_eviction(state_checker: *StateChecker, client_id: u128) void {
            const removed = state_checker.client_replies.swapRemove(client_id);
            maybe(removed);
            // Disable checking of `Client.request_inflight`, to guard against the following panic:
            // 1. Client `A` sends an `operation=register` to a fresh cluster. (`A₁`)
            // 2. Cluster prepares + commits `A₁`, and sends the reply to `A`.
            // 4. `A` receives the reply to `A₁`, and issues a second request (`A₂`).
            // 5. `clients_max` other clients register, evicting `A`'s session.
            // 6. An old retry (or replay) of `A₁` arrives at the cluster.
            // 7. `A₁` is committed (for a second time, as a different op).
            //    If `StateChecker` were to check `Client.request_inflight`, it would see that `A₁`
            //    is not actually in-flight, despite being committed for the "first time" by a
            //    replica.
            state_checker.clients_exhaustive = false;
        }

        pub fn on_message(state_checker: *StateChecker, message: *const Message) void {
            switch (message.header.into_any()) {
                .prepare_ok => |header| {
                    const head = &state_checker.replica_head_max[header.replica];
                    if (header.view > head.view or
                        (header.view == head.view and header.op > head.op))
                    {
                        head.view = header.view;
                        head.op = header.op;
                    }
                },
                .reply => |header| {
                    if (header.operation == .register and
                        header.op > state_checker.clients_register_op_latest)
                    {
                        state_checker.client_replies
                            .putAssumeCapacityNoClobber(header.client, header.*);
                        state_checker.clients_register_op_latest = header.op;
                    } else {
                        if (state_checker.client_replies.getEntry(header.client)) |entry| {
                            if (entry.value_ptr.op < header.op) {
                                entry.value_ptr.* = header.*;
                            } else {
                                // An old message is replayed.
                            }
                        } else {
                            // Client was evicted, an old message is replayed.
                        }
                    }
                },
                else => {},
            }
        }

        /// Verify that the cluster has advanced since the replica was lost.
        /// Then forget about the given replica's progress, since its data file has been "lost".
        pub fn reformat(state_checker: *StateChecker, replica_index: u8) void {
            const reformat_state = state_checker.replica_head_max[replica_index];
            var commit_advanced: bool = false;
            for (
                state_checker.commit_mins[0..state_checker.replica_head_max.len],
                0..,
            ) |commit_min, i| {
                if (i != replica_index) {
                    commit_advanced = commit_advanced or reformat_state.op < commit_min;
                }
            }
            assert(commit_advanced);

            state_checker.replica_head_max[replica_index] = .{ .view = 0, .op = 0 };
            state_checker.commit_mins[replica_index] = 0;
        }

        /// Returns whether the replica's state changed since the last check_state().
        pub fn check_state(state_checker: *StateChecker, replica_index: u8) !void {
            const replica = &state_checker.replicas[replica_index];
            if (replica.syncing == .updating_checkpoint) {
                // Allow a syncing replica to fast-forward its commit.
                //
                // But "fast-forwarding" may actually move commit_min slightly backwards:
                // 1. Suppose op X is a checkpoint trigger.
                // 2. We are committing op X-1 but are stuck due to a block that does not exist in
                //    the cluster anymore.
                // 3. When we sync, `commit_min` "backtracks", to `X - lsm_compaction_ops`.
                const commit_min_source = state_checker.commit_mins[replica_index];
                const commit_min_target =
                    replica.syncing.updating_checkpoint.header.op;
                assert(commit_min_source <= commit_min_target + constants.lsm_compaction_ops);
                state_checker.commit_mins[replica_index] = commit_min_target;
                return;
            }

            assert(replica.view >= state_checker.replica_head_max[replica_index].view);

            const commit_root_op = replica.superblock.working.vsr_state.checkpoint.header.op;
            const commit_root = replica.superblock.working.vsr_state.checkpoint.header.checksum;

            const commit_a = state_checker.commit_mins[replica_index];
            const commit_b = replica.commit_min;

            const header_b = replica.journal.header_with_op(replica.commit_min);

            if (header_b == null and replica.commit_min != replica.op_checkpoint()) {
                // The slot with commit_min may have been overwritten by an op from the next wrap.
                // Further, the op may then also be truncated as part of a view change.
                if (replica.journal.header_for_op(replica.commit_min)) |header| {
                    assert(header.op == replica.commit_min + constants.journal_slot_count);
                }
                return;
            }

            if (header_b != null) assert(header_b.?.op == commit_b);

            const checksum_a = state_checker.commits.items[commit_a].header.checksum;
            // Even if we have header_b, if its op is commit_root_op, we can't trust it.
            // If we just finished state sync, the header in our log might not have been
            // committed (it might be left over from before sync).
            const checksum_b = if (commit_b == commit_root_op) commit_root else header_b.?.checksum;

            assert(checksum_b != commit_root or
                replica.commit_min == replica.superblock.working.vsr_state.checkpoint.header.op);
            assert((commit_a == commit_b) == (checksum_a == checksum_b));

            if (checksum_a == checksum_b) return;

            assert(commit_b < commit_a or commit_a + 1 == commit_b);
            state_checker.commit_mins[replica_index] = commit_b;

            // If some other replica has already reached this state, then it will be in the commit
            // history:
            if (replica.commit_min < state_checker.commits.items.len) {
                const commit = &state_checker.commits.items[commit_b];
                if (replica.op_checkpoint() < replica.commit_min) {
                    if (commit.release) |release| assert(release.value == replica.release.value);
                } else {
                    // When op_checkpoint==commit_min, we recovered from checkpoint, so it is ok if
                    // the release doesn't match. (commit_min is not actually being executed.)
                    assert(replica.op_checkpoint() == replica.commit_min);
                }

                assert(checksum_b == commit.header.checksum);
                commit.replicas.set(replica_index);

                assert(replica.commit_min < state_checker.commits.items.len);
                // A replica may transition more than once to the same state, for example, when
                // restarting after a crash and replaying the log. The more important invariant is
                // that the cluster as a whole may not transition to the same state more than once,
                // and once transitioned may not regress.
                return;
            }

            if (header_b == null) return;
            assert(header_b.?.checksum == checksum_b);
            assert(header_b.?.parent == checksum_a);
            assert(header_b.?.op > 0);
            assert(header_b.?.command == .prepare);
            assert(header_b.?.operation != .reserved);

            if (header_b.?.client == 0) {
                assert(header_b.?.operation == .upgrade or
                    header_b.?.operation == .pulse);
            } else {
                if (state_checker.clients_exhaustive) {
                    // The replica has transitioned to state `b` that is not yet in the commit
                    // history. Check if this is a valid new state based on the originating client's
                    // inflight request.
                    const client: *const Client = for (state_checker.clients) |*client| {
                        if (client.*.?.id == header_b.?.client) break &client.*.?;
                    } else unreachable;

                    if (client.request_inflight == null) {
                        return error.ReplicaTransitionedToInvalidState;
                    }

                    const request = client.request_inflight.?.message;
                    assert(request.header.client == header_b.?.client);
                    assert(request.header.checksum == header_b.?.request_checksum);
                    assert(request.header.request == header_b.?.request);
                    assert(request.header.command == .request);
                    assert(request.header.operation == header_b.?.operation);
                    assert(request.header.size == header_b.?.size);
                    // `checksum_body` will not match; the leader's StateMachine updated the
                    // timestamps in the prepare body's accounts/transfers.
                } else {
                    // Either:
                    // - The cluster is running with one or more raw MessageBus "clients", so there
                    //   may be requests not found in `Cluster.clients`.
                    // - The test includes one or more client evictions.
                }
            }

            state_checker.requests_committed += 1;
            assert(state_checker.requests_committed == header_b.?.op);

            const release = release: {
                if (header_b.?.operation == .root or
                    header_b.?.operation == .upgrade)
                {
                    break :release null;
                } else {
                    break :release replica.release;
                }
            };

            assert(state_checker.commits.items.len == header_b.?.op);
            state_checker.commits.append(.{
                .header = header_b.?.*,
                .release = release,
            }) catch unreachable;
            state_checker.commits.items[header_b.?.op].replicas.set(replica_index);
        }

        pub fn replica_convergence(state_checker: *StateChecker, replica_index: u8) bool {
            const a = state_checker.commits.items.len - 1;
            const b = state_checker.commit_mins[replica_index];
            return a == b;
        }

        pub fn assert_cluster_convergence(state_checker: *StateChecker) void {
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

        pub fn header_with_op(state_checker: *StateChecker, op: u64) vsr.Header.Prepare {
            assert(op < state_checker.commits.items.len);
            const commit = &state_checker.commits.items[op];
            assert(commit.header.op == op);
            assert(commit.replicas.count() > 0);
            return commit.header;
        }
    };
}
