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
    /// For operation=deregister: the first report of a replica that committed the op (outside of
    /// WAL replay). Every later report must match it.
    deregister: ?struct { client: u128, session_removed: bool } = null,
});

/// Coverage counters for client session churn, reported by the VOPR.
/// Each op is counted once, when the first replica commits it.
pub const DeregisterStats = struct {
    /// Deregisters that removed the session.
    session_removed: u32 = 0,
    /// Deregisters that found no session (for example, a register ahead in the pipeline evicted
    /// it).
    session_missing: u32 = 0,
    /// Deregisters committed while the session's previous reply was still being written to its
    /// reply slot. (The swarm seldom produces this, if ever. The interleaving is safe by
    /// construction: `ClientReplies.write_reply_callback()` only updates `writing`/`writes`, the
    /// read callbacks re-resolve the slot with `get_slot_for_header()`, so a late read cannot mark
    /// a free slot faulty, and `remove_reply()` only clears `faulty`.)
    reply_write_inflight: u32 = 0,
    /// Registers that took a free slot below an occupied one, without evicting. Slots are taken
    /// lowest first, and an eviction reuses the slot that it frees, so only a deregister leaves
    /// such a slot. (Slots freed at the top of the table are not counted.)
    register_into_freed_slot: u32 = 0,
    /// Registers and deregisters less than one prepare queue apart.
    register_near_deregister: u32 = 0,
};

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
        /// Tracks the latest reply for every non-evicted, non-deregistered client.
        client_replies: std.AutoArrayHashMapUnmanaged(u128, vsr.Header.Reply),
        /// The clients that are gone: some replica has ended their session at their request
        /// (operation=deregister). A gone client never sends another request, so the only op
        /// that the cluster may still commit for it is a replayed (zombie) register.
        clients_gone: std.AutoArrayHashMapUnmanaged(u128, void),
        /// The op of each gone client's deregister. There is one per client: a deregistered client
        /// is deinitialized, so it never deregisters again (and a zombie session never sends one).
        clients_deregister_op: std.AutoArrayHashMapUnmanaged(u128, u64),
        deregister_stats: DeregisterStats = .{},
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

            var clients_gone: std.AutoArrayHashMapUnmanaged(u128, void) = .{};
            try clients_gone.ensureTotalCapacity(allocator, options.clients.len);
            errdefer clients_gone.deinit(allocator);

            var clients_deregister_op: std.AutoArrayHashMapUnmanaged(u128, u64) = .{};
            try clients_deregister_op.ensureTotalCapacity(allocator, options.clients.len);
            errdefer clients_deregister_op.deinit(allocator);

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
                .clients_gone = clients_gone,
                .clients_deregister_op = clients_deregister_op,
                .replica_head_max = replica_head_max,
            };
        }

        pub fn deinit(state_checker: *StateChecker) void {
            const allocator = state_checker.commits.allocator;

            allocator.free(state_checker.replica_head_max);
            state_checker.clients_deregister_op.deinit(allocator);
            state_checker.clients_gone.deinit(allocator);
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

        /// Unlike an eviction, a deregistration doesn't disable checking of
        /// `Client.request_inflight`: the client asked for it, so its session can only end once,
        /// and `check_state()` accounts for the one way in which the cluster can still commit an
        /// op for the client afterwards: a replayed (zombie) register.
        ///
        /// Called by every replica that commits the deregister (outside of WAL replay), whether or
        /// not the session still existed (it may have been evicted while the deregister was being
        /// prepared). The replicas must agree on the outcome.
        pub fn on_client_deregistration(
            state_checker: *StateChecker,
            deregistration: struct { op: u64, client: u128, session_removed: bool },
        ) void {
            assert(deregistration.op > 0);
            assert(deregistration.client != 0);

            // The replica reports `.committed` (so `check_state()` has recorded the op) before it
            // updates its client table.
            assert(deregistration.op < state_checker.commits.items.len);
            const commit = &state_checker.commits.items[deregistration.op];
            assert(commit.header.op == deregistration.op);
            assert(commit.header.operation == .deregister);
            assert(commit.header.client == deregistration.client);

            // This catches a replica that removes a different session, or that disagrees about
            // whether the session existed. It cannot catch a replica that never reports (for
            // example, because it state-synced past the op): such a divergence surfaces at the
            // next checkpoint instead, since the client table is part of it.
            if (commit.deregister) |deregister| {
                assert(deregister.client == deregistration.client);
                assert(deregister.session_removed == deregistration.session_removed);
            } else {
                commit.deregister = .{
                    .client = deregistration.client,
                    .session_removed = deregistration.session_removed,
                };
                if (deregistration.session_removed) {
                    state_checker.deregister_stats.session_removed += 1;
                } else {
                    state_checker.deregister_stats.session_missing += 1;
                }
            }

            const op_gop =
                state_checker.clients_deregister_op.getOrPutAssumeCapacity(deregistration.client);
            if (op_gop.found_existing) {
                assert(op_gop.value_ptr.* == deregistration.op);
            } else {
                op_gop.value_ptr.* = deregistration.op;
            }
            state_checker.clients_gone.putAssumeCapacity(deregistration.client, {});
        }

        /// A deregistered session never comes back, except as a newer (zombie) session that a
        /// replayed register created. This is checked only on replicas that are not syncing, and
        /// that have executed the deregister and its client table update: `check_state()` runs
        /// before the client table update of `commit_min`, hence `commit_min > op`.
        /// - A replica that lags behind the deregister still has the session.
        /// - A replica that restarted from a checkpoint below the deregister replays it without
        ///   updating its client table, but that table reflects the checkpoint's trigger, which is
        ///   at or above the deregister. The same holds for a replica that state-synced.
        fn check_deregistered(state_checker: *const StateChecker, replica_index: u8) void {
            const replica = &state_checker.replicas[replica_index];
            assert(replica.syncing == .idle);

            for (
                state_checker.clients_deregister_op.keys(),
                state_checker.clients_deregister_op.values(),
            ) |client, op| {
                if (replica.commit_min > op) {
                    const sessions = &replica.client_sessions;
                    if (sessions.entries_by_client.get(client)) |slot_index| {
                        assert(sessions.entries[slot_index].session > op);
                    }
                }
            }
        }

        fn count_deregister_stats(
            state_checker: *StateChecker,
            replica_index: u8,
            header: *const vsr.Header.Prepare,
        ) void {
            assert(header.operation == .register or header.operation == .deregister);
            assert(header.client != 0);
            // The op is about to be appended to the commit history.
            assert(header.op == state_checker.commits.items.len);

            const replica = &state_checker.replicas[replica_index];
            const sessions = &replica.client_sessions;
            const stats = &state_checker.deregister_stats;

            const operation_near: vsr.Operation = switch (header.operation) {
                .register => .deregister,
                .deregister => .register,
                else => unreachable,
            };
            const ops_near = @min(header.op, constants.pipeline_prepare_queue_max);
            for (state_checker.commits.items[header.op - ops_near .. header.op]) |*commit| {
                if (commit.header.operation == operation_near) {
                    stats.register_near_deregister += 1;
                    break;
                }
            }

            if (header.operation == .register) {
                assert(sessions.entries_by_client.get(header.client) == null);
                if (sessions.count() < sessions.capacity()) {
                    const slot_free = sessions.entries_present.first_unset().?;
                    for (slot_free + 1..constants.clients_max) |slot_index| {
                        if (sessions.entries_present.is_set(slot_index)) {
                            stats.register_into_freed_slot += 1;
                            break;
                        }
                    }
                }
            } else {
                if (sessions.entries_by_client.get(header.client)) |slot_index| {
                    if (replica.client_replies.writing.is_set(slot_index)) {
                        stats.reply_write_inflight += 1;
                    }
                }
            }
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
                    if (header.operation == .deregister) {
                        if (state_checker.client_replies.getEntry(header.client)) |entry| {
                            if (entry.value_ptr.op < header.op) {
                                _ = state_checker.client_replies.swapRemove(header.client);
                            } else {
                                // An old message is replayed (the client registered again).
                            }
                        } else {
                            // Client was evicted, or an old message is replayed.
                        }
                    } else if (header.operation == .register and
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

            if (replica.syncing == .idle) state_checker.check_deregistered(replica_index);

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
                    if (header_b.?.operation == .register and
                        state_checker.clients_gone.contains(header_b.?.client))
                    {
                        // The client's session has ended at its request (and the client may have
                        // been deinitialized already, or may still be waiting for the deregister
                        // to complete), but the network replayed its register, which created a
                        // new (zombie) session.
                    } else {
                        const client: *const Client = for (state_checker.clients) |*client| {
                            if (client.*) |*client_open| {
                                if (client_open.id == header_b.?.client) break client_open;
                            }
                        } else return error.ReplicaTransitionedToInvalidState;

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
                    }
                } else {
                    // Either:
                    // - The cluster is running with one or more raw MessageBus "clients", so there
                    //   may be requests not found in `Cluster.clients`.
                    // - The test includes one or more client evictions.
                }
            }

            state_checker.requests_committed += 1;
            assert(state_checker.requests_committed == header_b.?.op);

            // The replica has not yet updated its client table for this op.
            switch (header_b.?.operation) {
                .register, .deregister => state_checker.count_deregister_stats(
                    replica_index,
                    header_b.?,
                ),
                else => {},
            }

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
