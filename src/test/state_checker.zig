const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");

const Cluster = @import("cluster.zig").Cluster;
const Network = @import("network.zig").Network;
const Storage = @import("storage.zig").Storage;
const Client = @import("cluster.zig").Client;
const Replica = @import("cluster.zig").Replica;

const message_pool = @import("../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

const StateTransitions = std.AutoHashMap(u128, u64);

const log = std.log.scoped(.state_checker);

pub const StateChecker = struct {
    /// Indexed by replica index.
    replica_states: [config.replicas_max]u128 = [_]u128{0} ** config.replicas_max,

    /// Keyed by committed `message.header.checksum`.
    ///
    /// The `0` key represents the state before the root prepare has committed (e.g. during WAL
    /// recovery).
    history: StateTransitions,

    // TODO When StateChecker is owned by the Simulation, use @fieldParentPtr to get these.
    replicas: []const Replica,
    clients: []const Client,

    /// The highest canonical state reached by the cluster.
    state: u128 = 0,

    /// The number of times the canonical state has been advanced *including*:
    /// * `register` operations, and
    /// * the root message.
    committed_messages: u64 = 0,

    /// The number of times the canonical state has been advanced *excluding*:
    /// * `register` operations, and
    /// * the root message.
    /// (That is, correspond 1:1 with `client.request()` calls).
    committed_requests: u64 = 0,

    pub fn init(
        allocator: mem.Allocator,
        replicas: []const Replica,
        clients: []const Client,
    ) !StateChecker {
        var history = StateTransitions.init(allocator);
        errdefer history.deinit();

        var state_checker = StateChecker{
            .history = history,
            .replicas = replicas,
            .clients = clients,
        };
        try state_checker.history.putNoClobber(0, state_checker.committed_messages);

        return state_checker;
    }

    pub fn deinit(state_checker: *StateChecker) void {
        state_checker.history.deinit();
    }

    pub fn check_state(state_checker: *StateChecker, replica_index: u8) !void {
        const replica = state_checker.replicas[replica_index];

        // Until the journal has recovered, `commit_min` may not be in the headers.
        // TODO Verify that `replica.commit_min` always exists after recovering via state transfer.
        const commit_header =
            if (replica.journal.recovered) replica.journal.header_with_op(replica.commit_min).? else null;

        const a = state_checker.replica_states[replica_index];
        const b = if (commit_header) |h| h.checksum else 0;

        if (b == a) return;
        state_checker.replica_states[replica_index] = b;

        // If some other replica has already reached this state, then it will be in the history:
        if (state_checker.history.get(b)) |transition| {
            // A replica may transition more than once to the same state, for example, when
            // restarting after a crash and replaying the log. The more important invariant is that
            // the cluster as a whole may not transition to the same state more than once, and once
            // transitioned may not regress.
            log.info(
                "{d:0>4}/{d:0>4} {x:0>32} > {x:0>32} {}",
                .{ transition, state_checker.committed_messages, a, b, replica_index },
            );
            return;
        }

        if (commit_header == null) return;
        assert(commit_header.?.command == .prepare);
        assert(commit_header.?.parent == a);
        assert(commit_header.?.operation != .reserved);

        // The replica has transitioned to state `b` that is not yet in the history.
        // Check if this is a valid new state based on the originating client's inflight request.
        if (commit_header.?.op == 0) {
            assert(commit_header.?.operation == .root);
        } else {
            const client = for (state_checker.clients) |*client| {
                if (client.id == commit_header.?.client) break client;
            } else unreachable;

            if (client.request_queue.empty()) {
                return error.ReplicaTransitionedToInvalidState;
            }

            const request = client.request_queue.head_ptr_const().?;
            assert(request.message.header.client == commit_header.?.client);
            assert(request.message.header.request == commit_header.?.request);
            assert(request.message.header.command == .request);
            assert(request.message.header.operation == commit_header.?.operation);
            assert(request.message.header.size == commit_header.?.size);
            // `checksum_body` will not match; the leader's StateMachine updated the timestamps in the
            // prepare body's accounts/transfers.
        }

        const transitions_executed = state_checker.history.get(a).?;
        if (transitions_executed < state_checker.committed_messages) {
            return error.ReplicaSkippedInterimTransitions;
        } else {
            assert(transitions_executed == state_checker.committed_messages);
        }

        state_checker.state = b;
        state_checker.committed_messages += 1;
        state_checker.committed_requests += @boolToInt(
            commit_header.?.operation != .register and
                commit_header.?.operation != .root,
        );
        assert(state_checker.committed_messages == commit_header.?.op + 1);

        log.info("     {d:0>4} {x:0>32} > {x:0>32} {}", .{
            state_checker.committed_messages,
            a,
            b,
            replica_index,
        });

        state_checker.history.putNoClobber(b, state_checker.committed_messages) catch {
            @panic("state checker unable to allocate memory for history.put()");
        };
    }

    pub fn convergence(state_checker: *StateChecker) bool {
        const a = state_checker.replica_states[0];
        for (state_checker.replica_states[1..state_checker.replicas[0].replica_count]) |b| {
            if (b != a) return false;
        }

        const transitions_executed = state_checker.history.get(a).?;
        if (transitions_executed < state_checker.committed_messages) {
            // Cluster reached convergence but on a regressed state.
            // A replica reached the transition limit, crashed, then repaired.
            return false;
        } else {
            assert(transitions_executed == state_checker.committed_messages);
        }

        return true;
    }
};
