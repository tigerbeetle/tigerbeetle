const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");

const Client = @import("../cluster.zig").Client;
const Replica = @import("../cluster.zig").Replica;
const message_pool = @import("../../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const RingBuffer = @import("../../ring_buffer.zig").RingBuffer;

const StateTransitions = std.AutoHashMap(u128, u64);

const log = std.log.scoped(.state_checker);

pub const StateChecker = struct {
    /// Indexed by replica index.
    replica_states: [constants.replicas_max]u128 = [_]u128{0} ** constants.replicas_max,

    /// Keyed by committed `message.header.checksum`.
    ///
    /// The first state is always `root_prepare.checksum`, since the root prepare doesn't
    /// commit normally.
    history: StateTransitions,

    root: u128,

    // TODO When StateChecker is owned by the Simulation, use @fieldParentPtr to get these.
    replicas: []const Replica,
    clients: []const Client,

    /// The highest canonical state reached by the cluster.
    state: u128 = 0,

    /// The number of times the canonical state has been advanced.
    requests_committed: u64 = 0,

    pub fn init(
        allocator: mem.Allocator,
        cluster: u32,
        replicas: []const Replica,
        clients: []const Client,
    ) !StateChecker {
        var history = StateTransitions.init(allocator);
        errdefer history.deinit();

        const root_checksum = vsr.Header.root_prepare(cluster).checksum;

        var state_checker = StateChecker{
            .history = history,
            .root = root_checksum,
            .replicas = replicas,
            .clients = clients,
        };
        try state_checker.history.putNoClobber(root_checksum, state_checker.requests_committed);

        return state_checker;
    }

    pub fn deinit(state_checker: *StateChecker) void {
        state_checker.history.deinit();
    }

    pub fn check_state(state_checker: *StateChecker, replica_index: u8) !void {
        const replica = &state_checker.replicas[replica_index];
        const commit_header = header: {
            if (replica.journal.status == .recovered) {
                const commit_header = replica.journal.header_with_op(replica.commit_min);
                assert(commit_header != null or replica.commit_min == replica.op_checkpoint);
                break :header replica.journal.header_with_op(replica.commit_min);
            } else {
                // Still recovering.
                break :header null;
            }
        };

        const a = state_checker.replica_states[replica_index];
        const b = if (commit_header) |h| h.checksum else state_checker.root;

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
                .{ transition, state_checker.requests_committed, a, b, replica_index },
            );
            return;
        }

        if (commit_header == null) return;
        assert(commit_header.?.parent == a);
        assert(commit_header.?.op > 0);
        assert(commit_header.?.command == .prepare);
        assert(commit_header.?.operation != .reserved);

        // The replica has transitioned to state `b` that is not yet in the history.
        // Check if this is a valid new state based on the originating client's inflight request.
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

        const transitions_executed = state_checker.history.get(a).?;
        if (transitions_executed < state_checker.requests_committed) {
            return error.ReplicaSkippedInterimTransitions;
        } else {
            assert(transitions_executed == state_checker.requests_committed);
        }

        state_checker.state = b;
        state_checker.requests_committed += 1;
        assert(state_checker.requests_committed == commit_header.?.op);

        log.info("     {d:0>4} {x:0>32} > {x:0>32} {}", .{
            state_checker.requests_committed,
            a,
            b,
            replica_index,
        });

        state_checker.history.putNoClobber(b, state_checker.requests_committed) catch {
            @panic("state checker unable to allocate memory for history.put()");
        };
    }

    pub fn convergence(state_checker: *StateChecker) bool {
        const a = state_checker.replica_states[0];
        for (state_checker.replica_states[1..state_checker.replicas[0].replica_count]) |b| {
            if (b != a) return false;
        }

        const transitions_executed = state_checker.history.get(a).?;
        if (transitions_executed < state_checker.requests_committed) {
            // Cluster reached convergence but on a regressed state.
            // A replica reached the transition limit, crashed, then repaired.
            return false;
        } else {
            assert(transitions_executed == state_checker.requests_committed);
        }

        return true;
    }
};
