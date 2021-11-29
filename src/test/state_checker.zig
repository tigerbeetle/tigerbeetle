const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");

const Cluster = @import("cluster.zig").Cluster;
const Network = @import("network.zig").Network;
const StateMachine = @import("state_machine.zig").StateMachine;

const message_pool = @import("../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

const RequestQueue = RingBuffer(u128, config.client_request_queue_max, .array);
const StateTransitions = std.AutoHashMap(u128, u64);

const log = std.log.scoped(.state_checker);

pub const StateChecker = struct {
    /// Indexed by client index as used by Cluster.
    client_requests: [config.clients_max]RequestQueue =
        [_]RequestQueue{.{}} ** config.clients_max,

    /// Indexed by replica index.
    state_machine_states: [config.replicas_max]u128,

    history: StateTransitions,

    /// The highest cannonical state reached by the cluster.
    state: u128,

    /// The number of times the cannonical state has been advanced.
    transitions: u64 = 0,

    pub fn init(allocator: mem.Allocator, cluster: *Cluster) !StateChecker {
        const state = cluster.state_machines[0].state;

        var state_machine_states: [config.replicas_max]u128 = undefined;
        for (cluster.state_machines) |state_machine, i| {
            assert(state_machine.state == state);
            state_machine_states[i] = state_machine.state;
        }

        var history = StateTransitions.init(allocator);
        errdefer history.deinit();

        var state_checker = StateChecker{
            .state_machine_states = state_machine_states,
            .history = history,
            .state = state,
        };

        try state_checker.history.putNoClobber(state, state_checker.transitions);

        return state_checker;
    }

    pub fn deinit(state_checker: *StateChecker) void {
        state_checker.history.deinit();
    }

    pub fn check_state(state_checker: *StateChecker, replica: u8) void {
        const cluster = @fieldParentPtr(Cluster, "state_checker", state_checker);

        const a = state_checker.state_machine_states[replica];
        const b = cluster.state_machines[replica].state;

        if (b == a) return;
        state_checker.state_machine_states[replica] = b;

        // If some other replica has already reached this state, then it will be in the history:
        if (state_checker.history.get(b)) |transition| {
            // A replica may transition more than once to the same state, for example, when
            // restarting after a crash and replaying the log. The more important invariant is that
            // the cluster as a whole may not transition to the same state more than once, and once
            // transitioned may not regress.
            log.info(
                "{d:0>4}/{d:0>4} {x:0>32} > {x:0>32} {}",
                .{ transition, state_checker.transitions, a, b, replica },
            );
            return;
        }

        // The replica has transitioned to state `b` that is not yet in the history.
        // Check if this is a valid new state based on all currently inflight client requests.
        for (state_checker.client_requests) |*queue| {
            if (queue.head_ptr()) |input| {
                if (b == StateMachine.hash(state_checker.state, std.mem.asBytes(input))) {
                    const transitions_executed = state_checker.history.get(a).?;
                    if (transitions_executed < state_checker.transitions) {
                        @panic("replica skipped interim transitions");
                    } else {
                        assert(transitions_executed == state_checker.transitions);
                    }

                    state_checker.state = b;
                    state_checker.transitions += 1;

                    log.info("     {d:0>4} {x:0>32} > {x:0>32} {}", .{
                        state_checker.transitions,
                        a,
                        b,
                        replica,
                    });

                    state_checker.history.putNoClobber(b, state_checker.transitions) catch {
                        @panic("state checker unable to allocate memory for history.put()");
                    };

                    // As soon as we reach a valid state we must pop the inflight request.
                    // We cannot wait until the client receives the reply because that would allow
                    // the inflight request to be used to reach other states in the interim.
                    // We must therefore use our own queue rather than the clients' queues.
                    _ = queue.pop();
                    return;
                }
            }
        }

        @panic("replica transitioned to an invalid state");
    }

    pub fn convergence(state_checker: *StateChecker) bool {
        const cluster = @fieldParentPtr(Cluster, "state_checker", state_checker);

        const a = state_checker.state_machine_states[0];
        for (state_checker.state_machine_states[1..cluster.options.replica_count]) |b| {
            if (b != a) return false;
        }

        const transitions_executed = state_checker.history.get(a).?;
        if (transitions_executed < state_checker.transitions) {
            @panic("cluster reached convergence but on a regressed state");
        } else {
            assert(transitions_executed == state_checker.transitions);
        }

        return true;
    }
};
