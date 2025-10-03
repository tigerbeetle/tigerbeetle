const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const ratio = stdx.PRNG.ratio;
const Ratio = stdx.PRNG.Ratio;

pub const RepairBudgetJournal = struct {
    capacity: u32,
    available: u32,

    replica_index: u8,

    // Tracks the prepare ops requested from each remote replica.
    replicas_requested_prepares: []RequestedPrepares,

    // Exponential weighted moving average of the repair latency for each remote replica.
    //
    // Repair latency is calculated as the duration elapsed between when a prepare is requested from
    // a remote replica, and when it is either received from the remote replica (see `decrement`),
    // or expired (see `maybe_expire_requested_prepares`).
    replicas_repair_latency: []stdx.Duration,

    // Probability of choosing a random replica with available budget, as opposed to one with the
    // best repair latency with available budget.
    //
    // Experiments ensure that we try alternative repair routes, and avoids potential resonance
    // wherein we keep requesting from a permanently crashed replica with the best repair latency.
    // This is because we don't penalize the repair latency once it exceeds `duration_expiry_max`,
    // so if a crashed replica has the best latency, it may remain that way forever.
    experiment_chance: Ratio = ratio(1, 10),

    // Multiple of repair latency used to determine expiry duration, which is the time we wait
    // before reclaiming the budget for an inflight repair request.
    repair_latency_multiple_expiry: u8 = 2,

    // The maximum amount of time we wait before reclaiming the budget for an inflight repair
    // request.
    //
    // Capped at 500ms to avoid an unbounded increase in the tracked repair latency for remote
    // replicas. Specifically, helps avoid the case where a partitioned replica with missing
    // prepares gets into a cycle of requesting prepares, waiting for them to expire, and then
    // increasing the repair latency on expiry.
    duration_expiry_max: stdx.Duration = .ms(500),

    // Maximum inflight `request_prepare` messages per remote replica, at any point of time.
    //
    // This is kept small to ensure that even if the budget to a remote replica is saturated
    // by multiple replicas, overflowing the egress `send_queue` (which leads to dropped messages)
    // on the remote replica is unlikely. For example, since the `send_queue` is currently sized
    // to 4 messages, if we were to set this limit to 4 as well, multiple repairing replicas are
    // more likely to overflow the remote replica's send queue.
    const repair_messages_inflight_count_max = 2;

    const RequestedPrepares = std.AutoArrayHashMapUnmanaged(u64, stdx.Instant);

    pub fn init(gpa: std.mem.Allocator, options: struct {
        replica_index: u8,
        replica_count: u8,
    }) !RepairBudgetJournal {
        const remote_replica_count = if (options.replica_index < options.replica_count)
            // Replicas can repair from all replicas but themselves.
            options.replica_count - 1
        else
            // Standbys can repair from all replicas.
            options.replica_count;

        var replicas_requested_prepares = try gpa.alloc(RequestedPrepares, options.replica_count);
        errdefer gpa.free(replicas_requested_prepares);

        for (replicas_requested_prepares, 0..) |*requested_prepares, replica| {
            errdefer for (replicas_requested_prepares[0..replica]) |*m| m.deinit(gpa);
            requested_prepares.* = .{};

            try requested_prepares.ensureTotalCapacity(gpa, repair_messages_inflight_count_max);
            errdefer requested_prepares.deinit(gpa);
        }

        errdefer for (replicas_requested_prepares) |*m| m.deinit(gpa);

        const replicas_repair_latency = try gpa.alloc(stdx.Duration, options.replica_count);
        errdefer gpa.free(replicas_repair_latency);

        // Initialize repair latency to 1 ms for all replicas, this gets refined as we start
        // repairing from these replicas. We choose a value lower than the the typical latency
        // between two replicas, so as to not bias replica selection when we have few measurements.
        @memset(replicas_repair_latency, .ms(1));

        return RepairBudgetJournal{
            .capacity = repair_messages_inflight_count_max * remote_replica_count,
            .available = repair_messages_inflight_count_max * remote_replica_count,
            .replica_index = options.replica_index,
            .replicas_requested_prepares = replicas_requested_prepares,
            .replicas_repair_latency = replicas_repair_latency,
        };
    }

    pub fn deinit(budget: *RepairBudgetJournal, gpa: std.mem.Allocator) void {
        for (budget.replicas_requested_prepares) |*requested_prepares| {
            requested_prepares.deinit(gpa);
        }
        gpa.free(budget.replicas_requested_prepares);
        gpa.free(budget.replicas_repair_latency);
    }

    /// Returns the index of the replica with the lowest repair latency, and budget availability, if
    /// one exists. Otherwise, returns null. For a fraction of ops (guided by `experiment_chance`),
    /// diverges from this heuristic and returns the index of a random replica with budget
    /// availability, using reservoir sampling.
    pub fn decrement(budget: *RepairBudgetJournal, options: struct {
        op: u64,
        now: stdx.Instant,
        prng: *stdx.PRNG,
    }) ?u8 {
        assert(budget.capacity > 0);
        assert(budget.available > 0);

        budget.assert_invariants();
        defer budget.assert_invariants();

        const experiment = options.prng.chance(budget.experiment_chance);
        var experiment_replica_index: ?u8 = null;
        var reservoir = stdx.PRNG.Reservoir.init();

        var repair_latency_min: ?stdx.Duration = null;
        var repair_latency_min_replica_index: ?u8 = null;

        for (budget.replicas_requested_prepares, 0..) |*requested_prepares, replica_index| {
            // Disallow requesting from a replica from which this op already been requested.
            if (requested_prepares.get(options.op) != null) continue;
            // Disallow requests to self.
            if (replica_index == budget.replica_index) continue;
            // Enforce per-replica budget.
            if (requested_prepares.count() == repair_messages_inflight_count_max) continue;

            const replica_repair_latency = budget.replicas_repair_latency[replica_index];

            if (repair_latency_min == null or replica_repair_latency.ns < repair_latency_min.?.ns) {
                repair_latency_min = replica_repair_latency;
                repair_latency_min_replica_index = @intCast(replica_index);
            }

            // Reservoir sampling with an arbitrarily chosen weight of 1 for each item suffices
            // our use case, as the goal is to get some degree of randomness during experiments.
            if (reservoir.replace(options.prng, 1)) {
                experiment_replica_index = @intCast(replica_index);
            }
        }
        assert((repair_latency_min == null) == (repair_latency_min_replica_index == null));
        assert((repair_latency_min_replica_index == null) == (experiment_replica_index == null));

        const replica_index_maybe = if (experiment)
            experiment_replica_index
        else
            repair_latency_min_replica_index;

        if (replica_index_maybe) |replica_index| {
            assert(replica_index != budget.replica_index);
            budget.replicas_requested_prepares[replica_index].putAssumeCapacityNoClobber(
                options.op,
                options.now,
            );

            budget.available -= 1;
        }

        return replica_index_maybe;
    }

    /// Increments the budget by 1 for each replica that this prepare op has been requested from.
    /// Also refines the repair latency for each of these replicas.
    pub fn increment(budget: *RepairBudgetJournal, options: struct {
        op: u64,
        now: stdx.Instant,
    }) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        for (budget.replicas_requested_prepares, 0..) |*requested_prepares, replica_index| {
            if (requested_prepares.fetchSwapRemove(options.op)) |requested_prepare| {
                budget.available += 1;

                // We have no information about the replica that sent this prepare, as the message
                // header stores the index of the primary processed that prepare. Consequently, we
                // refine repair latency for all replicas that this prepare op was requested from.
                // This would lead to some inaccuracy in the latency measurement, but is acceptable
                // since the scenario where a prepare has been requested from multiple replicas is
                // rare in practice. The more common scenario is that we have a large number of
                // prepares missing (for e.g. after state sync, or if a lagging replica transitions
                // to a new checkpoint), in which case we request a unique op from each replica.
                budget.replicas_repair_latency[replica_index] = ewma_add_duration(
                    budget.replicas_repair_latency[replica_index],
                    options.now.duration_since(requested_prepare.value),
                );
            }
        }
    }

    pub fn refill(budget: *RepairBudgetJournal) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        for (budget.replicas_requested_prepares) |*requested_prepares| {
            requested_prepares.clearRetainingCapacity();
        }
        budget.available = budget.capacity;
    }

    /// Iterates through the inflight requests across all remote replicas, and reclaims the budget
    /// for expired requests. Penalizes the replicas for which some expired requests were found,
    /// duration spent waiting for the expired requests to their repair latency.
    ///
    /// Expiry provides resilience to network faults, by ensuring that a dropped packet or the
    /// remote replica crashing doesn't cause an op to get stuck in the queue for a remote replica.
    /// We avoid spurious expiry due to transient network hiccups like increased latency by waiting
    /// for twice the measured repair latency.
    pub fn maybe_expire_requested_prepares(budget: *RepairBudgetJournal, now: stdx.Instant) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        for (budget.replicas_requested_prepares, 0..) |*requested_prepares, replica_index| {
            var requested_prepares_index: u32 = 0;

            while (requested_prepares_index < requested_prepares.entries.len) {
                const requested_at = requested_prepares.values()[requested_prepares_index];
                const duration_since_requested_at = now.duration_since(requested_at);
                const duration_expiry_ns = @min(
                    budget.repair_latency_multiple_expiry *
                        budget.replicas_repair_latency[replica_index].ns,
                    budget.duration_expiry_max.ns,
                );
                if (duration_since_requested_at.ns > duration_expiry_ns) {
                    requested_prepares.swapRemoveAt(requested_prepares_index);
                    budget.replicas_repair_latency[replica_index] = ewma_add_duration(
                        budget.replicas_repair_latency[replica_index],
                        duration_since_requested_at,
                    );
                    budget.available += 1;
                } else {
                    requested_prepares_index += 1;
                }
            }
        }
    }

    fn assert_invariants(budget: *const RepairBudgetJournal) void {
        assert(budget.available <= budget.capacity);
        if (budget.replica_index < budget.replicas_requested_prepares.len) {
            assert(budget.replicas_requested_prepares[budget.replica_index].count() == 0);
        }

        var requested_prepares_count: u32 = 0;
        for (budget.replicas_requested_prepares) |*requested_prepares| {
            requested_prepares_count += @intCast(requested_prepares.count());
        }
        assert(budget.capacity - budget.available == requested_prepares_count);
    }

    fn ewma_add_duration(old: stdx.Duration, new: stdx.Duration) stdx.Duration {
        return .{
            .ns = @divFloor((old.ns * 4) + new.ns, 5),
        };
    }
};

pub const RepairBudgetGrid = struct {
    capacity: u32,
    available: u32,
    refill_max: u32,
    requested: std.AutoArrayHashMapUnmanaged(BlockIdentifier, void),

    const BlockIdentifier = struct { address: u64, checksum: u128 };

    pub fn init(gpa: std.mem.Allocator, options: struct {
        capacity: u32,
        refill_max: u32,
    }) !RepairBudgetGrid {
        assert(options.refill_max <= options.capacity);

        var requested: std.AutoArrayHashMapUnmanaged(BlockIdentifier, void) = .{};
        try requested.ensureTotalCapacity(gpa, options.capacity);
        errdefer requested.deinit(gpa);

        return RepairBudgetGrid{
            .capacity = options.capacity,
            .available = options.capacity,
            .refill_max = options.refill_max,
            .requested = requested,
        };
    }

    pub fn deinit(budget: *RepairBudgetGrid, gpa: std.mem.Allocator) void {
        budget.requested.deinit(gpa);
    }

    fn assert_invariants(budget: *RepairBudgetGrid) void {
        assert(budget.available <= budget.capacity);
        assert(budget.available + budget.requested.count() <= budget.capacity);
    }

    pub fn decrement(budget: *RepairBudgetGrid, block_identifier: BlockIdentifier) bool {
        budget.assert_invariants();
        defer budget.assert_invariants();
        assert(budget.available > 0);
        assert(block_identifier.address > 0);

        const gop = budget.requested.getOrPutAssumeCapacity(block_identifier);
        if (gop.found_existing) {
            return false;
        } else {
            budget.available -= 1;
            return true;
        }
    }

    pub fn increment(budget: *RepairBudgetGrid, block_identifier: BlockIdentifier) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        if (budget.requested.swapRemove(block_identifier)) {
            budget.available = @min((budget.available + 1), budget.capacity);
        }
    }

    pub fn refill(budget: *RepairBudgetGrid) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        budget.available = @min((budget.available + budget.refill_max), budget.capacity);
        budget.requested.clearRetainingCapacity();
    }
};
