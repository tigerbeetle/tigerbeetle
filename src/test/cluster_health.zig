const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log.scoped(.health);

pub const ClusterHealthOptions = struct {
    replica_count: u8,
    /// The minimum number of replicas that must be up.
    //replica_count_min: u8,
    seed: u64,

    /// Probability per tick that a crash will occur.
    crash_probability: f64,
    /// Minimum duration of a crash.
    crash_stability: u32,
    /// Probability per tick that a crashed replica will recovery.
    restart_probability: f64,
    /// Minimum time a replica is up until it is crashed again.
    restart_stability: u32,
};

const ReplicaHealth = union(enum) {
    /// When >0, the replica cannot crash.
    /// When =0, the replica may crash.
    up: u32,
    /// When >0, this is the ticks remaining until recovery is possible.
    /// When =0, the replica may recover.
    down: u32,
};

pub const ClusterHealth = struct {
    options: ClusterHealthOptions,
    prng: std.rand.DefaultPrng,

    /// Health by replica.
    health: []ReplicaHealth,

    pub fn init(allocator: mem.Allocator, options: ClusterHealthOptions) !ClusterHealth {
        assert(options.replica_count > 0);
        assert(options.crash_probability < 1.0);
        assert(options.crash_probability >= 0.0);
        assert(options.restart_probability < 1.0);
        assert(options.restart_probability >= 0.0);

        const health = try allocator.alloc(ReplicaHealth, options.replica_count);
        errdefer allocator.dealloc(health);
        mem.set(ReplicaHealth, health, .{.up = 0});

        return ClusterHealth{
            .options = options,
            .prng = std.rand.DefaultPrng.init(options.seed),
            .health = health,
        };
    }

    pub fn deinit(self: *ClusterHealth, allocator: mem.Allocator) void {
        allocator.free(self.health);
    }

    pub fn tick(self: *ClusterHealth) void {
        for (self.health) |*health, replica| {
            switch (health.*) {
                .up => |*ticks| ticks.* -|= 1,
                .down => |*ticks| {
                    if (ticks.* == 0) {
                        if (!self.sample(self.options.restart_probability)) continue;
                        health.* = .{.up = self.options.restart_stability};
                        log.debug("restart replica={}", .{replica});
                    } else {
                        ticks.* -= 1;
                    }
                },
            }
        }
    }

    /// Returns whether the replica was crashed.
    pub fn maybe_crash_replica(self: *ClusterHealth, replica: u8) bool {
        switch (self.health[replica]) {
            .up => |ticks| {
                // The replica is temporarily protected from crashing again.
                if (ticks > 0) return false;
            },
            .down => unreachable,
        }

        if (!self.sample(self.options.crash_probability)) return false;

        self.health[replica] = .{.down = self.options.crash_stability};
        log.debug("crash replica={}", .{replica});
        return true;
    }

    pub fn up_count(self: *const ClusterHealth) u8 {
        var count: u8 = 0;
        for (self.health) |_, replica| {
            if (self.up(@intCast(u8, replica))) {
                count += 1;
            }
        }
        return count;
    }

    pub fn up(self: *const ClusterHealth, replica: u8) bool {
        switch (self.health[replica]) {
            .up => return true,
            .down => return false,
        }
    }

    // Return true with the given probability.
    fn sample(self: *ClusterHealth, probability: f64) bool {
        assert(probability <= 1.0);
        assert(probability >= 0.0);
        return self.prng.random().float(f64) < probability;
    }
};
