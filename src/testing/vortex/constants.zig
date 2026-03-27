const std = @import("std");
const constants = @This();

pub const vsr = @import("../../constants.zig");

pub const vortex = struct {
    pub const cluster_id = 0;
    // Maximum number of connections *per replica*.
    // -1 since replicas don't connect to themselves.
    // +1 for the single driver/client.
    pub const connections_count_max = (vsr.replicas_max - 1) + 1;

    // We allow the cluster to not make progress processing requests for this amount of time.
    // After that it's considered a test failure.
    // TODO: This is long for a couple reasons:
    // - CFO currently oversaturate s CPU.
    // - Vortex's liveness check doesn't consider how long replicas have been up. e.g. if you have 3
    //   replicas, and alternate stopping/starting the backups (such that there is always at least
    //   2/3 replicas running) then as far as Supervisor is concerned, that cluster should be making
    //   progress, even if neither replica is up long enough to catch up to the primary.
    pub const liveness_requirement_seconds = 180;
    pub const liveness_requirement_micros = liveness_requirement_seconds * std.time.us_per_s;

    pub const replica_ports_actual = brk: {
        var ports: [constants.vsr.replicas_max]u16 = undefined;
        var replica_num: u16 = 0;
        while (replica_num < constants.vsr.replicas_max) : (replica_num += 1) {
            ports[replica_num] = 4000 + replica_num;
        }
        break :brk ports;
    };
};
