const std = @import("std");
const constants = @This();

pub const vsr = @import("../../constants.zig");

pub const vortex = struct {
    pub const cluster_id = 0;
    pub const connections_count_max = @divFloor(
        constants.vsr.clients_max,
        constants.vsr.replicas_max,
    );

    // We allow the cluster to not make progress processing requests for this amount of time.
    // After that it's considered a test failure.
    pub const liveness_requirement_seconds = 120;
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
