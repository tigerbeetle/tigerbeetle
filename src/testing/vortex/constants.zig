const std = @import("std");
const constants = @import("../../constants.zig");

pub usingnamespace constants;

pub const cluster_id = 1;
pub const connections_count_max = @divFloor(constants.clients_max, constants.replicas_max);

// We allow the cluster to not make progress processing requests for this amount of time. After
// that it's considered a test failure.
pub const liveness_requirement_seconds = 120;
pub const liveness_requirement_micros = liveness_requirement_seconds * std.time.us_per_s;

pub const replica_ports_actual = brk: {
    var ports: [constants.replicas_max]u16 = undefined;
    var replica_num: u16 = 0;
    while (replica_num < constants.replicas_max) : (replica_num += 1) {
        ports[replica_num] = 4000 + replica_num;
    }
    break :brk ports;
};

pub const replica_ports_proxied = brk: {
    var ports: [constants.replicas_max]u16 = undefined;
    var replica_num: u16 = 0;
    while (replica_num < constants.replicas_max) : (replica_num += 1) {
        ports[replica_num] = 3000 + replica_num;
    }
    break :brk ports;
};
