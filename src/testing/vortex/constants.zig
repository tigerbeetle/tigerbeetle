const constants = @import("../../constants.zig");

pub usingnamespace constants;

pub const cluster_id = 1;
pub const replica_count = 3;
pub const connections_count_max = @divFloor(constants.clients_max, replica_count);

// We allow the cluster to not make progress processing requests for this amount of time. After
// that it's considered a test failure.
pub const liveness_requirement_seconds = 60;

// How many replicas can be faulty while still expecting the cluster to make progress (based on
// 2f+1).
pub const liveness_faulty_replicas_max = @divFloor(replica_count - 1, 2);
