const constants = @import("../../constants.zig");

pub usingnamespace constants;

pub const cluster_id = 1;
pub const replica_count = 3;
pub const connections_count_max = @divFloor(constants.clients_max, replica_count);
