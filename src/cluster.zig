const std = @import("std");
const log = std.log.scoped(.cluster);
const config = @import("tigerbeetle.conf");

const Connection = @import("connections.zig").Connection;

pub const Cluster = struct {
    id: u128,
    nodes: [config.nodes.len]ClusterNode,

    pub fn init() !Cluster {
        var nodes: [config.nodes.len]ClusterNode = undefined;
        log.info("cluster_id={}", .{config.cluster_id});
        inline for (config.nodes) |node, index| {
            nodes[index] = .{
                .id = node.id,
                .address = try std.net.Address.parseIp4(node.ip, node.port),
                .connecting = false,
                .connection = null,
                .connection_errors = 0,
            };
            log.info("node {}: address={}", .{ nodes[index].id, nodes[index].address });
        }
        return Cluster{
            .id = config.cluster_id,
            .nodes = nodes,
        };
    }

    pub fn deinit(self: *Cluster) void {}
};

pub const ClusterNode = struct {
    id: u128,
    address: std.net.Address,
    connecting: bool,
    connection: ?*Connection,
    connection_errors: u4,

    pub fn increment_connection_errors(self: *ClusterNode) void {
        // Saturate the counter at its maximum value if the addition wraps:
        self.connection_errors +%= 1;
        if (self.connection_errors == 0) self.connection_errors -%= 1;
    }
};
