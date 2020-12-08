const std = @import("std");
const log = std.log.scoped(.cluster);

const config = @import("tigerbeetle.conf");

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
            };
            log.info("node {}: address={}", .{
                nodes[index].id,
                nodes[index].address,
            });
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
};
