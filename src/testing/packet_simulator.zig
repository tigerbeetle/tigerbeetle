const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.packet_simulator);
const vsr = @import("../vsr.zig");
const fuzz = @import("./fuzz.zig");
const stdx = @import("stdx");
const constants = @import("../constants.zig");
const Ratio = stdx.PRNG.Ratio;
const Duration = stdx.Duration;
const Instant = stdx.Instant;

pub const PacketSimulatorOptions = struct {
    node_count: u8,
    client_count: u8,
    seed: u64,

    recorded_count_max: u8 = 0,

    /// Mean for the exponential distribution used to calculate forward delay.
    one_way_delay_mean: Duration,
    one_way_delay_min: Duration,

    packet_loss_probability: Ratio = Ratio.zero(),
    packet_replay_probability: Ratio = Ratio.zero(),

    /// How the partitions should be generated
    partition_mode: PartitionMode = .none,

    partition_symmetry: PartitionSymmetry = .symmetric,

    /// Probability per tick that a partition will occur
    partition_probability: Ratio = Ratio.zero(),

    /// Probability per tick that a partition will resolve
    unpartition_probability: Ratio = Ratio.zero(),

    /// Minimum time a partition lasts
    partition_stability: u32 = 0,

    /// Minimum time the cluster is fully connected until it is partitioned again
    unpartition_stability: u32 = 0,

    /// The maximum number of in-flight packets a path can have before packets are randomly dropped.
    path_maximum_capacity: u8,

    /// Mean for the exponential distribution used to calculate how long a path is clogged for.
    path_clog_duration_mean: Duration,
    path_clog_probability: Ratio,
};

pub const Path = struct {
    source: u8,
    target: u8,
};

pub const LinkFilter = std.enums.EnumSet(vsr.Command);

/// Determines how the partitions are created. Partitions
/// are two-way, i.e. if i cannot communicate with j, then
/// j cannot communicate with i.
///
/// Only nodes (replicas or standbys) are partitioned. There will always be exactly two partitions.
pub const PartitionMode = enum {
    /// Disable automatic partitioning.
    none,

    /// Draws the size of the partition uniformly at random from (1, n-1).
    /// Replicas are randomly assigned a partition.
    uniform_size,

    /// Assigns each node to a partition uniformly at random. This biases towards
    /// equal-size partitions.
    uniform_partition,

    /// Isolates exactly one node.
    isolate_single,
};

pub const PartitionSymmetry = enum { symmetric, asymmetric };

pub fn PacketSimulatorType(comptime Packet: type) type {
    return struct {
        const PacketSimulator = @This();

        const VTable = struct {
            packet_command: *const fn (*PacketSimulator, Packet) vsr.Command,
            packet_clone: *const fn (*PacketSimulator, Packet) Packet,
            packet_deinit: *const fn (*PacketSimulator, Packet) void,
            packet_deliver: *const fn (*PacketSimulator, Packet, Path) void,
            packet_delay: *const fn (*PacketSimulator, Packet, Path) Duration =
                &packet_delay_default,
        };

        const LinkPacket = struct {
            ready_at: Instant,
            packet: Packet,

            fn less_than(_: void, a: LinkPacket, b: LinkPacket) std.math.Order {
                return std.math.order(a.ready_at.ns, b.ready_at.ns);
            }
        };

        pub const LinkDropPacketFn = *const fn (packet: Packet) bool;

        const Link = struct {
            queue: std.PriorityQueue(LinkPacket, void, LinkPacket.less_than),
            /// Commands in the set are delivered.
            /// Commands not in the set are dropped.
            filter: LinkFilter = LinkFilter.initFull(),
            drop_packet_fn: ?LinkDropPacketFn = null,
            /// Commands in the set are recorded for a later replay.
            record: LinkFilter = .{},
            /// We can arbitrary clog a path until a given moment.
            clogged_till: Instant = .{ .ns = 0 },

            fn should_drop(link: *const @This(), packet: Packet, command: vsr.Command) bool {
                if (!link.filter.contains(command)) {
                    return true;
                }
                if (link.drop_packet_fn) |drop_packet_fn| {
                    return drop_packet_fn(packet);
                }
                return false;
            }
        };

        const RecordedPacket = struct {
            packet: Packet,
            path: Path,
        };
        const Recorded = std.ArrayListUnmanaged(RecordedPacket);

        options: PacketSimulatorOptions,
        vtable: VTable,
        prng: stdx.PRNG,
        ticks: u64 = 0,

        /// A send and receive path between each node in the network.
        /// Indexed by path_index().
        links: []Link,

        /// Recorded messages for manual replay in unit-tests.
        recorded: Recorded,

        /// Scratch space for automatically generating partitions.
        /// The "source of truth" for partitions is links[*].filter.
        auto_partition: []bool,
        auto_partition_active: bool,
        auto_partition_nodes: []u8,
        auto_partition_stability: u32,

        pub fn init(
            allocator: std.mem.Allocator,
            options: PacketSimulatorOptions,
            vtable: VTable,
        ) !PacketSimulator {
            assert(options.node_count > 0);
            assert(options.one_way_delay_mean.ns >= options.one_way_delay_min.ns);

            const process_count_ = options.node_count + options.client_count;
            const links = try allocator.alloc(Link, @as(usize, process_count_) * process_count_);
            errdefer allocator.free(links);

            for (links, 0..) |*link, i| {
                errdefer for (links[0..i]) |*l| l.queue.deinit();

                link.* = .{
                    .queue = std.PriorityQueue(LinkPacket, void, LinkPacket.less_than)
                        .init(allocator, {}),
                };
                try link.queue.ensureTotalCapacity(options.path_maximum_capacity);
            }
            errdefer for (links) |*link| link.queue.deinit();

            var recorded = try Recorded.initCapacity(allocator, options.recorded_count_max);
            errdefer recorded.deinit(allocator);

            const auto_partition = try allocator.alloc(bool, @as(usize, options.node_count));
            errdefer allocator.free(auto_partition);
            @memset(auto_partition, false);

            const auto_partition_nodes = try allocator.alloc(u8, @as(usize, options.node_count));
            errdefer allocator.free(auto_partition_nodes);
            for (auto_partition_nodes, 0..) |*node, i| node.* = @intCast(i);

            return PacketSimulator{
                .options = options,
                .vtable = vtable,
                .prng = stdx.PRNG.from_seed(options.seed),
                .links = links,

                .recorded = recorded,

                .auto_partition_active = false,
                .auto_partition = auto_partition,
                .auto_partition_nodes = auto_partition_nodes,
                .auto_partition_stability = options.unpartition_stability,
            };
        }

        pub fn deinit(self: *PacketSimulator, allocator: std.mem.Allocator) void {
            for (self.links) |*link| {
                for (link.queue.items) |link_packet| {
                    self.packet_deinit(link_packet.packet);
                }

                link.queue.deinit();
            }

            while (self.recorded.pop()) |recorded_packet| {
                self.packet_deinit(recorded_packet.packet);
            }
            self.recorded.deinit(allocator);

            allocator.free(self.links);
            allocator.free(self.auto_partition);
            allocator.free(self.auto_partition_nodes);
        }

        /// Drop all pending packets.
        pub fn link_clear(self: *PacketSimulator, path: Path) void {
            const link = &self.links[self.path_index(path)];
            while (link.queue.removeOrNull()) |link_packet| {
                self.packet_deinit(link_packet.packet);
            }
            assert(link.queue.count() == 0);
        }

        pub fn link_filter(self: *PacketSimulator, path: Path) *LinkFilter {
            return &self.links[self.path_index(path)].filter;
        }

        pub fn link_drop_packet_fn(self: *PacketSimulator, path: Path) *?LinkDropPacketFn {
            return &self.links[self.path_index(path)].drop_packet_fn;
        }

        pub fn link_record(self: *PacketSimulator, path: Path) *LinkFilter {
            return &self.links[self.path_index(path)].record;
        }

        pub fn replay_recorded(self: *PacketSimulator) void {
            assert(self.recorded.items.len > 0);

            var recording = false;
            for (self.links) |*link| {
                recording = recording or link.record.bits.count() > 0;
                link.record = .{};
            }
            assert(recording);

            while (self.recorded.pop()) |packet| {
                self.submit_packet(packet.packet, packet.path);
            }
        }

        fn process_count(self: *const PacketSimulator) usize {
            return self.options.node_count + self.options.client_count;
        }

        fn path_index(self: *const PacketSimulator, path: Path) usize {
            assert(path.source < self.process_count());
            assert(path.target < self.process_count());

            return @as(usize, path.source) * self.process_count() + path.target;
        }

        fn should_drop(self: *PacketSimulator) bool {
            return self.prng.chance(self.options.packet_loss_probability);
        }

        fn is_clogged(self: *PacketSimulator, path: Path) bool {
            return self.links[self.path_index(path)].clogged_till.ns > self.tick_instant().ns;
        }

        fn should_clog(self: *PacketSimulator, path: Path) bool {
            _ = path;

            return self.prng.chance(self.options.path_clog_probability);
        }

        fn clog_for(self: *PacketSimulator, path: Path, duration: Duration) void {
            self.links[self.path_index(path)].clogged_till =
                self.tick_instant().add(duration);
            log.debug("Path path.source={} path.target={} clogged for {}", .{
                path.source,
                path.target,
                duration,
            });
        }

        fn should_replay(self: *PacketSimulator) bool {
            return self.prng.chance(self.options.packet_replay_probability);
        }

        fn should_partition(self: *PacketSimulator) bool {
            return self.prng.chance(self.options.partition_probability);
        }

        fn should_unpartition(self: *PacketSimulator) bool {
            return self.prng.chance(self.options.unpartition_probability);
        }

        /// Partitions the network. Guaranteed to isolate at least one replica.
        fn auto_partition_network(self: *PacketSimulator) void {
            assert(self.options.node_count > 1);

            var partition = self.auto_partition;
            switch (self.options.partition_mode) {
                .none => @memset(partition, false),
                .uniform_size => {
                    const partition_size =
                        self.prng.range_inclusive(u8, 1, self.options.node_count - 1);
                    self.prng.shuffle(u8, self.auto_partition_nodes);
                    for (self.auto_partition_nodes, 0..) |r, i| {
                        partition[r] = i < partition_size;
                    }
                },
                .uniform_partition => {
                    var only_same = true;
                    partition[0] = self.prng.boolean();

                    var i: usize = 1;
                    while (i < self.options.node_count) : (i += 1) {
                        partition[i] = self.prng.boolean();
                        only_same =
                            only_same and (partition[i - 1] == partition[i]);
                    }

                    if (only_same) {
                        const n = self.prng.index(partition);
                        partition[n] = true;
                    }
                },
                .isolate_single => {
                    @memset(partition, false);
                    const n = self.prng.index(partition);
                    partition[n] = true;
                },
            }

            self.auto_partition_active = true;
            self.auto_partition_stability = self.options.partition_stability;

            const asymmetric_partition_side = self.prng.boolean();
            var from: u8 = 0;
            while (from < self.process_count()) : (from += 1) {
                var to: u8 = 0;
                while (to < self.process_count()) : (to += 1) {
                    const path: Path = .{ .source = from, .target = to };
                    const enabled =
                        from >= self.options.node_count or
                        to >= self.options.node_count or
                        partition[from] == partition[to] or
                        (self.options.partition_symmetry == .asymmetric and
                            partition[from] == asymmetric_partition_side);
                    self.links[self.path_index(path)].filter =
                        if (enabled) LinkFilter.initFull() else LinkFilter{};
                }
            }
        }

        pub fn step(self: *PacketSimulator) bool {
            var advanced = false;
            for (0..self.process_count()) |from| {
                for (0..self.process_count()) |to| {
                    const path: Path = .{ .source = @intCast(from), .target = @intCast(to) };
                    if (self.is_clogged(path)) continue;

                    const queue = &self.links[self.path_index(path)].queue;
                    if (queue.peek()) |link_packet| {
                        if (link_packet.ready_at.ns <= self.tick_instant().ns) {
                            _ = queue.remove();
                            self.submit_packet_finish(path, link_packet);
                            self.packet_deinit(link_packet.packet);
                            advanced = true;
                        }
                    }
                }
            }
            return advanced;
        }

        pub fn tick(self: *PacketSimulator) void {
            self.ticks += 1;

            if (self.auto_partition_stability > 0) {
                self.auto_partition_stability -= 1;
            } else {
                if (self.auto_partition_active) {
                    if (self.should_unpartition()) {
                        self.auto_partition_active = false;
                        self.auto_partition_stability = self.options.unpartition_stability;
                        @memset(self.auto_partition, false);
                        for (self.links) |*link| link.filter = LinkFilter.initFull();
                        log.warn("unpartitioned network: partition={any}", .{self.auto_partition});
                    }
                } else {
                    if (self.options.node_count > 1 and self.should_partition()) {
                        self.auto_partition_network();
                        log.warn("partitioned network: partition={any}", .{self.auto_partition});
                    }
                }
            }

            for (0..self.process_count()) |from| {
                for (0..self.process_count()) |to| {
                    const path: Path = .{ .source = @intCast(from), .target = @intCast(to) };
                    if (self.should_clog(path)) {
                        self.clog_for(path, .{
                            .ns = fuzz.random_int_exponential(
                                &self.prng,
                                u64,
                                self.options.path_clog_duration_mean.ns,
                            ),
                        });
                    }
                }
            }
        }

        pub fn submit_packet(
            self: *PacketSimulator,
            packet: Packet,
            path: Path,
        ) void {
            const queue = &self.links[self.path_index(path)].queue;
            const queue_count = queue.count();
            if (queue_count + 1 > self.options.path_maximum_capacity) {
                const link_packet = queue.removeIndex(self.prng.index(queue.items));
                defer self.packet_deinit(link_packet.packet);

                log.warn("submit_packet: {} reached capacity, dropped packet: {}", .{
                    path,
                    if (@typeInfo(Packet) == .pointer)
                        link_packet.packet.header
                    else
                        link_packet.packet,
                });
            }

            queue.add(.{
                .ready_at = self.tick_instant().add(self.packet_delay(packet, path)),
                .packet = packet,
            }) catch unreachable;

            const command = self.packet_command(packet);
            const recording = self.links[self.path_index(path)].record.contains(command);
            if (recording) {
                self.recorded.addOneAssumeCapacity().* = .{
                    .packet = self.packet_clone(packet),
                    .path = path,
                };
            }
        }

        fn submit_packet_finish(self: *PacketSimulator, path: Path, link_packet: LinkPacket) void {
            assert(link_packet.ready_at.ns <= self.tick_instant().ns);
            const command = self.packet_command(link_packet.packet);
            if (self.links[self.path_index(path)].should_drop(link_packet.packet, command)) {
                log.warn(
                    "dropped packet (different partitions): from={} to={}: {}",
                    .{
                        path.source,
                        path.target,
                        if (@typeInfo(Packet) == .pointer)
                            link_packet.packet.header
                        else
                            link_packet.packet,
                    },
                );
                return;
            }

            if (self.should_drop()) {
                log.warn("dropped packet from={} to={}: {}", .{
                    path.source,
                    path.target,
                    if (@typeInfo(Packet) == .pointer)
                        link_packet.packet.header
                    else
                        link_packet.packet,
                });
                return;
            }

            if (self.should_replay()) {
                self.submit_packet(
                    self.packet_clone(link_packet.packet),
                    path,
                );
                log.debug("replayed packet from={} to={}", .{ path.source, path.target });
            }

            log.debug("delivering packet from={} to={}", .{ path.source, path.target });
            self.packet_deliver(link_packet.packet, path);
        }

        fn tick_instant(self: *const PacketSimulator) Instant {
            return .{ .ns = self.ticks * constants.tick_ms * std.time.ns_per_ms };
        }

        fn packet_command(self: *PacketSimulator, packet: Packet) vsr.Command {
            return self.vtable.packet_command(self, packet);
        }

        fn packet_clone(self: *PacketSimulator, packet: Packet) Packet {
            return self.vtable.packet_clone(self, packet);
        }

        fn packet_deinit(self: *PacketSimulator, packet: Packet) void {
            self.vtable.packet_deinit(self, packet);
        }

        fn packet_deliver(self: *PacketSimulator, packet: Packet, path: Path) void {
            self.vtable.packet_deliver(self, packet, path);
        }

        fn packet_delay(self: *PacketSimulator, packet: Packet, path: Path) Duration {
            return self.vtable.packet_delay(self, packet, path);
        }

        /// Return a value produced using an exponential distribution with
        /// the minimum and mean specified in self.options
        fn packet_delay_default(self: *PacketSimulator, _: Packet, _: Path) Duration {
            const min = self.options.one_way_delay_min;
            const mean = self.options.one_way_delay_mean;
            return .{
                .ns = @max(min.ns, fuzz.random_int_exponential(&self.prng, u64, mean.ns)),
            };
        }
    };
}
