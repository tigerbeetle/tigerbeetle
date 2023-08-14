const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const log = std.log.scoped(.packet_simulator);
const vsr = @import("../vsr.zig");
const PriorityQueue = std.PriorityQueue;
const fuzz = @import("./fuzz.zig");

pub const PacketSimulatorOptions = struct {
    node_count: u8,
    client_count: u8,
    seed: u64,

    recorded_count_max: u8 = 0,

    /// Mean for the exponential distribution used to calculate forward delay.
    one_way_delay_mean: u64,
    one_way_delay_min: u64,

    packet_loss_probability: u8 = 0,
    packet_replay_probability: u8 = 0,

    /// How the partitions should be generated
    partition_mode: PartitionMode = .none,

    partition_symmetry: PartitionSymmetry = .symmetric,

    /// Probability per tick that a partition will occur
    partition_probability: u8 = 0,

    /// Probability per tick that a partition will resolve
    unpartition_probability: u8 = 0,

    /// Minimum time a partition lasts
    partition_stability: u32 = 0,

    /// Minimum time the cluster is fully connected until it is partitioned again
    unpartition_stability: u32 = 0,

    /// The maximum number of in-flight packets a path can have before packets are randomly dropped.
    path_maximum_capacity: u8,

    /// Mean for the exponential distribution used to calculate how long a path is clogged for.
    path_clog_duration_mean: u64,
    path_clog_probability: u8,
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
        const Self = @This();

        const LinkPacket = struct {
            expiry: u64,
            callback: *const fn (packet: Packet, path: Path) void,
            packet: Packet,
        };

        const Link = struct {
            queue: PriorityQueue(LinkPacket, void, order_packets),
            /// Commands in the set are delivered.
            /// Commands not in the set are dropped.
            filter: LinkFilter = LinkFilter.initFull(),
            /// Commands in the set are recorded for a later replay.
            record: LinkFilter = .{},
            /// We can arbitrary clog a path until a tick.
            clogged_till: u64 = 0,
        };

        const RecordedPacket = struct {
            callback: *const fn (packet: Packet, path: Path) void,
            packet: Packet,
            path: Path,
        };
        const Recorded = std.ArrayListUnmanaged(RecordedPacket);

        options: PacketSimulatorOptions,
        prng: std.rand.DefaultPrng,
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

        pub fn init(allocator: std.mem.Allocator, options: PacketSimulatorOptions) !Self {
            assert(options.node_count > 0);
            assert(options.one_way_delay_mean >= options.one_way_delay_min);

            const process_count_ = options.node_count + options.client_count;
            const links = try allocator.alloc(Link, @as(usize, process_count_) * process_count_);
            errdefer allocator.free(links);

            for (links, 0..) |*link, i| {
                errdefer for (links[0..i]) |l| l.queue.deinit();

                var queue = PriorityQueue(LinkPacket, void, order_packets).init(allocator, {});
                try queue.ensureTotalCapacity(options.path_maximum_capacity);
                link.* = .{ .queue = queue };
            }
            errdefer for (links) |link| link.queue.deinit();

            var recorded = try Recorded.initCapacity(allocator, options.recorded_count_max);
            errdefer recorded.deinit(allocator);

            const auto_partition = try allocator.alloc(bool, @as(usize, options.node_count));
            errdefer allocator.free(auto_partition);
            @memset(auto_partition, false);

            const auto_partition_nodes = try allocator.alloc(u8, @as(usize, options.node_count));
            errdefer allocator.free(auto_partition_nodes);
            for (auto_partition_nodes, 0..) |*node, i| node.* = @as(u8, @intCast(i));

            return Self{
                .options = options,
                .prng = std.rand.DefaultPrng.init(options.seed),
                .links = links,

                .recorded = recorded,

                .auto_partition_active = false,
                .auto_partition = auto_partition,
                .auto_partition_nodes = auto_partition_nodes,
                .auto_partition_stability = options.unpartition_stability,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            for (self.links) |*link| {
                while (link.queue.peek()) |_| link.queue.remove().packet.deinit();
                link.queue.deinit();
            }

            while (self.recorded.popOrNull()) |packet| packet.packet.deinit();
            self.recorded.deinit(allocator);

            allocator.free(self.links);
            allocator.free(self.auto_partition);
            allocator.free(self.auto_partition_nodes);
        }

        pub fn link_filter(self: *Self, path: Path) *LinkFilter {
            return &self.links[self.path_index(path)].filter;
        }

        pub fn link_record(self: *Self, path: Path) *LinkFilter {
            return &self.links[self.path_index(path)].record;
        }

        pub fn replay_recorded(self: *Self) void {
            assert(self.recorded.items.len > 0);

            var recording = false;
            for (self.links) |*link| {
                recording = recording or link.record.bits.count() > 0;
                link.record = .{};
            }
            assert(recording);

            while (self.recorded.popOrNull()) |packet| {
                self.submit_packet(packet.packet, packet.callback, packet.path);
            }
        }

        fn order_packets(context: void, a: LinkPacket, b: LinkPacket) math.Order {
            _ = context;

            return math.order(a.expiry, b.expiry);
        }

        fn process_count(self: Self) usize {
            return self.options.node_count + self.options.client_count;
        }

        fn path_index(self: Self, path: Path) usize {
            assert(path.source < self.process_count());
            assert(path.target < self.process_count());

            return @as(usize, path.source) * self.process_count() + path.target;
        }

        fn should_drop(self: *Self) bool {
            return self.prng.random().uintAtMost(u8, 100) < self.options.packet_loss_probability;
        }

        fn is_clogged(self: *Self, path: Path) bool {
            return self.links[self.path_index(path)].clogged_till > self.ticks;
        }

        fn should_clog(self: *Self, path: Path) bool {
            _ = path;

            return self.prng.random().uintAtMost(u8, 100) < self.options.path_clog_probability;
        }

        fn clog_for(self: *Self, path: Path, ticks: u64) void {
            const clog_expiry = &self.links[self.path_index(path)].clogged_till;
            clog_expiry.* = self.ticks + ticks;
            log.debug("Path path.source={} path.target={} clogged for ticks={}", .{
                path.source,
                path.target,
                ticks,
            });
        }

        fn should_replay(self: *Self) bool {
            return self.prng.random().uintAtMost(u8, 100) < self.options.packet_replay_probability;
        }

        fn should_partition(self: *Self) bool {
            return self.prng.random().uintAtMost(u8, 100) < self.options.partition_probability;
        }

        fn should_unpartition(self: *Self) bool {
            return self.prng.random().uintAtMost(u8, 100) < self.options.unpartition_probability;
        }

        /// Return a value produced using an exponential distribution with
        /// the minimum and mean specified in self.options
        fn one_way_delay(self: *Self) u64 {
            const min = self.options.one_way_delay_min;
            const mean = self.options.one_way_delay_mean;
            return min + fuzz.random_int_exponential(self.prng.random(), u64, mean - min);
        }

        /// Partitions the network. Guaranteed to isolate at least one replica.
        fn auto_partition_network(self: *Self) void {
            assert(self.options.node_count > 1);

            const random = self.prng.random();
            var partition = self.auto_partition;
            switch (self.options.partition_mode) {
                .none => @memset(partition, false),
                .uniform_size => {
                    // Exclude cases partition_size == 0 and partition_size == node_count
                    const partition_size =
                        1 + random.uintAtMost(u8, self.options.node_count - 2);
                    random.shuffle(u8, self.auto_partition_nodes);
                    for (self.auto_partition_nodes, 0..) |r, i| {
                        partition[r] = i < partition_size;
                    }
                },
                .uniform_partition => {
                    var only_same = true;
                    partition[0] = random.uintLessThan(u8, 2) == 1;

                    var i: usize = 1;
                    while (i < self.options.node_count) : (i += 1) {
                        partition[i] = random.uintLessThan(u8, 2) == 1;
                        only_same =
                            only_same and (partition[i - 1] == partition[i]);
                    }

                    if (only_same) {
                        const n = random.uintLessThan(u8, self.options.node_count);
                        partition[n] = true;
                    }
                },
                .isolate_single => {
                    @memset(partition, false);
                    const n = random.uintLessThan(u8, self.options.node_count);
                    partition[n] = true;
                },
            }

            self.auto_partition_active = true;
            self.auto_partition_stability = self.options.partition_stability;

            const asymmetric_partition_side = random.boolean();
            var from: u8 = 0;
            while (from < self.process_count()) : (from += 1) {
                var to: u8 = 0;
                while (to < self.process_count()) : (to += 1) {
                    const path = .{ .source = from, .target = to };
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

        pub fn tick(self: *Self) void {
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

            var from: u8 = 0;
            while (from < self.process_count()) : (from += 1) {
                var to: u8 = 0;
                while (to < self.process_count()) : (to += 1) {
                    const path = .{ .source = from, .target = to };
                    if (self.is_clogged(path)) continue;

                    const queue = &self.links[self.path_index(path)].queue;
                    while (queue.peek()) |*link_packet| {
                        if (link_packet.expiry > self.ticks) break;

                        _ = queue.remove();
                        defer link_packet.packet.deinit();

                        if (!self.links[self.path_index(path)].filter.contains(link_packet.packet.command())) {
                            log.warn("dropped packet (different partitions): from={} to={}", .{ from, to });
                            continue;
                        }

                        if (self.should_drop()) {
                            log.warn("dropped packet from={} to={}", .{ from, to });
                            continue;
                        }

                        if (self.should_replay()) {
                            self.submit_packet(link_packet.packet.clone(), link_packet.callback, path);
                            log.debug("replayed packet from={} to={}", .{ from, to });
                        }

                        log.debug("delivering packet from={} to={}", .{ from, to });
                        link_packet.callback(link_packet.packet, path);
                    }

                    const reverse_path: Path = .{ .source = to, .target = from };

                    if (self.should_clog(reverse_path)) {
                        const ticks = fuzz.random_int_exponential(
                            self.prng.random(),
                            u64,
                            self.options.path_clog_duration_mean,
                        );
                        self.clog_for(reverse_path, ticks);
                    }
                }
            }
        }

        pub fn submit_packet(
            self: *Self,
            packet: Packet, // Callee owned.
            callback: *const fn (packet: Packet, path: Path) void,
            path: Path,
        ) void {
            const queue = &self.links[self.path_index(path)].queue;
            var queue_length = queue.count();
            if (queue_length + 1 > self.options.path_maximum_capacity) {
                const index = self.prng.random().uintLessThanBiased(u64, queue_length);
                const link_packet = queue.removeIndex(index);
                link_packet.packet.deinit();
                log.warn("submit_packet: {} reached capacity, dropped packet={}", .{
                    path,
                    index,
                });
            }

            queue.add(.{
                .expiry = self.ticks + self.one_way_delay(),
                .packet = packet,
                .callback = callback,
            }) catch unreachable;

            const recording = self.links[self.path_index(path)].record.contains(packet.command());
            if (recording) {
                self.recorded.addOneAssumeCapacity().* = .{
                    .packet = packet.clone(),
                    .callback = callback,
                    .path = path,
                };
            }
        }
    };
}
