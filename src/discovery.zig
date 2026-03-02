const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const stdx = @import("stdx");
const IOPSType = stdx.IOPSType;
const Instant = stdx.Instant;
const Duration = stdx.Duration;

const constants = @import("constants.zig");
const log = std.log.scoped(.discovery);

const vsr = @import("vsr.zig");

const Timeout = vsr.Timeout;
const Time = vsr.time.Time;

pub fn DiscoveryType(IO: type) type {
    return struct {
        io: *IO,
        time: Time,
        prng: stdx.PRNG,

        socket: ?IO.socket_t,

        addresses: [constants.discovery_addresses_max]SocketAddress,
        replicas: [constants.members_max]SocketAddress = @splat(.expired),
        clients: [constants.discovery_clients_max]?SocketAddress = @splat(null),

        discover_self: vsr.Header.Discover,
        send_completion: IOPSType(IO.Completion, constants.discovery_concurrency) = .{},
        recv_completion: IO.Completion = undefined,
        recv_buffer: []align(16) u8,
        recv_address: *std.net.Address,
        timeout: Timeout,
        start_on_tick: bool = false,

        pub const discovery_message_size_max = @sizeOf(vsr.Header) +
            (constants.members_max * (@sizeOf(u128) + @sizeOf(u16)));

        pub const freshness_threshold: Duration = .minutes(10);

        const replica_unset: u8 = 255;

        const Discovery = @This();

        pub fn init(gpa: std.mem.Allocator, io: *IO, time: Time, options: struct {
            prng_seed: u64,
            cluster: u128,
            process: union(enum) { replica: u8, client },
            targets: []const std.net.Address,
        }) !Discovery {
            assert(options.targets.len > 0);
            assert(options.targets.len <= constants.discovery_addresses_max);

            var addresses: [constants.discovery_addresses_max]SocketAddress = @splat(.expired);
            for (options.targets, 0..) |target, index| {
                const target_address = try SocketAddress.from_std(target, .pinned);
                addresses[index] = target_address;
            }

            var discover_self: vsr.Header.Discover = .{
                .checksum = undefined,
                .checksum_padding = 0,
                .checksum_body = comptime vsr.checksum(&.{}),
                .checksum_body_padding = 0,
                .cluster = options.cluster,
                .size = @sizeOf(vsr.Header),
                .epoch = 0,
                .view = 0,
                .release = constants.config.process.release,
                .protocol = vsr.Version,
                .command = .discover,
                .replica = switch (options.process) {
                    .replica => |replica| replica,
                    .client => replica_unset,
                },
            };
            discover_self.set_checksum();

            const recv_buffer = try gpa.alignedAlloc(u8, 16, discovery_message_size_max);
            errdefer gpa.free(recv_buffer);

            const recv_address = try gpa.create(std.net.Address);
            errdefer gpa.destroy(recv_address);

            var discovery: Discovery = .{
                .io = io,
                .time = time,
                .prng = .from_seed(options.prng_seed),
                .socket = null,
                .discover_self = discover_self,
                .addresses = addresses,
                .recv_buffer = recv_buffer,
                .recv_address = recv_address,
                .timeout = .{
                    .name = "discovery_timeout",
                    .id = 0,
                    .after = 1_000 / constants.tick_ms,
                },
            };
            errdefer discovery.deinit();

            if (options.process == .client) {
                const family = options.targets[0].any.family;
                discovery.socket = io.open_socket_udp(.{ .family = family }) catch |err| b: {
                    log.err("init: failed to open socket {}", .{err});
                    break :b null;
                };
                discovery.start_on_tick = true;
            }

            return discovery;
        }

        pub fn deinit(discovery: *Discovery, gpa: std.mem.Allocator) void {
            if (discovery.socket) |socket| discovery.io.close_socket(socket);
            gpa.destroy(discovery.recv_address);
            assert(discovery.recv_buffer.len == discovery_message_size_max);
            gpa.free(discovery.recv_buffer);
            discovery.* = undefined;
        }

        pub fn listen(discovery: *Discovery, address: std.net.Address) !void {
            assert(discovery.socket == null);
            discovery.socket = try discovery.io.open_socket_udp(.{ .source = address });
            discovery.recv();
            discovery.timeout.start();
        }

        pub fn tick(discovery: *Discovery) void {
            if (discovery.start_on_tick) {
                discovery.recv();
                discovery.timeout.start();
                discovery.start_on_tick = false;
            }

            discovery.timeout.tick();
            if (discovery.timeout.fired()) {
                discovery.timeout.reset();

                const now = discovery.time.monotonic();
                discovery.broadcast(now);
            }
        }

        pub fn broadcast(discovery: *Discovery, now: Instant) void {
            const fd = discovery.socket orelse return;

            var permutation: [constants.discovery_addresses_max]u8 = undefined;
            for (&permutation, 0..) |*slot, index| slot.* = @intCast(index);
            discovery.prng.shuffle(u8, &permutation);

            for (permutation) |index| {
                const address = discovery.addresses[index];
                if (address.fresh(now, freshness_threshold)) {
                    assert(address.ip != 0);
                    assert(address.port != 0);
                    const completion = discovery.send_completion.acquire() orelse
                        break;
                    discovery.io.send_to(
                        *Discovery,
                        discovery,
                        broadcast_send_callback,
                        completion,
                        fd,
                        std.mem.asBytes(&discovery.discover_self),
                        address.to_std(),
                    );
                }
            }

            const clients = discovery.clients;
            @memset(&discovery.clients, null);

            for (clients) |client| {
                if (client) |address| {
                    const completion = discovery.send_completion.acquire() orelse
                        break;
                    log.info("broadcast client address={}", .{address});
                    discovery.io.send_to(
                        *Discovery,
                        discovery,
                        broadcast_send_callback,
                        completion,
                        fd,
                        std.mem.asBytes(&discovery.discover_self),
                        address.to_std(),
                    );
                }
            }
        }

        fn broadcast_send_callback(
            discovery: *Discovery,
            completion: *IO.Completion,
            result: IO.SendError!usize,
        ) void {
            discovery.send_completion.release(completion);
            const send_size = result catch |err| {
                log.err("send_msg: error={s}", .{@errorName(err)});
                return;
            };
            if (send_size != @sizeOf(vsr.Header)) {
                log.err("send_msg: unexpected size ({}!={})", .{ send_size, @sizeOf(vsr.Header) });
            }
        }

        fn recv(discovery: *Discovery) void {
            if (discovery.socket) |fd| {
                discovery.io.recv_from(
                    *Discovery,
                    discovery,
                    recv_callback,
                    &discovery.recv_completion,
                    fd,
                    discovery.recv_buffer,
                    discovery.recv_address,
                );
            }
        }

        fn recv_callback(
            discovery: *Discovery,
            completion: *IO.Completion,
            size_or_error: IO.RecvError!usize,
        ) void {
            assert(completion == &discovery.recv_completion);
            defer discovery.recv();

            const size = size_or_error catch |err| {
                log.err("recv_callback: err={s}", .{@errorName(err)});
                return;
            };
            assert(size <= discovery.recv_buffer.len);

            const header, const ips, const ports = parse_incoming(discovery.recv_buffer[0..size], .{
                .cluster = discovery.discover_self.cluster,
            }) orelse {
                log.err("invalid header={}", .{
                    @as(*const vsr.Header.Discover, @ptrCast(discovery.recv_buffer.ptr)),
                });
                return;
            };
            assert(ips.len == ports.len);
            assert((ips.len == 0) == (header.size == @sizeOf(vsr.Header)));

            const now = discovery.time.monotonic();

            if (header.replica == discovery.discover_self.replica and
                header.replica != replica_unset)
            {
                // log.err("recv_callback: ignoring (misdirected to self)", .{});
                return;
            }

            if (ips.len == 0) {
                const address = SocketAddress.from_std(
                    discovery.recv_address.*,
                    .{ .learned_at = now },
                ) catch |err| switch (err) {
                    // The family should match the socket, and socket's family checked in init.
                    error.UnsupportedFamily => unreachable,
                };

                if (header.replica == replica_unset) {
                    outer: for (&discovery.clients) |*client| {
                        if (client.*) |address_existing| {
                            if (address_existing.ip == address.ip and
                                address_existing.port == address.port)
                            {
                                break :outer;
                            }
                        } else {
                            client.* = address;
                            break :outer;
                        }
                    }
                } else {
                    assert(header.replica < constants.members_max);
                    if (!discovery.replicas[header.replica].fresh(now, freshness_threshold)) {
                        log.info("recv_callback: learned replica={} address={}", .{
                            header.replica,
                            address,
                        });
                    }
                    discovery.replicas[header.replica] = address;
                }
            } else {
                for (ips, ports) |ip_new, port_new| {
                    if (discovery.find_existing(ip_new, port_new)) |index| {
                        assert(discovery.addresses[index].ip == ip_new);
                        assert(discovery.addresses[index].port == port_new);

                        if (discovery.addresses[index].freshness == .pinned) {
                            // Heard back from pre-configured address, no changes, remains
                            // pre-configured (perpetual).
                        } else {
                            // Heard again from a known address, reset the age.
                            discovery.addresses[index].freshness = .{ .learned_at = now };
                            log.debug("recv_callback: updated discovery address={}", .{
                                discovery.addresses[index],
                            });
                        }
                    } else {
                        const address_new: SocketAddress = .{
                            .ip = ip_new,
                            .port = port_new,
                            .freshness = .{ .learned_at = now },
                        };
                        if (SocketAddress.oldest(&discovery.addresses)) |index| {
                            discovery.addresses[index] = address_new;
                            log.info(
                                "recv_callback: learned discovery address={} index={}",
                                .{ address_new, index },
                            );
                        } else {
                            log.debug("recv_callback: ignoring (all pinned) address={}", .{
                                address_new,
                            });
                        }
                    }
                }
            }
        }

        fn find_existing(discovery: *const Discovery, ip: u128, port: u16) ?usize {
            return for (&discovery.addresses, 0..) |address, index| {
                if (address.ip == ip and address.port == port) break index;
            } else null;
        }

        pub fn parse_incoming(untrusted: []align(16) const u8, options: struct {
            cluster: ?u128,
        }) ?struct { *const vsr.Header, []const u128, []const u16 } {
            // No logs, silence is golden.
            if (untrusted.len > discovery_message_size_max) return null;
            if (untrusted.len < @sizeOf(vsr.Header)) return null;
            assert(untrusted.len >= @sizeOf(vsr.Header));
            assert(untrusted.len <= discovery_message_size_max);

            const body_size = untrusted.len - @sizeOf(vsr.Header);
            if (body_size % (@sizeOf(u128) + @sizeOf(u16)) != 0) return null;

            const header = std.mem.bytesAsValue(
                vsr.Header,
                untrusted[0..@sizeOf(vsr.Header)],
            );
            if (options.cluster) |cluster| {
                if (!stdx.equal_timing_safe(header.cluster, cluster)) {
                    return null;
                }
                assert(header.cluster == cluster);
            }

            if (!header.valid_checksum()) return null;
            if (header.size != untrusted.len) return null;
            if (header.replica != std.math.maxInt(u8) and
                header.replica >= constants.members_max) return null;

            const body = untrusted[@sizeOf(vsr.Header)..];
            assert(body.len == body_size);

            const address_count = @divExact(body.len, @sizeOf(u128) + @sizeOf(u16));

            const addresses: []const u128 = std.mem.bytesAsSlice(
                u128,
                body[0 .. address_count * @sizeOf(u128)],
            );
            assert(addresses.len == address_count);

            const ports: []const u16 = @alignCast(std.mem.bytesAsSlice(
                u16,
                body[address_count * @sizeOf(u128) ..],
            ));
            assert(ports.len == address_count);

            assert(header.size ==
                @sizeOf(vsr.Header) + @sizeOf(u128) * addresses.len + @sizeOf(u16) * ports.len);
            return .{ header, addresses, ports };
        }
    };
}

// NB: Both IP and port are numbers, in native, little endian.
pub const SocketAddress = struct {
    // IPv6 or IPv6-mapped IPv4.
    ip: u128,
    port: u16,
    // FODC 5: Topology doesn't change;
    // Therefore, every socket address must include freshness information
    // to judge if address and port are still valid.
    freshness: Freshness,

    pub const Freshness = union(enum) {
        learned_at: Instant,
        pinned,
    };

    pub const expired: SocketAddress = .{
        .ip = 0,
        .port = 0,
        .freshness = .{ .learned_at = .{ .ns = 0 } },
    };

    const ipv4_prefix: u128 = 0x0000_0000_0000_0000_0000_FFFF;

    pub fn family(address: SocketAddress) enum { IPv4, IPv6 } {
        if ((address.ip >> 32) == ipv4_prefix) return .IPv4;
        return .IPv6;
    }

    pub fn format(
        address: SocketAddress,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        switch (address.family()) {
            .IPv4 => {
                const ipv4: u32 = @truncate(address.ip);
                const octets: [4]u8 = @bitCast(mem.nativeToBig(u32, ipv4));
                try writer.print("{}.{}.{}.{}:{}", .{
                    octets[0], octets[1], octets[2], octets[3], address.port,
                });
            },
            .IPv6 => {
                const quibbles: [8]u16 = @bitCast(mem.nativeToBig(u128, address.ip));
                try writer.writeAll("[");
                for (quibbles, 0..) |quibble, i| {
                    if (i > 0) try writer.writeAll(":");
                    if (quibble == 0) {
                        // Shorthand notation.
                    } else {
                        try writer.print("{x}", .{mem.bigToNative(u16, quibble)});
                    }
                }
                try writer.print("]:{}", .{address.port});
            },
        }
    }

    pub fn to_std(address: SocketAddress) std.net.Address {
        switch (address.family()) {
            .IPv4 => {
                const ipv4: u32 = @truncate(address.ip);
                const octets: [4]u8 = @bitCast(mem.nativeToBig(u32, ipv4));
                return .{ .in = std.net.Ip4Address.init(octets, address.port) };
            },
            .IPv6 => {
                const octets: [16]u8 = @bitCast(mem.nativeToBig(u128, address.ip));
                return .{ .in6 = std.net.Ip6Address.init(octets, address.port, 0, 0) };
            },
        }
    }

    pub fn from_std(
        address: std.net.Address,
        freshness: Freshness,
    ) error{UnsupportedFamily}!SocketAddress {
        const ip, const port = switch (address.any.family) {
            std.posix.AF.INET => b: {
                const ipv4 = mem.bigToNative(u32, address.in.sa.addr);
                const ipv6 = ipv4_prefix << 32 | @as(u128, ipv4);
                const port = mem.bigToNative(u16, address.in.sa.port);
                break :b .{ ipv6, port };
            },
            std.posix.AF.INET6 => b: {
                const ip: u128 = mem.readInt(u128, &address.in6.sa.addr, .big);
                const port = mem.bigToNative(u16, address.in6.sa.port);
                break :b .{ ip, port };
            },
            else => return error.UnsupportedFamily,
        };

        return .{ .ip = ip, .port = port, .freshness = freshness };
    }

    pub fn fresh(address: SocketAddress, now: Instant, threshold: Duration) bool {
        return switch (address.freshness) {
            .learned_at => |past| {
                assert(past.ns <= now.ns);
                return now.duration_since(past).ns < threshold.ns;
            },
            .pinned => true,
        };
    }

    pub fn oldest(addresses: []const SocketAddress) ?usize {
        var index_oldest: ?usize = null;
        for (addresses, 0..) |address, index| {
            switch (address.freshness) {
                .pinned => {},
                .learned_at => |instant| {
                    if (index_oldest == null or
                        instant.ns < addresses[index_oldest.?].freshness.learned_at.ns)
                    {
                        index_oldest = index;
                    }
                },
            }
        }
        return index_oldest;
    }
};
