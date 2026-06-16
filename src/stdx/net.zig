//! Physical and logical representation of IP addresses.
//!
//! The primary purpose of `std.net.Address` is to match kernel ABI for Berkeley sockets.
//! It enables communication between the program and the kernel.
//!
//! Here, we instead focus on communication between programs running on different machines.
//! We don't support Unix domain socket, but we do support transferring over the wire in
//! binary format, avoiding stringification.
//!
//! Use uniform representation for both IPv6 and IPv4. As this is "outside view" of an IP address,
//! scope_id and flowinfo are not represented.
const builtin = @import("builtin");
const std = @import("std");
const stdx = @import("./stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const expectError = std.testing.expectError;
const expectEqualStrings = std.testing.expectEqualStrings;

/// An IPv6 or IPv6-mapped IPv4.
pub const IPAddress = extern struct {
    // - Array instead of u128 to avoid endian ambiguity.
    // - Natural alignment to allow re-interpreting as u128.
    big: [16]u8 align(16),

    pub const Family = enum {
        IPv4,
        IPv6,

        pub fn to_std(f: Family) u32 {
            return switch (f) {
                .IPv4 => std.posix.AF.INET,
                .IPv6 => std.posix.AF.INET6,
            };
        }
    };

    const IPv4_prefix: u128 = 0x0000_0000_0000_0000_0000_FFFF_0000_0000;
    const IPv4_prefix_octets: [12]u8 =
        @as([16]u8, @bitCast(std.mem.nativeToBig(u128, IPv4_prefix)))[0..12].*;

    pub const @"127.0.0.1" = parse("127.0.0.1") catch unreachable;

    comptime {
        // The code is endianness-clean, aspirationally. Audit before running on your PowerPC!
        assert(builtin.target.cpu.arch.endian() == .little);

        assert(@sizeOf(IPAddress) == 16);
        assert(@alignOf(IPAddress) == 16);

        for (0..10) |i| assert(IPv4_prefix_octets[i] == 0);
        for (10..12) |i| assert(IPv4_prefix_octets[i] == 0xFF);
        assert(IPv4_prefix_octets.len == 12);
    }

    pub fn family(ip: IPAddress) Family {
        if ((ip.as_u128() >> 32) == (IPv4_prefix >> 32)) return .IPv4;
        return .IPv6;
    }

    pub fn parse(text: []const u8) error{InvalidIPAddress}!IPAddress {
        const v4 = std.mem.indexOfScalar(u8, text, '.') != null;
        return if (v4) parse_v4(text) else parse_v6(text);
    }

    fn parse_v4(text: []const u8) error{InvalidIPAddress}!IPAddress {
        var octets: [4]u8 = undefined;
        var rest = text;
        for (0..octets.len - 1) |index| {
            const octet_text, rest = stdx.cut(rest, ".") orelse return error.InvalidIPAddress;
            octets[index] = stdx.parse_int(u8, octet_text, .{ .base = 10 }) catch
                return error.InvalidIPAddress;
        }
        octets[octets.len - 1] = stdx.parse_int(u8, rest, .{ .base = 10 }) catch
            return error.InvalidIPAddress;

        return IPAddress.from_v4(octets);
    }

    fn parse_v6(text: []const u8) error{InvalidIPAddress}!IPAddress {
        const prefix, const suffix =
            stdx.cut(text, "::") orelse .{ text, null };

        inline for (.{ prefix, suffix orelse "" }) |affix| {
            if (std.mem.endsWith(u8, affix, ":")) return error.InvalidIPAddress;
            if (std.mem.startsWith(u8, affix, ":")) return error.InvalidIPAddress;
        }

        const prefix_count = quibble_count(prefix);
        const suffix_count = quibble_count(suffix orelse "");
        if (prefix_count +| suffix_count > 8) return error.InvalidIPAddress;
        const shorthand_count = 8 - (prefix_count + suffix_count);
        if (suffix == null) {
            if (prefix_count != 8) return error.InvalidIPAddress;
        } else {
            maybe(shorthand_count == 1); // Non-canonical, but valid.
            if (shorthand_count == 0) return error.InvalidIPAddress;
        }

        var quibbles_big: [8]u16 = @splat(0);
        inline for (.{
            .{ prefix, 0, prefix_count },
            .{ suffix orelse "", prefix_count + shorthand_count, suffix_count },
        }) |text_start_index_count| {
            var rest, var index: usize, const count = text_start_index_count;
            if (count > 0) {
                for (0..count - 1) |_| {
                    const quibble_text, rest = stdx.cut(rest, ":").?;
                    const quibble = stdx.parse_int(u16, quibble_text, .{
                        .base = 16,
                        .allow_leading_zero = true,
                    }) catch
                        return error.InvalidIPAddress;
                    quibbles_big[index] = std.mem.nativeToBig(u16, quibble);
                    index += 1;
                }
                const quibble = stdx.parse_int(u16, rest, .{
                    .base = 16,
                    .allow_leading_zero = true,
                }) catch
                    return error.InvalidIPAddress;
                quibbles_big[index] = std.mem.nativeToBig(u16, quibble);
                index += 1;
            }
        }

        return .{ .big = @bitCast(quibbles_big) };
    }

    fn quibble_count(text: []const u8) usize {
        if (text.len == 0) return 0;
        return std.mem.count(u8, text, ":") + 1;
    }

    pub fn format(
        ip: IPAddress,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        comptime assert(fmt.len == 0);
        _ = options;
        switch (ip.family()) {
            .IPv4 => try ip.format_v4(writer),
            .IPv6 => try ip.format_v6(writer),
        }
    }

    fn format_v4(ip: IPAddress, writer: anytype) !void {
        const octets = ip.as_v4().?;
        try writer.print("{}.{}.{}.{}", .{ octets[0], octets[1], octets[2], octets[3] });
    }

    // https://en.wikipedia.org/wiki/IPv6#Address_representation
    fn format_v6(ip: IPAddress, writer: anytype) !void {
        const quibbles_big: [8]u16 = @bitCast(ip.big);

        const run = compressable_run(quibbles_big);

        const prefix_count = if (run) |zeroes| zeroes.start else quibbles_big.len;
        for (0..prefix_count) |index| {
            if (index > 0) try writer.writeAll(":");
            const quibble = std.mem.bigToNative(u16, quibbles_big[index]);
            try writer.print("{x}", .{quibble});
        }
        if (run) |zeroes| {
            try writer.writeAll("::");
            for (zeroes.start..zeroes.start + zeroes.count) |index| {
                assert(quibbles_big[index] == 0);
            }
            for (zeroes.start + zeroes.count..quibbles_big.len) |index| {
                if (index > zeroes.start + zeroes.count) try writer.writeAll(":");
                const quibble = std.mem.bigToNative(u16, quibbles_big[index]);
                try writer.print("{x}", .{quibble});
            }
        }
    }

    const Run = struct { start: usize, count: usize };
    fn compressable_run(quibbles: [8]u16) ?Run {
        var longest: ?Run = null;
        var current: ?Run = null;
        for (0..quibbles.len) |index| {
            if (quibbles[index] == 0) {
                if (current == null) current = .{ .start = index, .count = 0 };
                current.?.count += 1;
                if (longest == null or
                    // The first sequence of zero bits MUST be shortened
                    // https://www.rfc-editor.org/info/rfc5952/#section-4.2.1
                    longest.?.count < current.?.count)
                {
                    longest = current.?;
                }
            } else {
                current = null;
            }
        }
        if (longest == null) return null;
        if (longest.?.count < 2) return null;
        return longest.?;
    }

    fn as_v4(ip: IPAddress) ?[4]u8 {
        if (ip.family() != .IPv4) return null;
        return ip.big[12..].*;
    }
    fn from_v4(big: [4]u8) IPAddress {
        return .{ .big = IPv4_prefix_octets ++ big };
    }

    fn as_u128(ip: IPAddress) u128 {
        return std.mem.bigToNative(u128, @bitCast(ip.big));
    }

    fn arbitrary(prng: *stdx.PRNG) IPAddress {
        var big: [16]u8 = @splat(0);
        if (prng.boolean()) {
            big[10] = 0xFF;
            big[11] = 0xFF;
            prng.fill(big[12..]);
        } else {
            prng.fill(&big);
        }
        return .{ .big = big };
    }
};

test IPAddress {
    const gpa = std.testing.allocator;

    const T = struct {
        fn check(options: struct {
            ok_canonical: []const []const u8,
            ok: []const []const u8,
            err: []const []const u8,
        }) !void {
            for (options.ok_canonical) |case| try check_ok_canonical(case);
            for (options.ok) |case| try check_ok_non_canonical(case);
            for (options.err) |case| try check_err(case);

            // Fix seed run to assert that we touch the boundary.
            var prng = stdx.PRNG.from_seed(92);
            const results = try check_fuzz(&prng, .{
                .corpus = &.{ options.ok_canonical, options.ok, options.err },
                .test_count = 10_000,
            });
            assert(results.ok > 0); //  Positive space covered.
            assert(results.err > 0); // Negative space covered.

            // Actual fuzzing with a true random seed.
            prng = stdx.PRNG.from_seed_testing();
            _ = try check_fuzz(&prng, .{
                .corpus = &.{ options.ok_canonical, options.ok, options.err },
                .test_count = 10_000,
            });
        }

        // An IPv6 address has many valid textual representations, but we print the canonical one.
        // https://www.rfc-editor.org/info/rfc5952/#section-4.2.1
        fn check_ok_canonical(text: []const u8) !void {
            const ip = try IPAddress.parse(text);
            var buffer: [64]u8 = undefined;
            const text_canonical = try std.fmt.bufPrint(&buffer, "{}", .{ip});
            try expectEqualStrings(text, text_canonical);

            try check_ok(text);
        }

        fn check_ok_non_canonical(text: []const u8) !void {
            const ip = try IPAddress.parse(text);
            var buffer: [64]u8 = undefined;
            const text_canonical = try std.fmt.bufPrint(&buffer, "{}", .{ip});
            if (std.mem.eql(u8, text, text_canonical)) {
                std.log.err("{s} is already canonical", .{text});
                return error.TestUnexpectedResult;
            }

            try check_ok(text);
        }

        // For valid addresses, we agree with std, and agree with our own formatting.
        fn check_ok(text: []const u8) !void {
            errdefer std.log.err("text={s}", .{text});

            const ip = try IPAddress.parse(text);
            const address = SocketAddress.to_std(.{ .ip = ip, .port = 0 });
            const address_std = try parse_std(text);
            if (!address.eql(address_std)) {
                std.log.err("{} != {}", .{ address, address_std });
                return error.TestUnexpectedResult;
            }

            var buffer: [64]u8 = undefined;
            const text_roundtrip = try std.fmt.bufPrint(&buffer, "{}", .{ip});
            const ip_roundtrip = try IPAddress.parse(text_roundtrip);
            assert(std.meta.eql(ip, ip_roundtrip));
        }

        // For invalid addresses, both we and std rejects.
        fn check_err(text: []const u8) !void {
            errdefer std.log.err("text={s}", .{text});

            try expectError(error.InvalidIPAddress, IPAddress.parse(text));

            const address_std = parse_std(text) catch return;
            if ((std.mem.startsWith(u8, text, ":") and !std.mem.startsWith(u8, text, "::")) or
                (std.mem.endsWith(u8, text, ":") and !std.mem.endsWith(u8, text, "::")))
            {
                return; //TODO(Zig): 0.14.1 incorrectly parses trailing/leading colons.
            }

            if (stdx.cut(text, "%")) |cut| {
                // Std supports scopes, but we intentionally don't.
                const address = try IPAddress.parse(cut.@"0");
                assert(address.family() == .IPv6);
                return;
            }

            if (stdx.cut_prefix(text, "::ffff:")) |ipv4| {
                // Similarly, don't support explicit IPv6-mapped-IPv4 syntax;
                const address = try IPAddress.parse(ipv4);
                assert(address.family() == .IPv4);
                return;
            }

            std.log.err("incorrectly parsed as {}", .{address_std});
            return error.ExpectedError;
        }

        // - Build alphabet from corpus + random draw.
        // - Compare with std, both negative and positive cases.
        // - Count stats to make sure both are covered.
        fn check_fuzz(prng: *stdx.PRNG, options: struct {
            corpus: []const []const []const u8,
            test_count: u32,
        }) !struct { ok: u32, err: u32 } {
            var corpus: std.ArrayListUnmanaged(u8) = .empty;
            defer corpus.deinit(gpa);

            for (options.corpus) |cases| for (cases) |case| try corpus.appendSlice(gpa, case);
            for (0..5) |_| try corpus.append(gpa, prng.int(u8));

            std.mem.sort(u8, corpus.items, {}, std.sort.asc(u8));

            const alphabet = stdx.unique(corpus.items);

            const text_size_max = 64;
            var buffer: [text_size_max]u8 = undefined;

            var ok: u32 = 0;
            var err: u32 = 0;
            for (0..options.test_count) |_| {
                const text_size = prng.range_inclusive(usize, 0, text_size_max);
                const text = buffer[0..text_size];
                for (text) |*c| c.* = alphabet[prng.index(alphabet)];

                if (IPAddress.parse(text)) |_| {
                    ok += 1;
                    try check_ok(text);
                } else |_| {
                    err += 1;
                    try check_err(text);
                }
            }

            assert(ok + err == options.test_count);
            return .{ .ok = ok, .err = err };
        }

        fn parse_std(text: []const u8) !std.net.Address {
            return try std.net.Address.parseIp(text, 0);
        }
    };

    try T.check(.{
        .ok_canonical = &.{
            "127.0.0.1",
            "0.0.0.0",
            "::",
            "::1",
            "1::",
            "255.255.255.255",
            "2001:db8::1:0:0:1",
            "2001:db8:0:1:1:1:1:1",
            "2001:db8::1",
            "ff01::101",
        },
        .ok = &.{
            "0::",
            "2001:0db8:85a3::8a2e:0370:7334",
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "eebb::2:be8:622:4:b450:52a",
            "2001:DB8:0:0:8:800:200C:417A",
            "FF01:0:0:0:0:0:0:101",
            "0:0:0:0:0:0:0:1",
            "0:0:0:0:0:0:0:0",
            "Ff01::101",
        },
        .err = &.{
            "::d3:",
            ":1d7d::",
            "0.0.0.001",
            "256.0.0.1",
            "127.0.0.1.",
            ".127.0.0.1",
            "127.0.0",
            "",
            ":",
            ":::",
            "::::",
            "b::8%4",
            "::ffff:192.0.2.128",
        },
    });
}

pub const SocketAddress = struct {
    ip: IPAddress,
    port: u16,

    pub fn to_std(socket: SocketAddress) std.net.Address {
        switch (socket.ip.family()) {
            .IPv4 => {
                const octets: [4]u8 = socket.ip.as_v4().?;
                return .{ .in = std.net.Ip4Address.init(octets, socket.port) };
            },
            .IPv6 => {
                // The following two fields are machine-local and can be safely zeroed-out.
                //
                // Flowinfo corresponds to the matching field in the IPv6 header, and is a property
                // of a connection, rather than a part of the address proper. std.net needs it
                // because the kernel API works this way.
                const flowinfo = 0;
                // On a machine with several network interfaces, each network interface might have
                // the _same_ link-local IPv6 address (in addition to a separate, globally routable
                // IPv6 address). Scope-id is another machine-local kernel API, telling the kernel
                // which interface to use.
                const scopeid = 0;

                return .{ .in6 = std.net.Ip6Address.init(
                    socket.ip.big,
                    socket.port,
                    flowinfo,
                    scopeid,
                ) };
            },
        }
    }

    pub fn from_std(address: std.net.Address) error{UnsupportedFamily}!SocketAddress {
        switch (address.any.family) {
            std.posix.AF.INET => {
                const octets_big: [4]u8 = @bitCast(address.in.sa.addr);
                const ip = IPAddress.from_v4(octets_big);
                const port = std.mem.bigToNative(u16, address.in.sa.port);
                return .{ .ip = ip, .port = port };
            },
            std.posix.AF.INET6 => {
                const ip: IPAddress = .{ .big = address.in6.sa.addr };
                const port = std.mem.bigToNative(u16, address.in6.sa.port);
                return .{ .ip = ip, .port = port };
            },
            else => return error.UnsupportedFamily,
        }
    }

    fn arbitrary(prng: *stdx.PRNG) SocketAddress {
        return .{ .ip = IPAddress.arbitrary(prng), .port = prng.int(u16) };
    }
};

test "SocketAddress: from_std bad family" {
    if (builtin.os.tag == .windows) return;
    const unix_domain = try std.net.Address.initUnix("/tmp/socket");
    try expectError(error.UnsupportedFamily, SocketAddress.from_std(unix_domain));
}

test "SocketAddress: fuzz to_std/from_std" {
    var prng = stdx.PRNG.from_seed_testing();
    for (0..1000) |_| {
        const socket = SocketAddress.arbitrary(&prng);
        const address = socket.to_std();
        const socket_roundtrip = try SocketAddress.from_std(address);
        assert(std.meta.eql(socket, socket_roundtrip));

        switch (socket.ip.family()) {
            .IPv4 => assert(address.any.family == std.posix.AF.INET),
            .IPv6 => assert(address.any.family == std.posix.AF.INET6),
        }
    }
}
