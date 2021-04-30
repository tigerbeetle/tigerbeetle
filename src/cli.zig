const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const net = std.net;
const os = std.os;

const config = @import("config.zig");

const usage = fmt.comptimePrint(
    \\Usage: tigerbeetle [options]
    \\
    \\ -h, --help
    \\        Print this help message and exit.
    \\
    \\Required Configuration Options:
    \\
    \\ --cluster-id=<hex id>
    \\        Set the cluster ID to the provided non-zero 128-bit hexadecimal number.
    \\
    \\ --replica-addresses=<addresses>
    \\        Set the addresses of all replicas in the cluster. Accepts a
    \\        comma-separated list of IPv4 addresses with port numbers.
    \\        Either the IPv4 address or port number, but not both, may be
    \\        ommited in which case a default of {[default_address]s} or {[default_port]d}
    \\        will be used.
    \\
    \\ --replica-index=<index>
    \\        Set the address in the array passed to the --replica-addresses option that
    \\        will be used for this replica process. The value of this option is
    \\        interpreted as a zero-based index into the array.
    \\
    \\Examples:
    \\
    \\ tigerbeetle --cluster-id=1a2b3c --replica-addresses=127.0.0.1:3003,127.0.0.1:3001,127.0.0.1:3002 --replica-index=0
    \\
    \\ tigerbeetle --cluster-id=1a2b3c --replica-addresses=3003,3001,3002 --replica-index=1
    \\
    \\ tigerbeetle --cluster-id=1a2b3c --replica-addresses=192.168.0.1,192.168.0.2,192.168.0.3 --replica-index=2
    \\
, .{
    .default_address = config.address,
    .default_port = config.port,
});

pub const Args = struct {
    cluster: u128,
    configuration: []net.Address,
    replica: u16,
};

var configuration_storage: [config.replicas_max]net.Address = undefined;

/// Parse the command line arguments passed to tigerbeetle.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args() Args {
    var maybe_cluster: ?[]const u8 = null;
    var maybe_configuration: ?[]const u8 = null;
    var maybe_replica: ?[]const u8 = null;

    var args = std.process.args();
    // Skip argv[0] which is the name of this executable
    _ = args.nextPosix();
    while (args.nextPosix()) |arg| {
        if (mem.startsWith(u8, arg, "--cluster-id")) {
            maybe_cluster = parse_flag("--cluster-id", arg);
        } else if (mem.startsWith(u8, arg, "--replica-addresses")) {
            maybe_configuration = parse_flag("--replica-addresses", arg);
        } else if (mem.startsWith(u8, arg, "--replica-index")) {
            maybe_replica = parse_flag("--replica-index", arg);
        } else if (mem.eql(u8, arg, "-h") or mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        } else {
            print_error_exit("unexpected argument '{s}'", .{arg});
        }
    }

    const raw_cluster = maybe_cluster orelse
        print_error_exit("the --cluster-id option is required", .{});
    const raw_configuration = maybe_configuration orelse
        print_error_exit("the --replica-addresses option is required", .{});
    const raw_replica = maybe_replica orelse
        print_error_exit("the --replica-index option is required", .{});

    const cluster = parse_cluster(raw_cluster);
    const configuration = parse_configuration(raw_configuration);
    const replica = parse_replica(raw_replica, @intCast(u16, configuration.len));

    return .{
        .cluster = cluster,
        .configuration = configuration,
        .replica = replica,
    };
}

/// Format and print an error message followed by the usage string to stderr,
/// then exit with an exit code of 1.
fn print_error_exit(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n\n" ++ usage, args) catch {};
    os.exit(1);
}

/// Parse e.g. `--cluster=1a2b3c` into `1a2b3c` with error handling.
fn parse_flag(comptime flag: []const u8, arg: []const u8) []const u8 {
    const value = arg[flag.len..];
    if (value.len < 2) {
        print_error_exit("{s} requires a value.", .{flag});
    }
    if (value[0] != '=') {
        print_error_exit("expected '=' after {s} but found '{c}'.", .{ flag, value[0] });
    }
    return value[1..];
}

fn parse_cluster(raw_cluster: []const u8) u128 {
    const cluster = fmt.parseUnsigned(u128, raw_cluster, 16) catch |err| switch (err) {
        error.Overflow => print_error_exit(
            \\value provided to --cluster-id does not fit in a 128bit unsigned integer.
        , .{}),
        error.InvalidCharacter => print_error_exit(
            \\value provided to --cluster-id contains an invalid character.
        , .{}),
    };
    if (cluster == 0) {
        print_error_exit("a value of 0 is not permitted for --cluster-id", .{});
    }
    return cluster;
}

/// Parse and store the configuration in the global configuration_storage array,
/// returning a slice into that array.
fn parse_configuration(raw_configuration: []const u8) []net.Address {
    var comma_it = mem.split(raw_configuration, ",");
    var i: usize = 0;
    while (comma_it.next()) |entry| : (i += 1) {
        if (entry.len == 0) {
            print_error_exit("--replica-addresses must not have a trailing comma", .{});
        }
        if (i == config.replicas_max) {
            print_error_exit("max {d} addresses are allowed for --replica-addresses", .{
                config.replicas_max,
            });
        }
        var colon_it = mem.split(entry, ":");
        // the split iterator will always return non-null once even if the delimiter is not found.
        const raw_ipv4 = colon_it.next().?;
        if (colon_it.next()) |raw_port| {
            if (colon_it.next() != null) {
                print_error_exit("more than one colon in address array entry '{s}'", .{entry});
            }
            const port = fmt.parseUnsigned(u16, raw_port, 10) catch |err| switch (err) {
                error.Overflow => {
                    print_error_exit("'{s}' is greater than the maximum port number (65535).", .{
                        raw_port,
                    });
                },
                error.InvalidCharacter => {
                    print_error_exit("invalid character in port '{s}'.", .{raw_port});
                },
            };
            configuration_storage[i] = net.Address.parseIp4(raw_ipv4, port) catch {
                print_error_exit("'{s}' is not a valid IPv4 address.", .{raw_ipv4});
            };
        } else {
            // There was no colon in entry so there are now two cases:
            // 1. an IPv4 address with the default port
            // 2. a port with the default IPv4 address.

            // Try to parse as a port first
            if (fmt.parseUnsigned(u16, entry, 10)) |port| {
                configuration_storage[i] = net.Address.parseIp4(
                    config.address,
                    port,
                ) catch unreachable;
            } else |err| switch (err) {
                error.Overflow => {
                    print_error_exit("'{s}' is greater than the maximum port number (65535).", .{
                        entry,
                    });
                },
                error.InvalidCharacter => {
                    // Found something that was not a digit, try parsing as an IPv4 instead.
                    configuration_storage[i] = net.Address.parseIp4(entry, config.port) catch {
                        print_error_exit("'{s}' is not a valid IPv4 address.", .{entry});
                    };
                },
            }
        }
    }
    return configuration_storage[0..i];
}

fn parse_replica(raw_replica: []const u8, configuration_len: u16) u16 {
    comptime assert(config.replicas_max <= std.math.maxInt(u16));
    const replica = fmt.parseUnsigned(u16, raw_replica, 10) catch |err| switch (err) {
        error.Overflow => print_error_exit(
            \\value provided to --replica-index greater than length of address array.
        , .{}),
        error.InvalidCharacter => print_error_exit(
            \\value provided to --replica-index contains an invalid character.
        , .{}),
    };
    if (replica >= configuration_len) {
        print_error_exit(
            \\value provided to --replica-index greater than length of address array.
        , .{});
    }
    return replica;
}
