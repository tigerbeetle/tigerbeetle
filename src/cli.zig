const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const net = std.net;
const os = std.os;

const config = @import("config.zig");
const vr = @import("vr.zig");

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

/// Parse the command line arguments passed to the tigerbeetle binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: *std.mem.Allocator) Args {
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
    const configuration = parse_configuration(allocator, raw_configuration);
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
        print_error_exit("a value of 0 is not permitted for --cluster-id.", .{});
    }
    return cluster;
}

/// Parse and allocate the configuration returning a slice into that array.
fn parse_configuration(allocator: *std.mem.Allocator, raw_configuration: []const u8) []net.Address {
    return vr.parse_configuration(allocator, raw_configuration) catch |err| switch (err) {
        error.AddressHasTrailingComma => {
            print_error_exit("--replica-addresses: invalid trailing comma", .{});
        },
        error.AddressLimitExceeded => {
            print_error_exit("--replica-addresses: too many addresses, at most {d} are allowed", .{
                config.replicas_max,
            });
        },
        error.AddressHasMoreThanOneColon => {
            print_error_exit("--replica-addresses: invalid address with more than one colon", .{});
        },
        error.PortOverflow => print_error_exit("--replica-addresses: a port exceeds 65535", .{}),
        error.PortInvalid => print_error_exit("--replica-addresses: invalid port number", .{}),
        error.AddressInvalid => print_error_exit("--replica-addresses: invalid IPv4 address", .{}),
        error.OutOfMemory => print_error_exit("--replica-addresses: out of memory", .{}),
    };
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
