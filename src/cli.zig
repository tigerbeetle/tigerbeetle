const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const os = std.os;

const config = @import("config.zig");
const vr = @import("vr.zig");

const usage = fmt.comptimePrint(
    \\Usage:
    \\  tigerbeetle [-h | --help]
    \\  tigerbeetle init --cluster-id=<hex id> --replica-index=<index>
    \\                   [--directory=<path>]
    \\  tigerbeetle start --cluster-id=<hex id> --replica-index=<index>
    \\                    --replica-addresses=<addresses> [--directory=<path>]
    \\
    \\Commands:
    \\
    \\  init   Create a new .tigerbeetle data file. Requires the --cluster-id and
    \\         --replica-index options. The file will be created in the path set by
    \\         the --directory option if provided. Otherwise it will be created in
    \\         the default {[default_directory]s}.
    \\
    \\  start  Run a tigerbeetle replica as part of the cluster specified by the
    \\         --cluster-id, --replica-addresses, and --replica-index options. This
    \\         requires a preexisting .tigerbeetle data file, either in the default
    \\         {[default_directory]s} or the path set with --directory.
    \\
    \\Options:
    \\
    \\  -h, --help
    \\        Print this help message and exit.
    \\
    \\  --cluster-id=<hex id>
    \\        Set the cluster ID to the provided non-zero 128-bit hexadecimal number.
    \\
    \\  --replica-index=<index>
    \\        Set the address in the array passed to the --replica-addresses option that
    \\        will be used for this replica process. The value of this option is
    \\        interpreted as a zero-based index into the array.
    \\
    \\  --replica-addresses=<addresses>
    \\        Set the addresses of all replicas in the cluster. Accepts a
    \\        comma-separated list of IPv4 addresses with port numbers.
    \\        Either the IPv4 address or port number, but not both, may be
    \\        ommited in which case a default of {[default_address]s} or {[default_port]d}
    \\        will be used.
    \\
    \\  --directory=<path>
    \\        Set the directory used for the .tigerbeetle data files. If this option is
    \\        omitted, the default {[default_directory]s} will be used.
    \\
    \\Examples:
    \\
    \\ tigerbeetle init --cluster-id=1a2b3c --replica-index=0 --directory=/tmp/example_directory
    \\
    \\ tigerbeetle start --cluster-id=1a2b3c --replica-addresses=127.0.0.1:3003,127.0.0.1:3001,127.0.0.1:3002 --replica-index=0
    \\
    \\ tigerbeetle start --cluster-id=1a2b3c --replica-addresses=3003,3001,3002 --replica-index=1
    \\
    \\ tigerbeetle start --cluster-id=1a2b3c --replica-addresses=192.168.0.1,192.168.0.2,192.168.0.3 --replica-index=2
    \\
, .{
    .default_directory = config.directory,
    .default_address = config.address,
    .default_port = config.port,
});

pub const Command = union(enum) {
    init: struct {
        cluster: u128,
        replica: u16,
        dir_fd: os.fd_t,
    },
    start: struct {
        cluster: u128,
        configuration: []net.Address,
        replica: u16,
        dir_fd: os.fd_t,
    },
};

/// Parse the command line arguments passed to the tigerbeetle binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: *std.mem.Allocator) Command {
    var maybe_cluster: ?[]const u8 = null;
    var maybe_configuration: ?[]const u8 = null;
    var maybe_replica: ?[]const u8 = null;
    var maybe_directory: ?[:0]const u8 = null;

    var args = std.process.args();
    // Skip argv[0] which is the name of this executable
    _ = args.nextPosix();

    const raw_command = args.nextPosix() orelse
        fatal("no command provided, expected 'start' or 'init'", .{});
    if (mem.eql(u8, raw_command, "-h") or mem.eql(u8, raw_command, "--help")) {
        std.io.getStdOut().writeAll(usage) catch os.exit(1);
        os.exit(0);
    }
    const command = meta.stringToEnum(meta.Tag(Command), raw_command) orelse
        fatal("unknown command '{s}', expected 'start' or 'init'", .{raw_command});

    while (args.nextPosix()) |arg| {
        if (mem.startsWith(u8, arg, "--cluster-id")) {
            maybe_cluster = parse_flag("--cluster-id", arg);
        } else if (mem.startsWith(u8, arg, "--replica-addresses")) {
            maybe_configuration = parse_flag("--replica-addresses", arg);
        } else if (mem.startsWith(u8, arg, "--replica-index")) {
            maybe_replica = parse_flag("--replica-index", arg);
        } else if (mem.startsWith(u8, arg, "--directory")) {
            maybe_directory = parse_flag("--directory", arg);
        } else if (mem.eql(u8, arg, "-h") or mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        } else {
            fatal("unexpected argument: '{s}'", .{arg});
        }
    }

    const raw_cluster = maybe_cluster orelse
        fatal("required argument: --cluster-id", .{});
    const raw_replica = maybe_replica orelse
        fatal("required argument: --replica-index", .{});

    const cluster = parse_cluster(raw_cluster);
    const replica = parse_replica(raw_replica);

    const dir_path = maybe_directory orelse config.directory;
    const dir_fd = os.openZ(dir_path, os.O_CLOEXEC | os.O_RDONLY, 0) catch |err|
        fatal("failed to open directory '{s}': {}", .{ dir_path, err });

    switch (command) {
        .init => {
            return .{ .init = .{
                .cluster = cluster,
                .replica = replica,
                .dir_fd = dir_fd,
            } };
        },
        .start => {
            const raw_configuration = maybe_configuration orelse
                fatal("required argument: --replica-addresses", .{});
            const configuration = parse_configuration(allocator, raw_configuration);

            if (replica >= configuration.len) {
                fatal(
                    \\--replica-index: value greater than length of address array
                , .{});
            }

            return .{ .start = .{
                .cluster = cluster,
                .configuration = configuration,
                .replica = replica,
                .dir_fd = dir_fd,
            } };
        },
    }
}

/// Format and print an error message followed by the usage string to stderr,
/// then exit with an exit code of 1.
fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n\n" ++ usage, args) catch {};
    os.exit(1);
}

/// Parse e.g. `--cluster=1a2b3c` into `1a2b3c` with error handling.
fn parse_flag(comptime flag: []const u8, arg: [:0]const u8) [:0]const u8 {
    const value = arg[flag.len..];
    if (value.len < 2) {
        fatal("{s} argument requires a value", .{flag});
    }
    if (value[0] != '=') {
        fatal("expected '=' after {s} but found '{c}'", .{ flag, value[0] });
    }
    return value[1..];
}

fn parse_cluster(raw_cluster: []const u8) u128 {
    const cluster = fmt.parseUnsigned(u128, raw_cluster, 16) catch |err| switch (err) {
        error.Overflow => fatal(
            \\--cluster-id: value does not fit into a 128-bit unsigned integer
        , .{}),
        error.InvalidCharacter => fatal(
            \\--cluster-id: value contains an invalid character
        , .{}),
    };
    if (cluster == 0) {
        fatal("--cluster-id: a value of 0 is not permitted", .{});
    }
    return cluster;
}

/// Parse and allocate the configuration returning a slice into that array.
fn parse_configuration(allocator: *std.mem.Allocator, raw_configuration: []const u8) []net.Address {
    return vr.parse_configuration(allocator, raw_configuration) catch |err| switch (err) {
        error.AddressHasTrailingComma => {
            fatal("--replica-addresses: invalid trailing comma", .{});
        },
        error.AddressLimitExceeded => {
            fatal("--replica-addresses: too many addresses, at most {d} are allowed", .{
                config.replicas_max,
            });
        },
        error.AddressHasMoreThanOneColon => {
            fatal("--replica-addresses: invalid address with more than one colon", .{});
        },
        error.PortOverflow => fatal("--replica-addresses: port exceeds 65535", .{}),
        error.PortInvalid => fatal("--replica-addresses: invalid port", .{}),
        error.AddressInvalid => fatal("--replica-addresses: invalid IPv4 address", .{}),
        error.OutOfMemory => fatal("--replica-addresses: out of memory", .{}),
    };
}

fn parse_replica(raw_replica: []const u8) u16 {
    comptime assert(config.replicas_max <= std.math.maxInt(u16));
    const replica = fmt.parseUnsigned(u16, raw_replica, 10) catch |err| switch (err) {
        error.Overflow => fatal(
            \\--replica-index: value greater than length of address array
        , .{}),
        error.InvalidCharacter => fatal(
            \\--replica-index: value contains an invalid character
        , .{}),
    };
    return replica;
}
