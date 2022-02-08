const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const os = std.os;

const config = @import("config.zig");
const vsr = @import("vsr.zig");
const IO = @import("io.zig").IO;

const usage = fmt.comptimePrint(
    \\Usage:
    \\
    \\  tigerbeetle [-h | --help]
    \\
    \\  tigerbeetle init  [--directory=<path>] --cluster=<integer> --replica=<index>
    \\
    \\  tigerbeetle start [--directory=<path>] --cluster=<integer> --replica=<index> --addresses=<addresses>
    \\
    \\Commands:
    \\
    \\  init   Create a new .tigerbeetle data file. Requires the --cluster and
    \\         --replica options. The file will be created in the path set by
    \\         the --directory option if provided. Otherwise, it will be created in
    \\         the default {[default_directory]s}.
    \\
    \\  start  Run a TigerBeetle replica as part of the cluster specified by the
    \\         --cluster, --replica, and --addresses options. This requires an
    \\         existing .tigerbeetle data file, either in the default
    \\         {[default_directory]s} or the path set with --directory.
    \\
    \\Options:
    \\
    \\  -h, --help
    \\        Print this help message and exit.
    \\
    \\  --directory=<path>
    \\        Set the directory used to store .tigerbeetle data files. If this option is
    \\        omitted, the default {[default_directory]s} will be used.
    \\
    \\  --cluster=<integer>
    \\        Set the cluster ID to the provided 32-bit unsigned integer.
    \\
    \\  --replica=<index>
    \\        Set the zero-based index that will be used for this replica process.
    \\        The value of this option will be interpreted as an index into the --addresses array.
    \\
    \\  --addresses=<addresses>
    \\        Set the addresses of all replicas in the cluster. Accepts a
    \\        comma-separated list of IPv4 addresses with port numbers.
    \\        Either the IPv4 address or port number, but not both, may be
    \\        ommited in which case a default of {[default_address]s} or {[default_port]d}
    \\        will be used.
    \\
    \\Examples:
    \\
    \\  tigerbeetle init --cluster=0 --replica=0 --directory=/var/lib/tigerbeetle
    \\  tigerbeetle init --cluster=0 --replica=1 --directory=/var/lib/tigerbeetle
    \\  tigerbeetle init --cluster=0 --replica=2 --directory=/var/lib/tigerbeetle
    \\
    \\  tigerbeetle start --cluster=0 --replica=0 --addresses=127.0.0.1:3003,127.0.0.1:3001,127.0.0.1:3002
    \\  tigerbeetle start --cluster=0 --replica=1 --addresses=3003,3001,3002
    \\  tigerbeetle start --cluster=0 --replica=2 --addresses=3003,3001,3002
    \\
    \\  tigerbeetle start --cluster=1 --replica=0 --addresses=192.168.0.1,192.168.0.2,192.168.0.3
    \\
, .{
    .default_directory = config.directory,
    .default_address = config.address,
    .default_port = config.port,
});

pub const Command = union(enum) {
    init: struct {
        cluster: u32,
        replica: u8,
        dir_fd: os.fd_t,
    },
    start: struct {
        cluster: u32,
        replica: u8,
        addresses: []net.Address,
        dir_fd: os.fd_t,
    },
};

/// Parse the command line arguments passed to the tigerbeetle binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: std.mem.Allocator) !Command {
    var maybe_cluster: ?[]const u8 = null;
    var maybe_replica: ?[]const u8 = null;
    var maybe_addresses: ?[]const u8 = null;
    var maybe_directory: ?[:0]const u8 = null;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip argv[0] which is the name of this executable
    const did_skip = args.skip();
    assert(did_skip);

    const raw_command = try (args.next(allocator) orelse
        fatal("no command provided, expected 'start' or 'init'", .{}));
    defer allocator.free(raw_command);

    if (mem.eql(u8, raw_command, "-h") or mem.eql(u8, raw_command, "--help")) {
        std.io.getStdOut().writeAll(usage) catch os.exit(1);
        os.exit(0);
    }
    const command = meta.stringToEnum(meta.Tag(Command), raw_command) orelse
        fatal("unknown command '{s}', expected 'start' or 'init'", .{raw_command});

    while (args.next(allocator)) |parsed_arg| {
        const arg = try parsed_arg;
        defer allocator.free(arg);

        if (mem.startsWith(u8, arg, "--cluster")) {
            maybe_cluster = parse_flag("--cluster", arg);
        } else if (mem.startsWith(u8, arg, "--replica")) {
            maybe_replica = parse_flag("--replica", arg);
        } else if (mem.startsWith(u8, arg, "--addresses")) {
            maybe_addresses = parse_flag("--addresses", arg);
        } else if (mem.startsWith(u8, arg, "--directory")) {
            maybe_directory = parse_flag("--directory", arg);
        } else if (mem.eql(u8, arg, "-h") or mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        } else if (mem.startsWith(u8, arg, "--")) {
            fatal("unexpected argument: '{s}'", .{arg});
        } else {
            fatal("unexpected argument: '{s}' (must start with '--')", .{arg});
        }
    }

    const raw_cluster = maybe_cluster orelse fatal("required argument: --cluster", .{});
    const raw_replica = maybe_replica orelse fatal("required argument: --replica", .{});

    const cluster = parse_cluster(raw_cluster);
    const replica = parse_replica(raw_replica);

    const dir_path = maybe_directory orelse config.directory;
    const dir_fd = IO.open_dir(dir_path) catch |err|
        fatal("failed to open directory '{s}': {}", .{ dir_path, err });

    switch (command) {
        .init => {
            if (maybe_addresses != null) {
                fatal("--addresses: supported only by 'start' command", .{});
            }

            return Command{
                .init = .{
                    .cluster = cluster,
                    .replica = replica,
                    .dir_fd = dir_fd,
                },
            };
        },
        .start => {
            const raw_addresses = maybe_addresses orelse
                fatal("required argument: --addresses", .{});
            const addresses = parse_addresses(allocator, raw_addresses);

            if (replica >= addresses.len) {
                fatal("--replica: value greater than length of --addresses array", .{});
            }

            return Command{
                .start = .{
                    .cluster = cluster,
                    .replica = replica,
                    .addresses = addresses,
                    .dir_fd = dir_fd,
                },
            };
        },
    }
}

/// Format and print an error message followed by the usage string to stderr,
/// then exit with an exit code of 1.
fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
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

fn parse_cluster(raw_cluster: []const u8) u32 {
    const cluster = fmt.parseUnsigned(u32, raw_cluster, 10) catch |err| switch (err) {
        error.Overflow => fatal("--cluster: value exceeds a 32-bit unsigned integer", .{}),
        error.InvalidCharacter => fatal("--cluster: value contains an invalid character", .{}),
    };
    return cluster;
}

/// Parse and allocate the addresses returning a slice into that array.
fn parse_addresses(allocator: std.mem.Allocator, raw_addresses: []const u8) []net.Address {
    return vsr.parse_addresses(allocator, raw_addresses) catch |err| switch (err) {
        error.AddressHasTrailingComma => fatal("--addresses: invalid trailing comma", .{}),
        error.AddressLimitExceeded => {
            fatal("--addresses: too many addresses, at most {d} are allowed", .{
                config.replicas_max,
            });
        },
        error.AddressHasMoreThanOneColon => {
            fatal("--addresses: invalid address with more than one colon", .{});
        },
        error.PortOverflow => fatal("--addresses: port exceeds 65535", .{}),
        error.PortInvalid => fatal("--addresses: invalid port", .{}),
        error.AddressInvalid => fatal("--addresses: invalid IPv4 address", .{}),
        error.OutOfMemory => fatal("--addresses: out of memory", .{}),
    };
}

fn parse_replica(raw_replica: []const u8) u8 {
    comptime assert(config.replicas_max <= std.math.maxInt(u8));
    const replica = fmt.parseUnsigned(u8, raw_replica, 10) catch |err| switch (err) {
        error.Overflow => fatal("--replica: value exceeds an 8-bit unsigned integer", .{}),
        error.InvalidCharacter => fatal("--replica: value contains an invalid character", .{}),
    };
    return replica;
}
