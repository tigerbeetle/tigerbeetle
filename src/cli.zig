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
    \\  tigerbeetle format --cluster=<integer> --replica=<index> <path>
    \\
    \\  tigerbeetle start --addresses=<addresses> <path>
    \\
    \\Commands:
    \\
    \\  format  Create a TigerBeetle replica data file at <path>.
    \\          The --cluster and --replica arguments are required.
    \\          Each TigerBeetle replica must have its own data file.
    \\
    \\  start   Run a TigerBeetle replica from the data file at <path>.
    \\
    \\Options:
    \\
    \\  -h, --help
    \\        Print this help message and exit.
    \\
    \\  --cluster=<integer>
    \\        Set the cluster ID to the provided 32-bit unsigned integer.
    \\
    \\  --replica=<index>
    \\        Set the zero-based index that will be used for the replica process.
    \\        The value of this argument will be interpreted as an index into the --addresses array.
    \\
    \\  --addresses=<addresses>
    \\        Set the addresses of all replicas in the cluster.
    \\        Accepts a comma-separated list of IPv4 addresses with port numbers.
    \\        Either the IPv4 address or port number (but not both) may be omitted,
    \\        in which case a default of {[default_address]s} or {[default_port]d}
    \\        will be used.
    \\
    \\Examples:
    \\
    \\  tigerbeetle format --cluster=7 --replica=0 7_0.tigerbeetle
    \\  tigerbeetle format --cluster=7 --replica=1 7_1.tigerbeetle
    \\  tigerbeetle format --cluster=7 --replica=2 7_2.tigerbeetle
    \\
    \\  tigerbeetle start --addresses=127.0.0.1:3003,127.0.0.1:3001,127.0.0.1:3002 7_0.tigerbeetle
    \\  tigerbeetle start --addresses=3003,3001,3002 7_1.tigerbeetle
    \\  tigerbeetle start --addresses=3003,3001,3002 7_2.tigerbeetle
    \\
    \\  tigerbeetle start --addresses=192.168.0.1,192.168.0.2,192.168.0.3 7_0.tigerbeetle
    \\
, .{
    .default_address = config.address,
    .default_port = config.port,
});

pub const Command = union(enum) {
    format: struct {
        cluster: u32,
        replica: u8,
        path: [:0]const u8,
    },
    start: struct {
        addresses: []net.Address,
        memory: u64,
        path: [:0]const u8,
    },
};

/// Parse the command line arguments passed to the `tigerbeetle` binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: std.mem.Allocator) Command {
    var path: ?[:0]const u8 = null;
    var cluster: ?[]const u8 = null;
    var replica: ?[]const u8 = null;
    var addresses: ?[]const u8 = null;
    var memory: ?[]const u8 = null;

    var args = std.process.args();

    // Skip argv[0] which is the name of this executable.
    _ = args.nextPosix();

    const raw_command = args.nextPosix() orelse
        fatal("no command provided, expected 'start' or 'format'", .{});
    if (mem.eql(u8, raw_command, "-h") or mem.eql(u8, raw_command, "--help")) {
        std.io.getStdOut().writeAll(usage) catch os.exit(1);
        os.exit(0);
    }
    const command = meta.stringToEnum(meta.Tag(Command), raw_command) orelse
        fatal("unknown command '{s}', expected 'start' or 'format'", .{raw_command});

    while (args.next(allocator)) |parsed_arg| {
        const arg = try parsed_arg;
        try args_allocated.append(arg);

        if (mem.startsWith(u8, arg, "--cluster")) {
            cluster = parse_flag("--cluster", arg);
        } else if (mem.startsWith(u8, arg, "--replica")) {
            replica = parse_flag("--replica", arg);
        } else if (mem.startsWith(u8, arg, "--addresses")) {
            addresses = parse_flag("--addresses", arg);
        } else if (mem.startsWith(u8, arg, "--memory")) {
            memory = parse_flag("--memory", arg);
        } else if (mem.eql(u8, arg, "-h") or mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        } else if (mem.startsWith(u8, arg, "-")) {
            fatal("unexpected argument: '{s}'", .{arg});
        } else if (path == null) {
            path = arg;
        } else {
            fatal("unexpected argument: '{s}' (must start with '--')", .{arg});
        }
    }

    switch (command) {
        .format => {
            if (addresses != null) fatal("--addresses: supported only by 'start' command", .{});
            if (memory != null) fatal("--memory: supported only by 'start' command", .{});

            return .{
                .format = .{
                    .cluster = parse_cluster(cluster orelse fatal("required: --cluster", .{})),
                    .replica = parse_replica(replica orelse fatal("required: --replica", .{})),
                    .path = path orelse fatal("required: <path>", .{}),
                },
            };
        },
        .start => {
            if (cluster != null) fatal("--cluster: supported only by 'format' command", .{});
            if (replica != null) fatal("--replica: supported only by 'format' command", .{});

            return .{
                .start = .{
                    .addresses = parse_addresses(
                        allocator,
                        addresses orelse fatal("required: --addresses", .{}),
                    ),
                    .memory = if (memory) |m| parse_size(m) else config.memory_size_max_default,
                    .path = path orelse fatal("required: <path>", .{}),
                },
            };
        },
    }
}

/// Format and print an error message followed by the usage string to stderr,
/// then exit with an exit code of 1.
pub fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    os.exit(1);
}

/// Parse e.g. `--cluster=123` into `123` with error handling.
fn parse_flag(comptime flag: []const u8, arg: [:0]const u8) [:0]const u8 {
    const value = arg[flag.len..];
    if (value.len < 2) {
        fatal("{s} argument requires a value", .{flag});
    }
    if (value[0] != '=') {
        fatal("expected '=' after '{s}' but found '{c}'", .{ flag, value[0] });
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

fn parse_size(string: []const u8) u64 {
    var value = mem.trim(u8, string, " ");

    const unit: u64 = blk: {
        if (parse_size_unit(&value, &[_][]const u8{ "TiB", "tib", "TB", "tb" })) {
            break :blk 1024 * 1024 * 1024 * 1024;
        } else if (parse_size_unit(&value, &[_][]const u8{ "GiB", "gib", "GB", "gb" })) {
            break :blk 1024 * 1024 * 1024;
        } else if (parse_size_unit(&value, &[_][]const u8{ "MiB", "mib", "MB", "mb" })) {
            break :blk 1024 * 1024;
        } else if (parse_size_unit(&value, &[_][]const u8{ "KiB", "kib", "KB", "kb" })) {
            break :blk 1024;
        } else {
            break :blk 1;
        }
    };

    const size = fmt.parseUnsigned(u64, value, 10) catch |err| switch (err) {
        error.Overflow => fatal("size value exceeds a 64-bit unsigned integer", .{}),
        error.InvalidCharacter => fatal("size value contains an invalid character", .{}),
    };

    return size * unit;
}

fn parse_size_unit(value: *[]const u8, suffixes: []const []const u8) bool {
    for (suffixes) |suffix| {
        if (mem.endsWith(u8, value.*, suffix)) {
            value.* = mem.trim(u8, value.*[0 .. value.*.len - suffix.len], " ");
            return true;
        }
    }
    return false;
}

test "parse_size" {
    const expectEqual = std.testing.expectEqual;

    const tib = 1024 * 1024 * 1024 * 1024;
    const gib = 1024 * 1024 * 1024;
    const mib = 1024 * 1024;
    const kib = 1024;

    try expectEqual(@as(u64, 0), parse_size("0"));
    try expectEqual(@as(u64, 1), parse_size("  1  "));
    try expectEqual(@as(u64, 140737488355328), parse_size(" 140737488355328 "));
    try expectEqual(@as(u64, 140737488355328), parse_size(" 128TiB "));

    try expectEqual(@as(u64, 1 * tib), parse_size("  1TiB "));
    try expectEqual(@as(u64, 10 * tib), parse_size("  10  tib "));
    try expectEqual(@as(u64, 100 * tib), parse_size("  100  TB "));
    try expectEqual(@as(u64, 1000 * tib), parse_size("  1000  tb "));

    try expectEqual(@as(u64, 1 * gib), parse_size("  1GiB "));
    try expectEqual(@as(u64, 10 * gib), parse_size("  10  gib "));
    try expectEqual(@as(u64, 100 * gib), parse_size("  100  GB "));
    try expectEqual(@as(u64, 1000 * gib), parse_size("  1000  gb "));

    try expectEqual(@as(u64, 1 * mib), parse_size("  1MiB "));
    try expectEqual(@as(u64, 10 * mib), parse_size("  10  mib "));
    try expectEqual(@as(u64, 100 * mib), parse_size("  100  MB "));
    try expectEqual(@as(u64, 1000 * mib), parse_size("  1000  mb "));

    try expectEqual(@as(u64, 1 * kib), parse_size("  1KiB "));
    try expectEqual(@as(u64, 10 * kib), parse_size("  10  kib "));
    try expectEqual(@as(u64, 100 * kib), parse_size("  100  KB "));
    try expectEqual(@as(u64, 1000 * kib), parse_size("  1000  kb "));
}

fn parse_replica(raw_replica: []const u8) u8 {
    comptime assert(config.replicas_max <= std.math.maxInt(u8));
    const replica = fmt.parseUnsigned(u8, raw_replica, 10) catch |err| switch (err) {
        error.Overflow => fatal("--replica: value exceeds an 8-bit unsigned integer", .{}),
        error.InvalidCharacter => fatal("--replica: value contains an invalid character", .{}),
    };
    return replica;
}
