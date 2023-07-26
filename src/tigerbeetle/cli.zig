const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const math = std.math;
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const os = std.os;

const vsr = @import("vsr");
const constants = vsr.constants;
const tigerbeetle = vsr.tigerbeetle;
const IO = vsr.io.IO;
const data_file_size_min = vsr.superblock.data_file_size_min;
const Grid = vsr.lsm.grid.GridType(vsr.storage.Storage);
const StateMachine = vsr.state_machine.StateMachineType(
    vsr.storage.Storage,
    constants.state_machine_config,
);

// TODO Document --cache-accounts, --cache-transfers, --cache-transfers-posted, --limit-storage
const usage = fmt.comptimePrint(
    \\Usage:
    \\
    \\  tigerbeetle [-h | --help]
    \\
    \\  tigerbeetle format --cluster=<integer> --replica=<index> --replica-count=<integer> <path>
    \\
    \\  tigerbeetle start --addresses=<addresses> [--cache-grid=<size><KB|MB|GB>] <path>
    \\
    \\  tigerbeetle version [--version]
    \\
    \\Commands:
    \\
    \\  format   Create a TigerBeetle replica data file at <path>.
    \\           The --cluster and --replica arguments are required.
    \\           Each TigerBeetle replica must have its own data file.
    \\
    \\  start    Run a TigerBeetle replica from the data file at <path>.
    \\
    \\  version  Print the TigerBeetle build version and the compile-time config values.
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
    \\        An index greater than or equal to "replica-count" makes the replica a standby.
    \\        The value of this argument will be interpreted as an index into the --addresses array.
    \\
    \\  --replica-count=<integer>
    \\        Set the number of replicas participating in replication.
    \\
    \\  --addresses=<addresses>
    \\        Set the addresses of all replicas in the cluster.
    \\        Accepts a comma-separated list of IPv4 addresses with port numbers.
    \\        Either the IPv4 address or port number (but not both) may be omitted,
    \\        in which case a default of {[default_address]s} or {[default_port]d}
    \\        will be used.
    \\        "addresses[i]" corresponds to replica "i".
    \\
    \\  --cache-grid=<size><KB|MB|GB>
    \\        Set the grid cache size. The grid cache acts like a page cache for TigerBeetle,
    \\        and should be set as large as possible.
    \\        On a machine running only TigerBeetle, this is somewhere around
    \\        (Total RAM) - 3GB (TigerBeetle) - 1GB (System), eg 12GB for a 16GB machine.
    \\        Defaults to 1GB.
    \\
    \\  --verbose
    \\        Print compile-time configuration along with the build version.
    \\
    \\Examples:
    \\
    \\  tigerbeetle format --cluster=0 --replica=0 --replica-count=3 0_0.tigerbeetle
    \\  tigerbeetle format --cluster=0 --replica=1 --replica-count=3 0_1.tigerbeetle
    \\  tigerbeetle format --cluster=0 --replica=2 --replica-count=3 0_2.tigerbeetle
    \\
    \\  tigerbeetle start --addresses=127.0.0.1:3003,127.0.0.1:3001,127.0.0.1:3002 0_0.tigerbeetle
    \\  tigerbeetle start --addresses=3003,3001,3002 0_1.tigerbeetle
    \\  tigerbeetle start --addresses=3003,3001,3002 0_2.tigerbeetle
    \\
    \\  tigerbeetle start --addresses=192.168.0.1,192.168.0.2,192.168.0.3 0_0.tigerbeetle
    \\
    \\  tigerbeetle version --verbose
    \\
, .{
    .default_address = constants.address,
    .default_port = constants.port,
});

pub const Command = union(enum) {
    pub const Start = struct {
        args_allocated: std.process.ArgIterator,
        addresses: []net.Address,
        cache_accounts: u32,
        cache_transfers: u32,
        cache_transfers_posted: u32,
        storage_size_limit: u64,
        cache_grid_blocks: u32,
        path: [:0]const u8,
    };

    format: struct {
        args_allocated: std.process.ArgIterator,
        cluster: u32,
        replica: u8,
        replica_count: u8,
        path: [:0]const u8,
    },
    start: Start,
    version: struct {
        args_allocated: std.process.ArgIterator,
        verbose: bool,
    },

    pub fn deinit(command: *Command, allocator: std.mem.Allocator) void {
        switch (command.*) {
            .start => |*start| allocator.free(start.addresses),
            else => {},
        }
        switch (command.*) {
            inline else => |*cmd| cmd.args_allocated.deinit(),
        }
        command.* = undefined;
    }
};

/// Parse the command line arguments passed to the `tigerbeetle` binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: std.mem.Allocator) !Command {
    var path: ?[:0]const u8 = null;
    var cluster: ?[]const u8 = null;
    var replica: ?[]const u8 = null;
    var replica_count: ?[]const u8 = null;
    var addresses: ?[]const u8 = null;
    var cache_accounts: ?[]const u8 = null;
    var cache_transfers: ?[]const u8 = null;
    var cache_transfers_posted: ?[]const u8 = null;
    var storage_size_limit: ?[]const u8 = null;
    var cache_grid: ?[]const u8 = null;
    var verbose: ?bool = null;

    // This iterator owns the arguments and is passed to the caller as a part of `Command`.
    var args = try std.process.argsWithAllocator(allocator);
    errdefer args.deinit(allocator);

    // Skip argv[0] which is the name of this executable
    const did_skip = args.skip();
    assert(did_skip);

    const raw_command = args.next() orelse
        fatal("no command provided, expected 'start', 'format', or 'version'", .{});

    if (mem.eql(u8, raw_command, "-h") or mem.eql(u8, raw_command, "--help")) {
        std.io.getStdOut().writeAll(usage) catch os.exit(1);
        os.exit(0);
    }
    const command = meta.stringToEnum(meta.Tag(Command), raw_command) orelse
        fatal("unknown command '{s}', expected 'start', 'format', or 'version'", .{raw_command});

    while (args.next()) |arg| {
        if (mem.startsWith(u8, arg, "--cluster")) {
            if (command != .format) fatal("--cluster: supported only by 'format' command", .{});
            cluster = parse_flag("--cluster", arg);
        } else if (mem.startsWith(u8, arg, "--replica-count")) {
            if (command != .format) fatal("--replica-count: supported only by 'format' command", .{});
            replica_count = parse_flag("--replica-count", arg);
        } else if (mem.startsWith(u8, arg, "--replica")) {
            if (command != .format) fatal("--replica: supported only by 'format' command", .{});
            replica = parse_flag("--replica", arg);
        } else if (mem.startsWith(u8, arg, "--addresses")) {
            if (command != .start) fatal("--addresses: supported only by 'start' command", .{});
            addresses = parse_flag("--addresses", arg);
        } else if (mem.startsWith(u8, arg, "--cache-accounts")) {
            if (command != .start) fatal("--cache-accounts: supported only by 'start' command", .{});
            cache_accounts = parse_flag("--cache-accounts", arg);
        } else if (mem.startsWith(u8, arg, "--cache-transfers")) {
            if (command != .start) fatal("--cache-transfers: supported only by 'start' command", .{});
            cache_transfers = parse_flag("--cache-transfers", arg);
        } else if (mem.startsWith(u8, arg, "--cache-transfers-posted")) {
            if (command != .start) fatal("--cache-transfers-posted: supported only by 'start' command", .{});
            cache_transfers_posted = parse_flag("--cache-transfers-posted", arg);
        } else if (mem.startsWith(u8, arg, "--limit-storage")) {
            if (command != .start) fatal("--limit-storage: supported only by 'start' command", .{});
            storage_size_limit = parse_flag("--limit-storage", arg);
        } else if (mem.startsWith(u8, arg, "--cache-grid")) {
            if (command != .start) fatal("--cache-grid: supported only by 'start' command", .{});
            cache_grid = parse_flag("--cache-grid", arg);
        } else if (mem.eql(u8, arg, "--verbose")) {
            if (command != .version) fatal("--verbose: supported only by 'version' command", .{});
            verbose = true;
        } else if (mem.eql(u8, arg, "-h") or mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        } else if (mem.startsWith(u8, arg, "-")) {
            fatal("unexpected argument: '{s}'", .{arg});
        } else if (path == null) {
            if (!(command == .format or command == .start)) fatal("unexpected path", .{});
            path = arg;
        } else {
            fatal("unexpected argument: '{s}' (must start with '--')", .{arg});
        }
    }

    switch (command) {
        .version => {
            return Command{
                .version = .{
                    .args_allocated = args,
                    .verbose = verbose orelse false,
                },
            };
        },
        .format => {
            const replica_count_parsed = parse_replica_count(
                replica_count orelse fatal("required: --replica-count", .{}),
            );
            return Command{
                .format = .{
                    .args_allocated = args,
                    .cluster = parse_cluster(cluster orelse fatal("required: --cluster", .{})),
                    .replica = parse_replica(
                        replica_count_parsed,
                        replica orelse fatal("required: --replica", .{}),
                    ),
                    .replica_count = replica_count_parsed,
                    .path = path orelse fatal("required: <path>", .{}),
                },
            };
        },
        .start => {
            const groove_config = StateMachine.Forest.groove_config;
            const AccountsValuesCache = groove_config.accounts_mutable.ObjectTree.TableMutable.ValuesCache;
            const TransfersValuesCache = groove_config.transfers.ObjectTree.TableMutable.ValuesCache;
            const PostedValuesCache = groove_config.posted.ObjectTree.TableMutable.ValuesCache;

            return Command{
                .start = .{
                    .args_allocated = args,
                    .addresses = parse_addresses(
                        allocator,
                        addresses orelse fatal("required: --addresses", .{}),
                    ),
                    .cache_accounts = parse_cache_size_to_count(
                        tigerbeetle.Account,
                        AccountsValuesCache,
                        cache_accounts,
                        constants.cache_accounts_size_default,
                    ),
                    .cache_transfers = parse_cache_size_to_count(
                        tigerbeetle.Transfer,
                        TransfersValuesCache,
                        cache_transfers,
                        constants.cache_transfers_size_default,
                    ),
                    .cache_transfers_posted = parse_cache_size_to_count(
                        u256, // TODO(#264): Use actual type here, once exposed.
                        PostedValuesCache,
                        cache_transfers_posted,
                        constants.cache_transfers_posted_size_default,
                    ),
                    .storage_size_limit = parse_storage_size(storage_size_limit),
                    .cache_grid_blocks = parse_cache_size_to_count(
                        [constants.block_size]u8,
                        Grid.Cache,
                        cache_grid,
                        constants.grid_cache_size_default,
                    ),
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
    return vsr.parse_addresses(allocator, raw_addresses, constants.members_max) catch |err| switch (err) {
        error.AddressHasTrailingComma => fatal("--addresses: invalid trailing comma", .{}),
        error.AddressLimitExceeded => {
            fatal("--addresses: too many addresses, at most {d} are allowed", .{
                constants.members_max,
            });
        },
        error.AddressHasMoreThanOneColon => {
            fatal("--addresses: invalid address with more than one colon", .{});
        },
        error.PortOverflow => fatal("--addresses: port exceeds 65535", .{}),
        error.PortInvalid => fatal("--addresses: invalid port", .{}),
        error.AddressInvalid => fatal("--addresses: invalid IPv4 address", .{}),
        error.OutOfMemory => fatal("out of memory", .{}),
    };
}

fn parse_storage_size(size_string: ?[]const u8) u64 {
    const size_min = data_file_size_min;
    const size_max = constants.storage_size_max;
    const size = if (size_string) |s| parse_size(s) else size_max;
    if (size > size_max) fatal("storage size {} exceeds maximum: {}", .{ size, size_max });
    if (size < size_min) fatal("storage size {} is below minimum: {}", .{ size, size_min });
    if (size % constants.sector_size != 0) {
        fatal("size value {} must be a multiple of sector size ({})", .{
            size,
            constants.sector_size,
        });
    }
    return size;
}

/// Given a limit like `10GiB`, a SetAssociativeCache and T return the largest `value_count_max`
/// that can fit in the limit.
fn parse_cache_size_to_count(
    comptime T: type,
    comptime SetAssociativeCache: type,
    size_string: ?[]const u8,
    default_size: u64,
) u32 {
    const target_size = if (size_string) |s| parse_size(s) else default_size;
    const value_count_max_multiple = SetAssociativeCache.value_count_max_multiple;

    const count_limit = @divFloor(target_size, @sizeOf(T));
    const count_rounded = @divFloor(
        count_limit,
        value_count_max_multiple,
    ) * value_count_max_multiple;

    return @intCast(u32, count_rounded);
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

fn parse_replica(replica_count: u8, raw_replica: []const u8) u8 {
    comptime assert(constants.members_max <= std.math.maxInt(u8));
    const replica = fmt.parseUnsigned(u8, raw_replica, 10) catch |err| switch (err) {
        error.Overflow => fatal("--replica: value exceeds an 8-bit unsigned integer", .{}),
        error.InvalidCharacter => fatal("--replica: value contains an invalid character", .{}),
    };
    if (replica >= constants.standbys_max + replica_count) {
        fatal(
            "--replica: value is too large ({}), at most {} is allowed",
            .{ replica, constants.standbys_max + replica_count - 1 },
        );
    }
    return replica;
}

fn parse_replica_count(raw_count: []const u8) u8 {
    comptime assert(constants.replicas_max <= std.math.maxInt(u8));
    const count = fmt.parseUnsigned(u8, raw_count, 10) catch |err| switch (err) {
        error.Overflow => fatal("--replica-count: value exceeds an 8-bit unsigned integer", .{}),
        error.InvalidCharacter => fatal("--replica-count: value contains an invalid character", .{}),
    };
    if (count == 0) {
        fatal("--replica-count: value needs to be greater than zero", .{});
    }
    if (count > constants.replicas_max) {
        fatal(
            "--replica-count: value is too large ({}), at most {} is allowed",
            .{ count, constants.replicas_max },
        );
    }
    return count;
}
