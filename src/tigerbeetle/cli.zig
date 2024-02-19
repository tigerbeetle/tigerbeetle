//! Parse and validate command-line arguments for the tigerbeetle binary.
//!
//! Everything that can be validated without reading the data file must be validated here.
//! Caller must additionally assert validity of arguments as a defense in depth.
//!
//! Some flags are experimental: intentionally undocumented and are not a part of the official
//! surface area. Even experimental features must adhere to the same strict standard of safety,
//! but they come without any performance or usability guarantees.
//!
//! Experimental features are not gated by comptime option for safety: it is much easier to review
//! code for correctness when it is initially added to the main branch, rather when a comptime flag
//! is lifted.

const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const net = std.net;

const vsr = @import("vsr");
const flags = vsr.flags;
const constants = vsr.constants;
const tigerbeetle = vsr.tigerbeetle;
const data_file_size_min = vsr.superblock.data_file_size_min;
const Grid = vsr.GridType(vsr.storage.Storage);
const StateMachine = vsr.state_machine.StateMachineType(
    vsr.storage.Storage,
    constants.state_machine_config,
);

const CliArgs = union(enum) {
    format: struct {
        cluster: u128,
        replica: ?u8 = null,
        // Experimental: standbys don't have a concrete practical use-case yet.
        standby: ?u8 = null,
        replica_count: u8,

        positional: struct {
            path: [:0]const u8,
        },
    },

    start: struct {
        addresses: []const u8,
        limit_storage: flags.ByteSize = .{ .bytes = constants.storage_size_limit_max },
        cache_accounts: flags.ByteSize = .{ .bytes = constants.cache_accounts_size_default },
        cache_transfers: flags.ByteSize = .{ .bytes = constants.cache_transfers_size_default },
        cache_transfers_pending: flags.ByteSize =
            .{ .bytes = constants.cache_transfers_pending_size_default },
        cache_account_balances: flags.ByteSize =
            .{ .bytes = constants.cache_account_balances_size_default },
        cache_grid: flags.ByteSize = .{ .bytes = constants.grid_cache_size_default },
        memory_lsm_manifest: flags.ByteSize =
            .{ .bytes = constants.lsm_manifest_memory_size_default },

        positional: struct {
            path: [:0]const u8,
        },
    },

    version: struct {
        verbose: bool = false,
    },

    repl: struct {
        addresses: []const u8,
        cluster: u128,
        verbose: bool = false,
        command: []const u8 = "",
    },

    // Experimental: the interface is subject to change.
    benchmark: struct {
        cache_accounts: ?[]const u8 = null,
        cache_transfers: ?[]const u8 = null,
        cache_transfers_pending: ?[]const u8 = null,
        cache_account_balances: ?[]const u8 = null,
        cache_grid: ?[]const u8 = null,
        account_count: usize = 10_000,
        account_balances: bool = false,
        transfer_count: usize = 10_000_000,
        transfer_pending: bool = false,
        query_count: usize = 100,
        transfer_count_per_second: usize = 1_000_000,
        print_batch_timings: bool = false,
        id_order: Command.Benchmark.IdOrder = .sequential,
        statsd: bool = false,
        addresses: ?[]const u8 = null,
    },

    // TODO Document --cache-accounts, --cache-transfers, --cache-transfers-posted, --limit-storage
    pub const help = fmt.comptimePrint(
        \\Usage:
        \\
        \\  tigerbeetle [-h | --help]
        \\
        \\  tigerbeetle format --cluster=<integer> --replica=<index> --replica-count=<integer> <path>
        \\
        \\  tigerbeetle start --addresses=<addresses> [--cache-grid=<size><KiB|MiB|GiB>] <path>
        \\
        \\  tigerbeetle version [--verbose]
        \\
        \\  tigerbeetle repl --cluster=<integer> --addresses=<addresses>
        \\
        \\Commands:
        \\
        \\  format     Create a TigerBeetle replica data file at <path>.
        \\             The --cluster and --replica arguments are required.
        \\             Each TigerBeetle replica must have its own data file.
        \\
        \\  start      Run a TigerBeetle replica from the data file at <path>.
        \\
        \\  version    Print the TigerBeetle build version and the compile-time config values.
        \\
        \\  repl       Enter the TigerBeetle client REPL.
        \\
        \\Options:
        \\
        \\  -h, --help
        \\        Print this help message and exit.
        \\
        \\  --cluster=<integer>
        \\        Set the cluster ID to the provided 128-bit unsigned decimal integer.
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
        \\        Accepts a comma-separated list of IPv4/IPv6 addresses with port numbers.
        \\        Either the address or port number (but not both) may be omitted,
        \\        in which case a default of {[default_address]s} or {[default_port]d}
        \\        will be used.
        \\        "addresses[i]" corresponds to replica "i".
        \\
        \\  --cache-grid=<size><KiB|MiB|GiB>
        \\        Set the grid cache size. The grid cache acts like a page cache for TigerBeetle,
        \\        and should be set as large as possible.
        \\        On a machine running only TigerBeetle, this is somewhere around
        \\        (Total RAM) - 3GiB (TigerBeetle) - 1GiB (System), eg 12GiB for a 16GiB machine.
        \\        Defaults to {[default_cache_grid_gb]d}GiB.
        \\
        \\  --memory-lsm-manifest=<size><KiB|MiB|GiB>
        \\        Sets the amount of memory allocated for LSM-tree manifests. When the
        \\        number or size of LSM-trees would become too large for their manifests to fit
        \\        into memory the server will terminate.
        \\        Defaults to {[default_memory_lsm_manifest_mb]d}MiB.
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
        \\  tigerbeetle start --addresses='[::1]:3003,[::1]:3001,[::1]:3002'  0_0.tigerbeetle
        \\
        \\  tigerbeetle version --verbose
        \\
        \\  tigerbeetle repl --addresses=3003,3002,3001 --cluster=0
        \\
    , .{
        .default_address = constants.address,
        .default_port = constants.port,
        .default_cache_grid_gb = @divExact(
            constants.grid_cache_size_default,
            1024 * 1024 * 1024,
        ),
        .default_memory_lsm_manifest_mb = @divExact(
            constants.lsm_manifest_memory_size_default,
            1024 * 1024,
        ),
    });
};

pub const Command = union(enum) {
    pub const Start = struct {
        addresses: []const net.Address,
        // true when the value of `--addresses` is exactly `0`. Used to enable "magic zero" mode for
        // testing. We check the raw string rather then the parsed address to prevent triggering
        // this logic by accident.
        addresses_zero: bool,
        cache_accounts: u32,
        cache_transfers: u32,
        cache_transfers_pending: u32,
        cache_account_balances: u32,
        storage_size_limit: u64,
        cache_grid_blocks: u32,
        lsm_forest_node_count: u32,
        path: [:0]const u8,
    };

    pub const Repl = struct {
        addresses: []const net.Address,
        cluster: u128,
        verbose: bool,
        statements: []const u8,
    };

    pub const Benchmark = struct {
        /// The ID order can affect the results of a benchmark significantly. Specifically,
        /// sequential is expected to be the best (since it can take advantage of various
        /// optimizations such as avoiding negative prefetch) while random/reversed can't.
        pub const IdOrder = enum { sequential, random, reversed };

        cache_accounts: ?[]const u8 = null,
        cache_transfers: ?[]const u8 = null,
        cache_transfers_pending: ?[]const u8 = null,
        cache_account_balances: ?[]const u8 = null,
        cache_grid: ?[]const u8 = null,
        account_count: usize = 10_000,
        account_balances: bool = false,
        transfer_count: usize = 10_000_000,
        transfer_pending: bool = false,
        query_count: usize = 100,
        transfer_count_per_second: usize = 1_000_000,
        print_batch_timings: bool = false,
        id_order: IdOrder = .sequential,
        statsd: bool = false,
        addresses: ?[]const net.Address = null,
    };

    format: struct {
        cluster: u128,
        replica: u8,
        replica_count: u8,
        path: [:0]const u8,
    },
    start: Start,
    version: struct {
        verbose: bool,
    },
    repl: Repl,
    benchmark: Benchmark,

    pub fn deinit(command: *Command, allocator: std.mem.Allocator) void {
        switch (command.*) {
            inline .start, .repl => |*cmd| allocator.free(cmd.addresses),
            .benchmark => |*cmd| {
                if (cmd.addresses) |addresses| allocator.free(addresses);
            },
            else => {},
        }
        command.* = undefined;
    }
};

/// Parse the command line arguments passed to the `tigerbeetle` binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: std.mem.Allocator, args_iterator: *std.process.ArgIterator) !Command {
    const cli_args = flags.parse(args_iterator, CliArgs);

    switch (cli_args) {
        .version => |version| {
            return Command{
                .version = .{
                    .verbose = version.verbose,
                },
            };
        },
        .format => |format| {
            if (format.replica_count == 0) {
                flags.fatal("--replica-count: value needs to be greater than zero", .{});
            }
            if (format.replica_count > constants.replicas_max) {
                flags.fatal("--replica-count: value is too large ({}), at most {} is allowed", .{
                    format.replica_count,
                    constants.replicas_max,
                });
            }

            if (format.replica == null and format.standby == null) {
                flags.fatal("--replica: argument is required", .{});
            }

            if (format.replica != null and format.standby != null) {
                flags.fatal("--standby: conflicts with '--replica'", .{});
            }

            if (format.replica) |replica| {
                if (replica >= format.replica_count) {
                    flags.fatal("--replica: value is too large ({}), at most {} is allowed", .{
                        replica,
                        format.replica_count - 1,
                    });
                }
            }

            if (format.standby) |standby| {
                if (standby < format.replica_count) {
                    flags.fatal("--standby: value is too small ({}), at least {} is required", .{
                        standby,
                        format.replica_count,
                    });
                }
                if (standby >= format.replica_count + constants.standbys_max) {
                    flags.fatal("--standby: value is too large ({}), at most {} is allowed", .{
                        standby,
                        format.replica_count + constants.standbys_max - 1,
                    });
                }
            }

            const replica = (format.replica orelse format.standby).?;
            assert(replica < constants.members_max);
            assert(replica < format.replica_count + constants.standbys_max);

            return Command{
                .format = .{
                    .cluster = format.cluster, // just an ID, any value is allowed
                    .replica = replica,
                    .replica_count = format.replica_count,
                    .path = format.positional.path,
                },
            };
        },
        .start => |start| {
            const groove_config = StateMachine.Forest.groove_config;
            const AccountsValuesCache = groove_config.accounts.ObjectsCache.Cache;
            const TransfersValuesCache = groove_config.transfers.ObjectsCache.Cache;
            const TransfersPendingValuesCache = groove_config.transfers_pending.ObjectsCache.Cache;
            const AccountBalancesValuesCache = groove_config.account_balances.ObjectsCache.Cache;

            const addresses = parse_addresses(allocator, start.addresses);

            const storage_size_limit = start.limit_storage.bytes;
            const storage_size_limit_min = data_file_size_min;
            const storage_size_limit_max = constants.storage_size_limit_max;
            if (storage_size_limit > storage_size_limit_max) {
                flags.fatal("--limit-storage: size {} exceeds maximum: {}", .{
                    storage_size_limit,
                    storage_size_limit_max,
                });
            }
            if (storage_size_limit < storage_size_limit_min) {
                flags.fatal("--limit-storage: size {} is below minimum: {}", .{
                    storage_size_limit,
                    storage_size_limit_min,
                });
            }
            if (storage_size_limit % constants.sector_size != 0) {
                flags.fatal(
                    "--limit-storage: size {} must be a multiple of sector size ({})",
                    .{ storage_size_limit, constants.sector_size },
                );
            }

            const lsm_manifest_memory = start.memory_lsm_manifest.bytes;
            const lsm_manifest_memory_max = constants.lsm_manifest_memory_size_max;
            const lsm_manifest_memory_min = constants.lsm_manifest_memory_size_min;
            const lsm_manifest_memory_multiplier = constants.lsm_manifest_memory_size_multiplier;
            if (lsm_manifest_memory > lsm_manifest_memory_max) {
                flags.fatal("--memory-lsm-manifest: size {} exceeds maximum: {}", .{
                    lsm_manifest_memory,
                    lsm_manifest_memory_max,
                });
            }
            if (lsm_manifest_memory < lsm_manifest_memory_min) {
                flags.fatal("--memory-lsm-manifest: size {} is below minimum: {}", .{
                    lsm_manifest_memory,
                    lsm_manifest_memory_min,
                });
            }
            if (lsm_manifest_memory % lsm_manifest_memory_multiplier != 0) {
                flags.fatal(
                    "--memory-lsm-manifest: size {} must be a multiple of size ({})",
                    .{ lsm_manifest_memory, lsm_manifest_memory_multiplier },
                );
            }

            const lsm_forest_node_count: u32 =
                @intCast(@divExact(lsm_manifest_memory, constants.lsm_manifest_node_size));

            return Command{
                .start = .{
                    .addresses = addresses,
                    .addresses_zero = std.mem.eql(u8, start.addresses, "0"),
                    .storage_size_limit = storage_size_limit,
                    .cache_accounts = parse_cache_size_to_count(
                        tigerbeetle.Account,
                        AccountsValuesCache,
                        start.cache_accounts,
                    ),
                    .cache_transfers = parse_cache_size_to_count(
                        tigerbeetle.Transfer,
                        TransfersValuesCache,
                        start.cache_transfers,
                    ),
                    .cache_transfers_pending = parse_cache_size_to_count(
                        StateMachine.TransferPending,
                        TransfersPendingValuesCache,
                        start.cache_transfers_pending,
                    ),
                    .cache_account_balances = parse_cache_size_to_count(
                        StateMachine.AccountBalancesGrooveValue,
                        AccountBalancesValuesCache,
                        start.cache_account_balances,
                    ),
                    .cache_grid_blocks = parse_cache_size_to_count(
                        [constants.block_size]u8,
                        Grid.Cache,
                        start.cache_grid,
                    ),
                    .lsm_forest_node_count = lsm_forest_node_count,
                    .path = start.positional.path,
                },
            };
        },
        .repl => |repl| {
            const addresses = parse_addresses(allocator, repl.addresses);

            return Command{
                .repl = .{
                    .addresses = addresses,
                    .cluster = repl.cluster,
                    .verbose = repl.verbose,
                    .statements = repl.command,
                },
            };
        },
        .benchmark => |benchmark| {
            const addresses = if (benchmark.addresses) |addresses|
                parse_addresses(allocator, addresses)
            else
                null;

            return Command{
                .benchmark = .{
                    .cache_accounts = benchmark.cache_accounts,
                    .cache_transfers = benchmark.cache_transfers,
                    .cache_transfers_pending = benchmark.cache_transfers_pending,
                    .cache_account_balances = benchmark.cache_account_balances,
                    .cache_grid = benchmark.cache_grid,
                    .account_count = benchmark.account_count,
                    .transfer_count = benchmark.transfer_count,
                    .transfer_pending = benchmark.transfer_pending,
                    .query_count = benchmark.query_count,
                    .transfer_count_per_second = benchmark.transfer_count_per_second,
                    .print_batch_timings = benchmark.print_batch_timings,
                    .id_order = benchmark.id_order,
                    .statsd = benchmark.statsd,
                    .addresses = addresses,
                },
            };
        },
    }
}

/// Parse and allocate the addresses returning a slice into that array.
fn parse_addresses(allocator: std.mem.Allocator, raw_addresses: []const u8) []net.Address {
    return vsr.parse_addresses(
        allocator,
        raw_addresses,
        constants.members_max,
    ) catch |err| switch (err) {
        error.AddressHasTrailingComma => flags.fatal("--addresses: invalid trailing comma", .{}),
        error.AddressLimitExceeded => {
            flags.fatal("--addresses: too many addresses, at most {d} are allowed", .{
                constants.members_max,
            });
        },
        error.AddressHasMoreThanOneColon => {
            flags.fatal("--addresses: invalid address with more than one colon", .{});
        },
        error.PortOverflow => flags.fatal("--addresses: port exceeds 65535", .{}),
        error.PortInvalid => flags.fatal("--addresses: invalid port", .{}),
        error.AddressInvalid => flags.fatal("--addresses: invalid IPv4 or IPv6 address", .{}),
        error.OutOfMemory => flags.fatal("out of memory", .{}),
    };
}

/// Given a limit like `10GiB`, a SetAssociativeCache and T return the largest `value_count_max`
/// that can fit in the limit.
fn parse_cache_size_to_count(
    comptime T: type,
    comptime SetAssociativeCache: type,
    size: flags.ByteSize,
) u32 {
    const value_count_max_multiple = SetAssociativeCache.value_count_max_multiple;

    const count_limit = @divFloor(size.bytes, @sizeOf(T));
    const count_rounded = @divFloor(
        count_limit,
        value_count_max_multiple,
    ) * value_count_max_multiple;

    const result: u32 = @intCast(count_rounded); // TODO: better error message on overflow
    assert(@as(u64, result) * @sizeOf(T) <= size.bytes);

    return result;
}
