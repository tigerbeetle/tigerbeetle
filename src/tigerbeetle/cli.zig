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
        development: bool = false,

        positional: struct {
            path: [:0]const u8,
        },
    },

    start: struct {
        // Stable CLI arguments.
        addresses: []const u8,
        cache_grid: ?flags.ByteSize = null,
        development: bool = false,
        positional: struct {
            path: [:0]const u8,
        },

        // Everything below here is considered experimental, and requires `--experimental` to be
        // set. Experimental flags disable automatic upgrades with multiversion binaries; each
        // replica has to be manually restarted.
        // Experimental flags must default to null.
        experimental: bool = false,

        limit_storage: ?flags.ByteSize = null,
        limit_pipeline_requests: ?u32 = null,
        limit_request: ?flags.ByteSize = null,
        cache_accounts: ?flags.ByteSize = null,
        cache_transfers: ?flags.ByteSize = null,
        cache_transfers_pending: ?flags.ByteSize = null,
        cache_account_balances: ?flags.ByteSize = null,
        memory_lsm_manifest: ?flags.ByteSize = null,
        memory_lsm_compaction: ?flags.ByteSize = null,
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
        account_batch_size: usize = @divExact(
            constants.message_size_max - @sizeOf(vsr.Header),
            @sizeOf(tigerbeetle.Account),
        ),
        transfer_count: usize = 10_000_000,
        transfer_pending: bool = false,
        transfer_batch_size: usize = @divExact(
            constants.message_size_max - @sizeOf(vsr.Header),
            @sizeOf(tigerbeetle.Transfer),
        ),
        transfer_batch_delay_us: usize = 0,
        query_count: usize = 100,
        print_batch_timings: bool = false,
        id_order: Command.Benchmark.IdOrder = .sequential,
        statsd: bool = false,
        addresses: ?[]const u8 = null,
        seed: ?u64 = null,
    },

    // TODO Document --cache-accounts, --cache-transfers, --cache-transfers-posted, --limit-storage,
    // --limit-pipeline-requests
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
        \\  --verbose
        \\        Print compile-time configuration along with the build version.
        \\
        \\  --development
        \\        Allow the replica to format/start even when Direct IO is unavailable.
        \\        Additionally, use smaller cache sizes and batch size by default.
        \\
        \\        Since this shrinks the batch size, note that:
        \\        * All replicas should use the same batch size. That is, if any replica in the cluster has
        \\          "--development", then all replicas should have "--development".
        \\        * It is always possible to increase the batch size by restarting without "--development".
        \\        * Shrinking the batch size of an existing cluster is possible, but not recommended.
        \\
        \\        For safety, production replicas should always enforce Direct IO -- this flag should only be
        \\        used for testing and development. It should not be used for production or benchmarks.
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
    });
};

const StartDefaults = struct {
    limit_pipeline_requests: u32,
    limit_request: flags.ByteSize,
    cache_accounts: flags.ByteSize,
    cache_transfers: flags.ByteSize,
    cache_transfers_pending: flags.ByteSize,
    cache_account_balances: flags.ByteSize,
    cache_grid: flags.ByteSize,
    memory_lsm_compaction: flags.ByteSize,
};

const start_defaults_production = StartDefaults{
    .limit_pipeline_requests = vsr.stdx.div_ceil(constants.clients_max, 2) -
        constants.pipeline_prepare_queue_max,
    .limit_request = .{ .value = constants.message_size_max },
    .cache_accounts = .{ .value = constants.cache_accounts_size_default },
    .cache_transfers = .{ .value = constants.cache_transfers_size_default },
    .cache_transfers_pending = .{ .value = constants.cache_transfers_pending_size_default },
    .cache_account_balances = .{ .value = constants.cache_account_balances_size_default },
    .cache_grid = .{ .value = constants.grid_cache_size_default },
    .memory_lsm_compaction = .{
        // By default, add a few extra blocks for beat-scoped work.
        .value = (lsm_compaction_block_count_min + 16) * constants.block_size,
    },
};

const start_defaults_development = StartDefaults{
    .limit_pipeline_requests = 0,
    .limit_request = .{ .value = 32 * 1024 }, // 32KiB
    .cache_accounts = .{ .value = 0 },
    .cache_transfers = .{ .value = 0 },
    .cache_transfers_pending = .{ .value = 0 },
    .cache_account_balances = .{ .value = 0 },
    .cache_grid = .{ .value = constants.block_size * Grid.Cache.value_count_max_multiple },
    .memory_lsm_compaction = .{ .value = lsm_compaction_block_memory_min },
};

const lsm_compaction_block_count_min = StateMachine.Forest.Options.compaction_block_count_min;
const lsm_compaction_block_memory_min = lsm_compaction_block_count_min * constants.block_size;

pub const Command = union(enum) {
    pub const Format = struct {
        cluster: u128,
        replica: u8,
        replica_count: u8,
        development: bool,
        path: [:0]const u8,
    };

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
        pipeline_requests_limit: u32,
        request_size_limit: u32,
        cache_grid_blocks: u32,
        lsm_forest_compaction_block_count: u32,
        lsm_forest_node_count: u32,
        development: bool,
        experimental: bool,
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

        cache_accounts: ?[]const u8,
        cache_transfers: ?[]const u8,
        cache_transfers_pending: ?[]const u8,
        cache_account_balances: ?[]const u8,
        cache_grid: ?[]const u8,
        account_count: usize,
        account_balances: bool,
        account_batch_size: usize,
        transfer_count: usize,
        transfer_pending: bool,
        transfer_batch_size: usize,
        transfer_batch_delay_us: usize,
        query_count: usize,
        print_batch_timings: bool,
        id_order: IdOrder,
        statsd: bool,
        addresses: ?[]const net.Address,
        seed: ?u64,
    };

    format: Format,
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
                    .development = format.development,
                    .path = format.positional.path,
                },
            };
        },
        .start => |start| {
            // Allowlist of stable flags. --development will disable automatic multiversion
            // upgrades too, but the flag itself is stable.
            const stable_args = .{
                "addresses",   "cache_grid",   "positional",
                "development", "experimental",
            };
            inline for (std.meta.fields(@TypeOf(start))) |field| {
                const stable_field = comptime for (stable_args) |stable_arg| {
                    assert(std.meta.fieldIndex(@TypeOf(start), stable_arg) != null);
                    if (std.mem.eql(u8, field.name, stable_arg)) {
                        break true;
                    }
                } else false;
                if (stable_field) continue;

                const flag_name = flags.flag_name(field);

                // If you've added a flag and get a comptime error here, it's likely because
                // we require experimental flags to default to null.
                assert(flags.default_value(field).? == null);

                if (@field(start, field.name) != null and !start.experimental) {
                    flags.fatal(
                        "{s} is marked experimental, add `--experimental` to continue.",
                        .{flag_name},
                    );
                }
            }

            const groove_config = StateMachine.Forest.groove_config;
            const AccountsValuesCache = groove_config.accounts.ObjectsCache.Cache;
            const TransfersValuesCache = groove_config.transfers.ObjectsCache.Cache;
            const TransfersPendingValuesCache = groove_config.transfers_pending.ObjectsCache.Cache;
            const AccountBalancesValuesCache = groove_config.account_balances.ObjectsCache.Cache;

            const addresses = parse_addresses(allocator, start.addresses);
            const defaults =
                if (start.development) start_defaults_development else start_defaults_production;

            const start_limit_storage: flags.ByteSize = start.limit_storage orelse
                .{ .value = constants.storage_size_limit_max };
            const start_memory_lsm_manifest: flags.ByteSize = start.memory_lsm_manifest orelse
                .{ .value = constants.lsm_manifest_memory_size_default };

            const storage_size_limit = start_limit_storage.bytes();
            const storage_size_limit_min = data_file_size_min;
            const storage_size_limit_max = constants.storage_size_limit_max;
            if (storage_size_limit > storage_size_limit_max) {
                flags.fatal("--limit-storage: size {}{s} exceeds maximum: {}MiB", .{
                    start_limit_storage.value,
                    start_limit_storage.suffix(),
                    @divExact(storage_size_limit_max, 1024 * 1024),
                });
            }
            if (storage_size_limit < storage_size_limit_min) {
                flags.fatal("--limit-storage: size {}{s} is below minimum: {}KiB", .{
                    start_limit_storage.value,
                    start_limit_storage.suffix(),
                    @divExact(storage_size_limit_min, 1024),
                });
            }
            if (storage_size_limit % constants.sector_size != 0) {
                flags.fatal(
                    "--limit-storage: size {}{s} must be a multiple of sector size ({}KiB)",
                    .{
                        start_limit_storage.value,
                        start_limit_storage.suffix(),
                        @divExact(constants.sector_size, 1024),
                    },
                );
            }

            const pipeline_limit =
                start.limit_pipeline_requests orelse defaults.limit_pipeline_requests;
            const pipeline_limit_min = 0;
            const pipeline_limit_max = constants.pipeline_request_queue_max;
            if (pipeline_limit > pipeline_limit_max) {
                flags.fatal("--limit-pipeline-requests: count {} exceeds maximum: {}", .{
                    pipeline_limit,
                    pipeline_limit_max,
                });
            }
            if (pipeline_limit < pipeline_limit_min) {
                flags.fatal("--limit-pipeline-requests: count {} is below minimum: {}", .{
                    pipeline_limit,
                    pipeline_limit_min,
                });
            }

            // The minimum is chosen rather arbitrarily as 4096 since it is the sector size.
            const request_size_limit = start.limit_request orelse defaults.limit_request;
            const request_size_limit_min = 4096;
            const request_size_limit_max = constants.message_size_max;
            if (request_size_limit.bytes() > request_size_limit_max) {
                if (comptime (request_size_limit_max >= 1024 * 1024)) {
                    flags.fatal("--limit-request: size {}{s} exceeds maximum: {}MiB", .{
                        request_size_limit.value,
                        request_size_limit.suffix(),
                        @divExact(request_size_limit_max, 1024 * 1024),
                    });
                } else {
                    flags.fatal("--limit-request: size {}{s} exceeds maximum: {}KiB", .{
                        request_size_limit.value,
                        request_size_limit.suffix(),
                        @divExact(request_size_limit_max, 1024),
                    });
                }
            }
            if (request_size_limit.bytes() < request_size_limit_min) {
                flags.fatal("--limit-request: size {}{s} is below minimum: {}B", .{
                    request_size_limit.value,
                    request_size_limit.suffix(),
                    request_size_limit_min,
                });
            }

            const lsm_manifest_memory = start_memory_lsm_manifest.bytes();
            const lsm_manifest_memory_max = constants.lsm_manifest_memory_size_max;
            const lsm_manifest_memory_min = constants.lsm_manifest_memory_size_min;
            const lsm_manifest_memory_multiplier = constants.lsm_manifest_memory_size_multiplier;
            if (lsm_manifest_memory > lsm_manifest_memory_max) {
                flags.fatal("--memory-lsm-manifest: size {}{s} exceeds maximum: {}MiB", .{
                    start_memory_lsm_manifest.value,
                    start_memory_lsm_manifest.suffix(),
                    @divExact(lsm_manifest_memory_max, 1024 * 1024),
                });
            }
            if (lsm_manifest_memory < lsm_manifest_memory_min) {
                flags.fatal("--memory-lsm-manifest: size {}{s} is below minimum: {}MiB", .{
                    start_memory_lsm_manifest.value,
                    start_memory_lsm_manifest.suffix(),
                    @divExact(lsm_manifest_memory_min, 1024 * 1024),
                });
            }
            if (lsm_manifest_memory % lsm_manifest_memory_multiplier != 0) {
                flags.fatal(
                    "--memory-lsm-manifest: size {}{s} must be a multiple of {}MiB",
                    .{
                        start_memory_lsm_manifest.value,
                        start_memory_lsm_manifest.suffix(),
                        @divExact(lsm_manifest_memory_multiplier, 1024 * 1024),
                    },
                );
            }

            const lsm_compaction_block_memory =
                start.memory_lsm_compaction orelse defaults.memory_lsm_compaction;
            const lsm_compaction_block_memory_max = constants.compaction_block_memory_size_max;
            if (lsm_compaction_block_memory.bytes() > lsm_compaction_block_memory_max) {
                flags.fatal("--memory-lsm-compaction: size {}{s} exceeds maximum: {}GiB", .{
                    lsm_compaction_block_memory.value,
                    lsm_compaction_block_memory.suffix(),
                    @divFloor(lsm_compaction_block_memory_max, 1024 * 1024 * 1024),
                });
            }
            if (lsm_compaction_block_memory.bytes() < lsm_compaction_block_memory_min) {
                flags.fatal("--memory-lsm-compaction: size {}{s} is below minimum: {}KiB", .{
                    lsm_compaction_block_memory.value,
                    lsm_compaction_block_memory.suffix(),
                    @divExact(lsm_compaction_block_memory_min, 1024),
                });
            }
            if (lsm_compaction_block_memory.bytes() % constants.block_size != 0) {
                flags.fatal(
                    "--memory-lsm-compaction: size {}{s} must be a multiple of {}KiB",
                    .{
                        lsm_compaction_block_memory.value,
                        lsm_compaction_block_memory.suffix(),
                        @divExact(constants.block_size, 1024),
                    },
                );
            }

            const lsm_forest_compaction_block_count: u32 =
                @intCast(@divExact(lsm_compaction_block_memory.bytes(), constants.block_size));
            const lsm_forest_node_count: u32 =
                @intCast(@divExact(lsm_manifest_memory, constants.lsm_manifest_node_size));

            return Command{
                .start = .{
                    .addresses = addresses,
                    .addresses_zero = std.mem.eql(u8, start.addresses, "0"),
                    .storage_size_limit = storage_size_limit,
                    .pipeline_requests_limit = pipeline_limit,
                    .request_size_limit = @intCast(request_size_limit.bytes()),
                    .cache_accounts = parse_cache_size_to_count(
                        tigerbeetle.Account,
                        AccountsValuesCache,
                        start.cache_accounts orelse defaults.cache_accounts,
                    ),
                    .cache_transfers = parse_cache_size_to_count(
                        tigerbeetle.Transfer,
                        TransfersValuesCache,
                        start.cache_transfers orelse defaults.cache_transfers,
                    ),
                    .cache_transfers_pending = parse_cache_size_to_count(
                        StateMachine.TransferPending,
                        TransfersPendingValuesCache,
                        start.cache_transfers_pending orelse defaults.cache_transfers_pending,
                    ),
                    .cache_account_balances = parse_cache_size_to_count(
                        StateMachine.AccountBalancesGrooveValue,
                        AccountBalancesValuesCache,
                        start.cache_account_balances orelse defaults.cache_account_balances,
                    ),
                    .cache_grid_blocks = parse_cache_size_to_count(
                        [constants.block_size]u8,
                        Grid.Cache,
                        start.cache_grid orelse defaults.cache_grid,
                    ),
                    .lsm_forest_compaction_block_count = lsm_forest_compaction_block_count,
                    .lsm_forest_node_count = lsm_forest_node_count,
                    .development = start.development,
                    .experimental = start.experimental,
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
                    .account_balances = benchmark.account_balances,
                    .account_batch_size = benchmark.account_batch_size,
                    .transfer_count = benchmark.transfer_count,
                    .transfer_pending = benchmark.transfer_pending,
                    .transfer_batch_size = benchmark.transfer_batch_size,
                    .transfer_batch_delay_us = benchmark.transfer_batch_delay_us,
                    .query_count = benchmark.query_count,
                    .print_batch_timings = benchmark.print_batch_timings,
                    .id_order = benchmark.id_order,
                    .statsd = benchmark.statsd,
                    .addresses = addresses,
                    .seed = benchmark.seed,
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

    const count_limit = @divFloor(size.bytes(), @sizeOf(T));
    const count_rounded = @divFloor(
        count_limit,
        value_count_max_multiple,
    ) * value_count_max_multiple;

    const result: u32 = @intCast(count_rounded); // TODO: better error message on overflow
    assert(@as(u64, result) * @sizeOf(T) <= size.bytes());

    return result;
}
