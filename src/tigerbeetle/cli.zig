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
const stdx = vsr.stdx;
const constants = vsr.constants;
const tigerbeetle = vsr.tigerbeetle;
const data_file_size_min = vsr.superblock.data_file_size_min;
const StateMachine = @import("./main.zig").StateMachine;
const Grid = @import("./main.zig").Grid;
const Ratio = stdx.PRNG.Ratio;
const ByteSize = stdx.ByteSize;
const Operation = tigerbeetle.Operation;
const Duration = stdx.Duration;

comptime {
    // Make sure we are running the Accounting StateMachine.
    assert(StateMachine.Operation == tigerbeetle.Operation);
}

const KiB = stdx.KiB;
const GiB = stdx.GiB;

const CLIArgs = union(enum) {
    const Format = struct {
        cluster: ?u128 = null,
        replica: ?u8 = null,
        // Experimental: standbys don't have a concrete practical use-case yet.
        standby: ?u8 = null,
        replica_count: u8,
        development: bool = false,
        log_debug: bool = false,

        @"--": void,
        path: []const u8,
    };

    const Recover = struct {
        cluster: u128,
        addresses: []const u8,
        replica: u8,
        replica_count: u8,
        development: bool = false,
        log_debug: bool = false,

        @"--": void,
        path: []const u8,
    };

    const Start = struct {
        // Stable CLI arguments.
        addresses: []const u8,
        cache_grid: ?ByteSize = null,
        development: bool = false,

        // Everything from here until positional arguments is considered experimental, and requires
        // `--experimental` to be set. Experimental flags disable automatic upgrades with
        // multiversion binaries; each replica has to be manually restarted. Experimental flags must
        // default to null, except for bools which must be false.
        experimental: bool = false,

        limit_storage: ?ByteSize = null,
        limit_pipeline_requests: ?u32 = null,
        limit_request: ?ByteSize = null,
        cache_accounts: ?ByteSize = null,
        cache_transfers: ?ByteSize = null,
        cache_transfers_pending: ?ByteSize = null,
        memory_lsm_manifest: ?ByteSize = null,
        memory_lsm_compaction: ?ByteSize = null,
        trace: ?[]const u8 = null,
        log_debug: bool = false,
        log_trace: bool = false,
        timeout_prepare_ms: ?u64 = null,
        timeout_grid_repair_message_ms: ?u64 = null,
        commit_stall_probability: ?Ratio = null,

        // Highly experimental options that will be removed in a future release:
        replicate_star: bool = false,

        statsd: ?[]const u8 = null,

        /// AOF (Append Only File) logs all transactions synchronously to disk before replying
        /// to the client. The logic behind this code has been kept as simple as possible -
        /// io_uring or kqueue aren't used, there aren't any fancy data structures. Just a simple
        /// log consisting of logged requests. Much like a redis AOF with fsync=on.
        /// Enabling this will have performance implications.
        aof_file: ?[]const u8 = null,

        /// Legacy AOF option. Mututally exclusive with aof_file, and will have the same effect as
        /// setting aof_file to '<data file path>.aof'.
        aof: bool = false,

        @"--": void,
        path: []const u8,
    };

    const Version = struct {
        verbose: bool = false,
    };

    const Repl = struct {
        addresses: []const u8,
        cluster: u128,
        verbose: bool = false,
        command: []const u8 = "",
        log_debug: bool = false,
    };

    // Experimental: the interface is subject to change.
    const Benchmark = struct {
        cache_accounts: ?[]const u8 = null,
        cache_transfers: ?[]const u8 = null,
        cache_transfers_pending: ?[]const u8 = null,
        cache_grid: ?[]const u8 = null,
        account_count: u32 = 10_000,
        account_count_hot: u32 = 0,
        log_debug: bool = false,
        log_debug_replica: bool = false,
        /// The probability distribution used to select accounts when making transfers or queries.
        account_distribution: Command.Benchmark.Distribution = .uniform,
        flag_history: bool = false,
        flag_imported: bool = false,
        account_batch_size: u32 = Operation.create_accounts.event_max(
            constants.message_body_size_max,
        ),
        transfer_count: u64 = 10_000_000,
        transfer_hot_percent: u32 = 100,
        transfer_pending: bool = false,
        transfer_batch_size: u32 = Operation.create_transfers.event_max(
            constants.message_body_size_max,
        ),
        transfer_batch_delay: Duration = .ms(0),
        validate: bool = false,
        checksum_performance: bool = false,
        query_count: u32 = 100,
        print_batch_timings: bool = false,
        id_order: Command.Benchmark.IdOrder = .sequential,
        clients: u32 = 1,
        statsd: ?[]const u8 = null,
        trace: ?[]const u8 = null,
        /// When set, don't delete the data file when the benchmark completes.
        file: ?[]const u8 = null,
        addresses: ?[]const u8 = null,
        seed: ?[]const u8 = null,
    };

    // Experimental: the interface is subject to change.
    const Inspect = union(enum) {
        constants,
        metrics,
        op: struct {
            @"--": void,
            op: u64,
        },
        superblock: struct {
            @"--": void,
            path: []const u8,
        },
        wal: struct {
            slot: ?usize = null,

            @"--": void,
            path: []const u8,
        },
        replies: struct {
            slot: ?usize = null,
            superblock_copy: ?u8 = null,

            @"--": void,
            path: []const u8,
        },
        grid: struct {
            block: ?u64 = null,
            superblock_copy: ?u8 = null,

            @"--": void,
            path: []const u8,
        },
        manifest: struct {
            superblock_copy: ?u8 = null,

            @"--": void,
            path: []const u8,
        },
        tables: struct {
            superblock_copy: ?u8 = null,
            tree: []const u8,
            level: ?u6 = null,

            @"--": void,
            path: []const u8,
        },
        integrity: struct {
            log_debug: bool = false,
            seed: ?[]const u8 = null,
            memory_lsm_manifest: ?ByteSize = null,
            skip_wal: bool = false,
            skip_client_replies: bool = false,
            skip_grid: bool = false,

            @"--": void,
            path: [:0]const u8,
        },

        pub const help =
            \\Usage:
            \\
            \\  tigerbeetle inspect [-h | --help]
            \\
            \\  tigerbeetle inspect constants
            \\
            \\  tigerbeetle inspect metrics
            \\
            \\  tigerbeetle inspect op <op>
            \\
            \\  tigerbeetle inspect superblock <path>
            \\
            \\  tigerbeetle inspect wal [--slot=<slot>] <path>
            \\
            \\  tigerbeetle inspect replies [--slot=<slot>] <path>
            \\
            \\  tigerbeetle inspect grid [--block=<address>] <path>
            \\
            \\  tigerbeetle inspect manifest <path>
            \\
            \\  tigerbeetle inspect tables --tree=<name|id> [--level=<integer>] <path>
            \\
            \\  tigerbeetle inspect integrity [--log-debug] [--seed=<seed>]
            \\                                [--memory-lsm-manifest=<size>]
            \\                                [--skip-wal] [--skip-client-replies] [--skip-grid]
            \\                                <path>
            \\
            \\Options:
            \\
            \\  When `--superblock-copy` is set, use the trailer referenced by that superblock copy.
            \\  Otherwise, the current quorum will be used by default.
            \\
            \\  -h, --help
            \\        Print this help message and exit.
            \\
            \\  constants
            \\        Print most important compile-time parameters.
            \\
            \\  metrics
            \\        List metrics and their cardinalities.
            \\
            \\  op
            \\        Print op numbers for adjacent checkpoints and triggers.
            \\
            \\  superblock
            \\        Inspect the superblock header copies.
            \\
            \\  wal
            \\        Inspect the WAL headers and prepares.
            \\
            \\  wal --slot=<slot>
            \\        Inspect the WAL header/prepare in the given slot.
            \\
            \\  replies [--superblock-copy=<copy>]
            \\        Inspect the client reply headers and session numbers.
            \\
            \\  replies --slot=<slot> [--superblock-copy=<copy>]
            \\        Inspect a particular client reply.
            \\
            \\  grid [--superblock-copy=<copy>]
            \\        Inspect the free set.
            \\
            \\  grid --block=<address>
            \\        Inspect the block at the given address.
            \\
            \\  manifest [--superblock-copy=<copy>]
            \\        Inspect the LSM manifest.
            \\
            \\  tables --tree=<name|id> [--level=<integer>] [--superblock-copy=<copy>]
            \\        List the tables matching the given tree/level.
            \\        Example tree names: "transfers" (object table), "transfers.amount" (index table).
            \\
            \\  integrity
            \\        Scans the data file and checks all internal checksums to verify internal
            \\        integrity.
            \\
        ;
    };

    // Internal: used to validate multiversion binaries.
    const Multiversion = struct {
        log_debug: bool = false,

        @"--": void,
        path: []const u8,
    };

    // CDC connector for AMQP targets.
    const AMQP = struct {
        addresses: []const u8,
        cluster: u128,
        host: []const u8,
        user: []const u8,
        password: []const u8,
        vhost: []const u8,
        publish_exchange: ?[]const u8 = null,
        publish_routing_key: ?[]const u8 = null,
        event_count_max: ?u32 = null,
        idle_interval_ms: ?u32 = null,
        requests_per_second_limit: ?u32 = null,
        timestamp_last: ?u64 = null,
        verbose: bool = false,
    };

    format: Format,
    recover: Recover,
    start: Start,
    version: Version,
    repl: Repl,
    benchmark: Benchmark,
    inspect: Inspect,
    multiversion: Multiversion,
    amqp: AMQP,

    // TODO Document --cache-accounts, --cache-transfers, --cache-transfers-posted, --limit-storage,
    // --limit-pipeline-requests
    pub const help = fmt.comptimePrint(
        \\Usage:
        \\
        \\  tigerbeetle [-h | --help]
        \\
        \\  tigerbeetle format [--cluster=<integer>] --replica=<index> --replica-count=<integer> <path>
        \\
        \\  tigerbeetle start --addresses=<addresses> [--cache-grid=<size><KiB|MiB|GiB>] <path>
        \\
        \\  tigerbeetle recover --cluster=<integer> --addresses=<addresses>
        \\                      --replica=<index> --replica-count=<integer> <path>
        \\
        \\  tigerbeetle version [--verbose]
        \\
        \\  tigerbeetle repl --cluster=<integer> --addresses=<addresses>
        \\
        \\Commands:
        \\
        \\  format     Create a TigerBeetle replica data file at <path>.
        \\             The --replica and --replica-count arguments are required.
        \\             Each TigerBeetle replica must have its own data file.
        \\
        \\  start      Run a TigerBeetle replica from the data file at <path>.
        \\
        \\  recover    Create a TigerBeetle replica data file at <path> for recovery.
        \\             Used when a replica's data file is completely lost.
        \\             Replicas with recovered data files must sync with the cluster before
        \\             they can participate in consensus.
        \\
        \\  version    Print the TigerBeetle build version and the compile-time config values.
        \\
        \\  repl       Enter the TigerBeetle client REPL.
        \\
        \\  amqp       CDC connector for AMQP targets.
        \\
        \\Options:
        \\
        \\  -h, --help
        \\        Print this help message and exit.
        \\
        \\  --cluster=<integer>
        \\        Set the cluster ID to the provided 128-bit unsigned decimal integer.
        \\        Defaults to generating a random cluster ID.
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
        \\        The addresses of all replicas in the cluster.
        \\        Accepts a comma-separated list of IPv4/IPv6 addresses with port numbers.
        \\        The order is significant and must match across all replicas and clients.
        \\        Either the address or port number (but not both) may be omitted,
        \\        in which case a default of {[default_address]s} or {[default_port]d} will be used.
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
        \\        Allow the replica to format/start/recover even when Direct IO is unavailable.
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
        \\  tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 0_0.tigerbeetle
        \\  tigerbeetle start --addresses=3000,3001,3002 0_1.tigerbeetle
        \\  tigerbeetle start --addresses=3000,3001,3002 0_2.tigerbeetle
        \\
        \\  tigerbeetle start --addresses=192.168.0.1,192.168.0.2,192.168.0.3 0_0.tigerbeetle
        \\
        \\  tigerbeetle start --addresses='[::1]:3000,[::1]:3001,[::1]:3002' 0_0.tigerbeetle
        \\
        \\  tigerbeetle recover --cluster=0 --addresses=3003,3001,3002 \
        \\                      --replica=1 --replica-count=3 0_1.tigerbeetle
        \\
        \\  tigerbeetle version --verbose
        \\
        \\  tigerbeetle repl --addresses=3000,3001,3002 --cluster=0
        \\
        \\  tigerbeetle amqp --addresses=3000,3001,3002 --cluster=0 \
        \\      --host=127.0.0.1 --vhost=/ --user=guest --password=guest \
        \\      --publish-exchange=my_exhange_name
        \\
    , .{
        .default_address = constants.address,
        .default_port = constants.port,
        .default_cache_grid_gb = @divExact(
            constants.grid_cache_size_default,
            GiB,
        ),
    });
};

const StartDefaults = struct {
    limit_pipeline_requests: u32,
    limit_request: ByteSize,
    cache_accounts: ByteSize,
    cache_transfers: ByteSize,
    cache_transfers_pending: ByteSize,
    cache_grid: ByteSize,
    memory_lsm_compaction: ByteSize,
};

const start_defaults_production = StartDefaults{
    .limit_pipeline_requests = vsr.stdx.div_ceil(constants.clients_max, 2) -
        constants.pipeline_prepare_queue_max,
    .limit_request = .{ .value = constants.message_size_max },
    .cache_accounts = .{ .value = constants.cache_accounts_size_default },
    .cache_transfers = .{ .value = constants.cache_transfers_size_default },
    .cache_transfers_pending = .{ .value = constants.cache_transfers_pending_size_default },
    .cache_grid = .{ .value = constants.grid_cache_size_default },
    .memory_lsm_compaction = .{
        // By default, add a few extra blocks for beat-scoped work.
        .value = (lsm_compaction_block_count_min + 16) * constants.block_size,
    },
};

const start_defaults_development = StartDefaults{
    .limit_pipeline_requests = 0,
    .limit_request = .{ .value = 32 * KiB },
    .cache_accounts = .{ .value = 0 },
    .cache_transfers = .{ .value = 0 },
    .cache_transfers_pending = .{ .value = 0 },
    .cache_grid = .{ .value = constants.block_size * Grid.Cache.value_count_max_multiple },
    .memory_lsm_compaction = .{ .value = lsm_compaction_block_memory_min },
};

const lsm_compaction_block_count_min = StateMachine.Forest.Options.compaction_block_count_min;
const lsm_compaction_block_memory_min = lsm_compaction_block_count_min * constants.block_size;

/// While CLIArgs store raw arguments as passed on the command line, Command ensures that arguments
/// are properly validated and desugared (e.g, sizes converted to counts where appropriate).
pub const Command = union(enum) {
    const Addresses = stdx.BoundedArrayType(std.net.Address, constants.members_max);
    const Path = stdx.BoundedArrayType(u8, std.fs.max_path_bytes);

    pub const Format = struct {
        cluster: u128,
        replica: u8,
        replica_count: u8,
        development: bool,
        path: []const u8,
        log_debug: bool,
    };

    pub const Recover = struct {
        cluster: u128,
        addresses: Addresses,
        replica: u8,
        replica_count: u8,
        development: bool,
        path: []const u8,
        log_debug: bool,
    };

    pub const Start = struct {
        addresses: Addresses,
        // true when the value of `--addresses` is exactly `0`. Used to enable "magic zero" mode for
        // testing. We check the raw string rather then the parsed address to prevent triggering
        // this logic by accident.
        addresses_zero: bool,
        cache_accounts: u32,
        cache_transfers: u32,
        cache_transfers_pending: u32,
        storage_size_limit: u64,
        pipeline_requests_limit: u32,
        request_size_limit: u32,
        cache_grid_blocks: u32,
        lsm_forest_compaction_block_count: u32,
        lsm_forest_node_count: u32,
        timeout_prepare_ticks: ?u64,
        timeout_grid_repair_message_ticks: ?u64,
        commit_stall_probability: ?Ratio,
        trace: ?[]const u8,
        development: bool,
        experimental: bool,
        replicate_star: bool,
        aof_file: ?Path,
        path: []const u8,
        log_debug: bool,
        log_trace: bool,
        statsd: ?std.net.Address,
    };

    pub const Version = struct {
        verbose: bool,
    };

    pub const Repl = struct {
        addresses: Addresses,
        cluster: u128,
        verbose: bool,
        statements: []const u8,
        log_debug: bool,
    };

    pub const Benchmark = struct {
        /// The ID order can affect the results of a benchmark significantly. Specifically,
        /// sequential is expected to be the best (since it can take advantage of various
        /// optimizations such as avoiding negative prefetch) while random/reversed can't.
        pub const IdOrder = enum { sequential, random, reversed };

        pub const Distribution = enum {
            /// Shuffled zipfian numbers where relatively few indexes are selected frequently.
            zipfian,
            /// Also zipfian, but the most recent indexes are selected frequently.
            latest,
            /// Uniform distribution; unrealistic workloads.
            uniform,
        };

        cache_accounts: ?[]const u8,
        cache_transfers: ?[]const u8,
        cache_transfers_pending: ?[]const u8,
        cache_grid: ?[]const u8,
        log_debug: bool,
        log_debug_replica: bool,
        account_count: u32,
        account_count_hot: u32,
        account_distribution: Distribution,
        flag_history: bool,
        flag_imported: bool,
        account_batch_size: u32,
        transfer_count: u64,
        transfer_hot_percent: u32,
        transfer_pending: bool,
        transfer_batch_size: u32,
        transfer_batch_delay: Duration,
        validate: bool,
        checksum_performance: bool,
        query_count: u32,
        print_batch_timings: bool,
        id_order: IdOrder,
        clients: u32,
        statsd: ?[]const u8,
        trace: ?[]const u8,
        file: ?[]const u8,
        addresses: ?Addresses,
        seed: ?[]const u8,
    };

    pub const Inspect = union(enum) {
        constants,
        metrics,
        op: u64,
        data_file: DataFile,
        integrity: Integrity,

        pub const DataFile = struct {
            path: []const u8,
            query: union(enum) {
                superblock,
                wal: struct {
                    slot: ?usize,
                },
                replies: struct {
                    slot: ?usize,
                    superblock_copy: ?u8,
                },
                grid: struct {
                    block: ?u64,
                    superblock_copy: ?u8,
                },
                manifest: struct {
                    superblock_copy: ?u8,
                },
                tables: struct {
                    superblock_copy: ?u8,
                    tree: []const u8,
                    level: ?u6,
                },
            },
        };

        pub const Integrity = struct {
            log_debug: bool,
            seed: ?[]const u8,
            lsm_forest_node_count: u32,
            skip_wal: bool,
            skip_client_replies: bool,
            skip_grid: bool,
            path: [:0]const u8,
        };
    };

    pub const Multiversion = struct {
        path: []const u8,
        log_debug: bool,
    };

    pub const AMQP = struct {
        addresses: Addresses,
        cluster: u128,
        host: std.net.Address,
        user: []const u8,
        password: []const u8,
        vhost: []const u8,
        publish_exchange: ?[]const u8,
        publish_routing_key: ?[]const u8,
        event_count_max: ?u32,
        idle_interval_ms: ?u32,
        requests_per_second_limit: ?u32,
        timestamp_last: ?u64,
        log_debug: bool,
    };

    format: Format,
    recover: Recover,
    start: Start,
    version: Version,
    repl: Repl,
    benchmark: Benchmark,
    inspect: Inspect,
    multiversion: Multiversion,
    amqp: AMQP,
};

/// Parse the command line arguments passed to the `tigerbeetle` binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(args_iterator: *std.process.ArgIterator) Command {
    const cli_args = stdx.flags(args_iterator, CLIArgs);

    return switch (cli_args) {
        .format => |format| .{ .format = parse_args_format(format) },
        .recover => |recover| .{ .recover = parse_args_recover(recover) },
        .start => |start| .{ .start = parse_args_start(start) },
        .version => |version| .{ .version = parse_args_version(version) },
        .repl => |repl| .{ .repl = parse_args_repl(repl) },
        .benchmark => |benchmark| .{ .benchmark = parse_args_benchmark(benchmark) },
        .inspect => |inspect| .{ .inspect = parse_args_inspect(inspect) },
        .multiversion => |multiversion| .{ .multiversion = parse_args_multiversion(multiversion) },
        .amqp => |amqp| .{ .amqp = parse_args_amqp(amqp) },
    };
}

fn parse_args_format(format: CLIArgs.Format) Command.Format {
    if (format.replica_count == 0) {
        vsr.fatal(.cli, "--replica-count: value needs to be greater than zero", .{});
    }
    if (format.replica_count > constants.replicas_max) {
        vsr.fatal(.cli, "--replica-count: value is too large ({}), at most {} is allowed", .{
            format.replica_count,
            constants.replicas_max,
        });
    }

    if (format.replica == null and format.standby == null) {
        vsr.fatal(.cli, "--replica: argument is required", .{});
    }

    if (format.replica != null and format.standby != null) {
        vsr.fatal(.cli, "--standby: conflicts with '--replica'", .{});
    }

    if (format.replica) |replica| {
        if (replica >= format.replica_count) {
            vsr.fatal(.cli, "--replica: value is too large ({}), at most {} is allowed", .{
                replica,
                format.replica_count - 1,
            });
        }
    }

    if (format.standby) |standby| {
        if (standby < format.replica_count) {
            vsr.fatal(.cli, "--standby: value is too small ({}), at least {} is required", .{
                standby,
                format.replica_count,
            });
        }
        if (standby >= format.replica_count + constants.standbys_max) {
            vsr.fatal(.cli, "--standby: value is too large ({}), at most {} is allowed", .{
                standby,
                format.replica_count + constants.standbys_max - 1,
            });
        }
    }

    const replica = (format.replica orelse format.standby).?;
    assert(replica < constants.members_max);
    assert(replica < format.replica_count + constants.standbys_max);

    const cluster_random = std.crypto.random.int(u128);
    assert(cluster_random != 0);
    const cluster = format.cluster orelse cluster_random;
    if (format.cluster == null) {
        std.log.info("generated random cluster id: {}\n", .{cluster});
    } else if (format.cluster.? == 0) {
        std.log.warn("a cluster id of 0 is reserved for testing and benchmarking, " ++
            "do not use in production", .{});
        std.log.warn("omit --cluster=0 to randomly generate a suitable id\n", .{});
    }

    return .{
        .cluster = cluster, // just an ID, any value is allowed
        .replica = replica,
        .replica_count = format.replica_count,
        .development = format.development,
        .path = format.path,
        .log_debug = format.log_debug,
    };
}

fn parse_args_recover(recover: CLIArgs.Recover) Command.Recover {
    if (recover.replica_count == 0) {
        vsr.fatal(.cli, "--replica-count: value needs to be greater than zero", .{});
    }
    if (recover.replica_count > constants.replicas_max) {
        vsr.fatal(.cli, "--replica-count: value is too large ({}), at most {} is allowed", .{
            recover.replica_count,
            constants.replicas_max,
        });
    }

    if (recover.replica >= recover.replica_count) {
        vsr.fatal(.cli, "--replica: value is too large ({}), at most {} is allowed", .{
            recover.replica,
            recover.replica_count - 1,
        });
    }
    if (recover.replica_count <= 2) {
        vsr.fatal(.cli, "--replica-count: 1- or 2- replica clusters don't support 'recover'", .{});
    }

    const replica = recover.replica;
    assert(replica < constants.members_max);
    assert(replica < recover.replica_count);

    return .{
        .cluster = recover.cluster,
        .addresses = parse_addresses(recover.addresses, "--addresses", Command.Addresses),
        .replica = replica,
        .replica_count = recover.replica_count,
        .development = recover.development,
        .path = recover.path,
        .log_debug = recover.log_debug,
    };
}

fn parse_args_start(start: CLIArgs.Start) Command.Start {
    // Allowlist of stable flags. --development will disable automatic multiversion
    // upgrades too, but the flag itself is stable.
    const stable_args = .{
        "addresses",   "cache_grid",
        "development", "experimental",
    };
    inline for (std.meta.fields(@TypeOf(start))) |field| {
        @setEvalBranchQuota(4_000);
        // Positional arguments can't be experimental.
        comptime if (std.mem.eql(u8, field.name, "--")) break;

        const stable_field = comptime for (stable_args) |stable_arg| {
            assert(std.meta.fieldIndex(@TypeOf(start), stable_arg) != null);
            if (std.mem.eql(u8, field.name, stable_arg)) {
                break true;
            }
        } else false;
        if (stable_field) continue;

        const flag_name = comptime blk: {
            var result: [2 + field.name.len]u8 = ("--" ++ field.name).*;
            std.mem.replaceScalar(u8, &result, '_', '-');
            break :blk result;
        };

        // If you've added a flag and get a comptime error here, it's likely because
        // we require experimental flags to default to null.
        const required_default = if (field.type == bool) false else null;
        assert(field.defaultValue().? == required_default);

        if (@field(start, field.name) != required_default and !start.experimental) {
            vsr.fatal(
                .cli,
                "{s} is marked experimental, add `--experimental` to continue.",
                .{flag_name},
            );
        }
    } else unreachable;

    const groove_config = StateMachine.Forest.groove_config;
    const AccountsValuesCache = groove_config.accounts.ObjectsCache.Cache;
    const TransfersValuesCache = groove_config.transfers.ObjectsCache.Cache;
    const TransfersPendingValuesCache = groove_config.transfers_pending.ObjectsCache.Cache;

    const addresses = parse_addresses(start.addresses, "--addresses", Command.Addresses);
    const defaults =
        if (start.development) start_defaults_development else start_defaults_production;

    const start_limit_storage: ByteSize = start.limit_storage orelse
        .{ .value = constants.storage_size_limit_default };
    const start_memory_lsm_manifest: ByteSize = start.memory_lsm_manifest orelse
        .{ .value = constants.lsm_manifest_memory_size_default };

    const storage_size_limit = start_limit_storage.bytes();
    const storage_size_limit_min = data_file_size_min;
    const storage_size_limit_max = constants.storage_size_limit_max;
    if (storage_size_limit > storage_size_limit_max) {
        vsr.fatal(.cli, "--limit-storage: size {}{s} exceeds maximum: {}", .{
            start_limit_storage.value,
            start_limit_storage.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(storage_size_limit_max),
        });
    }
    if (storage_size_limit < storage_size_limit_min) {
        vsr.fatal(.cli, "--limit-storage: size {}{s} is below minimum: {}", .{
            start_limit_storage.value,
            start_limit_storage.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(storage_size_limit_min),
        });
    }
    if (storage_size_limit % constants.sector_size != 0) {
        vsr.fatal(
            .cli,
            "--limit-storage: size {}{s} must be a multiple of sector size ({})",
            .{
                start_limit_storage.value,
                start_limit_storage.suffix(),
                vsr.stdx.fmt_int_size_bin_exact(constants.sector_size),
            },
        );
    }

    const pipeline_limit =
        start.limit_pipeline_requests orelse defaults.limit_pipeline_requests;
    const pipeline_limit_min = 0;
    const pipeline_limit_max = constants.pipeline_request_queue_max;
    if (pipeline_limit > pipeline_limit_max) {
        vsr.fatal(.cli, "--limit-pipeline-requests: count {} exceeds maximum: {}", .{
            pipeline_limit,
            pipeline_limit_max,
        });
    }
    if (pipeline_limit < pipeline_limit_min) {
        vsr.fatal(.cli, "--limit-pipeline-requests: count {} is below minimum: {}", .{
            pipeline_limit,
            pipeline_limit_min,
        });
    }

    // The minimum is chosen rather arbitrarily as 4096 since it is the sector size.
    const request_size_limit = start.limit_request orelse defaults.limit_request;
    const request_size_limit_min = 4096;
    const request_size_limit_max = constants.message_size_max;
    if (request_size_limit.bytes() > request_size_limit_max) {
        vsr.fatal(.cli, "--limit-request: size {}{s} exceeds maximum: {}", .{
            request_size_limit.value,
            request_size_limit.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(request_size_limit_max),
        });
    }
    if (request_size_limit.bytes() < request_size_limit_min) {
        vsr.fatal(.cli, "--limit-request: size {}{s} is below minimum: {}", .{
            request_size_limit.value,
            request_size_limit.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(request_size_limit_min),
        });
    }

    const lsm_manifest_memory = start_memory_lsm_manifest.bytes();
    const lsm_manifest_memory_max = constants.lsm_manifest_memory_size_max;
    const lsm_manifest_memory_min = constants.lsm_manifest_memory_size_min;
    const lsm_manifest_memory_multiplier = constants.lsm_manifest_memory_size_multiplier;
    if (lsm_manifest_memory > lsm_manifest_memory_max) {
        vsr.fatal(.cli, "--memory-lsm-manifest: size {}{s} exceeds maximum: {}", .{
            start_memory_lsm_manifest.value,
            start_memory_lsm_manifest.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(lsm_manifest_memory_max),
        });
    }
    if (lsm_manifest_memory < lsm_manifest_memory_min) {
        vsr.fatal(.cli, "--memory-lsm-manifest: size {}{s} is below minimum: {}", .{
            start_memory_lsm_manifest.value,
            start_memory_lsm_manifest.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(lsm_manifest_memory_min),
        });
    }
    if (lsm_manifest_memory % lsm_manifest_memory_multiplier != 0) {
        vsr.fatal(
            .cli,
            "--memory-lsm-manifest: size {}{s} must be a multiple of {}",
            .{
                start_memory_lsm_manifest.value,
                start_memory_lsm_manifest.suffix(),
                vsr.stdx.fmt_int_size_bin_exact(lsm_manifest_memory_multiplier),
            },
        );
    }

    const lsm_compaction_block_memory =
        start.memory_lsm_compaction orelse defaults.memory_lsm_compaction;
    const lsm_compaction_block_memory_max = constants.compaction_block_memory_size_max;
    if (lsm_compaction_block_memory.bytes() > lsm_compaction_block_memory_max) {
        vsr.fatal(.cli, "--memory-lsm-compaction: size {}{s} exceeds maximum: {}", .{
            lsm_compaction_block_memory.value,
            lsm_compaction_block_memory.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(lsm_compaction_block_memory_max),
        });
    }
    if (lsm_compaction_block_memory.bytes() < lsm_compaction_block_memory_min) {
        vsr.fatal(.cli, "--memory-lsm-compaction: size {}{s} is below minimum: {}", .{
            lsm_compaction_block_memory.value,
            lsm_compaction_block_memory.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(lsm_compaction_block_memory_min),
        });
    }
    if (lsm_compaction_block_memory.bytes() % constants.block_size != 0) {
        vsr.fatal(
            .cli,
            "--memory-lsm-compaction: size {}{s} must be a multiple of {}",
            .{
                lsm_compaction_block_memory.value,
                lsm_compaction_block_memory.suffix(),
                vsr.stdx.fmt_int_size_bin_exact(constants.block_size),
            },
        );
    }

    const lsm_forest_compaction_block_count: u32 =
        @intCast(@divExact(lsm_compaction_block_memory.bytes(), constants.block_size));
    const lsm_forest_node_count: u32 =
        @intCast(@divExact(lsm_manifest_memory, constants.lsm_manifest_node_size));

    const aof_file: ?Command.Path = if (start.aof) blk: {
        if (start.aof_file != null) {
            vsr.fatal(.cli, "--aof is mutually exclusive with --aof-file", .{});
        }

        var aof_file: Command.Path = .{};
        if (aof_file.capacity() < start.path.len + 4) {
            vsr.fatal(.cli, "data file path is too long for --aof. use --aof-file", .{});
        }
        aof_file.push_slice(start.path);
        aof_file.push_slice(".aof");

        std.log.warn(
            "--aof is deprecated. consider switching to '--aof-file={s}'",
            .{aof_file.const_slice()},
        );

        break :blk aof_file;
    } else if (start.aof_file) |start_aof_file| blk: {
        if (!std.mem.endsWith(u8, start_aof_file, ".aof")) {
            vsr.fatal(.cli, "--aof-file must end with .aof: '{s}'", .{start_aof_file});
        }

        var aof_file: Command.Path = .{};
        if (aof_file.capacity() < start.path.len) {
            vsr.fatal(.cli, "--aof-file path is too long", .{});
        }
        aof_file.push_slice(start_aof_file);

        break :blk aof_file;
    } else null;

    if (start.log_trace and !start.log_debug) {
        vsr.fatal(.cli, "--log-debug must be provided when using --log-trace", .{});
    }

    return .{
        .addresses = addresses,
        .addresses_zero = std.mem.eql(u8, start.addresses, "0"),
        .storage_size_limit = storage_size_limit,
        .pipeline_requests_limit = pipeline_limit,
        .request_size_limit = @intCast(request_size_limit.bytes()),
        .cache_accounts = parse_cache_size_to_count(
            tigerbeetle.Account,
            AccountsValuesCache,
            start.cache_accounts orelse defaults.cache_accounts,
            "--cache-accounts",
        ),
        .cache_transfers = parse_cache_size_to_count(
            tigerbeetle.Transfer,
            TransfersValuesCache,
            start.cache_transfers orelse defaults.cache_transfers,
            "--cache-transfers",
        ),
        .cache_transfers_pending = parse_cache_size_to_count(
            vsr.state_machine.TransferPending,
            TransfersPendingValuesCache,
            start.cache_transfers_pending orelse defaults.cache_transfers_pending,
            "--cache-transfers-pending",
        ),
        .cache_grid_blocks = parse_cache_size_to_count(
            [constants.block_size]u8,
            Grid.Cache,
            start.cache_grid orelse defaults.cache_grid,
            "--cache-grid",
        ),
        .lsm_forest_compaction_block_count = lsm_forest_compaction_block_count,
        .lsm_forest_node_count = lsm_forest_node_count,
        .timeout_prepare_ticks = parse_timeout_to_ticks(
            start.timeout_prepare_ms,
            "--timeout-prepare-ms",
        ),
        .timeout_grid_repair_message_ticks = parse_timeout_to_ticks(
            start.timeout_grid_repair_message_ms,
            "--timeout-grid-repair-message-ms",
        ),
        .commit_stall_probability = start.commit_stall_probability,
        .development = start.development,
        .experimental = start.experimental,
        .trace = start.trace,
        .replicate_star = start.replicate_star,
        .aof_file = aof_file,
        .path = start.path,
        .log_debug = start.log_debug,
        .log_trace = start.log_trace,
        .statsd = if (start.statsd) |statsd_address|
            parse_address_and_port(statsd_address, "--statsd", 8125)
        else
            null,
    };
}

fn parse_args_version(version: CLIArgs.Version) Command.Version {
    return .{
        .verbose = version.verbose,
    };
}

fn parse_args_repl(repl: CLIArgs.Repl) Command.Repl {
    const addresses = parse_addresses(repl.addresses, "--addresses", Command.Addresses);

    return .{
        .addresses = addresses,
        .cluster = repl.cluster,
        .verbose = repl.verbose,
        .statements = repl.command,
        .log_debug = repl.log_debug,
    };
}

const account_batch_size_max = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tigerbeetle.Account),
);

const transfer_batch_size_max = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tigerbeetle.Transfer),
);

fn parse_args_benchmark(benchmark: CLIArgs.Benchmark) Command.Benchmark {
    const addresses = if (benchmark.addresses) |addresses|
        parse_addresses(addresses, "--addresses", Command.Addresses)
    else
        null;

    if (benchmark.addresses != null and benchmark.file != null) {
        vsr.fatal(.cli, "--file: --addresses and --file are mutually exclusive", .{});
    }

    if (benchmark.account_batch_size == 0) {
        vsr.fatal(.cli, "--account-batch-size must be greater than 0", .{});
    }

    if (benchmark.account_batch_size > account_batch_size_max) {
        vsr.fatal(
            .cli,
            "--account-batch-size must be less than or equal to {}",
            .{account_batch_size_max},
        );
    }

    if (benchmark.transfer_batch_size == 0) {
        vsr.fatal(.cli, "--transfer-batch-size must be greater than 0", .{});
    }

    if (benchmark.transfer_batch_size > transfer_batch_size_max) {
        vsr.fatal(
            .cli,
            "--transfer-batch-size must be less than or equal to {}",
            .{transfer_batch_size_max},
        );
    }

    return .{
        .cache_accounts = benchmark.cache_accounts,
        .cache_transfers = benchmark.cache_transfers,
        .cache_transfers_pending = benchmark.cache_transfers_pending,
        .cache_grid = benchmark.cache_grid,
        .log_debug = benchmark.log_debug,
        .log_debug_replica = benchmark.log_debug_replica,
        .account_count = benchmark.account_count,
        .account_count_hot = benchmark.account_count_hot,
        .account_distribution = benchmark.account_distribution,
        .flag_history = benchmark.flag_history,
        .flag_imported = benchmark.flag_imported,
        .account_batch_size = benchmark.account_batch_size,
        .transfer_count = benchmark.transfer_count,
        .transfer_hot_percent = benchmark.transfer_hot_percent,
        .transfer_pending = benchmark.transfer_pending,
        .transfer_batch_size = benchmark.transfer_batch_size,
        .transfer_batch_delay = benchmark.transfer_batch_delay,
        .validate = benchmark.validate,
        .checksum_performance = benchmark.checksum_performance,
        .query_count = benchmark.query_count,
        .print_batch_timings = benchmark.print_batch_timings,
        .clients = benchmark.clients,
        .id_order = benchmark.id_order,
        .statsd = benchmark.statsd,
        .trace = benchmark.trace,
        .file = benchmark.file,
        .addresses = addresses,
        .seed = benchmark.seed,
    };
}

fn parse_args_inspect_integrity(args: CLIArgs.Inspect) Command.Inspect.Integrity {
    const integrity = args.integrity;

    const scrub_memory_lsm_manifest: ByteSize = integrity.memory_lsm_manifest orelse
        .{ .value = constants.lsm_manifest_memory_size_default };

    const lsm_manifest_memory = scrub_memory_lsm_manifest.bytes();
    const lsm_manifest_memory_max = constants.lsm_manifest_memory_size_max;
    const lsm_manifest_memory_min = constants.lsm_manifest_memory_size_min;
    const lsm_manifest_memory_multiplier = constants.lsm_manifest_memory_size_multiplier;
    if (lsm_manifest_memory > lsm_manifest_memory_max) {
        vsr.fatal(.cli, "--memory-lsm-manifest: size {}{s} exceeds maximum: {}", .{
            scrub_memory_lsm_manifest.value,
            scrub_memory_lsm_manifest.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(lsm_manifest_memory_max),
        });
    }
    if (lsm_manifest_memory < lsm_manifest_memory_min) {
        vsr.fatal(.cli, "--memory-lsm-manifest: size {}{s} is below minimum: {}", .{
            scrub_memory_lsm_manifest.value,
            scrub_memory_lsm_manifest.suffix(),
            vsr.stdx.fmt_int_size_bin_exact(lsm_manifest_memory_min),
        });
    }
    if (lsm_manifest_memory % lsm_manifest_memory_multiplier != 0) {
        vsr.fatal(
            .cli,
            "--memory-lsm-manifest: size {}{s} must be a multiple of {}",
            .{
                scrub_memory_lsm_manifest.value,
                scrub_memory_lsm_manifest.suffix(),
                vsr.stdx.fmt_int_size_bin_exact(lsm_manifest_memory_multiplier),
            },
        );
    }

    const lsm_forest_node_count: u32 =
        @intCast(@divExact(lsm_manifest_memory, constants.lsm_manifest_node_size));

    return .{
        .path = integrity.path,
        .log_debug = integrity.log_debug,
        .seed = integrity.seed,
        .skip_wal = integrity.skip_wal,
        .skip_client_replies = integrity.skip_client_replies,
        .skip_grid = integrity.skip_grid,
        .lsm_forest_node_count = lsm_forest_node_count,
    };
}

fn parse_args_inspect(inspect: CLIArgs.Inspect) Command.Inspect {
    const path = switch (inspect) {
        .constants => return .constants,
        .metrics => return .metrics,
        .op => |args| return .{ .op = args.op },
        .integrity => return .{ .integrity = parse_args_inspect_integrity(inspect) },
        inline else => |args| args.path,
    };

    return .{ .data_file = .{
        .path = path,
        .query = switch (inspect) {
            .constants,
            .metrics,
            .op,
            .integrity,
            => unreachable,
            .superblock => .superblock,
            .wal => |args| .{ .wal = .{ .slot = args.slot } },
            .replies => |args| .{ .replies = .{
                .slot = args.slot,
                .superblock_copy = args.superblock_copy,
            } },
            .grid => |args| .{ .grid = .{
                .block = args.block,
                .superblock_copy = args.superblock_copy,
            } },
            .manifest => |args| .{ .manifest = .{
                .superblock_copy = args.superblock_copy,
            } },
            .tables => |args| .{ .tables = .{
                .superblock_copy = args.superblock_copy,
                .tree = args.tree,
                .level = args.level,
            } },
        },
    } };
}

fn parse_args_multiversion(multiversion: CLIArgs.Multiversion) Command.Multiversion {
    return .{
        .path = multiversion.path,
        .log_debug = multiversion.log_debug,
    };
}

fn parse_args_amqp(amqp: CLIArgs.AMQP) Command.AMQP {
    const addresses = parse_addresses(amqp.addresses, "--addresses", Command.Addresses);
    const host = parse_address_and_port(
        amqp.host,
        "--host",
        vsr.cdc.amqp.tcp_port_default,
    );

    if (amqp.publish_exchange == null and amqp.publish_routing_key == null) {
        vsr.fatal(
            .cli,
            "--publish-exchange and --publish-routing-key cannot both be empty.",
            .{},
        );
    }

    if (amqp.requests_per_second_limit) |requests_per_second_limit| {
        if (requests_per_second_limit == 0) {
            vsr.fatal(
                .cli,
                "--requests-per-second-limit must not be zero.",
                .{},
            );
        }
    }

    return .{
        .addresses = addresses,
        .cluster = amqp.cluster,
        .host = host,
        .user = amqp.user,
        .password = amqp.password,
        .vhost = amqp.vhost,
        .publish_exchange = amqp.publish_exchange,
        .publish_routing_key = amqp.publish_routing_key,
        .event_count_max = amqp.event_count_max,
        .idle_interval_ms = amqp.idle_interval_ms,
        .requests_per_second_limit = amqp.requests_per_second_limit,
        .timestamp_last = amqp.timestamp_last,
        .log_debug = amqp.verbose,
    };
}

/// Parse and allocate the addresses returning a slice into that array.
fn parse_addresses(
    raw_addresses: []const u8,
    comptime flag: []const u8,
    comptime BoundedArray: type,
) BoundedArray {
    comptime assert(std.mem.startsWith(u8, flag, "--"));
    var result: BoundedArray = .{};

    const addresses_parsed = vsr.parse_addresses(
        raw_addresses,
        result.unused_capacity_slice(),
    ) catch |err| switch (err) {
        error.AddressHasTrailingComma => {
            vsr.fatal(.cli, flag ++ ": invalid trailing comma", .{});
        },
        error.AddressLimitExceeded => {
            vsr.fatal(.cli, flag ++ ": too many addresses, at most {d} are allowed", .{
                result.capacity(),
            });
        },
        error.AddressHasMoreThanOneColon => {
            vsr.fatal(.cli, flag ++ ": invalid address with more than one colon", .{});
        },
        error.PortOverflow => vsr.fatal(.cli, flag ++ ": port exceeds 65535", .{}),
        error.PortInvalid => vsr.fatal(.cli, flag ++ ": invalid port", .{}),
        error.AddressInvalid => vsr.fatal(.cli, flag ++ ": invalid IPv4 or IPv6 address", .{}),
    };
    assert(addresses_parsed.len > 0);
    assert(addresses_parsed.len <= result.capacity());
    result.resize(addresses_parsed.len) catch unreachable;
    return result;
}

fn parse_address_and_port(
    raw_address: []const u8,
    comptime flag: []const u8,
    port_default: u16,
) std.net.Address {
    comptime assert(std.mem.startsWith(u8, flag, "--"));

    const address = vsr.parse_address_and_port(.{
        .string = raw_address,
        .port_default = port_default,
    }) catch |err| switch (err) {
        error.AddressHasMoreThanOneColon => {
            vsr.fatal(.cli, flag ++ ": invalid address with more than one colon", .{});
        },
        error.PortOverflow => vsr.fatal(.cli, flag ++ ": port exceeds 65535", .{}),
        error.PortInvalid => vsr.fatal(.cli, flag ++ ": invalid port", .{}),
        error.AddressInvalid => vsr.fatal(.cli, flag ++ ": invalid IPv4 or IPv6 address", .{}),
    };
    return address;
}

/// Given a limit like `10GiB`, a SetAssociativeCache and T return the largest `value_count_max`
/// that can fit in the limit.
fn parse_cache_size_to_count(
    comptime T: type,
    comptime SetAssociativeCache: type,
    size: ByteSize,
    cli_flag: []const u8,
) u32 {
    const value_count_max_multiple = SetAssociativeCache.value_count_max_multiple;

    const count_limit = @divFloor(size.bytes(), @sizeOf(T));
    const count_rounded = @divFloor(
        count_limit,
        value_count_max_multiple,
    ) * value_count_max_multiple;

    if (count_rounded > std.math.maxInt(u32)) {
        vsr.fatal(.cli, "{s}: exceeds the limit", .{cli_flag});
    }

    const result: u32 = @intCast(count_rounded);
    assert(@as(u64, result) * @sizeOf(T) <= size.bytes());

    return result;
}

fn parse_timeout_to_ticks(timeout_ms: ?u64, cli_flag: []const u8) ?u64 {
    if (timeout_ms) |ms| {
        if (ms == 0) {
            vsr.fatal(.cli, "{s}: timeout {}ms be nonzero", .{ cli_flag, ms });
        }

        if (ms % constants.tick_ms != 0) {
            vsr.fatal(
                .cli,
                "{s}: timeout {}ms must be a multiple of {}ms",
                .{ cli_flag, ms, constants.tick_ms },
            );
        }

        return @divExact(ms, constants.tick_ms);
    } else {
        return null;
    }
}
