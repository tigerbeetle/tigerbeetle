const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log = std.log.scoped(.main);

const vsr = @import("vsr");
const stdx = vsr.stdx;
const constants = vsr.constants;
const config = constants.config;

const benchmark_driver = @import("benchmark_driver.zig");
const cli = @import("cli.zig");
const inspect = @import("inspect.zig");

const IO = vsr.io.IO;
const Time = vsr.time.Time;
const TimeOS = vsr.time.TimeOS;
const Tracer = vsr.trace.Tracer;
pub const Storage = vsr.storage.StorageType(IO);
const AOF = vsr.aof.AOFType(IO);

const MessageBus = vsr.message_bus.MessageBusType(IO);
const MessagePool = vsr.message_pool.MessagePool;
pub const StateMachine = vsr.state_machine.StateMachineType(Storage);
pub const Grid = vsr.GridType(Storage);

const Client = vsr.ClientType(StateMachine.Operation, MessageBus);
pub const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, AOF);
const ReplicaReformat =
    vsr.ReplicaReformatType(StateMachine, MessageBus, Storage);
const data_file_size_min = vsr.superblock.data_file_size_min;

const KiB = stdx.KiB;
const MiB = stdx.MiB;
const GiB = stdx.GiB;

/// The runtime maximum log level.
/// One of: .err, .warn, .info, .debug
pub var log_level_runtime: std.log.Level = .info;

pub fn log_runtime(
    comptime message_level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    // A microbenchmark places the cost of this if at somewhere around 1600us for 10 million calls.
    if (@intFromEnum(message_level) <= @intFromEnum(log_level_runtime)) {
        stdx.log_with_timestamp(message_level, scope, format, args);
    }
}

pub const std_options: std.Options = .{
    // The comptime log_level. This needs to be debug - otherwise messages are compiled out.
    // The runtime filtering is handled by log_level_runtime.
    .log_level = .debug,
    .logFn = log_runtime,
};

pub fn main() !void {
    if (builtin.os.tag == .windows) try vsr.multiversion.wait_for_parent_to_exit();

    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_instance.deinit();

    // Arena is an implementation detail, all memory must be freed.
    const gpa = arena_instance.allocator();

    var arg_iterator = try std.process.argsWithAllocator(gpa);
    defer arg_iterator.deinit();

    var command = cli.parse_args(&arg_iterator);

    if (command == .version) {
        try command_version(gpa, command.version.verbose);
        return; // Exit early before initializing IO.
    }

    log_level_runtime = switch (command) {
        .version => unreachable,
        .inspect => .info,
        inline else => |*args| if (args.log_debug) .debug else .info,
    };

    // Try and init IO early, before a file has even been created, so if it fails (eg, io_uring
    // is not available) there won't be a dangling file.
    var io = try IO.init(128, 0);
    defer io.deinit();

    var time_os: TimeOS = .{};
    const time = time_os.time();

    var trace_file: ?std.fs.File = null;
    defer if (trace_file) |file| file.close();

    var statsd_address: ?std.net.Address = null;
    var log_trace = true;

    switch (command) {
        .start => |*args| {
            if (args.trace) |path| {
                trace_file = std.fs.cwd().createFile(path, .{ .exclusive = true }) catch |err| {
                    log.err("error creating trace file '{s}': {}", .{ path, err });
                    return err;
                };
            }
            if (args.statsd) |address| statsd_address = address;
            log_trace = args.log_trace;
        },
        .benchmark => {}, // Forwards trace and statsd argument to child tigerbeetle.
        inline else => |args| comptime {
            assert(!@hasField(@TypeOf(args), "trace"));
            assert(!@hasField(@TypeOf(args), "statsd"));
        },
    }

    var tracer = try Tracer.init(gpa, time, .unknown, .{
        .writer = if (trace_file) |file| file.writer().any() else null,
        .statsd_options = if (statsd_address) |address| .{
            .udp = .{
                .io = &io,
                .address = address,
            },
        } else .log,
        .log_trace = log_trace,
    });
    defer tracer.deinit(gpa);

    switch (command) {
        .version => unreachable, // Handled earlier.
        inline .format, .start, .recover => |*args, command_storage| {
            const direct_io: vsr.io.DirectIO =
                if (!constants.direct_io)
                    .direct_io_disabled
                else if (args.development)
                    .direct_io_optional
                else
                    .direct_io_required;

            var storage = try Storage.init(&io, &tracer, .{
                .path = args.path,
                .size_min = data_file_size_min,
                .purpose = switch (command_storage) {
                    .format, .recover => .format,
                    .start => .open,
                    else => comptime unreachable,
                },
                .direct_io = direct_io,
            });
            defer storage.deinit();

            switch (command_storage) {
                .format => try command_format(gpa, &storage, args),
                .start => try command_start(gpa, &io, time, &tracer, &storage, args),
                .recover => try command_reformat(gpa, &io, time, &storage, args),
                else => comptime unreachable,
            }
        },
        .repl => |*args| try command_repl(gpa, &io, time, args),
        .benchmark => |*args| try benchmark_driver.command_benchmark(gpa, &io, time, args),
        .inspect => |*args| try inspect.command_inspect(gpa, &io, &tracer, args),
        .multiversion => |*args| {
            var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());
            var stdout_writer = stdout_buffer.writer();
            const stdout = stdout_writer.any();

            try vsr.multiversion.print_information(gpa, args.path, stdout);
            try stdout_buffer.flush();
        },
        .amqp => |*args| try command_amqp(gpa, time, args),
    }
}

fn command_version(gpa: mem.Allocator, verbose: bool) !void {
    var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());
    var stdout_writer = stdout_buffer.writer();
    const stdout = stdout_writer.any();

    try std.fmt.format(stdout, "TigerBeetle version {}\n", .{constants.semver});

    if (verbose) {
        try stdout.writeAll("\n");
        inline for (.{ "mode", "zig_version" }) |declaration| {
            try print_value(stdout, "build." ++ declaration, @field(builtin, declaration));
        }

        // Zig 0.10 doesn't see field_name as comptime if this `comptime` isn't used.
        try stdout.writeAll("\n");
        inline for (comptime std.meta.fieldNames(@TypeOf(config.cluster))) |field_name| {
            try print_value(
                stdout,
                "cluster." ++ field_name,
                @field(config.cluster, field_name),
            );
        }

        try stdout.writeAll("\n");
        inline for (comptime std.meta.fieldNames(@TypeOf(config.process))) |field_name| {
            try print_value(
                stdout,
                "process." ++ field_name,
                @field(config.process, field_name),
            );
        }

        try stdout.writeAll("\n");
        const self_exe_path = try vsr.multiversion.self_exe_path(gpa);
        defer gpa.free(self_exe_path);

        vsr.multiversion.print_information(gpa, self_exe_path, stdout) catch {};
    }
    try stdout_buffer.flush();
}

fn command_format(
    gpa: mem.Allocator,
    storage: *Storage,
    args: *const cli.Command.Format,
) !void {
    try vsr.format(Storage, gpa, storage, .{
        .cluster = args.cluster,
        .replica = args.replica,
        .replica_count = args.replica_count,
        .release = config.process.release,
        .view = null,
    });

    log.info("{}: formatted: cluster={} replica_count={}", .{
        args.replica,
        args.cluster,
        args.replica_count,
    });
}

fn command_start(
    base_allocator: mem.Allocator,
    io: *IO,
    time: vsr.time.Time,
    tracer: *Tracer,
    storage: *Storage,
    args: *const cli.Command.Start,
) !void {
    var counting_allocator = vsr.CountingAllocator.init(base_allocator);
    const gpa = counting_allocator.allocator();

    // TODO Panic if the data file's size is larger that args.storage_size_limit.
    // (Here or in Replica.open()?).

    var message_pool = try MessagePool.init(gpa, .{ .replica = .{
        .members_count = args.addresses.count_as(u8),
        .pipeline_requests_limit = args.pipeline_requests_limit,
        .message_bus = .tcp,
    } });
    defer message_pool.deinit(gpa);

    var aof: ?AOF = if (args.aof_file) |*aof_file| blk: {
        const aof_dir = std.fs.path.dirname(aof_file.const_slice()) orelse ".";
        const aof_dir_fd = try IO.open_dir(aof_dir);
        defer std.posix.close(aof_dir_fd);

        break :blk try AOF.init(io, .{
            .dir_fd = aof_dir_fd,
            .relative_path = std.fs.path.basename(aof_file.const_slice()),
        });
    } else null;
    defer if (aof != null) aof.?.close();

    const grid_cache_size = @as(u64, args.cache_grid_blocks) * constants.block_size;
    const grid_cache_size_min = constants.block_size * Grid.Cache.value_count_max_multiple;

    // The amount of bytes in `--cache-grid` must be a multiple of
    // `constants.block_size` and `SetAssociativeCache.value_count_max_multiple`,
    // and it may have been converted to zero if a smaller value is passed in.
    if (grid_cache_size == 0) {
        if (comptime (grid_cache_size_min >= MiB)) {
            vsr.fatal(.cli, "Grid cache must be greater than {}MiB. See --cache-grid", .{
                @divExact(grid_cache_size_min, MiB),
            });
        } else {
            vsr.fatal(.cli, "Grid cache must be greater than {}KiB. See --cache-grid", .{
                @divExact(grid_cache_size_min, KiB),
            });
        }
    }
    assert(grid_cache_size >= grid_cache_size_min);

    const grid_cache_size_warn = 1 * GiB;
    if (grid_cache_size < grid_cache_size_warn) {
        log.warn("Grid cache size of {}MiB is small. See --cache-grid", .{
            @divExact(grid_cache_size, MiB),
        });
    }

    const nonce = stdx.unique_u128();

    var self_exe_path: ?[:0]const u8 = null;
    defer if (self_exe_path) |path| gpa.free(path);

    var multiversion_os: ?vsr.multiversion.MultiversionOS = null;
    defer if (multiversion_os != null) multiversion_os.?.deinit(gpa);

    const multiversion: vsr.multiversion.Multiversion = blk: {
        if (constants.config.process.release.value ==
            vsr.multiversion.Release.minimum.value)
        {
            log.info("multiversioning: upgrades disabled for development ({}) release.", .{
                constants.config.process.release,
            });
            break :blk .single_release(constants.config.process.release);
        }
        if (constants.aof_recovery) {
            log.info("multiversioning: upgrades disabled due to aof_recovery.", .{});
            break :blk .single_release(constants.config.process.release);
        }

        if (args.addresses_zero) {
            log.info("multiversioning: upgrades disabled due to --addresses=0", .{});
            break :blk .single_release(constants.config.process.release);
        }

        self_exe_path = try vsr.multiversion.self_exe_path(gpa);
        multiversion_os = try vsr.multiversion.MultiversionOS.init(
            gpa,
            io,
            self_exe_path.?,
            .native,
        );
        // The error from .open_sync() is ignored - timeouts and checking for new binaries are still
        // enabled even if the first version fails to load.
        multiversion_os.?.open_sync() catch {};

        break :blk multiversion_os.?.multiversion();
    };

    log.info("release={}", .{config.process.release});
    log.info("release_client_min={}", .{config.process.release_client_min});
    log.info("releases_bundled={any}", .{multiversion.releases_bundled().slice()});
    log.info("git_commit={?s}", .{config.process.git_commit});

    const clients_limit = constants.pipeline_prepare_queue_max + args.pipeline_requests_limit;

    var replica: Replica = undefined;
    replica.open(
        gpa,
        time,
        storage,
        &message_pool,
        .{
            .node_count = args.addresses.count_as(u8),
            .release = config.process.release,
            .release_client_min = config.process.release_client_min,
            .multiversion = multiversion,
            .pipeline_requests_limit = args.pipeline_requests_limit,
            .storage_size_limit = args.storage_size_limit,
            .aof = if (aof != null) &aof.? else null,
            .nonce = nonce,
            .timeout_prepare_ticks = args.timeout_prepare_ticks,
            .timeout_grid_repair_message_ticks = args.timeout_grid_repair_message_ticks,
            .commit_stall_probability = args.commit_stall_probability,
            .state_machine_options = .{
                .batch_size_limit = args.request_size_limit - @sizeOf(vsr.Header),
                .lsm_forest_compaction_block_count = args.lsm_forest_compaction_block_count,
                .lsm_forest_node_count = args.lsm_forest_node_count,
                .cache_entries_accounts = args.cache_accounts,
                .cache_entries_transfers = args.cache_transfers,
                .cache_entries_transfers_pending = args.cache_transfers_pending,
                .log_trace = args.log_trace,
            },
            .message_bus_options = .{
                .configuration = args.addresses.const_slice(),
                .io = io,
                .clients_limit = clients_limit,
            },
            .grid_cache_blocks_count = args.cache_grid_blocks,
            .tracer = tracer,
            .replicate_options = .{
                .star = args.replicate_star,
            },
        },
    ) catch |err| switch (err) {
        error.NoAddress => vsr.fatal(.cli, "all --addresses must be provided", .{}),
        else => |e| return e,
    };

    // Mark grid cache as MADV_DONTDUMP, after transitioning to static in replica.open, to reduce
    // core dump size.
    replica.grid.madv_dont_dump() catch |e| {
        log.warn("unable to mark grid cache as MADV_DONTDUMP - " ++
            "core dumps will be large: {}", .{e});
    };

    if (multiversion_os != null) {
        if (args.development) {
            log.info("multiversioning: upgrade polling disabled due to --development.", .{});
        } else {
            multiversion_os.?.timeout_start(replica.replica);
        }

        if (args.experimental) {
            log.warn("multiversioning: upgrade polling and --experimental enabled - " ++
                "make sure to check CLI argument compatibility before upgrading.", .{});
            log.warn("If the cluster upgrades automatically, and incompatible experimental " ++
                "CLI arguments are set, it will crash.", .{});
        }
    }

    // Note that this does not account for the fact that any allocations will be rounded up to
    // the nearest page by `std.heap.page_allocator`.
    log.info("{}: Allocated {}MiB during replica init", .{
        replica.replica,
        @divFloor(counting_allocator.live_size(), MiB),
    });
    log.info("{}: Grid cache: {}MiB, LSM-tree manifests: {}MiB", .{
        replica.replica,
        @divFloor(grid_cache_size, MiB),
        @divFloor(args.lsm_forest_node_count * constants.lsm_manifest_node_size, MiB),
    });

    log.info("{}: cluster={}: listening on {}", .{
        replica.replica,
        replica.cluster,
        replica.message_bus.accept_address.?,
    });

    if (constants.aof_recovery) {
        log.warn(
            "{}: started in AOF recovery mode. This is potentially dangerous - if it's" ++
                " unexpected, please recompile TigerBeetle with -Dconfig-aof-recovery=false.",
            .{replica.replica},
        );
    }

    if (constants.verify) {
        log.info("{}: started with extra verification checks", .{replica.replica});
    }

    if (replica.aof != null) {
        log.warn(
            "{}: started with --aof - expect much reduced performance.",
            .{replica.replica},
        );
    }

    // It is possible to start tigerbeetle passing `0` as an address:
    //     $ tigerbeetle start --addresses=0 0_0.tigerbeetle
    // This enables a couple of special behaviors, useful in tests:
    // - The operating system picks a free port, avoiding "address already in use" errors.
    // - The port, and only the port, is printed to the stdout, so that the parent process
    //   can learn it.
    // - tigerbeetle process exits when its stdin gets closed.
    if (args.addresses_zero) {
        const port_actual = replica.message_bus.accept_address.?.getPort();
        const stdout = std.io.getStdOut();
        try stdout.writer().print("{}\n", .{port_actual});
        stdout.close();

        // While it is possible to integrate stdin with our io_uring loop, using a dedicated
        // thread is simpler, and gives us _un_graceful shutdown, which is exactly what we want
        // to keep behavior close to the normal case.
        const watchdog = try std.Thread.spawn(.{}, struct {
            fn thread_main() void {
                var buf: [1]u8 = .{0};
                _ = std.io.getStdIn().read(&buf) catch {};
                log.info("stdin closed, exiting", .{});
                std.process.exit(0);
            }
        }.thread_main, .{});
        watchdog.detach();
    }

    if (!args.development) {
        // Try to lock all memory in the process to avoid the kernel swapping pages to disk and
        // potentially introducing undetectable disk corruption into memory.
        // This is a best-effort attempt and not a hard rule as it may not cover all memory edge
        // case. So warn on error to notify the operator to adjust conditions if possible.
        stdx.memory_lock_allocated(.{
            .allocated_size = counting_allocator.live_size(),
        }) catch {
            log.warn(
                "If this is a production replica, consider either " ++
                    "running the replica with CAP_IPC_LOCK privilege, " ++
                    "increasing the MEMLOCK process limit, " ++
                    "or disabling swap system-wide.",
                .{},
            );
        };

        if (replica.cluster == 0) {
            log.warn("a cluster id of 0 is reserved for testing and benchmarking, " ++
                "do not use in production", .{});
        }
    }

    while (true) {
        replica.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

fn command_reformat(
    gpa: mem.Allocator,
    io: *IO,
    time: vsr.time.Time,
    storage: *Storage,
    args: *const cli.Command.Recover,
) !void {
    var message_pool = try MessagePool.init(gpa, .client);
    defer message_pool.deinit(gpa);

    var client = try Client.init(
        gpa,
        time,
        &message_pool,
        .{
            .id = stdx.unique_u128(),
            .cluster = args.cluster,
            .replica_count = args.replica_count,

            .message_bus_options = .{
                .configuration = args.addresses.const_slice(),
                .io = io,
                .clients_limit = null,
            },
            .eviction_callback = &reformat_client_eviction_callback,
        },
    );
    defer client.deinit(gpa);

    var reformatter = try ReplicaReformat.init(gpa, &client, storage, .{
        .cluster = args.cluster,
        .replica = args.replica,
        .replica_count = args.replica_count,
        .release = config.process.release,
        .view = null,
    });
    defer reformatter.deinit(gpa);

    reformatter.start();
    while (reformatter.done() == null) {
        client.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    switch (reformatter.done().?) {
        .failed => |err| {
            log.err("{}: error: {s}", .{ args.replica, @errorName(err) });
            return err;
        },
        .ok => log.info("{}: success", .{args.replica}),
    }
}

fn reformat_client_eviction_callback(
    client: *Client,
    eviction: *const MessagePool.Message.Eviction,
) void {
    _ = client;
    std.debug.panic("error: client evicted: {s}", .{@tagName(eviction.header.reason)});
}

fn command_repl(
    gpa: mem.Allocator,
    io: *IO,
    time: Time,
    args: *const cli.Command.Repl,
) !void {
    const Repl = vsr.repl.ReplType(vsr.message_bus.MessageBusType(IO));

    var repl_instance = try Repl.init(gpa, io, time, .{
        .cluster_id = args.cluster,
        .addresses = args.addresses.const_slice(),
        .verbose = args.verbose,
    });
    defer repl_instance.deinit(gpa);

    try repl_instance.run(args.statements);
}

fn command_amqp(gpa: mem.Allocator, time: Time, args: *const cli.Command.AMQP) !void {
    var runner: vsr.cdc.Runner = undefined;
    try runner.init(
        gpa,
        time,
        .{
            .cluster_id = args.cluster,
            .addresses = args.addresses.const_slice(),
            .host = args.host,
            .user = args.user,
            .password = args.password,
            .vhost = args.vhost,
            .publish_exchange = args.publish_exchange,
            .publish_routing_key = args.publish_routing_key,
            .event_count_max = args.event_count_max,
            .idle_interval_ms = args.idle_interval_ms,
            .requests_per_second_limit = args.requests_per_second_limit,
            .recovery_mode = if (args.timestamp_last) |timestamp_last|
                .{ .override = timestamp_last }
            else
                .recover,
        },
    );
    defer runner.deinit();

    while (true) {
        runner.tick();
    }
}

fn print_value(
    writer: anytype,
    field: []const u8,
    value: anytype,
) !void {
    if (@TypeOf(value) == ?[40]u8) {
        assert(std.mem.eql(u8, field, "process.git_commit"));
        return std.fmt.format(writer, "{s}=\"{?s}\"\n", .{
            field,
            value,
        });
    }

    switch (@typeInfo(@TypeOf(value))) {
        .@"fn" => {}, // Ignore the log() function.
        .pointer => try std.fmt.format(writer, "{s}=\"{s}\"\n", .{
            field,
            std.fmt.fmtSliceEscapeLower(value),
        }),
        else => try std.fmt.format(writer, "{s}={any}\n", .{
            field,
            value,
        }),
    }
}
