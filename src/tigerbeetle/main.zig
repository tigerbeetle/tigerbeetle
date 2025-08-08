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
const sigill = @import("sigill.zig");

const IO = vsr.io.IO;
const Time = vsr.time.Time;
const TimeOS = vsr.time.TimeOS;
const Tracer = vsr.trace.Tracer;
pub const Storage = vsr.storage.StorageType(IO);
const AOF = vsr.aof.AOFType(IO);

const MessageBus = vsr.message_bus.MessageBusReplica;
const MessagePool = vsr.message_pool.MessagePool;
pub const StateMachine =
    vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
pub const Grid = vsr.GridType(Storage);

const Client = vsr.ClientType(StateMachine, vsr.message_bus.MessageBusClient);
const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, AOF);
const ReplicaReformat =
    vsr.ReplicaReformatType(StateMachine, vsr.message_bus.MessageBusClient, Storage);
const data_file_size_min = vsr.superblock.data_file_size_min;

const KiB = stdx.KiB;
const MiB = stdx.MiB;
const GiB = stdx.GiB;

/// The runtime maximum log level.
/// One of: .err, .warn, .info, .debug
pub var log_level_runtime: std.log.Level = .info;

fn log_runtime(
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
    try sigill.install_handler();

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

    switch (command) {
        .start => |*args| {
            if (args.trace) |path| {
                trace_file = std.fs.cwd().createFile(path, .{ .exclusive = true }) catch |err| {
                    log.err("error creating trace file '{s}': {}", .{ path, err });
                    return err;
                };
            }
            if (args.statsd) |address| statsd_address = address;
        },
        inline else => |args| comptime {
            assert(!@hasField(args, "trace"));
            assert(!@hasField(args, "statsd"));
        },
    }

    var tracer = try Tracer.init(gpa, time, .unknown, .{
        .writer = if (trace_file) |file| file.writer().any() else null,
        .statsd_options = if (statsd_address) |address| .{ .udp = .{
            .io = &io,
            .address = address,
        } } else .log,
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
                .mode = switch (command_storage) {
                    .format, .recover => .create,
                    .start => .open,
                    else => comptime unreachable,
                },
                .direct_io = direct_io,
            });
            defer storage.deinit();

            switch (command_storage) {
                .format => try command_format(gpa, &storage, args),
                .start => try command_start(gpa, &io, &tracer, &storage, args),
                .recover => try command_reformat(gpa, &io, &tracer, &storage, args),
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

            try vsr.multiversioning.print_information(gpa, args.path, stdout);
            try stdout_buffer.flush();
        },
        .amqp => |*args| try command_amqp(gpa, time, args),
    }
}

fn command_version(gpa: mem.Allocator, verbose: bool) !void {
    var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());
    var stdout_writer = stdout_buffer.writer();
    const stdout = stdout_writer.any();

    try stdout.print("TigerBeetle version {}\n", .{constants.semver});

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
        const self_exe_path = try vsr.multiversioning.self_exe_path(gpa);
        defer gpa.free(self_exe_path);
        vsr.multiversioning.print_information(gpa, self_exe_path, stdout) catch {};
    }
    try stdout_buffer.flush();
}

fn command_format(
    gpa: mem.Allocator,
    storage: *Storage,
    args: *const cli.Command.Format,
) !void {
    vsr.format(
        Storage,
        gpa,
        .{
            .cluster = args.cluster,
            .replica = args.replica,
            .replica_count = args.replica_count,
            .release = config.process.release,
            .view = null,
        },
        .{
            .storage = storage,
            .storage_size_limit = data_file_size_min,
        },
    ) catch |err| {
        log.err("{}: error: {s}", .{ args.replica, @errorName(err) });
        storage.dangerously_delete_data_file();
        return err;
    };

    log.info("{}: formatted: cluster={} replica_count={}", .{
        args.replica,
        args.cluster,
        args.replica_count,
    });
}

fn command_start(
    base_allocator: mem.Allocator,
    io: *IO,
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

    var multiversion: ?vsr.multiversioning.Multiversion = blk: {
        if (constants.config.process.release.value ==
            vsr.multiversioning.Release.minimum.value)
        {
            log.info("multiversioning: upgrades disabled for development ({}) release.", .{
                constants.config.process.release,
            });
            break :blk null;
        }
        if (constants.aof_recovery) {
            log.info("multiversioning: upgrades disabled due to aof_recovery.", .{});
            break :blk null;
        }

        if (args.addresses_zero) {
            log.info("multiversioning: upgrades disabled due to --addresses=0", .{});
            break :blk null;
        }

        self_exe_path = try vsr.multiversioning.self_exe_path(gpa);
        break :blk try vsr.multiversioning.Multiversion.init(
            gpa,
            io,
            self_exe_path.?,
            .native,
        );
    };

    defer if (multiversion != null) multiversion.?.deinit(gpa);

    // The error from .open_sync() is ignored - timeouts and checking for new binaries are still
    // enabled even if the first version fails to load.
    if (multiversion != null) multiversion.?.open_sync() catch {};

    var releases_bundled_baseline: vsr.multiversioning.ReleaseList = .{};
    releases_bundled_baseline.push(constants.config.process.release);

    const releases_bundled = if (multiversion != null)
        &multiversion.?.releases_bundled
    else
        &releases_bundled_baseline;

    log.info("release={}", .{config.process.release});
    log.info("release_client_min={}", .{config.process.release_client_min});
    log.info("releases_bundled={any}", .{releases_bundled.const_slice()});
    log.info("git_commit={?s}", .{config.process.git_commit});

    const clients_limit = constants.pipeline_prepare_queue_max + args.pipeline_requests_limit;

    var replica: Replica = undefined;
    replica.open(gpa, .{
        .node_count = args.addresses.count_as(u8),
        .release = config.process.release,
        .release_client_min = config.process.release_client_min,
        .releases_bundled = releases_bundled,
        .release_execute = replica_release_execute,
        .release_execute_context = if (multiversion) |*pointer| pointer else null,
        .pipeline_requests_limit = args.pipeline_requests_limit,
        .storage_size_limit = args.storage_size_limit,
        .storage = storage,
        .aof = if (aof != null) &aof.? else null,
        .message_pool = &message_pool,
        .nonce = nonce,
        .time = tracer.time,
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
        },
        .message_bus_options = .{
            .configuration = args.addresses.const_slice(),
            .io = io,
            .clients_limit = clients_limit,
        },
        .grid_cache_blocks_count = args.cache_grid_blocks,
        .tracer = tracer,
        .replicate_options = .{
            .closed_loop = args.replicate_closed_loop,
            .star = args.replicate_star,
        },
    }) catch |err| switch (err) {
        error.NoAddress => vsr.fatal(.cli, "all --addresses must be provided", .{}),
        else => |e| return e,
    };

    if (multiversion != null) {
        if (args.development) {
            log.info("multiversioning: upgrade polling disabled due to --development.", .{});
        } else {
            multiversion.?.timeout_start(replica.replica);
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
        replica.message_bus.process.accept_address,
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
        const port_actual = replica.message_bus.process.accept_address.getPort();
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
        if (multiversion != null) multiversion.?.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

fn command_reformat(
    gpa: mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    storage: *Storage,
    args: *const cli.Command.Recover,
) !void {
    var message_pool = try MessagePool.init(gpa, .client);
    defer message_pool.deinit(gpa);

    var client = try Client.init(gpa, .{
        .id = stdx.unique_u128(),
        .cluster = args.cluster,
        .replica_count = args.replica_count,
        .time = tracer.time,
        .message_pool = &message_pool,
        .message_bus_options = .{
            .configuration = args.addresses.const_slice(),
            .io = io,
            .clients_limit = null,
        },
        .eviction_callback = &command_reformat_client_eviction_callback,
    });
    defer client.deinit(gpa);

    var reformatter = try ReplicaReformat.init(gpa, &client, .{
        .format = .{
            .cluster = args.cluster,
            .replica = args.replica,
            .replica_count = args.replica_count,
            .release = config.process.release,
            .view = null,
        },
        .superblock = .{
            .storage = storage,
            .storage_size_limit = data_file_size_min,
        },
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
            storage.dangerously_delete_data_file();
            return err;
        },
        .ok => log.info("{}: success", .{args.replica}),
    }
}

fn command_reformat_client_eviction_callback(
    _: *Client,
    eviction: *const MessagePool.Message.Eviction,
) void {
    std.debug.panic("error: client evicted: {s}", .{@tagName(eviction.header.reason)});
}

fn command_repl(
    gpa: mem.Allocator,
    io: *IO,
    time: Time,
    args: *const cli.Command.Repl,
) !void {
    const Repl = vsr.repl.ReplType(vsr.message_bus.MessageBusClient);

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

fn replica_release_execute(replica: *Replica, release: vsr.Release) noreturn {
    assert(release.value != replica.release.value);
    assert(release.value != vsr.Release.zero.value);
    assert(release.value != vsr.Release.minimum.value);
    const release_execute_context = replica.release_execute_context orelse {
        @panic("replica_release_execute unsupported");
    };

    const multiversion: *vsr.multiversioning.Multiversion =
        @ptrCast(@alignCast(release_execute_context));

    for (replica.releases_bundled.const_slice()) |release_bundled| {
        if (release_bundled.value == release.value) break;
    } else {
        log.err("{}: release_execute: release {} is not available;" ++
            " upgrade (or downgrade) the binary", .{
            replica.replica,
            release,
        });
        @panic("release not available");
    }

    if (builtin.os.tag == .windows) {
        // Unlike on Linux / macOS which use `execve{at,z}` for multiversion binaries,
        // Windows has to use CreateProcess. This is a problem, because it's a race between
        // the parent process exiting and the new process starting. Work around this by
        // deinit'ing Replica and storage before continuing.
        // We don't need to clean up all resources here, since the process will be terminated
        // in any case; only the resources that would block a new process from starting up.
        const storage = replica.superblock.storage;
        const fd = storage.fd;
        replica.deinit(replica.static_allocator.parent_allocator);
        storage.deinit();

        // FD is managed by Command, normally. Shut it down explicitly.
        std.posix.close(fd);
    }

    // We have two paths here, depending on if we're upgrading or downgrading. If we're downgrading
    // the invariant is that this code is running _before_ we've finished opening, that is,
    // release_transition is called in open().
    if (release.value < replica.release.value) {
        multiversion.exec_release(
            release,
        ) catch |err| {
            std.debug.panic("failed to execute previous release: {}", .{err});
        };
    } else {
        // For the upgrade case, re-run the latest binary in place. If we need something older
        // than the latest, that'll be handled when the case above is hit when re-execing:
        // (current version v1) -> (latest version v4) -> (desired version v2)
        multiversion.exec_current(release) catch |err| {
            std.debug.panic("failed to execute latest release: {}", .{err});
        };
    }

    unreachable;
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
