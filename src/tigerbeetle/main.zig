const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log = std.log.scoped(.main);

const vsr = @import("vsr");
const constants = vsr.constants;
const config = constants.config;
const tracer = vsr.tracer;

const benchmark_driver = @import("benchmark_driver.zig");
const cli = @import("cli.zig");
const fatal = vsr.flags.fatal;
const inspect = @import("inspect.zig");

const IO = vsr.io.IO;
const Time = vsr.time.Time;
const Storage = vsr.storage.Storage(IO);
const AOF = vsr.aof.AOF;

const MessageBus = vsr.message_bus.MessageBusReplica;
const MessagePool = vsr.message_pool.MessagePool;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Grid = vsr.GridType(Storage);

const AOFType = if (constants.aof_record) AOF else void;
const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, Time, AOFType);
const SuperBlock = vsr.SuperBlockType(Storage);
const superblock_zone_size = vsr.superblock.superblock_zone_size;
const data_file_size_min = vsr.superblock.data_file_size_min;

pub const std_options = .{
    .log_level = constants.log_level,
    .logFn = constants.log,
};

pub fn main() !void {
    try SigIllHandler.register();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    try tracer.init(allocator);
    defer tracer.deinit(allocator);

    var arg_iterator = try std.process.argsWithAllocator(allocator);
    defer arg_iterator.deinit();

    var command = cli.parse_args(&arg_iterator);

    switch (command) {
        .format => |*args| try Command.format(allocator, args, .{
            .cluster = args.cluster,
            .replica = args.replica,
            .replica_count = args.replica_count,
            .release = config.process.release,
        }),
        .start => |*args| try Command.start(arena.allocator(), args),
        .version => |*args| try Command.version(allocator, args.verbose),
        .repl => |*args| try Command.repl(&arena, args),
        .benchmark => |*args| try benchmark_driver.main(allocator, args),
        .inspect => |*args| inspect.main(allocator, args) catch |err| {
            // Ignore BrokenPipe so that e.g. "tigerbeetle inspect ... | head -n12" succeeds.
            if (err != error.BrokenPipe) return err;
        },
        .multiversion => |*args| {
            var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());
            var stdout_writer = stdout_buffer.writer();
            const stdout = stdout_writer.any();

            try vsr.multiversioning.print_information(allocator, args.path, stdout);
            try stdout_buffer.flush();
        },
    }
}

const SigIllHandler = struct {
    var original_posix_sigill_handler: ?*const fn (
        i32,
        *const std.posix.siginfo_t,
        ?*anyopaque,
    ) callconv(.C) void = null;

    fn handle_sigill_windows(
        info: *std.os.windows.EXCEPTION_POINTERS,
    ) callconv(std.os.windows.WINAPI) c_long {
        if (info.ExceptionRecord.ExceptionCode == std.os.windows.EXCEPTION_ILLEGAL_INSTRUCTION) {
            display_message();
        }
        return std.os.windows.EXCEPTION_CONTINUE_SEARCH;
    }

    fn handle_sigill_posix(
        sig: i32,
        info: *const std.posix.siginfo_t,
        ctx_ptr: ?*anyopaque,
    ) callconv(.C) noreturn {
        display_message();
        original_posix_sigill_handler.?(sig, info, ctx_ptr);
        unreachable;
    }

    fn display_message() void {
        std.log.err("", .{});
        std.log.err("TigerBeetle's binary releases are compiled targeting modern CPU", .{});
        std.log.err("instructions such as NEON / AES on ARM and x86_64_v3 / AES-NI on", .{});
        std.log.err("x86-64.", .{});
        std.log.err("", .{});
        std.log.err("These instructions can be unsupported on older processors, leading to", .{});
        std.log.err("\"illegal instruction\" panics.", .{});
        std.log.err("", .{});
        std.log.err("If you'd like to try TigerBeetle on an older processor, you can", .{});
        std.log.err("compile from source with the changes detailed at", .{});
        std.log.err("https://github.com/tigerbeetle/tigerbeetle/issues/1592", .{});
        std.log.err("", .{});
    }

    fn register() !void {
        switch (builtin.os.tag) {
            .windows => {
                // The 1 indicates this will run first, before Zig's built in handler. Internally,
                // it returns EXCEPTION_CONTINUE_SEARCH so that Zig's handler is called next.
                _ = std.os.windows.kernel32.AddVectoredExceptionHandler(1, handle_sigill_windows);
            },
            .linux, .macos => {
                // For Linux / macOS, save the original signal handler so it can be called by this
                // new handler once the log message has been printed.
                assert(original_posix_sigill_handler == null);
                var act = std.posix.Sigaction{
                    .handler = .{ .sigaction = handle_sigill_posix },
                    .mask = std.posix.empty_sigset,
                    .flags = (std.posix.SA.SIGINFO | std.posix.SA.RESTART | std.posix.SA.RESETHAND),
                };

                var oact: std.posix.Sigaction = undefined;

                try std.posix.sigaction(std.posix.SIG.ILL, &act, &oact);
                original_posix_sigill_handler = oact.handler.sigaction.?;
            },
            else => unreachable,
        }
    }
};

const Command = struct {
    dir_fd: std.posix.fd_t,
    fd: std.posix.fd_t,
    io: IO,
    storage: Storage,
    self_exe_path: [:0]const u8,

    fn init(
        command: *Command,
        allocator: mem.Allocator,
        path: [:0]const u8,
        options: struct {
            must_create: bool,
            development: bool,
        },
    ) !void {
        // TODO Resolve the parent directory properly in the presence of .. and symlinks.
        // TODO Handle physical volumes where there is no directory to fsync.
        const dirname = std.fs.path.dirname(path) orelse ".";
        command.dir_fd = try IO.open_dir(dirname);
        errdefer std.posix.close(command.dir_fd);

        const direct_io: vsr.io.DirectIO = if (!constants.direct_io)
            .direct_io_disabled
        else if (options.development)
            .direct_io_optional
        else
            .direct_io_required;

        const basename = std.fs.path.basename(path);
        command.fd = try IO.open_file(
            command.dir_fd,
            basename,
            data_file_size_min,
            if (options.must_create) .create else .open,
            direct_io,
        );
        errdefer std.posix.close(command.fd);

        command.io = try IO.init(128, 0);
        errdefer command.io.deinit();

        command.storage = try Storage.init(&command.io, command.fd);
        errdefer command.storage.deinit();

        command.self_exe_path = try vsr.multiversioning.self_exe_path(allocator);
        errdefer allocator.free(command.self_exe_path);
    }

    fn deinit(command: *Command, allocator: mem.Allocator) void {
        allocator.free(command.self_exe_path);
        command.storage.deinit();
        command.io.deinit();
        std.posix.close(command.fd);
        std.posix.close(command.dir_fd);
    }

    pub fn format(
        allocator: mem.Allocator,
        args: *const cli.Command.Format,
        options: SuperBlock.FormatOptions,
    ) !void {
        var command: Command = undefined;
        try command.init(allocator, args.path, .{
            .must_create = true,
            .development = args.development,
        });
        defer command.deinit(allocator);

        var superblock = try SuperBlock.init(
            allocator,
            .{
                .storage = &command.storage,
                .storage_size_limit = data_file_size_min,
            },
        );
        defer superblock.deinit(allocator);

        try vsr.format(Storage, allocator, options, &command.storage, &superblock);

        log.info("{}: formatted: cluster={} replica_count={}", .{
            options.replica,
            options.cluster,
            options.replica_count,
        });
    }

    pub fn start(base_allocator: std.mem.Allocator, args: *const cli.Command.Start) !void {
        var counting_allocator = vsr.CountingAllocator.init(base_allocator);

        var traced_allocator = if (constants.tracer_backend == .tracy)
            tracer.TracyAllocator("tracy").init(counting_allocator.allocator())
        else
            &counting_allocator;

        // TODO Panic if the data file's size is larger that args.storage_size_limit.
        // (Here or in Replica.open()?).

        const allocator = traced_allocator.allocator();

        var command: Command = undefined;
        try command.init(allocator, args.path, .{
            .must_create = false,
            .development = args.development,
        });
        defer command.deinit(allocator);

        var message_pool = try MessagePool.init(allocator, .{ .replica = .{
            .members_count = args.addresses.count_as(u8),
            .pipeline_requests_limit = args.pipeline_requests_limit,
        } });
        defer message_pool.deinit(allocator);

        var aof: AOFType = undefined;
        if (constants.aof_record) {
            const aof_path = try std.fmt.allocPrint(allocator, "{s}.aof", .{args.path});
            defer allocator.free(aof_path);

            aof = try AOF.from_absolute_path(aof_path);
        }

        const grid_cache_size = @as(u64, args.cache_grid_blocks) * constants.block_size;
        const grid_cache_size_min = constants.block_size * Grid.Cache.value_count_max_multiple;

        // The amount of bytes in `--cache-grid` must be a multiple of
        // `constants.block_size` and `SetAssociativeCache.value_count_max_multiple`,
        // and it may have been converted to zero if a smaller value is passed in.
        if (grid_cache_size == 0) {
            if (comptime (grid_cache_size_min >= 1024 * 1024)) {
                fatal("Grid cache must be greater than {}MiB. See --cache-grid", .{
                    @divExact(grid_cache_size_min, 1024 * 1024),
                });
            } else {
                fatal("Grid cache must be greater than {}KiB. See --cache-grid", .{
                    @divExact(grid_cache_size_min, 1024),
                });
            }
        }
        assert(grid_cache_size >= grid_cache_size_min);

        const grid_cache_size_warn = 1024 * 1024 * 1024;
        if (grid_cache_size < grid_cache_size_warn) {
            log.warn("Grid cache size of {}MiB is small. See --cache-grid", .{
                @divExact(grid_cache_size, 1024 * 1024),
            });
        }

        const nonce = std.crypto.random.int(u128);
        assert(nonce != 0); // Broken CSPRNG is the likeliest explanation for zero.

        var multiversion: ?vsr.multiversioning.Multiversion =
            if (constants.config.process.release.value ==
            vsr.multiversioning.Release.minimum.value)
        blk: {
            log.info(
                "multiversioning: disabled for development ({}) release.",
                .{constants.config.process.release},
            );
            break :blk null;
        } else if (args.development) blk: {
            log.info("multiversioning: disabled due to --development.", .{});
            break :blk null;
        } else if (args.experimental) blk: {
            log.info("multiversioning: disabled due to --experimental.", .{});
            break :blk null;
        } else if (constants.aof_recovery) blk: {
            log.info("multiversioning: disabled due to aof_recovery.", .{});
            break :blk null;
        } else try vsr.multiversioning.Multiversion.init(
            allocator,
            &command.io,
            command.self_exe_path,
            .native,
        );

        defer if (multiversion != null) multiversion.?.deinit(allocator);

        // The error from .open_sync() is ignored - timeouts and checking for new binaries are still
        // enabled even if the first version fails to load.
        if (multiversion != null) multiversion.?.open_sync() catch {};

        var releases_bundled_baseline: vsr.multiversioning.ReleaseList = .{};
        releases_bundled_baseline.append_assume_capacity(constants.config.process.release);

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
        replica.open(allocator, .{
            .node_count = args.addresses.count_as(u8),
            .release = config.process.release,
            .release_client_min = config.process.release_client_min,
            .releases_bundled = releases_bundled,
            .release_execute = replica_release_execute,
            .multiversion = if (multiversion) |*multiversion_| multiversion_ else null,
            .pipeline_requests_limit = args.pipeline_requests_limit,
            .storage_size_limit = args.storage_size_limit,
            .storage = &command.storage,
            .aof = &aof,
            .message_pool = &message_pool,
            .nonce = nonce,
            .time = .{},
            .state_machine_options = .{
                .batch_size_limit = args.request_size_limit - @sizeOf(vsr.Header),
                .lsm_forest_compaction_block_count = args.lsm_forest_compaction_block_count,
                .lsm_forest_node_count = args.lsm_forest_node_count,
                .cache_entries_accounts = args.cache_accounts,
                .cache_entries_transfers = args.cache_transfers,
                .cache_entries_posted = args.cache_transfers_pending,
                .cache_entries_account_balances = args.cache_account_balances,
            },
            .message_bus_options = .{
                .configuration = args.addresses.const_slice(),
                .io = &command.io,
                .clients_limit = clients_limit,
            },
            .grid_cache_blocks_count = args.cache_grid_blocks,
        }) catch |err| switch (err) {
            error.NoAddress => fatal("all --addresses must be provided", .{}),
            else => |e| return e,
        };

        // Enable checking for new binaries on disk after the replica has been opened. Only
        // supported on Linux.
        if (multiversion != null and builtin.target.os.tag == .linux) {
            multiversion.?.timeout_enable();
        }

        // Note that this does not account for the fact that any allocations will be rounded up to
        // the nearest page by `std.heap.page_allocator`.
        log.info("{}: Allocated {}MiB during replica init", .{
            replica.replica,
            @divFloor(counting_allocator.size, 1024 * 1024),
        });
        log.info("{}: Grid cache: {}MiB, LSM-tree manifests: {}MiB", .{
            replica.replica,
            @divFloor(grid_cache_size, 1024 * 1024),
            @divFloor(args.lsm_forest_node_count * constants.lsm_manifest_node_size, 1024 * 1024),
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
            log.warn("{}: started with constants.verify - expect reduced performance. " ++
                "Recompile with -Dconfig=production if unexpected.", .{replica.replica});
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

        while (true) {
            replica.tick();
            try command.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
        }
    }

    pub fn version(allocator: mem.Allocator, verbose: bool) !void {
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
            const self_exe_path = try vsr.multiversioning.self_exe_path(allocator);
            defer allocator.free(self_exe_path);
            vsr.multiversioning.print_information(allocator, self_exe_path, stdout) catch {};
        }
        try stdout_buffer.flush();
    }

    pub fn repl(arena: *std.heap.ArenaAllocator, args: *const cli.Command.Repl) !void {
        const Repl = vsr.repl.ReplType(vsr.message_bus.MessageBusClient);
        try Repl.run(
            arena,
            args.addresses.const_slice(),
            args.cluster,
            args.statements,
            args.verbose,
        );
    }
};

fn replica_release_execute(replica: *Replica, release: vsr.Release) noreturn {
    assert(release.value != replica.release.value);
    assert(release.value != vsr.Release.zero.value);
    assert(release.value != vsr.Release.minimum.value);

    const multiversion = replica.multiversion orelse {
        @panic("replica_release_execute unsupported");
    };

    for (replica.releases_bundled.const_slice()) |release_bundled| {
        if (release_bundled.value == release.value) break;
    } else {
        log.err("{}: release_execute: release {} is not available;" ++
            "upgrade (or downgrade) the binary", .{
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
        assert(replica.release.value ==
            replica.releases_bundled.get(replica.releases_bundled.count() - 1).value);

        multiversion.exec_release(
            release,
        ) catch |err| {
            std.debug.panic("failed to execute previous release: {}", .{err});
        };
    } else {
        // For the upgrade case, re-run the latest binary in place. If we need something older
        // than the latest, that'll be handled when the case above is hit when re-execing:
        // (current version v1) -> (latest version v4) -> (desired version v2)
        multiversion.exec_latest() catch |err| {
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
        .Fn => {}, // Ignore the log() function.
        .Pointer => try std.fmt.format(writer, "{s}=\"{s}\"\n", .{
            field,
            std.fmt.fmtSliceEscapeLower(value),
        }),
        else => try std.fmt.format(writer, "{s}={any}\n", .{
            field,
            value,
        }),
    }
}
