const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log_main = std.log.scoped(.main);

const vsr = @import("vsr");
const constants = vsr.constants;
const config = constants.config;
const tracer = vsr.tracer;

const benchmark_driver = @import("benchmark_driver.zig");
const cli = @import("cli.zig");
const fatal = vsr.flags.fatal;

const IO = vsr.io.IO;
const Time = vsr.time.Time;
const Storage = vsr.storage.Storage;
const AOF = vsr.aof.AOF;

const MessageBus = vsr.message_bus.MessageBusReplica;
const MessagePool = vsr.message_pool.MessagePool;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Grid = vsr.GridType(vsr.storage.Storage);

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

    var command = try cli.parse_args(allocator, &arg_iterator);
    defer command.deinit(allocator);

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

    fn init(
        command: *Command,
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
    }

    fn deinit(command: *Command) void {
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
        try command.init(args.path, .{
            .must_create = true,
            .development = args.development,
        });
        defer command.deinit();

        var superblock = try SuperBlock.init(
            allocator,
            .{
                .storage = &command.storage,
                .storage_size_limit = data_file_size_min,
            },
        );
        defer superblock.deinit(allocator);

        try vsr.format(Storage, allocator, options, &command.storage, &superblock);

        log_main.info("{}: formatted: cluster={} replica_count={}", .{
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
        try command.init(args.path, .{
            .must_create = false,
            .development = args.development,
        });
        defer command.deinit();

        var message_pool = try MessagePool.init(allocator, .{ .replica = .{
            .members_count = @intCast(args.addresses.len),
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
            fatal("Grid cache must be greater than {}MiB. See --cache-grid", .{
                @divExact(grid_cache_size_min, 1024 * 1024),
            });
        }
        assert(grid_cache_size >= grid_cache_size_min);

        const grid_cache_size_warn = 1024 * 1024 * 1024;
        if (grid_cache_size < grid_cache_size_warn) {
            log_main.warn("Grid cache size of {}MiB is small. See --cache-grid", .{
                @divExact(grid_cache_size, 1024 * 1024),
            });
        }

        const nonce = std.crypto.random.int(u128);
        assert(nonce != 0); // Broken CSPRNG is the likeliest explanation for zero.

        const releases_bundled = &[_]vsr.Release{config.process.release};

        log_main.info("release={}", .{config.process.release});
        log_main.info("release_client_min={}", .{config.process.release_client_min});
        log_main.info("releases_bundled={any}", .{releases_bundled.*});
        log_main.info("git_commit={?s}", .{config.process.git_commit});

        const clients_limit = constants.pipeline_prepare_queue_max + args.pipeline_requests_limit;

        var replica: Replica = undefined;
        replica.open(allocator, .{
            .node_count = @intCast(args.addresses.len),
            .release = config.process.release,
            .release_client_min = config.process.release_client_min,
            .releases_bundled = releases_bundled,
            .release_execute = replica_release_execute,
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
                .configuration = args.addresses,
                .io = &command.io,
                .clients_limit = clients_limit,
            },
            .grid_cache_blocks_count = args.cache_grid_blocks,
        }) catch |err| switch (err) {
            error.NoAddress => fatal("all --addresses must be provided", .{}),
            else => |e| return e,
        };

        // Note that this does not account for the fact that any allocations will be rounded up to
        // the nearest page by `std.heap.page_allocator`.
        log_main.info("{}: Allocated {}MiB during replica init", .{
            replica.replica,
            @divFloor(counting_allocator.size, 1024 * 1024),
        });
        log_main.info("{}: Grid cache: {}MiB, LSM-tree manifests: {}MiB", .{
            replica.replica,
            @divFloor(grid_cache_size, 1024 * 1024),
            @divFloor(args.lsm_forest_node_count * constants.lsm_manifest_node_size, 1024 * 1024),
        });

        log_main.info("{}: cluster={}: listening on {}", .{
            replica.replica,
            replica.cluster,
            replica.message_bus.process.accept_address,
        });

        if (constants.aof_recovery) {
            log_main.warn("{}: started in AOF recovery mode. This is potentially dangerous - " ++
                "if it's unexpected, please recompile TigerBeetle with -Dconfig-aof-recovery=false.", .{replica.replica});
        }

        if (constants.verify) {
            log_main.warn("{}: started with constants.verify - expect reduced performance. " ++
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
                    log_main.info("stdin closed, exiting", .{});
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
        _ = allocator;

        var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());
        const stdout = stdout_buffer.writer();
        try std.fmt.format(stdout, "TigerBeetle version {}\n", .{constants.semver});

        if (verbose) {
            try stdout.writeAll("\n");
            inline for (.{ "mode", "zig_version" }) |declaration| {
                try print_value(stdout, "build." ++ declaration, @field(builtin, declaration));
            }

            // Zig 0.10 doesn't see field_name as comptime if this `comptime` isn't used.
            try stdout.writeAll("\n");
            inline for (comptime std.meta.fieldNames(@TypeOf(config.cluster))) |field_name| {
                try print_value(stdout, "cluster." ++ field_name, @field(config.cluster, field_name));
            }

            try stdout.writeAll("\n");
            inline for (comptime std.meta.fieldNames(@TypeOf(config.process))) |field_name| {
                try print_value(stdout, "process." ++ field_name, @field(config.process, field_name));
            }
        }
        try stdout_buffer.flush();
    }

    pub fn repl(arena: *std.heap.ArenaAllocator, args: *const cli.Command.Repl) !void {
        const Repl = vsr.repl.ReplType(vsr.message_bus.MessageBusClient);
        try Repl.run(arena, args.addresses, args.cluster, args.statements, args.verbose);
    }
};

fn replica_release_execute(replica: *Replica, release: vsr.Release) noreturn {
    assert(release.value != replica.release.value);

    for (replica.releases_bundled.const_slice()) |release_bundled| {
        if (release_bundled.value == release.value) break;
    } else {
        log_main.err("{}: release_execute: release {} is not available; upgrade the binary", .{
            replica.replica,
            release,
        });
        @panic("release_execute: binary missing required version");
    }

    // TODO(Multiversioning) Exec into the new release.
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
