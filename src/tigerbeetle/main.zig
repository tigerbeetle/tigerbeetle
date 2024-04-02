const builtin = @import("builtin");
const std = @import("std");
const vsr_web = @import("web.zig");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log_main = std.log.scoped(.main);

const build_options = @import("vsr_options");

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

pub const std_options = struct {
    pub const log_level: std.log.Level = constants.log_level;
    pub const logFn = constants.log;
};

pub fn main() !void {
    // TODO(zig): Zig defaults to 16MB stack size on Linux, but not yet on mac as of 0.11.
    // Override it here, so it can have the same stack size. Trying to set `tigerbeetle.stack_size`
    // in build.zig doesn't work.
    if (builtin.target.os.tag == .macos)
        os.setrlimit(os.rlimit_resource.STACK, .{
            .cur = 16 * 1024 * 1024,
            .max = 16 * 1024 * 1024,
        }) catch @panic("unable to adjust stack limit");

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
        .format => |*args| try Command.format(allocator, .{
            .cluster = args.cluster,
            .replica = args.replica,
            .replica_count = args.replica_count,
            .release = config.process.release,
        }, args.path),
        .start => |*args| try Command.start(&arena, args),
        .version => |*args| try Command.version(allocator, args.verbose),
        .repl => |*args| try Command.repl(&arena, args),
        .web => |*args| try Command.web(&arena, args),
        .benchmark => |*args| try benchmark_driver.main(allocator, args),
    }
}

const Command = struct {
    dir_fd: os.fd_t,
    fd: os.fd_t,
    io: IO,
    storage: Storage,
    message_pool: MessagePool,

    fn init(
        command: *Command,
        allocator: mem.Allocator,
        path: [:0]const u8,
        must_create: bool,
    ) !void {
        // TODO Resolve the parent directory properly in the presence of .. and symlinks.
        // TODO Handle physical volumes where there is no directory to fsync.
        const dirname = std.fs.path.dirname(path) orelse ".";
        command.dir_fd = try IO.open_dir(dirname);
        errdefer os.close(command.dir_fd);

        const basename = std.fs.path.basename(path);
        command.fd = try IO.open_file(command.dir_fd, basename, data_file_size_min, if (must_create) .create else .open);
        errdefer os.close(command.fd);

        command.io = try IO.init(128, 0);
        errdefer command.io.deinit();

        command.storage = try Storage.init(&command.io, command.fd);
        errdefer command.storage.deinit();

        command.message_pool = try MessagePool.init(allocator, .replica);
        errdefer command.message_pool.deinit(allocator);
    }

    fn deinit(command: *Command, allocator: mem.Allocator) void {
        command.message_pool.deinit(allocator);
        command.storage.deinit();
        command.io.deinit();
        os.close(command.fd);
        os.close(command.dir_fd);
    }

    pub fn format(allocator: mem.Allocator, options: SuperBlock.FormatOptions, path: [:0]const u8) !void {
        var command: Command = undefined;
        try command.init(allocator, path, true);
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

        log_main.info("{}: formatted: cluster={} replica_count={}", .{
            options.replica,
            options.cluster,
            options.replica_count,
        });
    }

    pub fn start(arena: *std.heap.ArenaAllocator, args: *const cli.Command.Start) !void {
        var traced_allocator = if (constants.tracer_backend == .tracy)
            tracer.TracyAllocator("tracy").init(arena.allocator())
        else
            arena;

        // TODO Panic if the data file's size is larger that args.storage_size_limit.
        // (Here or in Replica.open()?).

        const allocator = traced_allocator.allocator();

        var command: Command = undefined;
        try command.init(allocator, args.path, false);
        defer command.deinit(allocator);

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

        var replica: Replica = undefined;
        replica.open(allocator, .{
            .node_count = @intCast(args.addresses.len),
            .release = config.process.release,
            // TODO Where should this be set?
            .release_client_min = config.process.release,
            .releases_bundled = &[_]vsr.Release{config.process.release},
            .release_execute = replica_release_execute,
            .storage_size_limit = args.storage_size_limit,
            .storage = &command.storage,
            .aof = &aof,
            .message_pool = &command.message_pool,
            .nonce = nonce,
            .time = .{},
            .state_machine_options = .{
                .lsm_forest_node_count = args.lsm_forest_node_count,
                .cache_entries_accounts = args.cache_accounts,
                .cache_entries_transfers = args.cache_transfers,
                .cache_entries_posted = args.cache_transfers_pending,
                .cache_entries_account_balances = args.cache_account_balances,
            },
            .message_bus_options = .{
                .configuration = args.addresses,
                .io = &command.io,
            },
            .grid_cache_blocks_count = args.cache_grid_blocks,
        }) catch |err| switch (err) {
            error.NoAddress => fatal("all --addresses must be provided", .{}),
            else => |e| return e,
        };

        // Calculate how many bytes are allocated inside `arena`.
        // TODO This does not account for the fact that any allocations will be rounded up to the nearest page by `std.heap.page_allocator`.
        var allocation_count: usize = 0;
        var allocation_size: usize = 0;
        {
            var node_maybe = arena.state.buffer_list.first;
            while (node_maybe) |node| {
                allocation_count += 1;
                allocation_size += node.data;
                node_maybe = node.next;
            }
        }
        log_main.info("{}: Allocated {}MiB in {} regions during replica init", .{
            replica.replica,
            @divFloor(allocation_size, 1024 * 1024),
            allocation_count,
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
        try std.fmt.format(
            stdout,
            "TigerBeetle version {s}\n",
            .{build_options.version},
        );

        if (verbose) {
            try std.fmt.format(
                stdout,
                \\
                \\git_commit="{s}"
                \\
            ,
                .{build_options.git_commit},
            );

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
    pub fn web(arena: *std.heap.ArenaAllocator, args: *const cli.Command.Web) !void {
        const Web = vsr_web.WebType(vsr.message_bus.MessageBusClient);
        try Web.run(arena, args.addresses, args.cluster, args.verbose);
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
