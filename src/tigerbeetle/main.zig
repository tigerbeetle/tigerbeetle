const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log_main = std.log.scoped(.main);

const build_options = @import("vsr_options");

const vsr = @import("vsr");
const constants = vsr.constants;
const config = vsr.config.configs.current;
const tracer = vsr.tracer;

const cli = @import("cli.zig");
const fatal = cli.fatal;

const IO = vsr.io.IO;
const Time = vsr.time.Time;
const Storage = vsr.storage.Storage;
const AOF = vsr.aof.AOF;

const MessageBus = vsr.message_bus.MessageBusReplica;
const MessagePool = vsr.message_pool.MessagePool;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);

const AOFType = if (constants.aof_record) AOF else void;
const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, Time, AOFType);
const SuperBlock = vsr.SuperBlockType(Storage);
const superblock_zone_size = vsr.superblock.superblock_zone_size;
const data_file_size_min = vsr.superblock.data_file_size_min;

pub const log_level: std.log.Level = constants.log_level;
pub const log = constants.log;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    try tracer.init(allocator);
    defer tracer.deinit(allocator);

    var parse_args = try cli.parse_args(allocator);
    defer parse_args.deinit(allocator);

    switch (parse_args) {
        .format => |*args| try Command.format(allocator, .{
            .cluster = args.cluster,
            .replica = args.replica,
            .replica_count = args.replica_count,
        }, args.path),
        .start => |*args| try Command.start(&arena, args),
        .version => |*args| try Command.version(allocator, args.verbose),
    }
}

// Pad the cluster id number and the replica index with 0s
const filename_fmt = "cluster_{d:0>10}_replica_{d:0>3}.tigerbeetle";
const filename_len = fmt.count(filename_fmt, .{ 0, 0 });

const Command = struct 


       {



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
            tracer.TracedAllocator.init(arena.allocator())
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
            var aof_path = try std.fmt.allocPrint(allocator, "{s}.aof", .{args.path});
            defer allocator.free(aof_path);

            aof = try AOF.from_absolute_path(aof_path);
        }

        const grid_cache_size = args.cache_grid_blocks * constants.block_size;
        const grid_cache_size_warn = 1024 * 1024 * 1024;
        if (grid_cache_size <= grid_cache_size_warn) {
            log_main.warn("Grid cache size of {}MB is small. See --cache-grid", .{
                @divExact(grid_cache_size, 1024 * 1024),
            });
        }

        const nonce = while (true) {
            // `random.uintLessThan` does not work for u128 as of Zig 0.9.1.x.
            const nonce_or_zero = std.crypto.random.int(u128);
            if (nonce_or_zero != 0) break nonce_or_zero;
        } else unreachable;
        assert(nonce != 0);

        var replica: Replica = undefined;
        replica.open(allocator, .{
            .node_count = @intCast(u8, args.addresses.len),
            .storage_size_limit = args.storage_size_limit,
            .storage = &command.storage,
            .aof = &aof,
            .message_pool = &command.message_pool,
            .nonce = nonce,
            .time = .{},
            .state_machine_options = .{
                // TODO Tune lsm_forest_node_count better.
                .lsm_forest_node_count = 4096,
                .cache_entries_accounts = args.cache_accounts,
                .cache_entries_transfers = args.cache_transfers,
                .cache_entries_posted = args.cache_transfers_posted,
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
                allocation_size += node.data.len;
                node_maybe = node.next;
            }
        }
        log_main.info("{}: Allocated {}MB in {} regions during replica init (Grid Cache: {}MB)", .{
            replica.replica,
            @divFloor(allocation_size, 1024 * 1024),
            allocation_count,
            @divFloor(grid_cache_size, 1024 * 1024),
        });

        log_main.info("{}: cluster={}: listening on {}", .{
            replica.replica,
            replica.cluster,
            args.addresses[replica.replica],
        });

        if (constants.aof_recovery) {
            log_main.warn("{}: started in AOF recovery mode. This is potentially dangerous - " ++
                "if it's unexpected, please recompile TigerBeetle with -Dconfig-aof-recovery=false.", .{replica.replica});
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
            "TigerBeetle version {s}",
            .{build_options.git_tag orelse "experimental"},
        );

        if (verbose) {
            try std.fmt.format(
                stdout,
                \\
                \\git_commit="{s}"
                \\
            ,
                .{build_options.git_commit orelse "?"},
            );

            try stdout.writeAll("\n");
            inline for (.{ "mode", "zig_version" }) |declaration| {
                try print_value(stdout, "build." ++ declaration, @field(builtin, declaration));
            }

            // TODO(Zig): Use meta.fieldNames() after upgrading to 0.10.
            // See: https://github.com/ziglang/zig/issues/10235
            try stdout.writeAll("\n");
            inline for (std.meta.fields(@TypeOf(config.cluster))) |field| {
                try print_value(stdout, "cluster." ++ field.name, @field(config.cluster, field.name));
            }

            try stdout.writeAll("\n");
            inline for (std.meta.fields(@TypeOf(config.process))) |field| {
                try print_value(stdout, "process." ++ field.name, @field(config.process, field.name));
            }
        }
        try stdout_buffer.flush();
    }
};

fn print_value(
    writer: anytype,
    comptime field: []const u8,
    comptime value: anytype,
) !void {
    switch (@typeInfo(@TypeOf(value))) {
        .Fn => {}, // Ignore the log() function.
        .Pointer => try std.fmt.format(writer, "{s}=\"{s}\"\n", .{
            field,
            std.fmt.fmtSliceEscapeLower(value),
        }),
        else => try std.fmt.format(writer, "{s}={}\n", .{
            field,
            value,
        }),
    }
}
