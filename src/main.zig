const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log = std.log;

const config = @import("config.zig");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

const cli = @import("cli.zig");
const fatal = cli.fatal;

const IO = @import("io.zig").IO;
const Time = @import("time.zig").Time;
const Storage = @import("storage.zig").Storage;

const Grid = @import("lsm/grid.zig").GridType(Storage);
const MessageBus = @import("message_bus.zig").MessageBusReplica;
const MessagePool = @import("message_pool.zig").MessagePool;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage);

const vsr = @import("vsr.zig");
const Replica = vsr.Replica(StateMachine, MessageBus, Storage, Time);
const ReplicaFormat = vsr.ReplicaFormatType(Storage);

const SuperBlock = vsr.SuperBlockType(Storage);
const superblock_zone_size = @import("vsr/superblock.zig").superblock_zone_size;
const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    switch (try cli.parse_args(allocator)) {
        .format => |args| try Command.format(allocator, args.cluster, args.replica, args.path),
        .start => |args| try Command.start(allocator, args.addresses, args.memory, args.path),
    }
}

// Pad the cluster id number and the replica index with 0s
const filename_fmt = "cluster_{d:0>10}_replica_{d:0>3}.tigerbeetle";
const filename_len = fmt.count(filename_fmt, .{ 0, 0 });

const Command = struct {
    dir_fd: os.fd_t,
    fd: os.fd_t,
    io: IO,
    storage: Storage,
    superblock: SuperBlock,
    message_pool: MessagePool,

    io_completed: bool,
    replica_format: ReplicaFormat,
    superblock_context: SuperBlock.Context,

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
        command.fd = try IO.open_file(command.dir_fd, basename, data_file_size_min, must_create);
        errdefer os.close(command.fd);

        command.io = try IO.init(128, 0);
        errdefer command.io.deinit();

        command.storage = try Storage.init(&command.io, command.fd);
        errdefer command.storage.deinit();

        command.message_pool = try MessagePool.init(allocator, .replica);
        // message_pool does not have deinit()

        command.superblock = try SuperBlock.init(
            allocator,
            &command.storage,
            &command.message_pool,
        );
        errdefer command.superblock.deinit(allocator);
    }

    fn deinit(command: *Command, allocator: mem.Allocator) void {
        command.superblock.deinit(allocator);
        command.storage.deinit();
        command.io.deinit();
        os.close(command.fd);
        os.close(command.dir_fd);
    }

    pub fn format(allocator: mem.Allocator, cluster: u32, replica: u8, path: [:0]const u8) !void {
        var command: Command = undefined;
        try command.init(allocator, path, true);
        defer command.deinit(allocator);

        command.replica_format = try ReplicaFormat.init(
            allocator,
            cluster,
            replica,
            &command.storage,
            &command.superblock,
        );
        defer command.replica_format.deinit(allocator);

        command.io_completed = false;
        try command.replica_format.format(format_callback);
        while (!command.io_completed) try command.io.tick();
    }

    fn format_callback(replica_format: *ReplicaFormat) void {
        const command = @fieldParentPtr(Command, "replica_format", replica_format);
        assert(!command.io_completed);
        command.io_completed = true;
    }

    pub fn start(
        allocator: mem.Allocator,
        addresses: []std.net.Address,
        memory: u64,
        path: [:0]const u8,
    ) !void {
        var command: Command = undefined;
        try command.init(allocator, path, false);
        defer command.deinit(allocator);

        command.io_completed = false;
        command.superblock.open(open_callback, &command.superblock_context);
        while (!command.io_completed) try command.io.tick();

        try command.run_replica(allocator, addresses, memory);
    }

    fn open_callback(superblock_context: *SuperBlock.Context) void {
        const command = @fieldParentPtr(Command, "superblock_context", superblock_context);
        assert(!command.io_completed);
        command.io_completed = true;
    }

    fn run_replica(
        command: *Command,
        allocator: mem.Allocator,
        addresses: []std.net.Address,
        memory: u64,
    ) !void {
        _ = memory; //TODO

        const cluster = command.superblock.working.cluster;
        const replica_index = command.superblock.working.replica;

        if (replica_index >= addresses.len) {
            fatal("all --addresses must be provided (cluster={}, replica={})", .{
                cluster,
                replica_index,
            });
        }

        var time: Time = .{};
        var message_bus = try MessageBus.init(
            allocator,
            cluster,
            addresses,
            replica_index,
            &command.io,
            &command.message_pool,
        );
        defer message_bus.deinit();

        var grid = try Grid.init(allocator, &command.superblock);
        defer grid.deinit(allocator);

        var replica = try Replica.init(
            allocator,
            .{
                .cluster = cluster,
                .superblock = &command.superblock,
                .replica_count = @intCast(u8, addresses.len),
                .replica_index = replica_index,
                .time = &time,
                .grid = &grid,
                .storage = &command.storage,
                .message_bus = &message_bus,
                .state_machine_options = .{
                    // TODO Tune lsm_forest_node_count better.
                    .lsm_forest_node_count = 4096,
                    .cache_size_accounts = config.accounts_max,
                    .cache_size_transfers = config.transfers_max,
                    .cache_size_posted = config.transfers_pending_max,
                },
            },
        );
        defer replica.deinit(allocator);
        message_bus.set_on_message(*Replica, &replica, Replica.on_message);

        log.info("cluster={} replica={}: listening on {}", .{
            replica.cluster,
            replica.replica,
            addresses[replica.replica],
        });

        while (true) {
            replica.tick();
            message_bus.tick();
            try command.io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
        }
    }
};
