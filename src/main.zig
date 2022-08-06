const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log = std.log.scoped(.main);

const config = @import("config.zig");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

const cli = @import("cli.zig");
const fatal = cli.fatal;

const IO = @import("io.zig").IO;
const Time = @import("time.zig").Time;
const Storage = @import("storage.zig").Storage;

const MessageBus = @import("message_bus.zig").MessageBusReplica;
const MessagePool = @import("message_pool.zig").MessagePool;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage);

const vsr = @import("vsr.zig");
const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, Time);
const ReplicaFormat = vsr.ReplicaFormatType(Storage);
const ReplicaOpenError = vsr.ReplicaOpenError;

const SuperBlock = vsr.SuperBlockType(Storage);
const superblock_zone_size = @import("vsr/superblock.zig").superblock_zone_size;
const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();
    
    var parse_args = try cli.parse_args(allocator);
    defer parse_args.deinit(allocator);

    switch (parse_args) {
        .format => |*args| try Command.format(allocator, args.cluster, args.replica, args.path),
        .start => |*args| try Command.start(allocator, args.addresses, args.memory, args.path),
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
    message_pool: MessagePool,

    io_completed: bool,
    replica_format: ReplicaFormat,
    replica_open: Replica.Open,
    replica: Replica = undefined,

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
        // TODO Implement & call deinit() for MessagePool
    }

    fn deinit(command: *Command, _: mem.Allocator) void {
        // TODO Should this deinit ReplicaFormat/Replica.Open/Replica/MessagePool?
        // TODO Add message_pool.deinit() once implemented.
        command.storage.deinit();
        command.io.deinit();
        os.close(command.fd);
        os.close(command.dir_fd);
    }

    pub fn format(allocator: mem.Allocator, cluster: u32, replica: u8, path: [:0]const u8) !void {
        var command: Command = undefined;
        try command.init(allocator, path, true);
        defer command.deinit(allocator);

        var superblock = try SuperBlock.init(
            allocator,
            &command.storage,
            &command.message_pool,
        );
        defer superblock.deinit(allocator);

        command.replica_format = try ReplicaFormat.init(
            allocator,
            cluster,
            replica,
            &command.storage,
            &superblock,
        );
        defer command.replica_format.deinit(allocator);

        command.io_completed = false;
        command.replica_format.format(format_callback);
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
        _ = memory; // TODO

        var command: Command = undefined;
        try command.init(allocator, path, false);
        defer command.deinit(allocator);

        command.io_completed = false;
        command.replica_open = try Replica.Open.init(allocator, &command.replica, .{
            .replica_count = @intCast(u8, addresses.len),
            .storage = &command.storage,
            .message_pool = &command.message_pool,
            .time = .{},
            .state_machine_options = .{
                // TODO Tune lsm_forest_node_count better.
                .lsm_forest_node_count = 4096,
                .cache_size_accounts = config.accounts_max,
                .cache_size_transfers = config.transfers_max,
                .cache_size_posted = config.transfers_pending_max,
            },
            .message_bus_options = .{
                .configuration = addresses,
                .io = &command.io,
            },
        });
        errdefer command.replica_open.deinit(allocator);

        command.replica_open.open(open_callback);
        while (!command.io_completed) try command.io.tick();

        try command.run_replica(allocator, addresses);
    }

    fn open_callback(replica_open: *Replica.Open, result: anyerror!void) void {
        const command = @fieldParentPtr(Command, "replica_open", replica_open);
        assert(!command.io_completed);
        command.io_completed = true;

        result catch |err| switch (err) {
            error.NoAddress => {
                // TODO Include the replica index here.
                fatal("all --addresses must be provided", .{ });
            },
            else => fatal("error opening replica err={}", .{ err }),
        };
    }

    fn run_replica(
        command: *Command,
        allocator: mem.Allocator,
        addresses: []std.net.Address,
    ) !void {
        // TODO Find a better place for this, it is awkward here.
        // Plus, if open_callback hit error.NoAddress, the Replica.Open isn't deinitialized.
        command.replica_open.deinit(allocator);

        log.info("open_callback: cluster={} replica={}: listening on {}", .{
            command.replica.cluster,
            command.replica.replica,
            addresses[command.replica.replica],
        });

        while (true) {
            command.replica.tick();
            try command.io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
        }
    }
};
