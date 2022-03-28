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
const SuperBlock = @import("lsm/superblock.zig").SuperBlockType(Storage);

const MessageBus = @import("message_bus.zig").MessageBusReplica;
const StateMachine = @import("state_machine.zig").StateMachine;

const vsr = @import("vsr.zig");
const Replica = vsr.Replica(StateMachine, MessageBus, Storage, Time);

const data_file_size_min = @import("lsm/superblock.zig").data_file_size_min;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    switch (cli.parse_args(allocator)) {
        .format => |args| try Command.format(allocator, args.cluster, args.replica, args.path),
        .start => |args| try Command.start(allocator, args.addresses, args.path),
    }
}

const Command = struct {
    allocator: mem.Allocator,
    fd: os.fd_t,
    io: IO,
    pending: u64,
    storage: Storage,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context,
    addresses: ?[]std.net.Address,

    pub fn format(allocator: mem.Allocator, cluster: u32, replica: u8, path: [:0]const u8) !void {
        const fd = try Storage.open(path, data_file_size_min, true);

        var command: Command = undefined;
        try command.init(allocator, fd, null);

        command.superblock.format(format_callback, &command.superblock_context, .{
            .cluster = cluster,
            .replica = replica,
            .size_max = config.size_max, // This can later become a runtime arg, to cap storage.
        });
        try command.run();
    }

    pub fn start(allocator: mem.Allocator, addresses: []std.net.Address, path: [:0]const u8) !void {
        const fd = try Storage.open(path, data_file_size_min, false);

        var command: Command = undefined;
        try command.init(allocator, fd, addresses);

        command.superblock.open(open_callback, &command.superblock_context);
        try command.run();

        // After opening the superblock, we immediately start the main event loop and remain there.
        // If we were to arrive here, then the memory for the command is about to go out of scope.
        unreachable;
    }

    fn run(command: *Command) !void {
        assert(command.pending == 0);
        command.pending += 1;

        while (command.pending > 0) try command.io.run_for_ns(std.time.ns_per_ms);
    }

    fn format_callback(superblock_context: *SuperBlock.Context) void {
        const command = @fieldParentPtr(Command, "superblock_context", superblock_context);
        command.pending -= 1;
    }

    fn open_callback(superblock_context: *SuperBlock.Context) void {
        const command = @fieldParentPtr(Command, "superblock_context", superblock_context);
        command.pending -= 1;

        command.event_loop() catch unreachable;
    }

    fn event_loop(command: *Command) !void {
        const cluster = command.superblock.working.cluster;
        const replica_index = command.superblock.working.replica;

        if (replica_index >= command.addresses.?.len) {
            fatal("all --addresses must be provided (cluster={}, replica={})", .{
                cluster,
                replica_index,
            });
        }

        var state_machine = try StateMachine.init(
            command.allocator,
            config.accounts_max,
            config.transfers_max,
            config.commits_max,
        );
        var time: Time = .{};
        var message_bus = try MessageBus.init(
            command.allocator,
            cluster,
            command.addresses.?,
            replica_index,
            &command.io,
        );
        var replica = try Replica.init(
            command.allocator,
            cluster,
            @intCast(u8, command.addresses.?.len),
            replica_index,
            &time,
            &command.storage,
            &message_bus,
            &state_machine,
        );
        message_bus.set_on_message(*Replica, &replica, Replica.on_message);

        log.info("cluster={} replica={}: listening on {}", .{
            cluster,
            replica_index,
            command.addresses.?[replica_index],
        });

        while (true) {
            replica.tick();
            message_bus.tick();
            try command.io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
        }
    }

    fn init(
        command: *Command,
        allocator: mem.Allocator,
        fd: os.fd_t,
        addresses: ?[]std.net.Address,
    ) !void {
        command.allocator = allocator;
        command.fd = fd;

        command.io = try IO.init(128, 0);
        errdefer command.io.deinit();

        command.pending = 0;

        command.storage = try Storage.init(&command.io, command.fd);
        errdefer command.storage.deinit();

        command.superblock = try SuperBlock.init(allocator, &command.storage);
        errdefer command.superblock.deinit(allocator);

        command.addresses = addresses;
    }
};
