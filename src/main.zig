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
const superblock_zone_size = @import("lsm/superblock.zig").superblock_zone_size;

const MessageBus = @import("message_bus.zig").MessageBusReplica;
const StateMachine = @import("state_machine.zig").StateMachine;

const vsr = @import("vsr.zig");
const Replica = vsr.Replica(StateMachine, MessageBus, Storage, Time);

const data_file_size_min = @import("lsm/superblock.zig").data_file_size_min;

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

/// Create a .tigerbeetle data file for the given args and exit
fn init(io: *IO, cluster: u32, replica: u8, dir_fd: os.fd_t) !void {
    // Add 1 for the terminating null byte
    var buffer: [filename_len + 1]u8 = undefined;
    const filename = fmt.bufPrintZ(&buffer, filename_fmt, .{ cluster, replica }) catch unreachable;
    assert(filename.len == filename_len);

    // TODO Expose data file size on the CLI.
    const fd = try io.open_file(
        dir_fd,
        filename,
        config.journal_size_max,
        true,
    );
    std.os.close(fd);

    const file = try (std.fs.Dir{ .fd = dir_fd }).openFile(filename, .{ .write = true });
    defer file.close();

    log.info("initialized data file", .{});
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
        const fd = try open_file(path, true);

        {
            const write_size_max = 4 * 1024 * 1024;
            var write: [write_size_max]u8 = undefined;
            var offset: u64 = superblock_zone_size;
            while (true) {
                const write_size = vsr.format_journal(cluster, offset, &write);
                if (write_size == 0) break;
                {
                    var written: usize = 0;
                    while (written < write_size) {
                        written += try os.write(fd, write[0..write_size][written..]);
                    }
                }
                offset += write_size;
            }
        }

        var command: Command = undefined;
        try command.init(allocator, fd, null);

        command.superblock.format(format_callback, &command.superblock_context, .{
            .cluster = cluster,
            .replica = replica,
            .size_max = config.size_max, // This can later become a runtime arg, to cap storage.
        });

        try command.run();
    }

    pub fn start(
        allocator: mem.Allocator,
        addresses: []std.net.Address,
        memory: u64,
        path: [:0]const u8,
    ) !void {
        _ = memory; // TODO

        const fd = try open_file(path, false);

        var command: Command = undefined;
        try command.init(allocator, fd, addresses);

        command.superblock.open(open_callback, &command.superblock_context);
        try command.run();

        // After opening the superblock, we immediately start the main event loop and remain there.
        // If we were to arrive here, then the memory for the command is about to go out of scope.
        unreachable;
    }

    fn open_file(path: [:0]const u8, must_create: bool) !os.fd_t {
        // TODO Resolve the parent directory properly in the presence of .. and symlinks.
        // TODO Handle physical volumes where there is no directory to fsync.
        const dirname = std.fs.path.dirname(path) orelse ".";
        const dir_fd = try IO.open_dir(dirname);

        const basename = std.fs.path.basename(path);
        return IO.open_file(dir_fd, basename, data_file_size_min, must_create);
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

        // TODO log an error and exit instead. This is reachable if we e.g. run out of memory.
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
