const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;
const log = std.log;

const config = @import("config.zig");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

const cli = @import("cli.zig");

const IO = @import("io.zig").IO;
const Time = @import("time.zig").Time;
const Storage = @import("storage.zig").Storage;
const MessageBus = @import("message_bus.zig").MessageBusReplica;
const StateMachine = @import("state_machine.zig").StateMachine;

const vsr = @import("vsr.zig");
const Replica = vsr.Replica(StateMachine, MessageBus, Storage, Time);

pub fn main() !void {
    var io = try IO.init(128, 0);
    defer io.deinit();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    switch (try cli.parse_args(allocator)) {
        .init => |args| try init(&io, args.cluster, args.replica, args.dir_fd),
        .start => |args| try start(
            &io,
            allocator,
            args.cluster,
            args.replica,
            args.addresses,
            args.dir_fd,
        ),
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

    {
        const write_size_max = 4 * 1024 * 1024;
        var write: [write_size_max]u8 = undefined;
        var offset: u64 = 0;
        while (true) {
            const write_size = vsr.format_journal(cluster, offset, &write);
            if (write_size == 0) break;
            try file.writeAll(write[0..write_size]);
            offset += write_size;
        }
    }

    log.info("initialized data file", .{});
}

/// Run as a replica server defined by the given args
fn start(
    io: *IO,
    allocator: mem.Allocator,
    cluster: u32,
    replica_index: u8,
    addresses: []std.net.Address,
    dir_fd: os.fd_t,
) !void {
    // Add 1 for the terminating null byte
    var buffer: [filename_len + 1]u8 = undefined;
    const filename = fmt.bufPrintZ(&buffer, filename_fmt, .{ cluster, replica_index }) catch {
        unreachable;
    };
    assert(filename.len == filename_len);

    // TODO Expose data file size on the CLI.
    const storage_fd = try io.open_file(
        dir_fd,
        filename,
        config.journal_size_max, // TODO Double-check that we have space for redundant headers.
        false,
    );

    var state_machine = try StateMachine.init(
        allocator,
        config.accounts_max,
        config.transfers_max,
        config.transfers_pending_max,
    );
    var storage = try Storage.init(config.journal_size_max, storage_fd, io);
    var message_bus = try MessageBus.init(
        allocator,
        cluster,
        addresses,
        replica_index,
        io,
    );
    var time: Time = .{};
    var replica = try Replica.init(
        allocator,
        cluster,
        @intCast(u8, addresses.len),
        replica_index,
        &time,
        &storage,
        &message_bus,
        &state_machine,
    );
    message_bus.set_on_message(*Replica, &replica, Replica.on_message);

    log.info("cluster={x} replica={}: listening on {}", .{
        cluster,
        replica_index,
        addresses[replica_index],
    });

    while (true) {
        replica.tick();
        message_bus.tick();
        try io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
    }
}
