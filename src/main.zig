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

const vr = @import("vr.zig");
const Replica = vr.Replica(StateMachine, MessageBus, Storage, Time);

pub fn main() !void {
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = &arena_allocator.allocator;

    switch (cli.parse_args(arena)) {
        .init => |args| try init(arena, args.cluster, args.replica, args.dir_fd),
        .start => |args| try start(arena, args.cluster, args.replica, args.addresses, args.dir_fd),
    }
}

// Pad the cluster id number and the replica index with 0s
const filename_fmt = "cluster_{d:0>10}_replica_{d:0>3}.tigerbeetle";
const filename_len = fmt.count(filename_fmt, .{ 0, 0 });

/// Create a .tigerbeetle data file for the given args and exit
fn init(arena: *mem.Allocator, cluster: u32, replica: u8, dir_fd: os.fd_t) !void {
    // Add 1 for the terminating null byte
    var buffer: [filename_len + 1]u8 = undefined;
    const filename = fmt.bufPrintZ(&buffer, filename_fmt, .{ cluster, replica }) catch unreachable;
    assert(filename.len == filename_len);

    // TODO Expose data file size on the CLI.
    _ = try Storage.open(
        dir_fd,
        filename,
        config.journal_size_max, // TODO Double-check that we have space for redundant headers.
        true,
    );

    log.info("initialized data file", .{});
}

/// Run as a replica server defined by the given args
fn start(
    arena: *mem.Allocator,
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
    const storage_fd = try Storage.open(
        dir_fd,
        filename,
        config.journal_size_max, // TODO Double-check that we have space for redundant headers.
        false,
    );
    var io = try IO.init(128, 0);
    var state_machine = try StateMachine.init(
        arena,
        config.accounts_max,
        config.transfers_max,
        config.commits_max,
    );
    var storage = try Storage.init(config.journal_size_max, storage_fd, &io);
    var message_bus = try MessageBus.init(
        arena,
        cluster,
        addresses,
        replica_index,
        &io,
    );
    var replica = try Replica.init(
        arena,
        cluster,
        @intCast(u8, addresses.len),
        replica_index,
        Time{},
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
