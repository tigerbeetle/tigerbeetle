const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const mem = std.mem;
const os = std.os;

const config = @import("config.zig");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

const cli = @import("cli.zig");

const IO = @import("io.zig").IO;
const Time = @import("time.zig").Time;
const Storage = @import("storage.zig").Storage;
const MessageBus = @import("message_bus.zig").MessageBusReplica;
const StateMachine = @import("state_machine.zig").StateMachine;

const vr = @import("vr.zig");
const Replica = vr.Replica;
const Journal = vr.Journal;

pub fn main() !void {
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = &arena_allocator.allocator;

    switch (cli.parse_args(arena)) {
        .init => |args| try init(arena, args.cluster, args.replica, args.dir_fd),
        .start => |args| try start(arena, args.cluster, args.replica, args.configuration, args.dir_fd),
    }
}

// Pad the cluster id (in hex) and the replica index with 0s
const filename_fmt = "cluster_{x:0>8}_replica_{d:0>5}.tigerbeetle";
const filename_len = fmt.count(filename_fmt, .{ 0, 0 });

/// Create a .tigerbeetle data file for the given args and exit
fn init(arena: *mem.Allocator, cluster: u128, replica: u16, dir_fd: os.fd_t) !void {
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
}

/// Run as a replica server defined by the given args
fn start(
    arena: *mem.Allocator,
    cluster: u128,
    replica_index: u16,
    configuration: []std.net.Address,
    dir_fd: os.fd_t,
) !void {
    // Add 1 for the terminating null byte
    var buffer: [filename_len + 1]u8 = undefined;
    const filename = fmt.bufPrintZ(&buffer, filename_fmt, .{ cluster, replica_index }) catch unreachable;
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
    var time = Time{};
    var storage = try Storage.init(config.journal_size_max, storage_fd, &io);
    var message_bus = try MessageBus.init(
        arena,
        cluster,
        configuration,
        replica_index,
        &io,
    );
    var replica = try Replica.init(
        arena,
        cluster,
        @intCast(u16, configuration.len),
        replica_index,
        &time,
        &storage,
        &message_bus,
        &state_machine,
    );
    // TODO: Get rid of this wart by moving MessageBus inside Replica or otherwise.
    message_bus.process.replica = &replica;

    std.log.info("cluster={x} replica={}: listening on {}", .{
        cluster,
        replica_index,
        configuration[replica_index],
    });

    while (true) {
        replica.tick();
        message_bus.tick();
        try io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
    }
}
