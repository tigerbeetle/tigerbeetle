const std = @import("std");

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

    const args = cli.parse_args(arena);

    // TODO Allow_create and path should be exposed on the CLI.
    // TODO Expose data file size on the CLI.
    // TODO Use config.directory or args.directory instead of the cwd.
    // Open the cwd as a real file descriptor that we can fsync, to fsync the file inode:
    const dir_fd = try std.os.openatZ(std.os.AT_FDCWD, ".", std.os.O_CLOEXEC | std.os.O_RDONLY, 0);
    // TODO Data file, e.g. "cluster_4294967295_replica_255.tigerbeetle":
    const relative_path = "journal.tigerbeetle";

    const must_create = try Storage.does_not_exist(dir_fd, relative_path);
    const storage_fd = try Storage.open(
        arena,
        dir_fd,
        relative_path,
        config.journal_size_max, // TODO Double-check that we have space for redundant headers.
        must_create,
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
    var journal = try Journal.init(
        arena,
        &storage,
        args.replica,
        config.journal_size_max,
        config.journal_headers_max,
    );
    var message_bus = try MessageBus.init(
        arena,
        args.cluster,
        args.configuration,
        args.replica,
        &io,
    );
    var replica = try Replica.init(
        arena,
        args.cluster,
        @intCast(u16, args.configuration.len),
        args.replica,
        &time,
        &journal,
        &message_bus,
        &state_machine,
    );
    // TODO: Get rid of this wart by moving MessageBus inside Replica or otherwise.
    message_bus.process.replica = &replica;

    std.log.info("cluster={x} replica={}: listening on {}", .{
        args.cluster,
        args.replica,
        args.configuration[args.replica],
    });

    while (true) {
        replica.tick();
        message_bus.tick();
        try io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
    }
}
