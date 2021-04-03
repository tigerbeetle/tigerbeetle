const std = @import("std");

const conf = @import("tigerbeetle.conf");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, conf.log_level);

const cli = @import("cli.zig");
const IO = @import("io.zig").IO;
const vr = @import("vr.zig");
const Replica = vr.Replica;
const Storage = vr.Storage;
const Journal = vr.Journal;
const MessageBus = @import("message_bus.zig").MessageBus;
const StateMachine = @import("state_machine.zig").StateMachine;

pub fn main() !void {
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = &arena_allocator.allocator;

    const args = cli.parse_args();

    // The smallest f such that 2f + 1 is less than or equal to the number of replicas.
    const f = (args.configuration.len - 1) / 2;

    var io = try IO.init(128, 0);
    var state_machine = try StateMachine.init(arena, conf.accounts_max, conf.transfers_max);
    var storage = try Storage.init(arena, conf.journal_size_max);
    var journal = try Journal.init(
        arena,
        &storage,
        args.replica,
        conf.journal_size_max,
        conf.journal_headers_max,
    );
    var message_bus: MessageBus = undefined;
    var replica = try Replica.init(
        arena,
        args.cluster,
        args.configuration,
        args.replica,
        @intCast(u32, f),
        &journal,
        &message_bus,
        &state_machine,
    );
    try message_bus.init(arena, &io, args.configuration, &replica, args.replica);

    std.log.info("cluster={x} replica={}: listening on {}", .{
        args.cluster,
        args.replica,
        args.configuration[args.replica],
    });

    while (true) {
        replica.tick();
        try io.run_for_ns(conf.tick_ms * std.time.ns_per_ms);
    }
}
