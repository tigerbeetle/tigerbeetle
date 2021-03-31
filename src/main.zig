const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const net = std.net;
const os = std.os;

const cli = @import("cli.zig");
const IO = @import("io_callbacks.zig").IO;
const MessageBus = @import("message_bus.zig").MessageBus;
const vr = @import("vr.zig");
const Replica = vr.Replica;
const Journal = vr.Journal;
const Storage = vr.Storage;
const StateMachine = @import("state_machine.zig").StateMachine;

const journal_size = 128 * 1024 * 1024;
const journal_headers = 16384;
const accounts_max = 1000;
const transfers_max = 8192;
const tick_ns = std.time.ns_per_ms * 10;

pub fn main() !void {
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = &arena_allocator.allocator;

    const args = cli.parse_args();

    // The smallest f such that 2f + 1 is less than or equal to the number of replicas.
    const f = (args.configuration.len - 1) / 2;

    var io = try IO.init(128, 0);
    var state_machine = try StateMachine.init(arena, accounts_max, transfers_max);
    var storage = try Storage.init(arena, journal_size);
    var journal = try Journal.init(arena, &storage, args.replica, journal_size, journal_headers);
    var message_bus: MessageBus = undefined;
    var replica = try Replica.init(
        arena,
        args.cluster,
        @intCast(u32, f),
        args.configuration,
        &message_bus,
        &journal,
        &state_machine,
        args.replica,
    );
    try message_bus.init(arena, &io, args.configuration, &replica, args.replica);

    var tick_completion: IO.Completion = undefined;
    io.timeout(*Replica, &replica, on_tick_timeout, &tick_completion, tick_ns);

    try io.run();
}

fn on_tick_timeout(replica: *Replica, tick_completion: *IO.Completion, result: IO.TimeoutError!void) void {
    result catch unreachable; // We never cancel the tick timeout
    replica.tick();
    tick_completion.io.timeout(*Replica, replica, on_tick_timeout, tick_completion, tick_ns);
}
