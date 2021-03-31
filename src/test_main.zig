const std = @import("std");

const conf = @import("tigerbeetle.conf");

const MessageBus = @import("test_message_bus.zig").MessageBus;
const vr = @import("vr.zig");
const Replica = vr.Replica;
const Journal = vr.Journal;
const Storage = vr.Storage;
const StateMachine = @import("state_machine.zig").StateMachine;

const log = std.log.default;

const tigerbeetle = @import("tigerbeetle.zig");

pub fn run() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const f = 1;
    const cluster = 123456789;

    var configuration: [3]*Replica = undefined;
    var message_bus = try MessageBus.init(allocator, &configuration);

    var storage: [2 * f + 1]Storage = undefined;
    var journals: [2 * f + 1]Journal = undefined;
    for (journals) |*journal, index| {
        storage[index] = try Storage.init(allocator, conf.journal_size_max);
        journal.* = try Journal.init(
            allocator,
            &storage[index],
            @intCast(u16, index),
            conf.journal_size_max,
            conf.journal_headers_max,
        );
    }

    var state_machines: [2 * f + 1]StateMachine = undefined;
    for (state_machines) |*state_machine| {
        state_machine.* = try StateMachine.init(allocator, conf.accounts_max, conf.transfers_max);
    }

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        replica.* = try Replica.init(
            allocator,
            cluster,
            f,
            &configuration,
            &message_bus,
            &journals[index],
            &state_machines[index],
            @intCast(u16, index),
        );
        configuration[index] = replica;
    }

    var ticks: usize = 0;
    while (true) : (ticks += 1) {
        std.debug.print("\n", .{});
        log.debug("ticking (message_bus has {} messages allocated)", .{message_bus.allocated});
        std.debug.print("\n", .{});

        var leader: ?*Replica = null;
        for (replicas) |*replica| {
            if (replica.status == .normal and replica.leader()) leader = replica;
            replica.tick();
        }

        if (leader) |replica| {
            if (ticks == 1 or ticks == 5) {
                var request = message_bus.create_message(conf.sector_size) catch unreachable;
                request.header.* = .{
                    .cluster = cluster,
                    .client = 1,
                    .view = 0,
                    .request = @intCast(u32, ticks),
                    .command = .request,
                    .operation = .create_accounts,
                    .size = @sizeOf(vr.Header) + @sizeOf(tigerbeetle.Account),
                };

                var body = request.buffer[@sizeOf(vr.Header)..request.header.size][0..@sizeOf(tigerbeetle.Account)];
                std.mem.bytesAsValue(tigerbeetle.Account, body).* = .{
                    .id = 1,
                    .custom = 0,
                    .flags = .{},
                    .unit = 710,
                    .debit_reserved = 0,
                    .debit_accepted = 0,
                    .credit_reserved = 0,
                    .credit_accepted = 0,
                    .debit_reserved_limit = 100_000,
                    .debit_accepted_limit = 1_000_000,
                    .credit_reserved_limit = 0,
                    .credit_accepted_limit = 0,
                };

                request.header.set_checksum_body(body);
                request.header.set_checksum();

                message_bus.send_message_to_replica(replica.replica, request);
            }
        }

        std.time.sleep(std.time.ns_per_ms * 500);
    }
}

pub fn main() !void {
    var frame = async run();
    nosuspend try await frame;
}
