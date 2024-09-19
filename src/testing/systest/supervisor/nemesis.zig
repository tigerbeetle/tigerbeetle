const std = @import("std");
const Replica = @import("./replica.zig");
const LoggedProcess = @import("./process.zig").LoggedProcess;

const assert = std.debug.assert;
const log = std.log.default;

const Self = @This();

allocator: std.mem.Allocator,
arena: std.heap.ArenaAllocator,
random: std.rand.Random,
replicas: []const Replica,

pub fn init(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    replicas: []const Replica,
) !*Self {
    const arena = std.heap.ArenaAllocator.init(allocator);

    const nemesis = try allocator.create(Self);
    nemesis.* = .{
        .allocator = allocator,
        .arena = arena,
        .random = random,
        .replicas = replicas,
    };

    return nemesis;
}

pub fn deinit(self: *Self) void {
    const allocator = self.allocator;
    self.arena.deinit();
    allocator.destroy(self);
}

const Havoc = enum {
    terminate_replica,
    restart_replica,
    sleep,
};

pub fn wreak_havoc(self: *Self) !void {
    const havoc = weighted(self.random, Havoc, .{
        .sleep = 20,
        .terminate_replica = 1,
        .restart_replica = 10,
    });
    switch (havoc) {
        .terminate_replica => try self.terminate_replica(),
        .restart_replica => try self.restart_replica(),
        .sleep => std.time.sleep(3 * std.time.ns_per_s),
    }
}

fn random_replica_in_state(self: *Self, state: LoggedProcess.State) !?Replica {
    var matching = std.ArrayList(Replica).init(self.allocator);
    defer matching.deinit();

    for (self.replicas) |replica| {
        if (replica.process.state() == state) {
            try matching.append(replica);
        }
    }
    if (matching.items.len == 0) {
        return null;
    }
    std.rand.shuffle(self.random, Replica, matching.items);
    return matching.items[0];
}

fn terminate_replica(self: *Self) !void {
    if (try self.random_replica_in_state(.running)) |replica| {
        log.info("nemesis: stopping {s}", .{replica.name});
        _ = try replica.process.terminate();
        log.info("nemesis: {s} stopped", .{replica.name});
    }
}

fn restart_replica(self: *Self) !void {
    if (try self.random_replica_in_state(.terminated)) |replica| {
        log.info("nemesis: restarting {s}", .{replica.name});
        try replica.process.start();
        log.info("nemesis: {s} back up again", .{replica.name});
    }
}

/// Draw an enum value from `E` based on the relative `weights`. Fields in the weights struct must match
/// the enum.
///
/// The `E` type parameter should be inferred, but seemingly to due to https://github.com/ziglang/zig/issues/19985,
/// it can't be.
fn weighted(random: std.rand.Random, comptime E: type, comptime weights: std.enums.EnumFieldStruct(E, u32, null)) E {
    const s = @typeInfo(@TypeOf(weights)).Struct;
    comptime var total: u64 = 0;
    comptime var enum_weights: [s.fields.len]std.meta.Tuple(&.{ E, comptime_int }) = undefined;

    comptime {
        for (s.fields, 0..) |field, i| {
            const weight: comptime_int = @field(weights, field.name);
            assert(weight > 0);
            total += weight;
            const value = std.meta.stringToEnum(E, field.name).?;
            enum_weights[i] = .{ value, weight };
        }
    }

    const pick = random.uintLessThan(u64, total) + 1;
    var current: u64 = 0;
    inline for (enum_weights) |w| {
        current += w[1];
        if (pick <= current) {
            return w[0];
        }
    }
    unreachable;
}
