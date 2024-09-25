const std = @import("std");
const Shell = @import("../../../shell.zig");
const Replica = @import("./replica.zig");
const LoggedProcess = @import("./logged_process.zig");

const assert = std.debug.assert;
const log = std.log.default;

const Self = @This();
const NetemRules = std.AutoHashMap(netem.Op, netem.Args);

shell: *Shell,
allocator: std.mem.Allocator,
random: std.rand.Random,
replicas: []const Replica,
netem_rules: NetemRules,

pub fn init(
    shell: *Shell,
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    replicas: []const Replica,
) !*Self {
    const nemesis = try allocator.create(Self);
    const netem_rules = NetemRules.init(allocator);
    nemesis.* = .{
        .shell = shell,
        .allocator = allocator,
        .random = random,
        .replicas = replicas,
        .netem_rules = netem_rules,
    };

    return nemesis;
}

pub fn deinit(self: *Self) void {
    var it = self.netem_rules.valueIterator();
    while (it.next()) |args| {
        self.allocator.free(args.*);
    }
    self.network_netem_delete_all() catch {};
    self.netem_rules.deinit();
    const allocator = self.allocator;
    allocator.destroy(self);
}

const Havoc = enum {
    terminate_replica,
    restart_replica,
    network_delay_add,
    network_deplay_remove,
    network_loss_add,
    network_loss_remove,
    sleep,
};

pub fn wreak_havoc(self: *Self) !bool {
    const havoc = weighted(self.random, Havoc, .{
        .sleep = 30,
        .terminate_replica = 1,
        .restart_replica = 10,
        .network_delay_add = 10,
        .network_deplay_remove = 1,
        .network_loss_add = 10,
        .network_loss_remove = 1,
    });
    switch (havoc) {
        .terminate_replica => return try self.terminate_replica(),
        .restart_replica => return try self.restart_replica(),
        .network_delay_add => {
            const delay_ms = self.random.intRangeAtMost(u16, 50, 1000);
            return try self.netem_rules_add(.delay, &.{
                try self.shell.fmt("{d}ms", .{delay_ms}),
                try self.shell.fmt("{d}ms", .{@divFloor(delay_ms, 10)}),
                "distribution",
                "normal",
            });
        },
        .network_deplay_remove => return try self.netem_rules_delete(.delay),
        .network_loss_add => {
            const loss_pct = self.random.intRangeAtMost(u16, 20, 100);
            return try self.netem_rules_add(.loss, &.{
                try self.shell.fmt("{d}%", .{loss_pct}),
                "75%",
            });
        },
        .network_loss_remove => return try self.netem_rules_delete(.loss),
        .sleep => {
            std.time.sleep(3 * std.time.ns_per_s);
            return true;
        },
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

fn terminate_replica(self: *Self) !bool {
    if (try self.random_replica_in_state(.running)) |replica| {
        log.info("nemesis: stopping {s}", .{replica.name});
        _ = try replica.process.terminate();
        log.info("nemesis: {s} stopped", .{replica.name});
        return true;
    } else return false;
}

fn restart_replica(self: *Self) !bool {
    if (try self.random_replica_in_state(.terminated)) |replica| {
        log.info("nemesis: restarting {s}", .{replica.name});
        try replica.process.start();
        log.info("nemesis: {s} back up again", .{replica.name});
        return true;
    } else return false;
}

const netem = struct {
    // See: https://man7.org/linux/man-pages/man8/tc-netem.8.html
    const Op = enum {
        limit,
        delay,
        loss,
        corrupt,
        duplication,
        reordering,
        rate,
        slot,
        seed,
    };
    const Args = []const []const u8;
};

fn netem_rules_add(self: *Self, op: netem.Op, args: netem.Args) !bool {
    if (self.netem_rules.contains(op)) {
        return false;
    }
    try self.netem_rules.put(op, try self.allocator.dupe([]const u8, args));
    assert(try self.netem_sync());
    return true;
}

fn netem_rules_delete(self: *Self, op: netem.Op) !bool {
    if (self.netem_rules.get(op)) |args| {
        self.allocator.free(args);
        assert(self.netem_rules.remove(op));
        assert(try self.netem_sync());
        return true;
    } else {
        return false;
    }
}

fn netem_sync(self: *Self) !bool {
    if (self.netem_rules.count() == 0) {
        try self.network_netem_delete_all();
        return true;
    }

    var all_args = std.ArrayList([]const u8).init(self.allocator);
    defer all_args.deinit();

    var it = self.netem_rules.iterator();
    while (it.next()) |kv| {
        try all_args.append(@tagName(kv.key_ptr.*));
        try all_args.appendSlice(kv.value_ptr.*);
    }
    const args_joined = try join_args(self.shell.arena.allocator(), all_args.items);
    log.info("nemesis: syncing netem {s}", .{args_joined});

    self.shell.exec(
        "tc qdisc replace dev lo root netem {args}",
        .{ .args = all_args.items },
    ) catch return false;

    return true;
}

fn network_netem_delete_all(self: *Self) !void {
    try self.shell.exec("tc qdisc del dev lo root", .{});
}

/// Draw an enum value from `E` based on the relative `weights`. Fields in the weights struct must
/// match the enum.
///
/// The `E` type parameter should be inferred, but seemingly to due to
/// https://github.com/ziglang/zig/issues/19985, it can't be.
fn weighted(
    random: std.rand.Random,
    comptime E: type,
    comptime weights: std.enums.EnumFieldStruct(E, u32, null),
) E {
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

fn join_args(allocator: std.mem.Allocator, args: []const []const u8) ![]const u8 {
    assert(args.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    const writer = out.writer();

    try writer.writeAll(args[0]);
    for (args[1..]) |arg| {
        try writer.writeByte(' ');
        try writer.writeAll(arg);
    }

    return out.toOwnedSlice();
}
