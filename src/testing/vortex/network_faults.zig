//! This provides actions for injecting network faults, to test TigerBeetle for fault tolerance.
//! It's inspired by Jepsen's "nemesis".

const std = @import("std");
const arbitrary = @import("./arbitrary.zig");

const assert = std.debug.assert;
const log = std.log.scoped(.network_faults);

// Internal state used to track which rules are active.
// See: https://man7.org/linux/man-pages/man8/tc-netem.8.html
const NetemRules = struct {
    const Delay = struct {
        time_ms: u32,
        jitter_ms: u32,
        correlation_pct: u8,
    };

    const Loss = struct {
        loss_pct: u8,
        correlation_pct: u8,
    };

    delay: ?Delay,
    loss: ?Loss,
    // Others not implemented: limit, corrupt, duplication, reordering, rate, slot, seed
};
var netem_rules = NetemRules{ .delay = null, .loss = null };

/// Needs to be called before doing network fault injection.
pub fn setup(allocator: std.mem.Allocator) !void {
    // Ensure loopback can be used for network fault injection.
    try exec_silent(allocator, &.{ "ip", "link", "set", "up", "dev", "lo" });
}

pub const Action = enum {
    network_delay_add,
    network_delay_remove,
    network_loss_add,
    network_loss_remove,
};

/// Executes an action, panicking if it's not enabled.
pub fn execute(allocator: std.mem.Allocator, random: std.Random, action: Action) !void {
    switch (action) {
        .network_delay_add => {
            assert(netem_rules.delay == null);
            const delay_ms = random.intRangeAtMost(u16, 10, 200);
            netem_rules.delay = .{
                .time_ms = delay_ms,
                .jitter_ms = @divFloor(delay_ms, 10),
                .correlation_pct = 75,
            };
            try netem_sync(allocator);
        },
        .network_delay_remove => {
            assert(netem_rules.delay != null);
            netem_rules.delay = null;
            try netem_sync(allocator);
        },
        .network_loss_add => {
            assert(netem_rules.loss == null);
            const loss_pct = random.intRangeAtMost(u8, 5, 100);
            netem_rules.loss = .{
                .loss_pct = loss_pct,
                .correlation_pct = 75,
            };
            try netem_sync(allocator);
        },
        .network_loss_remove => {
            assert(netem_rules.loss != null);
            netem_rules.loss = null;
            try netem_sync(allocator);
        },
    }
}

/// Zeroes out the weights of actions that are not enabled.
pub fn adjusted_weights(weights: arbitrary.EnumWeights(Action)) arbitrary.EnumWeights(Action) {
    return .{
        .network_delay_add = if (netem_rules.delay != null) 0 else weights.network_delay_add,
        .network_delay_remove = if (netem_rules.delay == null) 0 else weights.network_delay_remove,
        .network_loss_add = if (netem_rules.loss != null) 0 else weights.network_loss_add,
        .network_loss_remove = if (netem_rules.loss == null) 0 else weights.network_loss_remove,
    };
}

fn netem_sync(allocator: std.mem.Allocator) !void {
    const args_max = (std.meta.fields(NetemRules).len + 1) * 8;
    var args_buf = std.mem.zeroes([args_max][]const u8);
    var args = std.ArrayListUnmanaged([]const u8).initBuffer(args_buf[0..]);

    args.appendSliceAssumeCapacity(&.{ "tc", "qdisc", "replace", "dev", "lo", "root", "netem" });

    var args_delay_time = std.mem.zeroes([6]u8);
    var args_delay_jitter = std.mem.zeroes([6]u8);
    var args_delay_correlation = std.mem.zeroes([4]u8);
    var args_loss_pct = std.mem.zeroes([4]u8);
    var args_loss_correlation = std.mem.zeroes([4]u8);

    if (netem_rules.delay) |delay| {
        args.appendAssumeCapacity("delay");
        args.appendAssumeCapacity(try std.fmt.bufPrint(
            args_delay_time[0..],
            "{d}ms",
            .{delay.time_ms},
        ));
        args.appendAssumeCapacity(try std.fmt.bufPrint(
            args_delay_jitter[0..],
            "{d}ms",
            .{delay.jitter_ms},
        ));
        args.appendAssumeCapacity(try std.fmt.bufPrint(
            args_delay_correlation[0..],
            "{d}%",
            .{delay.correlation_pct},
        ));
        args.appendAssumeCapacity("distribution");
        args.appendAssumeCapacity("normal");
    }

    if (netem_rules.loss) |loss| {
        args.appendAssumeCapacity("loss");
        args.appendAssumeCapacity(try std.fmt.bufPrint(
            args_loss_pct[0..],
            "{d}%",
            .{loss.loss_pct},
        ));
        args.appendAssumeCapacity(try std.fmt.bufPrint(
            args_loss_correlation[0..],
            "{d}%",
            .{loss.correlation_pct},
        ));
    }

    // Everything stack-allocated and simple up to here. Now this feels like a shame just for
    // logging. We could log just the `self.netem_rules` stuct with `{any}`, but it's a bit verbose.
    const rules_formatted = try std.mem.join(allocator, " ", args.items);
    defer allocator.free(rules_formatted);
    log.info("syncing netem: {any}", .{netem_rules});

    try exec_silent(allocator, args.items);
}

fn exec_silent(allocator: std.mem.Allocator, argv: []const []const u8) !void {
    assert(argv.len > 0);

    var child = std.process.Child.init(argv, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Inherit;

    try child.spawn();

    errdefer {
        _ = child.kill() catch {};
    }

    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code == 0) {
                return;
            } else {
                log.err("{s} returned code {d}", .{ argv[0], code });
                return error.ProcessFailed;
            }
        },
        else => {
            log.err("{s} failed with: {any}", .{ argv[0], term });
            return error.ProcessFailed;
        },
    }
}
