//! Models a replica, which can be started, terminated, and restarted over time.
//!
//! As this can restart the underlying LoggedProcess over time, it needs to be managed, i.e. hold
//! on to an allocator.
//!
//! NOTE: In the future we might want to implement various upgrade procedures here.

const std = @import("std");
const LoggedProcess = @import("./logged_process.zig");

const assert = std.debug.assert;

const Self = @This();
pub const State = enum(u8) { initial, running, terminated, completed };

allocator: std.mem.Allocator,
executable_path: []const u8,
replica_addresses_arg: []const u8,
datafile: []const u8,
process: ?*LoggedProcess,

pub fn create(
    allocator: std.mem.Allocator,
    executable_path: []const u8,
    replica_addresses: []const u8,
    datafile: []const u8,
) !*Self {
    const self = try allocator.create(Self);
    errdefer allocator.destroy(self);

    const replica_addresses_arg = try std.fmt.allocPrint(
        allocator,
        "--addresses={s}",
        .{replica_addresses},
    );

    self.* = .{
        .allocator = allocator,
        .executable_path = executable_path,
        .replica_addresses_arg = replica_addresses_arg,
        .datafile = datafile,
        .process = null,
    };
    return self;
}

pub fn destroy(self: *Self) void {
    assert(self.state() == .initial or self.state() == .terminated);
    const allocator = self.allocator;
    if (self.process) |process| {
        process.destroy(allocator);
    }
    allocator.free(self.replica_addresses_arg);
    allocator.destroy(self);
}

pub fn state(self: *Self) State {
    if (self.process) |process| {
        switch (process.state()) {
            .running => return .running,
            .terminated => return .terminated,
            .completed => return .completed,
        }
    } else return .initial;
}

pub fn start(self: *Self) !void {
    assert(self.state() != .running);
    defer assert(self.state() == .running);

    if (self.process) |process| {
        process.destroy(self.allocator);
    }

    const argv = &.{
        self.executable_path,
        "start",
        self.replica_addresses_arg,
        self.datafile,
    };

    self.process = try LoggedProcess.spawn(self.allocator, argv);
}

pub fn terminate(
    self: *Self,
) !std.process.Child.Term {
    assert(self.state() == .running);
    defer assert(self.state() == .terminated);

    assert(self.process != null);
    return try self.process.?.terminate();
}
