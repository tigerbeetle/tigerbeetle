//! Controls a _driver_ process, which is a wrapper around a TigerBeetle client library in some
//! language, receiving events from and returning results back to the workload.
//!
//! The workload and drivers communicate with a binary protocol over stdio. The protocol is based
//! on the extern structs in `src/tigerbeetle.zig` and `src/state_machine.zig`, and it works like
//! this:
//!
//! 1. Workload sends a request, which is:
//!    * the _operation_ (1 byte),
//!    * the _event count_ (4 bytes), and
//!    * the events (event count * size of event).
//! 2. The driver uses its client to submit those events. When receiving results, it sends them
//!    back on its stdout as:
//!    * the _operation_ (1 byte)
//!    * the _result count_ (4 bytes), and
//!    * the results (resulgt count * size of result pair), where each pair holds an index and a
//!      result enum value (see `src/tigerbeetle.zig`)
//! 3. The workload receives the results, and expects them to be of the same operation type as
//!    originally requested. There might be fewer results than events, because clients can omit
//!    .ok results.

const std = @import("std");
const builtin = @import("builtin");
const constants = @import("../../constants.zig");
const StateMachineType = @import("../../state_machine.zig").StateMachineType;

const log = std.log.scoped(.driver);
const assert = std.debug.assert;

const StateMachine = StateMachineType(void, constants.state_machine_config);
pub const Operation = StateMachine.Operation;
pub const Event = StateMachine.Event;
pub const Result = StateMachine.Result;

const Self = @This();
pub const State = enum(u8) { running, terminated };
const AtomicState = std.atomic.Value(State);

pub const events_count_max = 8190;

// Allocated by init
child: std.process.Child,
// Lifecycle state
current_state: AtomicState,

pub fn spawn(
    allocator: std.mem.Allocator,
    argv: []const []const u8,
) !*Self {
    comptime assert(builtin.target.cpu.arch.endian() == .little);

    const self = try allocator.create(Self);
    errdefer allocator.destroy(self);

    self.* = .{
        .current_state = AtomicState.init(.running),
        .child = std.process.Child.init(argv, allocator),
    };

    self.child.stdin_behavior = .Pipe;
    self.child.stdout_behavior = .Pipe;
    self.child.stderr_behavior = .Inherit;

    try self.child.spawn();

    errdefer {
        _ = self.child.kill() catch {};
    }

    return self;
}

pub fn destroy(self: *Self, allocator: std.mem.Allocator) void {
    assert(self.state() == .terminated);
    allocator.destroy(self);
}

pub fn state(self: *Self) State {
    return self.current_state.load(.seq_cst);
}

pub fn terminate(
    self: *Self,
) !std.process.Child.Term {
    assert(self.state() == .running);
    defer assert(self.state() == .terminated);

    const term = self.child.kill();
    self.current_state.store(.terminated, .seq_cst);

    return term;
}

pub fn send(
    self: *const Self,
    comptime op: Operation,
    events: []const Event(op),
) !void {
    assert(events.len <= events_count_max);

    const writer = self.child.stdin.?.writer().any();

    try writer.writeInt(u8, @intFromEnum(op), .little);
    try writer.writeInt(u32, @intCast(events.len), .little);

    const bytes: []const u8 = std.mem.sliceAsBytes(events);
    try writer.writeAll(bytes);
}

pub fn receive(
    self: *const Self,
    comptime op: Operation,
    results: []Result(op),
) ![]Result(op) {
    assert(results.len <= events_count_max);

    const results_count = try self.child.stdout.?.reader().readInt(u32, .little);
    assert(results_count <= results.len);

    const buf: []u8 = std.mem.sliceAsBytes(results[0..results_count]);
    assert(try self.child.stdout.?.reader().readAtLeast(buf, buf.len) == buf.len);

    return results[0..results_count];
}
