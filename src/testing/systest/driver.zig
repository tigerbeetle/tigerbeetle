//! Controls a _driver_ process, which is a around a TigerBeetle client library in some language,
//! receiving events from and returning results back to the workload.
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

fn Slice(child: type) type {
    return @Type(.{
        .Pointer = .{
            .child = child,
            .size = .Slice,
            .is_const = true,
            .is_volatile = false,
            .alignment = @alignOf(child),
            .address_space = .generic,
            .is_allowzero = false,
            .sentinel = null,
        },
    });
}

pub const Request: type = blk: {
    const ops = std.enums.values(Operation);
    var fields: [ops.len]std.builtin.Type.UnionField = undefined;

    var i = 0;
    for (ops) |op| {
        fields[i] = .{
            .name = @tagName(op),
            .type = Slice(Event(op)),
            .alignment = @alignOf(Slice(Event(op))),
        };
        i += 1;
    }

    break :blk @Type(.{ .Union = .{
        .fields = &fields,
        .layout = .auto,
        .decls = &.{},
        .tag_type = Operation,
    } });
};

pub const Response = blk: {
    const ops = std.enums.values(Operation);
    var fields: [ops.len]std.builtin.Type.UnionField = undefined;

    var i = 0;
    for (ops) |op| {
        fields[i] = .{
            .name = @tagName(op),
            .type = Slice(Result(op)),
            .alignment = @alignOf(Result(op)),
        };
        i += 1;
    }

    break :blk @Type(.{ .Union = .{
        .fields = &fields,
        .layout = .auto,
        .decls = &.{},
        .tag_type = Operation,
    } });
};

const Self = @This();
pub const State = enum(u8) { running, terminated };
const AtomicState = std.atomic.Value(State);

pub const events_count_max = 1024;

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

    // Kill the process.
    _ = kill: {
        if (builtin.os.tag == .windows) {
            const exit_code = 1;
            break :kill std.os.windows.TerminateProcess(self.child.id, exit_code);
        } else {
            break :kill std.posix.kill(self.child.id, std.posix.SIG.KILL);
        }
    } catch |err| {
        log.err(
            "failed to kill process {d}: {any}\n",
            .{ self.child.id, err },
        );
    };

    // Await the terminated process.
    const term = self.child.wait() catch unreachable;

    self.current_state.store(.terminated, .seq_cst);

    return term;
}

pub fn send(
    self: *Self,
    comptime op: Operation,
    events: []const Event(op),
) !void {
    assert(events.len <= events_count_max);

    const writer = self.child.stdin.?.writer().any();

    try writer.writeInt(u8, @intFromEnum(op), .little);
    try writer.writeInt(u32, @intCast(events.len), .little);

    const bytes = std.mem.sliceAsBytes(events);
    try writer.writeAll(bytes);
}

// TODO, ignore this for now
pub fn receive(
    self: *Self,
    op: Operation,
    buffer: []Result(op),
) ![]Result(op) {
    assert(buffer.len <= events_count_max);

    const count: u32 = undefined;

    if (try self.child.stdout.?.read(&.{count}) != @sizeOf(u32)) {
        return error.ResponseReadError;
    }

    const slice = buffer[0..count];
    if (try self.child.stdout.?.read(slice) != @sizeOf(Response) * count) {
        return error.ResponseReadError;
    }

    return slice;
}
