const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.state_machine);

pub const StateMachine = struct {
    pub const Operation = enum(u8) {
        /// Operations reserved by VR protocol (for all state machines):
        reserved,
        root,
        register,

        hash,
    };

    pub const Config = struct {
        seed: u64,
    };

    state: u128,
    prepare_timestamp: u64 = 0,
    commit_timestamp: u64 = 0,

    callback: ?fn(*StateMachine) void = null,
    callback_ticks: usize = 0,

    pub fn init(_: std.mem.Allocator, config: Config) !StateMachine {
        return StateMachine{ .state = hash(0, std.mem.asBytes(&config.seed)) };
    }

    pub fn deinit(_: *StateMachine) void {}

    // TODO Is this part of StateMachine's API?
    pub fn tick(self: *StateMachine) void {
        if (self.callback) |callback| {
            if (self.callback_ticks == 0) {
                self.callback = null;
                callback(self);
            } else {
                self.callback_ticks -= 1;
            }
        }
    }

    pub fn prepare(
        state_machine: *StateMachine,
        operation: Operation,
        input: []u8,
    ) u64 {
        _ = operation;
        _ = input;

        return state_machine.prepare_timestamp;
    }

    pub fn prefetch(self: *StateMachine, op_number: u64, callback: fn(*StateMachine) void) void {
        _ = op_number;
        assert(self.callback == null);
        assert(self.callback_ticks == 0);
        self.callback = callback;
        self.callback_ticks = 100; // TODO vary delay
    }

    pub fn compact(self: *StateMachine, op_number: u64, callback: fn(*StateMachine) void) void {
        _ = op_number;
        assert(self.callback == null);
        assert(self.callback_ticks == 0);
        self.callback = callback;
        self.callback_ticks = 100; // TODO vary delay
    }

    pub fn checkpoint(self: *StateMachine, op_number: u64, callback: fn(*StateMachine) void) void {
        _ = op_number;
        assert(self.callback == null);
        assert(self.callback_ticks == 0);
        self.callback = callback;
        self.callback_ticks = 100; // TODO vary delay
    }

    pub fn commit(
        state_machine: *StateMachine,
        client: u128,
        op_number: u64,
        operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        _ = op_number;

        switch (operation) {
            .reserved, .root => unreachable,
            .register => return 0,

            // TODO: instead of always using the first 32 bytes of the output
            // buffer, get tricky and use a random but deterministic slice
            // of it, filling the rest with 0s.
            .hash => {
                // Fold the input into our current state, creating a hash chain.
                // Hash the input with the client ID since small inputs may collide across clients.
                const client_input = hash(client, input);
                const new_state = hash(state_machine.state, std.mem.asBytes(&client_input));

                log.debug("state={x} input={x} input.len={} new state={x}", .{
                    state_machine.state,
                    client_input,
                    input.len,
                    new_state,
                });

                state_machine.state = new_state;
                std.mem.copy(u8, output, std.mem.asBytes(&state_machine.state));
                return @sizeOf(@TypeOf(state_machine.state));
            },
        }
    }

    pub fn hash(state: u128, input: []const u8) u128 {
        var key: [32]u8 = [_]u8{0} ** 32;
        std.mem.copy(u8, key[0..16], std.mem.asBytes(&state));
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(input, &target, .{ .key = key });
        return @bitCast(u128, target[0..16].*);
    }
};
