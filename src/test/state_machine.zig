const std = @import("std");

const log = std.log.scoped(.state_machine);

pub const StateMachine = struct {
    pub const Operation = enum(u8) {
        /// Operations reserved by VR protocol (for all state machines):
        reserved,
        init,
        register,

        hash,
    };

    state: u128,

    pub fn init(seed: u64) StateMachine {
        return .{ .state = hash(0, std.mem.asBytes(&seed)) };
    }

    pub fn deinit(state_machine: *StateMachine) void {}

    pub fn prepare(
        state_machine: *StateMachine,
        realtime: i64,
        operation: Operation,
        input: []u8,
    ) void {
        // TODO: use realtime in some way to test the system
    }

    pub fn commit(
        state_machine: *StateMachine,
        operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        switch (operation) {
            .reserved, .init => unreachable,
            .register => return 0,

            // TODO: instead of always using the first 32 bytes of the output
            // buffer, get tricky and use a random but deterministic slice
            // of it, filling the rest with 0s.
            .hash => {
                // Fold the input into our current state, creating a hash chain
                const new_state = hash(state_machine.state, input);

                log.debug("state={} input={} input.len={} new state={}", .{
                    state_machine.state,
                    hash(0, input),
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
