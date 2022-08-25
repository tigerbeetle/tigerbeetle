const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.state_machine);

pub fn StateMachineType(comptime Storage: type) type {
    _ = Storage;
    return struct {
        const StateMachine = @This();
        const Grid = @import("../lsm/grid.zig").GridType(Storage);

        pub const Operation = enum(u8) {
            /// Operations reserved by VR protocol (for all state machines):
            reserved,
            root,
            register,

            hash,
        };

        /// Minimum/mean number of ticks to perform the specified operation.
        /// Each mean must be greater-or-equal-to their respective minimum.
        pub const Options = struct {
            seed: u64,
            prefetch_mean: u64,
            compact_mean: u64,
            checkpoint_mean: u64,
        };

        options: Options,
        state: u128,
        prng: std.rand.DefaultPrng,
        prepare_timestamp: u64 = 0,
        commit_timestamp: u64 = 0,

        callback: ?*const fn (state_machine: *StateMachine) void = null,
        callback_ticks: usize = 0,

        pub fn init(_: std.mem.Allocator, _: *Grid, options: Options) !StateMachine {
            return StateMachine{
                .state = hash(0, std.mem.asBytes(&options.seed)),
                .options = options,
                .prng = std.rand.DefaultPrng.init(options.seed),
            };
        }

        pub fn deinit(_: *StateMachine, _: std.mem.Allocator) void {}

        pub fn tick(state_machine: *StateMachine) void {
            if (state_machine.callback) |callback| {
                if (state_machine.callback_ticks == 0) {
                    state_machine.callback = null;
                    callback(state_machine);
                } else {
                    state_machine.callback_ticks -= 1;
                }
            }
        }

        /// Don't add latency to `StateMachine.open`: the simulator calls it synchronously during
        /// replica setup.
        pub fn open(self: *StateMachine, callback: *const fn (*StateMachine) void) void {
            callback(self);
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

        pub fn prefetch(
            state_machine: *StateMachine,
            callback: *const fn (*StateMachine) void,
            op: u64,
            operation: Operation,
            input: []const u8,
        ) void {
            _ = op;
            _ = operation;
            _ = input;
            assert(state_machine.callback == null);
            assert(state_machine.callback_ticks == 0);
            state_machine.callback = callback;
            state_machine.callback_ticks = state_machine.latency(state_machine.options.prefetch_mean);
        }

        pub fn commit(
            state_machine: *StateMachine,
            client: u128,
            op: u64,
            operation: Operation,
            input: []const u8,
            output: []u8,
        ) usize {
            _ = op;

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

        pub fn compact(
            state_machine: *StateMachine,
            callback: *const fn (*StateMachine) void,
            op: u64,
        ) void {
            _ = op;
            assert(state_machine.callback == null);
            assert(state_machine.callback_ticks == 0);
            state_machine.callback = callback;
            state_machine.callback_ticks = state_machine.latency(state_machine.options.compact_mean);
        }

        pub fn checkpoint(
            state_machine: *StateMachine,
            callback: *const fn (*StateMachine) void,
        ) void {
            assert(state_machine.callback == null);
            assert(state_machine.callback_ticks == 0);
            state_machine.callback = callback;
            state_machine.callback_ticks = state_machine.latency(state_machine.options.checkpoint_mean);
        }

        pub fn hash(state: u128, input: []const u8) u128 {
            var key: [32]u8 = [_]u8{0} ** 32;
            std.mem.copy(u8, key[0..16], std.mem.asBytes(&state));
            var target: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(input, &target, .{ .key = key });
            return @bitCast(u128, target[0..16].*);
        }

        fn latency(state_machine: *StateMachine, mean: u64) u64 {
            return state_machine.prng.random().uintLessThan(u64, 2 * mean);
        }
    };
}
