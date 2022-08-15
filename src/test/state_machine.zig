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

            random,
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
        prng: std.rand.DefaultPrng,
        prepare_timestamp: u64 = 0,
        commit_timestamp: u64 = 0,

        callback: ?fn (state_machine: *StateMachine) void = null,
        callback_ticks: usize = 0,

        pub fn init(_: std.mem.Allocator, _: *Grid, options: Options) !StateMachine {
            return StateMachine{
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
        pub fn open(self: *StateMachine, callback: fn (*StateMachine) void) void {
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
            callback: fn (*StateMachine) void,
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
            _ = state_machine;
            _ = client;
            _ = input;
            _ = output;
            assert(op != 0);

            switch (operation) {
                .reserved, .root => unreachable,
                .register => return 0,
                .random => return 0,
            }
        }

        pub fn compact(
            state_machine: *StateMachine,
            callback: fn (*StateMachine) void,
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
            callback: fn (*StateMachine) void,
        ) void {
            assert(state_machine.callback == null);
            assert(state_machine.callback_ticks == 0);
            state_machine.callback = callback;
            state_machine.callback_ticks = state_machine.latency(state_machine.options.checkpoint_mean);
        }

        fn latency(state_machine: *StateMachine, mean: u64) u64 {
            return state_machine.prng.random().uintLessThan(u64, 2 * mean);
        }
    };
}
