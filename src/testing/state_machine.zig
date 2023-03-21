const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.state_machine);

pub fn StateMachineType(comptime Storage: type, comptime constants_: struct {
    message_body_size_max: usize,
    lsm_batch_multiple: usize,
}) type {
    _ = Storage;
    _ = constants_;

    return struct {
        const StateMachine = @This();
        const Grid = @import("../lsm/grid.zig").GridType(Storage);

        pub const Workload = WorkloadType(StateMachine);

        pub const Operation = enum(u8) {
            /// Operations reserved by VR protocol (for all state machines):
            reserved,
            root,
            register,

            echo,
        };

        pub const Options = struct {};

        options: Options,
        grid: *Grid,
        grid_block: Grid.BlockPtr,
        grid_write: Grid.Write = undefined,
        prepare_timestamp: u64 = 0,
        commit_timestamp: u64 = 0,

        callback: ?fn (state_machine: *StateMachine) void = null,

        pub fn init(allocator: std.mem.Allocator, grid: *Grid, options: Options) !StateMachine {
            const grid_block = try allocator.alignedAlloc(
                u8,
                constants.sector_size,
                constants.block_size,
            );
            errdefer allocator.free(grid_block);
            std.mem.set(u8, grid_block, 0);

            return StateMachine{
                .options = options,
                .grid = grid,
                .grid_block = grid_block[0..constants.block_size],
            };
        }

        pub fn deinit(state_machine: *StateMachine, allocator: std.mem.Allocator) void {
            allocator.free(state_machine.grid_block);
        }

        // TODO Grid.next_tick
        pub fn open(state_machine: *StateMachine, callback: fn (*StateMachine) void) void {
            callback(state_machine);
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

            state_machine.next_tick(callback);
        }

        pub fn commit(
            state_machine: *StateMachine,
            client: u128,
            op: u64,
            timestamp: u64,
            operation: Operation,
            input: []const u8,
            output: []u8,
        ) usize {
            _ = state_machine;
            _ = client;
            _ = timestamp;
            _ = input;
            _ = output;
            assert(op != 0);

            switch (operation) {
                .reserved, .root => unreachable,
                .register => return 0,
                .echo => {
                    stdx.copy_disjoint(.exact, u8, output, input);
                    return input.len;
                },
            }
        }

        // TODO(Grid Recovery): Actually write blocks so that this state machine can be used
        // to test grid recovery.
        pub fn compact(
            state_machine: *StateMachine,
            callback: fn (*StateMachine) void,
            op: u64,
        ) void {
            _ = op;
            state_machine.next_tick(callback);
        }

        pub fn checkpoint(
            state_machine: *StateMachine,
            callback: fn (*StateMachine) void,
        ) void {
            state_machine.next_tick(callback);
        }

        // TODO Replace with Grid.next_tick()
        fn next_tick(state_machine: *StateMachine, callback: fn (*StateMachine) void) void {
            // TODO This is a hack to defer till the next tick; use Grid.next_tick instead.
            var free_set = state_machine.grid.superblock.free_set;
            const reservation = free_set.reserve(1).?;
            defer free_set.forfeit(reservation);

            const address = free_set.acquire(reservation).?;
            const header = std.mem.bytesAsValue(
                vsr.Header,
                state_machine.grid_block[0..@sizeOf(vsr.Header)],
            );
            header.op = address;

            assert(state_machine.callback == null);
            state_machine.callback = callback;
            state_machine.grid.write_block(
                .disk,
                next_tick_callback,
                &state_machine.grid_write,
                &state_machine.grid_block,
                address,
            );
        }

        fn next_tick_callback(write: *Grid.Write) void {
            const state_machine = @fieldParentPtr(StateMachine, "grid_write", write);
            const callback = state_machine.callback.?;
            state_machine.callback = null;
            callback(state_machine);
        }
    };
}

fn WorkloadType(comptime StateMachine: type) type {
    return struct {
        const Workload = @This();

        random: std.rand.Random,
        requests_sent: usize = 0,
        requests_delivered: usize = 0,

        pub fn init(
            allocator: std.mem.Allocator,
            random: std.rand.Random,
            options: Options,
        ) !Workload {
            _ = allocator;
            _ = options;

            return Workload{
                .random = random,
            };
        }

        pub fn deinit(workload: *Workload, allocator: std.mem.Allocator) void {
            _ = workload;
            _ = allocator;
        }

        pub fn done(workload: *const Workload) bool {
            return workload.requests_sent == workload.requests_delivered;
        }

        pub fn build_request(
            workload: *Workload,
            client_index: usize,
            body: []align(@alignOf(vsr.Header)) u8,
        ) struct {
            operation: StateMachine.Operation,
            size: usize,
        } {
            _ = client_index;

            workload.requests_sent += 1;

            // +1 for inclusive limit.
            const size = workload.random.uintLessThan(usize, constants.message_body_size_max + 1);
            workload.random.bytes(body[0..size]);

            return .{
                .operation = .echo,
                .size = size,
            };
        }

        pub fn on_reply(
            workload: *Workload,
            client_index: usize,
            operation: vsr.Operation,
            timestamp: u64,
            request_body: []align(@alignOf(vsr.Header)) const u8,
            reply_body: []align(@alignOf(vsr.Header)) const u8,
        ) void {
            _ = workload;
            _ = client_index;
            _ = timestamp;

            workload.requests_delivered += 1;
            assert(workload.requests_delivered <= workload.requests_sent);

            assert(operation.cast(StateMachine) == .echo);
            assert(std.mem.eql(u8, request_body, reply_body));
        }

        pub const Options = struct {
            pub fn generate(random: std.rand.Random, options: struct {
                client_count: usize,
                in_flight_max: usize,
            }) Options {
                _ = random;
                _ = options;
                return .{};
            }
        };
    };
}
