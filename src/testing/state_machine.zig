const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.state_machine);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const GrooveType = @import("../lsm/groove.zig").GrooveType;
const ForestType = @import("../lsm/forest.zig").ForestType;

pub fn StateMachineType(
    comptime Storage: type,
    comptime config: constants.StateMachineConfig,
) type {
    return struct {
        const StateMachine = @This();
        const Grid = @import("../lsm/grid.zig").GridType(Storage);

        pub const Workload = WorkloadType(StateMachine);

        pub const Operation = enum(u8) {
            echo = config.vsr_operations_reserved + 0,
        };

        pub const Options = struct {
            lsm_forest_node_count: u32,
        };

        pub const Forest = ForestType(Storage, .{ .things = ThingGroove });

        const ThingGroove = GrooveType(
            Storage,
            Thing,
            .{
                .ids = .{
                    .timestamp = 1,
                    .id = 2,
                    .value = 3,
                },
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple,
                    .id = config.lsm_batch_multiple,
                    .value = config.lsm_batch_multiple,
                },
                .ignored = &[_][]const u8{},
                .derived = .{},
            },
        );

        const Thing = extern struct {
            timestamp: u64,
            value: u64,
            id: u128,
        };

        options: Options,
        forest: Forest,

        prepare_timestamp: u64 = 0,
        commit_timestamp: u64 = 0,

        prefetch_context: ThingGroove.PrefetchContext = undefined,
        callback: ?fn (state_machine: *StateMachine) void = null,

        pub fn init(allocator: std.mem.Allocator, grid: *Grid, options: Options) !StateMachine {
            var forest = try Forest.init(
                allocator,
                grid,
                options.lsm_forest_node_count,
                .{
                    .things = .{
                        .prefetch_entries_max = 1,
                        .tree_options_object = .{ .cache_entries_max = 2048 },
                        .tree_options_id = .{ .cache_entries_max = 2048 },
                        .tree_options_index = .{ .value = .{} },
                    },
                },
            );
            errdefer forest.deinit(allocator);

            return StateMachine{
                .options = options,
                .forest = forest,
            };
        }

        pub fn deinit(state_machine: *StateMachine, allocator: std.mem.Allocator) void {
            state_machine.forest.deinit(allocator);
        }

        pub fn reset(state_machine: *StateMachine) void {
            state_machine.forest.reset();

            state_machine.* = .{
                .options = state_machine.options,
                .forest = state_machine.forest,
            };
        }

        pub fn open(state_machine: *StateMachine, callback: fn (*StateMachine) void) void {
            assert(state_machine.callback == null);

            state_machine.callback = callback;
            state_machine.forest.open(open_callback);
        }

        fn open_callback(forest: *Forest) void {
            const state_machine = @fieldParentPtr(StateMachine, "forest", forest);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }

        pub fn prepare(
            state_machine: *StateMachine,
            operation: Operation,
            input: []align(16) u8,
        ) void {
            _ = state_machine;
            _ = operation;
            _ = input;
        }

        pub fn prefetch(
            state_machine: *StateMachine,
            callback: fn (*StateMachine) void,
            op: u64,
            operation: Operation,
            input: []align(16) const u8,
        ) void {
            _ = op;
            _ = operation;
            _ = input;

            assert(state_machine.callback == null);
            state_machine.callback = callback;

            // TODO(Snapshots) Pass in the target snapshot.
            state_machine.forest.grooves.things.prefetch_setup(null);
            state_machine.forest.grooves.things.prefetch_enqueue(op);
            state_machine.forest.grooves.things.prefetch(prefetch_callback, &state_machine.prefetch_context);
        }

        fn prefetch_callback(completion: *ThingGroove.PrefetchContext) void {
            const state_machine = @fieldParentPtr(StateMachine, "prefetch_context", completion);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }

        pub fn commit(
            state_machine: *StateMachine,
            client: u128,
            op: u64,
            timestamp: u64,
            operation: Operation,
            input: []align(16) const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            _ = client;
            assert(op != 0);

            switch (operation) {
                .echo => {
                    const thing = state_machine.forest.grooves.things.get(op);
                    assert(thing == null);

                    state_machine.forest.grooves.things.put(&.{
                        .timestamp = timestamp,
                        .id = op,
                        .value = @truncate(u64, vsr.checksum(input)),
                    });

                    stdx.copy_disjoint(.inexact, u8, output, input);
                    return input.len;
                },
            }
        }

        pub fn compact(
            state_machine: *StateMachine,
            callback: fn (*StateMachine) void,
            op: u64,
        ) void {
            assert(op != 0);
            assert(state_machine.callback == null);

            state_machine.callback = callback;
            state_machine.forest.compact(compact_callback, op);
        }

        fn compact_callback(forest: *Forest) void {
            const state_machine = @fieldParentPtr(StateMachine, "forest", forest);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }

        pub fn checkpoint(
            state_machine: *StateMachine,
            callback: fn (*StateMachine) void,
        ) void {
            assert(state_machine.callback == null);

            state_machine.callback = callback;
            state_machine.forest.checkpoint(checkpoint_callback);
        }

        fn checkpoint_callback(forest: *Forest) void {
            const state_machine = @fieldParentPtr(StateMachine, "forest", forest);
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
            operation: StateMachine.Operation,
            timestamp: u64,
            request_body: []align(@alignOf(vsr.Header)) const u8,
            reply_body: []align(@alignOf(vsr.Header)) const u8,
        ) void {
            _ = client_index;
            _ = timestamp;

            workload.requests_delivered += 1;
            assert(workload.requests_delivered <= workload.requests_sent);

            assert(operation == .echo);
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
