const std = @import("std");
const allocator = std.testing.allocator;
const assert = std.debug.assert;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;
const Storage = @import("../test/storage.zig").Storage;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = config.message_body_size_max,
});

const GridType = @import("grid.zig").GridType;
const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

pub fn FuzzRunner(
    comptime Tree: type,
    comptime TreeContext: type,
) type {
    return struct {
        const Runner = @This();

        const cluster = 32;
        const replica = 4;
        // TODO Is this appropriate for the number of fuzz_ops we want to run?
        pub const size_max = vsr.Zone.superblock.size().? +
            vsr.Zone.wal_headers.size().? +
            vsr.Zone.wal_prepares.size().? +
            1024 * 1024 * 1024;

        const Callback = fn (*Runner) void;
        const State = enum {
            initialized,
            superblock_format,
            superblock_open,
            tree_open,
            running,
            tree_access,
            tree_compact,
            tree_checkpoint,
            superblock_checkpoint,
            stopped,
        };

        state: State,
        callback: ?Callback,
        pending: usize,
        storage: *Storage,
        message_pool: MessagePool,
        superblock: SuperBlock,
        superblock_context: SuperBlock.Context,
        grid: Grid,
        tree: Tree,
        tree_context: TreeContext,
        checkpoint_op: ?u64,

        pub fn run(runner: *Runner, callback: Callback, storage: *Storage) !void {
            runner.state = .initialized;
            runner.callback = null;
            runner.pending = 0;
            runner.storage = storage;
            runner.checkpoint_op = null;

            runner.message_pool = try MessagePool.init(allocator, .replica);
            defer runner.message_pool.deinit(allocator);

            runner.superblock = try SuperBlock.init(allocator, storage, &runner.message_pool);
            defer runner.superblock.deinit(allocator);

            runner.grid = try Grid.init(allocator, &runner.superblock);
            defer runner.grid.deinit(allocator);

            runner.tree = undefined;
            runner.tree_context = try TreeContext.init();
            defer runner.tree_context.deinit();

            runner.format_then_open(callback);
            while (runner.state != .stopped) {
                runner.grid.tick();
                runner.storage.tick();
            }
        }

        fn start_transition(runner: *Runner, current: State, next: State, callback: Callback) void {
            assert(runner.callback == null);
            assert(runner.state == current);

            runner.callback = callback;
            runner.state = next;
        }

        fn intermediate_transition(runner: *Runner, current: State, next: State) void {
            assert(runner.callback != null);
            assert(runner.state == current);

            runner.state = next;
        }

        fn finish_transition(runner: *Runner, current: State, next: State) void {
            assert(runner.callback != null);
            assert(runner.state == current);

            const callback = runner.callback.?;
            runner.callback = null;
            runner.state = next;
            callback(runner);
        }

        pub fn format_then_open(runner: *Runner, callback: Callback) void {
            runner.start_transition(.initialized, .superblock_format, callback);
            runner.superblock.format(superblock_format_callback, &runner.superblock_context, .{
                .cluster = cluster,
                .replica = replica,
                .size_max = size_max,
            });
        }

        fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
            const runner = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            runner.intermediate_transition(.superblock_format, .superblock_open);
            runner.superblock.open(superblock_open_callback, &runner.superblock_context);
        }

        fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
            const runner = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            runner.intermediate_transition(.superblock_open, .tree_open);

            runner.tree_context.create(&runner.tree, &runner.grid);
            runner.tree.open(tree_open_callback);
        }

        fn tree_open_callback(tree: *Tree) void {
            const runner = @fieldParentPtr(@This(), "tree", tree);
            runner.finish_transition(.tree_open, .running);
        }

        pub fn compact(runner: *Runner, callback: Callback, op: u64) void {
            runner.start_transition(.running, .tree_compact, callback);
            runner.tree.compact(tree_compact_callback, op);
        }

        fn tree_compact_callback(tree: *Tree) void {
            const runner = @fieldParentPtr(@This(), "tree", tree);
            runner.finish_transition(.tree_compact, .running);
        }

        pub fn checkpoint(runner: *Runner, callback: Callback, op: u64) void {
            runner.start_transition(.running, .tree_checkpoint, callback);

            assert(runner.checkpoint_op == null);
            runner.checkpoint_op = op - config.lsm_batch_multiple;
            runner.tree.compact(tree_checkpoint_callback, op);
        }

        fn tree_checkpoint_callback(tree: *Tree) void {
            const runner = @fieldParentPtr(@This(), "tree", tree);
            runner.intermediate_transition(.tree_checkpoint, .superblock_checkpoint);

            const op = runner.checkpoint_op.?;
            runner.checkpoint_op = null;
            runner.superblock.checkpoint(superblock_checkpoint_callback, &runner.superblock_context, .{
                .commit_min_checksum = runner.superblock.working.vsr_state.commit_min_checksum + 1,
                .commit_min = op,
                .commit_max = op + 1,
                .view_normal = 0,
                .view = 0,
            });
        }

        fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
            const runner = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            runner.finish_transition(.superblock_checkpoint, .running);
        }

        pub fn access(runner: *Runner, callback: Callback, arg: anytype) void {
            runner.start_transition(.running, .tree_access, callback);
            runner.tree_context.access(access_callback, &runner.tree, arg);
        }

        fn access_callback(tree_context: *TreeContext) void {
            const runner = @fieldParentPtr(Runner, "tree_context", tree_context);
            runner.finish_transition(.tree_access, .running);
        }

        pub fn stop(runner: *Runner) void {
            assert(runner.callback == null);
            assert(runner.state == .running);
            runner.state = .stopped;
        }
    };
}
