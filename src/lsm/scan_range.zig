const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;

const Direction = @import("../direction.zig").Direction;
const Pending = error{Pending};

/// Apply a custom filter and/or stop-condition when scanning a range of values.
pub const EvaluateNext = enum {
    include_and_continue,
    include_and_stop,
    exclude_and_continue,
    exclude_and_stop,
};

pub fn ScanRangeType(
    comptime Tree: type,
    comptime Storage: type,
    comptime EvaluatorContext: type,
    /// Decides whether to exclude a value or stop scanning.
    /// Useful to implement filters over range scans for custom logic (e.g. expired transfers).
    comptime value_next: fn (
        context: EvaluatorContext,
        value: *const Tree.Table.Value,
    ) callconv(.@"inline") EvaluateNext,
    /// Extracts the ObjectTree's timestamp from the table value.
    comptime timestamp_from_value: fn (
        context: EvaluatorContext,
        value: *const Tree.Table.Value,
    ) callconv(.@"inline") u64,
) type {
    return struct {
        const ScanRange = @This();

        pub const Callback = *const fn (*Context, *ScanRange) void;
        pub const Context = struct {
            callback: Callback,
        };

        const ScanTree = ScanTreeType(*Context, Tree, Storage);

        evaluator_context: EvaluatorContext,
        scan_tree: ScanTree,

        pub fn init(
            evaluator_context: EvaluatorContext,
            tree: *Tree,
            buffer: *const ScanBuffer,
            snapshot_: u64,
            key_min: Tree.Table.Key,
            key_max: Tree.Table.Key,
            direction: Direction,
        ) ScanRange {
            return .{
                .evaluator_context = evaluator_context,
                .scan_tree = ScanTree.init(
                    tree,
                    buffer,
                    snapshot_,
                    key_min,
                    key_max,
                    direction,
                ),
            };
        }

        pub fn read(scan: *ScanRange, context: *Context) void {
            scan.scan_tree.read(context, on_read_callback);
        }

        fn on_read_callback(context: *Context, ptr: *ScanTree) void {
            const parent: *ScanRange = @fieldParentPtr("scan_tree", ptr);
            context.callback(context, parent);
        }

        pub fn next(scan: *ScanRange) Pending!?u64 {
            while (try scan.scan_tree.next()) |value| {
                if (Tree.Table.tombstone(&value)) continue;

                switch (value_next(scan.evaluator_context, &value)) {
                    .include_and_continue => {},
                    .include_and_stop => scan.scan_tree.abort(),
                    .exclude_and_continue => continue,
                    .exclude_and_stop => {
                        scan.scan_tree.abort();
                        break;
                    },
                }

                return timestamp_from_value(scan.evaluator_context, &value);
            }

            return null;
        }

        pub fn snapshot(scan: *const ScanRange) u64 {
            return scan.scan_tree.snapshot;
        }
    };
}
