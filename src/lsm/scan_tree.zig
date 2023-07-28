const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.scan);
const tracer = @import("../tracer.zig");

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const lsm = @import("tree.zig");
const schema = @import("schema.zig");
const binary_search = @import("binary_search.zig");
const k_way_merge = @import("k_way_merge.zig");

const BinarySearchRange = binary_search.BinarySearchRange;
const Direction = @import("direction.zig").Direction;
const GridType = @import("grid.zig").GridType;
const TableInfoType = @import("manifest.zig").TableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const ScanBufferType = @import("scan_buffer.zig").ScanBufferType;
const LevelTableValueBlockIteratorType =
    @import("level_data_iterator.zig").LevelTableValueBlockIteratorType;

/// Scans a range of keys over a Tree, in ascending or descending order.
pub fn ScanTreeType(
    comptime Context: type,
    comptime Tree_: type,
    comptime Storage: type,
) type {
    return struct {
        const ScanTree = @This();

        pub const Callback = *const fn (context: Context, scan: *ScanTree) void;

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;

        const TableInfo = TableInfoType(Table);
        const Manifest = ManifestType(Table, Storage);

        const ScanBuffer = ScanBufferType(Storage);

        pub const Tree = Tree_;
        const Table = Tree.Table;
        const Key = Table.Key;
        const Value = Table.Value;
        const compare_keys = Table.compare_keys;
        const key_from_value = Table.key_from_value;

        const KWayMergeIterator = KWayMergeIteratorType(ScanTree);
        const ScanTreeLevel = ScanTreeLevelType(ScanTree, Storage);

        tree: *Tree,
        buffer: *const ScanBuffer,
        next_tick: Grid.NextTick = undefined,

        direction: Direction,
        key_min: Key,
        key_max: Key,
        snapshot: u64,

        // TODO It's a temporary solution until we can iterate over table mutable in sorted order.
        table_mutable_values: []const Table.Value,
        table_mutable_cursor: RangeCursor,

        table_immutable_cursor: RangeCursor,

        state: union(enum) {
            idle: void,
            seeking,
            fetching: struct {
                context: Context,
                callback: Callback,
                pending_count: u32,
            },
        },
        levels: [constants.lsm_levels]ScanTreeLevel,

        merge_iterator: ?KWayMergeIterator,

        pub fn init(
            tree: *Tree,
            buffer: *const ScanBuffer,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) ScanTree {
            assert(Table.compare_keys(key_min, key_max) != .gt);

            // TODO It's a temporary solution until we can iterate over table mutable
            // in sorted order.
            const table_mutable_values = tree.table_mutable.sort_into_values();
            const table_mutable_cursor = RangeCursor.init(
                binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    table_mutable_values,
                    key_min,
                    key_max,
                ),
                direction,
            );

            const table_immutable_cursor = RangeCursor.init(
                binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    if (!tree.table_immutable.free and
                        tree.table_immutable.snapshot_min <= snapshot)
                        tree.table_immutable.values
                    else
                        &[_]Value{},
                    key_min,
                    key_max,
                ),
                direction,
            );

            return .{
                .tree = tree,
                .buffer = buffer,

                .state = .idle,
                .snapshot = snapshot,
                .key_min = key_min,
                .key_max = key_max,
                .direction = direction,

                .table_mutable_values = table_mutable_values,
                .table_mutable_cursor = table_mutable_cursor,
                .table_immutable_cursor = table_immutable_cursor,
                .levels = undefined,
                .merge_iterator = null,
            };
        }

        pub fn read(scan: *ScanTree, context: Context, callback: Callback) void {
            switch (scan.state) {
                .idle => for (scan.levels) |*level, i| {
                    level.* = ScanTreeLevel.init(
                        scan,
                        scan.buffer.levels[i],
                        @intCast(u8, i),
                    );
                    level.next_table();
                },
                .seeking => {},
                .fetching => unreachable,
            }

            scan.state = .{
                .fetching = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            // Track an extra "level" that will finish after the loop.
            // This allows us to call the callback if there's no more levels to fetch.
            scan.state.fetching.pending_count += 1;

            for (scan.levels) |*level| {
                switch (level.values) {
                    .fetching => {
                        scan.state.fetching.pending_count += 1;
                        level.fetch();
                    },
                    else => {},
                }
            }

            scan.state.fetching.pending_count -= 1;
            if (scan.state.fetching.pending_count == 0) {
                scan.tree.grid.on_next_tick(
                    on_next_tick,
                    &scan.next_tick,
                );
            }
        }

        pub fn next(scan: *ScanTree) error{ReadAgain}!?Value {
            return if (scan.merge_iterator) |*it|
                it.pop() catch |err| switch (err) {
                    error.Drained => error.ReadAgain,
                }
            else
                error.ReadAgain;
        }

        fn on_next_tick(next_tick: *Grid.NextTick) void {
            const scan = @fieldParentPtr(ScanTree, "next_tick", next_tick);
            scan.read_complete();
        }

        fn levels_read_complete(scan: *ScanTree) void {
            assert(scan.state == .fetching);
            assert(scan.state.fetching.pending_count > 0);

            scan.state.fetching.pending_count -= 1;
            if (scan.state.fetching.pending_count == 0) scan.read_complete();
        }

        fn read_complete(scan: *ScanTree) void {
            assert(scan.state == .fetching);
            assert(scan.state.fetching.pending_count == 0);

            const context = scan.state.fetching.context;
            const callback = scan.state.fetching.callback;
            scan.state = .seeking;

            if (scan.merge_iterator == null) {
                scan.merge_iterator = KWayMergeIterator.init(
                    scan,
                    KWayMergeStreams.streams_count,
                    scan.direction,
                );
            }

            callback(context, scan);
        }

        fn merge_table_mutable_peek(scan: *const ScanTree) error{ Drained, Empty }!Key {
            assert(scan.state == .seeking);

            const value: *const Value = scan.table_mutable_cursor.get(
                scan.table_mutable_values,
            ) orelse return error.Empty;

            const key = key_from_value(value);
            return key;
        }

        fn merge_table_immutable_peek(scan: *const ScanTree) error{ Drained, Empty }!Key {
            assert(scan.state == .seeking);

            const value: *const Value = scan.table_immutable_cursor.get(
                scan.tree.table_immutable.values,
            ) orelse return error.Empty;

            const key = key_from_value(value);
            return key;
        }

        fn merge_level_peek(scan: *const ScanTree, level_index: u32) error{ Drained, Empty }!Key {
            assert(scan.state == .seeking);
            assert(level_index < constants.lsm_levels);

            var level = &scan.levels[level_index];
            switch (level.values) {
                .fetching => return error.Drained,
                .buffered => |cursor| {
                    const value: ?*const Value = cursor.get(
                        Table.data_block_values_used(level.buffer.data_block),
                    );

                    // It's not expected to be null here,
                    // since the previous pop must have triggered the iterator
                    // in the case of Empty.
                    assert(value != null);

                    return key_from_value(value.?);
                },
                .finished => return error.Empty,
            }
        }

        fn merge_table_mutable_pop(scan: *ScanTree) Value {
            assert(scan.state == .seeking);

            const value = scan.table_mutable_cursor.get(
                scan.table_mutable_values,
            ) orelse unreachable;

            _ = scan.table_mutable_cursor.move(scan.direction);
            return value.*;
        }

        fn merge_table_immutable_pop(scan: *ScanTree) Value {
            assert(scan.state == .seeking);

            const value = scan.table_immutable_cursor.get(
                scan.tree.table_immutable.values,
            ) orelse unreachable;

            _ = scan.table_immutable_cursor.move(scan.direction);
            return value.*;
        }

        fn merge_level_pop(scan: *ScanTree, level_index: u32) Value {
            assert(scan.state == .seeking);
            assert(level_index < constants.lsm_levels);

            var level = &scan.levels[level_index];
            switch (level.values) {
                .buffered => |*cursor| {
                    const value = cursor.get(
                        Table.data_block_values_used(level.buffer.data_block),
                    ) orelse unreachable;

                    if (!cursor.move(scan.direction)) {
                        level.values = .fetching;
                    }
                    return value.*;
                },
                else => unreachable,
            }
        }
    };
}

/// Scans over one level of the LSM IndexTree.
fn ScanTreeLevelType(comptime ScanTree: type, comptime Storage: type) type {
    return struct {
        const ScanTreeLevel = @This();
        const LevelTableValueBlockIterator = LevelTableValueBlockIteratorType(
            ScanTree.Table,
            Storage,
        );
        const BlockPtr = ScanTree.Grid.BlockPtr;
        const BlockPtrConst = ScanTree.Grid.BlockPtrConst;

        const TableInfo = ScanTree.TableInfo;
        const Manifest = ScanTree.Manifest;

        const LevelBuffer = ScanTree.ScanBuffer.LevelBuffer;

        const Table = ScanTree.Table;
        const Key = Table.Key;
        const Value = Table.Value;
        const compare_keys = Table.compare_keys;
        const key_from_value = Table.key_from_value;

        scan: *ScanTree,
        level_index: u8,
        iterator: LevelTableValueBlockIterator,
        buffer: LevelBuffer,

        /// State over the manifest.
        manifest: union(enum) {
            begin,
            next_table: ScanTree.Key,
            finished,
        },

        /// State over the values.
        values: union(enum) {
            fetching,
            buffered: RangeCursor,
            finished,
        },

        tracer_slot: ?tracer.SpanStart = null,

        pub inline fn init(scan: *ScanTree, buffer: LevelBuffer, level_index: u8) ScanTreeLevel {
            return .{
                .level_index = level_index,
                .scan = scan,
                .iterator = LevelTableValueBlockIterator.init(),
                .buffer = buffer,
                .manifest = .begin,
                .values = .fetching,
            };
        }

        /// Moves the level iterator to the next `table_info` that might contain the key range.
        pub fn next_table(level: *ScanTreeLevel) void {
            const scan: *ScanTree = level.scan;

            const table_info: ?*const TableInfo = blk: {
                const manifest: *Manifest = &scan.tree.manifest;
                var key_exclusive: ?Key = switch (level.manifest) {
                    .begin => null,
                    .next_table => |key| key,
                    .finished => unreachable,
                };

                if (manifest.next_table(.{
                    .level = level.level_index,
                    .snapshot = scan.snapshot,
                    .key_min = scan.key_min,
                    .key_max = scan.key_max,
                    .key_exclusive = key_exclusive,
                    .direction = scan.direction,
                })) |next_table_info| {
                    key_exclusive = switch (scan.direction) {
                        .ascending => next_table_info.key_max,
                        .descending => next_table_info.key_min,
                    };

                    if (compare_keys(key_exclusive.?, scan.key_min) == .lt or
                        compare_keys(key_exclusive.?, scan.key_max) == .gt)
                    {
                        level.manifest = .finished;
                    } else {
                        level.manifest = .{ .next_table = key_exclusive.? };
                    }
                    break :blk next_table_info;
                } else {
                    level.manifest = .finished;
                    break :blk null;
                }
            };

            level.iterator.start(.{
                .grid = scan.tree.grid,
                .level = level.level_index,
                .snapshot = scan.snapshot,
                .index_block = level.buffer.index_block,
                .tables = .{ .scan = table_info },
                .direction = scan.direction,
            });
        }

        pub fn fetch(level: *ScanTreeLevel) void {
            assert(level.values == .fetching);

            level.iterator.next(.{
                .on_index = on_index_block,
                .on_data = on_data_block,
            });
        }

        fn on_index_block(
            iterator: *LevelTableValueBlockIterator,
        ) LevelTableValueBlockIterator.DataBlockAddresses {
            const level = @fieldParentPtr(ScanTreeLevel, "iterator", iterator);
            const scan: *ScanTree = level.scan;

            assert(level.values == .fetching);
            assert(scan.state == .fetching);
            assert(scan.state.fetching.pending_count > 0);

            const keys = Table.index_data_keys_used(iterator.context.index_block);
            const index_schema = schema.TableIndex.from(iterator.context.index_block);
            const addresses = index_schema.data_addresses_used(iterator.context.index_block);
            const checksums = index_schema.data_checksums_used(iterator.context.index_block);

            const indexes = binary_search.binary_search_keys_range_upsert_indexes(
                Key,
                compare_keys,
                keys,
                scan.key_min,
                scan.key_max,
            );

            if (indexes.start == keys.len) return .{
                .addresses = &[_]u64{},
                .checksums = &[_]u128{},
            } else if (indexes.end == keys.len) return .{
                .addresses = addresses[indexes.start..],
                .checksums = checksums[indexes.start..],
            } else return .{
                .addresses = addresses[indexes.start .. indexes.end + 1],
                .checksums = checksums[indexes.start .. indexes.end + 1],
            };
        }

        fn on_data_block(
            iterator: *LevelTableValueBlockIterator,
            data_block: ?BlockPtrConst,
        ) void {
            const level = @fieldParentPtr(ScanTreeLevel, "iterator", iterator);
            const scan: *ScanTree = level.scan;

            assert(scan.state == .fetching);
            assert(scan.state.fetching.pending_count > 0);

            if (data_block) |data| {
                stdx.copy_disjoint(.exact, u8, level.buffer.data_block, data);

                var values = Table.data_block_values_used(level.buffer.data_block);
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    values,
                    scan.key_min,
                    scan.key_max,
                );

                switch (level.values) {
                    .fetching => if (range.count > 0) {
                        level.values = .{
                            .buffered = RangeCursor.init(range, scan.direction),
                        };
                    },
                    else => unreachable,
                }
            } else {
                switch (level.manifest) {
                    .begin => unreachable,
                    .next_table => level.next_table(),
                    .finished => level.values = .finished,
                }
            }

            switch (level.values) {
                // Keep loading from storage.
                .fetching => level.fetch(),

                // Finished.
                .buffered, .finished => scan.levels_read_complete(),
            }
        }
    };
}

const RangeCursor = struct {
    range: BinarySearchRange,
    index: ?u32,

    pub inline fn init(range: BinarySearchRange, direction: Direction) RangeCursor {
        return .{
            .range = range,
            .index = if (range.count == 0) null else switch (direction) {
                .ascending => 0,
                .descending => range.count - 1,
            },
        };
    }

    pub inline fn empty(self: *const RangeCursor) bool {
        return self.range.count == 0;
    }

    pub inline fn get(self: *const RangeCursor, items: anytype) blk: {
        assert(meta.trait.isIndexable(@TypeOf(items)));
        const T = meta.Child(@TypeOf(items));
        break :blk ?*const T;
    } {
        if (self.index) |index| {
            const slice = items[self.range.start..][0..self.range.count];
            return &slice[index];
        }

        return null;
    }

    pub inline fn move(self: *RangeCursor, direction: Direction) bool {
        assert(self.index != null);
        assert(!self.empty());
        assert(self.index.? < self.range.count);

        switch (direction) {
            .ascending => {
                const next_index = self.index.? + 1;
                if (next_index == self.range.count) {
                    self.index = null;
                    return false;
                } else {
                    self.index = next_index;
                    return true;
                }
            },
            .descending => {
                if (self.index.? == 0) {
                    self.index = null;
                    return false;
                } else {
                    self.index.? -= 1;
                    return true;
                }
            },
        }
    }
};

/// KWayMerge stream identifier for each level of the LSM tree,
/// plus the mutable and immutable tables.
const KWayMergeStreams = enum(u32) {
    const streams_count = constants.lsm_levels + 2;

    // Tables mutable and immutable are well-known indexes.
    table_mutable = constants.lsm_levels,
    table_immutable = constants.lsm_levels + 1,

    // The rest of the lsm levels are represented as a non-exhaustive enum.
    _,
};

/// KWayMergeIterator for merging results from al levels of the LSM tree.
fn KWayMergeIteratorType(comptime ScanTree: type) type {
    const stream = struct {
        fn peek(
            scan: *ScanTree,
            stream_index: u32,
        ) error{ Drained, Empty }!ScanTree.Key {
            assert(stream_index < KWayMergeStreams.streams_count);

            return switch (@intToEnum(KWayMergeStreams, stream_index)) {
                .table_mutable => scan.merge_table_mutable_peek(),
                .table_immutable => scan.merge_table_immutable_peek(),
                _ => |index| scan.merge_level_peek(@enumToInt(index)),
            };
        }

        fn pop(scan: *ScanTree, stream_index: u32) ScanTree.Value {
            assert(stream_index < KWayMergeStreams.streams_count);

            return switch (@intToEnum(KWayMergeStreams, stream_index)) {
                .table_mutable => scan.merge_table_mutable_pop(),
                .table_immutable => scan.merge_table_immutable_pop(),
                _ => |index| scan.merge_level_pop(@enumToInt(index)),
            };
        }

        fn precedence(scan: *const ScanTree, a: u32, b: u32) bool {
            _ = scan;
            assert(a != b);
            assert(a < KWayMergeStreams.streams_count and b < KWayMergeStreams.streams_count);

            return switch (@intToEnum(KWayMergeStreams, a)) {
                .table_mutable => true,
                .table_immutable => @intToEnum(KWayMergeStreams, b) != .table_mutable,
                else => a < b and b < constants.lsm_levels,
            };
        }
    };

    return k_way_merge.KWayMergeIteratorType(
        ScanTree,
        ScanTree.Key,
        ScanTree.Value,
        ScanTree.key_from_value,
        ScanTree.compare_keys,
        KWayMergeStreams.streams_count,
        stream.peek,
        stream.pop,
        stream.precedence,
    );
}
