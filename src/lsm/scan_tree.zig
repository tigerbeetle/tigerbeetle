const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.scan);
const tracer = @import("../tracer.zig");

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const constants = @import("../constants.zig");
const lsm = @import("tree.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;
const schema = @import("schema.zig");
const binary_search = @import("binary_search.zig");
const k_way_merge = @import("k_way_merge.zig");

const BinarySearchRange = binary_search.BinarySearchRange;
const Direction = @import("../direction.zig").Direction;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const TreeTableInfoType = @import("manifest.zig").TreeTableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const ScanState = @import("scan_state.zig").ScanState;
const TableValueIteratorType =
    @import("table_data_iterator.zig").TableValueIteratorType;

/// Scans a range of keys over a Tree, in ascending or descending order.
/// At a high level, this is an ordered iterator over the values in a tree, at a particular
/// snapshot, within a given key range, merged across all levels (including the in-memory tables).
///
/// 1. Sort the in-memory tables and perform a binary search on them for the key range.
/// 2. Fetch from storage and fill the buffer with values from all LSM levels that match the key
///    range (see `ScanTreeLevel`).
/// 3. Perform a k-way merge to iterate over buffers from different levels and memory tables in
///    ascending or descending order.
/// 4. Repeat step 2 when the buffer of at least one level has been consumed, until all levels
///    have been exhausted.
pub fn ScanTreeType(
    comptime Context: type,
    comptime Tree_: type,
    comptime Storage: type,
) type {
    return struct {
        const ScanTree = @This();

        pub const Callback = *const fn (context: Context, scan: *ScanTree) void;

        const Grid = GridType(Storage);

        const TableInfo = TreeTableInfoType(Table);
        const Manifest = ManifestType(Table, Storage);

        pub const Tree = Tree_;
        const Table = Tree.Table;
        const Key = Table.Key;
        const Value = Table.Value;
        const key_from_value = Table.key_from_value;

        const ScanTreeLevel = ScanTreeLevelType(ScanTree, Storage);

        /// KWayMerge stream identifier for each level of the LSM tree,
        /// plus the mutable and immutable tables.
        /// The `KWayMerge` API requires it to be a `u32`.
        const KWayMergeStreams = enum(u32) {
            const streams_count = constants.lsm_levels + 2;

            // Tables mutable and immutable are well-known indexes.
            table_mutable = constants.lsm_levels,
            table_immutable = constants.lsm_levels + 1,

            // The rest of the lsm levels are represented as a non-exhaustive enum.
            _,
        };

        /// KWayMergeIterator for merging results from all levels of the LSM tree.
        const KWayMergeIterator = T: {
            const stream = struct {
                fn peek(
                    scan: *ScanTree,
                    stream_index: u32,
                ) error{ Drained, Empty }!ScanTree.Key {
                    assert(stream_index < KWayMergeStreams.streams_count);

                    return switch (@as(KWayMergeStreams, @enumFromInt(stream_index))) {
                        .table_mutable => scan.merge_table_mutable_peek(),
                        .table_immutable => scan.merge_table_immutable_peek(),
                        _ => |index| scan.merge_level_peek(@intFromEnum(index)),
                    };
                }

                fn pop(scan: *ScanTree, stream_index: u32) ScanTree.Value {
                    assert(stream_index < KWayMergeStreams.streams_count);

                    return switch (@as(KWayMergeStreams, @enumFromInt(stream_index))) {
                        .table_mutable => scan.merge_table_mutable_pop(),
                        .table_immutable => scan.merge_table_immutable_pop(),
                        _ => |index| scan.merge_level_pop(@intFromEnum(index)),
                    };
                }

                // Precedence is: table_mutable > table_immutable > level 0 > level 1 > ...
                fn precedence(scan: *const ScanTree, a: u32, b: u32) bool {
                    _ = scan;
                    assert(a != b);
                    assert(a < KWayMergeStreams.streams_count);
                    assert(b < KWayMergeStreams.streams_count);

                    return switch (@as(KWayMergeStreams, @enumFromInt(a))) {
                        .table_mutable => true,
                        .table_immutable => @as(
                            KWayMergeStreams,
                            @enumFromInt(b),
                        ) != .table_mutable,
                        else => a < b and b < constants.lsm_levels,
                    };
                }
            };

            break :T k_way_merge.KWayMergeIteratorType(
                ScanTree,
                ScanTree.Key,
                ScanTree.Value,
                ScanTree.key_from_value,
                KWayMergeStreams.streams_count,
                stream.peek,
                stream.pop,
                stream.precedence,
            );
        };

        tree: *Tree,
        buffer: *const ScanBuffer,

        direction: Direction,
        key_min: Key,
        key_max: Key,
        snapshot: u64,

        table_mutable_values: []const Value,
        table_immutable_values: []const Value,

        state: union(ScanState) {
            /// The scan has not been executed yet.
            /// All levels are still uninitialized.
            idle,

            /// The scan is at a valid position and ready to yield values, e.g. calling `next()`.
            /// All levels are either in the state `.buffered` or `.finished`.
            seeking,

            /// The scan needs to load data from the LSM levels, e.g. calling `read()`.
            /// At least one level is in the state `.fetching`.
            /// It's also possible for levels to be in the state `.buffered` and `.finished`.
            needs_data,

            /// The scan is attempting to load data from the LSM levels,
            /// e.g. in between calling `read()` and receiving the callback.
            /// Only levels in the state `.fetching` will load from storage.
            /// It's also possible for levels to be in the state `.buffered` and `.finished`.
            buffering: struct {
                context: Context,
                callback: Callback,
                pending_count: u32,
            },

            /// The scan was aborted and will not yield any more values.
            aborted,
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
            assert(key_min <= key_max);

            const table_mutable_values: []const Value = blk: {
                if (snapshot != snapshot_latest) break :blk &.{};

                tree.table_mutable.sort();
                const values = tree.table_mutable.values_used();
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    values,
                    key_min,
                    key_max,
                );
                break :blk values[range.start..][0..range.count];
            };

            const table_immutable_values: []const Value = blk: {
                if (snapshot <
                    tree.table_immutable.mutability.immutable.snapshot_min) break :blk &.{};

                const values = tree.table_immutable.values_used();
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    values,
                    key_min,
                    key_max,
                );
                break :blk values[range.start..][0..range.count];
            };

            return .{
                .tree = tree,
                .buffer = buffer,
                .state = .idle,
                .snapshot = snapshot,
                .key_min = key_min,
                .key_max = key_max,
                .direction = direction,
                .table_mutable_values = table_mutable_values,
                .table_immutable_values = table_immutable_values,
                .levels = undefined,
                .merge_iterator = null,
            };
        }

        pub fn read(self: *ScanTree, context: Context, callback: Callback) void {
            assert(self.state == .idle or self.state == .needs_data);

            const state_before = self.state;
            self.state = .{
                .buffering = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            for (&self.levels, 0..) |*level, i| {
                if (state_before == .idle) {
                    // Initializing all levels for the first read.
                    level.* = ScanTreeLevel.init(
                        self,
                        self.buffer.levels[i],
                        @intCast(i),
                    );
                }

                switch (level.values) {
                    .fetching => {
                        assert(level.state == .iterating_manifest or
                            level.state == .iterating_values or
                            level.state == .finished);

                        if (level.state == .iterating_manifest) level.table_next(.begin);
                        self.state.buffering.pending_count += 1;
                        level.fetch();
                    },
                    .buffered => {
                        assert(level.state == .iterating_values);
                        assert(state_before == .needs_data);
                    },
                    .finished => {
                        assert(level.state == .finished);
                        assert(state_before == .needs_data);
                    },
                }
            }
        }

        pub fn abort(self: *ScanTree) void {
            assert(self.state != .buffering);
            self.state = .aborted;
        }

        /// Moves the iterator to the next position and returns its `Value` or `null` if the
        /// iterator has no more values to iterate.
        /// May return `error.ReadAgain` if a data block needs to be loaded, in this case
        /// call `read()` and resume the iteration after the read callback.
        pub fn next(self: *ScanTree) error{ReadAgain}!?Value {
            switch (self.state) {
                .idle => {
                    assert(self.merge_iterator == null);
                    return error.ReadAgain;
                },
                .seeking => return self.merge_iterator.?.pop() catch |err| switch (err) {
                    error.Drained => {
                        self.state = .needs_data;
                        return error.ReadAgain;
                    },
                },
                .needs_data => return error.ReadAgain,
                .buffering => unreachable,
                .aborted => return null,
            }
        }

        /// Modifies the key_min/key_max range and moves the scan to the next value such that
        /// `value.key >= probe_key` (ascending) or `value.key <= probe_key` (descending).
        /// The scan may become `Empty` or `Drained` _after_ probing.
        /// Should not be called when the current key already matches the `probe_key`.
        pub fn probe(self: *ScanTree, probe_key: Key) void {
            if (self.state == .aborted) return;
            assert(self.state != .buffering);

            // No need to move if the current range is already tighter.
            // It can abort scanning if the key is unreachable.
            if (probe_key < self.key_min) {
                if (self.direction == .descending) self.abort();
                return;
            } else if (self.key_max < probe_key) {
                if (self.direction == .ascending) self.abort();
                return;
            }

            // It's allowed to probe multiple times with the same `probe_key`.
            // In this case, there's no need to move since the key range was already set.
            if (switch (self.direction) {
                .ascending => self.key_min == probe_key,
                .descending => self.key_max == probe_key,
            }) {
                assert(self.state == .idle or
                    self.state == .seeking or
                    self.state == .needs_data);
                return;
            }

            // Updates the scan range depending on the direction.
            switch (self.direction) {
                .ascending => {
                    assert(self.key_min < probe_key);
                    assert(probe_key <= self.key_max);
                    self.key_min = probe_key;
                },
                .descending => {
                    assert(probe_key < self.key_max);
                    assert(self.key_min <= probe_key);
                    self.key_max = probe_key;
                },
            }

            // Re-slicing the in-memory tables:
            inline for (.{ &self.table_mutable_values, &self.table_immutable_values }) |field| {
                const table_memory = field.*;
                const slice: []const Value = probe_values(self.direction, table_memory, probe_key);
                assert(slice.len <= table_memory.len);
                field.* = slice;
            }

            switch (self.state) {
                .idle => {},
                .seeking, .needs_data => {
                    for (&self.levels) |*level| {
                        // Forwarding the `probe` to each level.
                        level.probe(probe_key);
                    }

                    // It's not expected to probe a scan that already produced a key equals
                    // or ahead the probe.
                    assert(self.merge_iterator.?.key_popped == null or
                        switch (self.direction) {
                        .ascending => self.merge_iterator.?.key_popped.? < probe_key,
                        .descending => self.merge_iterator.?.key_popped.? > probe_key,
                    });

                    // Once the underlying streams have been changed, the merge iterator needs
                    // to reset its state, otherwise it may have dirty keys buffered.
                    self.merge_iterator.?.reset();
                },
                .buffering, .aborted => unreachable,
            }
        }

        fn levels_read_complete(self: *ScanTree) void {
            assert(self.state == .buffering);
            assert(self.state.buffering.pending_count > 0);

            self.state.buffering.pending_count -= 1;
            if (self.state.buffering.pending_count == 0) self.read_complete();
        }

        /// The next data block for each level is available.
        fn read_complete(self: *ScanTree) void {
            assert(self.state == .buffering);
            assert(self.state.buffering.pending_count == 0);

            const context = self.state.buffering.context;
            const callback = self.state.buffering.callback;
            self.state = .seeking;

            if (self.merge_iterator == null) {
                self.merge_iterator = KWayMergeIterator.init(
                    self,
                    KWayMergeStreams.streams_count,
                    self.direction,
                );
            }

            callback(context, self);
        }

        fn merge_table_mutable_peek(self: *const ScanTree) error{ Drained, Empty }!Key {
            return self.table_memory_peek(self.table_mutable_values);
        }

        fn merge_table_immutable_peek(self: *const ScanTree) error{ Drained, Empty }!Key {
            return self.table_memory_peek(self.table_immutable_values);
        }

        fn merge_table_mutable_pop(self: *ScanTree) Value {
            return table_memory_pop(self, &self.table_mutable_values);
        }

        fn merge_table_immutable_pop(self: *ScanTree) Value {
            return table_memory_pop(self, &self.table_immutable_values);
        }

        inline fn table_memory_peek(
            self: *const ScanTree,
            values: []const Value,
        ) error{ Drained, Empty }!Key {
            assert(self.state == .seeking);

            if (values.len == 0) return error.Empty;

            const value: *const Value = switch (self.direction) {
                .ascending => &values[0],
                .descending => &values[values.len - 1],
            };

            const key = key_from_value(value);
            return key;
        }

        inline fn table_memory_pop(
            self: *ScanTree,
            field_reference: *[]const Value,
        ) Value {
            assert(self.state == .seeking);

            // The slice is re-sliced during pop,
            // updating the backing field at the end.
            var values = field_reference.*;
            defer field_reference.* = values;

            assert(values.len > 0);
            // Discarding duplicated entries from TableMemory, last entry wins:
            switch (self.direction) {
                .ascending => {
                    while (values.len > 1 and
                        key_from_value(&values[0]) ==
                        key_from_value(&values[1]))
                    {
                        values = values[1..];
                    }

                    const value_first = values[0];
                    values = values[1..];
                    return value_first;
                },
                .descending => {
                    const value_last = values[values.len - 1];
                    while (values.len > 1 and
                        key_from_value(&values[values.len - 1]) ==
                        key_from_value(&values[values.len - 2]))
                    {
                        values = values[0 .. values.len - 1];
                    }

                    values = values[0 .. values.len - 1];
                    return value_last;
                },
            }
        }

        fn merge_level_peek(self: *const ScanTree, level_index: u32) error{ Drained, Empty }!Key {
            assert(self.state == .seeking);
            assert(level_index < constants.lsm_levels);

            const level = &self.levels[level_index];
            return level.peek();
        }

        fn merge_level_pop(self: *ScanTree, level_index: u32) Value {
            assert(self.state == .seeking);
            assert(level_index < constants.lsm_levels);

            const level = &self.levels[level_index];
            return level.pop();
        }

        fn probe_values(direction: Direction, values: []const Value, key: Key) []const Value {
            switch (direction) {
                .ascending => {
                    const start = binary_search.binary_search_values_upsert_index(
                        Key,
                        Value,
                        key_from_value,
                        values,
                        key,
                        .{ .mode = .lower_bound },
                    );

                    return if (start == values.len) &.{} else values[start..];
                },
                .descending => {
                    const end = end: {
                        const index = binary_search.binary_search_values_upsert_index(
                            Key,
                            Value,
                            key_from_value,
                            values,
                            key,
                            .{ .mode = .upper_bound },
                        );
                        break :end index + @intFromBool(
                            index < values.len and key_from_value(&values[index]) <= key,
                        );
                    };

                    return if (end == 0) &.{} else values[0..end];
                },
            }
        }
    };
}

/// Scans a range of keys over a single LSM Level, in ascending or descending order.
///
/// 1. Iterate over the in-memory manifest to find the next `table_info` that might
///    contain the key range.
/// 2. Load the `index_block` of the selected `table_info`.
/// 3. Perform a binary search on the `index_block` to retrieve an array of addresses
///    and checksums of all `value_block`s that might contain the key range.
/// 4. Load a `value_block` from the address/checksum array (in ascending or descending order).
/// 5. Perform a binary search on the `value_block` and buffer the entries that match
///    the key range.
/// 6. When the buffer is consumed, repeat step [4] for loading the next `value_block`,
///    or, if there are no more `value_block`s in the current `index_block`,
///    repeat step [1] for the next `table_info`.
fn ScanTreeLevelType(comptime ScanTree: type, comptime Storage: type) type {
    return struct {
        const ScanTreeLevel = @This();

        const Grid = GridType(Storage);
        const TableValueIterator = TableValueIteratorType(Storage);

        const TableInfo = ScanTree.TableInfo;
        const Manifest = ScanTree.Manifest;

        const Table = ScanTree.Table;
        const Key = Table.Key;
        const Value = Table.Value;
        const key_from_value = Table.key_from_value;

        const State = union(enum) {
            const ManifestTableNext = union(enum) {
                key_exclusive: Key,
                out_of_range,
            };

            iterating_manifest,
            loading_index: struct {
                manifest_table_next: ManifestTableNext,
                address: u64,
                checksum: u128,
                read: Grid.Read = undefined,
            },
            iterating_values: struct {
                manifest_table_next: ManifestTableNext,
                iterator: TableValueIterator,
            },
            finished: struct {
                next_tick: Grid.NextTick = undefined,
            },
        };

        const Values = union(enum) {
            fetching,
            buffered: []const Value,
            finished,
        };

        scan: *ScanTree,
        level_index: u8,
        buffer: ScanBuffer.LevelBuffer,

        state: State,
        values: Values,

        pub fn init(
            scan: *ScanTree,
            buffer: ScanBuffer.LevelBuffer,
            level_index: u8,
        ) ScanTreeLevel {
            assert(level_index < constants.lsm_levels);

            return .{
                .level_index = level_index,
                .scan = scan,
                .buffer = buffer,
                .state = .iterating_manifest,
                .values = .fetching,
            };
        }

        /// Moves the iterator to the next `table_info` that might contain the key range.
        /// No IO is performed until `fetch()` is called.
        pub fn table_next(
            self: *ScanTreeLevel,
            key_exclusive: union(enum) {
                begin,
                next: Key,
            },
        ) void {
            assert(self.state == .iterating_manifest);
            assert(self.values == .fetching);

            const scan: *ScanTree = self.scan;
            assert(scan.state == .buffering);

            const manifest: *Manifest = &scan.tree.manifest;
            if (manifest.next_table(.{
                .level = self.level_index,
                .snapshot = scan.snapshot,
                .key_min = scan.key_min,
                .key_max = scan.key_max,
                .key_exclusive = switch (key_exclusive) {
                    .begin => null,
                    .next => |key| key,
                },
                .direction = scan.direction,
            })) |table_info| {
                const manifest_table_next: State.ManifestTableNext = table_next: {
                    // The last key depending on the direction:
                    const key_last = switch (scan.direction) {
                        .ascending => table_info.key_max,
                        .descending => table_info.key_min,
                    };

                    // If the last key is out of the key range,
                    // there are no more tables to scan next.
                    if (scan.key_max < key_last) {
                        assert(scan.direction == .ascending);
                        break :table_next .out_of_range;
                    } else if (key_last < scan.key_min) {
                        assert(scan.direction == .descending);
                        break :table_next .out_of_range;
                    }

                    break :table_next .{ .key_exclusive = key_last };
                };

                self.state = .{
                    .loading_index = .{
                        .manifest_table_next = manifest_table_next,
                        .address = table_info.address,
                        .checksum = table_info.checksum,
                    },
                };
                self.values = .fetching;
            } else {
                self.state = .{ .finished = .{} };
                self.values = .finished;
            }
        }

        pub fn fetch(self: *ScanTreeLevel) void {
            assert(self.scan.state == .buffering);

            switch (self.state) {
                .iterating_manifest => unreachable,
                .loading_index => |*loading_index| {
                    assert(self.values == .fetching);
                    // Reading the index blocks:
                    const grid: *Grid = self.scan.tree.grid;
                    grid.read_block(
                        .{ .from_local_or_global_storage = index_block_callback },
                        &loading_index.read,
                        loading_index.address,
                        loading_index.checksum,
                        .{ .cache_read = true, .cache_write = true },
                    );
                },
                .iterating_values => |*iterating_values| {
                    assert(self.values == .fetching);
                    iterating_values.iterator.next(value_block_callback);
                },
                .finished => |*finished| {
                    assert(self.values == .finished);
                    const grid: *Grid = self.scan.tree.grid;
                    grid.on_next_tick(
                        finished_callback,
                        &finished.next_tick,
                    );
                },
            }
        }

        pub fn peek(self: *const ScanTreeLevel) error{ Drained, Empty }!Key {
            maybe(self.state == .iterating_manifest or
                self.state == .iterating_values or
                self.state == .finished);
            assert(self.scan.state == .seeking);

            switch (self.values) {
                .fetching => return error.Drained,
                .buffered => |values| {
                    assert(values.len > 0);
                    assert(@intFromPtr(values.ptr) >= @intFromPtr(self.buffer.data_block));
                    assert(@intFromPtr(values.ptr) <=
                        @intFromPtr(self.buffer.data_block) + self.buffer.data_block.len);

                    const value: *const Value = switch (self.scan.direction) {
                        .ascending => &values[0],
                        .descending => &values[values.len - 1],
                    };

                    const key = key_from_value(value);
                    return key;
                },
                .finished => return error.Empty,
            }
        }

        pub fn pop(self: *ScanTreeLevel) Value {
            maybe(self.state == .iterating_manifest or
                self.state == .iterating_values or
                self.state == .finished);
            assert(self.values == .buffered);
            assert(self.scan.state == .seeking);

            var values = self.values.buffered;
            assert(values.len > 0);
            assert(@intFromPtr(values.ptr) >= @intFromPtr(self.buffer.data_block));
            assert(@intFromPtr(values.ptr) <=
                @intFromPtr(self.buffer.data_block) + self.buffer.data_block.len);

            // The buffer is re-sliced during pop,
            // updating the backing field at the end.
            defer if (values.len == 0) {
                self.values = .fetching;
            } else {
                self.values.buffered = values;
            };

            switch (self.scan.direction) {
                .ascending => {
                    const first_value = values[0];
                    values = values[1..];
                    return first_value;
                },
                .descending => {
                    const last_value = values[values.len - 1];
                    values = values[0 .. values.len - 1];
                    return last_value;
                },
            }
        }

        pub fn probe(self: *ScanTreeLevel, probe_key: Key) void {
            maybe(self.state == .iterating_manifest or
                self.state == .iterating_values or
                self.state == .finished);

            switch (self.values) {
                .fetching => {},
                .buffered => |buffer| {
                    assert(buffer.len > 0);
                    const slice: []const Value = ScanTree.probe_values(
                        self.scan.direction,
                        buffer,
                        probe_key,
                    );

                    if (slice.len == 0) {
                        self.values = .fetching;
                    } else {
                        // The next exclusive key must be ahead of (or equals) the probe key,
                        // so the level iterator state can be preserved without reading the
                        // index block again.
                        if (self.state == .iterating_values and
                            self.state.iterating_values.manifest_table_next == .key_exclusive)
                        {
                            const key_exclusive =
                                self.state.iterating_values.manifest_table_next.key_exclusive;
                            assert(switch (self.scan.direction) {
                                .ascending => key_exclusive >= probe_key,
                                .descending => key_exclusive <= probe_key,
                            });
                        }

                        self.values = .{
                            .buffered = slice,
                        };
                    }
                },
                .finished => {
                    assert(self.state == .finished);
                    return;
                },
            }

            if (self.values == .fetching) {
                // The key couldn't be found in the buffered data.
                // The level iterator must read the index block again from the new key range.
                //
                // TODO: We may use the already buffered `index_block` to check if the key
                // is present in other value blocks within the same table, advancing the level
                // iterator instead of calling `table_next(.begin)`.
                // However, it's most likely the index block is still in the grid cache, so this
                // may not represent any real improvement.
                self.state = .iterating_manifest;
            }
        }

        fn index_block_callback(
            read: *Grid.Read,
            index_block: BlockPtrConst,
        ) void {
            const LoadingIndex = std.meta.FieldType(State, .loading_index);
            const loading_index: *LoadingIndex = @fieldParentPtr("read", read);
            const state: *State = @fieldParentPtr("loading_index", loading_index);
            const self: *ScanTreeLevel = @fieldParentPtr("state", state);
            const scan: *const ScanTree = self.scan;

            assert(self.state == .loading_index);
            assert(self.values == .fetching);
            assert(scan.state == .buffering);
            assert(scan.state.buffering.pending_count > 0);

            // `index_block` is only valid for this callback, so copy it's contents.
            stdx.copy_disjoint(.exact, u8, self.buffer.index_block, index_block);

            const Range = struct { start: u32, end: u32 };
            const range_found: ?Range = range: {
                const keys = Table.index_data_keys_used(self.buffer.index_block, .key_max);
                const indexes = binary_search.binary_search_keys_range_upsert_indexes(
                    Key,
                    keys,
                    scan.key_min,
                    scan.key_max,
                );

                // The key range was not found.
                if (indexes.start == keys.len) break :range null;

                // Because we search `key_max` in the index block, if the search does not find an
                // exact match it returns the index of the next greatest key, which may contain
                // the key depending on the `key_min`.
                const end = end: {
                    const keys_min = Table.index_data_keys_used(
                        index_block,
                        .key_min,
                    );
                    break :end indexes.end + @intFromBool(
                        indexes.end < keys.len and keys_min[indexes.end] <= scan.key_max,
                    );
                };

                // TODO: Secondary indexes are keyed by `Prefix+timestamp`, and differently of
                // monotonic ids/timestamps, they cannot be efficiently filtered by key_min/key_max.
                // This may be a valid use case for bloom filters (by prefix only).
                break :range if (indexes.start == end) null else .{
                    .start = indexes.start,
                    .end = end,
                };
            };

            const index_schema = schema.TableIndex.from(self.buffer.index_block);
            const data_addresses = index_schema.data_addresses_used(self.buffer.index_block);
            const data_checksums = index_schema.data_checksums_used(self.buffer.index_block);
            assert(data_addresses.len == data_checksums.len);

            self.state = .{
                .iterating_values = .{
                    .manifest_table_next = self.state.loading_index.manifest_table_next,
                    .iterator = undefined,
                },
            };

            const context: TableValueIterator.Context = if (range_found) |range| .{
                .grid = scan.tree.grid,
                .addresses = data_addresses[range.start..range.end],
                .checksums = data_checksums[range.start..range.end],
                .direction = scan.direction,
            } else .{
                .grid = scan.tree.grid,
                .addresses = &.{},
                .checksums = &.{},
                .direction = scan.direction,
            };

            self.state.iterating_values.iterator.init(context);
            self.state.iterating_values.iterator.next(value_block_callback);
        }

        fn value_block_callback(
            iterator: *TableValueIterator,
            value_block_maybe: ?BlockPtrConst,
        ) void {
            const IteratingValues = std.meta.FieldType(State, .iterating_values);
            const iterating_values: *IteratingValues = @fieldParentPtr("iterator", iterator);
            const state: *State = @fieldParentPtr("iterating_values", iterating_values);
            const self: *ScanTreeLevel = @fieldParentPtr("state", state);
            const scan: *ScanTree = self.scan;

            assert(self.state == .iterating_values);
            assert(self.values == .fetching);
            assert(scan.state == .buffering);
            assert(scan.state.buffering.pending_count > 0);

            if (value_block_maybe) |value_block| {
                const values = Table.data_block_values_used(value_block);
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    values,
                    scan.key_min,
                    scan.key_max,
                );

                if (range.count > 0) {
                    // The buffer is a whole grid block, but only the matching values should
                    // be copied to save memory bandwidth. The buffer `data block` does not
                    // follow the block layout (e.g. header + values).
                    const buffer: []Value = std.mem.bytesAsSlice(Value, self.buffer.data_block);
                    stdx.copy_disjoint(
                        .exact,
                        Value,
                        buffer[0..range.count],
                        values[range.start..][0..range.count],
                    );
                    // Found values that match the range query.
                    self.values = .{ .buffered = buffer[0..range.count] };
                } else {
                    // The `data_block` *might* contain the scan range,
                    // otherwise, it shouldn't have been returned by the iterator.
                    const key_min = key_from_value(&values[0]);
                    const key_max = key_from_value(&values[values.len - 1]);
                    assert(key_min < scan.key_min and
                        scan.key_max < key_max);

                    // No matches were found.
                    // Keep loading `value_block`s from the same table.
                    self.values = .fetching;
                }
            } else {
                // There are no more value blocks in this table:
                switch (self.state.iterating_values.manifest_table_next) {
                    .key_exclusive => |key_exclusive| {
                        assert(switch (self.scan.direction) {
                            .ascending => self.scan.key_min <= key_exclusive,
                            .descending => self.scan.key_max >= key_exclusive,
                        });
                        // Loading the next table_info.
                        self.state = .iterating_manifest;
                        self.values = .fetching;
                        self.table_next(.{
                            .next = key_exclusive,
                        });
                    },
                    .out_of_range => {
                        // The next `table_info` is out of the key range, so it's finished.
                        self.state = .{ .finished = .{} };
                        self.values = .finished;
                    },
                }
            }

            switch (self.values) {
                .fetching => self.fetch(),
                .buffered, .finished => scan.levels_read_complete(),
            }
        }

        fn finished_callback(next_tick: *Grid.NextTick) void {
            const Finished = std.meta.FieldType(State, .finished);
            const finished: *Finished = @fieldParentPtr("next_tick", next_tick);
            const state: *State = @alignCast(@fieldParentPtr("finished", finished));
            const self: *ScanTreeLevel = @fieldParentPtr("state", state);
            const scan: *ScanTree = self.scan;

            assert(self.state == .finished);
            assert(self.values == .finished);
            assert(scan.state == .buffering);
            assert(scan.state.buffering.pending_count > 0);

            scan.levels_read_complete();
        }
    };
}
