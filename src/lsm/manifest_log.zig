//! Maintains a durable manifest log of the latest TableInfo's for every LSM tree's in-memory
//! manifest.
//!
//! Invariants:
//!
//! * Checkpointing the manifest log must flush all buffered log blocks.
//!
//! * Opening the manifest log must emit only the latest TableInfo's to be inserted.
//!
//! * The latest version of a table must never be dropped from the log through a compaction, unless
//!   the table was removed.
//!
//! * Removes that are recorded in a log block must also queue that log block for compaction.
//!
//! * Compaction must compact partially full blocks, even where it must rewrite all entries to the
//!   tail end of the log.
//!
//! * If a remove is dropped from the log, then all prior inserts/updates must already have been
//!   dropped.

const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const maybe = stdx.maybe;

const log = std.log.scoped(.manifest_log);

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");

const SuperBlockType = vsr.SuperBlockType;
const GridType = @import("../vsr/grid.zig").GridType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const BlockType = @import("schema.zig").BlockType;
const tree = @import("tree.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const schema = @import("schema.zig");
const TableInfo = schema.ManifestNode.TableInfo;

const block_builder_schema = schema.ManifestNode{
    .entry_count = schema.ManifestNode.entry_count_max,
};

pub fn ManifestLogType(comptime Storage: type) type {
    return struct {
        const ManifestLog = @This();

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Label = schema.ManifestNode.Label;

        pub const Callback = *const fn (manifest_log: *ManifestLog) void;

        pub const OpenEvent = *const fn (
            manifest_log: *ManifestLog,
            level: u6,
            table: *const TableInfo,
        ) void;

        const Write = struct {
            manifest_log: *ManifestLog,
            write: Grid.Write = undefined,
        };

        const TableExtents = std.AutoHashMapUnmanaged(u64, TableExtent);
        const TablesRemoved = std.AutoHashMapUnmanaged(u64, void);

        pub const TableExtent = struct {
            block: u64, // Manifest block address.
            entry: u32, // Index within the manifest block Label/TableInfo arrays.
        };

        const BlockReference = struct {
            checksum: u128,
            address: u64,
        };

        superblock: *SuperBlock,
        grid: *Grid,
        options: Options,

        grid_reservation: ?Grid.Reservation = null,

        /// This is a struct-of-arrays of `BlockReference`s.
        /// It includes:
        /// - blocks that are written
        /// - blocks that have closed, but not yet flushed
        /// - blocks that are being flushed
        ///
        /// Entries are ordered from oldest to newest.
        log_block_checksums: RingBuffer(u128, .slice),
        log_block_addresses: RingBuffer(u64, .slice),

        /// The head block accumulates a full block, to be written at the next flush.
        /// The remaining blocks must accommodate all further appends.
        blocks: RingBuffer(BlockPtr, .slice),

        /// The number of blocks that have been appended to, filled up, and then closed.
        blocks_closed: u8 = 0,

        /// The number of entries in the open block.
        ///
        /// Invariants:
        /// - When `entry_count = 0`, there is no open block.
        /// - `entry_count < entry_count_max`. When `entry_count` reaches the maximum, the open
        ///   block is closed, and `entry_count` resets to 0.
        entry_count: u32 = 0,

        opened: bool = false,
        open_event: OpenEvent = undefined,

        /// Set for the duration of `open` and `compact`.
        reading: bool = false,
        read: Grid.Read = undefined,
        read_callback: ?Callback = null,

        /// Set for the duration of `flush` and `checkpoint`.
        writing: bool = false,
        writes: []Write,
        writes_pending: usize = 0,
        write_callback: ?Callback = null,

        next_tick: Grid.NextTick = undefined,

        /// A map from table address to the manifest block and entry that is the latest extent
        /// version. Used to determine whether a table should be dropped in a compaction.
        table_extents: TableExtents,

        /// For a particular table in the manifest, the sequence of events is:
        ///
        ///   insert(0|1), update(0+), remove(0|1)
        ///
        /// During open(), manifest entries are processed in reverse-chronological order.
        ///
        /// This hash-set tracks tables that have been removed but whose corresponding "insert" has
        /// not yet been encountered. Given that the maximum number of tables in the forest at any
        /// given moment is `table_count_max`, there are likewise at most `table_count_max`
        /// "unpaired" removes to track.
        // TODO(Optimization) This memory (~35MiB) is only needed during open() – maybe borrow it
        // from the grid cache or node pool instead so that we don't pay for it during normal
        // operation.
        tables_removed: TablesRemoved,

        pub fn init(allocator: mem.Allocator, grid: *Grid, options: Options) !ManifestLog {
            assert(options.tree_id_min <= options.tree_id_max);

            // TODO Replace with the actual computed upper-bound.
            const log_blocks_max = 65536;

            var log_block_checksums =
                try RingBuffer(u128, .slice).init(allocator, log_blocks_max);
            errdefer log_block_checksums.deinit(allocator);

            var log_block_addresses =
                try RingBuffer(u64, .slice).init(allocator, log_blocks_max);
            errdefer log_block_addresses.deinit(allocator);

            // TODO RingBuffer for .slice should be extended to take care of alignment:
            var blocks =
                try RingBuffer(BlockPtr, .slice).init(allocator, options.blocks_count_max());
            errdefer blocks.deinit(allocator);

            for (blocks.buffer, 0..) |*block, i| {
                errdefer for (blocks.buffer[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (blocks.buffer) |b| allocator.free(b);

            var writes = try allocator.alloc(Write, options.blocks_count_max());
            errdefer allocator.free(writes);
            @memset(writes, undefined);

            var table_extents = TableExtents{};
            try table_extents.ensureTotalCapacity(allocator, tree.table_count_max);
            errdefer table_extents.deinit(allocator);

            var tables_removed = TablesRemoved{};
            try tables_removed.ensureTotalCapacity(allocator, tree.table_count_max);
            errdefer tables_removed.deinit(allocator);

            return ManifestLog{
                .superblock = grid.superblock,
                .grid = grid,
                .options = options,
                .log_block_checksums = log_block_checksums,
                .log_block_addresses = log_block_addresses,
                .blocks = blocks,
                .writes = writes,
                .table_extents = table_extents,
                .tables_removed = tables_removed,
            };
        }

        pub fn deinit(manifest_log: *ManifestLog, allocator: mem.Allocator) void {
            manifest_log.tables_removed.deinit(allocator);
            manifest_log.table_extents.deinit(allocator);
            allocator.free(manifest_log.writes);
            for (manifest_log.blocks.buffer) |block| allocator.free(block);
            manifest_log.blocks.deinit(allocator);
            manifest_log.log_block_addresses.deinit(allocator);
            manifest_log.log_block_checksums.deinit(allocator);
        }

        pub fn reset(manifest_log: *ManifestLog) void {
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);

            manifest_log.log_block_checksums.clear();
            manifest_log.log_block_addresses.clear();
            for (manifest_log.blocks.buffer) |block| @memset(block, 0);
            manifest_log.table_extents.clearRetainingCapacity();
            manifest_log.tables_removed.clearRetainingCapacity();

            manifest_log.* = .{
                .superblock = manifest_log.superblock,
                .grid = manifest_log.grid,
                .options = manifest_log.options,
                .log_block_checksums = manifest_log.log_block_checksums,
                .log_block_addresses = manifest_log.log_block_addresses,
                .blocks = .{ .buffer = manifest_log.blocks.buffer },
                .writes = manifest_log.writes,
                .table_extents = manifest_log.table_extents,
                .tables_removed = manifest_log.tables_removed,
            };
        }

        /// Opens the manifest log.
        /// Reads the manifest blocks in reverse order and passes extent table inserts to event().
        /// Therefore, only the latest version of a table will be emitted by event() for insertion
        /// into the in-memory manifest. Older versions of a table in older manifest blocks will not
        /// be emitted, as an optimization to not replay all table mutations.
        /// `ManifestLog.table_extents` is used to track the latest version of a table.
        // TODO(Optimization): Accumulate tables unordered, then sort all at once to splice into the
        // ManifestLevels' SegmentedArrays. (Constructing SegmentedArrays by repeated inserts is
        // expensive.)
        pub fn open(manifest_log: *ManifestLog, event: OpenEvent, callback: Callback) void {
            assert(!manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);

            assert(manifest_log.log_block_checksums.count == 0);
            assert(manifest_log.log_block_addresses.count == 0);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);
            assert(manifest_log.table_extents.count() == 0);
            assert(manifest_log.tables_removed.count() == 0);

            manifest_log.open_event = event;
            manifest_log.reading = true;
            manifest_log.read_callback = callback;

            const references = manifest_log.superblock.working.manifest_references();
            assert(references.block_count <= manifest_log.log_block_checksums.buffer.len);

            if (references.empty()) {
                manifest_log.grid.on_next_tick(open_next_tick_callback, &manifest_log.next_tick);
            } else {
                manifest_log.open_read_block(.{
                    .checksum = references.newest_checksum,
                    .address = references.newest_address,
                });
            }
        }

        fn open_next_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "next_tick", next_tick);
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            assert(manifest_log.log_block_checksums.count == 0);
            assert(manifest_log.log_block_addresses.count == 0);
            assert(manifest_log.table_extents.count() == 0);
            assert(manifest_log.tables_removed.count() == 0);
            assert(manifest_log.superblock.working.manifest_references().empty());

            manifest_log.open_done();
        }

        fn open_read_block(manifest_log: *ManifestLog, block_reference: BlockReference) void {
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(manifest_log.read_callback != null);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.table_extents.count() <= tree.table_count_max);
            assert(manifest_log.tables_removed.count() <= tree.table_count_max);
            assert(manifest_log.log_block_checksums.count <
                manifest_log.log_block_checksums.buffer.len);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.log_block_checksums.count <
                manifest_log.superblock.working.vsr_state.checkpoint.manifest_block_count);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);
            assert(block_reference.address > 0);

            if (constants.verify) {
                // The manifest block list has no cycles.
                var address_iterator = manifest_log.log_block_addresses.iterator();
                while (address_iterator.next()) |address| {
                    assert(address != block_reference.address);
                }
            }

            manifest_log.log_block_checksums.push_head_assume_capacity(block_reference.checksum);
            manifest_log.log_block_addresses.push_head_assume_capacity(block_reference.address);

            manifest_log.grid.read_block(
                .{ .from_local_or_global_storage = open_read_block_callback },
                &manifest_log.read,
                block_reference.address,
                block_reference.checksum,
                .{ .cache_read = true, .cache_write = true },
            );
        }

        fn open_read_block_callback(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.log_block_addresses.count > 0);
            assert(manifest_log.log_block_checksums.count > 0);
            assert(!manifest_log.superblock.working.manifest_references().empty());

            const block_checksum = manifest_log.log_block_checksums.head().?;
            const block_address = manifest_log.log_block_addresses.head().?;
            verify_block(block, block_checksum, block_address);

            const block_schema = schema.ManifestNode.from(block);
            const labels_used = block_schema.labels_const(block);
            const tables_used = block_schema.tables_const(block);
            assert(block_schema.entry_count > 0);
            assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);

            var entry = block_schema.entry_count;
            while (entry > 0) {
                entry -= 1;

                const label = labels_used[entry];
                const table = &tables_used[entry];
                assert(table.tree_id >= manifest_log.options.tree_id_min);
                assert(table.tree_id <= manifest_log.options.tree_id_max);
                assert(table.address > 0);

                if (label.event == .remove) {
                    const table_removed =
                        manifest_log.tables_removed.fetchPutAssumeCapacity(table.address, {});
                    assert(table_removed == null);
                } else {
                    if (manifest_log.tables_removed.get(table.address)) |_| {
                        if (label.event == .insert) {
                            assert(manifest_log.tables_removed.remove(table.address));
                        }
                    } else {
                        var extent =
                            manifest_log.table_extents.getOrPutAssumeCapacity(table.address);
                        if (!extent.found_existing) {
                            extent.value_ptr.* = .{ .block = block_address, .entry = entry };
                            manifest_log.open_event(manifest_log, label.level, table);
                        }
                    }
                }
            }

            log.debug("{}: opened: checksum={} address={} entries={}", .{
                manifest_log.superblock.replica_index.?,
                block_checksum,
                block_address,
                block_schema.entry_count,
            });

            const checkpoint_state = &manifest_log.superblock.working.vsr_state.checkpoint;
            if (checkpoint_state.manifest_oldest_address == block_address) {
                // When we find the oldest block, stop iterating the linked list – any more blocks
                // have already been compacted away.
                assert(checkpoint_state.manifest_oldest_checksum == block_checksum);

                manifest_log.open_done();
            } else {
                const block_reference_previous = schema.ManifestNode.previous(block).?;

                manifest_log.open_read_block(.{
                    .checksum = block_reference_previous.checksum,
                    .address = block_reference_previous.address,
                });
            }
        }

        fn open_done(manifest_log: *ManifestLog) void {
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(manifest_log.read_callback != null);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.table_extents.count() <= tree.table_count_max);
            assert(manifest_log.tables_removed.count() <= tree.table_count_max);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.superblock.working.vsr_state.checkpoint.manifest_block_count);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);

            log.debug("{}: open_done: opened block_count={} table_count={}", .{
                manifest_log.superblock.replica_index.?,
                manifest_log.log_block_checksums.count,
                manifest_log.table_extents.count(),
            });

            const callback = manifest_log.read_callback.?;
            manifest_log.opened = true;
            manifest_log.open_event = undefined;
            manifest_log.reading = false;
            manifest_log.read_callback = null;

            callback(manifest_log);
        }

        /// Appends an insert of a table to a level.
        pub fn insert(manifest_log: *ManifestLog, level: u6, table: *const TableInfo) void {
            maybe(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.table_extents.get(table.address) == null);

            manifest_log.append(.{ .level = level, .event = .insert }, table);
        }

        /// Appends an update or a direct move of a table to a level.
        /// A move is only recorded as an update, there is no remove from the previous level, since
        /// this is safer (no potential to get the event order wrong) and reduces fragmentation.
        pub fn update(manifest_log: *ManifestLog, level: u6, table: *const TableInfo) void {
            maybe(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.table_extents.get(table.address) != null);

            manifest_log.append(.{ .level = level, .event = .update }, table);
        }

        /// Appends the removal of a table from a level.
        /// The table must have previously been inserted to the manifest log.
        pub fn remove(manifest_log: *ManifestLog, level: u6, table: *const TableInfo) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.table_extents.get(table.address) != null);

            manifest_log.append(.{ .level = level, .event = .remove }, table);
        }

        /// The table extent must be updated immediately when appending, without delay.
        /// Otherwise, ManifestLog.compact() may append a stale version over the latest.
        fn append(manifest_log: *ManifestLog, label: Label, table: *const TableInfo) void {
            assert(manifest_log.opened);
            assert(!manifest_log.writing);
            maybe(manifest_log.reading);
            assert(manifest_log.grid_reservation != null);
            assert(label.level < constants.lsm_levels);
            assert(table.address > 0);
            assert(table.snapshot_min > 0);
            assert(table.snapshot_max > table.snapshot_min);

            if (manifest_log.entry_count == 0) {
                assert(manifest_log.blocks.count == manifest_log.blocks_closed);
                manifest_log.acquire_block();
            } else if (manifest_log.entry_count > 0) {
                assert(manifest_log.blocks.count > 0);
            }

            assert(manifest_log.entry_count < schema.ManifestNode.entry_count_max);
            assert(manifest_log.blocks.count - manifest_log.blocks_closed == 1);

            log.debug(
                "{}: {s}: level={} tree={} checksum={} address={} snapshot={}..{}",
                .{
                    manifest_log.superblock.replica_index.?,
                    @tagName(label.event),
                    label.level,
                    table.tree_id,
                    table.checksum,
                    table.address,
                    table.snapshot_min,
                    table.snapshot_max,
                },
            );

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry = manifest_log.entry_count;
            block_builder_schema.labels(block)[entry] = label;
            block_builder_schema.tables(block)[entry] = table.*;

            const block_header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const block_address = block_header.op;

            switch (label.event) {
                .insert,
                .update,
                => {
                    var extent = manifest_log.table_extents.getOrPutAssumeCapacity(table.address);
                    if (extent.found_existing) {
                        maybe(label.event == .insert); // (Compaction.)
                    } else {
                        assert(label.event == .insert);
                    }
                    extent.value_ptr.* = .{ .block = block_address, .entry = entry };
                },
                .remove => assert(manifest_log.table_extents.remove(table.address)),
            }

            manifest_log.entry_count += 1;
            if (manifest_log.entry_count == schema.ManifestNode.entry_count_max) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
            }
        }

        fn flush(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);

            log.debug("{}: flush: writing {} block(s)", .{
                manifest_log.superblock.replica_index.?,
                manifest_log.blocks_closed,
            });

            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            for (0..manifest_log.blocks_closed) |_| manifest_log.write_block();
            assert(manifest_log.blocks_closed == manifest_log.writes_pending);

            if (manifest_log.writes_pending == 0) {
                manifest_log.grid.on_next_tick(flush_next_tick_callback, &manifest_log.next_tick);
            }
        }

        fn flush_next_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "next_tick", next_tick);
            assert(manifest_log.writing);

            manifest_log.flush_done();
        }

        fn flush_done(manifest_log: *ManifestLog) void {
            assert(manifest_log.writing);
            assert(manifest_log.write_callback != null);
            assert(manifest_log.blocks_closed == 0);

            const callback = manifest_log.write_callback.?;
            manifest_log.write_callback = null;
            manifest_log.writing = false;
            callback(manifest_log);
        }

        fn write_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(manifest_log.writing);
            assert(manifest_log.blocks_closed > 0);
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);
            assert(manifest_log.writes_pending < manifest_log.blocks_closed);

            const block_index = manifest_log.writes_pending;
            const block = manifest_log.blocks.get_ptr(block_index).?;
            verify_block(block.*, null, null);

            const block_schema = schema.ManifestNode.from(block.*);
            assert(block_schema.entry_count > 0);

            const header = schema.header_from_block(block.*);
            const address = header.op;
            assert(address > 0);

            if (block_index == manifest_log.blocks_closed - 1) {
                // This might be the last block of a checkpoint, which can be a partial block.
                assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);
            } else {
                assert(block_schema.entry_count == schema.ManifestNode.entry_count_max);
            }

            log.debug("{}: write_block: checksum={} address={} entries={}", .{
                manifest_log.superblock.replica_index.?,
                header.checksum,
                address,
                block_schema.entry_count,
            });

            const write = &manifest_log.writes[block_index];
            write.* = .{ .manifest_log = manifest_log };

            manifest_log.writes_pending += 1;
            manifest_log.grid.create_block(write_block_callback, &write.write, block);
        }

        fn write_block_callback(grid_write: *Grid.Write) void {
            const write = @fieldParentPtr(Write, "write", grid_write);
            const manifest_log = write.manifest_log;
            assert(manifest_log.opened);
            assert(manifest_log.writing);
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);

            manifest_log.writes_pending -= 1;

            if (manifest_log.writes_pending == 0) {
                for (0..manifest_log.blocks_closed) |_| manifest_log.blocks.advance_head();
                manifest_log.blocks_closed = 0;

                if (manifest_log.blocks.count == 0) {
                    assert(manifest_log.entry_count == 0);
                } else {
                    assert(manifest_log.blocks.count == 1);
                    assert(manifest_log.entry_count < schema.ManifestNode.entry_count_max);
                }

                manifest_log.flush_done();
            }
        }

        /// `compact` does not close a partial block; that is only necessary during `checkpoint`.
        // TODO Make sure block reservation cannot fail — before compaction begins verify that
        // enough free blocks are available for all reservations.
        pub fn compact(manifest_log: *ManifestLog, callback: Callback, op: u64) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            assert(manifest_log.blocks.count ==
                manifest_log.blocks_closed + @intFromBool(manifest_log.entry_count > 0));
            assert(op % @divExact(constants.lsm_batch_multiple, 2) == 0);

            if (op < constants.lsm_batch_multiple or
                manifest_log.superblock.working.vsr_state.op_compacted(op))
            {
                manifest_log.read_callback = callback;
                manifest_log.grid.on_next_tick(compact_tick_callback, &manifest_log.next_tick);
                return;
            }

            // +1: During manifest-log compaction, we will create at most one block.
            manifest_log.grid_reservation =
                manifest_log.grid.reserve(.{
                    .acquire_blocks = 1 + @as(u32, @intCast(manifest_log.options.blocks_count_appends())),
                    .release_blocks = 1,
                }).?;

            manifest_log.read_callback = callback;
            manifest_log.flush(compact_flush_callback);
        }

        fn compact_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "next_tick", next_tick);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.entry_count == 0);

            const callback = manifest_log.read_callback.?;
            manifest_log.read_callback = null;
            callback(manifest_log);
        }

        fn compact_flush_callback(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.read_callback != null);
            assert(manifest_log.grid_reservation != null);

            if (manifest_log.blocks.count < manifest_log.log_block_checksums.count) {
                const oldest_checksum = manifest_log.log_block_checksums.head().?;
                const oldest_address = manifest_log.log_block_addresses.head().?;
                assert(oldest_address > 0);

                manifest_log.reading = true;
                manifest_log.grid.read_block(
                    .{ .from_local_or_global_storage = compact_read_block_callback },
                    &manifest_log.read,
                    oldest_address,
                    oldest_checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
            } else {
                manifest_log.compact_done_callback();
            }
        }

        fn compact_read_block_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.read_callback != null);
            assert(manifest_log.grid_reservation != null);

            const oldest_checksum = manifest_log.log_block_checksums.pop().?;
            const oldest_address = manifest_log.log_block_addresses.pop().?;
            verify_block(block, oldest_checksum, oldest_address);

            const block_schema = schema.ManifestNode.from(block);
            assert(block_schema.entry_count > 0);
            assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);

            var frees: u32 = 0;
            for (
                block_schema.labels_const(block),
                block_schema.tables_const(block),
                0..block_schema.entry_count,
            ) |label, *table, entry_index| {
                const entry: u32 = @intCast(entry_index);
                switch (label.event) {
                    // Append the table, updating the table extent:
                    .insert,
                    .update,
                    => {
                        // Update the extent if the table is the latest version.
                        // We must iterate entries in forward order to drop the extent here.
                        // Otherwise, stale versions earlier in the block may reappear.
                        if (std.meta.eql(
                            manifest_log.table_extents.get(table.address),
                            .{ .block = oldest_address, .entry = entry },
                        )) {
                            // Append the table, updating the table extent:
                            manifest_log.append(label, table);
                        } else {
                            // Either:
                            // - This is not the latest insert for this table, so it can be dropped.
                            // - The table was removed some time after this insert.
                            frees += 1;
                        }
                    },
                    // Since we compact oldest blocks first, we know that we have already
                    // compacted all inserts that were eclipsed by this remove, so this remove
                    // can now be safely dropped.
                    .remove => frees += 1,
                }
            }

            log.debug("{}: compacted: checksum={} address={} free={}/{}", .{
                manifest_log.superblock.replica_index.?,
                oldest_checksum,
                oldest_address,
                frees,
                block_schema.entry_count,
            });

            // Blocks are compacted in sequence – not skipped, even if no entries will be freed.
            // (That should be rare though, since blocks are large.)
            // This is necessary to update the block's "previous block" pointer in the header.
            maybe(frees == 0);
            assert(manifest_log.blocks_closed <= 1);

            manifest_log.grid.release(&manifest_log.grid_reservation.?, oldest_address);
            manifest_log.reading = false;
            manifest_log.compact_done_callback();
        }

        fn compact_done_callback(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks_closed <= 1);
            assert(manifest_log.read_callback != null);
            assert(manifest_log.grid_reservation != null);

            const callback = manifest_log.read_callback.?;
            manifest_log.read_callback = null;

            callback(manifest_log);
        }

        pub fn compact_end(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);
            assert(manifest_log.write_callback == null);

            if (manifest_log.grid_reservation) |*grid_reservation| {
                manifest_log.grid.forfeit(grid_reservation);
                manifest_log.grid_reservation = null;
            } else {
                // Compaction was skipped for this half-bar.
                assert(manifest_log.entry_count == 0);
                assert(manifest_log.blocks.count == 0);
                assert(manifest_log.blocks_closed == 0);
            }
        }

        pub fn checkpoint(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);

            if (manifest_log.entry_count > 0) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
                assert(manifest_log.blocks_closed > 0);
                assert(manifest_log.blocks_closed == manifest_log.blocks.count);
            }

            manifest_log.flush(callback);
        }

        pub fn checkpoint_references(
            manifest_log: *const ManifestLog,
        ) vsr.SuperBlockManifestReferences {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);

            if (manifest_log.log_block_addresses.count == 0) {
                return std.mem.zeroes(vsr.SuperBlockManifestReferences);
            } else {
                return .{
                    .oldest_checksum = manifest_log.log_block_checksums.head().?,
                    .oldest_address = manifest_log.log_block_addresses.head().?,
                    .newest_checksum = manifest_log.log_block_checksums.tail().?,
                    .newest_address = manifest_log.log_block_addresses.tail().?,
                    .block_count = @intCast(manifest_log.log_block_addresses.count),
                };
            }
        }

        fn acquire_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.entry_count == 0);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.blocks.count == manifest_log.blocks_closed);
            assert(!manifest_log.blocks.full());

            manifest_log.blocks.advance_tail();

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const block_address = manifest_log.grid.acquire(&manifest_log.grid_reservation.?);

            const newest_checksum = manifest_log.log_block_checksums.tail() orelse 0;
            const newest_address = manifest_log.log_block_addresses.tail() orelse 0;

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            header.* = .{
                .context = undefined,
                .cluster = manifest_log.superblock.working.cluster,
                .parent = newest_checksum,
                .commit = newest_address,
                .op = block_address,
                .size = undefined,
                .command = .block,
                .operation = BlockType.manifest.operation(),
            };
        }

        fn close_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks.count == manifest_log.blocks_closed + 1);
            assert(manifest_log.log_block_checksums.count <
                manifest_log.log_block_checksums.buffer.len);

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry_count = manifest_log.entry_count;
            assert(entry_count > 0);
            assert(entry_count <= schema.ManifestNode.entry_count_max);

            const block_schema = schema.ManifestNode{ .entry_count = entry_count };
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = header.op;
            assert(header.cluster == manifest_log.superblock.working.cluster);
            assert(header.command == .block);
            assert(address > 0);
            header.size = block_schema.size();
            header.context = @bitCast(schema.ManifestNode.Context{ .entry_count = entry_count });

            if (entry_count < schema.ManifestNode.entry_count_max) {
                // Copy the labels backwards, since the block-builder schema assumed that the block
                // would be full, and it is not.
                stdx.copy_left(
                    .exact,
                    Label,
                    block_schema.labels(block),
                    block_builder_schema.labels(block)[0..entry_count],
                );
            }

            // Zero padding:
            @memset(block[header.size..], 0);

            header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
            header.set_checksum();
            verify_block(block, null, null);

            manifest_log.log_block_checksums.push_assume_capacity(header.checksum);
            manifest_log.log_block_addresses.push_assume_capacity(address);

            log.debug("{}: close_block: checksum={} address={} entries={}/{}", .{
                manifest_log.superblock.replica_index.?,
                header.checksum,
                address,
                entry_count,
                schema.ManifestNode.entry_count_max,
            });

            manifest_log.blocks_closed += 1;
            manifest_log.entry_count = 0;
            assert(manifest_log.blocks.count == manifest_log.blocks_closed);
        }

        fn verify_block(block: BlockPtrConst, checksum: ?u128, address: ?u64) void {
            const header = schema.header_from_block(block);
            assert(BlockType.from(header.operation) == .manifest);

            if (constants.verify) {
                assert(header.valid_checksum());
                assert(header.valid_checksum_body(block[@sizeOf(vsr.Header)..header.size]));
            }

            assert(address == null or header.op == address.?);
            assert(checksum == null or header.checksum == checksum.?);

            const block_schema = schema.ManifestNode.from(block);
            assert(block_schema.entry_count > 0);
            assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);
        }
    };
}

pub const Options = struct {
    tree_id_min: u16, // inclusive
    tree_id_max: u16, // inclusive

    /// The total number of trees in the forest.
    pub fn forest_tree_count(options: *const Options) usize {
        assert(options.tree_id_min <= options.tree_id_max);

        return (options.tree_id_max - options.tree_id_min) + 1;
    }

    /// The maximum number of table updates to the manifest by a half-measure of table
    /// compaction.
    ///
    /// This counts:
    /// - Input tables are updated in the manifest (snapshot_max is reduced).
    /// - Input tables are removed from the manifest (if not held by a persistent snapshot).
    /// - Output tables are inserted into the manifest.
    /// This does not count:
    /// - Manifest log compaction.
    /// - Releasing persistent snapshots.
    // TODO If insert-then-remove can update in-memory, then we can only count input tables once.
    pub fn compaction_appends_max(options: *const Options) usize {
        return options.forest_tree_count() *
            tree.compactions_max *
            (tree.compaction_tables_input_max + // Update snapshot_max.
            tree.compaction_tables_input_max + // Remove.
            tree.compaction_tables_output_max);
    }

    fn blocks_count_appends(options: *const Options) usize {
        return stdx.div_ceil(options.compaction_appends_max(), schema.ManifestNode.entry_count_max);
    }

    /// The upper-bound of manifest blocks we must buffer.
    ///
    /// `blocks` must have sufficient capacity for:
    /// - a manifest log compaction (+1 block in the worst case)
    /// - a leftover open block from the previous ops (+1 block)
    /// - table updates from a half bar of compactions
    fn blocks_count_max(options: *const Options) usize {
        const count = 1 + 1 + options.blocks_count_appends();
        assert(count >= 3);

        return count;
    }
};
