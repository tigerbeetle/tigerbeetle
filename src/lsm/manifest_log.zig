//! Maintains a durable manifest log of the latest TableInfo's for every LSM tree's in-memory
//! manifest.
//!
//! Invariants:
//!
//! * Checkpointing the manifest log must flush all buffered log blocks.
//!
//! * Opening the manifest log must emit only the latest TableInfo's to be inserted.
//!
//! * Opening the manifest log after a crash must result in exactly the same `compaction_set` in
//!   `SuperBlock.Manifest` as before the crash assuming that the crash was exactly at a checkpoint.
//!
//! * The latest version of a table must never be dropped from the log through a compaction, unless
//!   the table was removed.
//!
//! * Removes that are recorded in a log block must also queue that log block for compaction.
//!
//! * Compaction must compact partially full blocks, even where it must rewrite all entries to the
//!   tail end of the log.
//!
//! * If a remove is dropped from the log, then all prior inserts must already have been dropped.

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
const TableInfo = schema.ManifestLog.TableInfo;

comptime {
    // We need enough manifest blocks to accommodate the upper-bound of tables.
    // *10 is an arbitrary lower-bound of available space for amplification, which allows compaction
    // to be spaced more efficiently.
    const block_entries = schema.ManifestLog.entry_count_max;
    const manifest_entries = block_entries * vsr.superblock.manifest_blocks_max;
    assert(manifest_entries >= tree.table_count_max * 10);
}

pub fn ManifestLogType(comptime Storage: type) type {
    return struct {
        const ManifestLog = @This();

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Label = schema.ManifestLog.Label;

        pub const Callback = *const fn (manifest_log: *ManifestLog) void;

        pub const OpenEvent = *const fn (
            manifest_log: *ManifestLog,
            level: u7,
            table: *const TableInfo,
        ) void;

        const Write = struct {
            manifest_log: *ManifestLog,
            write: Grid.Write = undefined,
        };

        superblock: *SuperBlock,
        grid: *Grid,
        options: Options,

        grid_reservation: ?Grid.Reservation = null,

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
        open_iterator: SuperBlock.Manifest.IteratorReverse = undefined,

        /// Set for the duration of `compact`.
        reading: bool = false,
        read: Grid.Read = undefined,
        read_callback: ?Callback = null,
        read_block_reference: ?SuperBlock.Manifest.BlockReference = null,

        /// Set for the duration of `flush` and `checkpoint`.
        writing: bool = false,
        writes: []Write,
        writes_pending: usize = 0,
        write_callback: ?Callback = null,

        next_tick: Grid.NextTick = undefined,

        pub fn init(allocator: mem.Allocator, grid: *Grid, options: Options) !ManifestLog {
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

            return ManifestLog{
                .superblock = grid.superblock,
                .grid = grid,
                .options = options,
                .blocks = blocks,
                .writes = writes,
            };
        }

        pub fn deinit(manifest_log: *ManifestLog, allocator: mem.Allocator) void {
            allocator.free(manifest_log.writes);
            for (manifest_log.blocks.buffer) |block| allocator.free(block);
            manifest_log.blocks.deinit(allocator);
        }

        pub fn reset(manifest_log: *ManifestLog) void {
            for (manifest_log.blocks.buffer) |block| @memset(block, 0);

            manifest_log.* = .{
                .superblock = manifest_log.superblock,
                .grid = manifest_log.grid,
                .options = manifest_log.options,
                .blocks = .{ .buffer = manifest_log.blocks.buffer },
                .writes = manifest_log.writes,
            };
        }

        /// Opens the manifest log.
        /// Reads the manifest blocks in reverse order and passes extent table inserts to event().
        /// Therefore, only the latest version of a table will be emitted by event() for insertion
        /// into the in-memory manifest. Older versions of a table in older manifest blocks will not
        /// be emitted, as an optimization to not replay all table mutations.
        /// SuperBlock.Manifest.tables is used to track the latest version of a table.
        pub fn open(manifest_log: *ManifestLog, event: OpenEvent, callback: Callback) void {
            assert(!manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);

            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);

            manifest_log.open_event = event;
            manifest_log.open_iterator = manifest_log.superblock.manifest.iterator_reverse();

            manifest_log.reading = true;
            manifest_log.read_callback = callback;

            manifest_log.open_read_block();
        }

        fn open_read_block(manifest_log: *ManifestLog) void {
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);

            manifest_log.read_block_reference = manifest_log.open_iterator.next();

            if (manifest_log.read_block_reference) |block| {
                assert(block.address > 0);

                manifest_log.grid.read_block(
                    .{ .from_local_or_global_storage = open_read_block_callback },
                    &manifest_log.read,
                    block.address,
                    block.checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
            } else {
                // Use next_tick because the manifest may be empty (no blocks to read).
                manifest_log.grid.on_next_tick(open_next_tick_callback, &manifest_log.next_tick);
            }
        }

        fn open_next_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "next_tick", next_tick);
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(manifest_log.read_callback != null);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);

            manifest_log.opened = true;
            manifest_log.open_event = undefined;
            manifest_log.open_iterator = undefined;

            const callback = manifest_log.read_callback.?;
            manifest_log.reading = false;
            manifest_log.read_callback = null;
            assert(manifest_log.read_block_reference == null);

            callback(manifest_log);
        }

        fn open_read_block_callback(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            const block_reference = manifest_log.read_block_reference.?;
            verify_block(block, block_reference.checksum, block_reference.address);

            const entry_count = schema.ManifestLog.entry_count(block);
            const labels_used = schema.ManifestLog.labels_const(block)[0..entry_count];
            const tables_used = schema.ManifestLog.tables_const(block)[0..entry_count];

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;

            var entry = entry_count;
            while (entry > 0) {
                entry -= 1;

                const label = labels_used[entry];
                const table = &tables_used[entry];

                if (manifest.insert_table_extent(table.address, block_reference.address, entry)) {
                    switch (label.event) {
                        .insert => manifest_log.open_event(manifest_log, label.level, table),
                        .remove => manifest.queue_for_compaction(block_reference.address),
                    }
                } else {
                    manifest.queue_for_compaction(block_reference.address);
                }
            }

            if (schema.ManifestLog.entry_count(block) < schema.ManifestLog.entry_count_max) {
                manifest.queue_for_compaction(block_reference.address);
            }

            log.debug("opened: checksum={} address={} entries={}", .{
                block_reference.checksum,
                block_reference.address,
                entry_count,
            });

            manifest_log.open_read_block();
        }

        /// Appends an insert, an update, or a direct move of a table to a level.
        /// A move is only recorded as an insert, there is no remove from the previous level, since
        /// this is safer (no potential to get the event order wrong) and reduces fragmentation.
        pub fn insert(manifest_log: *ManifestLog, level: u7, table: *const TableInfo) void {
            maybe(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);

            manifest_log.append(.{ .level = level, .event = .insert }, table);
        }

        /// Appends the removal of a table from a level.
        /// The table must have previously been inserted to the manifest log.
        pub fn remove(manifest_log: *ManifestLog, level: u7, table: *const TableInfo) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);

            manifest_log.append(.{ .level = level, .event = .remove }, table);
        }

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

            assert(manifest_log.entry_count < schema.ManifestLog.entry_count_max);
            assert(manifest_log.blocks.count - manifest_log.blocks_closed == 1);

            log.debug(
                "{s}: level={} tree={} checksum={} address={} snapshot={}..{}",
                .{
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
            schema.ManifestLog.labels(block)[entry] = label;
            schema.ManifestLog.tables(block)[entry] = table.*;

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = header.op;
            if (manifest.update_table_extent(table.address, address, entry)) |previous_block| {
                manifest.queue_for_compaction(previous_block);
                if (label.event == .remove) manifest.queue_for_compaction(address);
            } else {
                // A remove must remove a insert, which implies that it must update the extent.
                assert(label.event != .remove);
            }

            manifest_log.entry_count += 1;
            if (manifest_log.entry_count == schema.ManifestLog.entry_count_max) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
            }
        }

        fn flush(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);

            log.debug("flush: writing {} block(s)", .{manifest_log.blocks_closed});

            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            if (manifest_log.blocks_closed == 0) {
                manifest_log.grid.on_next_tick(
                    flush_next_tick_callback,
                    &manifest_log.next_tick,
                );
            } else {
                for (0..manifest_log.blocks_closed) |_| {
                    manifest_log.write_block();
                }
                assert(manifest_log.writes_pending == manifest_log.blocks_closed);
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

            const header = schema.header_from_block(block.*);
            const address = header.op;
            assert(address > 0);

            const entry_count = schema.ManifestLog.entry_count(block.*);
            assert(entry_count > 0);

            if (block_index == manifest_log.blocks_closed - 1) {
                // This might be the last block of a checkpoint, which can be a partial block.
                assert(entry_count <= schema.ManifestLog.entry_count_max);
            } else {
                assert(entry_count == schema.ManifestLog.entry_count_max);
            }

            log.debug("write_block: checksum={} address={} entries={}", .{
                header.checksum,
                address,
                entry_count,
            });

            const write = &manifest_log.writes[block_index];
            write.* = .{ .manifest_log = manifest_log };

            manifest_log.writes_pending += 1;
            manifest_log.grid.write_block(
                write_block_callback,
                &write.write,
                block,
                address,
            );
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
                    assert(manifest_log.entry_count < schema.ManifestLog.entry_count_max);
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
                manifest_log.grid.reserve(1 + manifest_log.options.blocks_count_appends()).?;

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

            // Compact a single manifest block — to minimize latency spikes, we want to do the bare
            // minimum of compaction work required.
            // TODO Compact more than 1 block if fragmentation is outstripping the compaction rate.
            // (Make sure to update the grid block reservation to account for this).
            // Or assert that compactions cannot update blocks fast enough to outpace manifest
            // log compaction (relative to the number of updates that fit in a manifest log block).
            if (manifest_log.superblock.manifest.oldest_block_queued_for_compaction()) |block| {
                assert(block.address > 0);

                manifest_log.reading = true;
                manifest_log.read_block_reference = block;

                manifest_log.grid.read_block(
                    .{ .from_local_or_global_storage = compact_read_block_callback },
                    &manifest_log.read,
                    block.address,
                    block.checksum,
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

            const block_reference = manifest_log.read_block_reference.?;
            verify_block(block, block_reference.checksum, block_reference.address);

            const entry_count = schema.ManifestLog.entry_count(block);
            const labels_used = schema.ManifestLog.labels_const(block)[0..entry_count];
            const tables_used = schema.ManifestLog.tables_const(block)[0..entry_count];

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;
            assert(manifest.tables.count() > 0);

            var frees: u32 = 0;
            var entry: u32 = 0;
            while (entry < entry_count) : (entry += 1) {
                const label = labels_used[entry];
                const table = &tables_used[entry];

                // Remove the extent if the table is the latest version.
                // We must iterate entries in forward order to drop the extent here.
                // Otherwise, stale versions earlier in the block may reappear.
                if (manifest.remove_table_extent(table.address, block_reference.address, entry)) {
                    switch (label.event) {
                        // Append the table, updating the table extent:
                        .insert => manifest_log.append(label, table),
                        // Since we compact oldest blocks first, we know that we have already
                        // compacted all inserts that were eclipsed by this remove, so this remove
                        // can now be safely dropped.
                        .remove => frees += 1,
                    }
                } else {
                    // The table is not the latest version and can dropped.
                    frees += 1;
                }
            }

            log.debug("compacted: checksum={} address={} frees={}/{}", .{
                block_reference.checksum,
                block_reference.address,
                frees,
                entry_count,
            });

            // Blocks may be compacted if they contain frees, or are not completely full.
            // (A partial block may be flushed as part of a checkpoint.)
            assert(frees > 0 or entry_count < schema.ManifestLog.entry_count_max);
            // At most one block could have been filled by the compaction.
            assert(manifest_log.blocks_closed <= 1);

            assert(manifest.queued_for_compaction(block_reference.address));
            manifest.remove(block_reference.checksum, block_reference.address);
            assert(!manifest.queued_for_compaction(block_reference.address));

            manifest_log.grid.release(block_reference.address);
            manifest_log.reading = false;
            manifest_log.read_block_reference = null;

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

            if (manifest_log.grid_reservation) |grid_reservation| {
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

        fn acquire_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.entry_count == 0);
            assert(manifest_log.blocks.count == manifest_log.blocks_closed);
            assert(!manifest_log.blocks.full());

            manifest_log.blocks.advance_tail();

            const block: BlockPtr = manifest_log.blocks.tail().?;

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            header.* = .{
                .cluster = manifest_log.superblock.working.cluster,
                .op = manifest_log.grid.acquire(manifest_log.grid_reservation.?),
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

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry_count = manifest_log.entry_count;
            assert(entry_count > 0);
            assert(entry_count <= schema.ManifestLog.entry_count_max);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.cluster == manifest_log.superblock.working.cluster);
            assert(header.op > 0);
            assert(header.command == .block);
            header.size = schema.ManifestLog.size(entry_count);

            // Zero unused labels:
            @memset(mem.sliceAsBytes(schema.ManifestLog.labels(block)[entry_count..]), 0);

            // Zero unused tables, and padding:
            @memset(block[header.size..], 0);

            header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
            header.set_checksum();

            verify_block(block, null, null);
            assert(schema.ManifestLog.entry_count(block) == entry_count);

            manifest_log.superblock.manifest.append(header.checksum, header.op);
            if (schema.ManifestLog.entry_count(block) < schema.ManifestLog.entry_count_max) {
                manifest_log.superblock.manifest.queue_for_compaction(header.op);
            }

            log.debug("close_block: checksum={} address={} entries={}/{}", .{
                header.checksum,
                header.op,
                entry_count,
                schema.ManifestLog.entry_count_max,
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

            const entry_count = schema.ManifestLog.entry_count(block);
            assert(entry_count > 0);
        }
    };
}

pub const Options = struct {
    /// The total number of trees in the forest.
    forest_tree_count: usize,

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
        return options.forest_tree_count *
            tree.compactions_max *
            (tree.compaction_tables_input_max + // Update snapshot_max.
            tree.compaction_tables_input_max + // Remove.
            tree.compaction_tables_output_max);
    }

    fn blocks_count_appends(options: *const Options) usize {
        return stdx.div_ceil(
            options.compaction_appends_max(),
            schema.ManifestLog.entry_count_max,
        );
    }

    /// The upper-bound of manifest log blocks we must buffer.
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
