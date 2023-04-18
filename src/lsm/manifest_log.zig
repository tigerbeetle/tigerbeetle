//! Maintains an on-disk manifest log of the latest TableInfo's in an LSM tree's in-memory manifest.
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

const log = std.log.scoped(.manifest_log);

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");

const SuperBlockType = vsr.SuperBlockType;
const GridType = @import("grid.zig").GridType;
const BlockType = @import("grid.zig").BlockType;
const alloc_block = @import("grid.zig").alloc_block;
const tree = @import("tree.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

/// ManifestLog block schema:
/// │ vsr.Header                  │ operation=BlockType.manifest
/// │ [entry_count_max]Label      │ level index, insert|remove
/// │ [≤entry_count_max]TableInfo │
/// │ […]u8{0}                    │ padding (to end of block)
/// Label and TableInfo entries correspond.
pub fn ManifestLogType(comptime Storage: type, comptime TableInfo: type) type {
    return struct {
        const ManifestLog = @This();

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        pub const Block = ManifestLogBlockType(Storage, TableInfo);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Label = Block.Label;

        pub const Callback = fn (manifest_log: *ManifestLog) void;

        pub const OpenEvent = fn (
            manifest_log: *ManifestLog,
            level: u7,
            table: *const TableInfo,
        ) void;

        const alignment = 16;

        comptime {
            // Bit 7 is reserved to indicate whether the event is an insert or remove.
            assert(constants.lsm_levels <= math.maxInt(u7) + 1);

            assert(@sizeOf(Label) == @sizeOf(u8));

            // All TableInfo's should already be 16-byte aligned because of the leading checksum.
            assert(@alignOf(TableInfo) == alignment);

            // For keys { 8, 16, 24, 32 } all TableInfo's should be a multiple of the alignment.
            // However, we still store Label ahead of TableInfo to save space on the network.
            // This means we store fewer entries per manifest block, to gain less padding,
            // since we must store entry_count_max of whichever array is first in the layout.
            // For a better understanding of this decision, see Block.size() below.
            assert(@sizeOf(TableInfo) % alignment == 0);
        }

        /// The maximum number of table updates to the manifest by a half-measure of table
        /// compaction (not including manifest log compaction).
        ///
        /// Input tables are updated in the manifest (snapshot_max is reduced).
        /// Input tables are removed from the manifest (if not held by a persistent snapshot).
        /// Output tables are inserted into the manifest.
        // TODO If insert-then-remove can update in-memory, then we can only count input tables once.
        pub const compaction_appends_max = tree.compactions_max *
            (tree.compaction_tables_input_max + // Update snapshot_max.
            tree.compaction_tables_input_max + // Remove.
            tree.compaction_tables_output_max);

        const blocks_count_appends = stdx.div_ceil(compaction_appends_max, Block.entry_count_max);

        /// The upper-bound of manifest log blocks we must buffer.
        ///
        /// `blocks` must have sufficient capacity for:
        /// - a manifest log compaction (+1 block in the worst case)
        /// - a leftover open block from the previous ops (+1 block)
        /// - table updates from a half bar of compactions
        ///   (This is typically +1 block, but may be more when the block size is small).
        ///   TODO(Beat compaction): blocks_count_appends only needs enough for 1 beat.
        const blocks_count_max = 1 + 1 + blocks_count_appends;

        comptime {
            assert(blocks_count_max >= 3);
            assert(blocks_count_max == 3 or constants.block_size < 64 * 1024);
        }

        superblock: *SuperBlock,
        grid: *Grid,
        grid_reservation: ?Grid.Reservation = null,
        tree_hash: u128,

        /// The head block is used to accumulate a full block, to be written at the next flush.
        /// The remaining blocks must accommodate all further appends.
        blocks: RingBuffer(BlockPtr, blocks_count_max, .array),

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
        write: Grid.Write = undefined,
        write_callback: ?Callback = null,

        pub fn init(allocator: mem.Allocator, grid: *Grid, tree_hash: u128) !ManifestLog {
            // TODO RingBuffer for .pointer should be extended to take care of alignment:

            var blocks: [blocks_count_max]BlockPtr = undefined;
            for (blocks) |*block, i| {
                errdefer for (blocks[0..i]) |b| allocator.free(b);
                block.* = try alloc_block(allocator);
            }
            errdefer for (blocks) |b| allocator.free(b);

            return ManifestLog{
                .superblock = grid.superblock,
                .grid = grid,
                .tree_hash = tree_hash,
                .blocks = .{ .buffer = blocks },
            };
        }

        pub fn deinit(manifest_log: *ManifestLog, allocator: mem.Allocator) void {
            for (manifest_log.blocks.buffer) |block| allocator.free(block);
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
            manifest_log.open_iterator = manifest_log.superblock.manifest.iterator_reverse(
                manifest_log.tree_hash,
            );

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
                assert(block.tree == manifest_log.tree_hash);
                assert(block.address > 0);

                manifest_log.grid.read_block(
                    open_read_block_callback,
                    &manifest_log.read,
                    block.address,
                    block.checksum,
                    .manifest,
                );
            } else {
                manifest_log.opened = true;
                manifest_log.open_event = undefined;
                manifest_log.open_iterator = undefined;

                const callback = manifest_log.read_callback.?;
                manifest_log.reading = false;
                manifest_log.read_callback = null;
                assert(manifest_log.read_block_reference == null);

                callback(manifest_log);
            }
        }

        fn open_read_block_callback(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            const block_reference = manifest_log.read_block_reference.?;
            verify_block(block, block_reference.checksum, block_reference.address);

            const entry_count = Block.entry_count(block);
            const labels_used = Block.labels_const(block)[0..entry_count];
            const tables_used = Block.tables_const(block)[0..entry_count];

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;

            var entry = entry_count;
            while (entry > 0) {
                entry -= 1;

                const label = labels_used[entry];
                const table = &tables_used[entry];

                if (manifest.insert_table_extent(manifest_log.tree_hash, table.address, block_reference.address, entry)) {
                    switch (label.event) {
                        .insert => manifest_log.open_event(manifest_log, label.level, table),
                        .remove => manifest.queue_for_compaction(block_reference.address),
                    }
                } else {
                    manifest.queue_for_compaction(block_reference.address);
                }
            }

            if (Block.entry_count(block) < Block.entry_count_max) {
                manifest.queue_for_compaction(block_reference.address);
            }

            log.debug("{}: opened: checksum={} address={} entries={}", .{
                manifest_log.tree_hash,
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
            assert(!manifest_log.writing);
            manifest_log.append(.{ .level = level, .event = .insert }, table);
        }

        /// Appends the removal of a table from a level.
        /// The table must have previously been inserted to the manifest log.
        pub fn remove(manifest_log: *ManifestLog, level: u7, table: *const TableInfo) void {
            assert(!manifest_log.writing);
            manifest_log.append(.{ .level = level, .event = .remove }, table);
        }

        fn append(manifest_log: *ManifestLog, label: Label, table: *const TableInfo) void {
            assert(manifest_log.opened);
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

            assert(manifest_log.entry_count < Block.entry_count_max);
            assert(manifest_log.blocks.count - manifest_log.blocks_closed == 1);

            log.debug(
                "{}: {s}: level={} checksum={} address={} flags={} snapshot={}..{}",
                .{
                    manifest_log.tree_hash,
                    @tagName(label.event),
                    label.level,
                    table.checksum,
                    table.address,
                    table.flags,
                    table.snapshot_min,
                    table.snapshot_max,
                },
            );

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry = manifest_log.entry_count;
            Block.labels(block)[entry] = label;
            Block.tables(block)[entry] = table.*;

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;
            const address = Block.address(block);
            if (manifest.update_table_extent(manifest_log.tree_hash, table.address, address, entry)) |previous_block| {
                manifest.queue_for_compaction(previous_block);
                if (label.event == .remove) manifest.queue_for_compaction(address);
            } else {
                // A remove must remove a insert, which implies that it must update the extent.
                assert(label.event != .remove);
            }

            manifest_log.entry_count += 1;
            if (manifest_log.entry_count == Block.entry_count_max) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
            }
        }

        fn flush(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);

            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            log.debug("{}: flush: writing {} block(s)", .{
                manifest_log.tree_hash,
                manifest_log.blocks_closed,
            });

            // The manifest is updated synchronously relative to the beginning of compact() and
            // checkpoint() so that the SuperBlock.Manifest.append()s are deterministic relative
            // to other trees' manifest logs.
            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;
            var i: usize = 0;
            while (i < manifest_log.blocks_closed) : (i += 1) {
                const block = manifest_log.blocks.get_ptr(i).?.*;
                verify_block(block, null, null);

                const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                const address = Block.address(block);
                assert(address > 0);

                manifest.append(manifest_log.tree_hash, header.checksum, address);
                if (Block.entry_count(block) < Block.entry_count_max) {
                    manifest.queue_for_compaction(address);
                }
            }

            manifest_log.write_block();
        }

        fn write_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(manifest_log.writing);
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);

            if (manifest_log.blocks_closed == 0) {
                if (manifest_log.blocks.count == 0) {
                    assert(manifest_log.entry_count == 0);
                } else {
                    assert(manifest_log.blocks.count == 1);
                    assert(manifest_log.entry_count < Block.entry_count_max);
                }

                const callback = manifest_log.write_callback.?;
                manifest_log.write_callback = null;
                manifest_log.writing = false;

                callback(manifest_log);
                return;
            }

            const block = manifest_log.blocks.head_ptr().?;
            verify_block(block.*, null, null);

            const header = mem.bytesAsValue(vsr.Header, block.*[0..@sizeOf(vsr.Header)]);
            const address = Block.address(block.*);
            assert(address > 0);

            const entry_count = Block.entry_count(block.*);

            if (manifest_log.blocks_closed == 1 and manifest_log.blocks.count == 1) {
                // This might be the last block of a checkpoint, which can be a partial block.
                assert(entry_count > 0);
            } else {
                assert(entry_count == Block.entry_count_max);
            }

            log.debug("{}: write_block: checksum={} address={} entries={}", .{
                manifest_log.tree_hash,
                header.checksum,
                address,
                entry_count,
            });

            manifest_log.grid.write_block(
                write_block_callback,
                &manifest_log.write,
                block,
                address,
            );
            manifest_log.blocks.advance_head();
        }

        fn write_block_callback(write: *Grid.Write) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "write", write);
            assert(manifest_log.opened);
            assert(manifest_log.writing);

            manifest_log.blocks_closed -= 1;
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);

            manifest_log.write_block();
        }

        pub fn reserve(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            // reserve() is called at the start of compaction, so we have:
            // - at most 1 closed block, and
            // - at most 1 open block
            // due to the last log compaction plus a leftover partial block.
            assert(manifest_log.blocks_closed <= 1);
            assert(manifest_log.blocks.count <= manifest_log.blocks_closed + 1);

            // TODO Make sure this cannot fail — before compaction begins verify that enough free
            // blocks are available for all reservations.
            // +1 for the manifest log block compaction, which acquires at most one block.
            manifest_log.grid_reservation = manifest_log.grid.reserve(1 + blocks_count_appends).?;
        }

        /// `compact` does not close a partial block; that is only necessary during `checkpoint`.
        pub fn compact(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation != null);

            const free_set = manifest_log.grid.superblock.free_set;
            assert(free_set.count_free_reserved(manifest_log.grid_reservation.?) >= 1);

            manifest_log.read_callback = callback;
            manifest_log.flush(compact_flush_callback);
        }

        fn compact_flush_callback(manifest_log: *ManifestLog) void {
            const callback = manifest_log.read_callback.?;

            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.grid_reservation != null);

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;

            // Compact a single manifest block — to minimize latency spikes, we want to do the bare
            // minimum of compaction work required.
            // TODO Compact more than 1 block if fragmentation is outstripping the compaction rate.
            // (Make sure to update the grid block reservation to account for this).
            // Or assert that compactions cannot update blocks fast enough to outpace manifest
            // log compaction (relative to the number of updates that fit in a manifest log block).
            if (manifest.oldest_block_queued_for_compaction(manifest_log.tree_hash)) |block| {
                assert(block.tree == manifest_log.tree_hash);
                assert(block.address > 0);

                manifest_log.reading = true;
                manifest_log.read_block_reference = block;

                manifest_log.grid.read_block(
                    compact_read_block_callback,
                    &manifest_log.read,
                    block.address,
                    block.checksum,
                    .manifest,
                );
            } else {
                manifest_log.read_callback = null;
                manifest_log.grid.forfeit(manifest_log.grid_reservation.?);
                manifest_log.grid_reservation = null;
                callback(manifest_log);
            }
        }

        fn compact_read_block_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            const block_reference = manifest_log.read_block_reference.?;
            verify_block(block, block_reference.checksum, block_reference.address);

            const entry_count = Block.entry_count(block);
            const labels_used = Block.labels_const(block)[0..entry_count];
            const tables_used = Block.tables_const(block)[0..entry_count];

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
                if (manifest.remove_table_extent(manifest_log.tree_hash, table.address, block_reference.address, entry)) {
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

            log.debug("{}: compacted: checksum={} address={} frees={}/{}", .{
                manifest_log.tree_hash,
                block_reference.checksum,
                block_reference.address,
                frees,
                entry_count,
            });

            // Blocks may be compacted if they contain frees, or are not completely full.
            // For example, a partial block may be flushed as part of a checkpoint.
            assert(frees > 0 or entry_count < Block.entry_count_max);

            assert(manifest.queued_for_compaction(block_reference.address));
            manifest.remove(
                manifest_log.tree_hash,
                block_reference.checksum,
                block_reference.address,
            );
            assert(!manifest.queued_for_compaction(block_reference.address));

            manifest_log.grid.release(block_reference.address);
            manifest_log.grid.forfeit(manifest_log.grid_reservation.?);
            manifest_log.grid_reservation = null;

            const callback = manifest_log.read_callback.?;
            manifest_log.reading = false;
            manifest_log.read_callback = null;
            manifest_log.read_block_reference = null;

            callback(manifest_log);
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
            assert(manifest_log.blocks.count == manifest_log.blocks_closed + 1);

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry_count = manifest_log.entry_count;
            assert(entry_count > 0);
            assert(entry_count <= Block.entry_count_max);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.cluster == manifest_log.superblock.working.cluster);
            assert(header.op > 0);
            assert(header.command == .block);
            header.size = Block.size(entry_count);

            // Zero unused labels:
            mem.set(u8, mem.sliceAsBytes(Block.labels(block)[entry_count..]), 0);

            // Zero unused tables, and padding:
            mem.set(u8, block[header.size..], 0);

            {
                vsr.checksum_context = "ManifestLog.close_block";
                defer vsr.checksum_context = null;

                header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();
            }

            verify_block(block, null, null);
            assert(Block.entry_count(block) == entry_count);

            log.debug("{}: close_block: checksum={} address={} entries={}", .{
                manifest_log.tree_hash,
                header.checksum,
                Block.address(block),
                entry_count,
            });

            manifest_log.blocks_closed += 1;
            manifest_log.entry_count = 0;
            assert(manifest_log.blocks.count == manifest_log.blocks_closed);
        }

        fn verify_block(block: BlockPtrConst, checksum: ?u128, address: ?u64) void {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(BlockType.from(header.operation) == .manifest);

            if (constants.verify) {
                assert(header.valid_checksum());
                assert(header.valid_checksum_body(block[@sizeOf(vsr.Header)..header.size]));
            }

            assert(checksum == null or header.checksum == checksum.?);

            assert(Block.address(block) > 0);
            assert(address == null or Block.address(block) == address.?);

            const entry_count = Block.entry_count(block);
            assert(entry_count > 0);
        }
    };
}

fn ManifestLogBlockType(comptime Storage: type, comptime TableInfo: type) type {
    return struct {
        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;

        const block_body_size = constants.block_size - @sizeOf(vsr.Header);
        const entry_size = @sizeOf(Label) + @sizeOf(TableInfo);
        const entry_count_max_unaligned = @divFloor(block_body_size, entry_size);
        pub const entry_count_max = @divFloor(
            entry_count_max_unaligned,
            @alignOf(TableInfo),
        ) * @alignOf(TableInfo);

        comptime {
            assert(entry_count_max > 0);
            assert((entry_count_max * @sizeOf(Label)) % @alignOf(TableInfo) == 0);
            assert((entry_count_max * @sizeOf(TableInfo)) % @alignOf(TableInfo) == 0);
        }

        pub const Label = packed struct {
            level: u7,
            event: enum(u1) { insert, remove },
        };

        pub fn address(block: BlockPtrConst) u64 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.command == .block);

            const block_address = header.op;
            assert(block_address > 0);
            return block_address;
        }

        pub fn checksum(block: BlockPtrConst) u128 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.command == .block);

            return header.checksum;
        }

        pub fn entry_count(block: BlockPtrConst) u32 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.command == .block);

            const labels_size = entry_count_max * @sizeOf(Label);
            const tables_size = header.size - @sizeOf(vsr.Header) - labels_size;

            const entry_count_ = @intCast(u32, @divExact(tables_size, @sizeOf(TableInfo)));
            assert(entry_count_ > 0);
            assert(entry_count_ <= entry_count_max);
            return entry_count_;
        }

        pub fn size(entry_count_: u32) u32 {
            assert(entry_count_ > 0);
            assert(entry_count_ <= entry_count_max);

            // Encode the smaller type first because this will be multiplied by entry_count_max.
            const labels_size = entry_count_max * @sizeOf(Label);
            assert(labels_size == labels_size_max);
            assert((@sizeOf(vsr.Header) + labels_size) % @alignOf(TableInfo) == 0);
            const tables_size = entry_count_ * @sizeOf(TableInfo);

            return @sizeOf(vsr.Header) + labels_size + tables_size;
        }

        const labels_size_max = entry_count_max * @sizeOf(Label);

        pub fn labels(block: BlockPtr) *[entry_count_max]Label {
            return mem.bytesAsSlice(
                Label,
                block[@sizeOf(vsr.Header)..][0..labels_size_max],
            )[0..entry_count_max];
        }

        pub fn labels_const(block: BlockPtrConst) *const [entry_count_max]Label {
            return mem.bytesAsSlice(
                Label,
                block[@sizeOf(vsr.Header)..][0..labels_size_max],
            )[0..entry_count_max];
        }

        const tables_size_max = entry_count_max * @sizeOf(TableInfo);

        pub fn tables(block: BlockPtr) *[entry_count_max]TableInfo {
            return mem.bytesAsSlice(
                TableInfo,
                block[@sizeOf(vsr.Header) + labels_size_max ..][0..tables_size_max],
            )[0..entry_count_max];
        }

        pub fn tables_const(block: BlockPtrConst) *const [entry_count_max]TableInfo {
            return mem.bytesAsSlice(
                TableInfo,
                block[@sizeOf(vsr.Header) + labels_size_max ..][0..tables_size_max],
            )[0..entry_count_max];
        }
    };
}
