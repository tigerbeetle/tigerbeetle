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

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const SuperBlockType = vsr.SuperBlockType;
const GridType = @import("grid.zig").GridType;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

pub fn ManifestLogType(comptime Storage: type, comptime TableInfo: type) type {
    return struct {
        const ManifestLog = @This();

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        const BlockPtr = *align(config.sector_size) [config.block_size]u8;
        const BlockPtrConst = *align(config.sector_size) const [config.block_size]u8;

        pub const Callback = fn (manifest_log: *ManifestLog) void;

        pub const OpenEvent = fn (
            manifest_log: *ManifestLog,
            level: u7,
            table: *const TableInfo,
        ) void;

        pub const Label = packed struct {
            level: u7,
            event: enum(u1) { insert, remove },
        };

        const alignment = 16;

        comptime {
            // Bit 7 is reserved to indicate whether the event is an insert or remove.
            assert(config.lsm_levels <= math.maxInt(u7) + 1);

            assert(@sizeOf(Label) == @sizeOf(u8));

            // All TableInfo's should already be 16-byte aligned because of the leading checksum.
            assert(@alignOf(TableInfo) == alignment);

            // For keys { 8, 16, 24, 32 } all TableInfo's should be a multiple of the alignment.
            // However, we still store Label ahead of TableInfo to save space on the network.
            // This means we store fewer entries per manifest block, to gain less padding,
            // since we must store entry_count_max of whichever array is first in the layout.
            // For a better understanding of this decision, see block_size() below.
            assert(@sizeOf(TableInfo) % alignment == 0);
        }

        const block_body_size = config.block_size - @sizeOf(vsr.Header);
        const entry_size = @sizeOf(Label) + @sizeOf(TableInfo);
        const entry_count_max_unaligned = @divFloor(block_body_size, entry_size);
        const entry_count_max = @divFloor(entry_count_max_unaligned, alignment) * alignment;

        comptime {
            assert(entry_count_max > 0);
            assert((entry_count_max * @sizeOf(Label)) % alignment == 0);
            assert((entry_count_max * @sizeOf(TableInfo)) % alignment == 0);
        }

        superblock: *SuperBlock,
        grid: *Grid,
        tree_hash: u128,

        /// The head block is used to accumulate a full block, to be written at the next flush.
        /// The remaining blocks must accommodate all further appends.
        // TODO Assert the relation between the number of blocks, and flush/compact/append.
        blocks: RingBuffer(BlockPtr, 3, .array),

        /// The number of blocks that have been appended to, filled up, and then closed.
        blocks_closed: u8 = 0,

        /// The number of entries in the open block.
        entry_count: u32 = 0,

        opened: bool = false,
        open_event: OpenEvent = undefined,
        open_iterator: SuperBlock.Manifest.IteratorReverse = undefined,

        reading: bool = false,
        read: Grid.Read = undefined,
        read_callback: Callback = undefined,
        read_block_reference: ?SuperBlock.Manifest.BlockReference = null,

        writing: bool = false,
        write: Grid.Write = undefined,
        write_callback: Callback = undefined,

        pub fn init(allocator: mem.Allocator, grid: *Grid, tree_hash: u128) !ManifestLog {
            // TODO RingBuffer for .pointer should be extended to take care of alignment:

            const a = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(a);

            const b = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(b);

            const c = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(b);

            return ManifestLog{
                .superblock = grid.superblock,
                .grid = grid,
                .tree_hash = tree_hash,
                .blocks = .{
                    .buffer = .{
                        a[0..config.block_size],
                        b[0..config.block_size],
                        c[0..config.block_size],
                    },
                },
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

            manifest_log.read_block_reference = manifest_log.open_iterator.next();

            if (manifest_log.read_block_reference) |block| {
                assert(block.tree == manifest_log.tree_hash);
                assert(block.address > 0);

                manifest_log.grid.read_block(
                    open_read_block_callback,
                    &manifest_log.read,
                    block.address,
                    block.checksum,
                );
            } else {
                manifest_log.opened = true;
                manifest_log.open_event = undefined;
                manifest_log.open_iterator = undefined;

                const callback = manifest_log.read_callback;
                manifest_log.reading = false;
                manifest_log.read_callback = undefined;
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

            const entry_count = block_entry_count(block);
            const labels_used = labels_const(block)[0..entry_count];
            const tables_used = tables_const(block)[0..entry_count];

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
            manifest_log.append(.{ .level = level, .event = .insert }, table);
        }

        /// Appends the removal of a table from a level.
        /// The table must have previously been inserted to the manifest log.
        pub fn remove(manifest_log: *ManifestLog, level: u7, table: *const TableInfo) void {
            manifest_log.append(.{ .level = level, .event = .remove }, table);
        }

        fn append(manifest_log: *ManifestLog, label: Label, table: *const TableInfo) void {
            assert(manifest_log.opened);
            assert(label.level < config.lsm_levels);
            assert(table.address > 0);
            assert(table.snapshot_min > 0);
            assert(table.snapshot_max > table.snapshot_min);

            if (manifest_log.blocks.empty()) {
                manifest_log.acquire_block();
            } else if (manifest_log.entry_count == entry_count_max) {
                assert(manifest_log.blocks.count > 0);
                manifest_log.close_block();
                manifest_log.acquire_block();
            } else if (manifest_log.entry_count > 0) {
                assert(manifest_log.blocks.count > 0);
            }

            assert(manifest_log.entry_count < entry_count_max);
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
            labels(block)[entry] = label;
            tables(block)[entry] = table.*;

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;
            const address = block_address(block);
            if (manifest.update_table_extent(table.address, address, entry)) |previous_block| {
                manifest.queue_for_compaction(previous_block);
                if (label.event == .remove) manifest.queue_for_compaction(address);
            } else {
                // A remove must remove a insert, which implies that it must update the extent.
                assert(label.event != .remove);
            }

            manifest_log.entry_count += 1;
        }

        pub fn flush(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);

            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            log.debug("{}: flush: writing {} block(s)", .{
                manifest_log.tree_hash,
                manifest_log.blocks_closed,
            });
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
                    assert(manifest_log.entry_count < entry_count_max);
                }

                const callback = manifest_log.write_callback;
                manifest_log.write_callback = undefined;
                manifest_log.writing = false;

                callback(manifest_log);
                return;
            }

            const block = manifest_log.blocks.head().?;
            verify_block(block, null, null);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = block_address(block);
            assert(address > 0);

            const entry_count = block_entry_count(block);

            if (manifest_log.blocks_closed == 1 and manifest_log.blocks.count == 1) {
                assert(entry_count > 0);
            } else {
                assert(entry_count == entry_count_max);
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
        }

        fn write_block_callback(write: *Grid.Write) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "write", write);
            assert(manifest_log.opened);
            assert(manifest_log.writing);

            const block = manifest_log.blocks.head().?;
            verify_block(block, null, null);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = block_address(block);
            assert(address > 0);

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;

            manifest.append(manifest_log.tree_hash, header.checksum, address);
            if (block_entry_count(block) < entry_count_max) manifest.queue_for_compaction(address);

            manifest_log.blocks_closed -= 1;
            manifest_log.blocks.advance_head();
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);

            manifest_log.write_block();
        }

        pub fn compact(manifest_log: *ManifestLog, callback: Callback) void {
            assert(!manifest_log.reading);
            manifest_log.read_callback = callback;
            manifest_log.flush(flush_callback);
        }

        fn flush_callback(manifest_log: *ManifestLog) void {
            const callback = manifest_log.read_callback;
            manifest_log.read_callback = undefined;

            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);

            const manifest: *SuperBlock.Manifest = &manifest_log.superblock.manifest;

            if (manifest.oldest_block_queued_for_compaction(manifest_log.tree_hash)) |block| {
                assert(block.tree == manifest_log.tree_hash);
                assert(block.address > 0);

                manifest_log.reading = true;
                manifest_log.read_callback = callback;
                manifest_log.read_block_reference = block;

                manifest_log.grid.read_block(
                    compact_callback,
                    &manifest_log.read,
                    block.address,
                    block.checksum,
                );
            } else {
                callback(manifest_log);
            }
        }

        fn compact_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            const block_reference = manifest_log.read_block_reference.?;
            verify_block(block, block_reference.checksum, block_reference.address);

            const entry_count = block_entry_count(block);
            const labels_used = labels_const(block)[0..entry_count];
            const tables_used = tables_const(block)[0..entry_count];

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

            log.debug("{}: compacted: checksum={} address={} frees={}/{}", .{
                manifest_log.tree_hash,
                block_reference.checksum,
                block_reference.address,
                frees,
                entry_count,
            });

            // Blocks may be compacted if they contain frees, or are not completely full.
            // For example, a partial block may be flushed as part of a checkpoint.
            assert(frees > 0 or entry_count < entry_count_max);

            assert(manifest.queued_for_compaction(block_reference.address));
            manifest.remove(
                manifest_log.tree_hash,
                block_reference.checksum,
                block_reference.address,
            );
            assert(!manifest.queued_for_compaction(block_reference.address));

            manifest_log.grid.release(block_reference.address);

            const callback = manifest_log.read_callback;
            manifest_log.reading = false;
            manifest_log.read_callback = undefined;
            manifest_log.read_block_reference = null;

            callback(manifest_log);
        }

        pub fn checkpoint(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);

            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            if (manifest_log.entry_count > 0) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
                assert(manifest_log.blocks_closed > 0);
                assert(manifest_log.blocks_closed == manifest_log.blocks.count);
            }

            log.debug("checkpoint: writing {} block(s)", .{manifest_log.blocks_closed});
            manifest_log.write_block();
        }

        fn acquire_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.entry_count == 0);
            assert(!manifest_log.blocks.full());

            manifest_log.blocks.advance_tail();

            const block: BlockPtr = manifest_log.blocks.tail().?;

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            header.* = .{
                .cluster = manifest_log.superblock.working.cluster,
                .op = manifest_log.grid.acquire(),
                .size = undefined,
                .command = .block,
            };
        }

        fn close_block(manifest_log: *ManifestLog) void {
            const block: BlockPtr = manifest_log.blocks.tail().?;

            const entry_count = manifest_log.entry_count;
            assert(entry_count > 0);
            assert(entry_count <= entry_count_max);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.cluster == manifest_log.superblock.working.cluster);
            assert(header.op > 0);
            header.size = block_size(entry_count);
            assert(header.command == .block);

            // Zero unused labels:
            mem.set(u8, mem.sliceAsBytes(labels(block)[entry_count..]), 0);

            // Zero unused tables, and padding:
            mem.set(u8, block[header.size..], 0);

            header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
            header.set_checksum();

            verify_block(block, null, null);
            assert(block_entry_count(block) == entry_count);

            log.debug("{}: close_block: checksum={} address={} entries={}", .{
                manifest_log.tree_hash,
                header.checksum,
                block_address(block),
                entry_count,
            });

            manifest_log.blocks_closed += 1;
            manifest_log.entry_count = 0;
        }

        fn verify_block(block: BlockPtrConst, checksum: ?u128, address: ?u64) void {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);

            if (config.verify) {
                assert(header.valid_checksum());
                assert(header.valid_checksum_body(block[@sizeOf(vsr.Header)..header.size]));
            }

            assert(checksum == null or header.checksum == checksum.?);

            assert(block_address(block) > 0);
            assert(address == null or block_address(block) == address.?);

            const entry_count = block_entry_count(block);
            assert(entry_count > 0);
        }

        fn block_address(block: BlockPtrConst) u64 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.command == .block);

            const address = header.op;
            assert(address > 0);
            return address;
        }

        fn block_checksum(block: BlockPtrConst) u128 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.command == .block);

            return header.checksum;
        }

        fn block_entry_count(block: BlockPtrConst) u32 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            assert(header.command == .block);

            const labels_size = entry_count_max * @sizeOf(Label);
            const tables_size = header.size - @sizeOf(vsr.Header) - labels_size;

            const entry_count = @intCast(u32, @divExact(tables_size, @sizeOf(TableInfo)));
            assert(entry_count > 0);
            assert(entry_count <= entry_count_max);
            return entry_count;
        }

        fn block_size(entry_count: u32) u32 {
            assert(entry_count > 0);
            assert(entry_count <= entry_count_max);

            // Encode the smaller type first because this will be multiplied by entry_count_max.
            const labels_size = entry_count_max * @sizeOf(Label);
            const tables_size = entry_count * @sizeOf(TableInfo);

            return @sizeOf(vsr.Header) + labels_size + tables_size;
        }

        const labels_size_max = entry_count_max * @sizeOf(Label);

        fn labels(block: BlockPtr) *[entry_count_max]Label {
            return mem.bytesAsSlice(
                Label,
                block[@sizeOf(vsr.Header)..][0..labels_size_max],
            )[0..entry_count_max];
        }

        fn labels_const(block: BlockPtrConst) *const [entry_count_max]Label {
            return mem.bytesAsSlice(
                Label,
                block[@sizeOf(vsr.Header)..][0..labels_size_max],
            )[0..entry_count_max];
        }

        const tables_size_max = entry_count_max * @sizeOf(TableInfo);

        fn tables(block: BlockPtr) *[entry_count_max]TableInfo {
            return mem.bytesAsSlice(
                TableInfo,
                block[@sizeOf(vsr.Header) + entry_count_max ..][0..tables_size_max],
            )[0..entry_count_max];
        }

        fn tables_const(block: BlockPtrConst) *const [entry_count_max]TableInfo {
            return mem.bytesAsSlice(
                TableInfo,
                block[@sizeOf(vsr.Header) + entry_count_max ..][0..tables_size_max],
            )[0..entry_count_max];
        }
    };
}

// TODO This is a manual runner to be replaced with a fuzz test.
fn ManifestLogTestType(
    comptime Storage: type,
    comptime TableInfo: type,
) type {
    return struct {
        const ManifestLogTest = @This();
        const ManifestLog = ManifestLogType(Storage, TableInfo);

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        superblock: *SuperBlock,
        superblock_context: SuperBlock.Context = undefined,
        manifest_log: ManifestLog,
        pending: usize = 0,

        fn init(allocator: mem.Allocator, grid: *Grid) !ManifestLogTest {
            const tree_hash: u128 = std.math.maxInt(u128);

            var manifest_log = try ManifestLog.init(allocator, grid, tree_hash);
            errdefer manifest_log.deinit(allocator);

            return ManifestLogTest{
                .superblock = grid.superblock,
                .manifest_log = manifest_log,
            };
        }

        fn deinit(t: *ManifestLogTest, allocator: mem.Allocator) void {
            t.manifest_log.deinit(allocator);
        }

        fn format_superblock(t: *ManifestLogTest) void {
            t.pending += 1;
            t.superblock.format(format_superblock_callback, &t.superblock_context, .{
                .cluster = 10,
                .replica = 0,
                .size_max = 512 * 1024 * 1024,
            });
        }

        fn format_superblock_callback(context: *SuperBlock.Context) void {
            const t = @fieldParentPtr(ManifestLogTest, "superblock_context", context);
            t.pending -= 1;
            t.open_superblock();
        }

        fn open_superblock(t: *ManifestLogTest) void {
            t.pending += 1;
            t.superblock.open(open_superblock_callback, &t.superblock_context);
        }

        fn open_superblock_callback(context: *SuperBlock.Context) void {
            const t = @fieldParentPtr(ManifestLogTest, "superblock_context", context);
            t.pending -= 1;

            t.open();
        }

        fn open(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.open(open_event, open_callback);
        }

        fn open_event(manifest_log: *ManifestLog, level: u7, table: *const TableInfo) void {
            log.debug(
                "{}: open_event: level={} checksum={} address={} flags={} snapshot={}..{}",
                .{
                    manifest_log.tree_hash,
                    level,
                    table.checksum,
                    table.address,
                    table.flags,
                    table.snapshot_min,
                    table.snapshot_max,
                },
            );
        }

        fn open_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
            t.pending -= 1;

            t.manifest_log.insert(2, &TableInfo{
                .checksum = 123,
                .address = 7,
                .flags = 0,
                .snapshot_min = 42,
                .key_min = 50,
                .key_max = 100,
            });

            t.flush();
        }

        fn flush(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.flush(flush_callback);
        }

        fn flush_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
            t.pending -= 1;
            t.checkpoint();
        }

        fn checkpoint(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.checkpoint(checkpoint_callback);
        }

        fn checkpoint_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
            t.pending -= 1;

            t.manifest_log.insert(2, &TableInfo{
                .checksum = 123,
                .address = 7,
                .flags = 0,
                .snapshot_min = 42,
                .snapshot_max = 50,
                .key_min = 50,
                .key_max = 100,
            });

            t.manifest_log.remove(2, &TableInfo{
                .checksum = 123,
                .address = 7,
                .flags = 0,
                .snapshot_min = 42,
                .snapshot_max = 50,
                .key_min = 50,
                .key_max = 100,
            });

            t.checkpoint_again();
        }

        fn checkpoint_again(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.checkpoint(checkpoint_again_callback);
        }

        fn checkpoint_again_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
            t.pending -= 1;
            t.compact();
        }

        fn compact(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.compact(compact_callback);
        }

        fn compact_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
            t.pending -= 1;

            const tree = t.manifest_log.tree_hash;
            if (t.manifest_log.superblock.manifest.oldest_block_queued_for_compaction(tree)) |_| {
                t.compact();
            } else {
                t.checkpoint_superblock();
            }
        }

        fn checkpoint_superblock(t: *ManifestLogTest) void {
            t.pending += 1;
            t.superblock.checkpoint(checkpoint_superblock_callback, &t.superblock_context);
        }

        fn checkpoint_superblock_callback(context: *SuperBlock.Context) void {
            const t = @fieldParentPtr(ManifestLogTest, "superblock_context", context);
            t.pending -= 1;
        }
    };
}

pub fn main() !void {
    const testing = std.testing;
    const allocator = testing.allocator;

    testing.log_level = .debug;

    const os = std.os;
    const IO = @import("../io.zig").IO;
    const Storage = @import("../storage.zig").Storage;
    const SuperBlock = SuperBlockType(Storage);
    const Grid = @import("grid.zig").GridType(Storage);

    const dir_path = ".";
    const dir_fd = os.openZ(dir_path, os.O.CLOEXEC | os.O.RDONLY, 0) catch |err| {
        std.debug.print("failed to open directory '{s}': {}", .{ dir_path, err });
        return;
    };

    const size_max = 2 * 1024 * 1024 * 1024;
    const storage_fd = try IO.open_file(dir_fd, "test_manifest_log", size_max, true);
    defer std.fs.cwd().deleteFile("test_manifest_log") catch {};

    var io = try IO.init(128, 0);
    defer io.deinit();

    var storage = try Storage.init(&io, storage_fd);
    defer storage.deinit();

    var superblock = try SuperBlock.init(allocator, &storage);
    defer superblock.deinit(allocator);

    var grid = try Grid.init(allocator, &superblock);
    defer grid.deinit(allocator);

    const TableInfo = extern struct {
        checksum: u128,
        address: u64,
        flags: u64 = 0,

        /// The minimum snapshot that can see this table (with exclusive bounds).
        /// This value is set to the current snapshot tick on table creation.
        snapshot_min: u64,

        /// The maximum snapshot that can see this table (with exclusive bounds).
        /// This value is set to the current snapshot tick on table deletion.
        snapshot_max: u64 = math.maxInt(u64),

        key_min: u128,
        key_max: u128,
    };
    assert(@sizeOf(TableInfo) == 48 + 16 * 2);
    assert(@alignOf(TableInfo) == 16);
    assert(@bitSizeOf(TableInfo) == @sizeOf(TableInfo) * 8);

    const ManifestLogTest = ManifestLogTestType(Storage, TableInfo);

    var t = try ManifestLogTest.init(allocator, &grid);
    defer t.deinit(allocator);

    t.format_superblock();
    while (t.pending > 0) try io.run_for_ns(100);
}
