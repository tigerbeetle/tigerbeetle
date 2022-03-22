const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const SuperBlockType = @import("superblock.zig").SuperBlockType;
const GridType = @import("grid.zig").GridType;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

const log = std.log.scoped(.manifest_log);

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

pub fn ManifestLogType(comptime Storage: type, comptime TableInfo: type) type {
    return struct {
        const ManifestLog = @This();

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        const BlockPtr = *align(config.sector_size) [config.block_size]u8;
        const BlockPtrConst = *align(config.sector_size) const [config.block_size]u8;

        pub const Callback = fn (manifest_log: *ManifestLog) void;

        pub const Label = packed struct {
            level: u5,
            event: enum(u3) { insert, update, remove },
        };

        comptime {
            // Bits { 5, 6, 7 } are reserved to indicate { insert, update, remove } respectively.
            assert(config.lsm_levels <= math.maxInt(u5));

            assert(@sizeOf(Label) == @sizeOf(u8));

            // All tables should already be 16-byte aligned because of the leading checksum.
            assert(@alignOf(TableInfo) == 16);
        }

        const alignment = 16;
        const block_body_size = config.block_size - @sizeOf(vsr.Header);
        const entry_size = @sizeOf(Label) + @sizeOf(TableInfo);
        const entry_count_max_unaligned = @divFloor(block_body_size, entry_size);
        const entry_count_max = 16; // TODO @divFloor(entry_count_max_unaligned, alignment) * alignment;

        superblock: *SuperBlock,
        grid: *Grid,
        tree: u8,

        // TODO Assert that one spare block is enough for all manifest updates for a tree.
        // It should be, provided block_size >= 16384 for 8 levels.
        blocks: RingBuffer(BlockPtr, 2, .array),

        /// The number of blocks that have been appended to, filled up, and then closed.
        blocks_closed: u8 = 0,

        entry_count: u32 = 0,

        reading: bool = false,
        read: Grid.Read = undefined,
        read_callback: Callback = undefined,

        writing: bool = false,
        write: Grid.Write = undefined,
        write_callback: Callback = undefined,

        open_iterator: SuperBlock.Manifest.IteratorReverse = undefined,
        open_block_reference: ?SuperBlock.Manifest.BlockReference = null,

        pub fn init(
            allocator: mem.Allocator,
            superblock: *SuperBlock,
            grid: *Grid,
            tree: u8,
        ) !ManifestLog {
            // TODO RingBuffer for .pointer should be extended to take care of alignment:

            const a = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(a);

            const b = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(b);

            return ManifestLog{
                .superblock = superblock,
                .grid = grid,
                .tree = tree,
                .blocks = .{
                    .buffer = .{
                        a[0..config.block_size],
                        b[0..config.block_size],
                    },
                },
            };
        }

        pub fn deinit(manifest_log: *ManifestLog, allocator: mem.Allocator) void {
            for (manifest_log.blocks.buffer) |block| allocator.free(block);
        }

        pub fn insert(manifest_log: *ManifestLog, level: u5, table: *const TableInfo) void {
            manifest_log.append(.{ .level = level, .event = .insert }, table);
        }

        pub fn update(manifest_log: *ManifestLog, level: u5, table: *const TableInfo) void {
            manifest_log.append(.{ .level = level, .event = .update }, table);
        }

        pub fn remove(manifest_log: *ManifestLog, level: u5, table: *const TableInfo) void {
            manifest_log.append(.{ .level = level, .event = .remove }, table);
        }

        pub fn flush(manifest_log: *ManifestLog, callback: Callback) void {
            assert(!manifest_log.writing);
            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            log.debug("flush: writing {} block(s)", .{manifest_log.blocks_closed});
            manifest_log.write_block();
        }

        pub fn checkpoint(manifest_log: *ManifestLog, callback: Callback) void {
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

        pub fn open(manifest_log: *ManifestLog, callback: Callback) void {
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);

            manifest_log.reading = true;
            manifest_log.read_callback = callback;
            manifest_log.open_iterator = manifest_log.superblock.manifest.iterator_reverse(
                manifest_log.tree,
            );

            manifest_log.open_read_block();
        }

        fn open_read_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            manifest_log.open_block_reference = manifest_log.open_iterator.next();

            if (manifest_log.open_block_reference) |block_reference| {
                assert(block_reference.tree == manifest_log.tree);
                assert(block_reference.address > 0);

                log.debug("open_read_block: checksum={x} address={}", .{
                    block_reference.checksum,
                    block_reference.address,
                });

                manifest_log.grid.read_block(
                    open_read_block_callback,
                    &manifest_log.read,
                    manifest_log.blocks.buffer[0], // TODO Grid will provide a BlockPtr.
                    block_reference.address,
                    block_reference.checksum,
                );
            } else {
                const callback = manifest_log.read_callback;
                manifest_log.reading = false;
                manifest_log.read_callback = undefined;
                manifest_log.open_iterator = undefined;
                callback(manifest_log);
            }
        }

        fn open_read_block_callback(read: *Grid.Read) void {
            const manifest_log = @fieldParentPtr(ManifestLog, "read", read);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            const block = manifest_log.blocks.buffer[0]; // TODO Grid will provide a BlockPtr.
            verify_block(
                block,
                manifest_log.open_block_reference.?.checksum,
                manifest_log.open_block_reference.?.address,
            );

            const entry_count = block_entry_count(block);
            assert(entry_count > 0);

            const labels_used = labels(block)[0..entry_count];
            const tables_used = tables(block)[0..entry_count];

            var index = entry_count;
            while (index > 0) {
                index -= 1;

                const label = labels_used[index];
                const table = tables_used[index];

                log.debug(
                    "{s}: tree={} level={} checksum={x} address={} flags={} snapshot={}..{}",
                    .{
                        @tagName(label.event),
                        manifest_log.tree,
                        label.level,
                        table.checksum,
                        table.address,
                        table.flags,
                        table.snapshot_min,
                        table.snapshot_max,
                    },
                );
            }

            manifest_log.open_read_block();
        }

        fn append(manifest_log: *ManifestLog, label: Label, table: *const TableInfo) void {
            assert(label.level < config.lsm_levels);
            assert(table.address > 0);
            assert(table.snapshot_min > 0);
            assert(table.snapshot_max > table.snapshot_min);
            // TODO Assert label.event in relation to table.snapshot_max.

            if (manifest_log.blocks.empty()) {
                assert(manifest_log.entry_count == 0);

                manifest_log.open_block();
            } else if (manifest_log.entry_count == entry_count_max) {
                assert(manifest_log.blocks.count > 0);
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);

                manifest_log.open_block();
            } else if (manifest_log.entry_count > 0) {
                assert(manifest_log.blocks.count > 0);
            }

            assert(manifest_log.entry_count < entry_count_max);
            assert(manifest_log.blocks.count - manifest_log.blocks_closed == 1);

            // TODO Use format specifier to pad checksum hex string.
            log.debug("{s}: tree={} level={} checksum={x} address={} flags={} snapshot={}..{}", .{
                @tagName(label.event),
                manifest_log.tree,
                label.level,
                table.checksum,
                table.address,
                table.flags,
                table.snapshot_min,
                table.snapshot_max,
            });

            const block: BlockPtr = manifest_log.blocks.tail().?;
            labels(block)[manifest_log.entry_count] = label;
            tables(block)[manifest_log.entry_count] = table.*;
            manifest_log.entry_count += 1;
        }

        fn open_block(manifest_log: *ManifestLog) void {
            assert(!manifest_log.blocks.full());
            assert(manifest_log.entry_count == 0);

            manifest_log.blocks.advance_tail();
        }

        fn close_block(manifest_log: *ManifestLog) void {
            const block: BlockPtr = manifest_log.blocks.tail().?;

            const entry_count = manifest_log.entry_count;
            assert(entry_count > 0);
            assert(entry_count <= entry_count_max);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = manifest_log.superblock.free_set.acquire().?;

            header.* = .{
                .cluster = manifest_log.superblock.working.cluster,
                .op = address,
                .size = block_size(entry_count),
                .command = .block,
            };

            // Zero unused labels:
            mem.set(u8, mem.sliceAsBytes(labels(block)[entry_count..]), 0);

            // Zero unused tables and padding:
            mem.set(u8, block[header.size..], 0);

            header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
            header.set_checksum();

            verify_block(block, null, address);
            assert(block_entry_count(block) == entry_count);

            log.debug("close_block: checksum={x} address={} entry_count={}", .{
                header.checksum,
                address,
                entry_count,
            });

            manifest_log.blocks_closed += 1;
            manifest_log.entry_count = 0;
        }

        fn write_block(manifest_log: *ManifestLog) void {
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

            log.debug("write_block: checksum={x} address={} entry_count={}", .{
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
            assert(manifest_log.writing);

            const block = manifest_log.blocks.head().?;
            verify_block(block, null, null);

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = block_address(block);
            assert(address > 0);

            manifest_log.superblock.manifest.append(manifest_log.tree, header.checksum, address);

            manifest_log.blocks_closed -= 1;
            manifest_log.blocks.advance_head();
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);

            manifest_log.write_block();
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

            const labels_size = entry_count_max * @sizeOf(Label);
            const tables_size = entry_count * @sizeOf(TableInfo);

            return @sizeOf(vsr.Header) + labels_size + tables_size;
        }

        fn labels(block: BlockPtr) *[entry_count_max]Label {
            const labels_size_max = entry_count_max * @sizeOf(Label);

            return mem.bytesAsSlice(
                Label,
                block[@sizeOf(vsr.Header)..][0..labels_size_max],
            )[0..entry_count_max];
        }

        fn tables(block: BlockPtr) *[entry_count_max]TableInfo {
            const tables_size_max = entry_count_max * @sizeOf(TableInfo);

            return mem.bytesAsSlice(
                TableInfo,
                block[@sizeOf(vsr.Header) + entry_count_max ..][0..tables_size_max],
            )[0..entry_count_max];
        }
    };
}

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

        fn init(allocator: mem.Allocator, superblock: *SuperBlock, grid: *Grid) !ManifestLogTest {
            const tree = 1;

            var manifest_log = try ManifestLog.init(allocator, superblock, grid, tree);
            errdefer manifest_log.deinit(allocator);

            return ManifestLogTest{
                .superblock = superblock,
                .manifest_log = manifest_log,
            };
        }

        fn deinit(t: *ManifestLogTest, allocator: mem.Allocator) void {
            t.manifest_log.deinit(allocator);
        }

        fn format(t: *ManifestLogTest) void {
            t.pending += 1;
            t.superblock.format(format_callback, &t.superblock_context, .{
                .cluster = 10,
                .replica = 0,
                .size_max = 512 * 1024 * 1024,
            });
        }

        fn format_callback(context: *SuperBlock.Context) void {
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

            t.append();
        }

        fn append(t: *ManifestLogTest) void {
            t.manifest_log.insert(2, &TableInfo{
                .checksum = 123,
                .address = 7,
                .flags = 0,
                .snapshot_min = 42,
                .key_min = 50,
                .key_max = 100,
            });

            t.manifest_log.update(2, &TableInfo{
                .checksum = 123,
                .address = 7,
                .flags = 0,
                .snapshot_min = 42,
                .snapshot_max = 50,
                .key_min = 50,
                .key_max = 100,
            });

            var i: usize = 0;
            while (i < 16) : (i += 1) {
                t.manifest_log.remove(2, &TableInfo{
                    .checksum = i,
                    .address = 7,
                    .flags = 0,
                    .snapshot_min = 42,
                    .snapshot_max = 50,
                    .key_min = 50,
                    .key_max = 100,
                });
            }

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
            t.checkpoint_nothing();
        }

        fn checkpoint_nothing(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.checkpoint(checkpoint_nothing_callback);
        }

        fn checkpoint_nothing_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
            t.pending -= 1;
            t.open();
        }

        fn open(t: *ManifestLogTest) void {
            t.pending += 1;
            t.manifest_log.open(open_callback);
        }

        fn open_callback(manifest_log: *ManifestLog) void {
            const t = @fieldParentPtr(ManifestLogTest, "manifest_log", manifest_log);
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
    const SuperBlock = @import("superblock.zig").SuperBlockType(Storage);
    const Grid = @import("grid.zig").GridType(Storage);

    const dir_path = ".";
    const dir_fd = os.openZ(dir_path, os.O.CLOEXEC | os.O.RDONLY, 0) catch |err| {
        std.debug.print("failed to open directory '{s}': {}", .{ dir_path, err });
        return;
    };

    const size_max = 2 * 1024 * 1024 * 1024;
    const storage_fd = try Storage.open(dir_fd, "test_manifest_log", size_max, true);
    defer std.fs.cwd().deleteFile("test_manifest_log") catch {};

    var io = try IO.init(128, 0);
    defer io.deinit();

    var storage = try Storage.init(&io, size_max, storage_fd);
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

    const ManifestLogTest = ManifestLogTestType(Storage, TableInfo);

    var t = try ManifestLogTest.init(allocator, &superblock, &grid);
    defer t.deinit(allocator);

    t.format();
    while (t.pending > 0) try io.run_for_ns(100);
}
