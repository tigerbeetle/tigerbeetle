//! The scrubber must not be enabled during the time between:
//! - request trailers, and
//! - `SuperBlock.sync`'s completion
//! otherwise TODO why not?
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.grid_scrubber);
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const IOPS = @import("../iops.zig").IOPS;
const schema = @import("../lsm/schema.zig");

const allocate_block = @import("../lsm/grid.zig").allocate_block;
const BlockType = @import("../lsm/grid.zig").BlockType;
const GridType = @import("../lsm/grid.zig").GridType;
const ForestTableIteratorType =
    @import("../lsm/forest_table_iterator.zig").ForestTableIteratorType;
const snapshot_from_op = @import("../lsm/manifest.zig").snapshot_from_op;
const TestStorage = @import("../testing/storage.zig").Storage;

pub fn GridScrubberType(comptime Forest: type) type {
    return struct {
        const GridScrubber = @This();
        const Grid = GridType(Forest.Storage);
        const ForestTableIterator = ForestTableIteratorType(Forest);
        const SuperBlock = vsr.SuperBlockType(Forest.Storage);

        const ReadSet = std.StaticBitSet(constants.grid_scrubber_reads_max);

        pub const BlockId = struct {
            block_checksum: u128,
            block_address: u64,
            block_type: BlockType,
        };

        pub const Read = struct {
            scrubber: *GridScrubber,
            read: Grid.Read = undefined,

            /// These fields are also on the Grid.ReadRepair, but we will need them even after the
            /// Grid's read is released. (See `reads_faulty`).
            /// TODO unnecessary; remove
            id: BlockId,

            /// When true: If the read completes and is corrupt, the block will *not* be added to
            /// the repair queue. This avoids a read-barrier at checkpoints (due to released
            /// blocks).
            ignore: bool = false,
        };

        pub const Options = struct {
            replica_index: u8,
        };

        // TODO pass into next() instead of init()?
        superblock: *SuperBlock,
        forest: *Forest,
        options: Options,

        reads: IOPS(Read, constants.grid_scrubber_reads_max) = .{},
        read_blocks: [constants.grid_scrubber_reads_max]Grid.BlockPtr,
        /// When `reads_faulty` is set, the corresponding Read in `reads`:
        /// - is not free,
        /// - is complete, and
        /// - must be repaired.
        reads_faulty: ReadSet = ReadSet.initEmpty(),

        /// When null: Scrubbing is disabled.
        /// When non-null: Scrub the entire forest slowly in the background.
        /// TODO Start each replica scrubbing from a (different) random offset.
        tour: ?union(enum) {
            init,
            done,
            manifest_log: struct {
                index: usize = 0,
            },
            table_index: struct {
                tables: ForestTableIterator,
            },
            table_content: struct {
                tables: ForestTableIterator,
                index_checksum: u128,
                index_address: u64,
                /// Points to `tour_index_block` once the index block has been read.
                index_block: ?Grid.BlockPtr = null,
                content_index: usize = 0,
            },
        },

        /// Contains a table index block when tour=table_content.
        tour_index_block: Grid.BlockPtr,

        pub fn init(
            allocator: std.mem.Allocator,
            forest: *Forest,
            options: Options,
        ) error{OutOfMemory}!GridScrubber {
            var read_blocks: [constants.grid_scrubber_reads_max]Grid.BlockPtr = undefined;
            for (read_blocks) |*buffer, i| {
                errdefer for (read_blocks[0..i]) |b| allocator.free(b);
                buffer.* = try allocate_block(allocator);
            }
            errdefer for (read_blocks) |buffer| allocator.free(buffer);

            var tour_index_block = try allocate_block(allocator);
            errdefer allocator.free(tour_index_block);

            return GridScrubber{
                .superblock = forest.grid.superblock,
                .forest = forest,
                .options = options,
                .read_blocks = read_blocks,
                .tour = null,
                .tour_index_block = tour_index_block,
            };
        }

        pub fn deinit(scrubber: *GridScrubber, allocator: std.mem.Allocator) void {
            allocator.free(scrubber.tour_index_block);
            for (scrubber.read_blocks) |buffer| allocator.free(buffer);

            scrubber.* = undefined;
        }

        pub fn start(scrubber: *GridScrubber) void {
            assert(scrubber.tour == null);
            assert(scrubber.forest.grid.superblock.opened);

            scrubber.tour = .init;

            log.debug("{}: start", .{scrubber.options.replica_index});
        }

        pub fn stop(scrubber: *GridScrubber) void {
            maybe(scrubber.tour == null);

            var reads = scrubber.reads.iterate();
            while (reads.next()) |read| scrubber.cancel_read(read);

            scrubber.tour = null;
        }

        pub fn checkpoint(scrubber: *GridScrubber) void {
            var reads = scrubber.reads.iterate();
            while (reads.next()) |read| {
                assert(!scrubber.superblock.free_set.is_free(read.id.block_address));

                if (scrubber.superblock.free_set.is_released(read.id.block_address)) {
                    scrubber.cancel_read(read);
                }
            }

            if (scrubber.tour != null and
                scrubber.tour.? == .table_content)
            {
                const index_address = scrubber.tour.?.table_content.index_address;
                assert(!scrubber.superblock.free_set.is_free(index_address));

                if (scrubber.superblock.free_set.is_released(index_address)) {
                    scrubber.tour_skip_content();
                }
            }
        }

        fn cancel_read(scrubber: *GridScrubber, read: *Read) void {
            const read_index = scrubber.reads.index(read);
            assert(!scrubber.reads.free.isSet(read_index));

            if (scrubber.reads_faulty.isSet(read_index)) {
                // This read already finished, so we can release it immediately.
                assert(!read.ignore);

                scrubber.reads_faulty.unset(read_index);
                scrubber.reads.release(read);
            } else {
                // Reads in progress will be cleaned up when they complete.
                maybe(read.ignore);

                read.ignore = true;
            }
        }

        pub fn read_next(scrubber: *GridScrubber) enum { read, busy, done } {
            assert(scrubber.tour != null);

            if (scrubber.reads.available() == 0) return .busy;

            const block_id = scrubber.tour_next() orelse {
                if (scrubber.reads.executing() > 0) return .busy;

                assert(scrubber.reads_faulty.count() == 0);
                assert(scrubber.tour.? == .done);

                log.debug("{}: read_next: completed cycle", .{ scrubber.options.replica_index });

                scrubber.tour = .init;
                return .done;
            };

            log.debug("{}: read_next: address={} checksum={} type={s}", .{
                scrubber.options.replica_index,
                block_id.block_address,
                block_id.block_checksum,
                @tagName(block_id.block_type),
            });

            const read = scrubber.reads.acquire().?;
            const read_index = scrubber.reads.index(read);
            assert(!scrubber.reads_faulty.isSet(read_index));

            read.* = .{
                .scrubber = scrubber,
                .id = block_id,
            };

            scrubber.forest.grid.read_block_repair_from_storage(
                read_next_callback,
                &read.read,
                //scrubber.read_blocks[read_index],
                read.id.block_address,
                read.id.block_checksum,
            );
            return .read;
        }

        fn read_next_callback(grid_read: *Grid.ReadRepair, result: Grid.ReadBlockResult) void {
            const read = @fieldParentPtr(Read, "read", grid_read);
            const read_index = read.scrubber.reads.index(read);
            const read_block = read.scrubber.read_blocks[read_index];
            const read_id = read.id;
            const scrubber = read.scrubber;
            assert(!scrubber.reads_faulty.isSet(read_index));
            assert(read_id.block_checksum == grid_read.checksum);
            assert(read_id.block_address == grid_read.address);

            log.debug("{}: read_next: result={s} (address={} checksum={} type={s} ignore={})", .{
                scrubber.options.replica_index,
                @tagName(result),
                read_id.block_address,
                read_id.block_checksum,
                @tagName(read_id.block_type),
                read.ignore,
            });

            if (read.ignore) {
                scrubber.reads.release(read);
            } else {
                if (result == .valid) {
                    const read_header = schema.header_from_block(read_block);
                    assert(read_header.checksum == read_id.block_checksum);
                    assert(read_header.op == read_id.block_address);
                    assert(BlockType.from(read_header.operation) == read_id.block_type);

                    scrubber.reads.release(read);
                } else {
                    scrubber.reads_faulty.set(read_index);
                }

                if (scrubber.tour.? == .table_content and
                    scrubber.tour.?.table_content.index_block == null and
                    scrubber.tour.?.table_content.index_checksum == read_id.block_checksum and
                    scrubber.tour.?.table_content.index_address == read_id.block_address)
                {
                    assert(scrubber.tour.?.table_content.content_index == 0);

                    if (result == .valid) {
                        stdx.copy_disjoint(.inexact, u8, scrubber.tour_index_block, read_block);
                        scrubber.tour.?.table_content.index_block = scrubber.tour_index_block;
                    } else {
                        scrubber.tour_skip_content();
                    }
                }
            }
        }

        pub fn next_fault(scrubber: *GridScrubber) ?BlockId {
            const read_index = scrubber.reads_faulty.findFirstSet() orelse return null;
            assert(!scrubber.reads.free.isSet(read_index));

            const read = &scrubber.reads.items[read_index];
            assert(!read.ignore);

            const block_id = read.id;
            scrubber.reads.release(read);
            scrubber.reads_faulty.unset(read_index);

            return block_id;
        }

        fn tour_next(scrubber: *GridScrubber) ?BlockId {
            assert(scrubber.tour != null);

            const tour = &scrubber.tour.?;
            if (tour.* == .init) {
                tour.* = .{ .table_index = .{ .tables = ForestTableIterator{} } };
            }

            if (tour.* == .table_content) {
                const index_block = tour.table_content.index_block orelse return null;
                const index_schema = schema.TableIndex.from(index_block);
                const content_index = tour.table_content.content_index;
                if (content_index < index_schema.content_blocks_used(scrubber.tour_index_block)) {
                    tour.table_content.content_index += 1;

                    const content_block =
                        index_schema.content_block(scrubber.tour_index_block, content_index);
                    return BlockId{
                        .block_checksum = content_block.block_checksum,
                        .block_address = content_block.block_address,
                        .block_type = content_block.block_type,
                    };
                } else {
                    const tables = tour.table_content.tables;
                    tour.* = .{ .table_index = .{ .tables = tables } };
                }
            }

            if (tour.* == .table_index) {
                if (tour.table_index.tables.next(scrubber.forest)) |table_info| {
                    if (Forest.Storage == TestStorage) {
                        TestStorage.verify.check_table(
                            scrubber.forest.grid.superblock.storage,
                            table_info.address,
                            table_info.checksum,
                        );
                    }

                    const tables = tour.table_index.tables;
                    tour.* = .{ .table_content = .{
                        .index_checksum = table_info.checksum,
                        .index_address = table_info.address,
                        .tables = tables,
                    } };

                    return BlockId{
                        .block_checksum = table_info.checksum,
                        .block_address = table_info.address,
                        .block_type = .index,
                    };
                } else {
                    tour.* = .{ .manifest_log = .{} };
                }
            }

            if (tour.* == .manifest_log) {
                if (tour.manifest_log.index < scrubber.superblock.manifest.count_checkpointed) {
                    const index = tour.manifest_log.index;
                    tour.manifest_log.index += 1;

                    return BlockId{
                        .block_checksum = scrubber.superblock.manifest.checksums[index],
                        .block_address = scrubber.superblock.manifest.addresses[index],
                        .block_type = .manifest,
                    };
                } else {
                    // The index may be beyond `manifest.count_checkpointed` due to a checkpoint
                    // which removed blocks.
                    tour.* = .done;
                }
            }

            assert(tour.* == .done);
            return null;
        }

        fn tour_skip_content(scrubber: *GridScrubber) void {
            assert(scrubber.tour != null);
            assert(scrubber.tour.? == .table_content);

            const tables = scrubber.tour.?.table_content.tables;
            scrubber.tour = .{ .table_index = .{ .tables = tables } };
        }
    };
}
