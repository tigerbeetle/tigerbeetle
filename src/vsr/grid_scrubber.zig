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
            block_type: BlockType,

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
        //read_blocks: [constants.grid_scrubber_reads_max]Grid.BlockPtr,
        /// When `reads_faulty` is set, the corresponding Read in `reads`:
        /// - is not free,
        /// - is complete, and
        /// - must be repaired.
        reads_faulty: ReadSet = ReadSet.initEmpty(),

        /// When null: Scrubbing is disabled.
        /// When non-null: Scrub the entire forest slowly in the background.
        /// TODO Start each replica scrubbing from a (different) random offset.
        tour: union(enum) {
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
            var tour_index_block = try allocate_block(allocator);
            errdefer allocator.free(tour_index_block);

            return GridScrubber{
                .superblock = forest.grid.superblock,
                .forest = forest,
                .options = options,
                .tour = .init,
                .tour_index_block = tour_index_block,
            };
        }

        pub fn deinit(scrubber: *GridScrubber, allocator: std.mem.Allocator) void {
            allocator.free(scrubber.tour_index_block);

            scrubber.* = undefined;
        }

        pub fn cancel(scrubber: *GridScrubber) void {
            var reads = scrubber.reads.iterate();
            while (reads.next()) |read| scrubber.reads.release(read);

            var reads_faulty = scrubber.reads_faulty.iterator(.{});
            while (reads_faulty.next()) |read_faulty| scrubber.reads_faulty.unset(read_faulty);

            scrubber.tour = .init;
        }

        pub fn checkpoint(scrubber: *GridScrubber) void {
            assert(scrubber.forest.grid.superblock.opened);

            var reads = scrubber.reads.iterate();
            while (reads.next()) |read| {
                assert(!scrubber.superblock.free_set.is_free(read.read.address));

                if (scrubber.superblock.free_set.is_released(read.read.address)) {
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
            }

            if (scrubber.tour == .table_content) {
                const index_address = scrubber.tour.table_content.index_address;
                assert(!scrubber.superblock.free_set.is_free(index_address));

                if (scrubber.superblock.free_set.is_released(index_address)) {
                    scrubber.tour_skip_content();
                }
            }
        }

        pub fn read_next(scrubber: *GridScrubber) enum { read, busy, done } {
            assert(scrubber.forest.grid.superblock.opened);
            assert(scrubber.forest.grid.canceling == null);

            if (scrubber.reads.available() == 0) return .busy;

            const block_id = scrubber.tour_next() orelse {
                if (scrubber.reads.executing() > 0) return .busy;

                assert(scrubber.reads_faulty.count() == 0);
                assert(scrubber.tour == .done);

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
                .block_type = block_id.block_type,
            };

            scrubber.forest.grid.read_block_async(
                .{ .local = read_next_callback },
                &read.read,
                block_id.block_address,
                block_id.block_checksum,
                .{
                    // This read is coherent as-of this moment.
                    // However, this read may span across a checkpoint where the block is freed.
                    .coherent = false,
                    .cache_check = false,
                    .cache_update = false,
                },
            );
            return .read;
        }

        fn read_next_callback(grid_read: *Grid.Read, result: Grid.ReadBlockResult) void {
            const read = @fieldParentPtr(Read, "read", grid_read);
            const read_index = read.scrubber.reads.index(read);
            const scrubber = read.scrubber;
            assert(!scrubber.reads_faulty.isSet(read_index));
            assert(read.read.checksum == grid_read.checksum);
            assert(read.read.address == grid_read.address);

            log.debug("{}: read_next_callback: result={s} " ++
                "(address={} checksum={} type={s} ignore={})", .{
                scrubber.options.replica_index,
                @tagName(result),
                read.read.address,
                read.read.checksum,
                read.block_type,
                read.ignore,
            });

            if (read.ignore) {
                scrubber.reads.release(read);
            } else {
                if (scrubber.tour == .table_content and
                    scrubber.tour.table_content.index_block == null and
                    scrubber.tour.table_content.index_checksum == read.read.checksum and
                    scrubber.tour.table_content.index_address == read.read.address)
                {
                    assert(scrubber.tour.table_content.content_index == 0);

                    if (result == .valid) {
                        stdx.copy_disjoint(.inexact, u8, scrubber.tour_index_block, result.valid);
                        scrubber.tour.table_content.index_block = scrubber.tour_index_block;
                    } else {
                        scrubber.tour_skip_content();
                    }
                }

                if (result == .valid) {
                    const read_header = schema.header_from_block(result.valid);
                    assert(read_header.checksum == read.read.checksum);
                    assert(read_header.op == read.read.address);

                    scrubber.reads.release(read);
                } else {
                    scrubber.reads_faulty.set(read_index);
                }
            }
        }

        pub fn next_fault(scrubber: *GridScrubber) ?BlockId {
            const read_index = scrubber.reads_faulty.findFirstSet() orelse return null;
            assert(!scrubber.reads.free.isSet(read_index));

            const read = &scrubber.reads.items[read_index];
            assert(!read.ignore);

            const read_id = BlockId{
                .block_address = read.read.address,
                .block_checksum = read.read.checksum,
                .block_type = read.block_type,
            };

            scrubber.reads.release(read);
            scrubber.reads_faulty.unset(read_index);

            return read_id;
        }

        fn tour_next(scrubber: *GridScrubber) ?BlockId {
            const tour = &scrubber.tour;
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
            assert(scrubber.tour == .table_content);

            const tables = scrubber.tour.table_content.tables;
            scrubber.tour = .{ .table_index = .{ .tables = tables } };
        }
    };
}
