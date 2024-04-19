//! Scrub grid blocks.
//!
//! A "data scrubber" is a background task that gradually/incrementally reads the disk and validates
//! what it finds. Its purpose is to discover faults proactively – as early as possibly – rather
//! than waiting for them to be discovered by normal database operation (e.g. during compaction).
//!
//! The most common type of disk fault is a latent sector error:
//!
//! - A "latent sector error" is the temporary or permanent inability to access the data of a
//!   particular sector. That is, the disk as a whole continues to function, but a small section of
//!   data is unavailable.
//! - "Latent" refers to: the error is not discoverable until the sector is actually read.
//! - "An Analysis of Latent Sector Errors in Disk Drives" (2007) found that >60% of latent sector
//!   errors were discovered by a scrubber that cycles every 2 weeks.
//!
//! Finding and repairing errors proactively minimizes the risk of cluster data loss due to multiple
//! intersecting faults (analogous to a "double-fault") – a scenario where we fail to read a block,
//! and try to repair the block from another replica, only to discover that the copy of the block on
//! the remote replica's disk is *also* faulty.
//!
//! TODO Start replicas scrubbing from distinct/random offsets in the tour to farther minimize risk
//! of cluster data loss. (Right now replicas will often have identical scrubbing schedules.)
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.grid_scrubber);

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

const allocate_block = @import("./grid.zig").allocate_block;
const GridType = @import("./grid.zig").GridType;
const BlockPtr = @import("./grid.zig").BlockPtr;
const ForestTableIteratorType = @import("../lsm/forest_table_iterator.zig").ForestTableIteratorType;
const snapshot_from_op = @import("../lsm/manifest.zig").snapshot_from_op;
const TestStorage = @import("../testing/storage.zig").Storage;

pub fn GridScrubberType(comptime Forest: type) type {
    return struct {
        const GridScrubber = @This();
        const Grid = GridType(Forest.Storage);
        const ForestTableIterator = ForestTableIteratorType(Forest);
        const SuperBlock = vsr.SuperBlockType(Forest.Storage);
        const ManifestBlockIterator = ManifestBlockIteratorType(Forest.ManifestLog);
        const CheckpointTrailer = vsr.CheckpointTrailerType(Forest.Storage);

        pub const BlockId = struct {
            block_checksum: u128,
            block_address: u64,
            block_type: schema.BlockType,
        };

        const Read = struct {
            scrubber: *GridScrubber,
            read: Grid.Read = undefined,
            block_type: schema.BlockType,

            status: enum {
                /// If `read.done`: The scrub failed – the block must be repaired.
                /// If `!read.done`: The scrub is still in progress. (This is the initial state).
                repair,
                /// The scrub succeeded.
                /// Don't repair the block.
                ok,
                /// The scrub was aborted (the replica is about to state-sync).
                /// Don't repair the block.
                canceled,
                /// The block was freed by a checkpoint in the time that the read was in progress.
                /// Don't repair the block.
                ///
                /// (At checkpoint, the FreeSet frees blocks released during the preceding
                /// checkpoint. We can scrub released blocks, but not free blocks. Setting this flag
                /// ensures that GridScrubber doesn't require a read-barrier at checkpoint.)
                released,
            },
            /// Whether the read is ready to be released.
            done: bool,

            /// "next" belongs to the FIFOs.
            next: ?*Read = null,
        };

        superblock: *SuperBlock,
        forest: *Forest,
        client_sessions_checkpoint: *const CheckpointTrailer,

        reads: IOPS(Read, constants.grid_scrubber_reads_max) = .{},

        /// A list of reads that are in progress.
        reads_busy: FIFO(Read) = .{ .name = "grid_scrubber_reads_busy" },
        /// A list of reads that are ready to be released.
        reads_done: FIFO(Read) = .{ .name = "grid_scrubber_reads_done" },

        /// Track the progress through the grid.
        /// - On an idle replica (i.e. not committing), a full tour from "init" to "done" scrubs
        ///   every acquired block in the grid.
        /// - On a non-idle replica, a full tour from "init" to "done" scrubs all blocks that
        ///   survived the entire span of the tour, but may not scrub blocks that were added during
        ///   the tour.
        tour: union(enum) {
            init,
            done,
            table_index,
            table_data: struct {
                index_checksum: u128,
                index_address: u64,
                /// Points to `tour_index_block` once the index block has been read.
                index_block: ?BlockPtr = null,
                data_block_index: u32 = 0,
            },
            /// The manifest log tour iterates manifest blocks in reverse order.
            /// (To ensure that manifest compaction doesn't lead to missed blocks.)
            manifest_log: struct { iterator: ManifestBlockIterator = .init },
            free_set: struct { index: usize = 0 },
            client_sessions: struct { index: usize = 0 },
        },

        /// When tour == .init, tour_tables == .{}
        /// When tour == .done, tour_tables.next() == null.
        tour_tables: ForestTableIterator,

        /// Contains a table index block when tour=table_data.
        tour_index_block: BlockPtr,

        /// These counters reset after every tour cycle.
        tour_blocks_scrubbed_count: usize,

        pub fn init(
            allocator: std.mem.Allocator,
            forest: *Forest,
            client_sessions_checkpoint: *const CheckpointTrailer,
        ) error{OutOfMemory}!GridScrubber {
            var tour_index_block = try allocate_block(allocator);
            errdefer allocator.free(tour_index_block);

            return .{
                .superblock = forest.grid.superblock,
                .forest = forest,
                .client_sessions_checkpoint = client_sessions_checkpoint,
                .tour = .init,
                .tour_tables = .{},
                .tour_index_block = tour_index_block,
                .tour_blocks_scrubbed_count = 0,
            };
        }

        pub fn deinit(scrubber: *GridScrubber, allocator: std.mem.Allocator) void {
            allocator.free(scrubber.tour_index_block);

            scrubber.* = undefined;
        }

        pub fn cancel(scrubber: *GridScrubber) void {
            for ([_]FIFO(Read){ scrubber.reads_busy, scrubber.reads_done }) |reads_fifo| {
                var reads_iterator = reads_fifo.peek();
                while (reads_iterator) |read| : (reads_iterator = read.next) {
                    read.status = .canceled;
                }
            }

            scrubber.tour = .init;
            scrubber.tour_tables = .{};
            scrubber.tour_blocks_scrubbed_count = 0;
        }

        /// Cancel queued reads to blocks that will be released by the imminent checkpoint.
        /// (The read still runs, but the results will be ignored.)
        pub fn checkpoint(scrubber: *GridScrubber) void {
            assert(scrubber.superblock.opened);
            // GridScrubber.checkpoint() is called immediately before FreeSet.checkpoint().
            // All released blocks are about to be freed.
            assert(scrubber.forest.grid.callback == .none);

            for ([_]FIFO(Read){ scrubber.reads_busy, scrubber.reads_done }) |reads_fifo| {
                var reads_iterator = reads_fifo.peek();
                while (reads_iterator) |read| : (reads_iterator = read.next) {
                    if (read.status == .repair) {
                        assert(!scrubber.forest.grid.free_set.is_free(read.read.address));

                        if (scrubber.forest.grid.free_set.is_released(read.read.address)) {
                            read.status = .released;
                        }
                    }
                }
            }

            if (scrubber.tour == .table_data) {
                const index_address = scrubber.tour.table_data.index_address;
                assert(!scrubber.forest.grid.free_set.is_free(index_address));

                if (scrubber.forest.grid.free_set.is_released(index_address)) {
                    // Skip scrubbing the table data, since the table is about to be released.
                    scrubber.tour = .table_index;
                }
            }
        }

        /// Returns whether or not a new Read was started.
        pub fn read_next(scrubber: *GridScrubber) bool {
            assert(scrubber.superblock.opened);
            assert(scrubber.forest.grid.callback != .cancel);
            assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());
            defer assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());

            if (scrubber.reads.available() == 0) return false;
            const block_id = scrubber.tour_next() orelse return false;
            scrubber.tour_blocks_scrubbed_count += 1;

            const read = scrubber.reads.acquire().?;
            assert(!scrubber.reads_busy.contains(read));
            assert(!scrubber.reads_done.contains(read));

            log.debug("{}: read_next: address={} checksum={x:0>32} type={s}", .{
                scrubber.superblock.replica_index.?,
                block_id.block_address,
                block_id.block_checksum,
                @tagName(block_id.block_type),
            });

            read.* = .{
                .scrubber = scrubber,
                .block_type = block_id.block_type,
                .status = .repair,
                .done = false,
            };
            scrubber.reads_busy.push(read);

            scrubber.forest.grid.read_block(
                .{ .from_local_storage = read_next_callback },
                &read.read,
                block_id.block_address,
                block_id.block_checksum,
                .{ .cache_read = false, .cache_write = false },
            );
            return true;
        }

        fn read_next_callback(grid_read: *Grid.Read, result: Grid.ReadBlockResult) void {
            const read = @fieldParentPtr(Read, "read", grid_read);
            const scrubber = read.scrubber;
            assert(scrubber.reads_busy.contains(read));
            assert(!scrubber.reads_done.contains(read));
            assert(!read.done);
            maybe(read.status != .repair);

            log.debug("{}: read_next_callback: result={s} " ++
                "(address={} checksum={x:0>32} type={s} status={?})", .{
                scrubber.superblock.replica_index.?,
                @tagName(result),
                read.read.address,
                read.read.checksum,
                @tagName(read.block_type),
                read.status,
            });

            if (read.status == .repair and
                scrubber.tour == .table_data and
                scrubber.tour.table_data.index_block == null and
                scrubber.tour.table_data.index_checksum == read.read.checksum and
                scrubber.tour.table_data.index_address == read.read.address)
            {
                assert(scrubber.tour.table_data.data_block_index == 0);

                if (result == .valid) {
                    stdx.copy_disjoint(.inexact, u8, scrubber.tour_index_block, result.valid);
                    scrubber.tour.table_data.index_block = scrubber.tour_index_block;
                } else {
                    // The scrubber can't scrub the table data blocks until it has the corresponding
                    // index block. We will wait for the index block, and keep re-scrubbing it until
                    // it is repaired (or until the block is released by a checkpoint).
                    //
                    // (Alternatively, we could just skip past the table data blocks, and we will
                    // come across them again during the next cycle. But waiting for them makes for
                    // nicer invariants + tests.)
                    log.debug("{}: read_next_callback: waiting for index repair " ++
                        "(address={} checksum={x:0>32})", .{
                        scrubber.superblock.replica_index.?,
                        read.read.address,
                        read.read.checksum,
                    });
                }
            }

            if (result == .valid) {
                if (read.status == .repair) {
                    read.status = .ok;
                }
            }

            read.done = true;
            scrubber.reads_busy.remove(read);
            scrubber.reads_done.push(read);
        }

        pub fn read_fault(scrubber: *GridScrubber) ?BlockId {
            assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());
            defer assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());

            while (scrubber.reads_done.pop()) |read| {
                defer scrubber.reads.release(read);
                assert(read.done);

                if (read.status == .repair) {
                    return .{
                        .block_address = read.read.address,
                        .block_checksum = read.read.checksum,
                        .block_type = read.block_type,
                    };
                }
            }
            return null;
        }

        fn tour_next(scrubber: *GridScrubber) ?BlockId {
            const tour = &scrubber.tour;
            if (tour.* == .init) {
                tour.* = .table_index;
            }

            if (tour.* == .table_data) {
                const index_block = tour.table_data.index_block orelse {
                    // The table index is `null` if:
                    // - It was corrupt when we just scrubbed it.
                    // - Or `grid_scrubber_reads > 1`.
                    // Keep trying until either we find it, or a checkpoint removes it.
                    // (See read_next_callback() for more detail.)
                    return .{
                        .block_checksum = tour.table_data.index_checksum,
                        .block_address = tour.table_data.index_address,
                        .block_type = .index,
                    };
                };

                const index_schema = schema.TableIndex.from(index_block);
                const data_block_index = tour.table_data.data_block_index;
                if (data_block_index <
                    index_schema.data_blocks_used(scrubber.tour_index_block))
                {
                    tour.table_data.data_block_index += 1;

                    const data_block_addresses =
                        index_schema.data_addresses_used(scrubber.tour_index_block);
                    const data_block_checksums =
                        index_schema.data_checksums_used(scrubber.tour_index_block);
                    return .{
                        .block_checksum = data_block_checksums[data_block_index].value,
                        .block_address = data_block_addresses[data_block_index],
                        .block_type = .data,
                    };
                } else {
                    assert(data_block_index ==
                        index_schema.data_blocks_used(scrubber.tour_index_block));
                    tour.* = .table_index;
                }
            }

            if (tour.* == .table_index) {
                if (scrubber.tour_tables.next(scrubber.forest)) |table_info| {
                    if (Forest.Storage == TestStorage) {
                        scrubber.superblock.storage.verify_table(
                            table_info.address,
                            table_info.checksum,
                        );
                    }

                    tour.* = .{ .table_data = .{
                        .index_checksum = table_info.checksum,
                        .index_address = table_info.address,
                    } };

                    return .{
                        .block_checksum = table_info.checksum,
                        .block_address = table_info.address,
                        .block_type = .index,
                    };
                } else {
                    tour.* = .{ .manifest_log = .{} };
                }
            }

            if (tour.* == .manifest_log) {
                if (tour.manifest_log.iterator.next(
                    &scrubber.forest.manifest_log,
                )) |block_reference| {
                    return .{
                        .block_checksum = block_reference.checksum,
                        .block_address = block_reference.address,
                        .block_type = .manifest,
                    };
                } else {
                    tour.* = .{ .free_set = .{} };
                }
            }

            if (tour.* == .free_set) {
                const free_set_trailer = &scrubber.forest.grid.free_set_checkpoint;
                if (free_set_trailer.callback != .none) return null;
                if (tour.free_set.index < free_set_trailer.block_count()) {
                    const index = tour.free_set.index;
                    tour.free_set.index += 1;
                    return .{
                        .block_checksum = free_set_trailer.block_checksums[index],
                        .block_address = free_set_trailer.block_addresses[index],
                        .block_type = .free_set,
                    };
                } else {
                    // A checkpoint can reduce the number of trailer blocks while we are scrubbing
                    // the trailer.
                    maybe(tour.free_set.index > free_set_trailer.block_count());
                    tour.* = .{ .client_sessions = .{} };
                }
            }

            if (tour.* == .client_sessions) {
                const client_sessions = scrubber.client_sessions_checkpoint;
                if (client_sessions.callback != .none) return null;
                if (tour.client_sessions.index < client_sessions.block_count()) {
                    const index = tour.client_sessions.index;
                    tour.client_sessions.index += 1;
                    return .{
                        .block_checksum = client_sessions.block_checksums[index],
                        .block_address = client_sessions.block_addresses[index],
                        .block_type = .client_sessions,
                    };
                } else {
                    // A checkpoint can reduce the number of trailer blocks while we are scrubbing
                    // the trailer.
                    maybe(tour.client_sessions.index > client_sessions.block_count());
                    tour.* = .done;
                }
            }

            // Note that this is just the end of the tour.
            // (Some of the cycle's reads may still be in progress).
            log.info("{}: tour_next: cycle done (toured_blocks={})", .{
                scrubber.superblock.replica_index.?,
                scrubber.tour_blocks_scrubbed_count,
            });

            // Wrap around to the next cycle.
            assert(tour.* == .done);
            tour.* = .init;
            scrubber.tour_tables = .{};
            scrubber.tour_blocks_scrubbed_count = 0;

            return null;
        }
    };
}

/// Iterate over every manifest block address/checksum in the manifest log.
///
/// This iterator is stable across ManifestLog mutation – that is, it is guaranteed to iterate over
/// every manifest block that survives the entire iteration.
fn ManifestBlockIteratorType(comptime ManifestLog: type) type {
    return union(enum) {
        const ManifestBlockIterator = @This();

        init,
        done,
        state: struct {
            /// The last-known index (within the manifest blocks) of the address/checksum.
            index: usize,
            /// The address/checksum of the most-recently iterated manifest block.
            address: u64,
            checksum: u128,
        },

        fn next(
            iterator: *ManifestBlockIterator,
            manifest_log: *const ManifestLog,
        ) ?vsr.BlockReference {
            // Don't scrub the trailing `blocks_closed`; they are not yet flushed to disk.
            const log_block_count =
                manifest_log.log_block_addresses.count - manifest_log.blocks_closed;

            const position: ?usize = switch (iterator.*) {
                .done => null,
                .init => if (log_block_count == 0) null else log_block_count - 1,
                .state => |state| position: {
                    // `index` may be beyond the limit due to blocks removed by manifest compaction.
                    maybe(state.index >= log_block_count);

                    // The block that we most recently scrubbed may:
                    // - be in the same position, or
                    // - have shifted earlier in the list (due to manifest compaction), or
                    // - have been removed from the list (due to manifest compaction).
                    // Use the block's old position to find its current position.
                    var position: usize = @min(state.index, log_block_count -| 1);
                    while (position > 0) : (position -= 1) {
                        if (manifest_log.log_block_addresses.get(position).? == state.address and
                            manifest_log.log_block_checksums.get(position).? == state.checksum)
                        {
                            break :position if (position == 0) null else position - 1;
                        }
                    } else {
                        break :position null;
                    }
                },
            };

            if (position) |index| {
                iterator.* = .{ .state = .{
                    .index = index,
                    .address = manifest_log.log_block_addresses.get(index).?,
                    .checksum = manifest_log.log_block_checksums.get(index).?,
                } };

                return .{
                    .address = iterator.state.address,
                    .checksum = iterator.state.checksum,
                };
            } else {
                iterator.* = .done;
                return null;
            }
        }
    };
}
