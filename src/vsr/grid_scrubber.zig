//! The scrubber must not be enabled during the time between:
//! - request trailers, and
//! - `SuperBlock.sync`'s completion
//! otherwise TODO why not?
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.grid_scrubber);

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const IOPS = @import("../iops.zig").IOPS;

const allocate_block = @import("../lsm/grid.zig").allocate_block;
const BlockType = @import("../lsm/grid.zig").BlockType;
const GridType = @import("../lsm/grid.zig").GridType;
const ForestTableIteratorType = @import("../lsm/forest_table_iterator.zig").ForestTableIteratorType;

pub const Mode = union(enum) {
    /// Scrubber is entirely disabled.
    stop,

    /// Scrubber runs slowly in the background.
    slow: struct {
        // TODO random offset
        //origin: ,
    },

    /// Scrubber quickly checks missing tables.
    sync: struct {
        snapshot_min: u64,
        snapshot_max: u64,
    },

    /// Invariants:
    /// - When `table_content` is set, `table_index` must be set.
    const checks = std.enums.EnumArray(
        std.meta.Tag(Mode),
        std.enums.EnumFieldStruct(enum {
            manifest_log,
            table_index,
            table_content,
        }),
    ).init(.{
        .stop = .{ .manifest_log = false, .table_index = false, .table_content = false },
        .slow = .{ .manifest_log = true, .table_index = true, .table_content = true },
        .sync = .{ .manifest_log = false, .table_index = true, .table_content = false },
    });
};

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
            read: Grid.ReadRepair = undefined,

            /// These fields are also on the Grid.ReadRepair, but we will need them even after the
            /// Grid's read is released. (See `reads_faulty`).
            id: BlockId,

            /// When true: If the read completes and is corrupt, the block will *not* be added to
            /// the repair queue. This avoids a read-barrier at checkpoints (due to released
            /// blocks).
            ignore: bool = false,
        };

        pub const Options = struct {
            replica_index: u8,
        };

        pub const Config = struct {
            check_manifest_log: bool,
            check_table_index: bool,
            check_table_content: bool,

            /// Only scrub tables created within this (inclusive) range.
            snapshot_min: u64 = 0,
            snapshot_max: u64 = std.math.max(u64),

            origin: u8,
            reads_max: usize,
        };

        superblock: *SuperBlock,
        forest: *Forest,
        options: Options,

        mode: Mode,
        config: Config,

        reads: IOPS(Read, constants.grid_scrubber_reads_max) = .{},
        read_blocks: [constants.grid_scrubber_reads_max]Grid.BlockPtr,
        /// When `reads_faulty` is set, the corresponding Read in `reads`:
        /// - is not free,
        /// - is complete, and
        /// - must be repaired.
        reads_faulty: ReadSet = ReadSet.initEmpty(),

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
                checksum: u128,
                address: u64,
                index: usize = 0,
                /// Points to `tour_index_block` once the index block has been read.
                block: ?Grid.BlockPtr = null,
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
            for (read_buffer) |*buffer, i| {
                errdefer for (read_buffer[0..i]) |b| allocator.free(b);
                buffer.* = try allocate_block(allocator);
            }
            errdefer for (read_blocks) |buffer| allocator.free(buffer);

            return .{
                .superblock = forest.grid.superblock,
                .forest = forest,
                .options = options,
                .read_blocks = read_blocks,
                //.config = .{
                //    .check_manifest_log = false,
                //    .check_table_index = false,
                //    .check_table_content = false,
                //    .origin = 0,
                //    .reads_max = 0,
                //},
                .tour = .done,
                .tour_index_block = tour_index_block,
            };
        }

        pub fn deinit(scrubber: *GridScrubber, allocator: std.mem.Allocator) void {
            for (scrubber.read_blocks) |buffer| allocator.free(buffer);

            scrubber.* = undefined;
        }

        pub fn configure(scrubber: *GridScrubber, config_new: *const Config) void {
            assert(config_new.reads_max <= constants.grid_scrubber_reads_max);
            if (config.scrub_table_content) assert(config.scrub_table_index);

            scrubber.config = config_new.*;
            scrubber.tour = .init;
        }

        pub fn checkpoint(scrubber: *Scrubber) void {
            var reads = scrubber.reads.iterate();
            while (reads.next()) |read| {
                if (scrubber.superblock.free_set.is_released(read.address)) {
                    scrubber.cancel_read(read);
                }
            }
        }

        pub fn cancel(scrubber: *GridScrubber) void {
            var reads = scrubber.reads.iterate();
            while (reads.next()) |read| scrubber.cancel_read(read);
        }

        fn cancel_read(scrubber: *Scrubber, read: *Read) void {
            const read_index = scrubber.reads.index(read);
            assert(!scrubber.reads.free.isSet(read_index));

            if (scrubber.reads_faulty.isSet(read_index)) {
                // The read already finished, so we can release it immediately.
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
            if (scrubber.reads.executing() >= scrubber.config.reads_max) {
                return .busy;
            }

            const block_id = scrubber.tour_next() orelse {
                if (scrubber.reads.executing() == 0) {
                    assert(scrubber.tour == .done);
                    return .done;
                } else {
                    return .busy;
                }
            };

            const read = scrubber.reads.acquire().?;
            const read_index = scrubber.reads.index(read);
            assert(!scrubber.reads_faulty.isSet(read_index));

            read.* = .{
                .scrubber = scrubber,
                .id = block_id,
            };

            scrubber.forest.grid.read_block_from_storage(
                read_next_callback,
                read,
                read.id.block_address,
                read.id.block_checksum,
                read.id.block_type,
            );
            return .read;
        }

        fn read_next_callback(grid_read: *Grid.ReadRepair, result: Grid.ReadBlockResult) void {
            const read = @fieldParentPtr(Read, "read", storage_read);
            const read_index = read.scrubber.reads.index(read);
            const read_block = read.read_blocks[read_index];
            const scrubber = read.scrubber;
            assert(!scrubber.reads_faulty.isSet(read_index));
            assert(read.id.block_checksum == grid_read.checksum);
            assert(read.id.block_address == grid_read.address);

            log.debug("{}: read_next: result={s} " ++
                "(address={} checksum={} type={s} ignore={})", .{
                scrubber.options.replica_index,
                @tagName(result),
                read.id.block_address,
                read.id.block_checksum,
                @tagName(read.id.block_type),
                read.ignore,
            });

            if (read.ignore) {
                scrubber.reads.release(read);
            } else {
                if (result == .valid) {
                    const read_header = schema.header_from_block(read_block);
                    assert(read_header.checksum == read.id.block_checksum);
                    assert(read_header.op == read.id.block_address);
                    assert(BlockType.from(read_header.operation) == read.id.block_type);

                    scrubber.reads.release(read);
                } else {
                    scrubber.reads_faulty.set(read_index);
                }

                if (scrubber.tour == .table_content and
                    scrubber.tour.table_content.table == null and
                    scrubber.tour.table_content.checksum == read.id.block_checksum and
                    scrubber.tour.table_content.address == read.id.block_address)
                {
                    assert(scrubber.tour.table_content.index == 0);

                    if (result == .valid) {
                        stdx.copy_disjoint(.inexact, u8, scrubber.tour_index_block, read_block);
                        scrubber.tour.table_content.block = scrubber.tour_index_block;
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
            if (scrubber.tour == .init) {
                scrubber.tour = .{ .table_index = .{ .tables = ForestTableIterator{} } };
            }

            if (scrubber.tour == .table_content) {
                assert(scrubber.config.table_content);

                const index_block = scrubber.tour.table_content.block orelse return null;
                const index_schema = schema.TableIndex.from(index_block);
                const content_index = scrubber.tour.table_content.index;
                if (content_index < index_schema.content_blocks_used(scrubber.tour_index_block)) {
                    scrubber.tour.table_content.index += 1;

                    const content_block = index_schema.content_block(content_index);
                    return .{
                        .block_checksum = content_block.block_checksum,
                        .block_address = content_block.block_address,
                        .block_type = content_block.block_type,
                    };
                } else {
                    scrubber.tour =
                        .{ .table_index = .{ .tables = scrubber.tour.table_content.tables } };
                }
            }

            if (scrubber.tour == .table_index) {
                if (scrubber.config.table_index and
                    scrubber.tour.table_index.tables.next(scrubber.forest)) |table_info|
                {
                    if (scrubber.config.table_content) {
                        scrubber.tour =
                            .{ .table_content = .{ .tables = scrubber.tour.table_index.tables } };
                    }

                    return .{
                        .block_checksum = table_info.checksum,
                        .block_address = table_info.address,
                        .block_type = .index,
                    };
                } else {
                    scrubber.tour = .manifest_log;
                }
            }

            if (scrubber.tour == .manifest_log) {
                if (scrubber.config.manifest_log and
                    scrubber.tour.manifest_log.index < scrubber.superblock.manifest.count)
                {
                    defer scrubber.tour.manifest_log.index += 1;

                    return .{
                        .block_checksum = manifest.checksums[scrubber.tour.manifest_log.index],
                        .block_address = manifest.addresses[scrubber.tour.manifest_log.index],
                        .block_type = .manifest,
                    };
                } else {
                    // The index may be beyond `manifest.count` due to a checkpoint which removed
                    // blocks.
                    scrubber.tour = .done;
                }
            }

            assert(scrubber.tour == .done);
            return null;
        }

        fn tour_skip_content(scrubber: *GridScrubber) void {
            assert(scrubber.tour == .table_content);
            assert(scrubber.tour.table_content.table == .null);
            assert(scrubber.tour.table_content.index == 0);

            scrubber.tour = .{ .table_index = .{ .tables = scrubber.tour.table_content.tables } };
        }
    };
}


//fn forest_table(
//    comptime Forest: type,
//    forest: *const Forest,
//    parameters: struct {
//        tree_index: usize,
//        level: u8,
//        table_address_previous: ?u64,
//    },
//) ?BlockId {
//    assert(parameters.tree_index < forest_tree_count(Forest));
//    assert(parameters.level < constants.lsm_levels);
//
//    inline for (std.meta.fields(Forest.Grooves)) |groove_field, i| {
//        const Groove = groove_field.field_type;
//        if (i == parameters.tree_index) {
//            return tree_table(Groove.ObjectTree, .{
//                .key = 
//                .level = parameters.level,
//            });
//        }
//    }
//}

//fn forest_tree_count(comptime Forest: type) usize {
//    var count: usize = 0;
//    inline for (std.meta.fields(Forest.Grooves)) |groove_field| {
//        const Groove = groove_field.field_type;
//        count += 1; // Object tree.
//        count += @boolToInt(Groove.IdTree != void);
//        count += std.meta.fields(Groove.IndexTrees).len;
//    }
//    return count;
//}

//const configs = .{
//    .none = .{
//        //.wal_prepares = false,
//        .manifest_log = false,
//        .table_index = false,
//        .table_content = false,
//        //.client_replies = false,
//        .reads = 0,
//        .done = .none,
//        // TODO read_sleep
//    },
//    .full = .{
//        //.wal_prepares = true,
//        .manifest_log = true,
//        .table_index = true,
//        .table_content = true,
//        //.client_replies = true,
//        .reads = constants.scrub_reads_max,
//        .done = .full,
//    },
//    .sync = .{
//        //.wal_prepares = false,
//        .manifest_log = false,
//        .table_index = true,
//        .table_content = false,
//        //.client_replies = true,
//        .reads = compaction.scrub_reads_max, // + TODO,
//        .done = .full,
//    },
//};

        //blocks_max: {
        //const Grooves = StateMachine.Forest.Grooves;
        //
        //var blocks_max: usize = 0;
        //inline for (std.meta.fields(Grooves)) |groove_field| {
        //    const Groove = groove_field.field_type;
        //
        //    blocks_max = @maximum(blocks_max, tree_blocks_max(Groove.ObjectTree));
        //    blocks_max = @maximum(blocks_max, tree_blocks_max(Groove.IdTree));
        //    inline for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
        //        blocks_max = @maximum(blocks_max, tree_blocks_max(tree_field.field_type));
        //    }
        //}
        //assert(blocks_max >= 2);

        ///// Wait for all pending writes the finish.
        ///// Ignore any new writes until invoking the callback.
        //fn flush(scrubber: *Scrubber, callback: fn(*Scrubber) void) void {
        //    assert(scrubber.flush_callback == null);
        //    scrubber.flush_callback = .{ .callback = callback };
        //
        //    if (scrubber.writes.executing() == 0) {
        //        scrubber.grid.on_next_tick(flush_next_tick_callback, &scrubber.flush_next_tick);
        //    }
        //}
        //
        //fn flush_next_tick_callback(next_tick: *Grid.NextTick) void {
        //    const scrubber = @fieldParentPtr(Scrubber, "flush_next_tick", next_tick);
        //    assert(scrubber.flush_callback != null);
        //    assert(scrubber.writes.executing() == 0);
        //
        //    scrubber.flush_done();
        //}
        //
        //fn flush_join(scrubber: *Scrubber) void {
        //    assert(scrubber.flush_callback != null);
        //
        //    if (scrubber.writes.executing() == 0) scrubber.flush_done();
        //}
        //
        //fn flush_done(scrubber: *Scrubber) void {
        //    assert(scrubber.flush_callback != null);
        //    assert(scrubber.writes.executing() == 0);
        //
        //    const callback = scrubber.flush_callback.?;
        //    scrubber.flush_callback = null;
        //
        //    callback(scrubber);
        //}

        //pub fn requests(
        //    scrubber: *const Scrubber,
        //    /// The number of replicas to split the fault list between.
        //    /// (Does not include our own replica, so this should always be less than
        //    /// Replica.replica_count.)
        //    group_count: usize,
        //    group: usize,
        //    requests: []vsr.BlockRequest,
        //) usize {
        //    assert(group_count <= constants.replicas_max);
        //    assert(group_count > 0);
        //    assert(group_count > group);
        //    assert(requests.len > 0);
        //    maybe(scrubber.flush_callback != null);
        //
        //    const group_faults = stdx.div_ceil(scrubber.entries.count(), group_count);
        //    const group_offset = groups_faults * group;
        //    const group_len = @minimum(
        //        @minimum(group_faults, requests.len),
        //        scrubber.entries.count() - group_offset,
        //    );
        //
        //    const fault_addresses = scrubber.entries.items(.key);
        //    const fault_checksums = scrubber.entries.items(.value);
        //    // TODO faulty_tables
        //
        //    for (requests[0..group_len]) |*request, i| {
        //        request.* = .{
        //            .block_address = fault_addresses[group_offset + i],
        //            .block_checksum = fault_checksums[group_offset + i],
        //        };
        //    }
        //    return group_len;
        //}

        //fn write_block(
        //    scrubber: *Scrubber,
        //    block_data: []const u8,
        //) error{NotFaulty, NoWrite}!void {
        //    const block_header = std.mem.bytesAsValue(vsr.Header, block_data[0..@sizeOf(vsr.Header)]);
        //    assert(block_header.command == .block);
        //    assert(block_header.operation.valid(BlockType));
        //
        //    const block_address = block_header.op;
        //    assert(block_address > 0);
        //
        //    const faulty_checksum = scrubber.faulty_read_ids.get(block_address) orelse {
        //        return error.NotFaulty;
        //    };
        //
        //    if (faulty_checksum != block_header.checksum) {
        //        return error.NotFaulty;
        //    }
        //
        //    if (block_header.operation.cast(BlockType) == .index) {
        //        // Missing/corrupt table index implies that all of the table's contents are missing.
        //        const faulty_table = scrubber.faulty_tables.acquire() orelse return error.NoWrite;
        //        const faulty_table_index = scrubber.faulty_tables.index(faulty_table);
        //        const block = scrubber.faulty_table_blocks[faulty_table_index];
        //
        //        stdx.copy_disjoint(.inexact, u8, block, block_data[0..block_header.size]);
        //        faulty_table.* = .{
        //            .filter_blocks_total = AnyTable.index_filter_blocks_used(block),
        //            .data_blocks_total = AnyTable.index_data_blocks_used(block),
        //        };
        //
        //        assert(faulty_table.filter_blocks_total > 0);
        //        assert(faulty_table.data_blocks_total > 0);
        //        return;
        //    }
        //
        //    const write = scrubber.writes.acquire() orelse return error.NoWrite;
        //    const write_index = scrubber.writes.index(write);
        //    const block = scrubber.write_blocks[write_index];
        //
        //    stdx.copy_disjoint(.inexact, u8, block, block_data[0..block_header.size]);
        //
        //    const removed = scrubber.faulty_read_ids.remove(block_address);
        //    assert(removed);
        //
        //    write.* = .{ .scrubber = scrubber };
        //
        //    // TODO don't update cache (grid could base it off of presence/absence in read_faulty_queue)
        //    // TODO this block isn't in the grid's read_faulty_queue
        //    scrubber.grid.write_block_repair(
        //        write_block_callback,
        //        &write.grid_write,
        //        block,
        //        block_address,
        //    );
        //}
        //
        //fn write_block_callback(grid_write: *Grid.Write) void {
        //    const write = @fieldParentPtr(Write, "grid_write", grid_write);
        //    const scrubber = write.scrubber;
        //    const write_index = scrubber.writes.index(write);
        //    const block = scrubber.write_block[write_index];
        //    const block_header = std.mem.bytesAsValue(vsr.Header, block_data[0..@sizeOf(vsr.Header)]);
        //    assert(block_header.command == .block);
        //
        //    scrubber.writes.release(grid_write);
        //
        //    if (scrubber.flush_callback) |_| {
        //        scrubber.flush_join();
        //    }
        //}

        //pub fn on_read_sleep_timeout(scrubber: *GridScrubber) void {
        //    assert(scrubber.checkpoint_callback == null);
        //    assert(scrubber.read_sleep_timeout.ticking);
        //    scrubber.read_sleep_timeout.reset();
        //
        //    if (scrubber.reads.available() == 0) return;
        //
        //    if (scrubber.faulty_read_ids.count() ==
        //        scrubber.faulty_read_ids.capacity())
        //    {
        //        return;
        //    }
        //
        //    scrubber.read_next();
        //}
        //
        //pub fn start(scrubber: *GridScrubber, options: Options) void {
        //    // TODO
        //    scrubber.read_sleep_timeout.after = options.sleep_ticks;
        //    scrubber.read_sleep_timeout.reset();
        //}
