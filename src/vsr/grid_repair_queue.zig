const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.grid_repair);
const maybe = stdx.maybe;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const vsr = @import("../vsr.zig");

const allocate_block = @import("../lsm/grid.zig").allocate_block;
const BlockType = @import("../lsm/grid.zig").BlockType;
const GridType = @import("../lsm/grid.zig").GridType;
const IOPS = @import("../iops.zig").IOPS;

/// Immediately after state sync we want access to all of the grid's write bandwidth to rapidly sync
/// table blocks.
const grid_repair_writes_max = constants.grid_iops_write_max;

comptime {
    assert(grid_repair_writes_max <= constants.grid_repair_blocks_max);
}

/// The maximum number of blocks that can possibly be referenced by any table index block.
///
/// - This is a very conservative (upper-bound) calculation that doesn't rely on the StateMachine's
///   tree configuration.
/// - This counts filter and data blocks, but does not count the index block itself.
const lsm_table_blocks_max = table_blocks_max: {
    const checksum_size = @sizeOf(u128);
    const address_size = @sizeOf(u64);
    break :table_blocks_max @divFloor(
        constants.block_size - @sizeOf(vsr.Header),
        (checksum_size + address_size),
    );
};

/// Track and repair faulty grid blocks.
///
/// The GridRepairQueue is LSM-aware.
/// It maintains the LSM invariant: if a table index block is in the grid, then all of the index
/// block's referenced filter/data blocks are in the grid.
pub fn GridRepairQueueType(comptime Storage: type) type {
    return struct {
        const GridRepairQueue = @This();
        const Grid = GridType(Storage);
        const WriteSet = std.StaticBitSet(grid_repair_writes_max);

        const Write = struct {
            queue: *GridRepairQueue,
            write: Grid.Write = undefined,
            checksum: u128,
        };

        const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, struct {
            checksum: u128,
            progress: FaultProgress,
            received: bool = false,
        });

        const FaultProgress = union(enum) {
            block,
            table_index: *FaultyTable,
            table_content: *FaultyTable,
        };

        const FaultyTable = struct {
            index_address: u64,
            index_checksum: u128,
            // TODO bitset
            content_blocks_written: usize = 0,
            // When null, the table is awaiting an index block.
            // When non-null, the table is awaiting content blocks.
            content_blocks_total: ?usize = null,
        };

        grid: *Grid,

        faulty_blocks: FaultyBlocks,
        /// Index within `faulty_blocks`, but may be past `faulty_blocks.count()` due to removes.
        ///
        /// Invariants:
        /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
        /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
        faulty_blocks_repair_index: usize = 0,

        faulty_tables: IOPS(FaultyTable, constants.grid_repair_tables_max) = .{},
        faulty_table_blocks: [constants.grid_repair_tables_max]Grid.BlockPtr,

        writes: IOPS(Write, grid_repair_writes_max) = .{},
        write_blocks: [grid_repair_writes_max]Grid.BlockPtr,

        checkpoint_tick_context: Grid.NextTick = undefined,
        checkpoint_progress: ?struct {
            /// Set bits correspond to `writes` that must complete before invoking `callback`.
            writes_pending: WriteSet,
            callback: fn(*GridRepairQueue) void,
        } = null,

        pub fn init(allocator: std.mem.Allocator, grid: *Grid) error{OutOfMemory}!GridRepairQueue {
            var faulty_blocks = FaultyBlocks{};
            errdefer faulty_blocks.deinit(allocator);

            try faulty_blocks.ensureTotalCapacity(
                allocator,
                constants.grid_repair_blocks_max +
                constants.grid_repair_tables_max * lsm_table_blocks_max,
            );

            var faulty_table_blocks: [constants.grid_repair_tables_max]Grid.BlockPtr = undefined;
            for (faulty_table_blocks) |*block, i| {
                errdefer for (faulty_table_blocks[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (faulty_table_blocks) |b| allocator.free(b);

            var write_blocks: [grid_repair_writes_max]Grid.BlockPtr = undefined;
            for (write_blocks) |*block, i| {
                errdefer for (write_blocks[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (write_blocks) |b| allocator.free(b);

            return GridRepairQueue{
                .grid = grid,
                .faulty_blocks = faulty_blocks,
                .faulty_table_blocks = faulty_table_blocks,
                .write_blocks = write_blocks,
            };
        }

        pub fn deinit(queue: *GridRepairQueue, allocator: std.mem.Allocator) void {
            for (queue.write_blocks) |block| allocator.free(block);
            for (queue.faulty_table_blocks) |block| allocator.free(block);
            queue.faulty_blocks.deinit(allocator);

            queue.* = undefined;
        }

        /// When the queue wants more blocks than fit in a single request message, successive calls
        /// to this function return different BlockRequests.
        pub fn block_requests(queue: *GridRepairQueue, requests: []vsr.BlockRequest) usize {
            const faults_total = queue.faulty_blocks.count();
            if (faults_total == 0) return 0;

            const faulty_block_addresses = queue.faulty_blocks.entries.items(.key);
            const faulty_block_data = queue.faulty_blocks.entries.items(.value);

            var requests_count: usize = 0;
            var fault_offset: usize = 0;
            while (fault_offset < faults_total and requests_count < requests.len)
                : (fault_offset += 1)
            {
                const fault_index =
                    (queue.faulty_blocks_repair_index + fault_offset) % faults_total;
                const fault = &faulty_block_data[fault_index];

                if (!fault.received) {
                    requests[requests_count] = .{
                        .block_address = faulty_block_addresses[fault_index],
                        .block_checksum = faulty_block_data[fault_index].checksum,
                    };
                    requests_count += 1;
                }
            }
            queue.faulty_blocks_repair_index =
                (queue.faulty_blocks_repair_index + fault_offset) % faults_total;

            return requests_count;
        }

        pub fn full(queue: *const GridRepairQueue) bool {
            const faulty_blocks_free =
                queue.faulty_blocks.count() -
                constants.grid_repair_tables_max * lsm_table_blocks_max;
            const tables_free = constants.grid_repair_tables_max - queue.faulty_tables.availabe();

            return faulty_blocks_free == 0 or tables_free == 0;
        }

        /// Queue a faulty block for repair.
        pub fn queue_fault(
            queue: *GridRepairQueue,
            address: u64,
            checksum: u128,
            repair_strategy: enum { block, table },
        ) error{
            /// The block is already marked as faulty.
            Faulty
            // The queue has insufficient capacity to queue the fault.
            //Full,
        }!void {
            assert(queue.checkpoint_progress == null);
            assert(!queue.grid.superblock.free_set.is_free(address));
            assert(!queue.full());

            if (queue.faulty_blocks.get(address)) |fault| {
                assert(fault.checksum == checksum);
                return error.Faulty;
            }

            const progress: FaultProgress = switch (repair_strategy) {
                .block => progress: {
                    //const faulty_blocks_free =
                    //    queue.faulty_blocks.count() -
                    //    constants.grid_repair_tables_max * lsm_table_blocks_max;
                    //if (faulty_blocks_free == 0) return error.Full;

                    break :progress .block;
                },
                .table => progress: {
                    const table = queue.faulty_tables.acquire().?;// orelse return error.Full;
                    table.* = .{ .index_address = address, .index_checksum = checksum };
                    break :progress .{ .table_index = table };
                },
            };

            queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
                .checksum = checksum,
                .progress = progress,
            });
        }

        fn queue_remove(queue: *GridRepairQueue, index: usize) void {
            queue.faulty_blocks.swapRemoveAt(index);

            if (queue.faulty_blocks_repair_index == queue.faulty_blocks.count()) {
                queue.faulty_blocks_repair_index = 0;
            } else {
                if (queue.faulty_blocks_repair_index > index) {
                    queue.faulty_blocks_repair_index -= 1;
                }
            }
        }

        pub fn repair(queue: *GridRepairQueue, block_data: Grid.BlockPtrConst) error{
            Canceling,
            /// The block is faulty and needs repair, but the queue is too busy right now.
            Busy,
            /// The block is not faulty; no need to repair it.
            Clean,
        }!void {
            assert(queue.checkpoint_progress == null);

            if (queue.grid.canceling) |_| return error.Canceling;

            const block_header = schema.header_from_block(block_data);

            const faulty_block_index =
                queue.faulty_blocks.getIndex(block_header.op) orelse return error.Clean;
            const fault = &queue.faulty_blocks.entries.items(.value)[faulty_block_index];
            if (fault.checksum != block_header.checksum) return error.Clean;
            if (fault.received) return error.Clean;
            fault.received = true;

            switch (fault.progress) {
                .table_index => |table| {
                    assert(table.content_blocks_total == null);
                    assert(block_header.checksum == table.index_checksum);
                    assert(BlockType.from(block_header.operation) == .index);

                    queue.queue_table_content(block_data, table);

                    stdx.copy_disjoint(
                        .inexact,
                        u8,
                        queue.faulty_table_blocks[queue.faulty_tables.index(table)],
                        block_data[0..block_header.size],
                    );

                    const index_schema = schema.TableIndex.from(block_data);
                    table.content_blocks_total = index_schema.content_blocks_used(block_data);

                    var content_block_index: usize = 0;
                    while (content_block_index < table.content_blocks_total.?)
                        : (content_block_index += 1)
                    {
                        const content_block_id =
                            index_schema.content_block(block_data, content_block_index);
                        queue.faulty_blocks.putAssumeCapacityNoClobber(content_block_id.address, .{
                            .checksum = content_block_id.checksum,
                            .progress = .{ .table_content = table },
                        });
                    }

                    return;
                },

                .block,
                .table_content,
                => {
                    const write = queue.writes.acquire() orelse return error.Busy;
                    const write_index = queue.writes.index(write);

                    stdx.copy_disjoint(.inexact, u8, queue.write_blocks[write_index], block_data);
                    write.* = .{ .queue = queue, .checksum = block_header.checksum };

                    queue.grid.write_block(
                        repair_write_block_callback,
                        &write.write,
                        &queue.write_blocks[write_index],
                        .repair,
                    );
                },
            }
        }

        fn repair_write_block_callback(grid_write: *Grid.Write) void {
            const write = @fieldParentPtr(Write, "write", grid_write);
            const write_index = write.queue.writes.index(write);
            const queue = write.queue;

            if (queue.grid.canceling) |_| return;

            const faulty_block_index = queue.faulty_blocks.getIndex(grid_write.address).?;
            const fault = queue.faulty_blocks.entries.items(.value)[faulty_block_index];
            assert(fault.received);

            queue.queue_remove(faulty_block_index);

            switch (fault.progress) {
                .block => {},
                .table_index => |table| {
                    assert(table.content_blocks_total.? > 0);
                    assert(table.content_blocks_total.? == table.content_blocks_written);

                    queue.faulty_tables.release(table);
                },
                .table_content => |table| {
                    assert(table.content_blocks_written < table.content_blocks_total.?);

                    table.content_blocks_written += 1;
                    if (table.content_blocks_written == table.content_blocks_total.?) {
                        std.mem.swap(
                            Grid.BlockPtr,
                            &queue.faulty_table_blocks[queue.faulty_tables.index(table)],
                            &queue.write_blocks[write_index],
                        );

                        write.* = .{ .queue = queue, .checksum = table.index_checksum };

                        queue.grid.write_block(
                            repair_write_block_callback,
                            &write.write,
                            &queue.write_blocks[write_index],
                            .repair,
                        );
                        return;
                    }
                },
            }

            queue.writes.release(write);

            if (queue.checkpoint_progress) |*checkpoint_progress| {
                if (checkpoint_progress.writes_pending.isSet(write_index)) {
                    checkpoint_progress.writes_pending.unset(write_index);
                    queue.checkpoint_join();
                }
            }
        }

        pub fn cancel(queue: *GridRepairQueue) void {
            assert(queue.checkpoint_progress == null);

            queue.faulty_blocks.clearRetainingCapacity();
            queue.faulty_blocks_repair_index = 0;

            var faulty_tables_iterator = queue.faulty_tables.iterate();
            while (faulty_tables_iterator.next()) |table| {
                queue.faulty_tables.release(table);
            }

            // Release the writes manually. GridRepairQueue.cancel() is invoked after Grid.cancel()
            // finishes – Grid.cancel() waited for these writes to complete, but did not invoke
            // their callbacks.
            var writes_iterator = queue.writes.iterate();
            while (writes_iterator.next()) |write| queue.writes.release(write);
            assert(queue.writes.executing() == 0);
        }

        /// The callback is invoked when the writes to every block *that is staged to be released*
        /// finish. (All other writes can safely complete after the checkpoint.)
        pub fn checkpoint(queue: *GridRepairQueue, callback: fn(*GridRepairQueue) void) void {
            assert(queue.checkpoint_progress == null);

            var faulty_blocks = queue.faulty_blocks.iterator();
            while (faulty_blocks.next()) |fault_entry| {
                if (queue.grid.superblock.free_set.is_released(fault_entry.key_ptr.*)) {
                    faulty_blocks.index -= 1;
                    faulty_blocks.len -= 1;
                    queue.queue_remove(faulty_blocks.index);
                }
            }

            var faulty_tables = queue.faulty_tables.iterate();
            while (faulty_tables.next()) |table| {
                if (queue.grid.superblock.free_set.is_released(table.index_address)) {
                    queue.faulty_tables.release(table);
                }
            }

            var writes_pending = WriteSet.initEmpty();
            var writes = queue.writes.iterate();
            while (writes.next()) |write| {
                if (queue.grid.superblock.free_set.is_released(write.write.address)) {
                    writes_pending.set(queue.writes.index(write));
                }
            }

            queue.checkpoint_progress = .{
                .writes_pending = writes_pending,
                .callback = callback,
            };

            if (writes_pending.count() == 0) {
                queue.grid.on_next_tick(checkpoint_tick_callback, &queue.checkpoint_tick_context);
            }
        }

        fn checkpoint_tick_callback(next_tick: *Grid.NextTick) void {
            const queue = @fieldParentPtr(GridRepairQueue, "checkpoint_tick_context", next_tick);
            assert(queue.checkpoint_progress != null);
            assert(queue.checkpoint_progress.?.writes_pending.count() == 0);

            queue.checkpoint_done();
        }

        fn checkpoint_join(queue: *GridRepairQueue) void {
            assert(queue.checkpoint_progress != null);

            if (queue.checkpoint_progress.?.writes_pending.count() == 0) {
                queue.checkpoint_done();
            }
        }

        fn checkpoint_done(queue: *GridRepairQueue) void {
            assert(queue.checkpoint_progress != null);

            const checkpoint_progress = queue.checkpoint_progress.?;
            assert(checkpoint_progress.writes_pending.count() == 0);

            queue.checkpoint_progress = null;
            checkpoint_progress.callback(queue);
        }
    };
}

//fn table_blocks_max(comptime Forest: type) usize {
//    var blocks_max: usize = 0;
//    inline for (std.meta.fields(Forest.Grooves)) |groove_field| {
//        const Groove = groove_field.field_type;
//
//        blocks_max = @maximum(blocks_max, tree_blocks_max(Groove.ObjectTree));
//        blocks_max = @maximum(blocks_max, tree_blocks_max(Groove.IdTree));
//        inline for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
//            blocks_max = @maximum(blocks_max, tree_blocks_max(tree_field.field_type));
//        }
//    }
//    assert(blocks_max >= 2);
//    return blocks_max;
//}

// TODO Implement this in table.zig. TableSchema{ ... }.etc()
//const AnyTable = struct {
//    fn index_schema(index_block: []align(16) const u8) struct {
//        filter_blocks: usize,
//        filter_blocks_max: usize,
//        data_blocks: usize,
//        data_blocks_max: usize,
//    } {
//        const block_header = std.mem.bytesAsValue(vsr.Header, block_data[0..@sizeOf(vsr.Header)]);
//        const filter_blocks = block_header.commit;
//        const filter_blocks_max = block_header.TODO;
//        const data_blocks = block_header.request;
//        const data_blocks_max = block_header.TODO;
//        assert(filter_blocks <= filter_blocks_max);
//        assert(data_blocks <= data_blocks_max);
//        assert(data_blocks >= filter_blocks);
//
//        return .{
//            .filter_blocks = filter_blocks,
//            .filter_blocks_max = filter_blocks_max,
//            .data_blocks = data_blocks,
//            .data_blocks_max = data_blocks_max,
//        };
//    }
//
//    fn index_content(index_block: []align(16) const u8, i: usize) struct {
//        address: u64,
//        checksum: u128,
//    } {
//        const schema = index_schema(index_block);
//        assert(i < schema.filter_blocks + schema.data_blocks);
//
//        if (i < schema.filter_blocks) {
//            // Filter block.
//            const filter_block_checksums_bytes =
//                index_block[@sizeOf(vsr.Header)..][0..schema.filter_blocks_max * @sizeOf(u128)];
//            const filter_block_checksums = std.mem.bytesAsSlice(u128, filter_block_checksums_bytes);
//            const filter_block_addresses_bytes =
//                index_block[@sizeOf(vsr.Header)..][0..schema.filter_blocks_max * @sizeOf(u128)];
//        } else {
//            // Data block.
//        }
//    }
//};

            //if (fault.table_progress != null and
            //    fault.table_progress.?.* == .await_index)
            //{
            //    assert(block_header.operation.case(BlockType) == .index);
            //    assert(fault.table_progress.?.await_index.table_address == block_header.op);
            //    assert(fault.table_progress.?.await_index.table_checksum == block_header.checksum);
            //
            //    const removed = queue.faulty_blocks.remove(block_header.op);
            //    assert(removed);
            //
            //    const table_progress_index =
            //        queue.faulty_tables.index(fault.table_progress_index.?);
            //
            //    const table_schema = TableSchema.from_index(block_data);
            //    fault.table_progress.* = .{ .await_content = .{
            //        .table_address = block_header.op,
            //        .table_checksum = block_header.checksum,
            //        .blocks_total = table_schema.filter_block_count + table_schema.data_block_count,
            //    } };
            //
            //    const table_progress_block = queue.faulty_table_blocks[table_progress_index];
            //    stdx.copy_disjoint(.inexact, u8, table_progress_block, block_data);
            //    return .repair;
            //}
            //
            //if (fault.table_progress != null and
            //    fault.table_progress.?.* == .await_content)
            //{
            //    fault.blocks_queued -= 1;
            //}

        //const TableProgress = union(enum) {
        //    /// The entire table needs to be repaired/synced.
        //    /// We are awaiting the index block.
        //    await_index: struct {
        //        table_address: u64,
        //        table_checksum: u128,
        //    },
        //
        //    /// Table index block is in the corresponding slot of `faulty_table_blocks`.
        //    /// We are awaiting its filter/data blocks.
        //    ///
        //    /// Invariants:
        //    /// - blocks_queued ≤ blocks_total
        //    /// - blocks_written ≤ blocks_total
        //    /// - blocks_written + blocks_queued ≤ blocks_total
        //    /// When complete:
        //    /// - blocks_queued = 0
        //    /// - blocks_written = blocks_total
        //    await_content: struct {
        //        table_address: u64,
        //        table_checksum: u128,
        //
        //        /// The table's `filter_block_count + data_block_count`.
        //        blocks_total: usize,
        //        /// The number of data/filter blocks from this table in `faulty_blocks`.
        //        blocks_queued: usize = 0,
        //        /// The number of data/filter blocks from this table that have been written.
        //        blocks_written: usize = 0,
        //    },
        //};
        //
        //const Fault = union(enum) {
        //    /// Waiting for a single block, which can be written immediately.
        //    block: struct { address: u64, checksum: u128 },
        //    /// The entire table needs to be repaired/synced.
        //    /// We are awaiting the index block.
        //    /// The index block will not be written until all of its content has been written.
        //    table_index: struct { address: u64, checksum: u128 },
        //    /// We are awaiting the table's filter/data blocks.
        //    table_content: *TableProgress,
        //};

        //pub fn contains(queue: *const GridRepairQueue, block_id: struct {
        //    block_address: u64,
        //    block_checksum: u128,
        //}) bool {
        //    
        //}
        //
        //fn find_fault(queue: *GridRepairQueue, fault_id: BlockId) ?union(enum) {
        //    block: *Fault,
        //    table_index: *Fault,
        //    table_content: struct {
        //        fault: *Fault,
        //        index: usize,
        //    },
        //} {
        //    var faults = queue.faults.iterate();
        //    const fault = while (faults.next()) |fault| {
        //        // TODO(Zig) inline-switch block+table_index
        //        switch (fault) {
        //            .block => |block_id| if (std.meta.eql(block_id, fault_id)) return fault;
        //            .table_index => |block_id| if (std.meta.eql(block_id, fault_id)) return fault;
        //            .table_content => |table_progress| {
        //                if (parameters.block_address == table_progress.index_address) {
        //                    assert(parameters.block_checksum == table_progress.index_checksum);
        //                    return .already_faulty;
        //                }
        //                const schema = TableSchema.from_index_block(table_progress.index_block);
        //                if (schema.find_content_block(table_progress.index_block, 
        //            },
        //        }
        //    }
        //    return null;
        //}

        //fn queue_table_content(
        //    queue: *GridRepairQueue,
        //    index_block_data: Grid.BlockPtrConst,
        //    table: *FaultyTable,
        //) void {
        //    const index_schema = schema.TableIndex.from(index_block_data);
        //    assert(index_schema.content_blocks_used(index_block_data) > 0);
        //
            //if (queue.faulty_blocks.capacity() <
            //    queue.faulty_blocks.count() + index_schema.content_blocks_used(index_block_data))
            //{
            //    return error.Busy;
            //}

            //for ([_]struct {
            //    checksums: []const u128,
            //    addresses: []const u64,
            //}{
            //    .{
            //        .checksums = index_schema.filter_checksums_used(index_block_data),
            //        .addresses = index_schema.filter_addresses_used(index_block_data),
            //    },
            //    .{
            //        .checksums = index_schema.data_checksums_used(index_block_data),
            //        .addresses = index_schema.data_addresses_used(index_block_data),
            //    },
            //}) |content| {
            //    assert(content.checksums.len > 0);
            //    assert(content.checksums.len == content.addresses.len);
            //
            //    for (content.checksums) |content_checksum, i| {
            //        queue.faulty_blocks.putAssumeCapacityNoClobber(content.addresses[i], .{
            //            .checksum = content_checksum,
            //            .progress = .{ .table_content = table },
            //        });
            //    }
            //}
        //}
