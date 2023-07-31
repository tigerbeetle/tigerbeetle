//! Track and repair corrupt/missing grid blocks.
//!
//! The GridRepairQueue is LSM-aware.
//! It maintains the LSM invariant: if a table index block is in the grid, then all of the index
//! block's referenced filter/data blocks are in the grid.
//!
//! The GridRepairQueue is "coherent" – that is, all of the blocks in the queue belong in the
//! replica's current checkpoint.
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

pub fn GridRepairQueueType(comptime Storage: type) type {
    return struct {
        const GridRepairQueue = @This();
        const Grid = GridType(Storage);
        const WriteSet = std.StaticBitSet(grid_repair_writes_max);
        const TableContentBlocksSet = std.StaticBitSet(lsm_table_blocks_max);

        const Write = struct {
            queue: *GridRepairQueue,
            write: Grid.Write = undefined,
            checksum: u128,
        };

        const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, struct {
            checksum: u128,
            progress: FaultProgress,
            /// Initially false.
            /// Set to true when we start writing this block.
            /// (After writing, the FaultyBlocks entry is removed from the hashmap).
            received: bool = false,
        });

        const FaultProgress = union(enum) {
            block,
            table_index: TableIndex,
            table_content: TableContent,

            const TableIndex = struct { table: *FaultyTable };
            const TableContent = struct { table: *FaultyTable, index: usize };
        };

        const FaultyTable = struct {
            index_address: u64,
            index_checksum: u128,
            content_blocks_received: TableContentBlocksSet = TableContentBlocksSet.initEmpty(),
            content_blocks_written: usize = 0,
            // When null, the table is awaiting an index block.
            // When non-null, the table is awaiting content blocks.
            content_blocks_total: ?usize = null,
        };

        grid: *Grid,

        faulty_blocks: FaultyBlocks,
        /// Index within `faulty_blocks`, used to cycle through block-repair requests.
        ///
        /// Invariants:
        /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
        /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
        faulty_blocks_repair_index: usize = 0,

        faulty_tables: IOPS(FaultyTable, constants.grid_repair_tables_max) = .{},
        // TODO(Grid Block Pool): Store these block references directly within each FaultyTable.
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
        /// to this function cycle through the pending BlockRequests.
        pub fn block_requests(queue: *GridRepairQueue, requests: []vsr.BlockRequest) usize {
            const faults_total = queue.faulty_blocks.count();
            if (faults_total == 0) return 0;
            assert(faults_total > queue.faulty_blocks_repair_index);

            const fault_addresses = queue.faulty_blocks.entries.items(.key);
            const fault_data = queue.faulty_blocks.entries.items(.value);

            var requests_count: usize = 0;
            var fault_offset: usize = 0;
            while (fault_offset < faults_total and requests_count < requests.len)
                : (fault_offset += 1)
            {
                const fault_index =
                    (queue.faulty_blocks_repair_index + fault_offset) % faults_total;
                const fault = &fault_data[fault_index];

                if (!fault.received) {
                    requests[requests_count] = .{
                        .block_address = fault_addresses[fault_index],
                        .block_checksum = fault_data[fault_index].checksum,
                    };
                    requests_count += 1;
                }
            }
            queue.faulty_blocks_repair_index =
                (queue.faulty_blocks_repair_index + fault_offset) % faults_total;

            return requests_count;
        }

        pub fn empty(queue: *const GridRepairQueue) bool {
            if (queue.faulty_blocks.count() == 0) {
                assert(queue.faulty_tables.executing() == 0);
                return true;
            } else {
                return false;
            }
        }

        pub fn full(queue: *const GridRepairQueue) bool {
            const faulty_blocks_free =
                queue.faulty_blocks.capacity() -
                queue.faulty_blocks.count() -
                constants.grid_repair_tables_max * lsm_table_blocks_max;
            const tables_free = queue.faulty_tables.available();

            return faulty_blocks_free == 0 or tables_free == 0;
        }

        /// Queue a faulty block for repair.
        /// error.Faulty: The block is already marked as faulty.
        pub fn queue_insert(
            queue: *GridRepairQueue,
            address: u64,
            checksum: u128,
            repair_strategy: enum { block, table },
        ) error{Faulty}!void {
            assert(queue.checkpoint_progress == null);
            assert(!queue.grid.superblock.free_set.is_free(address));
            assert(!queue.full());

            if (queue.faulty_blocks.get(address)) |fault| {
                assert(fault.checksum == checksum);
                return error.Faulty;
            }

            queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
                .checksum = checksum,
                .progress = switch (repair_strategy) {
                    .block => .block,
                    .table => progress: {
                        const table = queue.faulty_tables.acquire().?;
                        table.* = .{ .index_address = address, .index_checksum = checksum };
                        break :progress .{ .table_index = .{ .table = table } };
                    },
                },
            });
        }

        fn queue_remove(queue: *GridRepairQueue, fault_index: usize) void {
            assert(queue.checkpoint_progress == null);
            assert(queue.faulty_blocks_repair_index < queue.faulty_blocks.count());

            const fault_address = queue.faulty_blocks.entries.items(.key)[fault_index];
            assert(!queue.grid.superblock.free_set.is_free(fault_address));

            queue.faulty_blocks.swapRemoveAt(fault_index);

            if (queue.faulty_blocks_repair_index == queue.faulty_blocks.count()) {
                queue.faulty_blocks_repair_index = 0;
            } else {
                if (queue.faulty_blocks_repair_index > fault_index) {
                    queue.faulty_blocks_repair_index -= 1;
                }
            }
        }

        /// error.Busy: The block is faulty and needs repair, but the queue is too busy right now.
        /// error.Clean: The block is not faulty; no need to repair it.
        pub fn repair(
            queue: *GridRepairQueue,
            block_data: Grid.BlockPtrConst,
        ) error{Canceling, Busy, Clean}!void {
            assert(queue.checkpoint_progress == null);

            if (queue.grid.canceling) |_| return error.Canceling;

            const block_header = schema.header_from_block(block_data);
            const fault_index =
                queue.faulty_blocks.getIndex(block_header.op) orelse return error.Clean;

            const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
            if (fault.checksum != block_header.checksum) return error.Clean;
            if (fault.received) return error.Clean;

            switch (fault.progress) {
                .block => try queue.repair_write_block(block_data),
                .table_index => |*progress| queue.repair_table_index(block_data, progress),
                .table_content => |*progress| try queue.repair_table_content(block_data, progress),
            }
            fault.received = true;
        }

        fn repair_table_index(
            queue: *GridRepairQueue,
            block_data: Grid.BlockPtrConst,
            progress: *FaultProgress.TableIndex,
        ) void {
            assert(queue.checkpoint_progress == null);
            assert(queue.grid.canceling == null);
            assert(progress.table.content_blocks_total == null);
            assert(progress.table.content_blocks_received.count() == 0);

            const block_header = schema.header_from_block(block_data);
            assert(block_header.checksum == progress.table.index_checksum);
            assert(BlockType.from(block_header.operation) == .index);

            stdx.copy_disjoint(
                .inexact,
                u8,
                queue.faulty_table_blocks[queue.faulty_tables.index(progress.table)],
                block_data[0..block_header.size],
            );

            const index_schema = schema.TableIndex.from(block_data);
            progress.table.content_blocks_total = index_schema.content_blocks_used(block_data);

            var content_block_index: usize = 0;
            while (content_block_index < progress.table.content_blocks_total.?)
                : (content_block_index += 1)
            {
                const content_block_id =
                    index_schema.content_block(block_data, content_block_index);
                const content_block_fault =
                    queue.faulty_blocks.getOrPutAssumeCapacity(content_block_id.block_address);

                if (content_block_fault.found_existing) {
                    assert(content_block_fault.value_ptr.progress == .block);
                    assert(content_block_fault.value_ptr.checksum ==
                        content_block_id.block_checksum);

                    content_block_fault.value_ptr.progress = .{ .table_content = progress.table };
                } else {
                    content_block_fault.value_ptr.* = .{
                        .checksum = content_block_id.block_checksum,
                        .progress = .{ .table_content = progress.table },
                    };
                }
            }
        }

        fn repair_table_content(
            queue: *GridRepairQueue,
            block_data: Grid.BlockPtrConst,
            progress: *FaultProgress.TableContent,
        ) error{Busy}!void {
            assert(queue.checkpoint_progress == null);
            assert(queue.grid.canceling == null);
            assert(progress.table.content_blocks_written < progress.table.content_blocks_total.?);
            assert(!progress.table.content_blocks_received.isSet(progress.index));

            {
                const index_block = queue.faulty_table_blocks[queue.tables.index(progress.table)];
                const index_schema = schema.TableIndex.from(index_block);
                const content_block_header = schema.header_from_block(block_data);
                const content_block_id = table_index_schema.content_block(progress.index);
                assert(content_block_id.block_address == content_block_header.op);
                assert(content_block_id.block_checksum == content_block_header.checksum);
                assert(content_block_id.block_type ==
                    BlockType.from(content_block_header.operation));
            }

            try queue.repair_write_block(block_data);

            progress.table.content_blocks_received.set(progress.index);
        }

        fn repair_write_block(
            queue: *GridRepairQueue,
            block_data: Grid.BlockPtrConst,
        ) error{Busy}!void {
            assert(queue.checkpoint_progress == null);
            assert(queue.grid.canceling == null);

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
        }

        fn repair_write_block_callback(grid_write: *Grid.Write) void {
            const write = @fieldParentPtr(Write, "write", grid_write);
            const write_index = write.queue.writes.index(write);
            const queue = write.queue;

            if (queue.grid.canceling) |_| return;

            const fault_index = queue.faulty_blocks.getIndex(grid_write.address).?;
            const fault_address = queue.faulty_blocks.entries.items(.key)[fault_index];
            const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
            assert(fault.received);
            assert(fault.checksum == write.checksum);
            assert(!queue.grid.superblock.free_set.is_free(fault_address));

            queue.queue_remove(fault_index);

            switch (fault.progress) {
                .block => {},
                .table_index => |progress| {
                    const table: *FaultyTable = table_index_progress.table;
                    assert(table.content_blocks_total.? > 0);
                    assert(table.content_blocks_total.? == table.content_blocks_received.count());
                    assert(table.content_blocks_total.? == table.content_blocks_written);

                    queue.faulty_tables.release(table);
                },
                .table_content => |progress| {
                    const table: *FaultyTable = table_index_progress.table;
                    assert(table.content_blocks_written < table.content_blocks_total.?);
                    assert(table.content_blocks_received.isSet(progress.index));
                    assert(table.content_blocks_received.count() <= table.content_blocks_total.?);
                    assert(table.content_blocks_received.count() > table.content_blocks_written);

                    table.content_blocks_written += 1;
                    if (table.content_blocks_written == table.content_blocks_total.?) {
                        write.* = .{ .queue = queue, .checksum = table.index_checksum };

                        queue.grid.write_block(
                            repair_write_block_callback,
                            &write.write,
                            &queue.faulty_table_blocks[queue.faulty_tables.index(table)],
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
