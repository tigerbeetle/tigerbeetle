//! Track and repair corrupt/missing grid blocks.
//!
//! - The GridRepairQueue is LSM-aware: it can repair entire tables.
//! - The GridRepairQueue is "coherent" – that is, all of the blocks in the queue belong in the
//!   replica's current checkpoint. (The GridRepairQueue repairs blocks that are not-free. The
//!   GridRepairQueue *may* repair released blocks, until they are freed at the checkpoint).
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.grid_repair_queue);
const maybe = stdx.maybe;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const vsr = @import("../vsr.zig");

const allocate_block = @import("../lsm/grid.zig").allocate_block;
const BlockType = @import("../lsm/grid.zig").BlockType;
const GridType = @import("../lsm/grid.zig").GridType;
const FIFO = @import("../fifo.zig").FIFO;
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

        // TODO should Replica own these?
        const Write = struct {
            queue: *GridRepairQueue,
            write: Grid.Write = undefined,
        };

        const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, struct {
            checksum: u128,
            progress: FaultProgress,
            /// Initially false. Set to true when we receive and begin writing this block.
            /// (The FaultyBlocks entry is removed from the hashmap after the block is written).
            received: bool = false,
        });

        const FaultProgress = union(enum) {
            /// Repair a single block.
            block,
            /// Repair the table and all of its content. Awaiting table index block.
            table_index: TableIndex,
            /// Repair the table and all of its content. Awaiting table content blocks.
            table_content: TableContent,

            const TableIndex = struct { table: *RepairTable };
            const TableContent = struct { table: *RepairTable, index: usize };
        };

        pub const RepairTableResult = enum { canceled, duplicate, repaired };
        pub const RepairTable = struct {
            index_address: u64,
            index_checksum: u128,
            /// TODO(Congestion control): This bitset is currently used only for extra validation.
            /// Eventually we should request tables using this + EWAH encoding, instead of
            /// block-by-block.
            content_blocks_received: TableContentBlocksSet = TableContentBlocksSet.initEmpty(),
            table_blocks_written: usize = 0,
            /// When null, the table is awaiting an index block.
            /// When non-null, the table is awaiting content blocks.
            /// The count is "1 + table.content_blocks_used" (+1 is the index block).
            table_blocks_total: ?usize = null,

            callback: fn (*RepairTable, RepairTableResult) void,
            next: ?*RepairTable = null,
            tick_context: Grid.NextTick = undefined,
        };

        grid: *Grid,

        faulty_blocks: FaultyBlocks,
        /// Index within `faulty_blocks`, used to cycle through block-repair requests.
        ///
        /// Invariants:
        /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
        /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
        faulty_blocks_repair_index: usize = 0,

        faulty_tables: FIFO(RepairTable) = .{ .name = "grid_repair_queue_tables" },

        writes: IOPS(Write, grid_repair_writes_max) = .{},
        write_blocks: [grid_repair_writes_max]Grid.BlockPtr,

        /// Guard against new block/table repairs queueing within RepairTable callbacks.
        canceling: bool = false,

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

            var write_blocks: [grid_repair_writes_max]Grid.BlockPtr = undefined;
            for (write_blocks) |*block, i| {
                errdefer for (write_blocks[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (write_blocks) |b| allocator.free(b);

            return GridRepairQueue{
                .grid = grid,
                .faulty_blocks = faulty_blocks,
                .write_blocks = write_blocks,
            };
        }

        pub fn deinit(queue: *GridRepairQueue, allocator: std.mem.Allocator) void {
            for (queue.write_blocks) |block| allocator.free(block);
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

        /// Count the number of non-table block repairs available.
        pub fn blocks_available(queue: *const GridRepairQueue) usize {
            const faulty_blocks_free =
                queue.faulty_blocks.capacity() -
                queue.faulty_blocks.count() -
                constants.grid_repair_tables_max * lsm_table_blocks_max;
            return faulty_blocks_free;
        }

        //pub fn scrub_block(
        //    queue: *GridRepairQueue,
        //    callback: fn (*GridRepairQueue, enum { released, repaired, valid }) void,
        //    address: u64,
        //    checksum: u128,
        //    repair_strategy: enum { block, table },
        //) void {
        //}

        /// Queue a faulty block or table to request from the cluster and repair.
        pub fn request_block(queue: *GridRepairQueue, address: u64, checksum: u128) void {
            assert(queue.checkpoint_progress == null); // TODO is this possible? we don't stop scrubbing during checkpoint
            assert(queue.blocks_available() > 0);
            assert(queue.faulty_tables.empty()); // TODO remove
            assert(queue.faulty_tables.count <= constants.grid_repair_tables_max);
            assert(!queue.canceling);
            assert(!queue.grid.superblock.free_set.is_free(address));

            if (queue.faulty_blocks.get(address)) |fault| {
                assert(fault.checksum == checksum);
                return;
            }

            log.debug("{}: request_block: address={} checksum={}", .{
                queue.grid.superblock.replica(),
                address,
                checksum,
            });

            queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
                .checksum = checksum,
                .progress = .block,
            });

            // TODO Check grid cache?
        }

        pub fn request_table(
            queue: *GridRepairQueue,
            callback: fn(*RepairTable, RepairTableResult) void,
            table: *RepairTable,
            address: u64,
            checksum: u128,
        ) void {
            assert(queue.checkpoint_progress == null);
            assert(queue.faulty_tables.count < constants.grid_repair_tables_max);
            assert(!queue.canceling);
            assert(!queue.grid.superblock.free_set.is_free(address));

            // TODO Allow block faults to coexist with table faults.
            if (constants.verify) {
                var queued_faults = queue.faulty_blocks.iterator();
                while (queued_faults.next()) |queued_fault| {
                    assert(queued_fault.value_ptr.progress != .block);
                }
            }

            table.* = .{
                .callback = callback,
                .index_address = address,
                .index_checksum = checksum,
            };

            var tables = queue.faulty_tables.peek();
            while (tables) |queue_table| : (tables = queue_table.next) {
                if (queue_table.index_checksum == checksum) {
                    assert(queue_table.index_address == address);

                    queue.grid.on_next_tick(request_table_tick_callback, &table.tick_context);
                    return;
                }
            }

            queue.faulty_tables.push(table);
            queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
                .checksum = checksum,
                .progress = .{ .table_index = .{ .table = table } },
            });
            queue.repair_block_from_cache(address);
        }

        fn request_table_tick_callback(tick_context: *Grid.NextTick) void {
            const table = @fieldParentPtr(RepairTable, "tick_context", tick_context);
            assert(table.index_address > 0);
            assert(table.content_blocks_received.count() == 0);
            assert(table.table_blocks_written == 0);
            assert(table.table_blocks_total == null);
            assert(table.next == null);

            (table.callback)(table, .duplicate);
        }

        /// Attempt to repair the block directly from the grid cache.
        /// During table repair after state sync, commit/compaction may simultaneously be retrieving
        /// blocks using the `Grid.read_remote_queue`. But those blocks aren't written immediately,
        /// only cached.
        fn repair_block_from_cache(queue: *GridRepairQueue, address: u64) void {
            assert(queue.checkpoint_progress == null); // TODO is this possible? we don't stop scrubbing during checkpoint
            assert(queue.grid.canceling == null);
            assert(!queue.canceling);
            assert(!queue.grid.superblock.free_set.is_free(address));

            const fault = queue.faulty_blocks.getPtr(address).?;
            const block = queue.grid.read_block_from_cache(
                address,
                fault.checksum,
                .repair,
            ) orelse return;

            queue.repair_block(block) catch |err| assert(err == error.Busy);
        }

        /// error.Busy: The block is faulty and needs repair, but the queue is too busy right now.
        /// error.Clean: The block is not faulty; no need to repair it.
        pub fn repair_block(
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

            const write = queue.writes.acquire() orelse return error.Busy;
            const write_index = queue.writes.index(write);

            stdx.copy_disjoint(.inexact, u8, queue.write_blocks[write_index], block_data);
            write.* = .{ .queue = queue };//, .checksum = block_header.checksum };

            queue.grid.write_block(
                repair_write_block_callback,
                &write.write,
                &queue.write_blocks[write_index],
                .repair,
            );

            switch (fault.progress) {
                .block => {},
                .table_index => |*progress| queue.repair_table_index(progress, block_data),
                .table_content => |*progress| queue.repair_table_content(progress),
            }

            fault.received = true;
        }

        fn repair_table_index(
            queue: *GridRepairQueue,
            progress: *FaultProgress.TableIndex,
            index_block_data: Grid.BlockPtrConst,
        ) void {
            assert(queue.checkpoint_progress == null);
            assert(queue.grid.canceling == null);
            assert(progress.table.table_blocks_total == null);
            assert(progress.table.content_blocks_received.count() == 0);

            const index_schema = schema.TableIndex.from(index_block_data);
            const index_block_header = schema.header_from_block(index_block_data);
            assert(index_block_header.op == progress.table.index_address);
            assert(index_block_header.checksum == progress.table.index_checksum);
            assert(BlockType.from(index_block_header.operation) == .index);

            const content_blocks_total = index_schema.content_blocks_used(index_block_data);
            progress.table.table_blocks_total = 1 + content_blocks_total;

            var content_block_index: usize = 0;
            while (content_block_index < content_blocks_total) : (content_block_index += 1) {
                const block_id = index_schema.content_block(index_block_data, content_block_index);
                assert(!queue.grid.superblock.free_set.is_free(block_id.block_address));

                const block_fault =
                    queue.faulty_blocks.getOrPutAssumeCapacity(block_id.block_address);
                if (block_fault.found_existing) {
                    // The content block may already have been queued by either the scrubber or a
                    // commit/compaction grid read.
                    assert(block_fault.value_ptr.progress == .block);
                    assert(block_fault.value_ptr.checksum == block_id.block_checksum);

                    block_fault.value_ptr.progress = .{ .table_content = .{
                        .table = progress.table,
                        .index = content_block_index,
                    } };

                    if (block_fault.value_ptr.received) {
                        progress.table.content_blocks_received.set(content_block_index);
                        progress.table.table_blocks_written += 1;
                    }
                } else {
                    block_fault.value_ptr.* = .{
                        .checksum = block_id.block_checksum,
                        .progress = .{ .table_content = .{
                            .table = progress.table,
                            .index = content_block_index,
                        } },
                    };
                }
            }
        }

        fn repair_table_content(
            queue: *GridRepairQueue,
            progress: *FaultProgress.TableContent,
        ) void {
            assert(queue.checkpoint_progress == null);
            assert(queue.grid.canceling == null);
            assert(progress.table.table_blocks_written < progress.table.table_blocks_total.?);
            assert(!progress.table.content_blocks_received.isSet(progress.index));

            progress.table.content_blocks_received.set(progress.index);
        }

        fn repair_write_block_callback(grid_write: *Grid.Write) void {
            const write = @fieldParentPtr(Write, "write", grid_write);
            const queue = write.queue;
            assert(queue.grid.canceling == null);
            maybe(queue.checkpoint_progress == null);

            defer queue.writes.release(write);

            if (queue.checkpoint_progress) |*checkpoint_progress| {
                const write_index = queue.writes.index(write);
                if (checkpoint_progress.writes_pending.isSet(write_index)) {
                    checkpoint_progress.writes_pending.unset(write_index);
                    queue.checkpoint_join();
                    return;
                }
            }

            const fault_index = queue.faulty_blocks.getIndex(grid_write.address).?;
            const fault_address = queue.faulty_blocks.entries.items(.key)[fault_index];
            const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
            assert(fault.received);
            assert(fault_address == write.write.address);
            assert(!queue.grid.superblock.free_set.is_free(fault_address));

            // Copy this to the stack so that it is available after releasing the fault.
            // (We want to release the fault prior to invoking the callback so that within the
            // callback there is definitely a free block in the hashmap).
            const fault_progress = fault.progress;

            queue.release_fault(fault_index);

            if (switch (fault_progress) {
                .block => null,
                .table_index => |progress| progress.table,
                .table_content => |progress| progress.table,
            }) |table| {
                assert(table.table_blocks_total != null); // We already received the index block.
                assert(table.table_blocks_written < table.table_blocks_total.?);
                assert(table.content_blocks_received.count() <= table.table_blocks_total.? - 1);

                table.table_blocks_written += 1;
                if (table.table_blocks_written == table.table_blocks_total.?) {
                    queue.faulty_tables.remove(table);
                    (table.callback)(table, .repaired);
                }
            }
        }

        fn release_fault(queue: *GridRepairQueue, fault_index: usize) void {
            assert(queue.checkpoint_progress == null); // TODO this should fail!
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

        pub fn cancel(queue: *GridRepairQueue) void {
            assert(queue.checkpoint_progress == null);
            assert(queue.grid.canceling == null);
            assert(!queue.canceling);

            queue.faulty_blocks.clearRetainingCapacity();
            queue.faulty_blocks_repair_index = 0;

            queue.canceling = true;
            while (queue.faulty_tables.pop()) |table| (table.callback)(table, .canceled);
            queue.canceling = false;

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
            assert(queue.grid.canceling == null);
            assert(!queue.canceling);

            var faulty_blocks = queue.faulty_blocks.iterator();
            while (faulty_blocks.next()) |fault_entry| {
                if (queue.grid.superblock.free_set.is_released(fault_entry.key_ptr.*)) {
                    faulty_blocks.index -= 1;
                    faulty_blocks.len -= 1;
                    queue.release_fault(faulty_blocks.index);
                }
            }

            queue.canceling = true;
            var tables: FIFO(RepairTable) = .{ .name = queue.faulty_tables.name };
            while (queue.faulty_tables.pop()) |table| {
                if (queue.grid.superblock.free_set.is_released(table.index_address)) {
                    (table.callback)(table, .canceled);
                } else {
                    tables.push(table);
                }
            }
            queue.faulty_tables = tables;
            queue.canceling = false;

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
            assert(!queue.canceling);

            queue.checkpoint_done();
        }

        fn checkpoint_join(queue: *GridRepairQueue) void {
            assert(queue.checkpoint_progress != null);
            assert(!queue.canceling);

            if (queue.checkpoint_progress.?.writes_pending.count() == 0) {
                queue.checkpoint_done();
            }
        }

        fn checkpoint_done(queue: *GridRepairQueue) void {
            assert(queue.checkpoint_progress != null);
            assert(!queue.canceling);

            const checkpoint_progress = queue.checkpoint_progress.?;
            assert(checkpoint_progress.writes_pending.count() == 0);

            queue.checkpoint_progress = null;
            checkpoint_progress.callback(queue);
        }
    };
}
