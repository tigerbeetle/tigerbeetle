//! Track and repair corrupt/missing grid blocks.
//!
//! - The GridRepairQueue is LSM-aware: it can repair entire tables.
//! - The GridRepairQueue is "coherent" – that is, all of the blocks in the queue belong in the
//!   replica's current checkpoint:
//!   - The GridRepairQueue will not repair freed blocks.
//!   - The GridRepairQueue will repair released blocks, until they are freed at the checkpoint.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.grid_repair_queue);
const maybe = stdx.maybe;

// 952788207565583899

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const vsr = @import("../vsr.zig");

const allocate_block = @import("../lsm/grid.zig").allocate_block;
const BlockType = @import("../lsm/grid.zig").BlockType;
const GridType = @import("../lsm/grid.zig").GridType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const BlockPtrConst = *align(constants.sector_size) const [constants.block_size]u8;

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

pub const GridRepairQueue = struct {
    const WriteSet = std.StaticBitSet(grid_repair_writes_max);
    const TableContentBlocksSet = std.StaticBitSet(lsm_table_blocks_max);

    /// A block is removed from the collection when:
    /// - the block's write completes, or
    /// - the block is released and the release is checkpointed, or
    /// - the grid is canceled.
    const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, FaultyBlock);

    const FaultyBlock = struct {
        checksum: u128,
        progress: FaultProgress,
        /// Transitions:
        /// - Initial state is `waiting`.
        /// - `waiting → writing` when the block arrives and begins to repair.
        /// - `writing → aborting` when the (writing) block is released by the checkpoint.
        state: enum { waiting, writing, aborting } = .waiting,
    };

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

    pub const RepairTableResult = enum { canceled, repaired };
    pub const RepairTable = struct {
        // TODO rename "table_address"/"table_checksum"?
        index_address: u64,
        index_checksum: u128,
        /// TODO(Congestion control): This bitset is currently used only for extra validation.
        /// Eventually we should request tables using this + EWAH encoding, instead of
        /// block-by-block.
        /// Invariants:
        /// - content_blocks_received.count < table_blocks_total
        content_blocks_received: TableContentBlocksSet = TableContentBlocksSet.initEmpty(),
        /// This count includes the index block.
        /// Invariants:
        /// - table_blocks_written ≤ table_blocks_total
        table_blocks_written: usize = 0,
        /// When null, the table is awaiting an index block.
        /// When non-null, the table is awaiting content blocks.
        /// This count includes the index block.
        table_blocks_total: ?usize = null,

        /// Invoked when the table index block and all content blocks have been written.
        callback: fn (*RepairTable, RepairTableResult) void,
        /// "next" belongs to the `faulty_tables` FIFO.
        next: ?*RepairTable = null,
    };

    /// Invariants:
    /// - For every block address in faulty_blocks, ¬free_set.is_free(address).
    faulty_blocks: FaultyBlocks,
    /// Index within `faulty_blocks`, used to cycle through block-repair requests.
    ///
    /// Invariants:
    /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
    /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
    faulty_blocks_repair_index: usize = 0,

    /// Invariants:
    /// - For every index address in faulty_tables: ¬free_set.is_free(address).
    faulty_tables: FIFO(RepairTable) = .{ .name = "grid_repair_queue_tables" },

    checkpointing: ?struct {
        /// The number of faulty_blocks with state=aborting.
        aborting: usize,
    } = null,

    /// Guard against new block/table repairs queueing within RepairTable callbacks.
    canceling: bool = false,

    pub fn init(allocator: std.mem.Allocator) error{OutOfMemory}!GridRepairQueue {
        var faulty_blocks = FaultyBlocks{};
        errdefer faulty_blocks.deinit(allocator);

        try faulty_blocks.ensureTotalCapacity(
            allocator,
            constants.grid_repair_blocks_max +
            constants.grid_repair_tables_max * lsm_table_blocks_max,
        );

        return GridRepairQueue{ .faulty_blocks = faulty_blocks };
    }

    pub fn deinit(queue: *GridRepairQueue, allocator: std.mem.Allocator) void {
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
            if (fault.state == .waiting) {
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
    pub fn request_blocks_available(queue: *const GridRepairQueue) usize {
        const faulty_blocks_free =
            queue.faulty_blocks.capacity() -
            queue.faulty_blocks.count() -
            constants.grid_repair_tables_max * lsm_table_blocks_max;
        return faulty_blocks_free;
    }

    /// Queue a faulty block or table to request from the cluster and repair.
    pub fn request_block(queue: *GridRepairQueue, address: u64, checksum: u128) void {
        assert(queue.request_blocks_available() > 0);
        assert(queue.faulty_tables.count <= constants.grid_repair_tables_max);
        assert(!queue.canceling);

        if (queue.faulty_blocks.get(address)) |fault| {
            assert(fault.checksum == checksum);
            return;
        }

        queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
            .checksum = checksum,
            .progress = .block,
        });
    }

    pub fn request_table_queued(queue: *GridRepairQueue, address: u64, checksum: u128) bool {
        var tables = queue.faulty_tables.peek();
        while (tables) |queue_table| : (tables = queue_table.next) {
            if (queue_table.index_address == address) {
                assert(queue_table.index_checksum == checksum);
                return true;
            }
        }
        return false;
    }

    pub fn request_table(
        queue: *GridRepairQueue,
        callback: fn(*RepairTable, RepairTableResult) void,
        table: *RepairTable,
        address: u64,
        checksum: u128,
    ) void {
        assert(queue.faulty_tables.count < constants.grid_repair_tables_max);
        assert(!queue.canceling);
        assert(!queue.request_table_queued(address, checksum));

        var tables = queue.faulty_tables.peek();
        while (tables) |queue_table| : (tables = queue_table.next) assert(queue_table != table);

        table.* = .{
            .callback = callback,
            .index_address = address,
            .index_checksum = checksum,
        };
        queue.faulty_tables.push(table);

        if (queue.faulty_blocks.getPtr(address)) |fault| {
            assert(fault.checksum == checksum);
            assert(fault.progress == .block);

            fault.progress = .{ .table_index = .{ .table = table } };
        } else {
            queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
                .checksum = checksum,
                .progress = .{ .table_index = .{ .table = table } },
            });
        }
    }

    pub fn repair_ready(queue: *const GridRepairQueue, address: u64, checksum: u128) bool {
        const fault_index = queue.faulty_blocks.getIndex(address) orelse return false;
        const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
        return fault.checksum == checksum and fault.state == .waiting;
    }

    pub fn repair_start(queue: *const GridRepairQueue, address: u64, checksum: u128) void {
        assert(queue.repair_ready(address, checksum));

        const fault_index = queue.faulty_blocks.getIndex(address).?;
        const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
        assert(fault.checksum == checksum);
        assert(fault.state == .waiting);

        if (fault.progress == .table_content) {
            const progress = &fault.progress.table_content;
            assert(progress.table.table_blocks_written < progress.table.table_blocks_total.?);
            assert(!progress.table.content_blocks_received.isSet(progress.index));

            progress.table.content_blocks_received.set(progress.index);
        }

        fault.state = .writing;
    }

    pub fn repair_done(queue: *GridRepairQueue, block: BlockPtrConst) void {
        const block_header = schema.header_from_block(block);
        const fault_index = queue.faulty_blocks.getIndex(block_header.op).?;
        const fault_address = queue.faulty_blocks.entries.items(.key)[fault_index];
        const fault: FaultyBlock = queue.faulty_blocks.entries.items(.value)[fault_index];
        assert(fault_address == block_header.op);
        assert(fault.checksum == block_header.checksum);
        assert(fault.state == .aborting or fault.state == .writing);

        queue.release_fault(fault_index);

        if (fault.state == .aborting) {
            queue.checkpointing.?.aborting -= 1;
            return;
        }

        if (fault.progress == .table_index) {
            // The reason that the content blocks are queued here (when the write ends) rather
            // than when the write begins is so that a `request_block()` can be converted to a
            // `request_table()` after the former's write is already in progress.
            queue.request_table_content(fault.progress.table_index.table, block);
        }

        if (switch (fault.progress) {
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

    fn request_table_content(
        queue: *GridRepairQueue,
        table: *RepairTable,
        index_block_data: BlockPtrConst,
    ) void {
        assert(table.table_blocks_total == null);
        assert(table.table_blocks_written == 0);
        assert(table.content_blocks_received.count() == 0);

        const index_schema = schema.TableIndex.from(index_block_data);
        const index_block_header = schema.header_from_block(index_block_data);
        assert(index_block_header.op == table.index_address);
        assert(index_block_header.checksum == table.index_checksum);
        assert(BlockType.from(index_block_header.operation) == .index);

        const content_blocks_total = index_schema.content_blocks_used(index_block_data);
        table.table_blocks_total = 1 + content_blocks_total;

        var content_block_index: usize = 0;
        while (content_block_index < content_blocks_total) : (content_block_index += 1) {
            const block_id = index_schema.content_block(index_block_data, content_block_index);

            const block_fault =
                queue.faulty_blocks.getOrPutAssumeCapacity(block_id.block_address);
            if (block_fault.found_existing) {
                // The content block may already have been queued by either the scrubber or a
                // commit/compaction grid read.
                assert(block_fault.value_ptr.progress == .block);
                assert(block_fault.value_ptr.checksum == block_id.block_checksum);

                block_fault.value_ptr.progress = .{ .table_content = .{
                    .table = table,
                    .index = content_block_index,
                } };

                if (block_fault.value_ptr.state == .writing) {
                    table.content_blocks_received.set(content_block_index);
                }
            } else {
                block_fault.value_ptr.* = .{
                    .checksum = block_id.block_checksum,
                    .progress = .{ .table_content = .{
                        .table = table,
                        .index = content_block_index,
                    } },
                };
            }
        }
    }

    fn release_fault(queue: *GridRepairQueue, fault_index: usize) void {
        assert(queue.faulty_blocks_repair_index < queue.faulty_blocks.count());

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
        assert(!queue.canceling);

        queue.faulty_blocks.clearRetainingCapacity();
        queue.faulty_blocks_repair_index = 0;

        queue.canceling = true;
        while (queue.faulty_tables.pop()) |table| (table.callback)(table, .canceled);
        queue.canceling = false;
    }

    pub fn checkpoint_start(
        queue: *GridRepairQueue,
        free_set: *const vsr.superblock.SuperBlockFreeSet,
    ) void {
        assert(!queue.canceling);

        var aborting: usize = 0;

        var faulty_blocks = queue.faulty_blocks.iterator();
        while (faulty_blocks.next()) |fault_entry| {
            const fault_address = fault_entry.key_ptr.*;
            assert(!free_set.is_free(fault_address));
            assert(fault_entry.value_ptr.state != .aborting);

            if (free_set.is_released(fault_address)) {
                switch (fault_entry.value_ptr.state) {
                    .waiting => {
                        faulty_blocks.index -= 1;
                        faulty_blocks.len -= 1;
                        queue.release_fault(faulty_blocks.index);
                    },
                    .writing => {
                        fault_entry.value_ptr.state = .aborting;
                        aborting += 1;
                    },
                    .aborting => unreachable,
                }
            }
        }

        queue.canceling = true;
        var tables: FIFO(RepairTable) = .{ .name = queue.faulty_tables.name };
        while (queue.faulty_tables.pop()) |table| {
            assert(!free_set.is_free(table.index_address));

            if (free_set.is_released(table.index_address)) {
                (table.callback)(table, .canceled);
            } else {
                tables.push(table);
            }
        }
        queue.faulty_tables = tables;
        queue.canceling = false;

        queue.checkpointing = .{ .aborting = aborting };
    }

    /// Returns `true` when the `state≠waiting` faults for blocks that are staged to be
    /// released have finished. (All other writes can safely complete after the checkpoint.)
    pub fn checkpoint_done(queue: *GridRepairQueue) bool {
        assert(!queue.canceling);
        assert(queue.checkpointing != null);

        if (queue.checkpointing.?.aborting == 0) {
            queue.checkpointing = null;

            var faulty_blocks = queue.faulty_blocks.iterator();
            while (faulty_blocks.next()) |fault_entry| {
                assert(fault_entry.value_ptr.state != .aborting);
            }

            return true;
        } else {
            return false;
        }
    }
};
