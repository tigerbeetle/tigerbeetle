//! Track corrupt/missing grid blocks.
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

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const vsr = @import("../vsr.zig");

const allocate_block = @import("./grid.zig").allocate_block;
const BlockType = @import("./grid.zig").BlockType;
const GridType = @import("./grid.zig").GridType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const BlockPtrConst = *align(constants.sector_size) const [constants.block_size]u8;

/// Immediately after state sync we want access to all of the grid's write bandwidth to rapidly sync
/// table blocks.
const grid_repair_writes_max = constants.grid_iops_write_max;

pub const GridRepairQueue = struct {
    const WriteSet = std.StaticBitSet(grid_repair_writes_max);

    /// A block is removed from the collection when:
    /// - the block's write completes, or
    /// - the block is released and the release is checkpointed, or
    /// - the grid is canceled.
    ///
    /// The map is keyed by block address.
    const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, FaultyBlock);

    const FaultyBlock = struct {
        checksum: u128,
        /// Transitions:
        /// - Initial state is `waiting`.
        /// - `waiting → writing` when the block arrives and begins to repair.
        /// - `writing → aborting` when the (writing) block is released by the checkpoint.
        state: enum { waiting, writing, aborting } = .waiting,
    };

    pub const RepairTableResult = enum {
        /// The table is synced: its index and content blocks are all written.
        repaired,
        /// The table no longer needs to be synced because the Grid was canceled.
        canceled,
        /// The table no longer needs to be synced because it was released at the checkpoint.
        released,
    };

    pub const Options = struct {
        /// Lower-bound for the limit of concurrent request_block()'s available.
        blocks_max: usize,
    };

    options: Options,

    /// Invariants:
    /// - For every block address in faulty_blocks, ¬free_set.is_free(address).
    faulty_blocks: FaultyBlocks,
    /// Index within `faulty_blocks`, used to cycle through block-repair requests.
    ///
    /// Invariants:
    /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
    /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
    faulty_blocks_repair_index: usize = 0,


    checkpointing: ?struct {
        /// The number of faulty_blocks with state=aborting.
        aborting: usize,
    } = null,

    pub fn init(allocator: std.mem.Allocator, options: Options) error{OutOfMemory}!GridRepairQueue {
        var faulty_blocks = FaultyBlocks{};
        errdefer faulty_blocks.deinit(allocator);

        try faulty_blocks.ensureTotalCapacity(allocator, options.blocks_max);

        return GridRepairQueue{
            .options = options,
            .faulty_blocks = faulty_blocks,
        };
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
            queue.faulty_blocks.count();
        return faulty_blocks_free;
    }

    /// Queue a faulty block to request from the cluster and repair.
    pub fn request_block(queue: *GridRepairQueue, address: u64, checksum: u128) void {
        assert(queue.request_blocks_available() > 0);

        if (queue.faulty_blocks.get(address)) |fault| {
            assert(fault.checksum == checksum);
            return;
        }

        queue.faulty_blocks.putAssumeCapacityNoClobber(address, .{
            .checksum = checksum,
        });
    }

    pub fn repair_ready(queue: *const GridRepairQueue, address: u64, checksum: u128) bool {
        const fault_index = queue.faulty_blocks.getIndex(address) orelse return false;
        const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
        return fault.checksum == checksum and fault.state == .waiting;
    }

    pub fn repair_commence(queue: *const GridRepairQueue, address: u64, checksum: u128) void {
        assert(queue.repair_ready(address, checksum));

        const fault_index = queue.faulty_blocks.getIndex(address).?;
        const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
        assert(fault.checksum == checksum);
        assert(fault.state == .waiting);

        fault.state = .writing;
    }

    pub fn repair_complete(queue: *GridRepairQueue, block: BlockPtrConst) void {
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
        assert(queue.checkpointing == null);

        queue.faulty_blocks.clearRetainingCapacity();
        queue.faulty_blocks_repair_index = 0;
    }

    pub fn checkpoint_commence(
        queue: *GridRepairQueue,
        free_set: *const vsr.superblock.SuperBlockFreeSet,
    ) void {
        assert(queue.checkpointing == null);

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

        queue.checkpointing = .{ .aborting = aborting };
    }

    /// Returns `true` when the `state≠waiting` faults for blocks that are staged to be
    /// released have finished. (All other writes can safely complete after the checkpoint.)
    pub fn checkpoint_complete(queue: *GridRepairQueue) bool {
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
