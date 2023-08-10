const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const free_set = @import("../vsr/superblock_free_set.zig");
const schema = @import("schema.zig");

const SuperBlockType = vsr.SuperBlockType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;
const stdx = @import("../stdx.zig");
const GridRepairQueue = @import("../vsr/grid_repair_queue.zig").GridRepairQueue;

const log = stdx.log.scoped(.grid);
const tracer = @import("../tracer.zig");

/// A block's type is implicitly determined by how its address is stored (e.g. in the index block).
/// BlockType is an additional check that a block has the expected type on read.
///
/// The BlockType is stored in the block's `header.operation`.
pub const BlockType = enum(u8) {
    /// Unused; verifies that no block is written with a default 0 operation.
    reserved = 0,

    manifest = 1,
    index = 2,
    filter = 3,
    data = 4,

    pub fn valid(vsr_operation: vsr.Operation) bool {
        _ = std.meta.intToEnum(BlockType, @enumToInt(vsr_operation)) catch return false;

        return true;
    }

    pub inline fn from(vsr_operation: vsr.Operation) BlockType {
        return @intToEnum(BlockType, @enumToInt(vsr_operation));
    }

    pub inline fn operation(block_type: BlockType) vsr.Operation {
        return @intToEnum(vsr.Operation, @enumToInt(block_type));
    }
};

// Leave this outside GridType so we can call it from modules that don't know about Storage.
pub fn allocate_block(
    allocator: mem.Allocator,
) error{OutOfMemory}!*align(constants.sector_size) [constants.block_size]u8 {
    const block = try allocator.alignedAlloc(u8, constants.sector_size, constants.block_size);
    mem.set(u8, block, 0);
    return block[0..constants.block_size];
}

/// The Grid provides access to on-disk blocks (blobs of `block_size` bytes).
/// Each block is identified by an "address" (`u64`, beginning at 1).
///
/// Recently/frequently-used blocks are transparently cached in memory.
pub fn GridType(comptime Storage: type) type {
    const block_size = constants.block_size;
    const SuperBlock = SuperBlockType(Storage);

    return struct {
        const Grid = @This();

        pub const read_iops_max = constants.grid_iops_read_max;
        pub const write_iops_max = constants.grid_iops_write_max;

        pub const RepairTable = GridRepairQueue.RepairTable;
        pub const RepairTableResult = GridRepairQueue.RepairTableResult;
        pub const BlockPtr = *align(constants.sector_size) [block_size]u8;
        pub const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;
        pub const BlockSlice = []align(constants.sector_size) const u8;
        pub const Reservation = free_set.Reservation;

        // Grid just reuses the Storage's NextTick abstraction for simplicity.
        pub const NextTick = Storage.NextTick;

        pub const Write = struct {
            callback: fn (*Grid.Write) void,
            address: u64,
            repair: bool,
            block: *BlockPtr,
            /// The current checkpoint when the write began.
            /// Verifies that the checkpoint does not advance during the (non-repair) write.
            checkpoint_id: u128,

            /// Link for the Grid.write_queue linked list.
            next: ?*Write = null,
        };

        const WriteIOP = struct {
            grid: *Grid,
            completion: Storage.Write,
            write: *Write,
        };

        const ReadBlockCallback = union(enum) {
            /// If the local read fails, report the error.
            local: fn (*Grid.Read, ReadBlockResult) void,
            /// If the local read fails, this read will be added to a linked list, which Replica can
            /// then interrogate each tick(). The callback passed to this function won't be called
            /// until the block has been recovered.
            remote: fn (*Grid.Read, BlockPtrConst) void,
        };

        pub const Read = struct {
            callback: ReadBlockCallback,
            address: u64,
            checksum: u128,
            /// The current checkpoint when the read began.
            /// Used to verify that the checkpoint does not advance while the read is in progress.
            checkpoint_id: u128,

            coherent: bool,
            cache_check: bool,
            cache_update: bool,
            pending: ReadPending = .{},
            resolves: FIFO(ReadPending) = .{ .name = null },

            grid: *Grid,
            next_tick: Grid.NextTick = undefined,

            /// Link for Grid.read_queue/Grid.read_remote_queue linked lists.
            next: ?*Read = null,
        };

        pub const ReadBlockResult = union(enum) {
            valid: BlockPtrConst,
            /// Checksum of block header is invalid.
            invalid_checksum,
            /// Checksum of block body is invalid.
            invalid_checksum_body,
            /// The block header is valid, but its `header.command` is not `block`.
            /// (This is possible due to misdirected IO).
            unexpected_command,
            /// The block is valid, but it is not the block we expected.
            unexpected_checksum,
        };

        const ReadPending = struct {
            /// Link for Read.resolves linked lists.
            next: ?*ReadPending = null,
        };

        const ReadIOP = struct {
            completion: Storage.Read,
            read: *Read,
        };

        const cache_interface = struct {
            inline fn address_from_address(address: *const u64) u64 {
                return address.*;
            }

            inline fn hash_address(address: u64) u64 {
                assert(address > 0);
                return stdx.hash_inline(address);
            }

            inline fn equal_addresses(a: u64, b: u64) bool {
                return a == b;
            }
        };

        const set_associative_cache_ways = 16;

        pub const Cache = SetAssociativeCache(
            u64,
            u64,
            cache_interface.address_from_address,
            cache_interface.hash_address,
            cache_interface.equal_addresses,
            .{
                .ways = set_associative_cache_ways,
                .value_alignment = @alignOf(u64),
            },
        );

        superblock: *SuperBlock,

        repair_queue: GridRepairQueue,

        cache: Cache,
        /// Each entry in cache has a corresponding block.
        cache_blocks: []BlockPtr,
        /// Each entry in cache has a corresponding bit.
        /// This bit tracks whether the cache block is definitely valid relative to the current
        /// checkpoint (matches what should be on disk). At the conclusion of state sync, all blocks
        /// are marked as invalid. This enables additional validation of cache consistency.
        cache_coherent: std.DynamicBitSetUnmanaged,
        /// TODO
        cache_durable: std.DynamicBitSetUnmanaged,

        write_iops: IOPS(WriteIOP, write_iops_max) = .{},
        write_iop_tracer_slots: [write_iops_max]?tracer.SpanStart = .{null} ** write_iops_max,
        write_queue: FIFO(Write) = .{ .name = "grid_write" },

        // Each read_iops has a corresponding block.
        read_iop_blocks: [read_iops_max]BlockPtr,
        read_iops: IOPS(ReadIOP, read_iops_max) = .{},
        read_iop_tracer_slots: [read_iops_max]?tracer.SpanStart = .{null} ** read_iops_max,
        read_queue: FIFO(Read) = .{ .name = "grid_read" },

        // List of Read.pending's which are in `read_queue` but also waiting for a free `read_iops`.
        read_pending_queue: FIFO(ReadPending) = .{ .name = "grid_read_pending" },
        read_remote_queue: FIFO(Read) = .{ .name = "grid_read_faulty" },
        // True if there's a read that is resolving callbacks.
        // If so, the read cache must not be invalidated.
        read_resolving: bool = false,

        checkpointing: ?struct { callback: fn (*Grid) void } = null,
        checkpointing_tick_context: NextTick = undefined,
        canceling: ?struct { callback: fn (*Grid) void } = null,
        canceling_tick_context: NextTick = undefined,

        pub fn init(allocator: mem.Allocator, options: struct {
            superblock: *SuperBlock,
            cache_blocks_count: u64 = Cache.value_count_max_multiple,
        }) !Grid {
            var repair_queue = try GridRepairQueue.init(allocator);
            errdefer repair_queue.deinit(allocator);

            const cache_blocks = try allocator.alloc(BlockPtr, options.cache_blocks_count);
            errdefer allocator.free(cache_blocks);

            var cache_coherent =
                try std.DynamicBitSetUnmanaged.initEmpty(allocator, options.cache_blocks_count);
            errdefer cache_coherent.deinit(allocator);

            var cache_durable =
                try std.DynamicBitSetUnmanaged.initEmpty(allocator, options.cache_blocks_count);
            errdefer cache_durable.deinit(allocator);

            for (cache_blocks) |*cache_block, i| {
                errdefer for (cache_blocks[0..i]) |block| allocator.free(block);
                cache_block.* = try allocate_block(allocator);
            }
            errdefer for (cache_blocks) |block| allocator.free(block);

            var cache = try Cache.init(allocator, options.cache_blocks_count, .{ .name = "grid" });
            errdefer cache.deinit(allocator);

            var read_iop_blocks: [read_iops_max]BlockPtr = undefined;

            for (&read_iop_blocks) |*read_iop_block, i| {
                errdefer for (read_iop_blocks[0..i]) |block| allocator.free(block);
                read_iop_block.* = try allocate_block(allocator);
            }
            errdefer for (&read_iop_blocks) |block| allocator.free(block);

            return Grid{
                .superblock = options.superblock,
                .repair_queue = repair_queue,
                .cache = cache,
                .cache_blocks = cache_blocks,
                .cache_coherent = cache_coherent,
                .cache_durable = cache_durable,
                .read_iop_blocks = read_iop_blocks,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            for (&grid.read_iop_blocks) |block| allocator.free(block);

            for (grid.read_iop_tracer_slots) |slot| assert(slot == null);
            for (grid.write_iop_tracer_slots) |slot| assert(slot == null);

            for (grid.cache_blocks) |block| allocator.free(block);
            allocator.free(grid.cache_blocks);

            grid.cache_durable.deinit(allocator);
            grid.cache_coherent.deinit(allocator);
            grid.cache.deinit(allocator);
            grid.repair_queue.deinit(allocator);

            grid.* = undefined;
        }

        pub fn checkpoint(grid: *Grid, callback: fn(*Grid) void) void {
            assert(grid.checkpointing == null);
            assert(grid.canceling == null);

            grid.repair_queue.checkpoint_start(&grid.superblock.free_set);

            grid.checkpointing = .{ .callback = callback };
            grid.on_next_tick(checkpoint_tick_callback, &grid.checkpointing_tick_context);
        }

        fn checkpoint_tick_callback(next_tick: *Grid.NextTick) void {
            const grid = @fieldParentPtr(Grid, "checkpointing_tick_context", next_tick);
            assert(grid.checkpointing != null);
            grid.checkpoint_join();
        }

        fn checkpoint_join(grid: *Grid) void {
            assert(grid.checkpointing != null);
            assert(grid.canceling == null);

            // We are still repairing some blocks that were released at the checkpoint.
            if (!grid.repair_queue.checkpoint_done()) return;

            var write_queue = grid.write_queue.peek();
            while (write_queue) |write| : (write_queue = write.next) {
                assert(write.repair);
                assert(!grid.superblock.free_set.is_free(write.address));
                assert(!grid.superblock.free_set.is_released(write.address));
            }

            var write_iops = grid.write_iops.iterate();
            while (write_iops.next()) |iop| {
                assert(iop.write.repair);
                assert(!grid.superblock.free_set.is_free(iop.write.address));
                assert(!grid.superblock.free_set.is_released(iop.write.address));
            }

            const callback = grid.checkpointing.?.callback;
            grid.checkpointing = null;
            callback(grid);
        }

        pub fn cancel(grid: *Grid, callback: fn (*Grid) void) void {
            assert(grid.canceling == null);
            assert(grid.checkpointing == null);

            grid.canceling = .{ .callback = callback };

            grid.repair_queue.cancel();
            grid.read_queue.reset();
            grid.read_pending_queue.reset();
            grid.read_remote_queue.reset();
            grid.write_queue.reset();
            grid.superblock.storage.reset_next_tick_lsm();
            grid.superblock.storage.on_next_tick(
                .vsr,
                cancel_tick_callback,
                &grid.canceling_tick_context,
            );
        }

        fn cancel_tick_callback(next_tick: *NextTick) void {
            const grid = @fieldParentPtr(Grid, "canceling_tick_context", next_tick);
            if (grid.canceling == null) return;

            assert(grid.checkpointing == null);
            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_remote_queue.empty());
            assert(grid.write_queue.empty());

            grid.cancel_join_callback();
        }

        fn cancel_join_callback(grid: *Grid) void {
            assert(grid.canceling != null);
            assert(grid.checkpointing == null);
            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_remote_queue.empty());
            assert(grid.write_queue.empty());

            if (grid.read_iops.executing() == 0 and
                grid.write_iops.executing() == 0)
            {
                const callback = grid.canceling.?.callback;
                grid.canceling = null;

                callback(grid);
            }
        }

        // TODO remove
        pub fn cache_invalidate(grid: *Grid) void {
            var cache_indexes = grid.cache_coherent.iterator(.{});
            while (cache_indexes.next()) |index| grid.cache_coherent.unset(index);
        }

        pub fn on_next_tick(
            grid: *Grid,
            callback: fn (*Grid.NextTick) void,
            next_tick: *Grid.NextTick,
        ) void {
            assert(grid.canceling == null);
            grid.superblock.storage.on_next_tick(.lsm, callback, next_tick);
        }

        /// Returning null indicates that there are not enough free blocks to fill the reservation.
        pub fn reserve(grid: *Grid, blocks_count: usize) ?Reservation {
            assert(grid.checkpointing == null);
            assert(grid.canceling == null);
            return grid.superblock.free_set.reserve(blocks_count);
        }

        /// Forfeit a reservation.
        pub fn forfeit(grid: *Grid, reservation: Reservation) void {
            assert(grid.checkpointing == null);
            assert(grid.canceling == null);
            return grid.superblock.free_set.forfeit(reservation);
        }

        /// Returns a just-allocated block.
        /// The caller is responsible for not acquiring more blocks than they reserved.
        pub fn acquire(grid: *Grid, reservation: Reservation) u64 {
            assert(grid.checkpointing == null);
            assert(grid.canceling == null);
            return grid.superblock.free_set.acquire(reservation).?;
        }

        /// This function should be used to release addresses, instead of release()
        /// on the free set directly, as this also demotes the address within the block cache.
        /// This reduces conflict misses in the block cache, by freeing ways soon after they are
        /// released.
        ///
        /// This does not remove the block from the cache — the block can be read until the next
        /// checkpoint.
        ///
        /// Asserts that the address is not currently being read from or written to.
        pub fn release(grid: *Grid, address: u64) void {
            assert(grid.checkpointing == null);
            assert(grid.canceling == null);
            assert(grid.writing(address, null) != .create);
            // It's safe to release an address that is being read from,
            // because the superblock will not allow it to be overwritten before
            // the end of the measure.

            grid.cache.demote(address);
            grid.superblock.free_set.release(address);
        }

        const Writing = enum { create, repair, none };

        /// If the address is being written to by a non-repair, return `.create`.
        /// If the address is being written to by a repair, return `.repair`.
        /// Otherwise return `.none`.
        ///
        /// Assert that the block pointer is not being used for any write if non-null.
        pub fn writing(grid: *Grid, address: u64, block: ?BlockPtrConst) Writing {
            assert(address > 0);

            var result = Writing.none;
            {
                var it = grid.write_queue.peek();
                while (it) |queued_write| : (it = queued_write.next) {
                    assert(block != queued_write.block.*);
                    if (address == queued_write.address) {
                        assert(result == .none);
                        result = if (queued_write.repair) .repair else .create;
                    }
                }
            }
            {
                var it = grid.write_iops.iterate();
                while (it.next()) |iop| {
                    assert(block != iop.write.block.*);
                    if (address == iop.write.address) {
                        assert(result == .none);
                        result = if (iop.write.repair) .repair else .create;
                    }
                }
            }
            return result;
        }

        /// Assert that the address is not currently being read from (disregarding repairs).
        /// Assert that the block pointer is not being used for any read if non-null.
        fn assert_not_reading(grid: *Grid, address: u64, block: ?BlockPtrConst) void {
            assert(address > 0);
            for ([_]*const FIFO(Read){
                &grid.read_queue,
                &grid.read_remote_queue,
            }) |queue| {
                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    if (queued_read.coherent) {
                        assert(address != queued_read.address);
                    }
                }
            }
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    if (iop.read.coherent) {
                        assert(address != iop.read.address);
                    }
                    const iop_block = grid.read_iop_blocks[grid.read_iops.index(iop)];
                    assert(block != iop_block);
                }
            }
        }

        // TODO move these functions to the bottom (verify_*)
        pub fn assert_only_repairing(grid: *Grid) void {
            assert(grid.canceling == null);
            assert(grid.read_remote_queue.empty());
            //assert(grid.read_pending_queue.empty());
            //assert(grid.read_queue.empty());

            var read_queue = grid.read_queue.peek();
            while (read_queue) |read| : (read_queue = read.next) {
                // Scrubber read are independent from LSM operations.
                assert(!read.coherent);
            }

            var write_queue = grid.write_queue.peek();
            while (write_queue) |write| : (write_queue = write.next) {
                assert(write.repair);
                assert(!grid.superblock.free_set.is_free(write.address));
            }

            var write_iops = grid.write_iops.iterate();
            while (write_iops.next()) |iop| {
                assert(iop.write.repair);
                assert(!grid.superblock.free_set.is_free(iop.write.address));
            }
        }

        pub fn fulfill_block(grid: *Grid, block: BlockPtrConst) bool {
            assert(grid.canceling == null);

            const block_header = schema.header_from_block(block);
            assert(block_header.cluster == grid.superblock.working.cluster);

            var reads_iterator = grid.read_remote_queue.peek();
            while (reads_iterator) |read| : (reads_iterator = read.next) {
                if (read.checksum == block_header.checksum and
                    read.address == block_header.op)
                {
                    if (read.cache_update) {
                        const cache_index = grid.cache.insert_index(&read.address);
                        const cache_block = grid.cache_blocks[cache_index];
                        stdx.copy_disjoint(.inexact, u8, cache_block, block[0..block_header.size]);
                    }

                    grid.read_remote_queue.remove(read);
                    grid.read_block_resolve(read, .{ .valid = block });
                    return true;
                }
            }
            return false;
        }

        pub fn repair_block_ready(grid: *Grid, address: u64, checksum: u128) bool {
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            return grid.repair_queue.repair_ready(address, checksum);
        }

        /// Write a block that should already exist but (maybe) doesn't because of:
        /// - a disk fault, or
        /// - the block was missed due to state sync.
        ///
        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn repair_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
        ) void {
            const block_header = schema.header_from_block(block.*);
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(grid.writing(block_header.op, block.*) == .none);
            assert(grid.repair_queue.repair_ready(block_header.op, block_header.checksum));
            assert(!grid.superblock.free_set.is_free(block_header.op));

            grid.repair_queue.repair_start(block_header.op, block_header.checksum);

            grid.write_block(callback, write, block, .repair);
        }

        /// Write a block for the first time.
        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn create_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
        ) void {
            const block_header = schema.header_from_block(block.*);
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(grid.writing(block_header.op, block.*) == .none);
            assert(!grid.repair_queue.repair_ready(block_header.op, block_header.checksum));
            assert(!grid.superblock.free_set.is_free(block_header.op));
            grid.assert_not_reading(block_header.op, block.*);

            grid.write_block(callback, write, block, .create);
        }

        fn write_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
            trigger: enum { create, repair },
        ) void {
            const header = schema.header_from_block(block.*);
            assert(header.cluster == grid.superblock.working.cluster);

            const address = header.op;
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(grid.writing(address, block.*) == .none);
            assert(!grid.superblock.free_set.is_free(address));

            if (constants.verify) {
                for (grid.cache_blocks) |cache_block| {
                    assert(cache_block != block.*);
                }
            }

            write.* = .{
                .callback = callback,
                .address = address,
                .repair = trigger == .repair,
                .block = block,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
            };

            const iop = grid.write_iops.acquire() orelse {
                grid.write_queue.push(write);
                return;
            };

            grid.write_block_with(iop, write);
        }

        fn write_block_with(grid: *Grid, iop: *WriteIOP, write: *Write) void {
            assert(!grid.superblock.free_set.is_free(write.address));

            const write_iop_index = grid.write_iops.index(iop);
            tracer.start(
                &grid.write_iop_tracer_slots[write_iop_index],
                .{ .grid_write_iop = .{ .index = write_iop_index } },
                @src(),
            );

            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .write = write,
            };

            grid.superblock.storage.write_sectors(
                write_block_callback,
                &iop.completion,
                write.block.*,
                .grid,
                block_offset(write.address),
            );
        }

        fn write_block_callback(completion: *Storage.Write) void {
            const iop = @fieldParentPtr(WriteIOP, "completion", completion);

            // We must copy these values to the stack as they will be overwritten
            // when we release the iop and potentially start a queued write.
            const grid = iop.grid;
            const completed_write = iop.write;

            // We can only update the cache if the Grid is not resolving callbacks with a cache block.
            assert(!grid.read_resolving);
            assert(!grid.superblock.free_set.is_free(completed_write.address));

            if (!completed_write.repair) {
                assert(grid.superblock.working.checkpoint_id() == completed_write.checkpoint_id);
            }

            // Insert the write block into the cache, and give the evicted block to the writer.
            const cache_index = grid.cache.insert_index(&completed_write.address);
            const cache_block = &grid.cache_blocks[cache_index];
            std.mem.swap(BlockPtr, cache_block, completed_write.block);
            std.mem.set(u8, completed_write.block.*, 0);
            grid.cache_coherent.set(cache_index);
            grid.cache_durable.set(cache_index);

            const write_iop_index = grid.write_iops.index(iop);
            tracer.end(
                &grid.write_iop_tracer_slots[write_iop_index],
                .{ .grid_write_iop = .{ .index = write_iop_index } },
            );

            if (grid.canceling) |_| {
                assert(grid.write_queue.empty());

                grid.write_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            if (completed_write.repair) grid.repair_queue.repair_done(cache_block.*);

            // Start a queued write if possible *before* calling the completed
            // write's callback. This ensures that if the callback calls
            // Grid.write_block() it doesn't preempt the queue.
            //
            // (Don't pop from the write queue until after the read-repairs are resolved.
            // Otherwise their resolution might complete grid cancellation, but the replica has
            // not released its own write iop (via callback).)
            if (grid.write_queue.pop()) |queued_write| {
                grid.write_block_with(iop, queued_write);
            } else {
                grid.write_iops.release(iop);
            }

            // This call must come after (logically) releasing the IOP. Otherwise we risk tripping
            // assertions forbidding concurrent writes using the same block/address
            // if the callback calls write_block().
            completed_write.callback(completed_write);
            if (grid.checkpointing) |_| grid.checkpoint_join();
        }

        /// Fetch the block synchronously from cache, if possible.
        /// The returned block pointer is only valid until the next Grid write.
        pub fn read_block_from_cache(
            grid: *Grid,
            address: u64,
            checksum: u128,
            options: struct { coherent: bool },
        ) ?BlockPtrConst {
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            if (options.coherent) {
                assert(grid.writing(address, null) != .create);
                assert(!grid.superblock.free_set.is_free(address));
            }

            assert(address > 0);

            const cache_index = grid.cache.get_index(address) orelse return null;
            const cache_block = grid.cache_blocks[cache_index];

            const header = schema.header_from_block(cache_block);
            assert(header.op == address);
            assert(header.cluster == grid.superblock.working.cluster);

            if (header.checksum == checksum) {
                if (constants.verify and
                    options.coherent and
                    grid.superblock.working.vsr_state.commit_unsynced_max == 0)
                {
                    grid.verify_read(address, cache_block);
                }

                return cache_block;
            } else {
                if (options.coherent) {
                    assert(grid.superblock.working.vsr_state.commit_unsynced_max > 0);
                }

                return null;
            }
        }

        pub fn read_block_from_cache_or_storage(
            grid: *Grid,
            callback: fn (*Grid.Read, BlockPtrConst) void,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
        ) void {
            grid.read_block_async(.{ .remote = callback }, read, address, checksum, .{
                .coherent = true,
                .cache_check = true,
                .cache_update = true,
            });
        }

        pub fn read_block_async(
            grid: *Grid,
            callback: ReadBlockCallback,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
            options: struct {
                coherent: bool,
                cache_check: bool,
                cache_update: bool,
            },
        ) void {
            assert(grid.superblock.opened);
            assert(address > 0);

            if (options.coherent) {
                assert(grid.canceling == null);
                assert(!grid.superblock.free_set.is_free(address));
                assert(grid.writing(address, null) != .create);
            } else {
                maybe(grid.canceling == null);
                // We try to read the block even when it is free — if we recently released it,
                // it might be found on disk anyway.
                // TODO maybe not, to unify invariants?
                maybe(grid.superblock.free_set.is_free(address));
                maybe(grid.writing(address, null) == .create);
            }

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .coherent = options.coherent, // TODO checkpoint_id relationship?
                .cache_check = options.cache_check,
                .cache_update = options.cache_update,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
                .grid = grid,
            };

            // TODO Maybe call synchronously when !options.check_cache.
            grid.on_next_tick(read_block_tick_callback, &read.next_tick);
        }

        fn read_block_tick_callback(next_tick: *Storage.NextTick) void {
            const read = @fieldParentPtr(Grid.Read, "next_tick", next_tick);
            const grid = read.grid;

            grid.read_block(read);
        }

        fn read_block(grid: *Grid, read: *Grid.Read) void {
            assert(grid.superblock.opened);
            if (read.coherent) {
                assert(grid.canceling == null);
                assert(!grid.superblock.free_set.is_free(read.address));
                assert(grid.writing(read.address, null) != .create);
            }

            assert(read.address > 0);

            // Check if a read is already processing/recovering and merge with it.
            for ([_]*const FIFO(Read){
                &grid.read_queue,
                &grid.read_remote_queue,
            }) |queue| {
                // Don't remote-repair repairs – the block may not belong in our current checkpoint.
                if (read.callback == .local) {
                    if (queue == &grid.read_remote_queue) continue;
                }

                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    if (queued_read.address == read.address) {
                        // TODO check all read options match
                        if (queued_read.checksum == read.checksum) {
                            queued_read.resolves.push(&read.pending);
                            return;
                        } else {
                            assert(!queued_read.coherent or !read.coherent);
                        }
                    }
                }
            }

            // When Read.cache_check is set, the caller of read_block() is responsible for calling
            // us via next_tick().
            // TODO should this be before the merge-with-queued-read step? Might be faster.
            if (read.cache_check) {
                if (grid.read_block_from_cache(
                    read.address,
                    read.checksum,
                    .{ .coherent = read.coherent },
                )) |cache_block| {
                    grid.read_block_resolve(read, .{ .valid = cache_block });
                    return;
                }
            }

            // Become the "root" read that's fetching the block for the given address.
            // The fetch happens asynchronously to avoid stack-overflow and nested cache invalidation.
            grid.read_queue.push(read);

            // Grab an IOP to resolve the block from storage.
            // Failure to do so means the read is queued to receive an IOP when one finishes.
            const iop = grid.read_iops.acquire() orelse {
                grid.read_pending_queue.push(&read.pending);
                return;
            };

            grid.read_block_with(iop, read);
        }

        fn read_block_with(grid: *Grid, iop: *Grid.ReadIOP, read: *Grid.Read) void {
            const address = read.address;
            assert(address > 0);

            // We can only update the cache if the Grid is not resolving callbacks with a cache block.
            assert(!grid.read_resolving);

            const read_iop_index = grid.read_iops.index(iop);
            tracer.start(
                &grid.read_iop_tracer_slots[read_iop_index],
                .{ .grid_read_iop = .{ .index = read_iop_index } },
                @src(),
            );

            iop.* = .{
                .completion = undefined,
                .read = read,
            };
            const iop_block = grid.read_iop_blocks[grid.read_iops.index(iop)];

            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                iop_block,
                .grid,
                block_offset(address),
            );
        }

        fn read_block_callback(completion: *Storage.Read) void {
            const iop = @fieldParentPtr(ReadIOP, "completion", completion);
            const read = iop.read;
            const grid = read.grid;
            //std.debug.print("{}: check\n", .{grid.superblock.replica()});
            const iop_block = &grid.read_iop_blocks[grid.read_iops.index(iop)];

            if (grid.canceling) |_| {
                grid.read_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            // Insert the block into the cache, and give the evicted block to `iop`.
            // TODO Maybe disable cache update for repairs?
            const cache_index =
                if (read.cache_update) grid.cache.insert_index(&read.address) else null;
            const cache_block = block: { // TODO rename "block"
                if (read.cache_update) {
                    const cache_block = &grid.cache_blocks[cache_index.?];
                    std.mem.swap(BlockPtr, iop_block, cache_block);
                    std.mem.set(u8, iop_block.*, 0);
                    break :block cache_block;
                } else {
                    break :block iop_block;
                }
            };

            //const cache_block = &grid.cache_blocks[cache_index];
            //std.mem.swap(BlockPtr, iop_block, cache_block);
            //std.mem.set(u8, iop_block.*, 0);
            //grid.cache_durable.set(cache_index);
            //if (read.coherent) {
            //    grid.cache_coherent.set(cache_index);
            //}

            const read_iop_index = grid.read_iops.index(iop);
            tracer.end(
                &grid.read_iop_tracer_slots[read_iop_index],
                .{ .grid_read_iop = .{ .index = read_iop_index } },
            );

            // Handoff the iop to a pending read or release it before resolving the callbacks below.
            if (grid.read_pending_queue.pop()) |pending| {
                const queued_read = @fieldParentPtr(Read, "pending", pending);
                grid.read_block_with(iop, queued_read);
            } else {
                grid.read_iops.release(iop);
            }

            // Remove the "root" read so that the address is no longer actively reading / locked.
            grid.read_queue.remove(read);

            const result = read_block_validate(cache_block.*, .{
                .address = read.address,
                .checksum = read.checksum,
            });

            if (result == .valid) {
                grid.read_block_resolve(read, .{ .valid = cache_block.* });
                return;
            }

            const header = mem.bytesAsValue(vsr.Header, cache_block.*[0..@sizeOf(vsr.Header)]);
            log.err(
                "{s}: expected address={} checksum={}, found address={} checksum={}",
                .{
                    @tagName(result),
                    read.address,
                    read.checksum,
                    header.op,
                    header.checksum,
                },
            );

            // Don't cache a corrupt or incorrect block.
            grid.cache.remove(read.address);
            //grid.cache_coherent.unset(cache_index);
            //grid.cache_durable.unset(cache_index);

            grid.read_block_resolve(read, result);
        }

        fn read_block_validate(block: BlockPtrConst, expect: struct {
            address: u64,
            checksum: u128,
        }) ReadBlockResult {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);

            if (!header.valid_checksum()) return .invalid_checksum;
            if (header.command != .block) return .unexpected_command;

            assert(header.size >= @sizeOf(vsr.Header));
            assert(header.size <= constants.block_size);

            const block_body = block[@sizeOf(vsr.Header)..header.size];
            if (!header.valid_checksum_body(block_body)) return .invalid_checksum_body;
            if (header.checksum != expect.checksum) return .unexpected_checksum;

            assert(header.op == expect.address);
            return .{ .valid = block };
        }

        fn read_block_resolve(grid: *Grid, read: *Grid.Read, result: ReadBlockResult) void {
            // Guard to make sure the cache cannot be updated by any read.callbacks() below.
            assert(!grid.read_resolving);
            grid.read_resolving = true;
            defer {
                assert(grid.read_resolving);
                grid.read_resolving = false;
            }

            if (read.coherent) {
                assert(!grid.superblock.free_set.is_free(read.address));
                assert(read.checkpoint_id == grid.superblock.working.checkpoint_id());
            }

            if (result == .valid) {
                const header = schema.header_from_block(result.valid);
                assert(header.cluster == grid.superblock.working.cluster);
                assert(header.op == read.address);
                assert(header.checksum == read.checksum);
            }

            var read_remote_resolves: FIFO(ReadPending) = .{ .name = read.resolves.name };

            // Resolve all reads queued to the address with the block.
            while (read.resolves.pop()) |pending| {
                const pending_read = @fieldParentPtr(Read, "pending", pending);
                assert(pending_read.address == read.address);
                assert(pending_read.checksum == read.checksum);
                //assert(pending_read.coherent == read.coherent);
                if (pending_read.coherent) {
                    assert(pending_read.checkpoint_id == grid.superblock.working.checkpoint_id());
                }

                switch (pending_read.callback) {
                    .local => |callback| callback(pending_read, result),
                    .remote => |callback| {
                        if (result == .valid) {
                            callback(pending_read, result.valid);
                        } else {
                            read_remote_resolves.push(&pending_read.pending);
                        }
                    },
                }
            }

            // Then invoke the callback with the cache block (which should be valid for the duration
            // of the callback as any nested Grid calls cannot synchronously update the cache).
            switch (read.callback) {
                .local => |callback| callback(read, result),
                .remote => |callback| {
                    if (result == .valid) {
                        callback(read, result.valid);
                    } else {
                        read_remote_resolves.push(&read.pending);
                    }
                },
            }

            // On the result of an invalid block, move the "root" read (and all others it
            // resolves) to recovery queue. Future reads on the same address will see the "root"
            // read in the recovery queue and enqueue to it.
            if (read_remote_resolves.pop()) |read_remote_head_pending| {
                const read_remote_head = @fieldParentPtr(Read, "pending", read_remote_head_pending);
                assert(read_remote_head.callback == .remote);

                log.debug("{}: read_block: fault: address={} checksum={}", .{
                    grid.superblock.replica(),
                    read_remote_head.address,
                    read_remote_head.checksum,
                });

                read_remote_head.resolves = read_remote_resolves;
                grid.read_remote_queue.push(read_remote_head);

                if (grid.repair_queue.request_blocks_available() > 0) {
                    grid.repair_queue.request_block(
                        read_remote_head.address,
                        read_remote_head.checksum,
                    );
                }
            }
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return (address - 1) * block_size;
        }

        fn verify_read(grid: *Grid, address: u64, cached_block: BlockPtrConst) void {
            assert(constants.verify);

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            const actual_block = grid.superblock.storage.grid_block(address).?;
            assert(std.mem.eql(u8, cached_block, actual_block));
        }
    };
}
