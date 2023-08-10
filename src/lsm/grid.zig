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

        pub const BlockPtr = *align(constants.sector_size) [block_size]u8;
        pub const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;
        pub const Reservation = free_set.Reservation;

        // Grid just reuses the Storage's NextTick abstraction for simplicity.
        pub const NextTick = Storage.NextTick;

        pub const Write = struct {
            callback: fn (*Grid.Write) void,
            address: u64,
            repair: bool,
            block: *BlockPtr,
            /// The current checkpoint when the write began.
            /// Used to verify that the checkpoint does not advance while the write is in progress.
            checkpoint_id: u128,

            /// Link for the Grid.write_queue linked list.
            next: ?*Write = null,
        };

        const WriteIOP = struct {
            grid: *Grid,
            completion: Storage.Write,
            write: *Write,
        };

        pub const Read = struct {
            callback: fn (*Grid.Read, BlockPtrConst) void,
            address: u64,
            checksum: u128,
            block_type: BlockType,
            /// The current checkpoint when the read began.
            /// Used to verify that the checkpoint does not advance while the read is in progress.
            checkpoint_id: u128,

            read_cache: bool,
            pending: ReadPending = .{},
            resolves: FIFO(ReadPending) = .{ .name = null },

            grid: *Grid,
            next_tick: Grid.NextTick = undefined,

            /// Link for Grid.read_queue/Grid.read_faulty_queue linked lists.
            next: ?*Read = null,
        };

        pub const ReadRepair = struct {
            callback: fn (*Grid.ReadRepair, error{BlockNotFound}!void) void,
            address: u64,
            checksum: u128,
            block: BlockPtr,
            grid: *Grid,
            next_tick: Grid.NextTick = undefined,
            completion: Storage.Read = undefined,
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

        cache: Cache,
        /// Each entry in cache has a corresponding block.
        cache_blocks: []BlockPtr,
        /// Each entry in cache has a corresponding bit.
        /// This bit tracks whether the cache block is definitely valid relative to the current
        /// checkpoint (matches what should be on disk). At the conclusion of state sync, all blocks
        /// are marked as invalid. This enables additional validation of cache consistency.
        cache_coherent: std.DynamicBitSetUnmanaged,

        write_iops: IOPS(WriteIOP, write_iops_max) = .{},
        write_iop_tracer_slots: [write_iops_max]?tracer.SpanStart = .{null} ** write_iops_max,
        write_queue: FIFO(Write) = .{ .name = "grid_write" },

        // Each read_iops has a corresponding block.
        read_iop_blocks: [read_iops_max]BlockPtr,
        read_iops: IOPS(ReadIOP, read_iops_max) = .{},
        read_iop_tracer_slots: [read_iops_max]?tracer.SpanStart = .{null} ** read_iops_max,
        read_queue: FIFO(Read) = .{ .name = "grid_read" },

        // List if Read.pending's which are in `read_queue` but also waiting for a free `read_iops`.
        read_pending_queue: FIFO(ReadPending) = .{ .name = "grid_read_pending" },
        read_faulty_queue: FIFO(Read) = .{ .name = "grid_read_faulty" },
        // True if there's a read that is resolving callbacks.
        // If so, the read cache must not be invalidated.
        read_resolving: bool = false,

        canceling: ?struct { callback: fn (*Grid) void } = null,
        canceling_tick_context: NextTick = undefined,

        on_read_fault: ?fn (*Grid, *Grid.Read) void,

        pub fn init(allocator: mem.Allocator, options: struct {
            superblock: *SuperBlock,
            cache_blocks_count: u64 = Cache.value_count_max_multiple,
            on_read_fault: ?fn (*Grid, *Grid.Read) void = null,
        }) !Grid {
            const cache_blocks = try allocator.alloc(BlockPtr, options.cache_blocks_count);
            errdefer allocator.free(cache_blocks);

            var cache_coherent =
                try std.DynamicBitSetUnmanaged.initEmpty(allocator, options.cache_blocks_count);
            errdefer cache_coherent.deinit(allocator);

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
                .cache = cache,
                .cache_blocks = cache_blocks,
                .cache_coherent = cache_coherent,
                .read_iop_blocks = read_iop_blocks,
                .on_read_fault = options.on_read_fault,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            for (&grid.read_iop_blocks) |block| allocator.free(block);

            for (grid.read_iop_tracer_slots) |slot| assert(slot == null);
            for (grid.write_iop_tracer_slots) |slot| assert(slot == null);

            for (grid.cache_blocks) |block| allocator.free(block);
            allocator.free(grid.cache_blocks);

            grid.cache_coherent.deinit(allocator);
            grid.cache.deinit(allocator);

            grid.* = undefined;
        }

        pub fn cancel(grid: *Grid, callback: fn (*Grid) void) void {
            assert(grid.canceling == null);

            grid.canceling = .{ .callback = callback };

            grid.read_queue.reset();
            grid.read_pending_queue.reset();
            grid.read_faulty_queue.reset();
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

            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_faulty_queue.empty());
            assert(grid.write_queue.empty());

            grid.cancel_join_callback();
        }

        fn cancel_join_callback(grid: *Grid) void {
            assert(grid.canceling != null);
            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_faulty_queue.empty());
            assert(grid.write_queue.empty());

            if (grid.read_iops.executing() == 0 and
                grid.write_iops.executing() == 0)
            {
                const callback = grid.canceling.?.callback;
                grid.canceling = null;

                callback(grid);
            }
        }

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
            return grid.superblock.free_set.reserve(blocks_count);
        }

        /// Forfeit a reservation.
        pub fn forfeit(grid: *Grid, reservation: Reservation) void {
            return grid.superblock.free_set.forfeit(reservation);
        }

        /// Returns a just-allocated block.
        /// The caller is responsible for not acquiring more blocks than they reserved.
        pub fn acquire(grid: *Grid, reservation: Reservation) u64 {
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
            assert(grid.writing(address, null) != .init);
            // It's safe to release an address that is being read from,
            // because the superblock will not allow it to be overwritten before
            // the end of the measure.

            grid.cache.demote(address);
            grid.superblock.free_set.release(address);
        }

        pub fn faulty(grid: *Grid, address: u64, checksum: ?u128) bool {
            assert(address > 0);

            var it = grid.read_faulty_queue.peek();
            while (it) |faulty_read| : (it = faulty_read.next) {
                if (faulty_read.address == address) {
                    assert(grid.cache.get_index(address) == null);
                    assert(!grid.superblock.free_set.is_free(address));

                    if (checksum == null or checksum.? == faulty_read.checksum) {
                        return true;
                    }
                }
            }
            return false;
        }

        const Writing = enum { init, repair, none };

        /// If the address is being written to by a non-repair, return `.init`.
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
                        result = if (queued_write.repair) .repair else .init;
                    }
                }
            }
            {
                var it = grid.write_iops.iterate();
                while (it.next()) |iop| {
                    assert(block != iop.write.block.*);
                    if (address == iop.write.address) {
                        assert(result == .none);
                        result = if (iop.write.repair) .repair else .init;
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
                &grid.read_faulty_queue,
            }) |queue| {
                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    assert(address != queued_read.address);
                }
            }
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    assert(address != iop.read.address);
                    const iop_block = grid.read_iop_blocks[grid.read_iops.index(iop)];
                    assert(block != iop_block);
                }
            }
        }

        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn write_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
            address: u64,
        ) void {
            assert(address > 0);
            assert(grid.canceling == null);
            assert(grid.writing(address, block.*) == .none);
            grid.assert_not_reading(address, block.*);

            if (constants.verify) {
                for (grid.cache_blocks) |cache_block| {
                    assert(cache_block != block.*);
                }
            }

            assert(grid.superblock.opened);
            assert(!grid.superblock.free_set.is_free(address));

            write.* = .{
                .callback = callback,
                .address = address,
                .repair = false,
                .block = block,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
            };

            const iop = grid.write_iops.acquire() orelse {
                grid.write_queue.push(write);
                return;
            };

            grid.write_block_with(iop, write);
        }

        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn write_block_repair(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
            address: u64,
        ) void {
            const header = schema.header_from_block(block.*);

            assert(address > 0);
            assert(address == header.op);
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(!grid.superblock.free_set.is_free(address));
            assert(grid.faulty(address, header.checksum));
            assert(grid.writing(address, block.*) == .none);

            write.* = .{
                .callback = callback,
                .address = address,
                .repair = true,
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
            assert(grid.superblock.working.checkpoint_id() == completed_write.checkpoint_id);

            // Insert the write block into the cache, and give the evicted block to the writer.
            const cache_index = grid.cache.insert_index(&completed_write.address);
            const cache_block = &grid.cache_blocks[cache_index];
            std.mem.swap(BlockPtr, cache_block, completed_write.block);
            std.mem.set(u8, completed_write.block.*, 0);
            grid.cache_coherent.set(cache_index);

            const write_iop_index = grid.write_iops.index(iop);
            tracer.end(
                &grid.write_iop_tracer_slots[write_iop_index],
                .{ .grid_write_iop = .{ .index = write_iop_index } },
            );

            if (grid.canceling) |_| {
                grid.write_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            if (completed_write.repair) {
                // We wait until the write completes to resolve the repair queue, to prevent
                // these writes from ever overlapping with compaction or checkpoints.
                const header = schema.header_from_block(cache_block.*);

                var read_ = grid.read_faulty_queue.peek();
                while (read_) |read| : (read_ = read.next) {
                    if (read.checksum == header.checksum and
                        read.address == completed_write.address)
                    {
                        grid.read_faulty_queue.remove(read);
                        grid.read_block_resolve(read, cache_block.*);
                        break;
                    }
                } else {
                    unreachable;
                }
            }

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
        }

        /// Fetch the block synchronously from cache, if possible.
        /// The returned block pointer is only valid until the next Grid write.
        pub fn read_block_from_cache(grid: *Grid, address: u64, checksum: u128) ?BlockPtrConst {
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(!grid.superblock.free_set.is_free(address));
            assert(grid.writing(address, null) != .init);

            assert(address > 0);

            const cache_index = grid.cache.get_index(address) orelse return null;
            const cache_block = grid.cache_blocks[cache_index];

            const header = schema.header_from_block(cache_block);
            assert(header.op == address);

            if (header.checksum == checksum) {
                if (constants.verify) grid.verify_read(address, cache_block);

                grid.cache_coherent.set(cache_index);
                return cache_block;
            } else {
                assert(!grid.cache_coherent.isSet(cache_index));
                return null;
            }
        }

        pub fn read_block_from_cache_or_storage(
            grid: *Grid,
            callback: fn (*Grid.Read, BlockPtrConst) void,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
            block_type: BlockType,
        ) void {
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(!grid.superblock.free_set.is_free(address));
            assert(grid.writing(address, null) != .init);

            assert(address > 0);
            assert(block_type != .reserved);

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .block_type = block_type,
                .read_cache = true,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
                .grid = grid,
            };

            grid.on_next_tick(read_block_tick_callback, &read.next_tick);
        }

        fn read_block_tick_callback(next_tick: *Storage.NextTick) void {
            const read = @fieldParentPtr(Grid.Read, "next_tick", next_tick);
            const grid = read.grid;

            grid.read_block(read);
        }

        /// This function transparently handles recovery if the checksum fails.
        /// If necessary, this read will be added to a linked list, which Replica can then
        /// interrogate each tick(). The callback passed to this function won't be called until the
        /// block has been recovered.
        pub fn read_block_from_storage(
            grid: *Grid,
            callback: fn (*Grid.Read, BlockPtrConst) void,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
            block_type: BlockType,
        ) void {
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(!grid.superblock.free_set.is_free(address));
            assert(grid.writing(address, null) != .init);

            assert(address > 0);
            assert(block_type != .reserved);

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .block_type = block_type,
                .read_cache = false,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
                .grid = grid,
            };

            grid.read_block(read);
        }

        fn read_block(grid: *Grid, read: *Grid.Read) void {
            assert(grid.superblock.opened);
            assert(grid.canceling == null);
            assert(!grid.superblock.free_set.is_free(read.address));
            assert(grid.writing(read.address, null) != .init);

            assert(read.address > 0);
            assert(read.block_type != .reserved);

            // Check if a read is already processing/recovering and merge with it.
            for ([_]*const FIFO(Read){
                &grid.read_queue,
                &grid.read_faulty_queue,
            }) |queue| {
                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    if (queued_read.address == read.address) {
                        assert(queued_read.checksum == read.checksum);
                        assert(queued_read.block_type == read.block_type);
                        queued_read.resolves.push(&read.pending);
                        return;
                    }
                }
            }

            // When Read.read_cache is set, the caller of read_block() is responsible for calling
            // us via next_tick().
            if (read.read_cache) {
                if (grid.read_block_from_cache(read.address, read.checksum)) |cache_block| {
                    grid.read_block_resolve(read, cache_block);
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
            const iop_block = &grid.read_iop_blocks[grid.read_iops.index(iop)];

            if (grid.canceling) |_| {
                grid.read_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            // Insert the block into the cache, and give the evicted block to `iop`.
            const cache_index = grid.cache.insert_index(&read.address);
            const cache_block = &grid.cache_blocks[cache_index];
            std.mem.swap(BlockPtr, iop_block, cache_block);
            std.mem.set(u8, iop_block.*, 0);
            grid.cache_coherent.set(cache_index);

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

            // A valid block filled by storage means the reads for the address can be resolved.
            if (read_block_valid(cache_block.*, .{
                .address = read.address,
                .checksum = read.checksum,
                .block_type = read.block_type,
            })) {
                grid.read_block_resolve(read, cache_block.*);
                return;
            }

            // Don't cache a corrupt or incorrect block.
            grid.cache.remove(read.address);
            grid.cache_coherent.unset(cache_index);

            // On the result of an invalid block, move the "root" read (and all others it
            // resolves) to recovery queue. Future reads on the same address will see the "root"
            // read in the recovery queue and enqueue to it.
            grid.read_faulty_queue.push(read);
            if (grid.on_read_fault) |on_read_fault| {
                on_read_fault(grid, read);
            } else {
                @panic("Grid.on_read_fault not set");
            }
        }

        fn read_block_valid(block: BlockPtrConst, expect: struct {
            address: u64,
            checksum: u128,
            block_type: ?BlockType,
        }) bool {
            const address = expect.address;
            const checksum = expect.checksum;
            const block_type = expect.block_type;

            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);

            if (!header.valid_checksum()) {
                log.err("invalid checksum at address {} (expected={})", .{ address, checksum });
                return false;
            }

            if (!header.valid_checksum_body(block[@sizeOf(vsr.Header)..header.size])) {
                log.err("invalid checksum body at address {} (expected={})", .{ address, checksum });
                return false;
            }

            if (header.checksum != checksum) {
                log.err(
                    "expected address={} checksum={} block_type={}, " ++
                        "found address={} checksum={} block_type={}",
                    .{
                        address,
                        checksum,
                        block_type,
                        header.op,
                        header.checksum,
                        @enumToInt(header.operation),
                    },
                );
                return false;
            }

            assert(header.op == address);
            if (block_type) |block_type_| {
                assert(header.operation == block_type_.operation());
            }
            return true;
        }

        fn read_block_resolve(grid: *Grid, read: *Grid.Read, block: BlockPtrConst) void {
            // Guard to make sure the cache cannot be updated by any read.callbacks() below.
            assert(!grid.read_resolving);
            grid.read_resolving = true;
            defer {
                assert(grid.read_resolving);
                grid.read_resolving = false;
            }

            assert(read.checkpoint_id == grid.superblock.working.checkpoint_id());

            const header = schema.header_from_block(block);
            assert(header.op == read.address);
            assert(header.checksum == read.checksum);

            // Resolve all reads queued to the address with the block.
            while (read.resolves.pop()) |pending| {
                const pending_read = @fieldParentPtr(Read, "pending", pending);
                assert(pending_read.address == read.address);
                assert(pending_read.checksum == read.checksum);
                assert(pending_read.checkpoint_id == grid.superblock.working.checkpoint_id());

                pending_read.callback(pending_read, block);
            }

            // Then invoke the callback with the cache block (which should be valid for the duration
            // of the callback as any nested Grid calls cannot synchronously update the cache).
            read.callback(read, block);
        }

        /// If the block is not present (or corrupt), we do not attempt to repair it — the repair's
        /// address/checksum is not assumed to match what "should" be in our block.
        /// Relatedly we still attempt to read free blocks — they might still hold the requested data.
        ///
        /// Even though this block is the block we expected to read, we can't safely
        /// cache this result:
        /// 1. Replica A writes block X₁ to address X.
        /// 2. Replica A writes block X₂ to address X (write is lost/misdirected).
        /// 3. Replica B requests block X₁ from replica A.
        /// It is safe for A to send back X₁, but A must not allow it to poison its cache.
        ///
        /// Additionally, we don't cache these reads for performance reasons:
        /// - it would fill the cache with non-temporally-local blocks, and
        /// - it would force an extra memcpy.
        pub fn read_block_repair(
            grid: *Grid,
            callback: fn (*Grid.ReadRepair, error{BlockNotFound}!void) void,
            read: *Grid.ReadRepair,
            block: BlockPtr,
            address: u64,
            checksum: u128,
        ) void {
            assert(address > 0);

            assert(grid.superblock.opened);
            maybe(grid.canceling == null);
            // We try to read the block even when it is free — if we recently released it,
            // it might be found on disk anyway.
            maybe(grid.superblock.free_set.is_free(address));
            // The caller will not attempt to help another replica repair a block that
            // we are already trying to repair ourselves.
            assert(!grid.faulty(address, null));
            maybe(grid.writing(address, null) == .init);

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .block = block,
                .grid = grid,
            };

            if (grid.cache.get_index(read.address)) |cache_index| {
                const cache_block = grid.cache_blocks[cache_index];

                const header = schema.header_from_block(cache_block);
                assert(header.op == read.address);

                if (grid.cache_coherent.isSet(cache_index)) {
                    if (constants.verify) grid.verify_read(read.address, cache_block);
                }

                if (header.checksum == read.checksum) {
                    stdx.copy_disjoint(.inexact, u8, read.block, cache_block[0..header.size]);
                } else {
                    // Signal to read_block_repair_tick_callback() that we found a block,
                    // but not the one we wanted.
                    std.mem.set(u8, read.block[0..header.size], 0);
                }

                if (header.checksum == read.checksum or
                    grid.cache_coherent.isSet(cache_index))
                {
                    // Use "vsr" on_next_tick() because Grid.cancel() can run concurrently with repairs.
                    grid.superblock.storage.on_next_tick(
                        .vsr,
                        read_block_repair_tick_callback,
                        &read.next_tick,
                    );
                    return;
                } else {
                    // We have a cached entry (wrong block), but the cache is not coherent.
                    // The block we actually want may be on disk.
                }
            }

            grid.superblock.storage.read_sectors(
                read_block_repair_callback,
                &read.completion,
                read.block,
                .grid,
                block_offset(read.address),
            );
        }

        fn read_block_repair_tick_callback(next_tick: *Grid.NextTick) void {
            const read = @fieldParentPtr(ReadRepair, "next_tick", next_tick);
            const header = mem.bytesAsValue(vsr.Header, read.block[0..@sizeOf(vsr.Header)]);

            if (header.op > 0) {
                read.callback(read, {});
            } else {
                read.callback(read, error.BlockNotFound);
            }
        }

        fn read_block_repair_callback(completion: *Storage.Read) void {
            const read = @fieldParentPtr(ReadRepair, "completion", completion);

            if (read_block_valid(read.block, .{
                .address = read.address,
                .checksum = read.checksum,
                .block_type = null,
            })) {
                read.callback(read, {});
            } else {
                read.callback(read, error.BlockNotFound);
            }
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return (address - 1) * block_size;
        }

        fn verify_read(grid: *Grid, address: u64, cached_block: BlockPtrConst) void {
            if (Storage != @import("../testing/storage.zig").Storage)
                // Too complicated to do async verification
                return;

            const actual_block = grid.superblock.storage.grid_block(address);
            assert(std.mem.eql(u8, cached_block, actual_block));
        }
    };
}
