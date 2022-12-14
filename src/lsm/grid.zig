const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const free_set = @import("../vsr/superblock_free_set.zig");

const SuperBlockType = vsr.SuperBlockType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const NodePool = @import("node_pool.zig").NodePool;
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;
const util = @import("../util.zig");

const log = std.log.scoped(.grid);

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

    pub inline fn from(vsr_operation: vsr.Operation) BlockType {
        return @intToEnum(BlockType, @enumToInt(vsr_operation));
    }

    pub inline fn operation(block_type: BlockType) vsr.Operation {
        return @intToEnum(vsr.Operation, @enumToInt(block_type));
    }
};

/// The Grid provides access to on-disk blocks (blobs of `block_size` bytes).
/// Each block is identified by an "address" (`u64`, beginning at 1).
///
/// Recently/frequently-used blocks are transparently cached in memory.
pub fn GridType(comptime Storage: type) type {
    const block_size = constants.block_size;
    const SuperBlock = SuperBlockType(Storage);

    const cache_interface = struct {
        inline fn address_from_block(block: *const *const [block_size]u8) u64 {
            const header_bytes = block.*[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);
            const address = header.op;
            assert(address > 0);
            return address;
        }

        inline fn hash_address(address: u64) u64 {
            assert(address > 0);
            return std.hash.Wyhash.hash(0, mem.asBytes(&address));
        }

        inline fn equal_addresses(a: u64, b: u64) bool {
            return a == b;
        }
    };

    return struct {
        const Grid = @This();

        pub const read_iops_max = 15;

        // TODO put more thought into how low/high this limit should be.
        pub const write_iops_max = 16;

        pub const BlockPtr = *align(constants.sector_size) [block_size]u8;
        pub const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;
        pub const Reservation = free_set.Reservation;

        pub const Write = struct {
            callback: fn (*Grid.Write) void,
            address: u64,
            block: BlockPtrConst,

            /// Link for the write_queue linked list.
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

            /// Link for read_queue/read_recovery_queue/ReadIOP.reads linked lists.
            next: ?*Read = null,
        };

        const ReadIOP = struct {
            grid: *Grid,
            completion: Storage.Read,
            reads: FIFO(Read) = .{},
            /// This is a pointer to a value in the block cache.
            block: BlockPtr,
        };

        const CacheBlocks = NodePool(block_size, constants.sector_size);

        const set_associative_cache_ways = 16;
        const Cache = SetAssociativeCache(
            u64,
            BlockPtr,
            cache_interface.address_from_block,
            cache_interface.hash_address,
            cache_interface.equal_addresses,
            .{
                .ways = set_associative_cache_ways,
                .value_alignment = @alignOf(BlockPtrConst),
            },
        );

        superblock: *SuperBlock,

        cache_blocks: CacheBlocks,
        cache: Cache,

        write_iops: IOPS(WriteIOP, write_iops_max) = .{},
        write_queue: FIFO(Write) = .{},

        /// `read_iops` maintains a list of ReadIOPs currently performing storage.read_sector() on
        /// a unique address.
        ///
        /// Invariants:
        /// * An address is listed in `read_iops` at most once. Multiple reads of the same address
        ///   (past or present) are coalesced.
        read_iops: IOPS(ReadIOP, read_iops_max) = .{},
        read_queue: FIFO(Read) = .{},

        /// Reads that were found to be in the cache on start_read() and queued to be resolved on
        /// the next tick(). This keeps read_block() always asynchronous to the caller.
        read_cached_queue: FIFO(Read) = .{},
        // TODO interrogate this list and do recovery in Replica.tick().
        read_recovery_queue: FIFO(Read) = .{},

        pub fn init(allocator: mem.Allocator, superblock: *SuperBlock) !Grid {
            // TODO Determine this at runtime based on runtime configured maximum
            // memory usage of tigerbeetle.
            const blocks_in_cache = 2048;

            var cache_blocks = try CacheBlocks.init(
                allocator,
                blocks_in_cache +
                    // Additional blocks for in-flight reads that have not yet been added to the cache.
                    read_iops_max +
                    // We acquire a block, evict a cache entry and release the evicted block.
                    // So we need 1 extra block to cover that period.
                    // TODO This could be avoided by tweaking the cache interface to evict first.
                    1,
            );
            errdefer cache_blocks.deinit(allocator);

            var cache = try Cache.init(allocator, blocks_in_cache);
            errdefer cache.deinit(allocator);

            return Grid{
                .superblock = superblock,
                .cache_blocks = cache_blocks,
                .cache = cache,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            grid.cache.deinit(allocator);
            grid.cache_blocks.release_all_and_deinit(allocator);

            grid.* = undefined;
        }

        pub fn tick(grid: *Grid) void {
            // Resolve reads that were seen in the cache during start_read()
            // but deferred to be asynchronously resolved on the next tick.
            //
            // Drain directly from the queue so that new cache reads (added upon completion of old
            // cache reads) that can be serviced immediately aren't deferred until the next tick
            // (which may be milliseconds later due to IO.run_for_ns). This is necessary to ensure
            // that groove prefetch completes promptly.
            //
            // Even still, we cap the reads processed to prevent going over
            // any implicit time slice expected of Grid.tick(). This limit is fairly arbitrary.
            var retry_max: u32 = 100_000;
            while (grid.read_cached_queue.pop()) |read| {
                if (grid.cache.get(read.address)) |block| {
                    if (constants.verify) grid.verify_cached_read(read.address, block.*);
                    read.callback(read, block.*);
                } else {
                    grid.start_read(read);
                }

                retry_max -= 1;
                if (retry_max == 0) break;
            }
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
            grid.assert_not_writing(address, null);
            grid.assert_not_reading(address, null);

            grid.cache.demote(address);
            grid.superblock.free_set.release(address);
        }

        /// Assert that the address is not currently being written to.
        /// Assert that the block pointer is not being used for any write if non-null.
        fn assert_not_writing(grid: *Grid, address: u64, block: ?BlockPtrConst) void {
            assert(address > 0);
            {
                var it = grid.write_queue.peek();
                while (it) |pending_write| : (it = pending_write.next) {
                    assert(address != pending_write.address);
                    assert(block != pending_write.block);
                }
            }
            {
                var it = grid.write_iops.iterate();
                while (it.next()) |iop| {
                    assert(address != iop.write.address);
                    assert(block != iop.write.block);
                }
            }
        }

        /// Assert that the address is not currently being read from.
        /// Assert that the block pointer is not being used for any read if non-null.
        fn assert_not_reading(grid: *Grid, address: u64, block: ?BlockPtrConst) void {
            assert(address > 0);
            for ([_]FIFO(Read){
                grid.read_queue,
                grid.read_cached_queue,
                grid.read_recovery_queue,
            }) |queue| {
                var it = queue.peek();
                while (it) |pending_read| : (it = pending_read.next) {
                    assert(address != pending_read.address);
                }
            }
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    const iop_read = iop.reads.peek() orelse continue;
                    assert(address != iop_read.address);
                    assert(block != iop.block);
                }
            }
        }

        pub fn write_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: BlockPtrConst,
            address: u64,
        ) void {
            assert(grid.superblock.opened);
            assert(address > 0);
            assert(!grid.superblock.free_set.is_free(address));
            grid.assert_not_writing(address, block);
            grid.assert_not_reading(address, block);

            write.* = .{
                .callback = callback,
                .address = address,
                .block = block,
            };

            const initial_iops_available = grid.write_iops.available();
            if (initial_iops_available > 0) {
                assert(grid.write_queue.empty());
            }

            grid.start_write(write);

            if (initial_iops_available > 0) {
                assert(grid.write_iops.available() == initial_iops_available - 1);
            }
        }

        fn start_write(grid: *Grid, write: *Write) void {
            grid.assert_not_writing(write.address, write.block);
            grid.assert_not_reading(write.address, write.block);

            const iop = grid.write_iops.acquire() orelse {
                grid.write_queue.push(write);
                return;
            };

            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .write = write,
            };

            grid.superblock.storage.write_sectors(
                write_block_callback,
                &iop.completion,
                write.block,
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

            const cached_block = grid.cache_blocks.acquire();
            util.copy_disjoint(.exact, u8, cached_block, completed_write.block);
            assert(cache_interface.address_from_block(&cached_block) == completed_write.address);
            const evicted_blocks = grid.cache.insert(&cached_block);
            for (evicted_blocks) |evicted_block| {
                if (evicted_block) |block_ptr| {
                    grid.cache_blocks.release(block_ptr);
                }
            }

            grid.write_iops.release(iop);

            // Start a queued write if possible *before* calling the completed
            // write's callback. This ensures that if the callback calls
            // Grid.write_block() it doesn't preempt the queue.
            if (grid.write_queue.pop()) |queued_write| {
                const initial_iops_available = grid.write_iops.available();
                assert(initial_iops_available > 0);
                grid.start_write(queued_write);
                assert(grid.write_iops.available() == initial_iops_available - 1);
            }

            // This call must come after releasing the IOP. Otherwise we risk tripping
            // assertions forbidding concurrent writes using the same block/address
            // if the callback calls write_block().
            completed_write.callback(completed_write);
        }

        /// This function transparently handles recovery if the checksum fails.
        /// If necessary, this read will be added to a linked list, which Replica can then
        /// interrogate each tick(). The callback passed to this function won't be called until the
        /// block has been recovered.
        pub fn read_block(
            grid: *Grid,
            callback: fn (*Grid.Read, BlockPtrConst) void,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
            block_type: BlockType,
        ) void {
            assert(grid.superblock.opened);
            assert(address > 0);
            assert(block_type != .reserved);

            grid.assert_not_writing(address, null);
            assert(!grid.superblock.free_set.is_free(address));

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .block_type = block_type,
            };

            grid.start_read(read);
        }

        fn start_read(grid: *Grid, read: *Grid.Read) void {
            grid.assert_not_writing(read.address, null);

            if (grid.superblock.free_set.is_free(read.address)) {
                // We cannot assert `free_set.is_free()` because of the following case:
                // 1. The replica receives a request_block from a repairing replica.
                //    The block is allocated but not cached — but is due to be freed at checkpoint.
                // 2. All of the Grid's Read IOPS are occupied, so queue the read.
                // 3. The replica checkpoints.
                // 4. The read dequeues, but the requested block is no longer allocated.
                // TODO(State Transfer):
                // 1. If a local read results in a fault, then the replica should attempt a
                //    remote read.
                // 2. If a remote replica has the block then it responds (and the local read
                //    completes), otherwise it nacks.
                // 3. If we receive too many nacks or if we get the feeling that we are too far
                //    behind (perhaps the primary nacks), then complete the read callback but now
                //    with a null result, so that it unwinds the stack all the way back to VSR,
                //    which then initiates state transfer. At present, we expect that reads always
                //    return a block, so to support this bubbling up, we'll need to make the block
                //    result optional.
                unreachable;
            }

            // Check if a read is already in progress for the target address.
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    const iop_read = iop.reads.peek() orelse continue;
                    if (iop_read.address == read.address) {
                        assert(iop_read.checksum == read.checksum);
                        iop.reads.push(read);
                        return;
                    }
                }
            }

            // If the block is already in the cache, queue up the read to be resolved
            // from the cache on the next tick. This keeps start_read() asynchronous.
            // Note that this must be called after we have checked for an in
            // progress read targeting the same address, otherwise we may read an
            // unininitialized block.
            if (grid.cache.exists(read.address)) {
                grid.read_cached_queue.push(read);
                return;
            }

            const iop = grid.read_iops.acquire() orelse {
                grid.read_queue.push(read);
                return;
            };

            const block = grid.cache_blocks.acquire();

            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .block = block,
            };

            // Collect the current Read and any other pending Reads for the same address to this IOP.
            // If we didn't gather them here, they would eventually be processed at the end of
            // read_block_callback(), but that would issue a new call to read_sectors().
            iop.reads.push(read);
            {
                // Make a copy here to avoid an infinite loop from pending_reads being
                // re-added to read_queue after not matching the current read.
                var copy = grid.read_queue;
                grid.read_queue = .{};
                while (copy.pop()) |pending_read| {
                    if (pending_read.address == read.address) {
                        assert(pending_read.checksum == read.checksum);
                        iop.reads.push(pending_read);
                    } else {
                        grid.read_queue.push(pending_read);
                    }
                }
            }

            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                iop.block,
                .grid,
                block_offset(read.address),
            );
        }

        inline fn block_locked(grid: *Grid, block: BlockPtrConst) bool {
            var it = grid.read_iops.iterate();
            while (it.next()) |iop| {
                if (block == iop.block) return true;
            }
            return false;
        }

        fn read_block_callback(completion: *Storage.Read) void {
            const iop = @fieldParentPtr(ReadIOP, "completion", completion);
            const grid = iop.grid;

            const address = iop.reads.peek().?.address;
            const checksum = iop.reads.peek().?.checksum;
            const block_type = iop.reads.peek().?.block_type;

            assert(cache_interface.address_from_block(&iop.block) == address);
            const evicted_blocks = grid.cache.insert(&iop.block);
            for (evicted_blocks) |evicted_block| {
                if (evicted_block) |block_ptr| {
                    grid.cache_blocks.release(block_ptr);
                }
            }

            const header_bytes = iop.block[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);

            const checksum_valid = header.valid_checksum();
            const checksum_body_valid = checksum_valid and
                header.valid_checksum_body(iop.block[@sizeOf(vsr.Header)..header.size]);
            const checksum_match = header.checksum == checksum;

            if (checksum_valid and checksum_body_valid and checksum_match) {
                assert(header.op == address);
                assert(header.operation == block_type.operation());

                // NOTE: read callbacks resolved here could queue up reads into this very iop.
                // This extends this while loop, but that's fine as it keeps the callbacks
                // asynchronous to themselves (preventing something like a stack-overflow).
                while (iop.reads.pop()) |read| {
                    assert(read.address == address);
                    assert(read.checksum == checksum);
                    assert(read.block_type == BlockType.from(header.operation));
                    read.callback(read, iop.block);
                }
            } else {
                if (!checksum_valid) {
                    log.err("invalid checksum at address {}", .{address});
                } else if (!checksum_body_valid) {
                    log.err("invalid checksum body at address {}", .{address});
                } else if (!checksum_match) {
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
                } else {
                    unreachable;
                }

                // IOP reads that fail checksum validation get punted to a recovery queue.
                // TODO: Have the replica do something with the pending reads here.
                while (iop.reads.pop()) |read| {
                    iop.grid.read_recovery_queue.push(read);
                }
            }

            grid.read_iops.release(iop);

            // Always iterate through the full list of pending reads instead of just one to ensure
            // that those serviced from the cache don't prevent others waiting for an IOP from
            // seeing the IOP that was just released.
            var copy = grid.read_queue;
            grid.read_queue = .{};
            while (copy.pop()) |read| {
                assert(read.address != address);
                grid.start_read(read);
            }
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return (address - 1) * block_size;
        }

        fn verify_cached_read(grid: *Grid, address: u64, cached_block: BlockPtrConst) void {
            if (Storage != @import("../test/storage.zig").Storage)
                // Too complicated to do async verification
                return;

            const actual_block = grid.superblock.storage.grid_block(address);
            assert(std.mem.eql(u8, cached_block, actual_block));
        }
    };
}
