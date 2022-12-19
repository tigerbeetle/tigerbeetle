const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const free_set = @import("../vsr/superblock_free_set.zig");

const SuperBlockType = vsr.SuperBlockType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
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
        inline fn address_from_block(block: *const [block_size]u8) u64 {
            const header_bytes = block[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);
            const address = header.op;
            assert(address > 0);
            return address;
        }

        inline fn set_address(block: *[block_size]u8, address: u64) void {
            const header = mem.toBytes(vsr.Header{
                .op = address,
                .cluster = undefined,
                .command = undefined,
            });
            const header_bytes = block[0..@sizeOf(vsr.Header)];
            header_bytes.* = header;
        }

        inline fn hash_address(address: u64) u64 {
            assert(address > 0);
            return std.hash.Wyhash.hash(0, mem.asBytes(&address));
        }

        inline fn equal_addresses(a: u64, b: u64) bool {
            return a == b;
        }
    };

    const set_associative_cache_ways = 16;
    const Cache = SetAssociativeCache(
        u64,
        [block_size]u8,
        cache_interface.address_from_block,
        cache_interface.hash_address,
        cache_interface.equal_addresses,
        .{
            .ways = set_associative_cache_ways,
            .value_alignment = constants.sector_size,
        },
    );

    return struct {
        const Grid = @This();

        pub const read_iops_max = 15;
        comptime {
            // This + 1 ensures that it is always possible for writes to add the written block
            // to the cache on completion, even if the maximum number of concurrent reads are in
            // progress and have locked all but one way in the target set.
            assert(read_iops_max + 1 <= set_associative_cache_ways);
        }

        // TODO put more thought into how low/high this limit should be.
        pub const write_iops_max = 16;

        pub const BlockPtr = *align(constants.sector_size) [block_size]u8;
        pub const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;
        pub const Reservation = free_set.Reservation;

        // Grid just reuses the Storage's NextTick abstraction for simplicity.
        pub const NextTick = Storage.NextTick;

        pub const Write = struct {
            callback: fn (*Grid.Write) void,
            address: u64,
            block: BlockPtrConst,

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

            pending: ReadPending = .{},
            resolves: FIFO(ReadPending) = .{},

            grid: *Grid,
            next_tick: Grid.NextTick = undefined,

            /// Link for Grid.read_queue/Grid.read_recovery_queue linked lists.
            next: ?*Read = null,
        };

        const ReadPending = struct {
            /// Link for Read.resolves linked lists.
            next: ?*ReadPending = null,
        };

        const ReadIOP = struct {
            completion: Storage.Read,
            read: *Read,
            block: BlockPtr,
        };

        superblock: *SuperBlock,
        cache: Cache,

        write_iops: IOPS(WriteIOP, write_iops_max) = .{},
        write_queue: FIFO(Write) = .{},

        read_iops: IOPS(ReadIOP, read_iops_max) = .{},
        read_queue: FIFO(Read) = .{},

        // List if Read.pending's which are in `read_queue` but also waiting for a free `read_iops`.
        read_pending_queue: FIFO(ReadPending) = .{},
        // TODO interrogate this list and do recovery in Replica.tick().
        read_recovery_queue: FIFO(Read) = .{},
        // True if there's a read thats resolving callbacks. If so, the read cache must not be invalidated.
        read_resolving: bool = false,

        pub fn init(allocator: mem.Allocator, superblock: *SuperBlock) !Grid {
            // TODO Determine this at runtime based on runtime configured maximum
            // memory usage of tigerbeetle.
            const blocks_in_cache = 2048;

            var cache = try Cache.init(allocator, blocks_in_cache);
            errdefer cache.deinit(allocator);

            return Grid{
                .superblock = superblock,
                .cache = cache,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            grid.cache.deinit(allocator);
            grid.* = undefined;
        }

        pub fn on_next_tick(
            grid: *Grid,
            callback: fn (*Grid.NextTick) void,
            next_tick: *Grid.NextTick,
        ) void {
            grid.superblock.storage.on_next_tick(callback, next_tick);
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
                while (it) |queued_write| : (it = queued_write.next) {
                    assert(address != queued_write.address);
                    assert(block != queued_write.block);
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
            for ([_]*const FIFO(Read){
                &grid.read_queue,
                &grid.read_recovery_queue,
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
            assert(address > 0);
            grid.assert_not_writing(address, block);
            grid.assert_not_reading(address, block);

            assert(grid.superblock.opened);
            assert(!grid.superblock.free_set.is_free(address));

            write.* = .{
                .callback = callback,
                .address = address,
                .block = block,
            };

            const iop = grid.write_iops.acquire() orelse {
                grid.write_queue.push(write);
                return;
            };

            grid.write_block_with(iop, write);
        }

        fn write_block_with(grid: *Grid, iop: *WriteIOP, write: *Write) void {
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

            // We can only update the cache if the Grid is not resolving callbacks with a cache block.
            assert(!grid.read_resolving);

            const cached_block = grid.cache.insert_preserve_locked(
                *Grid,
                block_locked,
                grid,
                completed_write.address,
            );
            util.copy_disjoint(.exact, u8, cached_block, completed_write.block);

            // Start a queued write if possible *before* calling the completed
            // write's callback. This ensures that if the callback calls
            // Grid.write_block() it doesn't preempt the queue.
            if (grid.write_queue.pop()) |queued_write| {
                grid.write_block_with(iop, queued_write);
            } else {
                grid.write_iops.release(iop);
            }

            // This call must come after (logicall) releasing the IOP. Otherwise we risk tripping
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
            assert(address > 0);
            assert(block_type != .reserved);
            grid.assert_not_writing(address, null);

            assert(grid.superblock.opened);
            assert(!grid.superblock.free_set.is_free(address));

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .block_type = block_type,
                .grid = grid,
            };

            // Check if a read is already processing/recovering and merge with it.
            for ([_]*const FIFO(Read){
                &grid.read_queue,
                &grid.read_recovery_queue,
            }) |queue| {
                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    if (address == queued_read.address) {
                        assert(checksum == queued_read.checksum);
                        assert(block_type == queued_read.block_type);
                        queued_read.resolves.push(&read.pending);
                        return;
                    }
                }
            }

            // Become the "root" read thats fetching the block for the given address.
            // The fetch happens asynchronously to avoid stack-overflow and nested cache invalidation.
            grid.read_queue.push(read);
            grid.on_next_tick(read_block_tick_callback, &read.next_tick);
        }

        fn read_block_tick_callback(next_tick: *Storage.NextTick) void {
            const read = @fieldParentPtr(Grid.Read, "next_tick", next_tick);
            const grid = read.grid;

            // Try to resolve the read from the cache.
            if (grid.cache.get(read.address)) |block| {
                if (constants.verify) grid.verify_cached_read(read.address, block);
                grid.read_block_resolve(read, block);
                return;
            }

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

            // Grab a block from the cache to read with.
            // This also inserts the block into the cache at the address.
            const block = grid.cache.insert_preserve_locked(
                *Grid,
                block_locked,
                grid,
                address,
            );

            // `block` will be initialized later when the read completes.
            // This is safe because as long as `read` is in `grid.read_queue` or `grid.read_recovery_queue`
            // we will never attempt to read from or overwrite this cache entry.
            // However, we do have to immediately set the cache key to uphold the
            // invariants of `SetAssociativeCache`.
            cache_interface.set_address(block, address);

            iop.* = .{
                .completion = undefined,
                .read = read,
                .block = block,
            };

            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                block,
                .grid,
                block_offset(address),
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
            const block = iop.block;
            const read = iop.read;
            const grid = read.grid;

            // Handoff the iop to a pending read or release it before resolving the callbacks below.
            if (grid.read_pending_queue.pop()) |pending| {
                const queued_read = @fieldParentPtr(Read, "pending", pending);
                grid.read_block_with(iop, queued_read);
            } else {
                grid.read_iops.release(iop);
            }

            // A valid block filled by storage means the reads for the address can be resolved
            if (read_block_valid(read, block)) {
                grid.read_block_resolve(read, block);
                return;
            }

            // On the result of an invalid block, move the "root" read (and all others it resolves)
            // to recovery queue. Future reads on the same address will see the "root" read in the
            // recovery queue and enqueue to it.
            grid.read_queue.remove(read);
            grid.read_recovery_queue.push(read);
        }

        fn read_block_valid(read: *Grid.Read, block: BlockPtrConst) bool {
            const address = read.address;
            const checksum = read.checksum;
            const block_type = read.block_type;

            const header_bytes = block[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);

            if (!header.valid_checksum()) {
                log.err("invalid checksum at address {}", .{address});
                return false;
            }

            if (!header.valid_checksum_body(block[@sizeOf(vsr.Header)..header.size])) {
                log.err("invalid checksum body at address {}", .{address});
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
            assert(header.operation == block_type.operation());
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

            // Resolve all reads queued to the address with the block.
            // Callbacks may queue more to read.resolves so it must drain any new/existing pendings.
            while (read.resolves.pop()) |pending| {
                const pending_read = @fieldParentPtr(Read, "pending", pending);
                pending_read.callback(pending_read, block);
            }

            // Remove the "root" read so that the address is no longer actively reading / locked.
            // Then invoke the callback with the cache block (which should be valid for the duration
            // of the callback as any nested Grid calls cannot synchronously update the cache).
            grid.read_queue.remove(read);
            read.callback(read, block);
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
