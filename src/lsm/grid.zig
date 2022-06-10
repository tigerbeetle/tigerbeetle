const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const SuperBlockType = @import("superblock.zig").SuperBlockType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

const log = std.log.scoped(.grid);

const superblock_zone_size = @import("superblock.zig").superblock_zone_size;
const write_ahead_log_zone_size = config.message_size_max * 1024; // TODO Use journal_slot_count.
const client_table_zone_size = config.message_size_max * config.clients_max * 2;

pub fn GridType(comptime Storage: type) type {
    const block_size = config.block_size;
    const BlockPtr = *align(config.sector_size) [block_size]u8;
    const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

    const SuperBlock = SuperBlockType(Storage);

    const cache_interface = struct {
        inline fn address_from_block(block: [block_size]u8) u64 {
            const header_bytes = block[0..@sizeOf(vsr.Header)];
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

    const set_associative_cache_ways = 16;
    const Cache = SetAssociativeCache(
        u64,
        [block_size]u8,
        cache_interface.address_from_block,
        cache_interface.hash_address,
        cache_interface.equal_addresses,
        .{
            .ways = set_associative_cache_ways,
            .value_alignment = config.sector_size,
        },
    );

    const read_iops_max = 15;
    // This + 1 ensures that it is always possible for writes to add the written block
    // to the cache on completion, even if the maximum number of concurrent reads are in
    // progress and have locked all but one way in the target set.
    assert(read_iops_max + 1 <= set_associative_cache_ways);

    // TODO put more thought into how low/high this limit should be.
    const write_iops_max = 16;

    const grid_offset: u64 = superblock_zone_size +
        write_ahead_log_zone_size +
        client_table_zone_size;

    return struct {
        const Grid = @This();

        pub const Write = struct {
            callback: fn (*Grid.Write) void,
            address: u64,
            block: BlockPtrConst,

            /// Link for the write_queue linked list.
            next: ?*Write = null,

            /// Call the user's callback, finishing the write.
            fn finish(write: *Write) void {
                const callback = write.callback;
                write.* = undefined;
                callback(write);
            }
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

            /// Link for read_queue/read_recovery_queue/ReadIOP.reads linked lists.
            next: ?*Read = null,

            /// Call the user's callback, finishing the read.
            /// May be called by Replica after recovering the block over the network.
            fn finish(read: *Read, block: BlockPtrConst) void {
                const callback = read.callback;
                read.* = undefined;
                callback(read, block);
            }
        };

        const ReadIOP = struct {
            grid: *Grid,
            completion: Storage.Read,
            reads: FIFO(Read) = .{},
            /// This is a pointer to a value in the block cache.
            block: BlockPtr,
        };

        superblock: *SuperBlock,
        cache: Cache,

        write_iops: IOPS(WriteIOP, write_iops_max) = .{},
        write_queue: FIFO(Write) = .{},

        read_iops: IOPS(ReadIOP, read_iops_max) = .{},
        read_queue: FIFO(Read) = .{},

        /// If false, this flag is set to true before running user provided read callbacks.
        /// If true, Grid.read_block() will add the read to read_recursion_queue instead of
        /// starting it directly. This prevents unbounded recursion if user provided callbacks
        /// submit new reads.
        /// The read_recursion_queue will be emptied once every call to Grid.tick().
        read_recursion_guard: bool = false,
        read_recursion_queue: FIFO(Read) = .{},

        // TODO interrogate this list and do recovery in Replica.tick().
        read_recovery_queue: FIFO(Read) = .{},

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

        pub fn tick(grid: *Grid) void {
            assert(!grid.read_recursion_guard);

            // Make a copy to avoid a potential infinite loop here.
            // If any new reads are added to the read_recursion_queue they will be started on
            // the next tick.
            var copy = grid.read_recursion_queue;
            grid.read_recursion_queue = .{};
            while (copy.pop()) |read| {
                grid.start_read(read);
            }
        }

        pub fn acquire(grid: *Grid) u64 {
            // We will reject incoming data before it reaches the point
            // where storage is full, so this assertion is safe.
            return grid.superblock.free_set.acquire().?;
        }

        /// This function must be used to release block addresses instead of calling release()
        /// on the free set directly as this function also removes the address from the block
        /// cache. This allows us to assume that addresses are not already in the cache on
        /// insertion and avoids a lookup on that path by using the "no clobber" put variant.
        /// Asserts that the address is not currently being read from or written to.
        pub fn release(grid: *Grid, address: u64) void {
            grid.assert_not_writing(address, null);
            grid.assert_not_reading(address, null);

            grid.cache.remove(address);
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
                grid.read_recursion_queue,
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
                    assert(address != iop.reads.peek().?.address);
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
                block_offset(write.address),
            );
        }

        fn write_block_callback(completion: *Storage.Write) void {
            const iop = @fieldParentPtr(WriteIOP, "completion", completion);

            // We must copy these values to the stack as they will be overwritten
            // when we release the iop and potentially start a queued write.
            const grid = iop.grid;
            const completed_write = iop.write;

            const cached_block = grid.cache.put_no_clobber_preserve_locked(
                *Grid,
                block_locked,
                grid,
                completed_write.address,
            );
            mem.copy(u8, cached_block, completed_write.block);

            grid.write_iops.release(iop);

            // Start a queued write if possible *before* calling the completed
            // write's callback through write.finish(). This ensures that if the
            // callback calls Grid.write_block() it doesn't preempt the queue.
            if (grid.write_queue.pop()) |queued_write| {
                const initial_iops_available = grid.write_iops.available();
                assert(initial_iops_available > 0);
                grid.start_write(queued_write);
                assert(grid.write_iops.available() == initial_iops_available - 1);
            }

            // This call must come after releasing the IOP. Otherwise we risk tripping
            // assertions forbidding concurrent writes using the same block/address
            // if the callback calls write_block().
            completed_write.finish();
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
        ) void {
            assert(grid.superblock.opened);
            assert(address > 0);
            assert(!grid.superblock.free_set.is_free(address));
            grid.assert_not_writing(address, null);

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
            };

            if (grid.read_recursion_guard) {
                grid.read_recursion_queue.push(read);
                return;
            }

            grid.start_read(read);
        }

        fn start_read(grid: *Grid, read: *Grid.Read) void {
            assert(!grid.read_recursion_guard);
            grid.assert_not_writing(read.address, null);

            // Check if a read is already in progress for the target address.
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    if (iop.reads.peek().?.address == read.address) {
                        assert(iop.reads.peek().?.checksum == read.checksum);
                        iop.reads.push(read);
                        return;
                    }
                }
            }

            // If the block is already in the cache, there's no work to do.
            // Note that this must be called after we have checked for an in
            // progress read targeting the same address.
            if (grid.cache.get(read.address)) |block| {
                assert(!grid.read_recursion_guard);
                grid.read_recursion_guard = true;
                defer grid.read_recursion_guard = false;
                read.finish(block);
                return;
            }

            const iop = grid.read_iops.acquire() orelse {
                grid.read_queue.push(read);
                return;
            };

            const block = grid.cache.put_no_clobber_preserve_locked(
                *Grid,
                block_locked,
                grid,
                read.address,
            );

            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .block = block,
            };
            iop.reads.push(read);

            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                iop.block,
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

            const header_bytes = iop.block[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);
            const body = iop.block[@sizeOf(vsr.Header)..header.size];

            const address = iop.reads.peek().?.address;
            const checksum = iop.reads.peek().?.checksum;

            const checksum_valid = header.valid_checksum();
            const checksum_body_valid = header.valid_checksum_body(body);
            const checksum_match = header.checksum == checksum;

            if (checksum_valid and checksum_body_valid and checksum_match) {
                assert(!grid.read_recursion_guard);
                grid.read_recursion_guard = true;
                defer grid.read_recursion_guard = false;
                while (iop.reads.pop()) |read| {
                    assert(read.address == address);
                    assert(read.checksum == checksum);
                    read.finish(iop.block);
                }
            } else {
                if (!checksum_valid) {
                    log.err("invalid checksum at address {}", .{address});
                } else if (!checksum_body_valid) {
                    log.err("invalid checksum body at address {}", .{address});
                } else if (!checksum_match) {
                    log.err(
                        "valid checksum {} at address {} does not match expected checksum {}",
                        .{ header.checksum, address, checksum },
                    );
                } else {
                    unreachable;
                }
                while (iop.reads.pop()) |read| {
                    iop.grid.read_recovery_queue.push(read);
                }
            }

            grid.read_iops.release(iop);

            // Always iterate through the full list of pending reads here to ensure that all
            // possible reads in the list are started, even in the presence of concurrent reads
            // targeting the same block address.
            var copy = grid.read_queue;
            grid.read_queue = .{};
            while (copy.pop()) |read| {
                grid.start_read(read);
            }
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return grid_offset + (address - 1) * block_size;
        }
    };
}
