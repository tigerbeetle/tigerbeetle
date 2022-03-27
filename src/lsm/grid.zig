const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const SuperBlockType = @import("superblock.zig").SuperBlockType;
const FIFO = @import("../fifo.zig").FIFO;
const IOPS = @import("../iops.zig").IOPS;
const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

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
            return header.op;
        }

        inline fn hash_address(address: u64) u64 {
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

    const read_iops_max = 16;
    assert(read_iops_max <= set_associative_cache_ways);

    const grid_offset: u64 = superblock_zone_size +
        write_ahead_log_zone_size +
        client_table_zone_size;

    return struct {
        const Grid = @This();

        pub const Write = Storage.Write;

        pub const Read = struct {
            callback: fn (*Grid.Read, BlockPtrConst) void,
            address: u64,
            checksum: u128,

            /// Link for reads_pending/read_recovery_queue linked lists.
            next: ?*Read = null,

            /// Call the user's callback, finishing the read.
            /// May be called by Replica after recovering the block over the network.
            pub fn finish(read: *Read, block: BlockPtrConst) void {
                const callback = read.callback;
                read.* = undefined;
                callback(read, block);
            }
        };

        pub const ReadIOP = struct {
            grid: *Grid,
            completion: Storage.Read,
            read: *Read,
            /// This is a pointer to a value in the block cache.
            block: BlockPtr,
        };

        superblock: *SuperBlock,
        cache: Cache,

        reads_pending: FIFO(Read) = .{},
        reads: IOPS(ReadIOP, read_iops_max) = .{},

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

        pub fn write_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: BlockPtrConst,
            address: u64,
        ) void {
            assert(grid.superblock.opened);
            assert(address > 0);
            // TODO Assert that address is acquired in the free set.
            // TODO Assert that the block ptr is not being used for another I/O (read or write).
            // TODO Assert that block is not already writing.

            grid.superblock.storage.write_sectors(callback, write, block, block_offset(address));
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
            // TODO Assert that address is acquired in the free set.
            // TODO Assert that the block ptr is not being used for another I/O (read or write).
            // TODO Queue concurrent reads to the same address.

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
            };

            if (grid.reads.available() > 0) {
                grid.start_read(read);
            } else {
                grid.reads_pending.push(read);
            }
        }

        fn start_read(grid: *Grid, read: *Grid.Read) void {
            const result = grid.cache.get_or_put_preserve_locked(
                *Grid,
                block_locked,
                grid,
                read.address,
            );

            const iop = grid.reads.acquire().?;
            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .read = read,
                .block = result.value_ptr,
            };

            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                iop.block,
                block_offset(read.address),
            );
        }

        inline fn block_locked(grid: *Grid, block: BlockPtrConst) bool {
            var it = grid.reads.iterate();
            while (it.next()) |iop| {
                if (block == iop.block) return true;
            }
            return false;
        }

        fn read_block_callback(completion: *Storage.Read) void {
            const iop = @fieldParentPtr(ReadIOP, "completion", completion);

            const header_bytes = iop.block[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);
            const body = iop.block[@sizeOf(vsr.Header)..header.size];

            if (header.checksum == iop.read.checksum and
                header.valid_checksum() and
                header.valid_checksum_body(body))
            {
                iop.read.finish(iop.block);
            } else {
                iop.grid.read_recovery_queue.push(iop.read);
            }

            const grid = iop.grid;
            grid.reads.release(iop);

            if (grid.reads_pending.pop()) |read| {
                grid.start_read(read);
            }
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return grid_offset + (address - 1) * block_size;
        }
    };
}
