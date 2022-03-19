const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const SuperBlockType = @import("superblock.zig").SuperBlockType;
const FIFO = @import("../fifo.zig").FIFO;

pub fn GridType(comptime Storage: type) type {
    const block_size = config.block_size;
    const BlockPtr = *align(config.sector_size) [block_size]u8;
    const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

    const SuperBlock = SuperBlockType(Storage);

    return struct {
        const Grid = @This();

        pub const Write = Storage.Write;

        pub const Read = struct {
            grid: *Grid,
            callback: fn (*Grid.Read) void,
            completion: Storage.Read,
            block: BlockPtr,
            address: u64,
            checksum: u128,

            /// Link for read_recovery_queue linked list.
            next: ?*Read = null,

            /// Call the user's callback, finishing the read.
            /// May be called by Replica after recovering the block over the network.
            pub fn finish(read: *Read) void {
                const callback = read.callback;
                read.* = undefined;
                callback(read);
            }
        };

        superblock: *SuperBlock,

        // TODO interrogate this list and do recovery in Replica.tick().
        read_recovery_queue: FIFO(Read) = .{},

        pub fn init(allocator: mem.Allocator, superblock: *SuperBlock) !Grid {
            // TODO SetAssociativeCache.init(allocator):
            _ = allocator;

            return Grid{
                .superblock = superblock,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            grid.* = undefined;

            // TODO cache.deinit(allocator):
            _ = allocator;
        }

        pub fn write_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: BlockPtrConst,
            address: u64,
        ) void {
            assert(grid.superblock.opened);
            assert(address != 0);
            // TODO Assert that address is acquired in the free set.
            // TODO Assert that the block ptr is not being used for another I/O (read or write).
            // TODO Assert that block is not already writing.

            grid.superblock.storage.write_sectors(
                callback,
                write,
                block,
                grid.block_offset(address),
            );
        }

        /// This function transparently handles recovery if the checksum fails.
        /// If necessary, this read will be added to a linked list, which Replica can then
        /// interrogate each tick(). The callback passed to this function won't be called until the
        /// block has been recovered.
        pub fn read_block(
            grid: *Grid,
            callback: fn (*Grid.Read) void,
            read: *Grid.Read,
            block: BlockPtr, // TODO Instead, provide this to the callback to eliminate the copy.
            address: u64,
            checksum: u128,
        ) void {
            assert(grid.superblock.opened);
            assert(address != 0);
            // TODO Assert that address is acquired in the free set.
            // TODO Assert that the block ptr is not being used for another I/O (read or write).
            // TODO Queue concurrent reads to the same address.

            read.* = .{
                .grid = grid,
                .callback = callback,
                .completion = undefined,
                .block = block,
                .address = address,
                .checksum = checksum,
            };

            grid.superblock.storage.read_sectors(
                on_read_sectors,
                &read.completion,
                block,
                grid.block_offset(address),
            );
        }

        fn on_read_sectors(completion: *Storage.Read) void {
            const read = @fieldParentPtr(Read, "completion", completion);

            const header_bytes = read.block[0..@sizeOf(vsr.Header)];
            const header = mem.bytesAsValue(vsr.Header, header_bytes);
            const body = read.block[@sizeOf(vsr.Header)..header.size];

            if (header.checksum == read.checksum and
                header.valid_checksum() and
                header.valid_checksum_body(body))
            {
                read.finish();
            } else {
                read.grid.read_recovery_queue.push(read);
            }
        }

        fn block_offset(grid: Grid, address: u64) u64 {
            assert(address != 0);
            return grid.offset + (address - 1) * block_size;
        }
    };
}
