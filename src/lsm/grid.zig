const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const SuperBlockFreeSet = @import("superblock_free_set.zig").SuperBlockFreeSet;
const FIFO = @import("../fifo.zig").FIFO;

pub fn GridType(comptime Storage: type) type {
    const block_size = config.block_size;
    const BlockPtr = *align(config.sector_size) [block_size]u8;
    const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

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

            /// Link for to_recover linked list.
            next: ?*Read = null,

            /// Call the user's callback, finishing the read.
            /// May be called by Replica after recovering the block over the network.
            pub fn finish(read: *Read) void {
                const callback = read.callback;
                read.* = undefined;
                callback(read);
            }
        };

        // TODO Replace `storage/cluster/free_set` fields with `superblock: *SuperBlock`:

        storage: *Storage,
        offset: u64,
        size: u64,

        cluster: u32,

        /// Owned by SuperBlock, shared with Grid.
        free_set: *SuperBlockFreeSet,

        // TODO interrogate this list and do recovery in Replica.tick().
        to_recover: FIFO(Read) = .{},

        pub fn init(
            storage: *Storage,
            offset: u64,
            size: u64,
            cluster: u32,
            free_set: *SuperBlockFreeSet,
        ) !Grid {
            return Grid{
                .storage = storage,
                .offset = offset,
                .size = size,
                .cluster = cluster,
                .free_set = free_set,
            };
        }

        pub fn deinit(grid: *Grid) void {
            grid.* = undefined;
        }

        pub fn write_block(
            grid: *Grid,
            callback: fn (*Grid.Write) void,
            write: *Grid.Write,
            block: BlockPtrConst,
            address: u64,
        ) void {
            assert(address != 0);

            grid.storage.write_sectors(callback, write, block, grid.block_offset(address));
        }

        /// This function transparently handles recovery if the checksum fails.
        /// If necessary, this read will be added to a linked list, which Replica can then
        /// interrogate each tick(). The callback passed to this function won't be called until the
        /// block has been recovered.
        pub fn read_block(
            grid: *Grid,
            callback: fn (*Grid.Read) void,
            read: *Grid.Read,
            block: BlockPtr,
            address: u64,
            checksum: u128,
        ) void {
            assert(address != 0);

            read.* = .{
                .grid = grid,
                .callback = callback,
                .completion = undefined,
                .block = block,
                .address = address,
                .checksum = checksum,
            };

            grid.storage.read_sectors(
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
                read.grid.to_recover.push(read);
            }
        }

        fn block_offset(grid: Grid, address: u64) u64 {
            assert(address != 0);
            return grid.offset + (address - 1) * block_size;
        }
    };
}
