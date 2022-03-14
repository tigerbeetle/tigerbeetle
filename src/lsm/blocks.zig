const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");

const SuperBlockFreeSet = @import("superblock_free_set.zig").SuperBlockFreeSet;

pub fn BlocksType(comptime Storage: type) type {
    const block_size = config.block_size;
    const BlockPtr = *align(config.sector_size) [block_size]u8;
    const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

    return struct {
        const Blocks = @This();

        // TODO Replace `storage/cluster/free_set` fields with `superblock: *SuperBlock`:

        storage: *Storage,
        offset: u64,
        size: u64,

        cluster: u32,

        /// Owned by SuperBlock, shared with Blocks.
        free_set: *SuperBlockFreeSet,

        pub fn init(
            allocator: mem.Allocator,
            storage: *Storage,
            offset: u64,
            size: u64,
            cluster: u32,
            free_set: *SuperBlockFreeSet,
        ) !Blocks {
            _ = allocator; // TODO

            return Blocks{
                .storage = storage,
                .offset = offset,
                .size = size,
                .cluster = cluster,
                .free_set = free_set,
            };
        }

        pub fn deinit(blocks: *Blocks, allocator: mem.Allocator) void {
            _ = blocks;
            _ = allocator; // TODO
        }

        pub fn write_block(
            blocks: *Blocks,
            callback: fn (*Storage.Write) void,
            write: *Storage.Write,
            block: BlockPtrConst,
            address: u64,
        ) void {
            assert(address != 0);

            _ = blocks;
            _ = callback;
            _ = write;
            _ = block;

            // TODO
        }

        /// This function transparently handles recovery if the checksum fails.
        /// If necessary, this read will be added to a linked list, which Replica can then
        /// interrogate each tick(). The callback passed to this function won't be called until the
        /// block has been recovered.
        pub fn read_block(
            blocks: *Blocks,
            callback: fn (*Storage.Read) void,
            read: *Storage.Read,
            block: BlockPtr,
            address: u64,
            checksum: u128,
        ) void {
            assert(address != 0);

            _ = blocks;
            _ = callback;
            _ = read;
            _ = block;
            _ = checksum;

            // TODO
        }
    };
}
