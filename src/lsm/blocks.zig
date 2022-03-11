const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");

const BlockFreeSet = @import("block_free_set.zig").BlockFreeSet;

pub fn Blocks(comptime Storage: type) type {
    const block_size = config.block_size;
    const BlockPtr = *align(config.sector_size) [block_size]u8;
    const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

    return struct {
        const BlocksGeneric = @This();

        storage: *Storage,
        offset: u64,
        size: u64,

        /// Owned by SuperBlock, shared with Blocks.
        free_set: *BlockFreeSet,

        pub fn init(
            allocator: mem.Allocator,
            storage: *Storage,
            offset: u64,
            size: u64,
            free_set: *BlockFreeSet,
        ) !BlocksGeneric {
            _ = allocator; // TODO

            return BlocksGeneric{
                .storage = storage,
                .offset = offset,
                .size = size,
                .free_set = free_set,
            };
        }

        pub fn deinit(blocks: *BlocksGeneric, allocator: mem.Allocator) void {
            _ = blocks;
            _ = allocator; // TODO
        }

        pub fn write_block(
            blocks: *BlocksGeneric,
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
            blocks: *BlocksGeneric,
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
