const std = @import("std");
const assert = std.debug.assert;

const BlockType = @import("../lsm/grid.zig").BlockType;
const schema = @import("../lsm/schema.zig");
const Storage = @import("./storage.zig").Storage;

/// Verify that the storage:
/// - contains the given index block
/// - contains every filter/data block referenced by the index block
pub fn check_table(storage: *const Storage, index_address: u64, index_checksum: u128) void {
    //if (Storage != TestStorage) return;

    assert(index_address > 0);

    //std.debug.print("{}: STORAGE_CHECKER:CHECK index={} checksum={}\n", .{storage.options.replica_index, index_address, index_checksum});

    const index_block = storage.grid_block(index_address).?;
    const index_schema = schema.TableIndex.from(index_block);
    const index_block_header = schema.header_from_block(index_block);
    assert(index_block_header.op == index_address);
    assert(index_block_header.checksum == index_checksum);
    assert(BlockType.from(index_block_header.operation) == .index);

    const content_blocks_used = index_schema.content_blocks_used(index_block);
    var content_block_index: usize = 0;
    while (content_block_index < content_blocks_used) : (content_block_index += 1) {
        const content_block_id = index_schema.content_block(index_block, content_block_index);
        const content_block = storage.grid_block(content_block_id.block_address).?;
        const content_block_header = schema.header_from_block(content_block);

        //if (content_block_header.checksum == 76648540669318493793937402168690919905) {
        //    std.debug.print("TABLE: {},{}\n", .{index_address, index_checksum});
        //}

        assert(content_block_header.op == content_block_id.block_address);
        assert(content_block_header.checksum == content_block_id.block_checksum);
        assert(BlockType.from(content_block_header.operation) == .filter or
            BlockType.from(content_block_header.operation) == .data);
    }
}

// TODO is this ever used?
//pub fn check_block(storage: *const Storage, address: u64, checksum: u128) void {
//    //if (Storage != TestStorage) return;
//
//    assert(address > 0);
//
//    const block = storage.grid_block(address).?;
//    const block_header = schema.header_from_block(block);
//    assert(block_header.op == address);
//    assert(block_header.checksum == checksum);
//
//    switch (BlockType.from(block_header.operation)) {
//        .reserved => unreachable,
//        .manifest => {},
//        .table => storage.check_table(address, checksum),
//        .data => {},
//        .filter => {},
//    }
//}
