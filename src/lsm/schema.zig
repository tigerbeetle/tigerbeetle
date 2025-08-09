//! Decode grid blocks.
//!
//! Rather than switching between specialized decoders depending on the tree, each schema encodes
//! relevant parameters directly into the block's header. This allows the decoders to not be
//! generic. This is convenient for compaction, but critical for the scrubber and repair queue.
//!
//! Index block body schema:
//! │ [value_block_count_max]u256   │ checksums of value blocks
//! │ [value_block_count_max]Key    │ the minimum/first key in the respective value block
//! │ [value_block_count_max]Key    │ the maximum/last key in the respective value block
//! │ [value_block_count_max]u64    │ addresses of value blocks
//! │ […]u8{0}                     │ padding (to end of block)
//!
//! Value block body schema:
//! │ [≤value_count_max]Value  │ At least one value (no empty tables).
//! │ […]u8{0}                 │ padding (to end of block)
//!
//! ManifestNode block body schema:
//! │ [entry_count]TableInfo      │
//! │ […]u8{0}                    │ padding (to end of block)
const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const stdx = @import("stdx");

const BlockReference = vsr.BlockReference;

const address_size = @sizeOf(u64);
const checksum_size = @sizeOf(u256);

const block_size = constants.block_size;
const block_body_size = block_size - @sizeOf(vsr.Header);

const BlockPtr = *align(constants.sector_size) [block_size]u8;
const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;

pub inline fn header_from_block(block: BlockPtrConst) *const vsr.Header.Block {
    const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);
    assert(header.command == .block);
    assert(header.address > 0);
    assert(header.size >= @sizeOf(vsr.Header)); // Every block has a header.
    assert(header.size > @sizeOf(vsr.Header)); // Every block has a non-empty body.
    assert(header.size <= block.len);
    assert(header.block_type.valid());
    assert(header.block_type != .reserved);
    assert(header.release.value > 0);
    return header;
}

/// A block's type is implicitly determined by how its address is stored (e.g. in the index block).
/// BlockType is an additional check that a block has the expected type on read.
///
/// The BlockType is stored in the block's `header.block_type`.
pub const BlockType = enum(u8) {
    /// Unused; verifies that no block is written with a default 0 block type.
    reserved = 0,

    free_set = 1,
    client_sessions = 2,
    manifest = 3,
    index = 4,
    value = 5,

    pub fn valid(block_type: BlockType) bool {
        _ = std.meta.intToEnum(BlockType, @intFromEnum(block_type)) catch return false;

        return true;
    }
};

// TODO(zig): Once "extern struct" supports u256, change all checksums to (padded) u256.
pub const Checksum = extern struct {
    value: u128,
    padding: u128 = 0,
};

pub const TableIndex = struct {
    /// Stored in every index block's header's `metadata_bytes` field.
    ///
    /// The max-counts are stored in the header despite being available (per-tree) at comptime:
    /// - Encoding schema parameters enables schema evolution.
    /// - Tables can be decoded without per-tree specialized decoders.
    ///   (In particular, this is useful for the scrubber and the grid repair queue).
    pub const Metadata = extern struct {
        value_block_count: u32,
        value_block_count_max: u32,
        key_size: u32,
        tree_id: u16,
        reserved: [82]u8 = @splat(0),

        comptime {
            assert(stdx.no_padding(Metadata));
            assert(@sizeOf(Metadata) == vsr.Header.Block.metadata_size);
        }
    };

    key_size: u32,
    value_block_count_max: u32,

    size: u32,
    value_checksums_offset: u32,
    value_checksums_size: u32,
    keys_min_offset: u32,
    keys_max_offset: u32,
    keys_size: u32,
    value_addresses_offset: u32,
    value_addresses_size: u32,
    padding_offset: u32,
    padding_size: u32,

    const Parameters = struct {
        key_size: u32,
        value_block_count_max: u32,
    };

    pub fn init(parameters: Parameters) TableIndex {
        assert(parameters.key_size > 0);
        assert(parameters.value_block_count_max > 0);
        assert(parameters.value_block_count_max <= constants.lsm_table_value_blocks_max);

        const value_checksums_offset = @sizeOf(vsr.Header);
        const value_checksums_size = parameters.value_block_count_max * checksum_size;

        const keys_size = parameters.value_block_count_max * parameters.key_size;
        const keys_min_offset = value_checksums_offset + value_checksums_size;
        const keys_max_offset = keys_min_offset + keys_size;

        const value_addresses_offset = keys_max_offset + keys_size;
        const value_addresses_size = parameters.value_block_count_max * address_size;

        const padding_offset = value_addresses_offset + value_addresses_size;
        assert(padding_offset <= constants.block_size);
        const padding_size = constants.block_size - padding_offset;

        // `keys_size * 2` for counting both key_min and key_max:
        const size = @sizeOf(vsr.Header) + value_checksums_size +
            (keys_size * 2) + value_addresses_size;
        assert(size <= constants.block_size);

        return .{
            .key_size = parameters.key_size,
            .value_block_count_max = parameters.value_block_count_max,
            .size = size,
            .value_checksums_offset = value_checksums_offset,
            .value_checksums_size = value_checksums_size,
            .keys_min_offset = keys_min_offset,
            .keys_max_offset = keys_max_offset,
            .keys_size = keys_size,
            .value_addresses_offset = value_addresses_offset,
            .value_addresses_size = value_addresses_size,
            .padding_offset = padding_offset,
            .padding_size = padding_size,
        };
    }

    pub fn from(index_block: BlockPtrConst) TableIndex {
        const header = header_from_block(index_block);
        assert(header.command == .block);
        assert(header.block_type == .index);
        assert(header.address > 0);
        assert(header.snapshot > 0);

        const header_metadata = metadata(index_block);
        const index = TableIndex.init(.{
            .key_size = header_metadata.key_size,
            .value_block_count_max = header_metadata.value_block_count_max,
        });

        for (index.padding(index_block)) |padding_area| {
            assert(stdx.zeroed(index_block[padding_area.start..padding_area.end]));
        }

        return index;
    }

    pub fn metadata(index_block: BlockPtrConst) *const Metadata {
        const header = header_from_block(index_block);
        assert(header.command == .block);
        assert(header.block_type == .index);

        const header_metadata = std.mem.bytesAsValue(Metadata, &header.metadata_bytes);
        assert(header_metadata.value_block_count <= header_metadata.value_block_count_max);
        assert(stdx.zeroed(&header_metadata.reserved));
        return header_metadata;
    }

    pub inline fn block_metadata(
        schema: *const TableIndex,
        index_block: BlockPtrConst,
    ) *const Metadata {
        const result = metadata(index_block);
        assert(result.key_size == schema.key_size);
        assert(result.value_block_count_max == schema.value_block_count_max);
        return result;
    }

    pub inline fn value_addresses(index: *const TableIndex, index_block: BlockPtr) []u64 {
        return @alignCast(mem.bytesAsSlice(
            u64,
            index_block[index.value_addresses_offset..][0..index.value_addresses_size],
        ));
    }

    pub inline fn value_addresses_used(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) []const u64 {
        const slice = mem.bytesAsSlice(
            u64,
            index_block[index.value_addresses_offset..][0..index.value_addresses_size],
        );
        return @alignCast(slice[0..index.value_blocks_used(index_block)]);
    }

    pub inline fn value_checksums(index: *const TableIndex, index_block: BlockPtr) []Checksum {
        return @alignCast(mem.bytesAsSlice(
            Checksum,
            index_block[index.value_checksums_offset..][0..index.value_checksums_size],
        ));
    }

    pub inline fn value_checksums_used(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) []const Checksum {
        const slice = mem.bytesAsSlice(
            Checksum,
            index_block[index.value_checksums_offset..][0..index.value_checksums_size],
        );
        return @alignCast(slice[0..index.value_blocks_used(index_block)]);
    }

    pub inline fn value_blocks_used(index: *const TableIndex, index_block: BlockPtrConst) u32 {
        const header_metadata = block_metadata(index, index_block);
        assert(header_metadata.value_block_count > 0);
        assert(header_metadata.value_block_count <= index.value_block_count_max);
        return header_metadata.value_block_count;
    }

    pub fn padding(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) [4]struct { start: usize, end: usize } {
        const value_checksums_skip = index.value_blocks_used(index_block) * checksum_size;
        const keys_min_skip = index.value_blocks_used(index_block) * index.key_size;
        const keys_max_skip = index.value_blocks_used(index_block) * index.key_size;
        const value_addresses_skip = index.value_blocks_used(index_block) * address_size;
        return .{
            .{
                .start = index.value_checksums_offset + value_checksums_skip,
                .end = index.value_checksums_offset + index.value_checksums_size,
            },
            .{
                .start = index.keys_min_offset + keys_min_skip,
                .end = index.keys_min_offset + index.keys_size,
            },
            .{
                .start = index.keys_max_offset + keys_max_skip,
                .end = index.keys_max_offset + index.keys_size,
            },
            .{
                .start = index.value_addresses_offset + value_addresses_skip,
                .end = index.value_addresses_offset + index.value_addresses_size,
            },
        };
    }
};

pub const TableValue = struct {
    /// Stored in every value block's header's `metadata_bytes` field.
    pub const Metadata = extern struct {
        value_count_max: u32,
        value_count: u32,
        value_size: u32,
        tree_id: u16,
        reserved: [82]u8 = @splat(0),

        comptime {
            assert(stdx.no_padding(Metadata));
            assert(@sizeOf(Metadata) == vsr.Header.Block.metadata_size);
        }
    };

    // @sizeOf(Table.Value)
    value_size: u32,
    // The maximum number of values in a value block.
    value_count_max: u32,

    values_offset: u32,
    values_size: u32,

    padding_offset: u32,
    padding_size: u32,

    pub const Parameters = struct {
        value_count_max: u32,
        value_size: u32,
    };

    pub fn init(parameters: Parameters) TableValue {
        assert(parameters.value_count_max > 0);
        assert(parameters.value_size > 0);
        assert(std.math.isPowerOfTwo(parameters.value_size));

        const value_count_max = parameters.value_count_max;

        const values_offset = @sizeOf(vsr.Header);
        const values_size = parameters.value_count_max * parameters.value_size;

        const padding_offset = values_offset + values_size;
        const padding_size = constants.block_size - padding_offset;

        return .{
            .value_size = parameters.value_size,
            .value_count_max = value_count_max,
            .values_offset = values_offset,
            .values_size = values_size,
            .padding_offset = padding_offset,
            .padding_size = padding_size,
        };
    }

    pub fn from(value_block: BlockPtrConst) TableValue {
        const header = header_from_block(value_block);
        assert(header.command == .block);
        assert(header.block_type == .value);
        assert(header.address > 0);
        assert(header.snapshot > 0);

        const header_metadata = metadata(value_block);

        return TableValue.init(.{
            .value_count_max = header_metadata.value_count_max,
            .value_size = header_metadata.value_size,
        });
    }

    pub fn metadata(value_block: BlockPtrConst) *const Metadata {
        const header = header_from_block(value_block);
        assert(header.command == .block);
        assert(header.block_type == .value);

        const header_metadata = std.mem.bytesAsValue(Metadata, &header.metadata_bytes);
        assert(header_metadata.value_size > 0);
        assert(header_metadata.value_count > 0);
        assert(header_metadata.value_count <= header_metadata.value_count_max);
        assert(header_metadata.tree_id > 0);
        assert(stdx.zeroed(&header_metadata.reserved));
        assert(@sizeOf(vsr.Header) + header_metadata.value_size * header_metadata.value_count ==
            header.size);

        return header_metadata;
    }

    pub inline fn block_metadata(
        schema: *const TableValue,
        value_block: BlockPtrConst,
    ) *const Metadata {
        const result = metadata(value_block);
        assert(result.value_size == schema.value_size);
        assert(result.value_count_max == schema.value_count_max);
        return result;
    }

    pub inline fn block_values_bytes(
        schema: *const TableValue,
        value_block: BlockPtr,
    ) []align(16) u8 {
        return @alignCast(value_block[schema.values_offset..][0..schema.values_size]);
    }

    pub inline fn block_values_bytes_const(
        schema: *const TableValue,
        value_block: BlockPtrConst,
    ) []align(16) const u8 {
        return @alignCast(value_block[schema.values_offset..][0..schema.values_size]);
    }

    pub inline fn block_values_used_bytes(
        schema: *const TableValue,
        value_block: BlockPtrConst,
    ) []align(16) const u8 {
        const header = header_from_block(value_block);
        assert(header.block_type == .value);

        const used_values: u32 = block_metadata(schema, value_block).value_count;
        assert(used_values > 0);
        assert(used_values <= schema.value_count_max);

        const used_bytes = used_values * schema.value_size;
        assert(@sizeOf(vsr.Header) + used_bytes == header.size);
        assert(header.size <= schema.padding_offset); // This is the maximum padding_offset
        return schema.block_values_bytes_const(value_block)[0..used_bytes];
    }
};

/// A TrailerNode is either a `BlockType.free_set` or `BlockType.client_sessions`.
pub const TrailerNode = struct {
    pub const Metadata = extern struct {
        previous_trailer_block_checksum: u128,
        previous_trailer_block_checksum_padding: u128 = 0,
        previous_trailer_block_address: u64,
        reserved: [56]u8 = @splat(0),

        comptime {
            assert(stdx.no_padding(Metadata));
            assert(@sizeOf(Metadata) == vsr.Header.Block.metadata_size);
        }
    };

    pub fn metadata(free_set_block: BlockPtrConst) *const Metadata {
        const header = header_from_block(free_set_block);
        assert(header.command == .block);
        assert(header.block_type == .free_set or header.block_type == .client_sessions);
        assert(header.address > 0);
        assert(header.snapshot == 0);

        const header_metadata = std.mem.bytesAsValue(Metadata, &header.metadata_bytes);
        assert(header_metadata.previous_trailer_block_checksum_padding == 0);
        assert(stdx.zeroed(&header_metadata.reserved));

        if (header_metadata.previous_trailer_block_address == 0) {
            assert(header_metadata.previous_trailer_block_checksum == 0);
        }

        assert(header.size > @sizeOf(vsr.Header));

        switch (header.block_type) {
            .free_set => {
                assert((header.size - @sizeOf(vsr.Header)) % @sizeOf(u64) == 0);
            },
            .client_sessions => {
                assert((header.size - @sizeOf(vsr.Header)) %
                    (@sizeOf(vsr.Header) + @sizeOf(u64)) == 0);
            },
            else => unreachable,
        }

        return header_metadata;
    }

    pub fn assert_valid_header(free_set_block: BlockPtrConst) void {
        _ = metadata(free_set_block);
    }

    pub fn previous(free_set_block: BlockPtrConst) ?BlockReference {
        const header_metadata = metadata(free_set_block);

        if (header_metadata.previous_trailer_block_address == 0) {
            assert(header_metadata.previous_trailer_block_checksum == 0);
            return null;
        } else {
            return .{
                .checksum = header_metadata.previous_trailer_block_checksum,
                .address = header_metadata.previous_trailer_block_address,
            };
        }
    }

    pub fn body(block: BlockPtrConst) []align(@sizeOf(vsr.Header)) const u8 {
        const header = header_from_block(block);
        return block[@sizeOf(vsr.Header)..header.size];
    }
};

/// A Manifest block's body is an array of TableInfo entries.
// TODO Store snapshot in header.
pub const ManifestNode = struct {
    const entry_size = @sizeOf(TableInfo);

    pub const entry_count_max = @divFloor(block_body_size, entry_size);

    comptime {
        assert(entry_count_max > 0);

        // Bit 7 is reserved to indicate whether the event is an insert or remove.
        assert(constants.lsm_levels <= std.math.maxInt(u6) + 1);

        assert(@sizeOf(Label) == @sizeOf(u8));
        assert(@alignOf(Label) == 1);

        // TableInfo should already be 16-byte aligned because of the leading padded key.
        const alignment = 16;
        assert(alignment <= @sizeOf(vsr.Header));
        assert(alignment == @alignOf(TableInfo));

        // For keys { 8, 16, 24, 32 } all TableInfo's should be a multiple of the alignment.
        assert(@sizeOf(TableInfo) % alignment == 0);
    }

    /// Stored in every manifest block's header's `metadata_bytes` field.
    pub const Metadata = extern struct {
        previous_manifest_block_checksum: u128,
        previous_manifest_block_checksum_padding: u128 = 0,
        previous_manifest_block_address: u64,
        entry_count: u32,
        reserved: [52]u8 = @splat(0),

        comptime {
            assert(stdx.no_padding(Metadata));
            assert(@sizeOf(Metadata) == vsr.Header.Block.metadata_size);
        }
    };

    /// See manifest.zig's TreeTableInfoType declaration for field documentation.
    pub const TableInfo = extern struct {
        /// All keys must fit within 32 bytes.
        pub const KeyPadded = [32]u8;

        key_min: KeyPadded,
        key_max: KeyPadded,
        checksum: u128,
        checksum_padding: u128 = 0,
        address: u64,
        snapshot_min: u64,
        snapshot_max: u64,
        value_count: u32,
        tree_id: u16,
        label: Label,
        reserved: [1]u8 = @splat(0),

        comptime {
            assert(@sizeOf(TableInfo) == 128);
            assert(@alignOf(TableInfo) == 16);
            assert(stdx.no_padding(TableInfo));
        }
    };

    pub const Event = enum(u2) {
        reserved = 0,
        insert = 1,
        update = 2,
        remove = 3,
    };

    pub const Label = packed struct(u8) {
        level: u6,
        event: Event,

        comptime {
            assert(@bitSizeOf(Label) == @sizeOf(Label) * 8);
        }
    };

    entry_count: u32,

    pub fn from(manifest_block: BlockPtrConst) ManifestNode {
        const header_metadata = metadata(manifest_block);
        return .{ .entry_count = header_metadata.entry_count };
    }

    pub fn metadata(manifest_block: BlockPtrConst) *const Metadata {
        const header = header_from_block(manifest_block);
        assert(header.command == .block);
        assert(header.block_type == .manifest);
        assert(header.address > 0);
        assert(header.snapshot == 0);

        const header_metadata = std.mem.bytesAsValue(Metadata, &header.metadata_bytes);
        assert(header_metadata.entry_count > 0);
        assert(header_metadata.entry_count <= entry_count_max);
        assert(header_metadata.entry_count ==
            @divExact(header.size - @sizeOf(vsr.Header), entry_size));
        assert(header_metadata.previous_manifest_block_checksum_padding == 0);
        assert(stdx.zeroed(&header_metadata.reserved));

        if (header_metadata.previous_manifest_block_address == 0) {
            assert(header_metadata.previous_manifest_block_checksum == 0);
        }

        return header_metadata;
    }

    /// Note that the returned block reference is no longer be part of the manifest if
    /// `manifest_block` is the oldest block in the superblock's CheckpointState.
    pub fn previous(manifest_block: BlockPtrConst) ?BlockReference {
        _ = from(manifest_block); // Validation only.

        const header_metadata = metadata(manifest_block);
        if (header_metadata.previous_manifest_block_address == 0) {
            assert(header_metadata.previous_manifest_block_checksum == 0);

            return null;
        } else {
            return .{
                .checksum = header_metadata.previous_manifest_block_checksum,
                .address = header_metadata.previous_manifest_block_address,
            };
        }
    }

    pub fn size(schema: *const ManifestNode) u32 {
        assert(schema.entry_count > 0);
        assert(schema.entry_count <= entry_count_max);

        const tables_size = schema.entry_count * @sizeOf(TableInfo);
        return @sizeOf(vsr.Header) + tables_size;
    }

    pub fn tables(schema: *const ManifestNode, block: BlockPtr) []TableInfo {
        return mem.bytesAsSlice(
            TableInfo,
            block[@sizeOf(vsr.Header)..][0 .. schema.entry_count * @sizeOf(TableInfo)],
        );
    }

    pub fn tables_const(schema: *const ManifestNode, block: BlockPtrConst) []const TableInfo {
        return mem.bytesAsSlice(
            TableInfo,
            block[@sizeOf(vsr.Header)..][0 .. schema.entry_count * @sizeOf(TableInfo)],
        );
    }
};
