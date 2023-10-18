//! Decode grid blocks.
//!
//! Rather than switching between specialized decoders depending on the tree, each schema encodes
//! relevant parameters directly into the block's header. This allows the decoders to not be
//! generic. This is convenient for compaction, but critical for the scrubber and repair queue.
//!
//! Every block begins with a `vsr.Header` that includes:
//!
//! * `checksum`, `checksum_body` verify the data integrity.
//! * `cluster` is the cluster id.
//! * `command` is `.block`.
//! * `op` is the block address.
//! * `size` is the block size excluding padding.
//!
//! Index block schema:
//! │ vsr.Header                   │ operation=BlockType.index,
//! │                              │ context: schema.TableIndex.Context,
//! │                              │ parent: schema.TableIndex.Parent,
//! │                              │ request=@sizeOf(Key)
//! │                              │ timestamp=snapshot_min
//! │ [data_block_count_max]u128   │ checksums of data blocks
//! │ [data_block_count_max]Key    │ the minimum/first key in the respective data block
//! │ [data_block_count_max]Key    │ the maximum/last key in the respective data block
//! │ [data_block_count_max]u64    │ addresses of data blocks
//! │ […]u8{0}                     │ padding (to end of block)
//!
//! Data block schema:
//! │ vsr.Header               │ operation=BlockType.data,
//! │                          │ context: schema.TableData.Context,
//! │                          │ parent: schema.TableData.Parent,
//! │                          │ request=values_count
//! │                          │ timestamp=snapshot_min
//! │ [≤value_count_max]Value  │ At least one value (no empty tables).
//! │ […]u8{0}                 │ padding (to end of block)
//!
//! Manifest block schema:
//! │ vsr.Header                  │ operation=BlockType.manifest
//! │                             │ context: schema.Manifest.Context
//! │ [entry_count]TableInfo      │
//! │ [entry_count]Label          │ level index, insert|remove
//! │ […]u8{0}                    │ padding (to end of block)
//! Label and TableInfo entries correspond.
const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const stdx = @import("../stdx.zig");

const address_size = @sizeOf(u64);
const checksum_size = @sizeOf(u128);

const block_size = constants.block_size;
const block_body_size = block_size - @sizeOf(vsr.Header);

const BlockPtr = *align(constants.sector_size) [block_size]u8;
const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;

pub inline fn header_from_block(block: BlockPtrConst) *const vsr.Header {
    const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
    assert(header.command == .block);
    assert(header.op > 0);
    assert(header.size >= @sizeOf(vsr.Header)); // Every block has a header.
    assert(header.size > @sizeOf(vsr.Header)); // Every block has a non-empty body.
    assert(header.size <= block.len);
    assert(BlockType.valid(header.operation));
    assert(BlockType.from(header.operation) != .reserved);
    return header;
}

/// A block's type is implicitly determined by how its address is stored (e.g. in the index block).
/// BlockType is an additional check that a block has the expected type on read.
///
/// The BlockType is stored in the block's `header.operation`.
pub const BlockType = enum(u8) {
    /// Unused; verifies that no block is written with a default 0 operation.
    reserved = 0,

    manifest = 1,
    index = 2,
    data = 3,

    pub fn valid(vsr_operation: vsr.Operation) bool {
        _ = std.meta.intToEnum(BlockType, @intFromEnum(vsr_operation)) catch return false;

        return true;
    }

    pub inline fn from(vsr_operation: vsr.Operation) BlockType {
        return @as(BlockType, @enumFromInt(@intFromEnum(vsr_operation)));
    }

    pub inline fn operation(block_type: BlockType) vsr.Operation {
        return @as(vsr.Operation, @enumFromInt(@intFromEnum(block_type)));
    }
};

pub const TableIndex = struct {
    /// Stored in every index block's header's `context` field.
    ///
    /// The max-counts are stored in the header despite being available (per-tree) at comptime:
    /// - Encoding schema parameters enables schema evolution.
    /// - Tables can be decoded without per-tree specialized decoders.
    ///   (In particular, this is useful for the scrubber and the grid repair queue).
    pub const Context = extern struct {
        data_block_count: u32,
        data_block_count_max: u32,
        padding: [8]u8 = [_]u8{0} ** 8,

        comptime {
            assert(@sizeOf(Context) == @sizeOf(u128));
            assert(stdx.no_padding(Context));
        }
    };

    pub const Parent = extern struct {
        tree_id: u16,
        reserved: [14]u8 = [_]u8{0} ** 14,

        comptime {
            assert(@sizeOf(Parent) == @sizeOf(u128));
            assert(stdx.no_padding(Parent));
        }
    };

    key_size: u32,
    data_block_count_max: u32,

    size: u32,
    data_checksums_offset: u32,
    data_checksums_size: u32,
    keys_min_offset: u32,
    keys_max_offset: u32,
    keys_size: u32,
    data_addresses_offset: u32,
    data_addresses_size: u32,
    padding_offset: u32,
    padding_size: u32,

    const Parameters = struct {
        key_size: u32,
        data_block_count_max: u32,
    };

    pub fn init(parameters: Parameters) TableIndex {
        assert(parameters.key_size > 0);
        assert(parameters.data_block_count_max > 0);
        assert(parameters.data_block_count_max <= constants.lsm_table_data_blocks_max);

        const data_checksums_offset = @sizeOf(vsr.Header);
        const data_checksums_size = parameters.data_block_count_max * checksum_size;

        const keys_size = parameters.data_block_count_max * parameters.key_size;
        const keys_min_offset = data_checksums_offset + data_checksums_size;
        const keys_max_offset = keys_min_offset + keys_size;

        const data_addresses_offset = keys_max_offset + keys_size;
        const data_addresses_size = parameters.data_block_count_max * address_size;

        const padding_offset = data_addresses_offset + data_addresses_size;
        assert(padding_offset <= constants.block_size);
        const padding_size = constants.block_size - padding_offset;

        // `keys_size * 2` for counting both key_min and key_max:
        const size = @sizeOf(vsr.Header) + data_checksums_size +
            (keys_size * 2) + data_addresses_size;
        assert(size <= constants.block_size);

        return .{
            .key_size = parameters.key_size,
            .data_block_count_max = parameters.data_block_count_max,
            .size = size,
            .data_checksums_offset = data_checksums_offset,
            .data_checksums_size = data_checksums_size,
            .keys_min_offset = keys_min_offset,
            .keys_max_offset = keys_max_offset,
            .keys_size = keys_size,
            .data_addresses_offset = data_addresses_offset,
            .data_addresses_size = data_addresses_size,
            .padding_offset = padding_offset,
            .padding_size = padding_size,
        };
    }

    pub fn from(index_block: BlockPtrConst) TableIndex {
        const header = header_from_block(index_block);
        assert(header.command == .block);
        assert(BlockType.from(header.operation) == .index);
        assert(header.op > 0);
        assert(header.timestamp > 0);

        const context = @as(Context, @bitCast(header.context));
        assert(context.data_block_count <= context.data_block_count_max);

        return TableIndex.init(.{
            .key_size = header.request,
            .data_block_count_max = context.data_block_count_max,
        });
    }

    pub fn tree_id(index_block: BlockPtrConst) u16 {
        const header = header_from_block(index_block);
        assert(BlockType.from(header.operation) == .index);

        const parent = @as(Parent, @bitCast(header.parent));
        assert(stdx.zeroed(&parent.reserved));
        return parent.tree_id;
    }

    pub inline fn data_addresses(index: *const TableIndex, index_block: BlockPtr) []u64 {
        return @alignCast(mem.bytesAsSlice(
            u64,
            index_block[index.data_addresses_offset..][0..index.data_addresses_size],
        ));
    }

    pub inline fn data_addresses_used(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) []const u64 {
        const slice = mem.bytesAsSlice(
            u64,
            index_block[index.data_addresses_offset..][0..index.data_addresses_size],
        );
        return @alignCast(slice[0..index.data_blocks_used(index_block)]);
    }

    pub inline fn data_checksums(index: *const TableIndex, index_block: BlockPtr) []u128 {
        return @alignCast(mem.bytesAsSlice(
            u128,
            index_block[index.data_checksums_offset..][0..index.data_checksums_size],
        ));
    }

    pub inline fn data_checksums_used(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) []const u128 {
        const slice = mem.bytesAsSlice(
            u128,
            index_block[index.data_checksums_offset..][0..index.data_checksums_size],
        );
        return @alignCast(slice[0..index.data_blocks_used(index_block)]);
    }

    pub inline fn data_blocks_used(index: *const TableIndex, index_block: BlockPtrConst) u32 {
        const header = header_from_block(index_block);
        const context = @as(Context, @bitCast(header.context));
        const value = @as(u32, @intCast(context.data_block_count));
        assert(value > 0);
        assert(value <= index.data_block_count_max);
        return value;
    }
};

pub const TableData = struct {
    /// Stored in every data block's header's `context` field.
    pub const Context = extern struct {
        value_count_max: u32,
        value_size: u32,
        padding: [8]u8 = [_]u8{0} ** 8,

        comptime {
            assert(@sizeOf(Context) == @sizeOf(u128));
            assert(stdx.no_padding(Context));
        }
    };

    pub const Parent = extern struct {
        tree_id: u16,
        reserved: [14]u8 = .{0} ** 14,

        comptime {
            assert(@sizeOf(Parent) == @sizeOf(u128));
            assert(stdx.no_padding(Parent));
        }
    };

    // @sizeOf(Table.Value)
    value_size: u32,
    // The maximum number of values in a data block.
    value_count_max: u32,

    values_offset: u32,
    values_size: u32,

    padding_offset: u32,
    padding_size: u32,

    pub fn init(parameters: Context) TableData {
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

    pub fn from(data_block: BlockPtrConst) TableData {
        const header = header_from_block(data_block);
        assert(header.command == .block);
        assert(BlockType.from(header.operation) == .data);
        assert(header.op > 0);
        assert(header.timestamp > 0);

        return TableData.init(@as(Context, @bitCast(header.context)));
    }

    pub fn tree_id(data_block: BlockPtrConst) u16 {
        const header = header_from_block(data_block);
        assert(BlockType.from(header.operation) == .data);

        const parent = @as(Parent, @bitCast(header.parent));
        assert(stdx.zeroed(&parent.reserved));
        return parent.tree_id;
    }

    pub inline fn block_values_bytes(
        schema: *const TableData,
        data_block: BlockPtr,
    ) []align(16) u8 {
        return @alignCast(data_block[schema.values_offset..][0..schema.values_size]);
    }

    pub inline fn block_values_bytes_const(
        schema: *const TableData,
        data_block: BlockPtrConst,
    ) []align(16) const u8 {
        return @alignCast(data_block[schema.values_offset..][0..schema.values_size]);
    }

    pub inline fn block_values_used_bytes(
        schema: *const TableData,
        data_block: BlockPtrConst,
    ) []align(16) const u8 {
        const header = header_from_block(data_block);
        assert(BlockType.from(header.operation) == .data);

        const used_values: u32 = header.request;
        assert(used_values > 0);
        assert(used_values <= schema.value_count_max);

        const used_bytes = used_values * schema.value_size;
        assert(@sizeOf(vsr.Header) + used_bytes == header.size);
        assert(header.size <= schema.padding_offset); // This is the maximum padding_offset
        return schema.block_values_bytes_const(data_block)[0..used_bytes];
    }
};

/// A Manifest block's body is a SoA of Labels and TableInfos.
/// Each Label/TableInfo pair is an "entry".
// TODO Store timestamp (snapshot) in header.
pub const Manifest = struct {
    const entry_size = @sizeOf(TableInfo) + @sizeOf(Label);

    pub const entry_count_max = @divFloor(block_body_size, entry_size);

    const tables_size_max = entry_count_max * @sizeOf(TableInfo);
    const labels_size_max = entry_count_max * @sizeOf(Label);

    comptime {
        assert(entry_count_max > 0);
        assert(tables_size_max % @alignOf(Label) == 0);
        assert(labels_size_max % @alignOf(Label) == 0);

        // Bit 7 is reserved to indicate whether the event is an insert or remove.
        assert(constants.lsm_levels <= std.math.maxInt(u6) + 1);

        assert(@sizeOf(Label) == @sizeOf(u8));
        assert(@alignOf(Label) == 1);

        // All TableInfo's should already be 16-byte aligned because of the leading checksum.
        const alignment = 16;
        assert(alignment <= @sizeOf(vsr.Header));
        assert(alignment == @alignOf(TableInfo));

        // For keys { 8, 16, 24, 32 } all TableInfo's should be a multiple of the alignment.
        // However, we still store Label ahead of TableInfo to save space on the network.
        // This means we store fewer entries per manifest block, to gain less padding,
        // since we must store entry_count_max of whichever array is first in the layout.
        // For a better understanding of this decision, see schema.Manifest.size().
        assert(@sizeOf(TableInfo) % alignment == 0);
    }

    /// Stored in every manifest block's header's `context` field.
    pub const Context = extern struct {
        entry_count: u32,
        reserved: [12]u8 = .{0} ** 12,

        comptime {
            assert(@sizeOf(Context) == @sizeOf(u128));
            assert(stdx.no_padding(Context));
        }
    };

    /// See manifest.zig's TreeTableInfoType declaration for field documentation.
    pub const TableInfo = extern struct {
        /// All keys must fit within 32 bytes.
        pub const KeyPadded = [32]u8;

        checksum: u128,
        address: u64,
        tree_id: u16,
        reserved: [6]u8 = .{0} ** 6,
        snapshot_min: u64,
        snapshot_max: u64,
        key_min: KeyPadded,
        key_max: KeyPadded,

        comptime {
            assert(@alignOf(TableInfo) == 16);
            assert(stdx.no_padding(TableInfo));
        }
    };

    pub const Event = enum(u2) {
        insert = 0,
        update = 1,
        remove = 2,
    };

    pub const Label = packed struct(u8) {
        level: u6,
        event: Event,

        comptime {
            assert(@bitSizeOf(Label) == @sizeOf(Label) * 8);
        }
    };

    entry_count: u32,

    pub fn from(manifest_block: BlockPtrConst) Manifest {
        const header = header_from_block(manifest_block);
        assert(header.command == .block);
        assert(BlockType.from(header.operation) == .manifest);
        assert(header.op > 0);

        const context = @as(Context, @bitCast(header.context));
        assert(context.entry_count > 0);
        assert(context.entry_count <= entry_count_max);
        assert(context.entry_count == @divExact(header.size - @sizeOf(vsr.Header), entry_size));
        assert(stdx.zeroed(&context.reserved));

        return .{ .entry_count = context.entry_count };
    }

    pub fn size(schema: *const Manifest) u32 {
        assert(schema.entry_count > 0);
        assert(schema.entry_count <= entry_count_max);

        const tables_size = schema.entry_count * @sizeOf(TableInfo);
        const labels_size = schema.entry_count * @sizeOf(Label);

        return @sizeOf(vsr.Header) + tables_size + labels_size;
    }

    pub fn tables(schema: *const Manifest, block: BlockPtr) []TableInfo {
        return mem.bytesAsSlice(
            TableInfo,
            block[@sizeOf(vsr.Header)..][0 .. schema.entry_count * @sizeOf(TableInfo)],
        );
    }

    pub fn tables_const(schema: *const Manifest, block: BlockPtrConst) []const TableInfo {
        return mem.bytesAsSlice(
            TableInfo,
            block[@sizeOf(vsr.Header)..][0 .. schema.entry_count * @sizeOf(TableInfo)],
        );
    }

    pub fn labels(schema: *const Manifest, block: BlockPtr) []Label {
        const tables_size = schema.entry_count * @sizeOf(TableInfo);
        const labels_size = schema.entry_count * @sizeOf(Label);
        return mem.bytesAsSlice(
            Label,
            block[@sizeOf(vsr.Header) + tables_size ..][0..labels_size],
        );
    }

    pub fn labels_const(schema: *const Manifest, block: BlockPtrConst) []const Label {
        const tables_size = schema.entry_count * @sizeOf(TableInfo);
        const labels_size = schema.entry_count * @sizeOf(Label);
        return mem.bytesAsSlice(
            Label,
            block[@sizeOf(vsr.Header) + tables_size ..][0..labels_size],
        );
    }
};
