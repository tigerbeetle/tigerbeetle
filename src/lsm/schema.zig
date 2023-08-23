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
//! │ [filter_block_count_max]u128 │ checksums of filter blocks
//! │ [data_block_count_max]u128   │ checksums of data blocks
//! │ [data_block_count_max]Key    │ the maximum/last key in the respective data block
//! │ [filter_block_count_max]u64  │ addresses of filter blocks
//! │ [data_block_count_max]u64    │ addresses of data blocks
//! │ […]u8{0}                     │ padding (to end of block)
//!
//! Filter block schema:
//! │ vsr.Header │ operation=BlockType.filter,
//! │            │ context: schema.TableFilter.Context
//! │            │ parent: schema.TableFilter.Parent,
//! │            │ timestamp=snapshot_min
//! │ […]u8      │ A split-block Bloom filter, "containing" every key from as many as
//! │            │   `filter_data_block_count_max` data blocks.
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
//! TODO(Unified manifest) Import the manifest log block too.
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
    assert(header.size >= @sizeOf(vsr.Header));
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
    filter = 3,
    data = 4,

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
    /// Every table has exactly one index block.
    const index_block_count = 1;

    /// Stored in every index block's header's `context` field.
    ///
    /// The max-counts are stored in the header despite being available (per-tree) at comptime:
    /// - Encoding schema parameters enables schema evolution.
    /// - Tables can be decoded without per-tree specialized decoders.
    ///   (In particular, this is useful for the scrubber and the grid repair queue).
    pub const Context = extern struct {
        filter_block_count: u32,
        filter_block_count_max: u32,
        data_block_count: u32,
        data_block_count_max: u32,

        comptime {
            assert(@sizeOf(Context) == @sizeOf(u128));
            assert(stdx.no_padding(Context));
        }
    };

    pub const Parent = extern struct {
        tree_id: u16,
        padding: [14]u8 = [_]u8{0} ** 14,

        comptime {
            assert(@sizeOf(Parent) == @sizeOf(u128));
            assert(stdx.no_padding(Parent));
        }
    };

    key_size: u32,
    filter_block_count_max: u32,
    data_block_count_max: u32,

    size: u32,
    filter_checksums_offset: u32,
    filter_checksums_size: u32,
    data_checksums_offset: u32,
    data_checksums_size: u32,
    keys_offset: u32,
    keys_size: u32,
    filter_addresses_offset: u32,
    filter_addresses_size: u32,
    data_addresses_offset: u32,
    data_addresses_size: u32,
    padding_offset: u32,
    padding_size: u32,

    const Parameters = struct {
        key_size: u32,
        filter_block_count_max: u32,
        data_block_count_max: u32,
    };

    pub fn init(parameters: Parameters) TableIndex {
        assert(parameters.key_size > 0);
        assert(parameters.filter_block_count_max > 0);
        assert(parameters.data_block_count_max > 0);

        const filter_checksums_offset = @sizeOf(vsr.Header);
        const filter_checksums_size = parameters.filter_block_count_max * checksum_size;

        const data_checksums_offset = filter_checksums_offset + filter_checksums_size;
        const data_checksums_size = parameters.data_block_count_max * checksum_size;

        const keys_offset = data_checksums_offset + data_checksums_size;
        const keys_size = parameters.data_block_count_max * parameters.key_size;

        const filter_addresses_offset = keys_offset + keys_size;
        const filter_addresses_size = parameters.filter_block_count_max * address_size;

        const data_addresses_offset = filter_addresses_offset + filter_addresses_size;
        const data_addresses_size = parameters.data_block_count_max * address_size;

        const padding_offset = data_addresses_offset + data_addresses_size;
        const padding_size = constants.block_size - padding_offset;

        const size = @sizeOf(vsr.Header) + filter_checksums_size + data_checksums_size +
            keys_size + filter_addresses_size + data_addresses_size;

        return .{
            .key_size = parameters.key_size,
            .filter_block_count_max = parameters.filter_block_count_max,
            .data_block_count_max = parameters.data_block_count_max,
            .size = size,
            .filter_checksums_offset = filter_checksums_offset,
            .filter_checksums_size = filter_checksums_size,
            .data_checksums_offset = data_checksums_offset,
            .data_checksums_size = data_checksums_size,
            .keys_offset = keys_offset,
            .keys_size = keys_size,
            .filter_addresses_offset = filter_addresses_offset,
            .filter_addresses_size = filter_addresses_size,
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
        assert(context.filter_block_count <= context.filter_block_count_max);
        assert(context.data_block_count <= context.data_block_count_max);

        return TableIndex.init(.{
            .key_size = header.request,
            .filter_block_count_max = context.filter_block_count_max,
            .data_block_count_max = context.data_block_count_max,
        });
    }

    pub fn tree_id(index_block: BlockPtrConst) u128 {
        const header = header_from_block(index_block);
        assert(BlockType.from(header.operation) == .index);

        return @as(Parent, @bitCast(header.parent)).tree_id;
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

    pub inline fn filter_addresses(index: *const TableIndex, index_block: BlockPtr) []u64 {
        return @alignCast(mem.bytesAsSlice(
            u64,
            index_block[index.filter_addresses_offset..][0..index.filter_addresses_size],
        ));
    }

    pub inline fn filter_addresses_used(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) []const u64 {
        const slice: []const u64 = @alignCast(mem.bytesAsSlice(
            u64,
            index_block[index.filter_addresses_offset..][0..index.filter_addresses_size],
        ));
        return slice[0..index.filter_blocks_used(index_block)];
    }

    pub inline fn filter_checksums(index: *const TableIndex, index_block: BlockPtr) []u128 {
        return @alignCast(mem.bytesAsSlice(
            u128,
            index_block[index.filter_checksums_offset..][0..index.filter_checksums_size],
        ));
    }

    pub inline fn filter_checksums_used(
        index: *const TableIndex,
        index_block: BlockPtrConst,
    ) []const u128 {
        const slice = mem.bytesAsSlice(
            u128,
            index_block[index.filter_checksums_offset..][0..index.filter_checksums_size],
        );
        return @alignCast(slice[0..index.filter_blocks_used(index_block)]);
    }

    inline fn blocks_used(index: *const TableIndex, index_block: BlockPtrConst) u32 {
        return index_block_count + index.filter_blocks_used(index_block) +
            data_blocks_used(index_block);
    }

    inline fn filter_blocks_used(index: *const TableIndex, index_block: BlockPtrConst) u32 {
        const header = header_from_block(index_block);
        const context = @as(Context, @bitCast(header.context));
        const value = @as(u32, @intCast(context.filter_block_count));
        assert(value > 0);
        assert(value <= index.filter_block_count_max);
        return value;
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

pub const TableFilter = struct {
    /// Stored in every filter block's header's `context` field.
    pub const Context = extern struct {
        data_block_count_max: u32,
        reserved: [12]u8 = [_]u8{0} ** 12,

        comptime {
            assert(@sizeOf(Context) == @sizeOf(u128));
            assert(stdx.no_padding(Context));
        }
    };

    pub const Parent = extern struct {
        tree_id: u16,
        padding: [14]u8 = [_]u8{0} ** 14,

        comptime {
            assert(@sizeOf(Parent) == @sizeOf(u128));
            assert(stdx.no_padding(Parent));
        }
    };

    /// The number of data blocks summarized by a single filter block.
    data_block_count_max: u32,

    filter_offset: u32,
    filter_size: u32,
    padding_offset: u32,
    padding_size: u32,

    pub fn init(parameters: Context) TableFilter {
        assert(parameters.data_block_count_max > 0);
        assert(stdx.zeroed(&parameters.reserved));

        const filter_offset = @sizeOf(vsr.Header);
        const filter_size = constants.block_size - filter_offset;
        assert(filter_size == block_body_size);

        const padding_offset = filter_offset + filter_size;
        const padding_size = constants.block_size - padding_offset;
        assert(padding_size == 0);

        return .{
            .data_block_count_max = parameters.data_block_count_max,
            .filter_offset = filter_offset,
            .filter_size = filter_size,
            .padding_offset = padding_offset,
            .padding_size = padding_size,
        };
    }

    pub fn from(filter_block: BlockPtrConst) TableFilter {
        const header = header_from_block(filter_block);
        assert(header.command == .block);
        assert(BlockType.from(header.operation) == .filter);
        assert(header.op > 0);
        assert(header.timestamp > 0);

        return TableFilter.init(@as(Context, @bitCast(header.context)));
    }

    pub fn tree_id(filter_block: BlockPtrConst) u128 {
        const header = header_from_block(filter_block);
        assert(BlockType.from(header.operation) == .filter);

        return @as(Parent, @bitCast(header.parent)).tree_id;
    }

    pub inline fn block_filter(
        filter: *const TableFilter,
        filter_block: BlockPtr,
    ) []u8 {
        return filter_block[filter.filter_offset..][0..filter.filter_size];
    }

    pub inline fn block_filter_const(
        filter: *const TableFilter,
        filter_block: BlockPtrConst,
    ) []const u8 {
        return filter_block[filter.filter_offset..][0..filter.filter_size];
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
        padding: [14]u8 = [_]u8{0} ** 14,

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

    pub fn tree_id(data_block: BlockPtrConst) u128 {
        const header = header_from_block(data_block);
        assert(BlockType.from(header.operation) == .data);

        return @as(Parent, @bitCast(header.parent)).tree_id;
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
        // TODO we should be able to cross-check this with the header size
        // for more safety.
        const used_values: u32 = header.request;
        assert(used_values > 0);
        assert(used_values <= schema.value_count_max);

        const used_bytes = used_values * schema.value_size;
        return schema.block_values_bytes_const(data_block)[0..used_bytes];
    }
};
