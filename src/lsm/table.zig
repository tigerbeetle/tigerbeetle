const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const binary_search = @import("binary_search.zig");
const bloom_filter = @import("bloom_filter.zig");

const stdx = @import("../stdx.zig");
const div_ceil = stdx.div_ceil;
const snapshot_latest = @import("tree.zig").snapshot_latest;
const key_fingerprint = @import("tree.zig").key_fingerprint;

const allocate_block = @import("../vsr/grid.zig").allocate_block;
const TreeTableInfoType = @import("manifest.zig").TreeTableInfoType;
const schema = @import("schema.zig");

pub const TableUsage = enum {
    /// General purpose table.
    general,
    /// If your usage fits this pattern:
    /// * Only put keys which are not present.
    /// * Only remove keys which are present.
    /// * TableKey == TableValue (modulo padding, eg CompositeKey)
    /// Then we can unlock additional optimizations.
    secondary_index,
};

const address_size = @sizeOf(u64);
const checksum_size = @sizeOf(u128);

const block_size = constants.block_size;
const block_body_size = block_size - @sizeOf(vsr.Header);

const BlockPtr = *align(constants.sector_size) [block_size]u8;
const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;

/// A table is a set of blocks:
///
/// * Index block (exactly 1)
/// * Filter blocks (at least one, at most `filter_block_count_max`)
///   Each filter block summarizes the keys for several adjacent (in terms of key) data blocks.
/// * Data blocks (at least one, at most `data_block_count_max`)
///   Store the actual keys/values, along with a small index of the keys to optimize lookups.
pub fn TableType(
    comptime TableKey: type,
    comptime TableValue: type,
    /// Returns the sort order between two keys.
    comptime table_compare_keys: fn (TableKey, TableKey) callconv(.Inline) math.Order,
    /// Returns the key for a value. For example, given `object` returns `object.id`.
    /// Since most objects contain an id, this avoids duplicating the key when storing the value.
    comptime table_key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    /// Must compare greater than all other keys.
    comptime table_sentinel_key: TableKey,
    /// Returns whether a value is a tombstone value.
    comptime table_tombstone: fn (*const TableValue) callconv(.Inline) bool,
    /// Returns a tombstone value representation for a key.
    comptime table_tombstone_from_key: fn (TableKey) callconv(.Inline) TableValue,
    /// The maximum number of values per table.
    comptime table_value_count_max: usize,
    comptime table_usage: TableUsage,
) type {
    return struct {
        const Table = @This();

        // Re-export all the generic arguments.
        pub const Key = TableKey;
        pub const Value = TableValue;
        pub const compare_keys = table_compare_keys;
        pub const key_from_value = table_key_from_value;
        pub const sentinel_key = table_sentinel_key;
        pub const tombstone = table_tombstone;
        pub const tombstone_from_key = table_tombstone_from_key;
        pub const value_count_max = table_value_count_max;
        pub const usage = table_usage;

        // Export hashmap context for Key and Value
        pub const HashMapContextValue = struct {
            pub inline fn eql(_: HashMapContextValue, a: Value, b: Value) bool {
                return compare_keys(key_from_value(&a), key_from_value(&b)) == .eq;
            }

            pub inline fn hash(_: HashMapContextValue, value: Value) u64 {
                return stdx.hash_inline(key_from_value(&value));
            }
        };

        pub const key_size = @sizeOf(Key);
        pub const value_size = @sizeOf(Value);

        comptime {
            assert(@alignOf(Key) == 8 or @alignOf(Key) == 16);
            // TODO(ifreund) What are our alignment expectations for Value?

            // There must be no padding in the Key/Value types to avoid buffer bleeds.
            assert(stdx.no_padding(Key));
            assert(stdx.no_padding(Value));

            // These impact our calculation of:
            // * the superblock trailer size, and
            // * the manifest log layout for alignment.
            assert(key_size >= 8);
            assert(key_size <= 32);
            assert(key_size == 8 or key_size == 16 or key_size == 24 or key_size == 32);
        }

        pub const layout = layout: {
            @setEvalBranchQuota(10_000);

            assert(block_size % constants.sector_size == 0);
            assert(math.isPowerOfTwo(block_size));

            // If the index is smaller than 16 keys then there are key sizes >= 4 such that
            // the total index size is not 64 byte cache line aligned.
            assert(@sizeOf(Key) >= 4);
            assert(@sizeOf(Key) % 4 == 0);

            const block_value_count_max = @divFloor(
                block_body_size,
                value_size,
            );

            // TODO Audit/tune this number for split block bloom filters:
            const filter_bytes_per_key = 2;
            const filter_data_block_count_max = @divFloor(
                block_body_size,
                block_value_count_max * filter_bytes_per_key,
            );

            // We need enough blocks to hold `value_count_max` values.
            const data_blocks = div_ceil(value_count_max, block_value_count_max);
            const filter_blocks = div_ceil(data_blocks, filter_data_block_count_max);

            break :layout .{
                // The maximum number of values in a data block.
                .block_value_count_max = block_value_count_max,

                .data_block_count_max = data_blocks,
                .filter_block_count_max = filter_blocks,

                // The number of data blocks covered by a single filter block.
                .filter_data_block_count_max = @min(
                    filter_data_block_count_max,
                    data_blocks,
                ),
            };
        };

        const index_block_count = 1;
        pub const filter_block_count_max = layout.filter_block_count_max;
        pub const data_block_count_max = layout.data_block_count_max;
        pub const block_count_max =
            index_block_count + filter_block_count_max + data_block_count_max;

        const index = schema.TableIndex.init(.{
            .key_size = key_size,
            .filter_block_count_max = filter_block_count_max,
            .data_block_count_max = data_block_count_max,
        });

        const filter = schema.TableFilter.init(.{
            .data_block_count_max = layout.filter_data_block_count_max,
        });

        pub const data = schema.TableData.init(.{
            .value_count_max = layout.block_value_count_max,
            .value_size = value_size,
        });

        const compile_log_layout = false;
        comptime {
            if (compile_log_layout) {
                @compileError(std.fmt.comptimePrint(
                    \\
                    \\
                    \\lsm parameters:
                    \\    value: {}
                    \\    value count max: {}
                    \\    key size: {}
                    \\    value size: {}
                    \\    block size: {}
                    \\layout:
                    \\    index block count: {}
                    \\    filter block count max: {}
                    \\    data block count max: {}
                    \\index:
                    \\    size: {}
                    \\    filter_checksums_offset: {}
                    \\    filter_checksums_size: {}
                    \\    data_checksums_offset: {}
                    \\    data_checksums_size: {}
                    \\    keys_offset: {}
                    \\    keys_size: {}
                    \\    filter_addresses_offset: {}
                    \\    filter_addresses_size: {}
                    \\    data_addresses_offset: {}
                    \\    data_addresses_size: {}
                    \\filter:
                    \\    data_block_count_max: {}
                    \\    filter_offset: {}
                    \\    filter_size: {}
                    \\    padding_offset: {}
                    \\    padding_size: {}
                    \\data:
                    \\    value_count_max: {}
                    \\    values_offset: {}
                    \\    values_size: {}
                    \\    padding_offset: {}
                    \\    padding_size: {}
                    \\
                ,
                    .{
                        Value,
                        value_count_max,
                        key_size,
                        value_size,
                        block_size,

                        index_block_count,
                        filter_block_count_max,
                        data_block_count_max,

                        index.size,
                        index.filter_checksums_offset,
                        index.filter_checksums_size,
                        index.data_checksums_offset,
                        index.data_checksums_size,
                        index.keys_offset,
                        index.keys_size,
                        index.filter_addresses_offset,
                        index.filter_addresses_size,
                        index.data_addresses_offset,
                        index.data_addresses_size,

                        filter.data_block_count_max,
                        filter.filter_offset,
                        filter.filter_size,
                        filter.padding_offset,
                        filter.padding_size,

                        data.value_count_max,
                        data.values_offset,
                        data.values_size,
                        data.padding_offset,
                        data.padding_size,
                    },
                ));
            }
        }

        comptime {
            assert(index_block_count > 0);
            assert(filter_block_count_max > 0);
            assert(data_block_count_max > 0);

            assert(filter.data_block_count_max > 0);
            // There should not be more data blocks per filter block than there are data blocks:
            assert(filter.data_block_count_max <= data_block_count_max);

            const filter_bytes_per_key = 2;
            assert(filter_block_count_max * filter.filter_size >=
                data_block_count_max * data.value_count_max * filter_bytes_per_key);

            assert(index.size == @sizeOf(vsr.Header) +
                data_block_count_max * (key_size + address_size + checksum_size) +
                filter_block_count_max * (address_size + checksum_size));
            assert(index.size == index.data_addresses_offset + index.data_addresses_size);
            assert(index.size <= block_size);
            assert(index.keys_size > 0);
            assert(index.keys_size % key_size == 0);
            assert(@divExact(index.data_addresses_size, @sizeOf(u64)) == data_block_count_max);
            assert(@divExact(index.filter_addresses_size, @sizeOf(u64)) == filter_block_count_max);
            assert(@divExact(index.data_checksums_size, @sizeOf(u128)) == data_block_count_max);
            assert(@divExact(index.filter_checksums_size, @sizeOf(u128)) == filter_block_count_max);
            assert(block_size == index.padding_offset + index.padding_size);
            assert(block_size == index.size + index.padding_size);

            // Split block bloom filters require filters to be a multiple of 32 bytes as they
            // use 256 bit blocks.
            assert(filter.filter_size % 32 == 0);
            assert(filter.filter_size == block_body_size);
            assert(block_size == filter.padding_offset + filter.padding_size);
            assert(block_size == @sizeOf(vsr.Header) + filter.filter_size + filter.padding_size);

            assert(data.value_count_max > 0);
            assert(@divExact(data.values_size, value_size) == data.value_count_max);
            assert(data.values_offset % constants.cache_line_size == 0);
            // You can have any size value you want, as long as it fits
            // neatly into the CPU cache lines :)
            assert((data.value_count_max * value_size) % constants.cache_line_size == 0);

            assert(data.padding_size >= 0);
            assert(block_size == @sizeOf(vsr.Header) + data.values_size + data.padding_size);
            assert(block_size == data.padding_offset + data.padding_size);

            // We expect no block padding at least for TigerBeetle's objects and indexes:
            if ((key_size == 8 and value_size == 128) or
                (key_size == 8 and value_size == 64) or
                (key_size == 16 and value_size == 16) or
                (key_size == 32 and value_size == 32))
            {
                assert(data.padding_size == 0);
            }
        }

        pub const Builder = struct {
            const TreeTableInfo = TreeTableInfoType(Table);

            key_min: Key = undefined, // Inclusive.
            key_max: Key = undefined, // Inclusive.

            index_block: BlockPtr,
            filter_block: BlockPtr,
            data_block: BlockPtr,

            data_block_count: u32 = 0,
            value_count: u32 = 0,

            filter_block_count: u32 = 0,
            data_blocks_in_filter: u32 = 0,

            pub fn init(allocator: mem.Allocator) !Builder {
                const index_block = try allocate_block(allocator);
                errdefer allocator.free(index_block);

                const filter_block = try allocate_block(allocator);
                errdefer allocator.free(filter_block);

                const data_block = try allocate_block(allocator);
                errdefer allocator.free(data_block);

                return Builder{
                    .index_block = index_block,
                    .filter_block = filter_block,
                    .data_block = data_block,
                };
            }

            pub fn deinit(builder: *Builder, allocator: mem.Allocator) void {
                allocator.free(builder.index_block);
                allocator.free(builder.filter_block);
                allocator.free(builder.data_block);

                builder.* = undefined;
            }

            pub fn reset(builder: *Builder) void {
                @memset(builder.index_block, 0);
                @memset(builder.filter_block, 0);
                @memset(builder.data_block, 0);

                builder.* = .{
                    .index_block = builder.index_block,
                    .filter_block = builder.filter_block,
                    .data_block = builder.data_block,
                };
            }

            pub fn data_block_values(builder: *Builder) []Value {
                return Table.data_block_values(builder.data_block);
            }

            pub fn data_block_empty(builder: *const Builder) bool {
                assert(builder.value_count <= data.value_count_max);
                return builder.value_count == 0;
            }

            pub fn data_block_full(builder: *const Builder) bool {
                assert(builder.value_count <= data.value_count_max);
                return builder.value_count == data.value_count_max;
            }

            const DataFinishOptions = struct {
                cluster: u32,
                address: u64,
                snapshot_min: u64,
                tree_id: u16,
            };

            pub fn data_block_finish(builder: *Builder, options: DataFinishOptions) void {
                // For each block we write the sorted values,
                // complete the block header, and add the block's max key to the table index.

                assert(options.address > 0);
                assert(builder.value_count > 0);

                const block = builder.data_block;
                const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                header.* = .{
                    .cluster = options.cluster,
                    .parent = @bitCast(schema.TableData.Parent{ .tree_id = options.tree_id }),
                    .context = @bitCast(schema.TableData.Context{
                        .value_count_max = data.value_count_max,
                        .value_size = value_size,
                    }),
                    .op = options.address,
                    .timestamp = options.snapshot_min,
                    .request = builder.value_count,
                    .size = @sizeOf(vsr.Header) + builder.value_count * @sizeOf(Value),
                    .command = .block,
                    .operation = schema.BlockType.data.operation(),
                };

                header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                const values = Table.data_block_values_used(block);
                { // Now that we have checksummed the block, sanity-check the result:

                    if (constants.verify) {
                        var a = &values[0];
                        for (values[1..]) |*b| {
                            assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                            a = b;
                        }
                    }

                    assert(builder.value_count == values.len);
                    assert(block_size - header.size ==
                        (data.value_count_max - values.len) * @sizeOf(Value) + data.padding_size);
                    // Padding is short on average, so assert unconditionally.
                    assert(stdx.zeroed(block[header.size..]));
                }

                { // Update the filter block:
                    if (constants.verify) {
                        if (builder.data_blocks_in_filter == 0) {
                            assert(stdx.zeroed(filter.block_filter(builder.filter_block)));
                        }
                    }

                    if (comptime Table.usage == .general) {
                        const filter_bytes = filter.block_filter(builder.filter_block);
                        for (values) |*value| {
                            const key = key_from_value(value);
                            const fingerprint = key_fingerprint(key);
                            bloom_filter.add(fingerprint, filter_bytes);
                        }
                    }
                }

                const key_max = key_from_value(&values[values.len - 1]);
                const current = builder.data_block_count;
                { // Update the index block:
                    index_data_keys(builder.index_block)[current] = key_max;
                    index.data_addresses(builder.index_block)[current] = options.address;
                    index.data_checksums(builder.index_block)[current] = header.checksum;
                }

                if (current == 0) builder.key_min = key_from_value(&values[0]);
                builder.key_max = key_max;

                if (current == 0 and values.len == 1) {
                    assert(compare_keys(builder.key_min, builder.key_max) == .eq);
                } else {
                    assert(compare_keys(builder.key_min, builder.key_max) == .lt);
                }
                assert(compare_keys(builder.key_max, sentinel_key) == .lt);

                if (current > 0) {
                    const key_max_prev = index_data_keys(builder.index_block)[current - 1];
                    assert(compare_keys(key_max_prev, key_from_value(&values[0])) == .lt);
                }

                builder.data_block_count += 1;
                builder.value_count = 0;

                builder.data_blocks_in_filter += 1;
            }

            pub fn filter_block_empty(builder: *const Builder) bool {
                assert(builder.data_blocks_in_filter <= filter.data_block_count_max);
                return builder.data_blocks_in_filter == 0;
            }

            pub fn filter_block_full(builder: *const Builder) bool {
                assert(builder.data_blocks_in_filter <= filter.data_block_count_max);
                return builder.data_blocks_in_filter == filter.data_block_count_max;
            }

            const FilterFinishOptions = struct {
                cluster: u32,
                address: u64,
                snapshot_min: u64,
                tree_id: u16,
            };

            pub fn filter_block_finish(builder: *Builder, options: FilterFinishOptions) void {
                assert(!builder.filter_block_empty());
                assert(builder.data_block_empty());
                assert(options.address > 0);

                const header = mem.bytesAsValue(vsr.Header, builder.filter_block[0..@sizeOf(vsr.Header)]);
                header.* = .{
                    .cluster = options.cluster,
                    .parent = @bitCast(schema.TableFilter.Parent{
                        .tree_id = options.tree_id,
                    }),
                    .context = @bitCast(schema.TableFilter.Context{
                        .data_block_count_max = data_block_count_max,
                    }),
                    .op = options.address,
                    .timestamp = options.snapshot_min,
                    .size = block_size - filter.padding_size,
                    .command = .block,
                    .operation = schema.BlockType.filter.operation(),
                };

                const body = builder.filter_block[@sizeOf(vsr.Header)..header.size];
                header.set_checksum_body(body);
                header.set_checksum();

                const current = builder.filter_block_count;
                index.filter_addresses(builder.index_block)[current] = options.address;
                index.filter_checksums(builder.index_block)[current] = header.checksum;

                builder.filter_block_count += 1;
                builder.data_blocks_in_filter = 0;
            }

            pub fn index_block_empty(builder: *const Builder) bool {
                assert(builder.data_block_count <= data_block_count_max);
                return builder.data_block_count == 0;
            }

            pub fn index_block_full(builder: *const Builder) bool {
                assert(builder.data_block_count <= data_block_count_max);
                return builder.data_block_count == data_block_count_max;
            }

            const IndexFinishOptions = struct {
                cluster: u32,
                address: u64,
                snapshot_min: u64,
                tree_id: u16,
            };

            pub fn index_block_finish(
                builder: *Builder,
                options: IndexFinishOptions,
            ) TreeTableInfo {
                assert(options.address > 0);
                assert(builder.filter_block_empty());
                assert(builder.data_block_empty());
                assert(builder.data_block_count > 0);
                assert(builder.value_count == 0);
                assert(builder.data_blocks_in_filter == 0);
                assert(builder.filter_block_count == div_ceil(
                    builder.data_block_count,
                    filter.data_block_count_max,
                ));

                const index_block = builder.index_block;
                const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
                header.* = .{
                    .cluster = options.cluster,
                    .parent = @bitCast(schema.TableIndex.Parent{
                        .tree_id = options.tree_id,
                    }),
                    .context = @bitCast(schema.TableIndex.Context{
                        .filter_block_count = builder.filter_block_count,
                        .filter_block_count_max = index.filter_block_count_max,
                        .data_block_count = builder.data_block_count,
                        .data_block_count_max = index.data_block_count_max,
                    }),
                    .request = index.key_size,
                    .op = options.address,
                    .timestamp = options.snapshot_min,
                    .size = index.size,
                    .command = .block,
                    .operation = schema.BlockType.index.operation(),
                };
                header.set_checksum_body(index_block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                const info: TreeTableInfo = .{
                    .checksum = header.checksum,
                    .address = options.address,
                    .snapshot_min = options.snapshot_min,
                    .key_min = builder.key_min,
                    .key_max = builder.key_max,
                };

                assert(info.snapshot_max == math.maxInt(u64));

                // Reset the builder to its initial state, leaving the buffers untouched.
                builder.* = .{
                    .key_min = undefined,
                    .key_max = undefined,
                    .index_block = builder.index_block,
                    .filter_block = builder.filter_block,
                    .data_block = builder.data_block,
                };

                return info;
            }
        };

        pub inline fn index_data_keys(index_block: BlockPtr) []Key {
            return mem.bytesAsSlice(Key, index_block[index.keys_offset..][0..index.keys_size]);
        }

        pub inline fn index_data_keys_used(index_block: BlockPtrConst) []const Key {
            const slice = mem.bytesAsSlice(
                Key,
                index_block[index.keys_offset..][0..index.keys_size],
            );
            return slice[0..index.data_blocks_used(index_block)];
        }

        /// Returns the zero-based index of the data block that may contain the key.
        /// May be called on an index block only when the key is in range of the table.
        inline fn index_data_block_for_key(index_block: BlockPtrConst, key: Key) u32 {
            // Because we store key_max in the index block we can use the raw binary search
            // here and avoid the extra comparison. If the search finds an exact match, we
            // want to return that data block. If the search does not find an exact match
            // it returns the index of the next greatest key, which again is the index of the
            // data block that may contain the key.
            const data_block_index = binary_search.binary_search_keys_upsert_index(
                Key,
                compare_keys,
                Table.index_data_keys_used(index_block),
                key,
                .{},
            );
            assert(data_block_index < index.data_blocks_used(index_block));
            return data_block_index;
        }

        pub const IndexBlocks = struct {
            filter_block_address: u64,
            filter_block_checksum: u128,
            data_block_address: u64,
            data_block_checksum: u128,
        };

        /// Returns all data stored in the index block relating to a given key.
        /// May be called on an index block only when the key is in range of the table.
        pub inline fn index_blocks_for_key(index_block: BlockPtrConst, key: Key) IndexBlocks {
            const d = Table.index_data_block_for_key(index_block, key);
            const f = @divFloor(d, filter.data_block_count_max);

            return .{
                .filter_block_address = index.filter_addresses_used(index_block)[f],
                .filter_block_checksum = index.filter_checksums_used(index_block)[f],
                .data_block_address = index.data_addresses_used(index_block)[d],
                .data_block_checksum = index.data_checksums_used(index_block)[d],
            };
        }

        pub inline fn data_block_values(data_block: BlockPtr) []Value {
            return mem.bytesAsSlice(Value, data.block_values_bytes(data_block));
        }

        pub inline fn data_block_values_used(data_block: BlockPtrConst) []const Value {
            return mem.bytesAsSlice(Value, data.block_values_used_bytes(data_block));
        }

        pub inline fn block_address(block: BlockPtrConst) u64 {
            const header = schema.header_from_block(block);
            const address = header.op;
            assert(address > 0);
            return address;
        }

        pub fn data_block_search(data_block: BlockPtrConst, key: Key) ?*const Value {
            const values = data_block_values_used(data_block);

            const result = binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                compare_keys,
                values,
                key,
                .{},
            );
            if (result.exact) {
                const value = &values[result.index];
                if (constants.verify) {
                    assert(compare_keys(key, key_from_value(value)) == .eq);
                }
                return value;
            }

            if (constants.verify) {
                for (values) |*value| {
                    assert(compare_keys(key, key_from_value(value)) != .eq);
                }
            }

            return null;
        }

        pub fn verify(
            comptime Storage: type,
            storage: *Storage,
            index_address: u64,
            key_min: ?Key,
            key_max: ?Key,
        ) void {
            if (Storage != @import("../testing/storage.zig").Storage)
                // Too complicated to do async verification
                return;

            const index_block = storage.grid_block(index_address);
            const addresses = index.data_addresses(index_block);
            const data_blocks_used = index.data_blocks_used(index_block);
            for (0..data_blocks_used) |data_block_index| {
                const address = addresses[data_block_index];
                const data_block = storage.grid_block(address);
                const values = data_block_values_used(data_block);
                if (values.len > 0) {
                    if (data_block_index == 0) {
                        assert(key_min == null or
                            compare_keys(key_min.?, key_from_value(&values[0])) == .eq);
                    }
                    if (data_block_index == data_blocks_used - 1) {
                        assert(key_max == null or
                            compare_keys(key_from_value(&values[values.len - 1]), key_max.?) == .eq);
                    }
                    var a = &values[0];
                    for (values[1..]) |*b| {
                        assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                        a = b;
                    }
                }
            }
        }
    };
}

test "Table" {
    const Key = @import("composite_key.zig").CompositeKey(u128);

    const Table = TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
        1, // Doesn't matter for this test.
        .general,
    );

    std.testing.refAllDecls(Table.Builder);
}
