const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");
const binary_search = @import("binary_search.zig");
const bloom_filter = @import("bloom_filter.zig");

const div_ceil = @import("../util.zig").div_ceil;
const eytzinger = @import("eytzinger.zig").eytzinger;
const snapshot_latest = @import("tree.zig").snapshot_latest;

const GridType = @import("grid.zig").GridType;
const ManifestType = @import("manifest.zig").ManifestType;

pub fn TableType(
    comptime TableStorage: type,
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
) type {
    return struct {
        const Table = @This();
        const Grid = GridType(Storage);
        const Manifest = ManifestType(Table);

        // Re-export all the generic arguments.
        pub const Storage = TableStorage;
        pub const Key = TableKey;
        pub const Value = TableValue;
        pub const compare_keys = table_compare_keys;
        pub const key_from_value = table_key_from_value;
        pub const sentinel_key = table_sentinel_key;
        pub const tombstone = table_tombstone;
        pub const tombstone_from_key = table_tombstone_from_key;

        // Export hashmap context for Key and Value
        pub const HashMapContextValue = struct {
            pub fn eql(_: HashMapContextValue, a: Value, b: Value) bool {
                return compare_keys(key_from_value(&a), key_from_value(&b)) == .eq;
            }

            pub fn hash(_: HashMapContextValue, value: Value) u64 {
                const key = key_from_value(&value);
                return std.hash_map.getAutoHashFn(Key, HashMapContextValue)(.{}, key);
            }
        };

        // Taken from tree.zig
        pub const block_size = config.block_size;
        pub const BlockPtr = *align(config.sector_size) [block_size]u8;
        pub const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

        pub const key_size = @sizeOf(Key);
        pub const value_size = @sizeOf(Value);

        comptime {
            assert(@alignOf(Key) == 8 or @alignOf(Key) == 16);
            // TODO(ifreund) What are our alignment expectations for Value?

            // There must be no padding in the Key/Value types to avoid buffer bleeds.
            assert(@bitSizeOf(Key) == @sizeOf(Key) * 8);
            assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);

            // We can relax these if necessary. These impact our calculation of the superblock trailer size.
            assert(key_size >= 8);
            assert(key_size <= 32);
        }

        const address_size = @sizeOf(u64);
        const checksum_size = @sizeOf(u128);
        const table_size_max = config.lsm_table_size_max;
        const table_block_count_max = @divExact(table_size_max, block_size);
        const block_body_size = block_size - @sizeOf(vsr.Header);

        pub const layout = blk: {
            assert(block_size % config.sector_size == 0);
            assert(math.isPowerOfTwo(table_size_max));
            assert(math.isPowerOfTwo(block_size));

            // Searching the values array is more expensive than searching the per-block index
            // as the larger values size leads to more cache misses. We can therefore speed
            // up lookups by making the per block index larger at the cost of reducing the
            // number of values that may be stored per block.
            //
            // X = values per block
            // Y = keys per block
            //
            // R = config.lsm_value_to_key_layout_ratio_min
            //
            // To maximize:
            //     Y
            // Given constraints:
            //     body >= X * value_size + Y * key_size
            //     (X * value_size) / (Y * key_size) >= R
            //     X >= Y
            //
            // Plots of above constraints:
            //     https://www.desmos.com/calculator/elqqaalgbc
            //
            // body - X * value_size = Y * key_size
            // Y = (body - X * value_size) / key_size
            //
            // (X * value_size) / (body - X * value_size) = R
            // (X * value_size) = R * body - R * X * value_size
            // (R + 1) * X * value_size = R * body
            // X = R * body / ((R + 1)* value_size)
            //
            // Y = (body - (R * body / ((R + 1) * value_size)) * value_size) / key_size
            // Y = (body - (R / (R + 1)) * body) / key_size
            // Y = body / ((R + 1) * key_size)
            var block_keys_layout_count = math.min(
                block_body_size / ((config.lsm_value_to_key_layout_ratio_min + 1) * key_size),
                block_body_size / (value_size + key_size),
            );

            // Round to the next lowest power of two. This speeds up lookups in the Eytzinger
            // layout and should help ensure better alignment for the following values.
            // We could round to the nearest power of two, but then we would need
            // care to avoid breaking e.g. the X >= Y invariant above.
            block_keys_layout_count = math.floorPowerOfTwo(u64, block_keys_layout_count);

            // If the index is smaller than 16 keys then there are key sizes >= 4 such that
            // the total index size is not 64 byte cache line aligned.
            assert(@sizeOf(Key) >= 4);
            assert(@sizeOf(Key) % 4 == 0);
            if (block_keys_layout_count < @divExact(config.cache_line_size, 4)) {
                block_keys_layout_count = 0;
            }
            assert((block_keys_layout_count * key_size) % config.cache_line_size == 0);

            const block_key_layout_size = block_keys_layout_count * key_size;
            const block_key_count = block_keys_layout_count - 1;

            const block_value_count_max = @divFloor(
                block_body_size - block_key_layout_size,
                value_size,
            );

            const data_index_entry_size = key_size + address_size + checksum_size;
            const filter_index_entry_size = address_size + checksum_size;

            // TODO Audit/tune this number for split block bloom filters:
            const filter_bytes_per_key = 2;
            const filter_data_block_count_max = @divFloor(
                block_body_size,
                block_value_count_max * filter_bytes_per_key,
            );

            var data_blocks = table_block_count_max - index_block_count;
            var data_index_size = 0;
            var filter_blocks = 0;
            var filter_index_size = 0;
            while (true) : (data_blocks -= 1) {
                data_index_size = data_index_entry_size * data_blocks;

                filter_blocks = div_ceil(data_blocks, filter_data_block_count_max);
                filter_index_size = filter_index_entry_size * filter_blocks;

                const index_size = @sizeOf(vsr.Header) + data_index_size + filter_index_size;
                const table_block_count = index_block_count + filter_blocks + data_blocks;
                if (index_size <= block_size and table_block_count <= table_block_count_max) {
                    break;
                }
            }

            const table_block_count = index_block_count + filter_blocks + data_blocks;
            assert(table_block_count <= table_block_count_max);

            break :blk .{
                .block_key_count = block_key_count,
                .block_key_layout_size = block_key_layout_size,
                .block_value_count_max = block_value_count_max,

                .data_block_count_max = data_blocks,
                .filter_block_count_max = filter_blocks,

                .filter_data_block_count_max = filter_data_block_count_max,
            };
        };

        const index_block_count = 1;
        const filter_block_count_max = layout.filter_block_count_max;
        pub const data_block_count_max = layout.data_block_count_max;

        const index = struct {
            const size = @sizeOf(vsr.Header) + filter_checksums_size + data_checksums_size +
                keys_size + filter_addresses_size + data_addresses_size;

            const filter_checksums_offset = @sizeOf(vsr.Header);
            const filter_checksums_size = filter_block_count_max * checksum_size;

            const data_checksums_offset = filter_checksums_offset + filter_checksums_size;
            const data_checksums_size = data_block_count_max * checksum_size;

            const keys_offset = data_checksums_offset + data_checksums_size;
            const keys_size = data_block_count_max * key_size;

            const filter_addresses_offset = keys_offset + keys_size;
            const filter_addresses_size = filter_block_count_max * address_size;

            const data_addresses_offset = filter_addresses_offset + filter_addresses_size;
            const data_addresses_size = data_block_count_max * address_size;

            const padding_offset = data_addresses_offset + data_addresses_size;
            const padding_size = block_size - padding_offset;
        };

        const filter = struct {
            const data_block_count_max = layout.filter_data_block_count_max;

            const filter_offset = @sizeOf(vsr.Header);
            const filter_size = block_size - filter_offset;

            const padding_offset = filter_offset + filter_size;
            const padding_size = block_size - padding_offset;
        };

        pub const data = struct {
            const key_count = layout.block_key_count;
            pub const value_count_max = layout.block_value_count_max;

            const key_layout_offset = @sizeOf(vsr.Header);
            const key_layout_size = layout.block_key_layout_size;

            const values_offset = key_layout_offset + key_layout_size;
            const values_size = value_count_max * value_size;

            const padding_offset = values_offset + values_size;
            const padding_size = block_size - padding_offset;
        };

        const compile_log_layout = false;
        comptime {
            if (compile_log_layout) {
                @compileError(std.fmt.comptimePrint(
                    \\
                    \\
                    \\lsm parameters:
                    \\    key size: {}
                    \\    value size: {}
                    \\    table size max: {}
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
                    \\    key_count: {}
                    \\    value_count_max: {}
                    \\    key_layout_offset: {}
                    \\    key_layout_size: {}
                    \\    values_offset: {}
                    \\    values_size: {}
                    \\    padding_offset: {}
                    \\    padding_size: {}
                    \\
                ,
                    .{
                        key_size,
                        value_size,
                        table_size_max,
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

                        data.key_count,
                        data.value_count_max,
                        data.key_layout_offset,
                        data.key_layout_size,
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
            assert(index_block_count + filter_block_count_max +
                data_block_count_max <= table_block_count_max);

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

            if (data.key_count > 0) {
                assert(data.key_count >= 3);
                assert(math.isPowerOfTwo(data.key_count + 1));
                assert(data.key_count + 1 == @divExact(data.key_layout_size, key_size));
                assert(data.values_size / data.key_layout_size >=
                    config.lsm_value_to_key_layout_ratio_min);
            } else {
                assert(data.key_count == 0);
                assert(data.key_layout_size == 0);
                assert(data.values_offset == data.key_layout_offset);
            }

            assert(data.value_count_max > 0);
            assert(data.value_count_max >= data.key_count);
            assert(@divExact(data.values_size, value_size) == data.value_count_max);
            assert(data.values_offset % config.cache_line_size == 0);
            // You can have any size value you want, as long as it fits
            // neatly into the CPU cache lines :)
            assert((data.value_count_max * value_size) % config.cache_line_size == 0);

            assert(data.padding_size >= 0);
            assert(block_size == @sizeOf(vsr.Header) + data.key_layout_size +
                data.values_size + data.padding_size);
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

        fn blocks_used(table: *Table) u32 {
            assert(!table.free);
            return Table.index_blocks_used(&table.blocks[0]);
        }

        fn filter_blocks_used(table: *Table) u32 {
            assert(!table.free);
            return Table.index_filter_blocks_used(&table.blocks[0]);
        }

        pub const Builder = struct {
            grid: *Grid,
            key_min: Key = undefined,
            key_max: Key = undefined,

            index_block: BlockPtr,
            filter_block: BlockPtr,
            data_block: BlockPtr,

            data_block_count: u32 = 0,
            value: u32 = 0,

            filter_block_count: u32 = 0,
            data_blocks_in_filter: u32 = 0,

            pub fn init(allocator: mem.Allocator) !Builder {
                _ = allocator;

                // TODO
                return error.ToDo;
            }

            pub fn deinit(builder: *Builder, allocator: mem.Allocator) void {
                _ = builder;
                _ = allocator;

                // TODO
            }

            pub fn data_block_append(builder: *Builder, value: *const Value) void {
                const values_max = data_block_values(builder.data_block);
                assert(values_max.len == data.value_count_max);

                values_max[builder.value] = value.*;
                builder.value += 1;

                const key = key_from_value(value);
                const fingerprint = bloom_filter.Fingerprint.create(mem.asBytes(&key));
                bloom_filter.add(fingerprint, filter_block_filter(builder.filter_block));
            }

            pub fn data_block_append_slice(builder: *Builder, values: []const Value) void {
                assert(values.len > 0);
                assert(builder.value + values.len <= data.value_count_max);

                const values_max = data_block_values(builder.data_block);
                assert(values_max.len == data.value_count_max);

                mem.copy(Value, values_max[builder.value..], values);
                builder.value += @intCast(u32, values.len);

                for (values) |value| {
                    const key = key_from_value(value);
                    const fingerprint = bloom_filter.Fingerprint.create(mem.asBytes(&key));
                    bloom_filter.add(fingerprint, filter_block_filter(builder.filter_block));
                }
            }

            pub fn data_block_empty(builder: Builder) bool {
                assert(builder.value <= data.value_count_max);
                return builder.value == 0;
            }

            pub fn data_block_full(builder: Builder) bool {
                assert(builder.value <= data.value_count_max);
                return builder.value == data.value_count_max;
            }

            pub fn data_block_finish(builder: *Builder) void {
                // For each block we write the sorted values, initialize the Eytzinger layout,
                // complete the block header, and add the block's max key to the table index.

                assert(builder.value > 0);

                const block = builder.data_block;
                const values_max = data_block_values(block);
                assert(values_max.len == data.value_count_max);

                const values = values_max[0..builder.value];

                if (config.verify) {
                    var a = &values[0];
                    for (values[1..]) |*b| {
                        assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                        a = b;
                    }
                }

                assert(@divExact(data.key_layout_size, key_size) == data.key_count + 1);
                const key_layout_bytes = @alignCast(
                    @alignOf(Key),
                    block[data.key_layout_offset..][0..data.key_layout_size],
                );
                const key_layout = mem.bytesAsValue([data.key_count + 1]Key, key_layout_bytes);

                const e = eytzinger(data.key_count, data.value_count_max);
                e.layout_from_keys_or_values(
                    Key,
                    Value,
                    key_from_value,
                    sentinel_key,
                    values,
                    key_layout,
                );

                const values_padding = mem.sliceAsBytes(values_max[builder.value..]);
                const block_padding = block[data.padding_offset..][0..data.padding_size];
                mem.set(u8, values_padding, 0);
                mem.set(u8, block_padding, 0);

                const header_bytes = block[0..@sizeOf(vsr.Header)];
                const header = mem.bytesAsValue(vsr.Header, header_bytes);

                const address = builder.grid.acquire();

                header.* = .{
                    .cluster = builder.grid.superblock.working.cluster,
                    .op = address,
                    .request = @intCast(u32, values.len),
                    .size = block_size - @intCast(u32, values_padding.len - block_padding.len),
                    .command = .block,
                };

                header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                const key_max = key_from_value(&values[values.len - 1]);

                const current = builder.data_block_count;
                index_data_keys(builder.index_block)[current] = key_max;
                index_data_addresses(builder.index_block)[current] = address;
                index_data_checksums(builder.index_block)[current] = header.checksum;

                if (current == 0) builder.key_min = key_from_value(&values[0]);
                builder.key_max = key_max;

                if (current == 0 and values.len == 1) {
                    assert(compare_keys(builder.key_min, builder.key_max) != .gt);
                } else {
                    assert(compare_keys(builder.key_min, builder.key_max) == .lt);
                }

                builder.data_block_count += 1;
                builder.value = 0;

                builder.data_blocks_in_filter += 1;
            }

            pub fn filter_block_empty(builder: Builder) bool {
                assert(builder.data_blocks_in_filter <= filter.data_block_count_max);
                return builder.data_blocks_in_filter == 0;
            }

            pub fn filter_block_full(builder: Builder) bool {
                assert(builder.data_blocks_in_filter <= filter.data_block_count_max);
                return builder.data_blocks_in_filter == filter.data_block_count_max;
            }

            pub fn filter_block_finish(builder: *Builder) void {
                assert(!builder.filter_block_empty());

                const address = builder.grid.acquire();

                const header_bytes = builder.filter_block[0..@sizeOf(vsr.Header)];
                const header = mem.bytesAsValue(vsr.Header, header_bytes);
                header.* = .{
                    .cluster = builder.grid.superblock.working.cluster,
                    .op = address,
                    .size = block_size - filter.padding_size,
                    .command = .block,
                };

                const body = builder.filter_block[@sizeOf(vsr.Header)..header.size];
                header.set_checksum_body(body);
                header.set_checksum();

                const current = builder.filter_block_count;
                index_filter_addresses(builder.index_block)[current] = address;
                index_filter_checksums(builder.index_block)[current] = header.checksum;

                builder.filter_block_count += 1;
                builder.data_blocks_in_filter = 0;
            }

            pub fn index_block_empty(builder: Builder) bool {
                assert(builder.data_block_count <= data_block_count_max);
                return builder.data_block_count == 0;
            }

            pub fn index_block_full(builder: Builder) bool {
                assert(builder.data_block_count <= data_block_count_max);
                return builder.data_block_count == data_block_count_max;
            }

            pub fn index_block_finish(builder: *Builder, snapshot_min: u64) Manifest.TableInfo {
                assert(builder.data_block_count > 0);
                assert(builder.value == 0);
                assert(builder.data_blocks_in_filter == 0);
                assert(builder.filter_block_count == div_ceil(
                    builder.data_block_count,
                    filter.data_block_count_max,
                ));

                const index_block = builder.index_block;

                const index_data_keys_padding = index_data_keys(index_block)[builder.data_block_count..];
                const index_data_keys_padding_bytes = mem.sliceAsBytes(index_data_keys_padding);
                mem.set(u8, index_data_keys_padding_bytes, 0);
                mem.set(u64, index_data_addresses(index_block)[builder.data_block_count..], 0);
                mem.set(u128, index_data_checksums(index_block)[builder.data_block_count..], 0);

                mem.set(u64, index_filter_addresses(index_block)[builder.filter_block_count..], 0);
                mem.set(u128, index_filter_checksums(index_block)[builder.filter_block_count..], 0);

                mem.set(u8, index_block[index.padding_offset..][0..index.padding_size], 0);

                const header_bytes = index_block[0..@sizeOf(vsr.Header)];
                const header = mem.bytesAsValue(vsr.Header, header_bytes);

                const address = builder.grid.acquire();
                _ = snapshot_min;

                header.* = .{
                    .cluster = builder.grid.superblock.working.cluster,
                    .op = address,
                    .commit = builder.filter_block_count,
                    .request = builder.data_block_count,
                    // .offset = snapshot_min, // TODO(King) not sure what to replace this with.
                    .size = index.size,
                    .command = .block,
                };
                header.set_checksum_body(index_block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                const info: Manifest.TableInfo = .{
                    .checksum = header.checksum,
                    .address = address,
                    .snapshot_min = snapshot_min,
                    .key_min = builder.key_min,
                    .key_max = builder.key_max,
                };

                assert(info.snapshot_max == math.maxInt(u64));

                // Reset the builder to its initial state, leaving the buffers untouched.
                builder.* = .{
                    .grid = builder.grid,
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

        inline fn index_data_keys_const(index_block: BlockPtrConst) []const Key {
            return mem.bytesAsSlice(Key, index_block[index.keys_offset..][0..index.keys_size]);
        }

        inline fn index_data_keys_used_const(index_block: BlockPtrConst) []const Key {
            return index_data_keys_const(index_block)[0..index_data_blocks_used(index_block)];
        }

        pub inline fn index_data_addresses(index_block: BlockPtr) []u64 {
            return mem.bytesAsSlice(
                u64,
                index_block[index.data_addresses_offset..][0..index.data_addresses_size],
            );
        }

        pub inline fn index_data_checksums(index_block: BlockPtr) []u128 {
            return mem.bytesAsSlice(
                u128,
                index_block[index.data_checksums_offset..][0..index.data_checksums_size],
            );
        }

        inline fn index_filter_addresses(index_block: BlockPtr) []u64 {
            return mem.bytesAsSlice(
                u64,
                index_block[index.filter_addresses_offset..][0..index.filter_addresses_size],
            );
        }

        inline fn index_filter_checksums(index_block: BlockPtr) []u128 {
            return mem.bytesAsSlice(
                u128,
                index_block[index.filter_checksums_offset..][0..index.filter_checksums_size],
            );
        }

        inline fn index_snapshot_min(index_block: BlockPtrConst) u32 {
            const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
            const value = @intCast(u32, header.offset);
            assert(value > 0);
            assert(value < snapshot_latest);
            return value;
        }

        inline fn index_blocks_used(index_block: BlockPtrConst) u32 {
            return index_block_count + index_filter_blocks_used(index_block) +
                index_data_blocks_used(index_block);
        }

        inline fn index_filter_blocks_used(index_block: BlockPtrConst) u32 {
            const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
            const value = @intCast(u32, header.commit);
            assert(value > 0);
            assert(value <= filter_block_count_max);
            return value;
        }

        pub inline fn index_data_blocks_used(index_block: BlockPtrConst) u32 {
            const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
            const value = @intCast(u32, header.request);
            assert(value > 0);
            assert(value <= data_block_count_max);
            return value;
        }

        /// Returns the zero-based index of the data block that may contain the key.
        /// May be called on an index block only when the key is already in range of the table.
        pub inline fn index_data_block_for_key(index_block: BlockPtrConst, key: Key) u32 {
            // Because we store key_max in the index block we can use the raw binary search
            // here and avoid the extra comparison. If the search finds an exact match, we
            // want to return that data block. If the search does not find an exact match
            // it returns the index of the next greatest key, which again is the index of the
            // data block that may contain the key.
            const data_block_index = binary_search.binary_search_keys_raw(
                Key,
                compare_keys,
                Table.index_data_keys_used_const(index_block),
                key,
            );
            assert(data_block_index < index_data_blocks_used(index_block));
            return data_block_index;
        }

        inline fn data_block_values(data_block: BlockPtr) []Value {
            return mem.bytesAsSlice(
                Value,
                data_block[data.values_offset..][0..data.values_size],
            );
        }

        pub inline fn data_block_values_used(data_block: BlockPtr) []const Value {
            const header = mem.bytesAsValue(vsr.Header, data_block[0..@sizeOf(vsr.Header)]);
            // TODO we should be able to cross-check this with the header size
            // for more safety.
            const used = @intCast(u32, header.request);
            assert(used <= data.value_count_max);
            return data_block_values(data_block)[0..used];
        }

        pub inline fn block_address(block: BlockPtrConst) u64 {
            const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
            const address = header.op;
            assert(address > 0);
            return address;
        }

        inline fn filter_block_filter(filter_block: BlockPtr) []u8 {
            return filter_block[filter.filter_offset..][0..filter.filter_size];
        }
    };
}

test "Table" {
    const Key = @import("composite_key.zig").CompositeKey(u128);
    const Storage = @import("../storage.zig").Storage;

    const Table = TableType(
        Storage,
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
    );

    _ = Table;
}
