const std = @import("std");
const math = std.math;
const mem = std.mem;
const vsr = @import("vsr.zig");
const eytzinger = @import("eytzinger.zig").eytzinger;

// StateMachine:
//
// /// state machine will pass this on to all object stores
// /// Read I/O only
// pub fn read(batch, callback) void
//
// /// write the ops in batch to the memtable/objcache, previously called commit()
// pub fn write(batch) void
//
// /// Flush in memory state to disk, preform merges, etc
// /// Only function that triggers Write I/O in LSMs, as well as some Read
// /// Make as incremental as possible, don't block the main thread, avoid high latency/spikes
// pub fn flush(callback) void
//
// /// Write manifest info for all object stores into buffer
// pub fn encode_superblock(buffer) void
//
// /// Restore all in-memory state from the superblock data
// pub fn decode_superblock(buffer) void
//

pub fn CompositeKey(comptime Secondary: type) type {
    assert(Secondary == u128 or Secondary == u64);

    return extern struct {
        const Self = @This();

        const tombstone_bit = 1 << 63;
        // If zeroed padding is needed after the timestamp field
        const pad = Secondary == u128;

        pub const Value = extern struct {
            secondary: Secondary,
            /// The most significant bit indicates if the value is a tombstone
            timestamp: u64,
            padding: (if (pad) u64 else void) = (if (pad) 0 else {}),

            comptime {
                assert(@sizeOf(Value) == @sizeOf(Secondary) * 2);
                assert(@alignOf(Value) == @alignOf(Secondary));
            }
        };

        secondary: Secondary,
        /// The most significant bit must be unset as it is used to indicate a tombstone
        timestamp: u64,
        padding: (if (pad) u64 else void) = (if (pad) 0 else {}),

        comptime {
            assert(@sizeOf(Self) == @sizeOf(Secondary) * 2);
            assert(@alignOf(Self) == @alignOf(Secondary));
        }

        // TODO: consider optimizing this by reinterpreting the raw memory in an advantageous way
        // This may require modifying the struct layout.
        pub fn compare_keys(a: Self, b: Self) math.Order {
            if (a.secondary < b.secondary) {
                return .lt;
            } else if (a.secondary > b.secondary) {
                return .gt;
            } else if (a.timestamp < b.timestamp) {
                return .lt;
            } else if (a.timestamp > b.timestamp) {
                return .gt;
            } else {
                return .eq;
            }
        }

        pub fn key_from_value(value: Value) Self {
            return .{
                .secondary = value.secondary,
                .timestamp = @truncate(u63, key.timestamp),
            };
        }

        pub fn tombstone(value: Value) bool {
            return value.timestamp & tombstone_bit != 0;
        }

        pub fn tombstone_from_key(key: Self) Value {
            return .{
                .secondary = key.secondary,
                .timestamp = key.timestamp | tombstone_bit,
            };
        }
    };
}

/// The 0 address is reserved for usage as a sentinel and will never be returned
/// by acquire().
/// TODO add encode/decode function for run-length encoding to/from SuperBlock.
pub const BlockFreeSet = struct {
    /// Bits set indicate free blocks
    free: std.bit_set.DynamicBitSetUnmanaged,

    pub fn init(allocator: *std.mem.Allocator, count: usize) !BlockFreeSet {
        return BlockFreeSet{
            .free = try std.bit_set.DynamicBitSetUnmanaged.initFull(count, allocator),
        };
    }

    pub fn deinit(set: *BlockFreeSet, allocator: *std.mem.Allocator) void {
        set.free.deinit(allocator);
    }

    // TODO consider "caching" the first set bit to speed up subsequent acquire() calls
    pub fn acquire(set: *BlockFreeSet) u64 {
        // TODO: To ensure this "unreachable" is never reached, the leader must reject
        // new requests when storage space is too low to fulfill them.
        const bit = set.free.findFirstSet() orelse unreachable;
        set.free.unset(bit);
        return @intCast(u64, address + 1);
    }

    pub fn release(set: *BlockFreeSet, address: u64) void {
        const bit = address - 1;
        assert(!set.free.isSet(bit));
        set.free.set(bit);
    }
};

// vsr.zig
pub const SuperBlock = packed struct {
    checksum: u128,

    // Monotonically increasing counter of superblock generations. This enables us to find the
    // latest SuperBlock at startup, which we cross-check using the parent hash chain.
    version: u64,
    parent: u128,

    vsr_committed_log_offset: u64,
    client_table: [config.clients_max]ClientTableEntry,

    // The block free set is stored separately from the SuperBlock in a pair of copy on write buffers.
    // The active buffer is determined by (SuperBlock.version % 2)
    block_free_set_size: u32,
    block_free_set_checksum: u128,

    /// The manifest addresses must be listed here in order:
    /// 1. all positive manifest blocks of LSM 1, in order of their appearance in the manifest.
    /// 2. all negative manifest blocks of LSM 1, in order of their appearance in the manifest.
    /// 3. all positive manifest blocks of LSM 2, ...
    /// 4. ...
    manifest_addresses: [2048]u64,
    manifest_checksums: [2048]u128,

    /// Timestamp of 0 indicates that the snapshot slot is free
    snapshot_timestamps: [config.lsm_snapshots_max]u64,
    snapshot_last_used: [config.lsm_snapshots_max]u64,
};

// vsr.zig
pub const ClientTableEntry = packed struct {
    message_checksum: u128,
    message_offset: u64,
    session: u64,
};

/// The size of the index is determined only by the number of blocks in the
/// table, not by the number of objects per block.
pub const Table = packed struct {
    // Contains the maximum size of the table including the header
    // Use one of the header fields for the table id
    header: vr.Header,
    // filter (fixed size)
    // index (fixed size)
    // data (append up to maximum size)
};

pub const Forest = struct {
    transfers_lsm: TransfersLsm,
    transfers_indexes_lsm: TransfersIndexesLsm,
    // ...
};

const LsmTreeOptions = struct {
    tables_max: u32,
    cluster: u32,
};

// const TransfersLsm = LsmTree(u64, Transfer, compare, key_from_value, storage);
// const TransfersIndexesLsm = LsmTree(TransferCompositeKey, TransferCompositeKey, compare, key_from_value, storage);

pub fn LsmTree(
    /// Key sizes of 8, 16, 32, etc. are supported with alignment 8 or 16.
    comptime Key: type,
    comptime Value: type,
    comptime compare_keys: fn (Key, Key) math.Order,
    comptime key_from_value: fn (Value) Key,
    /// Must compare greater than all other keys.
    comptime sentinel_key: Key,
    comptime tombstone: fn (Value) bool,
    comptime tombstone_from_key: fn (Key) Value,
    comptime Storage: type,
) type {
    assert(@alignOf(Key) == 8 or @alignOf(Key) == 16);
    // There must be no padding in the Key type. This avoids buffer bleeds.
    assert(@bitSizeOf(Key) == @sizeOf(Key) * 8);

    const value_size = @sizeOf(Value);
    const key_size = @sizeOf(Key);
    return struct {
        const Self = @This();

        pub const Manifest = struct {
            /// 4MiB table
            ///
            /// 32_768 bytes transfers
            /// 65_536 bytes bloom filter
            /// 16_384 bytes index
            ///
            /// First 128 bytes of the table are a VSR protocol header for a repair message.
            /// This data is filled in on writing the table so that we don't
            /// need to do any extra work before sending the message on repair.
            pub const TableInfo = extern struct {
                checksum: u128,
                address: u64,
                timestamp: u64,
                key_min: Key,
                key_max: Key,

                comptime {
                    assert(@sizeOf(TableInfo) == 32 + key_size * 2);
                    assert(@alignOf(TableInfo) == 16);
                }
            };

            levels: [config.lsm_levels][]TableInfo,

            pub fn level_tables(manifest: *Manifest, level: u32, timestamp_max: u64) []TableInfo {}
        };

        // Point queries go through the object cache instead of directly accessing this table.
        // Range queries are not supported on MemTables, they must instead be made immutable.
        pub const MutableTable = struct {
            const ValuesContext = struct {
                pub fn eql(a: Value, b: Value) bool {
                    return compare_keys(key_from_value(a), key_from_value(b)) == .eq;
                }
                pub fn hash(value: Value) u64 {
                    const key = key_from_value(value);
                    return std.hash_map.getAutoHashFn(Key, ValuesContext)(key);
                }
            };
            const Values = std.HashMapUnmanaged(Value, void, ValuesContext, 50);

            values: Values = .{},
            /// Used as a scratch buffer to provide a sorted iterator for construction
            /// of ImmutableTables.
            values_sorted: *[block_value_count_max]Value,

            pub fn init(allocator: *std.mem.Allocator) !MutableTable {
                var table: MutableTable = .{
                    .values_sorted = try table.allocator.create([block_value_count_max]Value),
                };
                errdefer allocator.destroy(table.values_sorted);
                try table.values.ensureTotalCapacity(block_value_count_max);
                return table;
            }

            /// Add the given value to the table
            pub fn put(table: *MutableTable, value: Value) void {
                table.values.putAssumeCapacity(value, {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= block_value_count_max);
            }

            pub fn remove(table: *MutableTable, key: Key) void {
                table.values.putAssumeCapacity(tombstone_from_key(key), {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= block_value_count_max);
            }

            pub fn get(table: *MutableTable, key: Key) ?Value {
                return table.values.get(tombstone_from_key(key));
            }

            pub const Iterator = struct {
                values: []Value,

                /// Returns the number of values copied, 0 if there are no values left.
                pub fn copy_values(iterator: *Iterator, target: []Value) usize {
                    const count = math.min(iterator.values.len, target.len);
                    mem.copy(Value, target, iterator.values[0..count]);
                    iterator.values = iterator.values[count..];
                    return count;
                }
            };

            pub fn iterator(table: *MutableTable) Iterator {
                var i: usize = 0;
                var it = table.values.keyIterator();
                while (it.next()) |value| : (i += 1) {
                    table.values_sorted[i] = value.*;
                }
                const values = table.values_sorted[0..i];
                assert(values.len == table.values.count());

                // Sort values by key:
                const less_than = struct {
                    pub fn less_than(_: void, a: Value, b: Value) bool {
                        return compare_keys(key_from_value(a), key_from_value(b)) == .lt;
                    }
                }.less_than;
                std.sort.insertionSort(Value, values, {}, less_than);

                return Iterator{ .values = values };
            }
        };

        pub const ImmutableTable = struct {
            const address_size = @sizeOf(u64);
            const checksum_size = @sizeOf(u128);
            const table_size_max = config.lsm_table_size_max;
            const table_block_count_max = @divExact(table_size_max, block_size);
            const block_size = config.lsm_table_block_size;
            const block_body_size = block_size - @sizeOf(vsr.Header);

            const layout = blk: {
                assert(config.lsm_table_block_size % sector_size == 0);

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
                block_keys_layout_count =
                    math.floorPowerOfTwo(comptime_int, block_keys_layout_count);

                // If the index is smaller than 16 keys then there are key sizes >= 4 such that
                // the total index size is not 64 byte cache line aligned.
                assert(@sizeOf(Key) >= 4);
                assert(@sizeOf(Key) % 4 == 0);
                if (block_keys_layout_count < config.cache_line_size / 4) {
                    block_keys_layout_count = 0;
                }
                assert((block_keys_layout_count * key_size) % config.cache_line_size == 0);

                const block_key_layout_size = block_keys_layout_count * key_size;
                const block_key_count = block_keys_layout_count - 1;

                const block_value_count_max =
                    (block_body_size - block_key_layout_size) / value_size;

                const data_index_entry_size = key_size + address_size + checksum_size;
                const filter_index_entry_size = address_size + checksum_size;
                const filter_bytes_per_key = 2;

                var data_index_size = 0;
                var filter_index_size = 0;
                var data_block_count = table_block_count_max - index.block_count;
                var filter_block_count = 0;
                while (true) : (data_block_count -= 1) {
                    data_index_size = data_index_entry_size * data_block_count;

                    filter_block_count = data_block_count * block_value_count_max *
                        filter_bytes_per_key / block_size;
                    filter_index_size = filter_index_entry_size * filter_block_count;

                    if (@sizeOf(vsr.Header) + data_index_size + filter_index_size <= block_size) {
                        break;
                    }
                }

                const total_block_count = index.block_count + data_block_count + filter_block_count;
                assert(total_block_count <= table_block_count_max);

                break :blk .{
                    .block_key_count = block_key_count,
                    .block_key_layout_size = block_key_layout_size,
                    .block_value_count_max = block_value_count_max,

                    .data_block_count = data_block_count,
                    .filter_block_count = filter_block_count,
                };
            };

            const index_block_count = 1;
            const filter_block_count = layout.filter_block_count;
            const data_block_count = layout.data_block_count;

            const index = struct {
                const size = header_size + filter_checksums_size + data_checksums_size +
                    keys_size + filter_addresses_size + data_addresses_size;

                const header_offset = 0;
                const header_size = @sizeOf(vsr.Header);

                const filter_checksums_offset = header_size;
                const filter_checksums_size = filter_block_count * checksum_size;

                const data_checksums_offset = filter_checksums_offset + filter_checksums_size;
                const data_checksums_size = data_block_count * checksum_size;

                const keys_offset = data_checksums_offset + data_checksums_size;
                const keys_size = data_block_count * key_size;

                const filter_addresses_offset = keys_offset + key_size;
                const filter_addresses_size = filter_block_count * address_size;

                const data_addresses_offset = filter_addresses_offset + filter_addresses_size;
                const data_addresses_size = data_block_count * address_size;
            };

            const filter = struct {
                const header_offset = 0;
                const header_size = @sizeOf(vsr.Header);

                const filter_offset = header_size;
            };

            const data = struct {
                const key_count = layout.block_key_count;
                const value_count_max = layout.block_value_count_max;

                const header_offset = 0;
                const header_size = @sizeOf(vsr.Header);

                const key_layout_offset = header_size;
                const key_layout_size = layout.block_key_layout_size;

                const values_offset = key_layout_offset + key_layout_size;
                const values_size = block_value_count_max * value_size;

                const padding_offset = values_offset + values_size;
                const padding_size = block_size - padding_offset;
            };

            comptime {
                assert(index_block_count > 0);
                assert(filter_block_count > 0);
                assert(data_block_count > 0);
                assert(index_block_count + filter_block_count +
                    data_block_count <= table_block_count_max);

                assert(index.size ==
                    data_block_count * (key_size + address_size + checksum_size) +
                    filter_block_count * (address_size + checksum_size));
                assert(index.size == index.data_addresses_offset + index.data_addresses_size);
                assert(index.size <= block_size);

                if (data.key_count > 0) {
                    assert(data.key_count >= 3);
                    assert(math.isPowerOfTwo(data.key_count + 1));
                    assert(data.key_count + 1 == @divExact(data.key_layout_size, key_size));
                    assert(data.values_size / data.key_layout_size >=
                        config.lsm_value_to_key_layout_ratio_min);
                } else {
                    assert(data.key_count == 0);
                    assert(data.key_layout_size == 0);
                }

                assert(data.value_count_max > 0);
                assert(data.values_offset % config.cache_line_size == 0);
                // You can have any size value you want, as long as it fits
                // neatly into the CPU cache lines :)
                assert((data.value_count_max * value_size) % config.cache_line_size == 0);

                assert(data.padding_size >= 0);
                assert(block_size == @sizeOf(vsr.Header) + data.key_layout_size +
                    data.values_size + data.padding_size);

                // We expect no block padding at least for TigerBeetle's objects and indexes:
                if ((key_size == 8 and value_size == 128) or
                    (key_size == 8 and value_size == 64) or
                    (key_size == 16 and value_size == 16) or
                    (key_size == 32 and value_size == 32))
                {
                    assert(data.padding_size == 0);
                }
            }

            /// The actual data to be written to disk.
            /// The first bytes are a vsr.Header containing checksum, id, count and timestamp.
            buffer: []align(config.sector_size) const u8,
            table_info: Manifest.TableInfo,

            pub fn create(
                cluster: u32,
                block_free_set: *BlockFreeSet,
                timestamp: u64,
                iterator: *MutableTable.Iterator,
                buffer: []align(config.sector_size) u8,
            ) ImmutableTable {
                const blocks = mem.bytesAsSlice([block_size]u8, buffer);

                const index_block = &blocks[0];
                const filter_blocks = blocks[index_block_count..][0..filter_block_count];
                const data_blocks =
                    blocks[index_block_count + filter_block_count ..][0..data_block_count];

                const index_keys = mem.bytesAsSlice(
                    Key,
                    index_block[index.keys_offset..][0..index.keys_size],
                );
                const index_data_addresses = mem.bytesAsSlice(
                    u64,
                    index_block[index.data_addresses_offset..][0..index.data_addresses_size],
                );
                const index_data_checksums = mem.bytesAsSlice(
                    u128,
                    index_block[index.data_checksums_offset..][0..index.data_checksums_size],
                );

                var key_min: Key = undefined;

                const data_blocks_used = for (data_blocks) |*block, i| {
                    // For each block we write the sorted values, initialize the Eytzinger layout,
                    // complete the block header, and add the block's max key to the table index.

                    const values_bytes = block[data.values_offset..][0..data.values_size];
                    const values_max = mem.bytesAsSlice(Value, values_bytes);
                    assert(values_max.len == data.value_count_max);

                    const value_count = iterator.copy_values(values_max);
                    assert(value_count <= data.value_count_max);

                    // The block is empty:
                    if (value_count == 0) break i;

                    const values = values_max[0..value_count];

                    if (i == 0) key_min = key_from_value(values[0]);

                    if (builtin.mode == .Debug) {
                        var a = values[0];
                        for (values[1..]) |b| {
                            assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                            a = b;
                        }
                    }

                    assert(@divExact(data.key_layout_size, key_size) == data.key_count + 1);
                    const key_layout_bytes =
                        block[data.key_layout_offset..][0..data.key_layout_size];
                    const key_layout = mem.bytesAsValue([data.key_count + 1]Key, key_layout_bytes);

                    const e = eytzinger(data.key_count, data.value_count_max);
                    e.layout(Key, Value, key_from_value, sentinel_key, key_layout, values);

                    const values_padding = mem.sliceAsBytes(values_max[value_count..]);
                    const block_padding = block[data.padding_offset..][0..data.padding_size];
                    mem.set(u8, values_padding, 0);
                    mem.set(u8, block_padding, 0);

                    const header_bytes = block[data.header_offset..][0..data.header_size];
                    const header = mem.bytesAsValue(vsr.Header, header_bytes);

                    header.* = .{
                        .cluster = cluster,
                        .op = block_free_set.acquire(),
                        .size = block_size - values_padding.len - block_padding.len,
                        .command = .block,
                    };

                    header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    index_keys[i] = values[values.len - 1];
                    index_data_addresses[i] = header.op;
                    index_data_checksums[i] = header.checksum;
                } else data_block_count;

                assert(data_blocks_used > 0);

                const index_keys_padding = mem.sliceAsBytes(index_keys[data_blocks_used..]);
                mem.set(u8, index_keys_padding, 0);
                mem.set(u64, index_data_addresses[data_blocks_used..], 0);
                mem.set(u128, index_data_checksums[data_blocks_used..], 0);

                // TODO implement filters
                const index_filter_addresses = mem.bytesAsSlice(
                    u64,
                    index_block[index.filter_addresses_offset..][0..index.filter_addresses_size],
                );
                const index_filter_checksums = mem.bytesAsSlice(
                    u128,
                    index_block[index.filter_checksums_offset..][0..index.filter_checksums_size],
                );
                for (filter_blocks) |*block| mem.set(u8, block, 0);
                mem.set(u64, index_filter_addresses, 0);
                mem.set(u128, index_filter_checksums, 0);

                const header_bytes = index_block[index.header_offset..][0..index.header_size];
                const header = mem.bytesAsValue(vsr.Header, header_bytes);

                header.* = .{
                    .cluster = cluster,
                    .op = block_free_set.acquire(),
                    .size = index.size,
                    .command = .block,
                };
                header.set_checksum_body(index_block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                return .{
                    .buffer = buffer,
                    .table_info = .{
                        .checksum = header.checksum,
                        .address = header.op,
                        .timestamp = timestamp,
                        .key_min = key_min,
                        .key_max = index_keys[data_blocks_used - 1],
                    },
                };
            }
        };

        block_free_set: *BlockFreeSet,
        storage: *Storage,
        options: LsmTreeOptions,

        write_transaction: WriteTransaction,

        manifest: []Manifest,

        pub fn init(
            allocator: *std.mem.Allocator,
            block_free_set: *BlockFreeSet,
            storage: *Storage,
            options: LsmTreeOptions,
        ) !Self {}

        pub const Error = error{
            IO,
        };

        pub fn put(tree: *Self, value: Value) void {}

        pub fn flush(
            tree: *Self,
            callback: fn (result: Error!void) void,
        ) void {}

        // ~Special case of put()
        pub fn remove(tree: *Self, value: Value) void {}

        pub fn get(
            tree: *Self,
            /// The snapshot timestamp, if any
            snapshot: ?u64,
            key: Key,
            callback: fn (result: Error!?Value) void,
        ) void {}

        pub const RangeQuery = union(enum) {
            bounded: struct {
                start: Key,
                end: Key,
            },
            open: struct {
                start: Key,
                order: enum {
                    ascending,
                    descending,
                },
            },
        };

        pub const RangeQueryIterator = struct {
            tree: *Self,
            snapshot: ?u64,
            query: RangeQuery,

            pub fn next(callback: fn (result: Error!?Value) void) void {}
        };

        pub fn range_query(
            tree: *Self,
            /// The snapshot timestamp, if any
            snapshot: ?u64,
            query: RangeQuery,
        ) RangeQueryIterator {}
    };
}
