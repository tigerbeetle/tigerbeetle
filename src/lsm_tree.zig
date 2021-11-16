const std = @import("std");
const math = std.math;
const mem = std.mem;
const vsr = @import("vsr.zig");

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
    // The active buffer is determined by (SuperBlock.version % 2
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
    comptime Key: type,
    comptime Value: type,
    comptime compare_keys: fn (Key, Key) math.Order,
    comptime key_from_value: fn (Value) Key,
    comptime tombstone: fn (Value) bool,
    comptime tombstone_from_key: fn (Key) Value,
    comptime Storage: type,
) type {
    assert(@sizeOf(Key) >= 4);
    assert(@sizeOf(Key) % 4 == 0);

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
            pub const TableInfo = packed struct {
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
            values_sorted: *[block_values_count]Value,

            pub fn init(allocator: *std.mem.Allocator) !MutableTable {
                var table: MutableTable = .{
                    .values_sorted = try table.allocator.create([block_values_count]Value),
                };
                errdefer allocator.destroy(table.values_sorted);
                try table.values.ensureTotalCapacity(block_values_count);
                return table;
            }

            /// Add the given value to the table
            pub fn put(table: *MutableTable, value: Value) void {
                table.values.putAssumeCapacity(value, {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= block_values_count);
            }

            pub fn remove(table: *MutableTable, key: Key) void {
                table.values.putAssumeCapacity(tombstone_from_key(key), {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= block_values_count);
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
            const block_size = config.lsm_table_block_size;
            const table_size_max = config.lsm_table_blocks_max * block_size;
            const block_body_size = block_size - @sizeOf(vsr.Header);

            const layout = blk: {
                assert(config.lsm_table_block_size % sector_size == 0);
                const index_entry_size = key_size + address_size + checksum_size;
                const filter_bytes_per_key = 2;

                var index_size = 0;
                var filter_size = 0;
                var blocks_size = table_size_max;
                var blocks_offset: comptime_int = undefined;

                while (true) : (blocks_size -= block_size) {
                    index_size = @divExact(blocks_size, block_size) * index_entry_size;
                    filter_size = @divExact(blocks_size, value_size) * filter_bytes_per_key;
                    blocks_offset = vsr.sector_ceil(@sizeOf(vsr.Header) + index_size + filter_size);
                    if (blocks_offset + blocks_size <= table_size_max) break;
                }

                var block_keys_count: comptime_int = undefined;
                var block_values_count: comptime_int = undefined;

                // Searching the values array is more expensive than searching the per-block index
                // as the larger values size leads to more cache misses. We can therefore speed
                // up lookups by making the per block index larger at the cost of reducing the
                // number of values that may be stored per block.
                //
                // X = values per block
                // Y = keys per block
                //
                // R = config.lsm_table_value_to_index_ratio_min
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
                block_keys_count = math.min(
                    block_body_size / ((config.lsm_table_value_to_index_ratio_min + 1) * key_size),
                    block_body_size / (value_size + key_size),
                );

                // Round to the next lowest power of two. This speeds up lookups in the Eytzinger
                // layout and should help ensure better alignment for the following values.
                // We could round to the nearest power of two, but then we would need
                // care to avoid breaking e.g. the X >= Y invariant above.
                block_keys_count = math.floorPowerOfTwo(comptime_int, block_keys_count);

                // If the index is smaller than 16 keys then there are key sizes >= 4 such that
                // the total index size is not 64 byte cache line aligned.
                assert(@sizeOf(Key) >= 4);
                assert(@sizeOf(Key) % 4 == 0);
                if (block_keys_count < config.cache_line_size / 4) block_keys_count = 0;
                assert((block_keys_count * key_size) % config.cache_line_size == 0);

                block_values_count = (block_body_size - block_keys_count * key_size) / value_size;

                break :blk .{
                    .index_offset = @sizeOf(vsr.Header),
                    .index_size = index_size,

                    .filter_offset = @sizeOf(vsr.Header) + index_size,
                    .filter_size = filter_size,

                    .blocks_offset = blocks_offset,
                    .blocks_size = blocks_size,

                    .table_size = blocks_offset + blocks_size,

                    .block_keys_count = block_keys_count,
                    .block_values_count = block_values_count,
                };
            };

            const filter_offset = layout.filter_offset;
            const filter_size = layout.filter_size;
            const index_offset = layout.index_offset;
            const index_size = layout.index_size;
            const blocks_offset = layout.blocks_offset;
            const blocks_size = layout.blocks_size;
            const table_size = layout.table_size;
            const block_keys_count = layout.block_keys_count;
            const block_values_count = layout.block_values_count;

            const block_keys_offset = 0;
            const block_keys_size = block_keys_count * key_size;
            const block_values_offset = block_keys_offset + block_keys_size;
            const block_values_size = block_values_count * value_size;
            const block_padding_offset = block_values_offset + block_values_size;
            const block_padding_size = block_size - block_padding_offset;

            comptime {
                const block_size = config.lsm_table_block_size;
                const filter_bytes_per_key = 2;

                assert(index_size > 0);
                assert(filter_size > 0);
                assert(blocks_size > 0);

                assert(index_size == @divExact(blocks_size, block_size) * (key_size + checksum_size));
                assert(filter_size == @divExact(blocks_size, value_size) * filter_bytes_per_key);
                assert(blocks_size % block_size == 0);

                assert(table_size == @sizeOf(vsr.Header) + filter_size + index_size + blocks_size);
                assert(table_size <= table_size_max);

                assert(index_offset == @sizeOf(vsr.Header));
                assert(filter_offset == index_offset + index_size);
                assert(blocks_offset == vsr.sector_ceil(filter_offset + filter_size));

                assert(block_keys_count >= 0);
                assert(block_keys_count == 0 or math.isPowerOfTwo(block_keys_count));
                assert(block_keys_size == 0 or block_values_size / block_keys_size >=
                    config.lsm_table_value_to_index_ratio_min);

                assert(block_values_count > 0);
                assert(block_values_offset % config.cache_line_size == 0);
                // You can have any value size you want, as long as it fits
                // neatly into the CPU cache lines :)
                assert((block_values_count * value_size) % config.cache_line_size == 0);

                assert(block_padding_size >= 0);
                assert(block_size == block_keys_size + block_values_size + block_padding_size);

                // We expect no block padding at least for TigerBeetle's objects and indexes:
                if ((key_size == 8 and value_size == 128) or
                    (key_size == 8 and value_size == 64) or
                    (key_size == 24 and value_size == 24))
                {
                    assert(block_padding_size == 0);
                }
            }

            /// The actual data to be written to disk.
            /// The first bytes are a vsr.Header containing checksum, id, count and timestamp.
            buffer: []align(config.sector_size) const u8,

            /// Technically redundant with the data in buffer, but having these as separate fields
            /// reduces indirection during queries.
            timestamp: u64,
            key_min: Key,
            key_max: Key,

            pub fn create(
                cluster: u32,
                id: u32,
                iterator: *MutableTable.Iterator,
                buffer: []align(config.sector_size) u8,
            ) ImmutableTable {
                const header = mem.bytesAsValue(vsr.Header, buffer[0..@sizeOf(vsr.Header)]);
                const index = mem.bytesAsSlice(Key, buffer[index_offset..][0..index_size]);
                const blocks = buffer[blocks_offset..][0..blocks_size];

                // TODO: finish this loop
                var block_index: usize = 0;
                while (true) : (block_index += 1) {
                    const block = blocks[block_index * block_size ..][0..block_size];
                    const block_values_bytes = block[block_values_offset..][0..block_values_size];
                    const block_values = mem.bytesAsSlice(Value, block_values_bytes);
                    assert(block_values.len == block_values_count);
                    const count = iterator.copy_values(block_values);
                    assert(count <= block_values_count);
                    // The block is empty
                    if (count == 0) break;

                    mem.set(u8, mem.sliceAsBytes(block_values[count..]), 0);
                    mem.set(u8, block[block_padding_offset..][0..block_padding_size], 0);

                    // TODO: construct the Eytzinger layout key index
                }

                if (builtin.mode == .Debug) {
                    var a = values[0];
                    for (values[1..]) |b| {
                        assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                        a = b;
                    }
                }

                // Zero the end of the last block and any subsequent unused blocks:
                // We want to prevent random memory from bleeding out to storage.
                mem.set(u8, blocks[values_size..], 0);

                // Ensure values do not straddle block boundaries to avoid padding between blocks:
                // For example, 8/16/24/64/128-byte values all divide a 6 sector block perfectly.
                comptime assert(config.lsm_table_block_size % value_size == 0);

                mem.set(u8, buffer[filter_offset..][0..filter_size], 0);

                // Construct index, a key_min for every block:
                {
                    const values_per_block = @divExact(config.lsm_table_block_size, value_size);
                    var i: usize = 0;
                    while (i < values.len) : (i += 1) {
                        index[i] = values[i * values_per_block];
                    }
                    mem.set(u8, mem.asBytes(index[i..]), 0);
                    mem.set(u8, buffer[index_offset + index_size .. blocks_offset], 0);
                }

                header.* = .{
                    .cluster = cluster,
                    .command = .table,
                    .offset = id,
                    .op = timestamp,
                    // The MessageBus will pad with zeroes to the next sector boundary
                    // on receiving the message.
                    .size = blocks_offset + values_size,
                };
                header.set_checksum_body(buffer[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                return .{
                    .buffer = buffer,
                    .timestamp = timestamp,
                    .key_min = key_from_value(values[0]),
                    .key_max = key_from_value(values[values.len - 1]),
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
