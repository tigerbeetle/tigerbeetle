const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

const eytzinger = @import("eytzinger.zig").eytzinger;
const CompositeKey = @import("composite_key.zig").CompositeKey;
const BlockFreeSet = @import("block_free_set.zig").BlockFreeSet;
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;

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

// vsr.zig
pub const SuperBlock = packed struct {
    checksum: u128,
    cluster: u32,
    local_storage_size: u32,

    /// Reserved for future use (e.g. changing compression algorithm of trailer)
    flags: u64,

    // Monotonically increasing counter of superblock generations. This enables us to find the
    // latest SuperBlock at startup, which we cross-check using the parent hash chain.
    version: u64,
    parent: u128,

    // TODO remove this?
    replica: u8,
    vsr_committed_log_offset: u64,
    client_table: [config.clients_max]ClientTableEntry,

    /// The size and checksum of the block free set stored in the SuperBlock trailer.
    block_free_set_size: u32,
    block_free_set_checksum: u128,

    /// The number of manifest block addresses and block checksums stored in the
    /// SuperBlock trailer and the checksum of this data.
    ///
    /// The block addresses and block checksums in the trailer are laid out as follows:
    /// [manifest_blocks_count]u64 address
    /// [manifest_blocks_count]u128 checksum
    ///
    /// A manifest_blocks_count of 4096 is more than enough to address 100 TiB of 64 MiB tables.
    /// Since we only write the bytes that we actually use however, we can be quite generous
    /// with the fixed size disk allocation for this trailer.
    ///
    /// TODO One possible layout
    /// 1. all positive manifest blocks of LSM 1, in order of their appearance in the manifest.
    /// 2. all negative manifest blocks of LSM 1, in order of their appearance in the manifest.
    /// 3. all positive manifest blocks of LSM 2, ...
    /// 4. ...
    manifest_blocks_count: u32,
    manifest_blocks_checksum: u128,

    /// Timestamp of 0 indicates that the snapshot slot is free
    snapshot_timestamps: [config.lsm_snapshots_max]u64,
    snapshot_last_used: [config.lsm_snapshots_max]u64,

    _reserved: [1024]u8,
};

// vsr.zig
pub const ClientTableEntry = packed struct {
    message_checksum: u128,
    message_offset: u64,
    session: u64,
};

const block_size = config.lsm_table_block_size;
const BlockPtr = *align(config.sector_size) [block_size]u8;
const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

// TODO Split a Partition type out of the current Storage.zig, the partition type will be what
// is shimmed for testing. Then we can put the free set and block writing methods in Storage.
pub fn BlockStorage(comptime Storage: type) type {
    return struct {
        cluster: u32,
        storage: *Storage,
        free_set: *BlockFreeSet,

        pub fn write_block(
            callback: fn (Storage.Write) void,
            write: *Storage.Write,
            block: BlockPtrConst,
            address: u64,
        ) void {
            // TODO
        }

        /// This function transparently handles recovery if the checksum fails.
        /// If necessary, this read will be added to a linked list in Storage,
        /// which Replica can then interrogate each tick(). The callback passed
        /// to this function won't be called until the block has been recovered.
        pub fn read_block(
            callback: fn (Storage.Read) void,
            read: *Storage.Read,
            block: BlockPtr,
            address: u64,
        ) void {
            // TODO
        }
    };
}

pub const Direction = enum {
    ascending,
    descending,
};

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
) type {
    assert(@alignOf(Key) == 8 or @alignOf(Key) == 16);
    // There must be no padding in the Key type. This avoids buffer bleeds.
    assert(@bitSizeOf(Key) == @sizeOf(Key) * 8);

    const value_size = @sizeOf(Value);
    const key_size = @sizeOf(Key);

    return struct {
        const Self = @This();

        pub const Manifest = struct {
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

            pub const Level = struct {
                key_mins: []Key,
                key_mins: []Key,
            };

            levels: [config.lsm_levels]Level,

            pub fn table(
                manifest: *Manifest,
                /// May pass math.maxInt(u64) if there is no snapshot.
                snapshot: u64,
                level: u8,
                key: Key,
            ) ?TableInfo {
                const info = manifest.levels[level].get(key, snapshot) orelse return null;

                assert(compare_keys(key, info.key_max) != .gt);
                if (compare_keys(key, info.key_min) != .lt) return info;

                return null;
            }

            fn table_index(
                manifest: *Manifest,
                /// May pass math.maxInt(u64) if there is no snapshot.
                snapshot: u64,
                level: u8,
                key: Key,
                direction: Direction,
            ) ?Index {
                // TODO
            }

            fn table_index(manifest: *Manifest, index: Index) void {}

            pub const Iterator = struct {
                manifest: *Manifest,
                snapshot: u64,
                level: u8,
                index: u32,
                end: Key,
                direction: Direction,

                pub fn next(it: *Iterator) ?TableInfo {
                    // assume direction is ascending
                    // search for the current key_min in the manifest, given level and snapshot
                    //
                }
            };

            pub fn get_tables(
                manifest: *Manifest,
                /// May pass math.maxInt(u64) if there is no snapshot.
                snapshot: u64,
                level: u8,
                key_min: Key,
                key_max: Key,
                direction: Direction,
            ) Iterator {
                return .{
                    .manifest = manifest,
                    .snapshot = snapshot,
                    .level = level,
                    .key_min = key_min,
                    .key_max = key_max,
                    .direction = direction,
                };
            }
        };

        /// Point queries go through the object cache instead of directly accessing this table.
        /// Range queries are not supported on MutableTable, they must instead be made immutable.
        pub const MutableTable = struct {
            const value_count_max = config.lsm_mutable_table_size_max / value_size;

            const ValuesContext = struct {
                pub fn eql(_: ValuesContext, a: Value, b: Value) bool {
                    return compare_keys(key_from_value(a), key_from_value(b)) == .eq;
                }
                pub fn hash(_: ValuesContext, value: Value) u64 {
                    const key = key_from_value(value);
                    return std.hash_map.getAutoHashFn(Key, ValuesContext)(.{}, key);
                }
            };
            const load_factor = 50;
            const Values = std.HashMapUnmanaged(Value, void, ValuesContext, load_factor);

            values: Values = .{},

            pub fn init(allocator: *std.mem.Allocator) !MutableTable {
                var table: MutableTable = .{};
                // TODO This allocates a bit more memory than we need as it rounds up to the next
                // power of two or similar based on the hash map's growth factor. We never resize
                // the hashmap so this is wasted memory for us.
                try table.values.ensureTotalCapacity(value_count_max);
                return table;
            }

            pub fn deinit(table: *MutableTable, allocator: *std.mem.Allocator) void {
                table.values.deinit(allocator);
            }

            /// Add the given value to the table
            pub fn put(table: *MutableTable, value: Value) void {
                table.values.putAssumeCapacity(value, {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= value_count_max);
            }

            pub fn remove(table: *MutableTable, key: Key) void {
                table.values.putAssumeCapacity(tombstone_from_key(key), {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= value_count_max);
            }

            pub fn get(table: *MutableTable, key: Key) ?Value {
                return table.values.get(tombstone_from_key(key));
            }

            pub fn sort_values(
                table: *MutableTable,
                sort_buffer: []align(@alignOf(Value)) u8,
            ) []const Value {
                assert(table.values.count() > 0);
                assert(sort_buffer.len == config.lsm_mutable_table_size_max);

                const values_buffer = mem.bytesAsSlice(Value, sort_buffer[0..value_count_max]);

                var i: usize = 0;
                var it = table.values.keyIterator();
                while (it.next()) |value| : (i += 1) {
                    values_buffer[i] = value.*;
                }
                const values = values_buffer[0..i];
                assert(values.len == table.values.count());

                // Sort values by key:
                const less_than = struct {
                    pub fn less_than(_: void, a: Value, b: Value) bool {
                        return compare_keys(key_from_value(a), key_from_value(b)) == .lt;
                    }
                }.less_than;
                std.sort.sort(Value, values, {}, less_than);

                return values;
            }
        };

        pub const Table = struct {
            const address_size = @sizeOf(u64);
            const checksum_size = @sizeOf(u128);
            const table_size_max = config.lsm_table_size_max;
            const table_block_count_max = @divExact(table_size_max, block_size);
            const block_body_size = block_size - @sizeOf(vsr.Header);

            const layout = blk: {
                assert(config.lsm_table_block_size % config.sector_size == 0);
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
                var data_blocks = table_block_count_max - index_block_count;
                var filter_blocks = 0;
                while (true) : (data_blocks -= 1) {
                    data_index_size = data_index_entry_size * data_blocks;

                    filter_blocks = math.divCeil(comptime_int, data_blocks * block_value_count_max *
                        filter_bytes_per_key, block_size) catch unreachable;
                    filter_index_size = filter_index_entry_size * filter_blocks;

                    const index_size = @sizeOf(vsr.Header) + data_index_size + filter_index_size;
                    const total_block_count = index_block_count + data_blocks + filter_blocks;
                    if (index_size <= block_size and total_block_count <= table_block_count_max) {
                        break;
                    }
                }

                const total_block_count = index_block_count + data_blocks + filter_blocks;
                assert(total_block_count <= table_block_count_max);

                break :blk .{
                    .block_key_count = block_key_count,
                    .block_key_layout_size = block_key_layout_size,
                    .block_value_count_max = block_value_count_max,

                    .data_block_count_max = data_blocks,
                    .filter_block_count = filter_blocks,
                };
            };

            const index_block_count = 1;
            const filter_block_count = layout.filter_block_count;
            const data_block_count_max = layout.data_block_count_max;

            const index = struct {
                const size = @sizeOf(vsr.Header) + filter_checksums_size + data_checksums_size +
                    keys_size + filter_addresses_size + data_addresses_size;

                const filter_checksums_offset = @sizeOf(vsr.Header);
                const filter_checksums_size = filter_block_count * checksum_size;

                const data_checksums_offset = filter_checksums_offset + filter_checksums_size;
                const data_checksums_size = data_block_count_max * checksum_size;

                const keys_offset = data_checksums_offset + data_checksums_size;
                const keys_size = data_block_count_max * key_size;

                const filter_addresses_offset = keys_offset + keys_size;
                const filter_addresses_size = filter_block_count * address_size;

                const data_addresses_offset = filter_addresses_offset + filter_addresses_size;
                const data_addresses_size = data_block_count_max * address_size;

                const padding_offset = data_addresses_offset + data_addresses_size;
                const padding_size = block_size - padding_offset;
            };

            const filter = struct {
                const filter_offset = @sizeOf(vsr.Header);
                const filter_size = 0;

                const padding_offset = filter_offset + filter_size;
                const padding_size = block_size - padding_offset;
            };

            const data = struct {
                const key_count = layout.block_key_count;
                const value_count_max = layout.block_value_count_max;

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
                        \\    filter block count: {}
                        \\    data block count: {}
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
                        \\    filter_offset: {}
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
                            filter_block_count,
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

                            filter.filter_offset,

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
                assert(filter_block_count > 0);
                assert(data_block_count_max > 0);
                assert(index_block_count + filter_block_count +
                    data_block_count_max <= table_block_count_max);
                const filter_bytes_per_key = 2;
                assert(filter_block_count * block_size >= data_block_count_max *
                    data.value_count_max * filter_bytes_per_key);

                assert(index.size == @sizeOf(vsr.Header) +
                    data_block_count_max * (key_size + address_size + checksum_size) +
                    filter_block_count * (address_size + checksum_size));
                assert(index.size == index.data_addresses_offset + index.data_addresses_size);
                assert(index.size <= block_size);
                assert(index.keys_size > 0);
                assert(index.keys_size % key_size == 0);
                assert(@divExact(index.data_addresses_size, @sizeOf(u64)) == data_block_count_max);
                assert(@divExact(index.filter_addresses_size, @sizeOf(u64)) == filter_block_count);
                assert(@divExact(index.data_checksums_size, @sizeOf(u128)) == data_block_count_max);
                assert(@divExact(index.filter_checksums_size, @sizeOf(u128)) == filter_block_count);
                assert(block_size == index.padding_offset + index.padding_size);
                assert(block_size == index.size + index.padding_size);

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

            /// The actual data to be written to disk.
            /// The first bytes are a vsr.Header containing checksum, id, count and timestamp.
            buffer: []align(config.sector_size) const u8,
            table_info: Manifest.TableInfo,
            flush_iterator: FlushIterator,

            pub fn create_from_sorted_values(
                table: *Table,
                storage: *BlockStorage(Storage),
                timestamp: u64,
                sorted_values: []const Value,
            ) void {
                assert(timestamp > 0);
                assert(sorted_values.len > 0);
                assert(sorted_values.len <= data.value_count_max * data_block_count_max);

                const buffer = table.buffer;
                const blocks = mem.bytesAsSlice([block_size]u8, buffer);

                var filter_blocks_index: u32 = 0;
                const filter_blocks = blocks[index_block_count..][0..filter_block_count];

                var builder: Builder = .{
                    .storage = storage,
                    .index_block = &blocks[0],
                    .filter_block = &filter_blocks[0],
                    .data_block = undefined,
                };
                filter_blocks_index += 1;

                const data_blocks =
                    blocks[index_block_count + filter_block_count ..][0..data_block_count_max];

                var stream = sorted_values;
                for (data_blocks) |*data_block| {
                    builder.data_block = data_block;

                    const slice = stream[0..math.min(data.value_count_max, stream.len)];
                    stream = stream[slice.len..];

                    builder.data_block_append_slice(slice);
                    builder.data_block_finish();

                    if (builder.filter_block_full() or stream.len == 0) {
                        builder.filter_block_finish();
                        builder.filter_block = filter_blocks[filter_blocks_index];
                        filter_blocks_index += 1;
                    }

                    if (stream.len == 0) break;

                    assert(data_block_values_used(data_block).len == data.value_count_max);
                } else {
                    // We must always copy *all* values from sorted_values into the table,
                    // which will result in breaking from the loop as `stream.len` is 0.
                    // This is the case even when all data blocks are completely filled.
                    unreachable;
                }
                assert(stream.len == 0);
                assert(filter_blocks_index <= filter_block_count);

                table.* = .{
                    .buffer = buffer,
                    .table_info = builder.index_block_finish(),
                    .flush_iterator = .{},
                };
            }

            const Builder = struct {
                const Self = @This();

                storage: *BlockStorage(Storage),
                key_min: Key = undefined,
                key_max: Key = undefined,

                index_block: BlockPtr,
                filter_block: BlockPtr,
                data_block: BlockPtr,

                block: u32 = 0,
                value: u32 = 0,

                pub fn init(allocator: *mem.Allocator) Self {
                    // TODO
                }

                pub fn deinit(builder: *Self, allocator: *mem.Allocator) void {
                    // TODO
                }

                pub fn data_block_append(builder: *Builder, value: Value) void {
                    const values_max = data_block_values(builder.data_block);
                    assert(values_max.len == data.value_count_max);

                    values_max[builder.value] = value;
                    builder.value += 1;
                    // TODO add this value's key to the correct filter block.
                }

                pub fn data_block_append_slice(builder: *Builder, values: []const Value) void {
                    assert(values.len > 0);
                    assert(builder.value + values.len <= data.value_count_max);

                    const values_max = data_block_values(builder.data_block);
                    assert(values_max.len == data.value_count_max);

                    mem.copy(Value, values_max[builder.value..], values);
                    builder.value += values.len;
                    // TODO add this value's key to the correct filter block.
                }

                pub fn data_block_full(builder: Builder) bool {
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

                    if (builtin.mode == .Debug) {
                        var a = values[0];
                        for (values[1..]) |b| {
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

                    const address = builder.storage.free_set.acquire();

                    header.* = .{
                        .cluster = builder.storage.cluster,
                        .op = address,
                        .request = values.len,
                        .size = block_size - @intCast(u32, values_padding.len - block_padding.len),
                        .command = .block,
                    };

                    header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    const key_max = key_from_value(values[values.len - 1]);

                    index_keys(builder.index)[builder.block] = key_max;
                    index_data_addresses(builder.index)[builder.block] = address;
                    index_data_checksums(builder.index)[builder.block] = header.checksum;

                    if (builder.block == 0) builder.key_min = key_from_value(values[0]);
                    builder.key_max = key_max;

                    if (builder.block == 0 and values.len == 1) {
                        assert(compare_keys(builder.key_min, builder.key_max) != .gt);
                    } else {
                        assert(compare_keys(builder.key_min, builder.key_max) == .lt);
                    }

                    builder.block += 1;
                    builder.value = 0;
                }

                /// Returns true if there is space for at least one more data block in the filter.
                pub fn filter_block_full(builder: Builder) bool {
                    // TODO
                }

                pub fn filter_block_finish(builder: *Builder) void {
                    // TODO
                }

                pub fn index_block_full(builder: Builder) bool {
                    return builder.block == data_block_count_max;
                }

                pub fn index_block_finish(builder: *Builder, timestamp: u64) Manifest.TableInfo {
                    assert(builder.block > 0);

                    // TODO assert that filter is finished

                    const index_block = builder.index_block;

                    const index_keys_padding = index_keys(index_block)[builder.block..];
                    const index_keys_padding_bytes = mem.sliceAsBytes(index_keys_padding);
                    mem.set(u8, index_keys_padding_bytes, 0);
                    mem.set(u64, index_data_addresses(index_block)[builder.block..], 0);
                    mem.set(u128, index_data_checksums(index_block)[builder.block..], 0);

                    // TODO implement filters
                    const filter_blocks_used = 0;
                    mem.set(u64, index_filter_addresses(index_block), 0);
                    mem.set(u128, index_filter_checksums(index_block), 0);

                    mem.set(u8, index_block[index.padding_offset..][0..index.padding_size], 0);

                    const header_bytes = index_block[0..@sizeOf(vsr.Header)];
                    const header = mem.bytesAsValue(vsr.Header, header_bytes);

                    const address = builder.storage.free_set.acquire();

                    header.* = .{
                        .cluster = builder.storage.cluster,
                        .op = address,
                        .commit = filter_blocks_used,
                        .request = builder.block,
                        .offset = timestamp,
                        .size = index.size,
                        .command = .block,
                    };
                    header.set_checksum_body(index_block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    const info: Manifest.TableInfo = .{
                        .checksum = header.checksum,
                        .address = address,
                        .timestamp = timestamp,
                        .key_min = builder.key_min,
                        .key_max = builder.key_max,
                    };

                    // Reset the builder to its initial state, leaving the buffers untouched.
                    builder.* = .{
                        .storage = builder.storage,
                        .key_min = undefined,
                        .key_max = undefined,
                        .index_block = builder.index_block,
                        .filter_block = builder.filter_block,
                        .data_block = builder.data_block,
                    };

                    return info;
                }
            };

            inline fn index_keys(index_block: BlockPtr) []Key {
                return mem.bytesAsSlice(Key, index_block[index.keys_offset..][0..index.keys_size]);
            }

            inline fn index_data_addresses(index_block: BlockPtr) []u64 {
                return mem.bytesAsSlice(
                    u64,
                    index_block[index.data_addresses_offset..][0..index.data_addresses_size],
                );
            }

            inline fn index_data_checksums(index_block: BlockPtr) []u128 {
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

            inline fn timestamp(index_block: BlockPtr) u32 {
                const header = mem.bytesAsValue(index_block[0..@sizeOf(vsr.Header)]);
                return @intCast(u32, header.offset);
            }

            inline fn data_blocks_used(index_block: BlockPtr) u32 {
                const header = mem.bytesAsValue(index_block[0..@sizeOf(vsr.Header)]);
                return @intCast(u32, header.request);
            }

            inline fn data_block_values(data_block: BlockPtr) []Value {
                return mem.bytesAsSlice(
                    Value,
                    data_block[data.values_offset..][0..data.values_size],
                );
            }

            inline fn data_block_values_used(data_block: BlockPtr) []const Value {
                const header = mem.bytesAsValue(vsr.Header, data_block[0..@sizeOf(vsr.Header)]);
                // TODO we should be able to cross-check this with the header size
                // for more safety.
                const values_used = @intCast(u32, header.request);
                assert(values_used <= data.value_count_max);
                return data_block_values(data_block)[0..values_used];
            }

            inline fn block_address(block: BlockPtr) u64 {
                const header = mem.bytesAsValue(block[0..@sizeOf(vsr.Header)]);
                return @intCast(u32, header.op);
            }

            pub const FlushIterator = struct {
                const Callback = fn () void;

                write: Storage.Write = undefined,
                /// The index of the block that is currently being written/will be written next.
                block: u32 = 0,

                blocks_max: u32 = undefined,
                callback: Callback = undefined,

                pub fn flush(it: *FlushIterator, blocks_max: ?u32, callback: Callback) void {
                    it.blocks_max = blocks_max orelse math.maxInt(u32);
                    it.callback = callback;
                    it.flush_internal();
                }

                fn flush_internal(it: *FlushIterator) void {
                    const table = @fieldParentPtr(Table, "flush_iterator", it);

                    const index_header = mem.bytesAsValue(table.buffer[0..@sizeOf(vsr.Header)]);
                    const filter_blocks_used = index_header.commit;
                    const data_blocks_used = index_header.request;
                    const total_blocks_used = 1 + filter_blocks_used + data_blocks_used;

                    if (it.block == total_blocks_used) {
                        it.flush_complete();
                        return;
                    }
                    assert(it.block < total_blocks_used);

                    if (it.blocks_max == 0) {
                        it.flush_complete();
                        return;
                    }

                    if (it.block == 1 + filter_blocks_used) {
                        const filter_blocks_unused = filter_block_count - filter_blocks_used;
                        it.block += filter_blocks_unused;
                    }

                    const block_buffer = table.buffer[it.block * block_size ..][0..block_size];
                    const header = mem.bytesAsValue(block_buffer[0..@sizeOf(vsr.Header)]);
                    const address = header.op;
                    storage.write_block(on_flush, &it.write, block_buffer, address);
                }

                fn flush_complete(it: *FlushIterator) void {
                    const callback = it.callback;
                    it.blocks_max = undefined;
                    it.callback = undefined;
                    callback();
                }

                fn on_flush(write: *Storage.Write) void {
                    const it = @fieldParentPtr(FlushIterator, "write", write);
                    it.block += 1;
                    it.blocks_max -= 1;
                    it.flush_internal();
                }
            };
        };

        /// Compact tables in level a with overlapping tables in level b.
        pub const Compaction = struct {
            pub const Callback = fn (it: *Compaction, done: bool) void;

            const level_0_table_count_max = 10;
            const level_a_table_count_max = level_0_table_count_max;

            const LevelAIterator = TableIterator(Compaction, on_io_done);
            const LevelBIterator = LevelIterator(Compaction, on_io_done);

            const MergeIterator = KWayMergeIterator(
                Compaction,
                Key,
                Value,
                key_from_value,
                compare_keys,
                // Add one for the level B iterator
                level_a_table_count_max + 1,
            );

            const BlockWrite = struct {
                block: BlockPtr,
                submit: bool,
                write: Storage.Write,
            };

            ticks: u32 = 0,
            io_pending: u32 = 0,
            callback: ?Callback = null,
            /// This is an implementation detail, the caller should use the done
            /// argument of the Callback to know when the compaction has finished.
            last_tick: bool = false,

            /// Addresses of all source tables in level a
            level_a_table_count: u32,
            level_a_iterators_max: [level_a_table_count_max]LevelAIterator,

            level_b_iterator: LevelBIterator,

            merge_iterator: MergeIterator,

            table_builder: Table.Builder,

            index: BlockWrite,
            filter: BlockWrite,
            data: BlockWrite,

            pub fn init(allocator: *mem.Allocator) Compaction {}

            pub fn start(
                compaction: *Compaction,
                level_a_tables: []u64,
                level_b: u32,
                level_b_key_min: Key,
                level_b_key_max: Key,
            ) void {
                assert(compaction.io_pending == 0);
                // There are at least 2 table inputs to the compaction.
                assert(level_a_tables.len + 1 >= 2);
            }

            pub fn tick(compaction: *Compaction, callback: Callback) void {
                assert(!compaction.last_tick);
                assert(compaction.io_pending == 0);
                assert(compaction.callback == null);
                compaction.callback = callback;

                // Submit all read/write I/O before starting the CPU intensive k way merge.
                // This allows the I/O to happen in parallel with the CPU work.
                if (compaction.ticks >= 0) compaction.tick_read();
                if (compaction.ticks >= 2) compaction.tick_write();

                if (compaction.ticks == 1) {
                    // We can't initialize the k way merge until we have at least one
                    // value to peek() from each read stream.
                    const k = compaction.level_a_table_count + 1;
                    assert(k >= 2);
                    compaction.merge_iterator = MergeIterator.init(compaction, k, .ascending);
                }

                if (compaction.ticks >= 1) {
                    if (compaction.merge_iterator.empty()) {
                        assert(compaction.ticks >= 2);
                        assert(!compaction.last_tick);
                        compaction.last_tick = true;
                    } else {
                        compaction.tick_merge();
                    }
                }

                compaction.ticks += 1;

                // We will always start I/O if the compaction has not yet been completed.
                // The callbacks for this I/O must fire asynchronously.
                assert(compaction.io_pending > 0);
            }

            fn tick_read(compaction: *Compaction) void {
                for (compaction.level_a_iterators()) |*it| {
                    if (it.tick()) compaction.io_pending += 1;
                }
                if (compaction.level_b_iterator.tick()) compaction.io_pending += 1;

                if (compaction.last_tick) assert(compaction.io_pending == 0);
            }

            fn tick_write(compaction: *Compaction) void {
                assert(compaction.ticks >= 2);
                assert(compaction.data.submit);

                compaction.maybe_submit_write(compaction.data, on_block_write("data"));
                compaction.maybe_submit_write(compaction.filter, on_block_write("filter"));
                compaction.maybe_submit_write(compaction.index, on_block_write("index"));

                assert(compaction.io_pending > 0);
                assert(!compaction.data.submit);
                assert(!compaction.filter.submit);
                assert(!compaction.index.submit);
            }

            fn tick_merge(compaction: *Compaction) void {
                assert(!compaction.last_tick);
                assert(compaction.ticks >= 1);
                assert(!compaction.merge_iterator.empty());

                assert(!compaction.data.submit);
                assert(!compaction.filter.submit);
                assert(!compaction.index.submit);

                while (!compaction.table_builder.data_block_full()) {
                    const value = compaction.merge_iterator.pop() orelse {
                        compaction.assert_read_iterators_empty();
                        break;
                    };
                    compaction.table_builder.data_block_append(value);
                }
                compaction.table_builder.data_block_finish();
                swap_buffers(&compaction.data, &compaction.table_builder.data_block);

                if (!compaction.merge_iterator.empty()) {
                    const values_used = Table.data_block_values_used(compaction.data.block).len;
                    assert(values_used == Table.data.value_count_max);
                }

                if (compaction.table_builder.filter_block_full() or
                    compaction.table_builder.index_block_full() or
                    compaction.merge_iterator.empty())
                {
                    compaction.table_builder.filter_block_finish();
                    swap_buffers(&compaction.filter, &compaction.table_builder.filter_block);
                }

                if (compaction.table_builder.index_block_full() or
                    compaction.merge_iterator.empty())
                {
                    const info = compaction.table_builder.index_block_finish();
                    swap_buffers(&compaction.index, &compaction.table_builder.index_block);

                    // TODO store info in the manifest at some point. We may need to wait
                    // until the table has been fully written to disk, or until the
                    // compaction finishes. Figure this out when implementing the Manifest.
                    _ = info;
                }

                assert(compaction.data.submit);
            }

            fn on_io_done(compaction: *Compaction) void {
                compaction.io_pending -= 1;
                if (compaction.io_pending == 0) {
                    const callback = compaction.callback.?;
                    compaction.callback = null;
                    callback(compaction, compaction.last_tick);
                }
            }

            fn maybe_submit_write(
                compaction: *Compaction,
                block_write: *BlockWrite,
                callback: fn (*Storage.Write) void,
            ) void {
                if (block_write.submit) {
                    block_write.submit = false;
                    compaction.io_pending += 1;
                    const address = Table.block_address(block_write.block);
                    storage.write_block(callback, &block_write.write, block_write.block, address);
                }
            }

            fn on_block_write(comptime field: []const u8) fn (*Storage.Write) void {
                return struct {
                    fn callback(write: *Storage.Write) void {
                        const block_write = @fieldParentPtr(BlockWrite, "write", write);
                        const compaction = @fieldParentPtr(Compaction, field, block_write);
                        on_io_done(compaction);
                    }
                }.callback;
            }

            fn swap_buffers(block_write: *BlockWrite, filled_block: *BlockPtr) void {
                mem.swap(*BlockPtr, &block_write.block, filled_block);
                assert(!block_write.submit);
                block_write.submit = true;
            }

            fn level_a_iterators(compaction: *Compaction) []LevelAIterator {
                return compaction.level_a_iterators_max[0..compaction.level_a_table_count];
            }

            fn assert_read_iterators_empty(compaction: Compaction) void {
                for (compaction.level_a_iterators()) |it| {
                    assert(it.buffered_all_values());
                    assert(it.peek() == null);
                }
                assert(compaction.level_b_iterator.buffered_all_values());
                assert(compaction.level_b_iterator.peek() == null);
            }

            fn stream_peek(compaction: *Compaction, stream_id: u32) ?Key {
                if (stream_id == 0) {
                    return compaction.level_b_iterator.peek();
                } else {
                    return compaction.level_a_iterators()[stream_id + 1].peek();
                }
            }

            fn stream_pop(compaction: *Compaction, stream_id: u32) Value {
                if (stream_id == 0) {
                    return compaction.level_b_iterator.pop();
                } else {
                    return compaction.level_a_iterators()[stream_id + 1].pop();
                }
            }

            /// Returns true if stream a has higher precedence than stream b.
            /// This is used to deduplicate values across streams.
            ///
            /// This assumes that all overlapping tables in level A at the time the compaction was
            /// started are included in the compaction. If this is not the case, the older table
            /// in a pair of overlapping tables could be left in level A and shadow the newer table
            /// in level B, resulting in data loss/invalid data.
            fn stream_precedence(compaction: *Compaction, a: u32, b: u32) bool {
                assert(a != b);
                // A stream_id of 0 indicates the level B iterator.
                // All tables in level A have higher precedence.
                if (a == 0) return false;
                if (b == 0) return true;

                const it_a = compaction.level_a_iterators()[a + 1];
                const it_b = compaction.level_a_iterators()[b + 1];
                const timestamp_a = Table.timestamp(it_a.index);
                const timestamp_b = Table.timestamp(it_b.index);
                assert(timestamp_a != timestamp_b);
                return timestamp_a > timestamp_b;
            }
        };

        fn LevelIterator(comptime Parent: type, comptime read_done: fn (*Parent) void) type {
            return struct {
                const Self = @This();

                const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);

                parent: *Parent,
                level: u32,
                key_min: Key,
                key_max: Key,
                values: ValuesRingBuffer,
                tables: RingBuffer(TableIterator(Self, on_read_done), 2, .array),

                fn init(allocator: *mem.Allocator) !Self {
                    var values = try ValuesRingBuffer.init(allocator);
                    errdefer values.deinit(allocator);

                    var table_a = try TableIterator.init(allocator);
                    errdefer table_a.deinit(allocator);

                    var table_b = try TableIterator.init(allocator);
                    errdefer table_b.deinit(allocator);

                    return Self{
                        .parent = undefined,
                        .level = undefined,
                        .key_min = undefined,
                        .key_max = undefined,
                        .values = values,
                        .tables = .{
                            .buffer = .{
                                table_a,
                                table_b,
                            },
                        },
                    };
                }

                fn deinit(it: *Self, allocator: *mem.Allocator) void {
                    it.values.deinit(allocator);
                    for (it.tables.buffer) |*table| table.deinit(allocator);
                    it.* = undefined;
                }

                fn reset(
                    it: *Self,
                    parent: *Parent,
                    level: u32,
                    key_min: Key,
                    key_max: Key,
                ) void {
                    it.* = .{
                        .level = level,
                        .key_min = key_min,
                        .key_max = key_max,
                        .values = .{ .buffer = it.values.buffer },
                        .tables = .{ .buffer = it.tables.buffer },
                    };
                    assert(values.empty());
                    assert(tables.empty());
                }

                fn tick(it: *Self) bool {
                    if (it.buffered_enough_values()) return false;

                    if (it.tables.tail_ptr()) |tail| {
                        // Buffer values as necessary for the current tail.
                        if (tail.tick()) return true;
                        // Since buffered_enough_values() was false above and tick did not start
                        // new I/O, the tail table must have already buffered all values.
                        // This is critical to ensure no values are skipped during iteration.
                        assert(tail.buffered_all_values());
                    }

                    if (it.tables.next_tail_ptr()) |next_tail| {
                        read_next_table(next_tail);
                        it.tables.advance_tail();
                        return true;
                    } else {
                        const table = it.tables.head().?;
                        while (table.peek() != null) {
                            it.values.push(table.pop()) catch unreachable;
                        }
                        it.tables.advance_head();

                        read_next_table(it.tables.next_tail_ptr().?);
                        it.tables.advance_tail();
                        return true;
                    }
                }

                fn read_next_table(table: *TableIterator(Self, on_read_done)) void {
                    // TODO this function doesn't exist yet
                    const address = manifest.get_next_address() orelse return false;
                    table.reset(address);
                    const read_pending = table.tick();
                    assert(read_pending);
                }

                fn on_read_done(it: *Self) void {
                    if (!it.tick()) {
                        assert(it.buffered_enough_values());
                        read_done(it.parent);
                    }
                }

                /// Returns true if all remaining values in the level have been buffered.
                fn buffered_all_values(it: Self) bool {
                    // TODO look at the manifest to determine this.
                }

                fn buffered_value_count(it: Self) u32 {
                    var value_count = @intCast(u32, it.values.count);
                    var tables_it = it.tables.iterator();
                    while (tables_it.next()) |table| {
                        value_count += table.buffered_value_count();
                    }
                    return value_count;
                }

                fn buffered_enough_values(it: Self) bool {
                    return it.buffered_all_values() or
                        it.buffered_value_count() >= Table.data.value_count_max;
                }

                fn peek(it: Self) ?Key {
                    if (it.values.head()) |value| return key_from_value(value);

                    const table = it.tables.head_ptr() orelse {
                        assert(it.buffered_all_values());
                        return null;
                    };

                    return table.peek().?;
                }

                /// This is only safe to call after peek() has returned non-null.
                fn pop(it: *Self) Value {
                    if (it.values.pop()) |value| return value;

                    const table = it.tables.head_ptr().?;
                    const value = table.pop();

                    if (table.peek() == null) it.tables.advance_head();

                    return value;
                }
            };
        }

        fn TableIterator(comptime Parent: type, comptime read_done: fn (*Parent) void) type {
            return struct {
                const Self = @This();

                const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);

                parent: *Parent,
                read_table_index: bool,
                address: u64,

                index: BlockPtr,
                /// The index of the current block in the table index block.
                block: u32,

                /// This ring buffer is used to hold not yet popped values in the case that we run
                /// out of blocks in the blocks ring buffer but haven't buffered a full block of
                /// values in memory. In this case, we copy values from the head of blocks to this
                /// ring buffer to make that block available for reading further values.
                /// Thus, we guarantee that iterators will always have at least a block's worth of
                /// values buffered. This simplifies the peek() interface as null always means that
                /// iteration is complete.
                values: ValuesRingBuffer,

                blocks: RingBuffer(BlockPtr, 2, .array),
                /// The index of the current value in the head of the blocks ring buffer.
                value: u32,

                read: Storage.Read = undefined,
                /// This field is only used for safety checks, it does not affect the behavior.
                read_pending = false,

                fn init(allocator: *mem.Allocator) !Self {
                    const index = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(index);

                    const values = try ValuesRingBuffer.init(allocator);
                    errdefer values.deinit(allocator);

                    const block_a = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(block_a);

                    const block_b = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(block_b);

                    return .{
                        .parent = undefined,
                        .read_table_index = undefined,
                        // Use 0 so that we can assert(address != 0) in tick().
                        .address = 0,
                        .index = index[0..block_size],
                        .block = undefined,
                        .values = values,
                        .blocks = .{
                            .buffer = .{
                                block_a[0..block_size],
                                block_b[0..block_size],
                            },
                        },
                        .value = undefined,
                    };
                }

                fn deinit(it: *Self, allocator: *mem.Allocator) void {
                    assert(!it.read_pending);
                    allocator.free(it.index);
                    values.deinit(allocator);
                    for (blocks.buffer) |block| allocator.free(block);
                    it.* = undefined;
                }

                fn reset(it: *Self, parent: *Parent, address: u64) void {
                    assert(!it.read_pending);
                    it.* = .{
                        .parent = parent,
                        .read_table_index = true,
                        .address = address,
                        .index = it.index,
                        .block = 0,
                        .values = .{ .buffer = it.values.buffer },
                        .blocks = .{ .buffer = it.blocks.buffer },
                        .value = 0,
                    };
                    assert(values.empty());
                    assert(blocks.empty());
                }

                /// Try to buffer at least a full block of values to be peek()'d.
                /// A full block may not always be buffered if all 3 blocks are partially full
                /// or if the end of the table is reached.
                /// Returns true if an IO operation was started. If this returns true,
                /// then read_done() will be called on completion.
                fn tick(it: *Self) bool {
                    assert(!it.read_pending);
                    assert(it.address != 0);

                    if (it.read_table_index) {
                        assert(!it.read_pending);
                        it.read_pending = true;
                        storage.read_block(on_read_table_index, &it.read, it.index, address);
                        return true;
                    }

                    if (it.buffered_enough_values()) return false;

                    if (it.blocks.next_tail()) |next_tail| {
                        it.read_next_data_block(next_tail);
                        return true;
                    } else {
                        const values = Table.data_block_values_used(it.blocks.head().?);
                        const values_remaining = values[it.value..];
                        it.values.push_slice(values_remaining) catch unreachable;
                        it.value = 0;
                        it.blocks.advance_head();
                        it.read_next_data_block(it.blocks.next_tail().?);
                        return true;
                    }
                }

                fn read_next_data_block(it: *Self, block: BlockPtr) void {
                    assert(!it.read_table_index);
                    assert(it.block < Table.data_blocks_used(it.index));

                    const addresses = Table.index_data_addresses(it.index);
                    const checksums = Table.index_data_checksums(it.index);
                    const address = addresses[it.block];
                    const checksum = checksums[it.block];

                    assert(!it.read_pending);
                    it.read_pending = true;
                    storage.read_block(on_read, &it.read, block, address, checksum);
                }

                fn on_read_table_index(read: *Storage.Read) void {
                    const it = @fieldParentPtr(Self, "read", read);
                    assert(it.read_pending);
                    it.read_pending = false;

                    assert(it.read_table_index);
                    it.read_table_index = false;

                    const read_pending = it.tick();
                    // After reading the table index, we always read at least one data block.
                    assert(read_pending);
                }

                fn on_read(read: *Storage.Read) void {
                    const it = @fieldParentPtr(Self, "read", read);
                    assert(it.read_pending);
                    it.read_pending = false;

                    assert(!it.read_table_index);

                    it.blocks.advance_tail();
                    it.block += 1;

                    if (!it.tick()) {
                        assert(it.buffered_enough_values());
                        read_done(it.parent);
                    }
                }

                /// Return true if all remaining values in the table have been buffered in memory.
                fn buffered_all_values(it: Self) bool {
                    assert(!it.read_pending);

                    const data_blocks_used = Table.data_blocks_used(it.index);
                    assert(it.block <= data_blocks_used);
                    return it.block == data_blocks_used;
                }

                fn buffered_value_count(it: Self) u32 {
                    assert(!it.read_pending);

                    var value_count: u32 = it.values.count;
                    var blocks_it = it.blocks.iterator();
                    while (blocks_it.next()) |block| {
                        value_count += Table.data_block_values_used(block).len;
                    }
                    // We do this subtraction last to avoid underflow.
                    value_count -= it.value;

                    return value_count;
                }

                fn buffered_enough_values(it: Self) bool {
                    assert(!it.read_pending);

                    return it.buffered_all_values() or
                        it.buffered_value_count() >= Table.data.value_count_max;
                }

                fn peek(it: Self) ?Key {
                    assert(!it.read_pending);
                    assert(!it.read_table_index);

                    if (it.values.head()) |value| return key_from_value(value);

                    const block = it.blocks.head() orelse {
                        assert(it.block == Table.data_blocks_used(it.index));
                        return null;
                    };

                    const values = Table.data_block_values_used(block);
                    return key_from_value(values[it.value]);
                }

                /// This is only safe to call after peek() has returned non-null.
                fn pop(it: *Self) Value {
                    assert(!it.read_pending);
                    assert(!it.read_table_index);

                    if (it.values.pop()) |value| return value;

                    const block = it.blocks.head().?;

                    const values = Table.data_block_values_used(block);
                    const value = values[it.value];

                    it.value += 1;
                    if (it.value == values.len) {
                        it.value = 0;
                        it.blocks.advance_head();
                    }

                    return value;
                }
            };
        }

        storage: *BlockStorage(Storage),

        /// We size and allocate this buffer as a function of MutableTable.value_count_max,
        /// leaving off unneeded data blocks at the end. This saves memory for each LSM tree,
        /// which is important as we have many LSM trees.
        immutable_table_buffer: []u8,

        manifest: []Manifest,

        pub fn init(
            allocator: *std.mem.Allocator,
            storage: *BlockStorage(Storage),
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

test {
    const Key = CompositeKey(u128);
    const TestTree = LsmTree(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
        @import("test/storage.zig").Storage,
    );

    // TODO ref all decls instead
    _ = TestTree;
    _ = TestTree.Table;
    _ = TestTree.Table.create;
}
