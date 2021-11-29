const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const vsr = @import("vsr.zig");

const config = @import("config.zig");
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

        pub const sentinel_key: Self = .{
            .secondary = math.maxInt(Secondary),
            .timestamp = math.maxInt(u64),
        };

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
                .timestamp = @truncate(u63, value.timestamp),
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
/// Bits set indicate free blocks.
pub const BlockFreeSet = struct {
    // Each bit of `index` is the OR of `shard_size` bits of `blocks`.
    index: std.bit_set.DynamicBitSetUnmanaged,
    blocks: std.bit_set.DynamicBitSetUnmanaged,

    // Fixing the shard size to a constant rather than varying the shard size (but
    // guaranteeing the index always a multiple of 64B) means that the top-level index
    // may have some unused bits. But the shards themselves are always a multiple of
    // the word size. In practice the tail end of the index will be accessed less
    // frequently than the head/middle anyway.
    //
    // 10TiB disk / 64KiB block size / 640B shard size = 4096B index
    const shard_size = 10 * (64 * 8); // 10 cache lines per shard

    pub fn init(allocator: *std.mem.Allocator, total_blocks: usize) !BlockFreeSet {
        // Round up to ensure that every block bit is covered by the index.
        const index_size = divCeil(usize, total_blocks, shard_size);
        var index = try std.bit_set.DynamicBitSetUnmanaged.initFull(index_size, allocator);
        errdefer index.deinit(allocator);

        const blocks = try std.bit_set.DynamicBitSetUnmanaged.initFull(total_blocks, allocator);
        return BlockFreeSet{
            .index = index,
            .blocks = blocks,
        };
    }

    pub fn deinit(set: *BlockFreeSet, allocator: *std.mem.Allocator) void {
        set.index.deinit(allocator);
        set.blocks.deinit(allocator);
    }

    // TODO consider "caching" the first set bit to speed up subsequent acquire() calls
    pub fn acquire(set: *BlockFreeSet) u64 {
        // TODO: To ensure this "unreachable" is never reached, the leader must reject
        // new requests when storage space is too low to fulfill them.
        return set.try_acquire() orelse unreachable;
    }

    fn try_acquire(set: *BlockFreeSet) ?u64 {
        const index_bit = set.index.findFirstSet() orelse return null;
        const shard_start = index_bit * shard_size;
        const shard_end = std.math.min(shard_start + shard_size, set.blocks.bit_length);
        assert(shard_start < set.blocks.bit_length);

        const bit = findFirstSetBit(set.blocks, shard_start, shard_end) orelse return null;
        assert(set.blocks.isSet(bit));
        set.blocks.unset(bit);

        // Update the index when every block in the shard is allocated.
        if (findFirstSetBit(set.blocks, shard_start, shard_end) == null) {
            set.index.unset(index_bit);
        }

        const address = bit + 1;
        return @intCast(u64, address);
    }

    pub fn release(set: *BlockFreeSet, address: u64) void {
        const bit = address - 1;
        assert(!set.blocks.isSet(bit));
        set.blocks.set(bit);

        const index_bit = @divTrunc(bit, shard_size);
        set.index.set(index_bit);
    }

    pub fn decode(bytes: []const u8) !BlockFreeSet {
        // TODO what if the size of the decoded block set disagrees with the configured total_blocks?
        const set = try BlockFreeSet.init(total_blocks);
        // TODO
        return set;
    }

    // Returns the number of bytes that the BlockFreeSet needs to encode to.
    pub fn predictSize(set: BlockFreeSet) usize {
        return set.encodingSchema().size;
    }

    fn encodingSchema(set: BlockFreeSet) BlockFreeSetSchema {
        const wc = divCeil(usize, set.blocks.bit_length, 64);
        var runs: usize = 0;
        for (set.blocks.masks[0..wc]) |word| {
            if (word == 0 or word == ~@as(usize, 0)) runs += 1;
        }
        const literals = wc - runs;
        const run_or_literal_size = divCeil(usize, runs + literals, 64) * 8; // 1 bit per run|literal, rounded up to word
        const run_of_zeroes_or_ones_size = divCeil(usize, runs, 64) * 8; // 1 bit per run, rounded up to word
        const run_lengths_size = divCeil(usize, runs, 8) * 8; // 1 byte per run

        const run_or_literal_offset = @sizeOf(u64);
        const run_of_zeroes_or_ones_offset = run_or_literal_offset + run_or_literal_size;
        const run_lengths_offset = run_of_zeroes_or_ones_offset + run_of_zeroes_or_ones_size;
        const literals_offset = run_lengths_offset + run_lengths_size;
        return .{
            .size = literals_offset + literals * 8,
            .run_or_literal_count = @intCast(u32, runs + literals),
            .run_or_literal_offset = run_or_literal_offset,
            .run_of_zeroes_or_ones_offset = run_of_zeroes_or_ones_offset,
            .run_lengths_offset = run_lengths_offset,
            .literals_offset = literals_offset,
        };
    }

    // Encoding:
    // - u32 count of runs + count of literals
    // - u32 (unused)
    // - [run_or_literal_count]u1 run_of_zeroes_or_ones (padded to word size)
    // - [number of runs]u1 run_of_zeroes_or_ones (padded to word size)
    // - [number of literals]u64 literals
    pub fn encodeTo(set: BlockFreeSet, dst: []u8) void {
        const schema = set.encodingSchema();
        assert(schema.size == dst.len);
        for (dst) |*b| b.* = 0;

        var run_or_literal = dst[schema.run_or_literal_offset..schema.run_of_zeroes_or_ones_offset];
        var run_or_literal_i: usize = 0;
        var run_of_zeroes_or_ones = dst[schema.run_of_zeroes_or_ones_offset..schema.run_lengths_offset];
        var run_of_zeroes_or_ones_i: usize = 0;
        var run_lengths = dst[schema.run_lengths_offset..schema.literals_offset];
        var run_lengths_i: usize = 0;
        var literals = dst[schema.literals_offset..schema.size];
        var literals_i: usize = 0;

        var w: usize = 0;
        const wc = divCeil(usize, set.blocks.bit_length, 64); // TODO those extra bits are being encoded too...
        while (w < wc) : (w += 1) {
            const word = set.blocks.masks[w];
            if (word == 0 or word == ~@as(usize, 0)) {
                var run_len: usize = 1;
                while (run_len < 256 and w + run_len < wc and set.blocks.masks[w + run_len] == word)
                    : (run_len += 1) {}
                run_lengths[run_lengths_i] = @intCast(u8, run_len - 1);
                run_lengths_i += 1;
                setBit(run_of_zeroes_or_ones, run_of_zeroes_or_ones_i);
                run_of_zeroes_or_ones_i += 1;
                setBit(run_or_literal, run_or_literal_i);
            } else {
                const o = 8 * literals_i;
                std.mem.writeIntLittle(u64, literals[o .. o + 8][0..8], word);
                literals_i += 1;
            }
            run_or_literal_i += 1;
        }
        std.mem.writeIntLittle(u32, dst[0..4], schema.run_or_literal_count);

        assert(run_of_zeroes_or_ones_i == run_lengths.len);
        assert(run_lengths_i == run_lengths.len);
        assert(literals_i * 8 == literals.len);
    }
};

fn setBit(b: []u8, i: usize) void {
    assert(b[@divTrunc(i, 8)] & (@as(u8, 1) << @truncate(u3, i % 8)) == 0); // TODO remove this, its just for testing!
    b[@divTrunc(i, 8)] |= @as(u8, 1) << @truncate(u3, i % 8);
}

const BlockFreeSetSchema = struct {
    size: usize, // bytes
    run_or_literal_count: u32,
    run_or_literal_offset: usize,
    run_of_zeroes_or_ones_offset: usize,
    run_lengths_offset: usize,
    literals_offset: usize,
};

// Returns the index of a set bit (relative to the start of the bitset) within start…end (inclusive…exclusive).
fn findFirstSetBit(bitset: std.bit_set.DynamicBitSetUnmanaged, start: usize, end: usize) ?usize {
    const MaskInt = std.bit_set.DynamicBitSetUnmanaged.MaskInt;
    const word_start = @divTrunc(start, @bitSizeOf(MaskInt));
    const word_offset = @mod(start, @bitSizeOf(MaskInt));
    const word_end = divCeil(usize, end, @bitSizeOf(MaskInt));
    assert(word_start < word_end);

    // Only iterate over the subset of bits that were requested.
    var iter = bitset.iterator(.{});
    iter.words_remain = bitset.masks[word_start+1..word_end];
    const mask = ~@as(MaskInt, 0);
    iter.bits_remain = bitset.masks[word_start] & std.math.shl(MaskInt, mask, word_offset);

    const b = start - word_offset + (iter.next() orelse return null);
    return if (b < end) b else null;
}

fn divCeil(comptime T: type, a: T, b: T) T {
    return @divTrunc(a + b - 1, b);
}

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

pub const LsmForest = struct {
    block_free_set: BlockFreeSet,
    mutable_table_iterator_buffer: [config.lsm_mutable_table_size_max]u8,
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

            pub const Iterator = struct {
                values_sorted: []Value,

                /// Returns the number of values copied, 0 if there are no values left.
                pub fn copy_values(it: *Iterator, target: []Value) usize {
                    const count = math.min(it.values_sorted.len, target.len);
                    mem.copy(Value, target, it.values_sorted[0..count]);
                    it.values_sorted = it.values_sorted[count..];
                    return count;
                }
            };

            pub fn iterator(table: *MutableTable, sort_buffer: []align(@alignOf(Value)) u8) Iterator {
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
                std.sort.insertionSort(Value, values, {}, less_than);

                return Iterator{ .values_sorted = values };
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

                    .data_block_count = data_blocks,
                    .filter_block_count = filter_blocks,
                };
            };

            const index_block_count = 1;
            const filter_block_count = layout.filter_block_count;
            const data_block_count = layout.data_block_count;

            const index = struct {
                const size = @sizeOf(vsr.Header) + filter_checksums_size + data_checksums_size +
                    keys_size + filter_addresses_size + data_addresses_size;

                const filter_checksums_offset = @sizeOf(vsr.Header);
                const filter_checksums_size = filter_block_count * checksum_size;

                const data_checksums_offset = filter_checksums_offset + filter_checksums_size;
                const data_checksums_size = data_block_count * checksum_size;

                const keys_offset = data_checksums_offset + data_checksums_size;
                const keys_size = data_block_count * key_size;

                const filter_addresses_offset = keys_offset + keys_size;
                const filter_addresses_size = filter_block_count * address_size;

                const data_addresses_offset = filter_addresses_offset + filter_addresses_size;
                const data_addresses_size = data_block_count * address_size;

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
                            data_block_count,

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
                assert(data_block_count > 0);
                assert(index_block_count + filter_block_count +
                    data_block_count <= table_block_count_max);
                const filter_bytes_per_key = 2;
                assert(filter_block_count * block_size >= data_block_count *
                    data.value_count_max * filter_bytes_per_key);

                assert(index.size == @sizeOf(vsr.Header) +
                    data_block_count * (key_size + address_size + checksum_size) +
                    filter_block_count * (address_size + checksum_size));
                assert(index.size == index.data_addresses_offset + index.data_addresses_size);
                assert(index.size <= block_size);
                assert(index.keys_size > 0);
                assert(index.keys_size % key_size == 0);
                assert(@divExact(index.data_addresses_size, @sizeOf(u64)) == data_block_count);
                assert(@divExact(index.filter_addresses_size, @sizeOf(u64)) == filter_block_count);
                assert(@divExact(index.data_checksums_size, @sizeOf(u128)) == data_block_count);
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

                var key_min: Key = undefined;

                const data_blocks_used = for (data_blocks) |*block, i| {
                    // For each block we write the sorted values, initialize the Eytzinger layout,
                    // complete the block header, and add the block's max key to the table index.

                    const values_bytes = @alignCast(
                        @alignOf(Value),
                        block[data.values_offset..][0..data.values_size],
                    );
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

                    const values_padding = mem.sliceAsBytes(values_max[value_count..]);
                    const block_padding = block[data.padding_offset..][0..data.padding_size];
                    mem.set(u8, values_padding, 0);
                    mem.set(u8, block_padding, 0);

                    const header_bytes = block[0..@sizeOf(vsr.Header)];
                    const header = mem.bytesAsValue(vsr.Header, header_bytes);

                    header.* = .{
                        .cluster = cluster,
                        .op = block_free_set.acquire(),
                        .size = block_size - @intCast(u32, values_padding.len - block_padding.len),
                        .command = .block,
                    };

                    header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    table.index_keys()[i] = key_from_value(values[values.len - 1]);
                    table.index_data_addresses()[i] = header.op;
                    table.index_data_checksums()[i] = header.checksum;
                } else data_block_count;

                assert(data_blocks_used > 0);

                const index_keys_padding = mem.sliceAsBytes(index_keys[data_blocks_used..]);
                mem.set(u8, index_keys_padding, 0);
                mem.set(u64, table.index_data_addresses()[data_blocks_used..], 0);
                mem.set(u128, table.index_data_checksums()[data_blocks_used..], 0);

                // TODO implement filters
                const filter_blocks_used = 0;
                for (filter_blocks) |*block| {
                    mem.set(u8, block[0..@sizeOf(vsr.Header)], 0);
                    comptime assert(filter.padding_offset == @sizeOf(vsr.Header));
                    mem.set(u8, block[filter.padding_offset..][0..filter.padding_size], 0);
                }
                mem.set(u64, table.index_filter_addresses(), 0);
                mem.set(u128, table.index_filter_checksums(), 0);

                mem.set(u8, index_block[index.padding_offset..][0..index.padding_size], 0);

                const header_bytes = index_block[0..@sizeOf(vsr.Header)];
                const header = mem.bytesAsValue(vsr.Header, header_bytes);

                header.* = .{
                    .cluster = cluster,
                    .op = block_free_set.acquire(),
                    .commit = filter_blocks_used,
                    .offset = data_blocks_used,
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
                    .flush_iterator = .{},
                };
            }

            inline fn index_keys(table: *ImmutableTable) []Key {
                return mem.bytesAsSlice(
                    Key,
                    table.buffer[index.keys_offset..][0..index.keys_size],
                );
            }

            inline fn index_data_addresses(table: *ImmutableTable) []u64 {
                return mem.bytesAsSlice(
                    u64,
                    table.buffer[index.data_addresses_offset..][0..index.data_addresses_size],
                );
            }

            inline fn index_data_checksums(table: *ImmutableTable) []u128 {
                return mem.bytesAsSlice(
                    u128,
                    table.buffer[index.data_checksums_offset..][0..index.data_checksums_size],
                );
            }

            inline fn index_filter_addresses(table: *ImmutableTable) []u64 {
                return mem.bytesAsSlice(
                    u64,
                    table.buffer[index.filter_addresses_offset..][0..index.filter_addresses_size],
                );
            }

            inline fn index_filter_checksums(table: *ImmutableTable) []u128 {
                return mem.bytesAsSlice(
                    u128,
                    table.buffer[index.filter_checksums_offset..][0..index.filter_checksums_size],
                );
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
                    const table = @fieldParentPtr(ImmutableTable, "flush_iterator", it);

                    const index_header = mem.bytesAsValue(table.buffer[0..@sizeOf(vsr.Header)]);
                    const filter_blocks_used = index_header.commit;
                    const data_blocks_used = index_header.offset;
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

        block_free_set: *BlockFreeSet,
        storage: *Storage,
        options: LsmTreeOptions,

        /// We size and allocate this buffer as a function of MutableTable.value_count_max,
        /// leaving off unneeded data blocks at the end. This saves memory for each LSM tree,
        /// which is important as we have many LSM trees.
        immutable_table_buffer: []u8,

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

//test {
//    const Key = CompositeKey(u128);
//    const TestTree = LsmTree(
//        Key,
//        Key.Value,
//        Key.compare_keys,
//        Key.key_from_value,
//        Key.sentinel_key,
//        Key.tombstone,
//        Key.tombstone_from_key,
//        @import("test/storage.zig").Storage,
//    );
//
//    // TODO ref all decls instead
//    _ = TestTree;
//    _ = TestTree.ImmutableTable;
//    _ = TestTree.ImmutableTable.create;
//}

fn testBlockFreeSet(total_blocks: usize) !void {
    const expectEqual = std.testing.expectEqual;
    // Acquire everything, then release, then acquire again.
    var set = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer set.deinit(std.testing.allocator);
    var i: usize = 0;
    while (i < total_blocks) : (i += 1) {
        try expectEqual(@as(?u64, i + 1), set.try_acquire());
    }
    try expectEqual(@as(?u64, null), set.try_acquire());

    i = 0;
    while (i < total_blocks) : (i += 1) set.release(@as(u64, i + 1));

    i = 0;
    while (i < total_blocks) : (i += 1) {
        try expectEqual(@as(?u64, i + 1), set.try_acquire());
    }
    try expectEqual(@as(?u64, null), set.try_acquire());
}

fn testBlockIndexSize(expect_index_size: usize, total_blocks: usize) !void {
    var set = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer set.deinit(std.testing.allocator);

    try std.testing.expectEqual(expect_index_size, set.index.bit_length);
}

test "BlockFreeSet" {
    {
        const block_bytes = 64 * 1024;
        const blocks_in_tb = (1 << 40) / block_bytes;
        try testBlockIndexSize(4096 * 8, 10 * blocks_in_tb);
        try testBlockIndexSize(1, 1); // At least one index bit is required.
    }
    {
        try testBlockFreeSet(64 * 64);
        var i: usize = 1;
        while (i < 128) : (i += 1) try testBlockFreeSet(64 * 8 + i);
    }
}

fn testBlockFreeSetEncode(total_blocks: usize, set_bits: usize) !void {
    var seed: u64 = undefined;
    try std.os.getrandom(std.mem.asBytes(&seed));
    var prng = std.rand.DefaultPrng.init(seed);

    var set = try BlockFreeSet.init(std.testing.allocator, total_blocks);
    defer set.deinit(std.testing.allocator);

    var b: usize = 0;
    while (b < set_bits) : (b += 1) {
        // Skip over the index for simplicity.
        if (set_bits == total_blocks) {
            set.blocks.set(b);
        } else {
            set.blocks.set(prng.random.uintLessThan(usize, total_blocks));
        }
    }

    var buf = try std.testing.allocator.alloc(u8, set.predictSize());
    defer std.testing.allocator.free(buf);
    set.encodeTo(buf);
}

test "BlockFreeSet encoding/decoding" {
    const total_blocks = 64 * 64 * 64;
    var t: usize = 0;
    while (t < 100) : (t += 1) {
        try testBlockFreeSetEncode(total_blocks, total_blocks / 4);
    }

    try testBlockFreeSetEncode(64 * 64, 0); // fully allocated
    try testBlockFreeSetEncode(64 * 64, 64 * 64); // fully free
}

test "findFirstSetBit" {
    const BitSet = std.bit_set.DynamicBitSetUnmanaged;
    const window = 8;

    // Verify that only bits within the specified range are returned.
    var size: usize = @bitSizeOf(BitSet.MaskInt);
    while (size <= @bitSizeOf(BitSet.MaskInt) * 2) : (size += 1) {
        var set = try BitSet.initEmpty(size, std.testing.allocator);
        defer set.deinit(std.testing.allocator);

        var s: usize = 0;
        while (s < size - window) : (s += 1) {
            var b: usize = 0;
            while (b < size) : (b += 1) {
                set.set(b);
                const expect = if (s <= b and b < s + window) b else null;
                try std.testing.expectEqual(expect, findFirstSetBit(set, s, s + window));
                set.unset(b);
            }
        }
    }

    {
        // Make sure the first bit is returned.
        var set = try BitSet.initEmpty(16, std.testing.allocator);
        defer set.deinit(std.testing.allocator);
        set.set(2);
        set.set(5);
        try std.testing.expectEqual(@as(?usize, 2), findFirstSetBit(set, 1, 9));
    }
}
