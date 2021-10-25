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

pub const CompositeKey = extern struct {
    const tombstone_bit = 1 << 63;

    pub const Value = extern struct {
        secondary: u128,
        /// The most significant bit indicates if the value is a tombstone
        timestamp: u64,

        comptime {
            assert(@sizeOf(Value) == 24);
            assert(@alignOf(Value) == 16);
        }
    };

    secondary: u128,
    /// The most significant bit must be unset as it is used to indicate a tombstone
    timestamp: u64,

    comptime {
        assert(@sizeOf(CompositeKey) == 24);
        assert(@alignOf(CompositeKey) == 16);
    }

    pub fn compare_keys(a: CompositeKey, b: CompositeKey) math.Order {
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

    pub fn key_from_value(value: Value) CompositeKey {
        return .{
            .secondary = value.secondary,
            .timestamp = @truncate(u63, key.timestamp),
        };
    }

    pub fn tombstone(value: Value) bool {
        return value.timestamp & tombstone_bit != 0;
    }

    pub fn tombstone_from_key(key: CompositeKey) Value {
        return .{
            .secondary = key.secondary,
            .timestamp = key.timestamp | tombstone_bit,
        };
    }
};

pub const TableFreeSet = struct {
    /// Bits set indicate free tables
    free: std.bit_set.DynamicBitSetUnmanaged,

    pub fn init(allocator: *std.mem.Allocator, count: usize) !TableFreeSet {
        return TableFreeSet{
            .free = try std.bit_set.DynamicBitSetUnmanaged.initFull(count, allocator),
        };
    }

    pub fn deinit(set: *TableFreeSet, allocator: *std.mem.Allocator) void {
        set.free.deinit(allocator);
    }

    pub fn acquire(set: *TableFreeSet) ?u32 {
        const table = set.free.findFirstSet() orelse return null;
        set.free.unset(table);
        return @intCast(u32, table);
    }

    pub fn release(set: *TableFreeSet, table: u32) void {
        assert(!set.free.isSet(table));
        set.free.set(table);
    }
};

// vsr.zig
pub const SuperBlock = packed struct {
    // IDEA: to reduce the size of the superblock we could make the manifest use
    // half disk sectors instead.
    pub const Manifest = packed struct {
        /// Hash chained checksum of manifest sectors stored outside the superblock.
        /// On startup, all sectors of the manifest are read in from disk and the checksum
        /// of each is calculated and chained together to produce this value. On writing a
        /// new manifest sector, we calculate the checksum of that sector and combine it
        /// with the current value of this checksum to obtain the new value.
        parent_checksum: u128,
        offset: u64,
        sectors: u32,
        /// This is stored in the superblock so that we can
        /// append new table metadata to the same sector without
        /// copy on write.
        tail: [config.sector_size]u8,
    };

    checksum: u128,
    vsr_committed_log_offset: u64,
    /// Consider replacing with a timestamp
    parent: u128,
    client_table: [config.clients_max]ClientTableEntry,

    manifests: [config.lsm_trees]Manifest,
    snapshot_manifests: [config.lsm_trees]Manifest,
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

/// Limited to message_size_max in size
///
/// (128 bytes data * 8 bits per byte + 9.04 bits per key + 1) / 8 bits per byte =
/// 129.13 bytes data + filter per key/value pair
///
/// The size of the index is determined only by the number of pages in the
/// table, not by the number of objects per page.
///
/// At the start of each page there is a tombstone_count value, which is
/// the count of removed/dead/tombstone keys in that page. When reading the page,
/// the implementation should first read N - tombstone_count values from the page
/// where N is the total number of values that fit in the page. Then if the key
/// is not found, tombstone_count keys should be read from the end of the page.
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

        // To obtain the checksums for a manifest, divide size by message_size_max
        // using ceiling division to determine the number of checksums.
        // The checksum slices for all manifests are stored in order in manifest_checksums.
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
            pub const Table = packed struct {
                checksum: u128,
                id: u32,
                size: u32,
                timestamp: u64,
                key_min: Key,
                key_max: Key,

                comptime {
                    assert(@sizeOf(Table) == 32 + key_size * 2);
                    assert(@alignOf(Table) == 16);
                }
            };

            superblock: SuperBlock.Manifest,
            tables: []Table,
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
            values_sorted: *[page_values_count]Value,

            pub fn init(allocator: *std.mem.Allocator) !MutableTable {
                var table: MutableTable = .{
                    .values_sorted = try table.allocator.create([page_values_count]Value),
                };
                errdefer allocator.destroy(table.values_sorted);
                try table.values.ensureTotalCapacity(page_values_count);
                return table;
            }

            /// Add the given value to the table
            pub fn put(table: *MutableTable, value: Value) void {
                table.values.putAssumeCapacity(value, {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= page_values_count);
            }

            pub fn remove(table: *MutableTable, key: Key) void {
                table.values.putAssumeCapacity(tombstone_from_key(key), {});
                // The hash map implementation may allocate more memory
                // than strictly needed due to a growth factor.
                assert(table.values.count() <= page_values_count);
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
            const page_size = config.lsm_table_page_size;

            const layout = blk: {
                const filter_bytes_per_key = 2;

                var filter_size = 0;
                var index_size = 0;
                var pages_size = page_size * @divFloor(
                    config.message_size_max - @sizeOf(vsr.Header),
                    page_size,
                );
                var pages_offset: comptime_int = undefined;

                while (true) : (pages_size -= page_size) {
                    filter_size = @divExact(pages_size, value_size) * filter_bytes_per_key;
                    index_size = @divExact(pages_size, page_size) * key_size;
                    pages_offset = vsr.sector_ceil(@sizeOf(vsr.Header) + filter_size + index_size);
                    if (pages_offset + pages_size <= config.message_size_max) break;
                }

                var page_keys_count: comptime_int = undefined;
                var page_values_count: comptime_int = undefined;

                // Searching the values array is more expensive than searching the per-page index
                // as the larger values size leads to more cache misses. We can therefore speed
                // up lookups by making the per page index larger at the cost of reducing the
                // number of values that may be stored per page.
                //
                // X = values per page
                // Y = keys per page
                //
                // R = config.lsm_table_value_to_index_ratio_min
                //
                // To maximize:
                //     Y
                // Given constraints:
                //     page_size >= X * value_size + Y * key_size
                //     (X * value_size) / (Y * key_size) >= R
                //     X >= Y
                //
                // Plots of above constraints:
                //     https://www.desmos.com/calculator/elqqaalgbc
                //
                // page_size - X * value_size = Y * key_size
                // Y = (page_size - X * value_size) / key_size
                //
                // (X * value_size) / (page_size - X * value_size) = R
                // (X * value_size) = R * page_size - R * X * value_size
                // (R + 1) * X * value_size = R * page_size
                // X = R * page_size / ((R + 1)* value_size)
                //
                // Y = (page_size - (R * page_size / ((R + 1) * value_size)) * value_size) / key_size
                // Y = (page_size - (R / (R + 1)) * page_size) / key_size
                // Y = page_size / ((R + 1) * key_size)
                page_keys_count = math.min(
                    page_size / ((config.lsm_table_value_to_index_ratio_min + 1) * key_size),
                    page_size / (value_size + key_size),
                );

                // Round to the next lowest power of two. This speeds up lookups in the Eytzinger
                // layout and should help ensure better alignment for the following values.
                // We could round to the nearest power of two, but then we would need
                // care to avoid breaking e.g. the X >= Y invariant above.
                page_keys_count = math.floorPowerOfTwo(comptime_int, page_keys_count);

                // If the index is smaller than 16 keys then there are key sizes >= 4 such that
                // the total index size is not 64 byte cache line aligned.
                assert(@sizeOf(Key) >= 4);
                assert(@sizeOf(Key) % 4 == 0);
                if (page_keys_count < config.cache_line_size / 4) page_keys_count = 0;
                assert((page_keys_count * key_size) % config.cache_line_size == 0);

                page_values_count = (page_size - page_keys_count * key_size) / value_size;

                break :blk .{
                    .filter_offset = @sizeOf(vsr.Header),
                    .filter_size = filter_size,

                    .index_offset = @sizeOf(vsr.Header) + filter_size,
                    .index_size = index_size,

                    .pages_offset = pages_offset,
                    .pages_size = pages_size,

                    .table_size = pages_offset + pages_size,

                    .page_keys_count = page_keys_count,
                    .page_values_count = page_values_count,
                };
            };

            const filter_offset = layout.filter_offset;
            const filter_size = layout.filter_size;
            const index_offset = layout.index_offset;
            const index_size = layout.index_size;
            const pages_offset = layout.pages_offset;
            const pages_size = layout.pages_size;
            const table_size = layout.table_size;
            const page_keys_count = layout.page_keys_count;
            const page_values_count = layout.page_values_count;

            const page_keys_offset = 0;
            const page_keys_size = page_keys_count * key_size;
            const page_values_offset = page_keys_offset + page_keys_size;
            const page_values_size = page_values_count * value_size;
            const page_padding_offset = page_values_offset + page_values_size;
            const page_padding_size = page_size - page_padding_offset;

            comptime {
                const page_size = config.lsm_table_page_size;
                const filter_bytes_per_key = 2;

                assert(filter_size > 0);
                assert(index_size > 0);
                assert(pages_size > 0);

                assert(filter_size == @divExact(pages_size, value_size) * filter_bytes_per_key);
                assert(index_size == @divExact(pages_size, page_size) * key_size);
                assert(pages_size % page_size == 0);

                assert(table_size == @sizeOf(vsr.Header) + filter_size + index_size + pages_size);
                assert(table_size < config.message_size_max);

                assert(filter_offset == @sizeOf(vsr.Header));
                assert(index_offset == filter_offset + filter_size);
                assert(pages_offset == vsr.sector_ceil(index_offset + index_size));

                assert(page_keys_count >= 0);
                assert(page_keys_count == 0 or math.isPowerOfTwo(page_keys_count));
                assert(page_keys_size == 0 or page_values_size / page_keys_size >=
                    config.lsm_table_value_to_index_ratio_min);

                assert(page_values_count > 0);
                assert(page_values_offset % config.cache_line_size == 0);
                // You can have any value size you want, as long as it fits
                // neatly into the CPU cache lines :)
                assert((page_values_count * value_size) % config.cache_line_size == 0);

                assert(page_padding_size >= 0);
                assert(page_size == page_keys_size + page_values_size + page_padding_size);
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
                const pages = buffer[pages_offset..][0..pages_size];

                // TODO: finish this loop
                var page_index: usize = 0;
                while (true) : (page_index += 1) {
                    const page = pages[page_index * page_size ..][0..page_size];
                    const page_values_bytes = page[page_values_offset..][0..page_values_size];
                    const page_values = mem.bytesAsSlice(Value, page_values_bytes);
                    assert(page_values.len == page_values_count);
                    const count = iterator.copy_values(page_values);
                    assert(count <= page_values_count);
                    // The page is empty
                    if (count == 0) break;

                    mem.set(u8, mem.sliceAsBytes(page_values[count..]), 0);
                    mem.set(u8, page[page_padding_offset..][0..page_padding_size], 0);

                    // TODO: construct the Eytzinger layout key index
                }

                if (builtin.mode == .Debug) {
                    var a = values[0];
                    for (values[1..]) |b| {
                        assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                        a = b;
                    }
                }

                // Zero the end of the last page and any subsequent unused pages:
                // We want to prevent random memory from bleeding out to storage.
                mem.set(u8, pages[values_size..], 0);

                // Ensure values do not straddle page boundaries to avoid padding between pages:
                // For example, 8/16/24/64/128-byte values all divide a 6 sector page perfectly.
                comptime assert(config.lsm_table_page_size % value_size == 0);

                mem.set(u8, buffer[filter_offset..][0..filter_size], 0);

                // Construct index, a key_min for every page:
                {
                    const values_per_page = @divExact(config.lsm_table_page_size, value_size);
                    var i: usize = 0;
                    while (i < values.len) : (i += 1) {
                        index[i] = values[i * values_per_page];
                    }
                    mem.set(u8, mem.asBytes(index[i..]), 0);
                    mem.set(u8, buffer[index_offset + index_size .. pages_offset], 0);
                }

                header.* = .{
                    .cluster = cluster,
                    .command = .table,
                    .offset = id,
                    .op = timestamp,
                    // The MessageBus will pad with zeroes to the next sector boundary
                    // on receiving the message.
                    .size = pages_offset + values_size,
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

        table_free_set: *TableFreeSet,
        storage: *Storage,
        options: LsmTreeOptions,

        write_transaction: WriteTransaction,

        manifest: []Manifest,

        pub fn init(
            allocator: *std.mem.Allocator,
            table_free_set: *TableFreeSet,
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
