const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const ManifestType = @import("manifest.zig").ManifestType;
const GridType = @import("grid.zig").GridType;

/// A TableIterator iterates a table's values in ascending-key order.
pub fn TableIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const TableIterator = @This();

        const Grid = GridType(Storage);
        const Manifest = ManifestType(Table, Storage);
        const ValuesRingBuffer = RingBuffer(Table.Value, Table.data.value_count_max, .pointer);

        const BlockPtrConst = *align(config.sector_size) const [config.block_size]u8;
        const IndexBlockCallback = fn (it: *TableIterator, index_block: BlockPtrConst) void;

        grid: *Grid,
        read_done: fn (*TableIterator) void,
        read_table_index: bool,

        /// We store only the address and checksum of the table's index block to save memory,
        /// since TableIterator is used in every LevelIterator.
        address: u64,
        checksum: u128,

        index_block: Grid.BlockPtr,
        index_block_callback: ?IndexBlockCallback,
        /// The index of the current block in the table index block.
        data_block_index: u32,

        /// This ring buffer is used to hold not yet popped values in the case that we run
        /// out of blocks in the blocks ring buffer but haven't buffered a full block of
        /// values in memory. In this case, we copy values from the head of blocks to this
        /// ring buffer to make that block available for reading further values.
        /// Thus, we guarantee that iterators will always have at least a block's worth of
        /// values buffered. This simplifies the peek() interface as null always means that
        /// iteration is complete.
        values: ValuesRingBuffer,

        data_blocks: RingBuffer(Grid.BlockPtr, 2, .array),
        /// The index of the current value in the head of the blocks ring buffer.
        value: u32,

        read: Grid.Read = undefined,
        /// This field is only used for safety checks, it does not affect the behavior.
        read_pending: bool = false,

        pub fn init(allocator: mem.Allocator) !TableIterator {
            const index_block = try allocator.alignedAlloc(
                u8,
                config.sector_size,
                config.block_size,
            );
            errdefer allocator.free(index_block);

            var values = try ValuesRingBuffer.init(allocator);
            errdefer values.deinit(allocator);

            const block_a = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(block_a);

            const block_b = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(block_b);

            return TableIterator{
                .grid = undefined,
                .read_done = undefined,
                .read_table_index = undefined,
                // Use 0 so that we can assert(address != 0) in tick().
                .address = 0,
                .checksum = undefined,
                .index_block = index_block[0..config.block_size],
                .index_block_callback = null,
                .data_block_index = undefined,
                .values = values,
                .data_blocks = .{
                    .buffer = .{
                        block_a[0..config.block_size],
                        block_b[0..config.block_size],
                    },
                },
                .value = undefined,
            };
        }

        pub fn deinit(it: *TableIterator, allocator: mem.Allocator) void {
            allocator.free(it.index_block);
            it.values.deinit(allocator);
            for (it.data_blocks.buffer) |block| allocator.free(block);
            it.* = undefined;
        }

        pub const Context = struct {
            grid: *Grid,
            address: u64, // Table index block address.
            checksum: u128, // Table index block checksum.
            index_block_callback: ?IndexBlockCallback = null,
        };

        pub fn start(
            it: *TableIterator,
            context: Context,
            read_done: fn (*TableIterator) void,
        ) void {
            assert(!it.read_pending);
            assert(it.index_block_callback == null);

            it.* = .{
                .grid = context.grid,
                .read_done = read_done,
                .read_table_index = true,
                .address = context.address,
                .checksum = context.checksum,
                .index_block = it.index_block,
                .index_block_callback = context.index_block_callback,
                .data_block_index = 0,
                .values = .{ .buffer = it.values.buffer },
                .data_blocks = .{ .buffer = it.data_blocks.buffer },
                .value = 0,
            };

            assert(it.values.empty());
            assert(it.data_blocks.empty());
        }

        /// Try to buffer at least a full block of values to be peek()'d.
        /// A full block may not always be buffered if all 3 blocks are partially full
        /// or if the end of the table is reached.
        /// Returns true if an IO operation was started. If this returns true,
        /// then read_done() will be called on completion.
        pub fn tick(it: *TableIterator) bool {
            assert(!it.read_pending);
            assert(it.address != 0);

            if (it.read_table_index) {
                assert(!it.read_pending);
                it.read_pending = true;
                it.grid.read_block(
                    on_read_table_index,
                    &it.read,
                    it.address,
                    it.checksum,
                    .index,
                );
                return true;
            }

            if (it.buffered_enough_values()) {
                return false;
            } else {
                it.read_next_data_block();
                return true;
            }
        }

        fn read_next_data_block(it: *TableIterator) void {
            assert(!it.read_table_index);
            assert(it.data_block_index < Table.index_data_blocks_used(it.index_block));

            const addresses = Table.index_data_addresses(it.index_block);
            const checksums = Table.index_data_checksums(it.index_block);
            const address = addresses[it.data_block_index];
            const checksum = checksums[it.data_block_index];

            assert(!it.read_pending);
            it.read_pending = true;
            it.grid.read_block(on_read, &it.read, address, checksum, .data);
        }

        fn on_read_table_index(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
            assert(it.read_pending);
            assert(it.data_block_index == 0);
            it.read_pending = false;

            assert(it.read_table_index);
            it.read_table_index = false;

            if (it.index_block_callback) |callback| {
                it.index_block_callback = null;
                callback(it, block);
            }

            // Copy the bytes read into a buffer owned by the iterator since the Grid
            // only guarantees the provided pointer to be valid in this callback.
            mem.copy(u8, it.index_block, block);

            const read_pending = it.tick();
            // After reading the table index, we always read at least one data block.
            assert(read_pending);
            assert(it.read_pending);
        }

        fn on_read(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
            assert(it.read_pending);
            it.read_pending = false;

            assert(!it.read_table_index);

            // If there is not currently a buffer available, copy remaining values to
            // an overflow ring buffer to make space.
            if (it.data_blocks.next_tail() == null) {
                const values = Table.data_block_values_used(it.data_blocks.head().?);
                const values_remaining = values[it.value..];
                it.values.push_slice(values_remaining) catch unreachable;
                it.value = 0;
                it.data_blocks.advance_head();
            }

            // Copy the bytes read into a buffer owned by the iterator since the Grid
            // only guarantees the provided pointer to be valid in this callback.
            mem.copy(u8, it.data_blocks.next_tail().?, block);

            it.data_blocks.advance_tail();
            it.data_block_index += 1;

            if (!it.tick()) {
                assert(it.buffered_enough_values());
                it.read_done(it);
            }
        }

        /// Return true if all remaining values in the table have been buffered in memory.
        pub fn buffered_all_values(it: TableIterator) bool {
            assert(!it.read_pending);

            const data_blocks_used = Table.index_data_blocks_used(it.index_block);
            assert(it.data_block_index <= data_blocks_used);
            return it.data_block_index == data_blocks_used;
        }

        pub fn buffered_value_count(it: TableIterator) u32 {
            assert(!it.read_pending);

            var value_count = it.values.count;
            var blocks_it = it.data_blocks.iterator();
            while (blocks_it.next()) |block| {
                value_count += Table.data_block_values_used(block).len;
            }
            // We do this subtraction last to avoid underflow.
            value_count -= it.value;

            return @intCast(u32, value_count);
        }

        fn buffered_enough_values(it: TableIterator) bool {
            assert(!it.read_pending);

            return it.buffered_all_values() or
                it.buffered_value_count() >= Table.data.value_count_max;
        }

        pub fn peek(it: TableIterator) ?Table.Key {
            assert(!it.read_pending);
            assert(!it.read_table_index);

            if (it.values.head_ptr_const()) |value| return Table.key_from_value(value);

            const block = it.data_blocks.head() orelse {
                assert(it.data_block_index == Table.index_data_blocks_used(it.index_block));
                return null;
            };

            const values = Table.data_block_values_used(block);
            return Table.key_from_value(&values[it.value]);
        }

        /// This is only safe to call after peek() has returned non-null.
        pub fn pop(it: *TableIterator) Table.Value {
            assert(!it.read_pending);
            assert(!it.read_table_index);

            if (it.values.pop()) |value| return value;

            const block = it.data_blocks.head().?;

            const values = Table.data_block_values_used(block);
            const value = values[it.value];

            it.value += 1;
            if (it.value == values.len) {
                it.value = 0;
                it.data_blocks.advance_head();
            }

            return value;
        }
    };
}
