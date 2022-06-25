const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const ManifestType = @import("manifest.zig").ManifestType;
const GridType = @import("grid.zig").GridType;

pub fn TableIteratorType(comptime Table: type) type {
    return struct {
        const TableIterator = @This();

        const Grid = GridType(Table.Storage);
        const Manifest = ManifestType(Table);
        const ValuesRingBuffer = RingBuffer(Table.Value, Table.data.value_count_max, .pointer);

        grid: *Grid,
        read_done: fn (*TableIterator) void,
        read_table_index: bool,
        address: u64,
        checksum: u128,

        index: Table.BlockPtr,
        /// The index of the current block in the table index block.
        block_index: u32,

        /// This ring buffer is used to hold not yet popped values in the case that we run
        /// out of blocks in the blocks ring buffer but haven't buffered a full block of
        /// values in memory. In this case, we copy values from the head of blocks to this
        /// ring buffer to make that block available for reading further values.
        /// Thus, we guarantee that iterators will always have at least a block's worth of
        /// values buffered. This simplifies the peek() interface as null always means that
        /// iteration is complete.
        values: ValuesRingBuffer,

        blocks: RingBuffer(Table.BlockPtr, 2, .array),
        /// The index of the current value in the head of the blocks ring buffer.
        value: u32,

        read: Grid.Read = undefined,
        /// This field is only used for safety checks, it does not affect the behavior.
        read_pending: bool = false,

        pub fn init(allocator: mem.Allocator) !TableIterator {
            const index = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            errdefer allocator.free(index);

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
                .index = index[0..config.block_size],
                .block_index = undefined,
                .values = values,
                .blocks = .{
                    .buffer = .{
                        block_a[0..config.block_size],
                        block_b[0..config.block_size],
                    },
                },
                .value = undefined,
            };
        }

        pub fn deinit(it: *TableIterator, allocator: mem.Allocator) void {
            assert(!it.read_pending);

            allocator.free(it.index);
            it.values.deinit(allocator);
            for (it.blocks.buffer) |block| allocator.free(block);
            it.* = undefined;
        }

        pub const Context = struct {
            // TODO info to extract address address checksum on start().
            grid: *Grid,

            address: u64,
            checksum: u128,
        };

        pub fn start(
            it: *TableIterator,
            context: Context,
            read_done: fn (*TableIterator) void,
        ) void {
            assert(!it.read_pending);
            it.* = .{
                .grid = context.grid,
                .read_done = read_done,
                .read_table_index = true,
                .address = context.address,
                .checksum = context.checksum,
                .index = it.index,
                .block_index = 0,
                .values = .{ .buffer = it.values.buffer },
                .blocks = .{ .buffer = it.blocks.buffer },
                .value = 0,
            };

            assert(it.values.empty());
            assert(it.blocks.empty());
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
            assert(it.block_index < Table.index_data_blocks_used(it.index));

            const addresses = Table.index_data_addresses(it.index);
            const checksums = Table.index_data_checksums(it.index);
            const address = addresses[it.block_index];
            const checksum = checksums[it.block_index];

            assert(!it.read_pending);
            it.read_pending = true;
            it.grid.read_block(on_read, &it.read, address, checksum);
        }

        fn on_read_table_index(read: *Grid.Read, block: Table.BlockPtrConst) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
            assert(it.read_pending);
            it.read_pending = false;

            assert(it.read_table_index);
            it.read_table_index = false;

            // Copy the bytes read into a buffer owned by the iterator since the Grid
            // only guarantees the provided pointer to be valid in this callback.
            mem.copy(u8, it.index, block);

            const read_pending = it.tick();
            // After reading the table index, we always read at least one data block.
            assert(read_pending);
        }

        fn on_read(read: *Grid.Read, block: Table.BlockPtrConst) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
            assert(it.read_pending);
            it.read_pending = false;

            assert(!it.read_table_index);

            // If there is not currently a buffer available, copy remaining values to
            // an overflow ring buffer to make space.
            if (it.blocks.next_tail() == null) {
                const values = Table.data_block_values_used(it.blocks.head().?);
                const values_remaining = values[it.value..];
                it.values.push_slice(values_remaining) catch unreachable;
                it.value = 0;
                it.blocks.advance_head();
            }

            // Copy the bytes read into a buffer owned by the iterator since the Grid
            // only guarantees the provided pointer to be valid in this callback.
            mem.copy(u8, it.blocks.next_tail().?, block);

            it.blocks.advance_tail();
            it.block_index += 1;

            if (!it.tick()) {
                assert(it.buffered_enough_values());
                it.read_done(it);
            }
        }

        /// Return true if all remaining values in the table have been buffered in memory.
        pub fn buffered_all_values(it: TableIterator) bool {
            assert(!it.read_pending);

            const data_blocks_used = Table.index_data_blocks_used(it.index);
            assert(it.block_index <= data_blocks_used);
            return it.block_index == data_blocks_used;
        }

        pub fn buffered_value_count(it: TableIterator) u32 {
            assert(!it.read_pending);

            var value_count = it.values.count;
            var blocks_it = it.blocks.iterator();
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

            const block = it.blocks.head() orelse {
                assert(it.block_index == Table.index_data_blocks_used(it.index));
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
