const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const GridType = @import("grid.zig").GridType;

pub fn TableIteratorType(
    comptime Table: type,
    comptime Parent: type, 
    comptime read_done: fn (*Parent) void,
) type {
    _ = read_done; // TODO

    return struct {
        const TableIterator = @This();

        const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);

        grid: *Grid,
        parent: *Parent,
        read_table_index: bool,
        address: u64,
        checksum: u128,

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

        read: Grid.Read = undefined,
        /// This field is only used for safety checks, it does not affect the behavior.
        read_pending: bool = false,

        pub fn init(allocator: mem.Allocator) !TableIterator {
            const index = try allocator.alignedAlloc(u8, config.sector_size, block_size);
            errdefer allocator.free(index);

            const values = try ValuesRingBuffer.init(allocator);
            errdefer values.deinit(allocator);

            const block_a = try allocator.alignedAlloc(u8, config.sector_size, block_size);
            errdefer allocator.free(block_a);

            const block_b = try allocator.alignedAlloc(u8, config.sector_size, block_size);
            errdefer allocator.free(block_b);

            return .{
                .grid = undefined,
                .parent = undefined,
                .read_table_index = undefined,
                // Use 0 so that we can assert(address != 0) in tick().
                .address = 0,
                .checksum = undefined,
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

        pub fn deinit(it: *TableIterator, allocator: mem.Allocator) void {
            assert(!it.read_pending);

            allocator.free(it.index);
            it.values.deinit(allocator);
            for (it.blocks.buffer) |block| allocator.free(block);
            it.* = undefined;
        }

        fn reset(
            it: *TableIterator,
            grid: *Grid,
            parent: *Parent,
            address: u64,
            checksum: u128,
        ) void {
            assert(!it.read_pending);
            it.* = .{
                .grid = grid,
                .parent = parent,
                .read_table_index = true,
                .address = address,
                .checksum = checksum,
                .index = it.index,
                .block = 0,
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
                    it.index,
                    it.address,
                    it.checksum,
                );
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

        fn read_next_data_block(it: *TableIterator, block: BlockPtr) void {
            assert(!it.read_table_index);
            assert(it.block < Table.index_data_blocks_used(it.index));

            const addresses = Table.index_data_addresses(it.index);
            const checksums = Table.index_data_checksums(it.index);
            const address = addresses[it.block];
            const checksum = checksums[it.block];

            assert(!it.read_pending);
            it.read_pending = true;
            it.grid.read_block(on_read, &it.read, block, address, checksum);
        }

        fn on_read_table_index(read: *Grid.Read) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
            assert(it.read_pending);
            it.read_pending = false;

            assert(it.read_table_index);
            it.read_table_index = false;

            const read_pending = it.tick();
            // After reading the table index, we always read at least one data block.
            assert(read_pending);
        }

        fn on_read(read: *Grid.Read) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
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
        pub fn buffered_all_values(it: TableIterator) bool {
            assert(!it.read_pending);

            const data_blocks_used = Table.index_data_blocks_used(it.index);
            assert(it.block <= data_blocks_used);
            return it.block == data_blocks_used;
        }

        fn buffered_value_count(it: TableIterator) u32 {
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

        pub fn peek(it: TableIterator) ?Key {
            assert(!it.read_pending);
            assert(!it.read_table_index);

            if (it.values.head()) |value| return key_from_value(value);

            const block = it.blocks.head() orelse {
                assert(it.block == Table.index_data_blocks_used(it.index));
                return null;
            };

            const values = Table.data_block_values_used(block);
            return key_from_value(values[it.value]);
        }

        /// This is only safe to call after peek() has returned non-null.
        pub fn pop(it: *TableIterator) Value {
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