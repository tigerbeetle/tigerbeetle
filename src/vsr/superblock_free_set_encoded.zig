const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;

const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");
const schema = @import("../lsm/schema.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const constants = @import("../constants.zig");
const superblock = @import("./superblock.zig");
const FreeSet = @import("./superblock_free_set.zig").FreeSet;
const BlockType = schema.BlockType;
const FreeSetReference = superblock.FreeSetReference;

/// FreeSetEncoded is the persistent component of the free set. It defines defines the layout of
/// the free set as stored in the grid between checkpoints.
///
/// Free set is stored as a linked list of blocks containing EWAH-encoding of a bitset of acquired
/// blocks. The length of the linked list is proportional to the degree of fragmentation, rather
/// that to  size of the data file. The common case is a single block.
///
/// The blocks holding free set itself are marked as free in the on-disk encoding, because the
/// number of blocks required to store the compressed bitset becomes known only after encoding.
/// This might or might not be related to Russel's paradox.
///
/// Linked list is a FIFO. While the blocks are written in the direct order, they have to be read in
/// the reverse order.
pub fn FreeSetEncodedType(comptime Storage: type) type {
    const Grid = GridType(Storage);

    return struct {
        const Self = @This();

        // Body of the block which holds encoded free set words.
        // All chunks except for possibly the last one are full.
        const chunk_size_max = constants.block_size - @sizeOf(vsr.Header);

        const Chunk = struct {
            start: u32,
            end: u32,
            size: u32,

            fn for_block(options: struct {
                block_index: u32,
                block_count: u32,
                free_set_size: u32,
            }) Chunk {
                assert(options.block_count > 0);
                assert(options.block_count == stdx.div_ceil(options.free_set_size, chunk_size_max));
                assert(options.block_index < options.block_count);

                const last_block = options.block_index == options.block_count - 1;
                const chunk_size = if (last_block)
                    options.free_set_size - (options.block_count - 1) * chunk_size_max
                else
                    chunk_size_max;

                const chunk_start = chunk_size_max * options.block_index;
                const chunk_end = chunk_start + chunk_size;
                assert(chunk_end <= options.free_set_size);
                assert(chunk_size % @sizeOf(FreeSet.Word) == 0);

                return .{ .start = chunk_start, .end = chunk_end, .size = chunk_size };
            }
        };

        // Reference to the grid is late-initialized in the open, because the free set is part of
        // the superblock, which doesn't have access to grid. It is set to null by reset, to verify
        // that the free set is not used before it is opened during sync.
        grid: ?*Grid = null,

        next_tick: Grid.NextTick = undefined,
        read: Grid.Read = undefined,
        write: Grid.Write = undefined,
        // As the free set is expected to fit in one block, it is written sequentialy, one block at
        // a time. This is the memory used for writing.
        write_block: Grid.BlockPtr,

        // SoA representation of block references holding the free set itself.
        //
        // After the set is read from disk and decoded, these blocks are manually marked as
        // acquired.
        block_addresses: []u64,
        block_checksums: []u128,
        block_count: u32 = 0,
        // The current block that is being read or written. It counts from 0 to block_count - 1
        // during checkpoint, and from block_count - 1 to zero during open.
        block_index: u32 = 0,

        // Size of the encoded set in bytes.
        size: u32 = 0,
        // The number of free set bytes read or written during disk IO. Used to cross-check that we
        // haven't lost any bytes along the way.
        size_transferred: u32 = 0,

        // In-memory buffer for storing encoded free set in contagious manner.
        // TODO: instead of copying the data, store a list of grid blocks and implement chunked
        // decoding. That way, the blocks can be shared with grid cache, increasing the usable cache
        // size in the common case of a small free set.
        buffer: []align(@alignOf(FreeSet.Word)) u8,

        callback: union(enum) {
            none,
            open: *const fn (set: *Self) void,
            checkpoint: *const fn (set: *Self) void,
        } = .none,

        comptime {
            assert(FreeSet.Word == schema.FreeSetNode.Word);
            assert(chunk_size_max % @sizeOf(FreeSet.Word) == 0);
        }

        pub fn init(allocator: mem.Allocator, block_count_limit: usize) !Self {
            const write_block = try allocate_block(allocator);
            errdefer allocator.free(write_block);

            const buffer_size = FreeSet.encode_size_max(block_count_limit);
            const buffer = try allocator.alignedAlloc(u8, @alignOf(FreeSet.Word), buffer_size);
            errdefer allocator.free(buffer);

            const block_count_max = stdx.div_ceil(buffer_size, chunk_size_max);
            const block_addresses = try allocator.alloc(u64, block_count_max);
            errdefer allocator.free(block_addresses);

            const block_checksums = try allocator.alloc(u128, block_count_max);
            errdefer allocator.free(block_checksums);

            return .{
                .write_block = write_block,
                .buffer = buffer,
                .block_addresses = block_addresses,
                .block_checksums = block_checksums,
            };
        }

        pub fn deinit(set: *Self, allocator: mem.Allocator) void {
            allocator.free(set.block_checksums);
            allocator.free(set.block_addresses);
            allocator.free(set.buffer);
            allocator.free(set.write_block);
        }

        pub fn reset(set: *Self) void {
            switch (set.callback) {
                .none, .open => {},
                // Checkpointing doesn't need to read blocks, so it's not cancelable.
                .checkpoint => unreachable,
            }
            set.* = .{
                .write_block = set.write_block,
                .buffer = set.buffer,
                .block_addresses = set.block_addresses,
                .block_checksums = set.block_checksums,
            };
        }

        // These data are stored in the superblock header.
        pub fn checkpoint_reference(set: *const Self) FreeSetReference {
            assert(set.size == set.size_transferred);
            assert(set.callback == .none);

            if (set.block_count == 0) {
                assert(set.size == 0);
                return .{
                    .head_address = 0,
                    .head_checksum = 0,
                    .size = 0,
                };
            } else {
                assert(set.size > 0);
                return .{
                    .head_address = set.block_addresses[set.block_count - 1],
                    .head_checksum = set.block_checksums[set.block_count - 1],
                    .size = set.size,
                };
            }
        }

        pub fn open(
            set: *Self,
            grid: *Grid,
            reference: FreeSetReference,
            callback: *const fn (set: *Self) void,
        ) void {
            set.grid = grid;
            assert(!set.grid.?.superblock.free_set.opened);

            assert(set.callback == .none);
            defer assert(set.callback == .open);

            assert(set.size == 0);
            assert(set.size_transferred == 0);
            assert(set.block_count == 0);
            assert(set.block_index == 0);

            set.size = reference.size;
            set.callback = .{ .open = callback };

            set.block_count = stdx.div_ceil(set.size, chunk_size_max);

            if (set.size == 0) {
                assert(reference.head_address == 0);
                set.block_index = 0;
                set.grid.?.on_next_tick(open_next_tick, &set.next_tick);
            } else {
                assert(reference.head_address != 0);
                // Start from the last block, as the linked list arranges data in the reverse order.
                set.block_index = set.block_count - 1;
                set.open_read_next(reference.head_address, reference.head_checksum);
            }
        }

        fn open_next_tick(next_tick: *Grid.NextTick) void {
            const set = @fieldParentPtr(Self, "next_tick", next_tick);
            assert(set.callback == .open);
            assert(set.block_count == 0);
            assert(set.size == 0);
            set.open_done();
        }

        fn open_read_next(set: *Self, address: u64, checksum: u128) void {
            assert(set.callback == .open);
            assert(set.block_index < set.block_count);
            assert(address != 0);
            assert((set.size_transferred == 0) == (set.block_index == set.block_count - 1));

            set.block_addresses[set.block_index] = address;
            set.block_checksums[set.block_index] = checksum;

            set.grid.?.read_block(
                .{ .from_local_or_global_storage = open_read_next_callback },
                &set.read,
                address,
                checksum,
                .{ .cache_read = true, .cache_write = false },
            );
        }

        fn open_read_next_callback(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const set = @fieldParentPtr(Self, "read", read);
            assert(set.callback == .open);
            assert(set.block_index < set.block_count);

            const encoded_words = schema.FreeSetNode.encoded_words(block);
            const chunk = Chunk.for_block(.{
                .block_index = set.block_index,
                .block_count = set.block_count,
                .free_set_size = set.size,
            });

            stdx.copy_disjoint(
                .exact,
                u8,
                set.buffer[chunk.start..chunk.end],
                encoded_words,
            );
            set.size_transferred += chunk.size;

            if (schema.FreeSetNode.next(block)) |next| {
                assert(set.block_index > 0);
                set.block_index -= 1;
                set.open_read_next(next.address, next.checksum);
            } else {
                assert(set.block_index == 0);
                set.open_done();
            }
        }

        fn open_done(set: *Self) void {
            assert(set.callback == .open);
            defer assert(set.callback == .none);

            assert(set.block_index == 0);
            assert(!set.grid.?.superblock.free_set.opened);
            assert(set.size_transferred == set.size);
            assert((set.size > 0) == (set.block_count > 0));

            set.grid.?.superblock.free_set.open(.{
                .encoded = set.buffer[0..set.size],
                .block_addresses = set.block_addresses[0..set.block_count],
            });

            set.grid.?.superblock.free_set.opened = true;
            assert((set.size > 0) == (set.grid.?.superblock.free_set.count_acquired() > 0));

            const callback = set.callback.open;
            set.callback = .none;
            callback(set);
        }

        pub fn checkpoint(set: *Self, callback: *const fn (set: *Self) void) void {
            assert(set.callback == .none);
            defer assert(set.callback == .checkpoint);

            // Checkpoint process is delicate:
            //   - encode free set,
            //   - derive the number of blocks required to store the encoding,
            //   - allocate blocks for the encoding (in the old checkpoint),
            //   - checkpoint the free set,
            //   - release the freshly acquired blocks in the new checkpoint.
            {
                set.grid.?.superblock.free_set.include_staging();
                defer set.grid.?.superblock.free_set.exclude_staging();

                set.size = @as(u32, @intCast(set.grid.?.superblock.free_set.encode(set.buffer)));
                set.size_transferred = 0;
                set.block_count = stdx.div_ceil(set.size, chunk_size_max);
            }

            assert(set.grid.?.superblock.free_set.count_reservations() == 0);
            const reservation = set.grid.?.superblock.free_set.reserve(set.block_count).?;
            for (0..set.block_count) |index| {
                const address = set.grid.?.superblock.free_set.acquire(reservation).?;
                set.block_addresses[index] = address;
                set.block_checksums[index] = undefined;
            }
            set.grid.?.superblock.free_set.forfeit(reservation);

            set.grid.?.superblock.free_set.checkpoint();
            for (0..set.block_count) |index| {
                const address = set.block_addresses[index];
                set.grid.?.superblock.free_set.release(address);
            }

            set.block_index = 0;
            set.callback = .{ .checkpoint = callback };

            if (set.size == 0) {
                assert(set.block_count == 0);
                set.grid.?.on_next_tick(checkpoint_next_tick, &set.next_tick);
            } else {
                assert(set.block_count > 0);
                set.checkpoint_write_next();
            }
        }

        fn checkpoint_next_tick(next_tick: *Grid.NextTick) void {
            const set = @fieldParentPtr(Self, "next_tick", next_tick);
            assert(set.callback == .checkpoint);
            assert(set.block_count == 0);
            assert(set.size == 0);
            set.checkpoint_done();
        }

        fn checkpoint_write_next(set: *Self) void {
            assert(set.callback == .checkpoint);
            assert(set.size > 0);
            assert(set.block_index < set.block_count);
            assert((set.size_transferred == 0) == (set.block_index == 0));

            const chunk = Chunk.for_block(.{
                .block_index = set.block_index,
                .block_count = set.block_count,
                .free_set_size = set.size,
            });

            const header = mem.bytesAsValue(
                vsr.Header.Block,
                set.write_block[0..@sizeOf(vsr.Header)],
            );
            header.* = .{
                .cluster = set.grid.?.superblock.working.cluster,
                .metadata_bytes = @bitCast(if (set.block_index == 0)
                    schema.FreeSetNode.Metadata{
                        .next_free_set_block_checksum = 0,
                        .next_free_set_block_address = 0,
                    }
                else
                    schema.FreeSetNode.Metadata{
                        .next_free_set_block_checksum = set.block_checksums[set.block_index - 1],
                        .next_free_set_block_address = set.block_addresses[set.block_index - 1],
                    }),
                .address = set.block_addresses[set.block_index],
                .snapshot = 0, // TODO(snapshots): Set this properly; it is useful for debugging.
                .size = @sizeOf(vsr.Header) + chunk.size,
                .command = .block,
                .block_type = .free_set,
            };
            stdx.copy_disjoint(
                .exact,
                u8,
                set.write_block[@sizeOf(vsr.Header)..][0..chunk.size],
                set.buffer[chunk.start..chunk.end],
            );
            set.size_transferred += chunk.size;
            header.set_checksum_body(set.write_block[@sizeOf(vsr.Header)..][0..chunk.size]);
            header.set_checksum();
            schema.FreeSetNode.assert_valid_header(set.write_block);

            set.block_checksums[set.block_index] = header.checksum;
            set.grid.?.create_block(
                checkpoint_write_next_callback,
                &set.write,
                &set.write_block,
            );
        }

        fn checkpoint_write_next_callback(write: *Grid.Write) void {
            const set = @fieldParentPtr(Self, "write", write);
            assert(set.callback == .checkpoint);

            if (set.block_index == set.block_count - 1) {
                set.checkpoint_done();
            } else {
                set.block_index += 1;
                set.checkpoint_write_next();
            }
        }

        fn checkpoint_done(set: *Self) void {
            assert(set.callback == .checkpoint);
            defer assert(set.callback == .none);

            if (set.block_count == 0) {
                assert(set.size == 0);
                assert(set.block_index == 0);
            } else {
                assert(set.block_index == set.block_count - 1);
            }

            assert(set.size_transferred == set.size);

            const callback = set.callback.checkpoint;
            set.callback = .none;
            callback(set);
        }
    };
}
