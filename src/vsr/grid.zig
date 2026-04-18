const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const schema = @import("../lsm/schema.zig");

const SuperBlockType = vsr.SuperBlockType;
const QueueType = @import("../queue.zig").QueueType;
const IOPSType = stdx.IOPSType;
const SetAssociativeCacheType = @import("../lsm/set_associative_cache.zig").SetAssociativeCacheType;
const stdx = @import("stdx");
const GridBlocksMissing = @import("./grid_blocks_missing.zig").GridBlocksMissing;
const Tracer = vsr.trace.Tracer;

const FreeSet = @import("./free_set.zig").FreeSet;

const log = stdx.log.scoped(.grid);

pub const BlockPtr = *align(constants.sector_size) [constants.block_size]u8;
pub const BlockPtrConst = *align(constants.sector_size) const [constants.block_size]u8;

// FIXME stash the checksum of a block on write_block(), make sure it doesn't get overwritten

/// The Grid provides access to on-disk blocks (blobs of `block_size` bytes).
/// Each block is identified by an "address" (`u64`, beginning at 1).
///
/// Recently/frequently-used blocks are transparently cached in memory.
pub fn GridType(comptime Storage: type) type {
    const block_size = constants.block_size;
    const SuperBlock = SuperBlockType(Storage);

    return struct {
        const Grid = @This();
        const CheckpointTrailer = vsr.CheckpointTrailerType(Storage);

        pub const read_iops_max = constants.grid_iops_read_max;
        pub const write_iops_max = constants.grid_iops_write_max;

        pub const RepairTable = GridBlocksMissing.RepairTable;
        pub const RepairTableResult = GridBlocksMissing.RepairTableResult;
        pub const Reservation = @import("./free_set.zig").Reservation;

        // Grid just reuses the Storage's NextTick abstraction for simplicity.
        pub const NextTick = Storage.NextTick;

        pub const Write = struct {
            callback: *const fn (*Grid.Write) void,
            address: u64,
            repair: bool,
            block: *BlockPtr,
            /// The current checkpoint when the write began.
            /// Verifies that the checkpoint does not advance during the (non-repair) write.
            checkpoint_id: u128,

            /// Link for the Grid.write_queue linked list.
            link: QueueType(Write).Link = .{},
        };

        const WriteIOP = struct {
            grid: *Grid,
            completion: Storage.Write,
            write: *Write,
        };

        const ReadBlockCallback = union(enum) {
            /// If the local read fails, report the error.
            from_local_storage: *const fn (*Grid.Read, ReadBlockResult) void,
            /// If the local read fails, this read will be added to a linked list, which Replica can
            /// then interrogate each tick(). The callback passed to this function won't be called
            /// until the block has been recovered.
            from_local_or_global_storage: *const fn (*Grid.Read, BlockPtrConst) void,
        };

        pub const Read = struct {
            callback: ReadBlockCallback,
            address: u64,
            checksum: u128,
            /// The current checkpoint when the read began.
            /// Used to verify that the checkpoint does not advance while the read is in progress.
            checkpoint_id: u128,
            checkpoint_durable: bool,

            /// When coherent=true:
            /// - the block (address+checksum) is part of the current checkpoint.
            /// - the read will complete before the next checkpoint occurs.
            /// - callback == .from_local_or_global_storage
            /// When coherent=false:
            /// - the block (address+checksum) is not necessarily part of the current checkpoint.
            /// - the read may complete after a future checkpoint.
            /// - callback == .from_local_storage
            coherent: bool,
            cache_read: bool,
            cache_write: bool,
            pending: ReadPending = .{},
            resolves: QueueType(ReadPending) = QueueType(ReadPending).init(.{ .name = null }),

            grid: *Grid,
            next_tick: Grid.NextTick = undefined,

            /// Link for Grid.read_queue/Grid.read_global_queue linked lists.
            link: QueueType(Read).Link = .{},
        };

        /// Although we distinguish between the reasons why the block is invalid, we only use this
        /// info for logging, not logic.
        pub const ReadBlockResult = union(enum) {
            valid: BlockPtrConst,
            /// Checksum of block header is invalid.
            invalid_checksum,
            /// Checksum of block body is invalid.
            invalid_checksum_body,
            /// The block header is valid, but its `header.command` is not `block`.
            /// (This is possible due to misdirected IO).
            unexpected_command,
            /// The block is valid, but it is not the block we expected.
            unexpected_checksum,
            /// The block is valid, and it is the block we expected, but the last sector's padding
            /// is corrupt, so we will repair it just to be safe.
            invalid_padding,
        };

        const ReadPending = struct {
            /// Link for Read.resolves linked lists.
            link: QueueType(ReadPending).Link = .{},
        };

        const ReadIOP = struct {
            completion: Storage.Read,
            read: *Read,
            location: u32,
        };

        const cache_interface = struct {
            inline fn address_from_address(address: *const u64) u64 {
                return address.*;
            }

            inline fn hash_address(address: u64) u64 {
                assert(address > 0);
                return stdx.hash_inline(address);
            }
        };

        const set_associative_cache_ways = 16;

        pub const Cache = SetAssociativeCacheType(
            u64,
            u64,
            cache_interface.address_from_address,
            cache_interface.hash_address,
            .{
                .ways = set_associative_cache_ways,
                // layout.cache_line_size isn't actually used to compute anything. Rather, it's
                // used by the SetAssociativeCache to assert() on sub-optimal values. In this case,
                // it's better to allow the user to be able to run with a much smaller grid cache
                // (256MiB vs 1GiB!) than trying to be completely optimal.
                .cache_line_size = 16,
                .value_alignment = @alignOf(u64),
            },
        );

        superblock: *SuperBlock,
        trace: *Tracer,
        free_set: FreeSet,
        free_set_checkpoint_blocks_acquired: CheckpointTrailer,
        free_set_checkpoint_blocks_released: CheckpointTrailer,

        /// Entries in `cache` map to `cache_blocks`/`cache_references` via the cache's
        /// `Value.offset`. FIXME
        blocks: []align(constants.sector_size) [constants.block_size]u8,
        blocks_references: []u8,
        blocks_missing: GridBlocksMissing,

        cache: Cache,
        /// The block at `cache[x]` is found at `blocks[cache_locations[x]]`.
        /// This indirection is necessary because the SetAssociativeCache is not aware of references
        /// held by e.g. scans, so it may evict a block we still need.
        ///
        /// Locations at indexes beyond `cache_blocks_count` correspond to stash entries.
        ///
        /// Invariants:
        /// - `cache_locations` is a permutation of the sequence from `0` to
        ///   `cache_locations.len - 1` (inclusive).
        /// FIXME Or maybe have a separate stash boundedarray (or two) for non-cache locations?
        /// Though, it is nice that it corresponds to `blocks` so maybe not.
        cache_locations: []u32,
        cache_blocks_count: u64,

        write_iops: IOPSType(WriteIOP, write_iops_max) = .{},
        write_queue: QueueType(Write) = QueueType(Write).init(.{ .name = "grid_write" }),

        read_iops: IOPSType(ReadIOP, read_iops_max) = .{},
        read_queue: QueueType(Read) = QueueType(Read).init(.{ .name = "grid_read" }),

        // List of Read.pending's which are in `read_queue` but also waiting for a free `read_iops`.
        read_pending_queue: QueueType(ReadPending) = QueueType(ReadPending).init(.{
            .name = "grid_read_pending",
        }),
        /// List of `Read`s which are waiting for a block repair from another replica.
        /// (Reads in this queue have already failed locally).
        ///
        /// Invariants:
        /// - For each read, read.callback=from_local_or_global_storage.
        read_global_queue: QueueType(Read) = QueueType(Read).init(.{ .name = "grid_read_global" }),
        // True if there's a read that is resolving callbacks.
        // If so, the read cache must not be invalidated.
        read_resolving: bool = false,

        callback: union(enum) {
            none,
            open: *const fn (*Grid) void,
            checkpoint: *const fn (*Grid) void,
            checkpoint_durable: *const fn (*Grid) void,
            cancel: *const fn (*Grid) void,
        } = .none,

        canceling_tick_context: NextTick = undefined,

        pub fn init(allocator: mem.Allocator, options: struct {
            superblock: *SuperBlock,
            trace: *Tracer,
            cache_blocks_count: u64 = Cache.value_count_max_multiple,
            stash_blocks_count: u64,
            missing_blocks_max: usize,
            missing_tables_max: usize,
            blocks_released_prior_checkpoint_durability_max: usize,
        }) !Grid {
            var free_set = try FreeSet.init(allocator, .{
                .grid_size_limit = options.superblock.grid_size_limit(),
                .blocks_released_prior_checkpoint_durability_max = options
                    .blocks_released_prior_checkpoint_durability_max,
            });
            errdefer free_set.deinit(allocator);

            const free_set_encoded_size_max = free_set.encode_size_max();
            var free_set_checkpoint_blocks_acquired =
                try CheckpointTrailer.init(allocator, .free_set, free_set_encoded_size_max);
            errdefer free_set_checkpoint_blocks_acquired.deinit(allocator);

            var free_set_checkpoint_blocks_released =
                try CheckpointTrailer.init(allocator, .free_set, free_set_encoded_size_max);
            errdefer free_set_checkpoint_blocks_released.deinit(allocator);

            const blocks_count = options.cache_blocks_count + options.stash_blocks_count;
            const blocks = try allocator.alignedAlloc(
                [constants.block_size]u8,
                constants.sector_size,
                blocks_count,
            );
            errdefer allocator.free(blocks);

            const blocks_references = try allocator.alloc(u8, blocks_count);
            errdefer allocator.free(blocks_references);
            @memset(blocks_references, 0);

            var blocks_missing = try GridBlocksMissing.init(allocator, .{
                .blocks_max = options.missing_blocks_max,
                .tables_max = options.missing_tables_max,
            });
            errdefer blocks_missing.deinit(allocator);

            var cache = try Cache.init(allocator, options.cache_blocks_count, .{ .name = "grid" });
            errdefer cache.deinit(allocator);

            const cache_locations = try allocator.alloc(u32, blocks_count);
            errdefer allocator.free(cache_locations);
            for (cache_locations, 0..) |*location, i| location.* = @intCast(i);

            return Grid{
                .superblock = options.superblock,
                .trace = options.trace,
                .free_set = free_set,
                .free_set_checkpoint_blocks_acquired = free_set_checkpoint_blocks_acquired,
                .free_set_checkpoint_blocks_released = free_set_checkpoint_blocks_released,
                .blocks = blocks,
                .blocks_references = blocks_references,
                .blocks_missing = blocks_missing,
                .cache = cache,
                .cache_locations = cache_locations,
                .cache_blocks_count = options.cache_blocks_count,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            grid.free_set_checkpoint_blocks_acquired.deinit(allocator);
            grid.free_set_checkpoint_blocks_released.deinit(allocator);

            grid.blocks_missing.deinit(allocator);
            allocator.free(grid.blocks_references);
            allocator.free(grid.blocks);

            allocator.free(grid.cache_locations);
            grid.cache.deinit(allocator);
            grid.free_set.deinit(allocator);

            grid.* = undefined;
        }

        pub fn open(grid: *Grid, callback: *const fn (*Grid) void) void {
            assert(grid.callback == .none);

            grid.callback = .{ .open = callback };
            grid.free_set_checkpoint_blocks_acquired.open(
                grid,
                grid.superblock.working.free_set_reference(.blocks_acquired),
                open_free_set_callback_blocks_acquired,
            );
            grid.free_set_checkpoint_blocks_released.open(
                grid,
                grid.superblock.working.free_set_reference(.blocks_released),
                open_free_set_callback_blocks_released,
            );
        }

        fn open_free_set_callback_blocks_acquired(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_acquired", trailer);
            grid.open_free_set_callback();
        }

        fn open_free_set_callback_blocks_released(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_released", trailer);
            grid.open_free_set_callback();
        }

        fn open_free_set_callback(grid: *Grid) void {
            assert(grid.free_set_checkpoint_blocks_acquired.callback == .none or
                grid.free_set_checkpoint_blocks_released.callback == .none);

            const callback = grid.callback.open;
            // May still be reading the CheckpointTrailer for `blocks_acquired`.
            if (grid.free_set_checkpoint_blocks_acquired.callback == .open) return;
            assert(grid.free_set_checkpoint_blocks_acquired.callback == .none);

            // May still be reading the CheckpointTrailer for `blocks_released`.
            if (grid.free_set_checkpoint_blocks_released.callback == .open) return;
            assert(grid.free_set_checkpoint_blocks_released.callback == .none);

            {
                assert(!grid.free_set.opened);
                defer assert(grid.free_set.opened);

                const block_count_encoded_blocks_acquired =
                    grid.free_set_checkpoint_blocks_acquired.block_count();
                const block_count_encoded_blocks_released =
                    grid.free_set_checkpoint_blocks_released.block_count();
                grid.free_set.open(.{
                    .encoded = .{
                        .blocks_acquired = grid.free_set_checkpoint_blocks_acquired.decode_chunks(),
                        .blocks_released = grid.free_set_checkpoint_blocks_released.decode_chunks(),
                    },
                    .free_set_block_addresses = .{
                        .blocks_acquired = grid.free_set_checkpoint_blocks_acquired
                            .block_addresses[0..block_count_encoded_blocks_acquired],
                        .blocks_released = grid.free_set_checkpoint_blocks_released
                            .block_addresses[0..block_count_encoded_blocks_released],
                    },
                });
                assert((grid.free_set.count_acquired() > 0) ==
                    (grid.free_set_checkpoint_blocks_acquired.size > 0));

                // Assert that the highest acquired address is compatible with storage_size.
                const storage_size: u64 = storage_size: {
                    var storage_size = vsr.superblock.data_file_size_min;
                    if (grid.free_set.highest_address_acquired()) |address| {
                        assert(address > 0);
                        assert(grid.free_set_checkpoint_blocks_acquired.size > 0);
                        maybe(grid.free_set_checkpoint_blocks_released.size == 0);

                        storage_size += address * constants.block_size;
                    } else {
                        assert(grid.free_set_checkpoint_blocks_acquired.size == 0);
                        assert(grid.free_set_checkpoint_blocks_released.size == 0);

                        assert(grid.free_set.count_released() == 0);
                    }
                    break :storage_size storage_size;
                };
                assert(storage_size == grid.superblock.working.vsr_state.checkpoint.storage_size);

                assert(grid.free_set.count_released() >=
                    (grid.free_set_checkpoint_blocks_acquired.block_count() +
                        grid.free_set_checkpoint_blocks_released.block_count()));

                assert(grid.free_set.count_reservations() == 0);
            }
            grid.callback = .none;
            callback(grid);
        }

        /// Checkpoint process is delicate:
        ///   1. Encode free set.
        ///   2. Derive the number of blocks required to store the encoding.
        ///   3. Allocate free set blocks for the encoding (in the old checkpoint).
        ///   4. Write the free set blocks to disk.
        ///   5. Mark the free set's own blocks as released (but not yet free).
        ///
        /// This function handles step 1, and calls CheckpointTrailer.checkpoint, which handles 2-4.
        /// The caller is responsible for calling Grid.mark_checkpoint_not_durable, which handles 5.
        pub fn checkpoint(grid: *Grid, callback: *const fn (*Grid) void) void {
            assert(grid.callback == .none);
            assert(grid.read_global_queue.empty());

            {
                assert(grid.free_set.count_reservations() == 0);

                const free_set_encoded = grid.free_set.encode_chunks(
                    grid.free_set_checkpoint_blocks_acquired.encode_chunks(),
                    grid.free_set_checkpoint_blocks_released.encode_chunks(),
                );

                grid.free_set_checkpoint_blocks_acquired.size =
                    free_set_encoded.encoded_size_blocks_acquired;
                grid.free_set_checkpoint_blocks_released.size =
                    free_set_encoded.encoded_size_blocks_released;

                assert(grid.free_set_checkpoint_blocks_acquired.size % @sizeOf(FreeSet.Word) == 0);
                assert(grid.free_set_checkpoint_blocks_released.size % @sizeOf(FreeSet.Word) == 0);
            }

            grid.callback = .{ .checkpoint = callback };
            grid.free_set_checkpoint_blocks_acquired
                .checkpoint(checkpoint_free_set_blocks_acquired_callback);
            grid.free_set_checkpoint_blocks_released
                .checkpoint(checkpoint_free_set_blocks_released_callback);
        }

        fn checkpoint_free_set_blocks_acquired_callback(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_acquired", trailer);
            assert(grid.callback == .checkpoint);

            grid.checkpoint_join();
        }

        fn checkpoint_free_set_blocks_released_callback(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_released", trailer);
            assert(grid.callback == .checkpoint);

            grid.checkpoint_join();
        }

        fn checkpoint_join(grid: *Grid) void {
            assert(grid.callback == .checkpoint);
            assert(grid.read_global_queue.empty());

            if (grid.free_set_checkpoint_blocks_acquired.callback == .checkpoint) {
                return; // Still writing free set `blocks_acquired` bitset.
            }
            assert(grid.free_set_checkpoint_blocks_acquired.callback == .none);

            if (grid.free_set_checkpoint_blocks_released.callback == .checkpoint) {
                return; // Still writing free set `blocks_released` bitset.
            }
            assert(grid.free_set_checkpoint_blocks_released.callback == .none);

            const callback = grid.callback.checkpoint;
            grid.callback = .none;
            callback(grid);
        }

        /// Mark the current checkpoint as not durable, then release the blocks acquired for the
        /// FreeSet checkpoints (to be freed when the *next* checkpoint becomes durable).
        ///
        /// The ordering is important here, if we were to release these blocks before the checkpoint
        /// is marked as not durable, they would erroneously be freed when the *current* checkpoint
        /// becomes durable.
        pub fn mark_checkpoint_not_durable(grid: *Grid) void {
            assert(grid.free_set.checkpoint_durable);
            defer assert(!grid.free_set.checkpoint_durable);

            grid.free_set.mark_checkpoint_not_durable();
            grid.release(grid.free_set_checkpoint_blocks_acquired
                .block_addresses[0..grid.free_set_checkpoint_blocks_acquired.block_count()]);
            grid.release(grid.free_set_checkpoint_blocks_released
                .block_addresses[0..grid.free_set_checkpoint_blocks_released.block_count()]);
        }

        /// Now that the checkpoint is durable on a commit quorum of replicas:
        /// 1. Await all pending repair-writes to blocks that are about to be freed.
        /// 2. Mark currently released blocks as free and eligible for acquisition.
        ///
        /// This function handles step 1.
        /// The caller is responsible for calling FreeSet.checkpoint which handles 2.
        pub fn checkpoint_durable(grid: *Grid, callback: *const fn (*Grid) void) void {
            assert(!grid.free_set.checkpoint_durable);
            grid.callback = .{ .checkpoint_durable = callback };

            grid.blocks_missing.checkpoint_durable_commence(&grid.free_set);
            if (grid.blocks_missing.state.checkpoint_durable.aborting == 0) {
                grid.checkpoint_durable_join();
            }
        }

        fn checkpoint_durable_join(grid: *Grid) void {
            assert(grid.callback == .checkpoint_durable);

            // We are still repairing some blocks released during the previous checkpoint interval.
            if (!grid.blocks_missing.checkpoint_durable_complete()) {
                assert(grid.write_iops.executing() > 0);
                return;
            }

            var write_queue_iterator = grid.write_queue.iterate();
            while (write_queue_iterator.next()) |write| {
                maybe(write.repair);
                assert(!grid.free_set.is_free(write.address));
                assert(!grid.free_set.to_be_freed_at_checkpoint_durability(write.address));
            }

            var write_iops_iterator = grid.write_iops.iterate();
            while (write_iops_iterator.next()) |iop| {
                assert(!grid.free_set.is_free(iop.write.address));
                assert(!grid.free_set.to_be_freed_at_checkpoint_durability(iop.write.address));
            }

            // Now that there are no writes to released blocks, we can safely mark them as free,
            // and also mark the checkpoint as durable.
            assert(!grid.free_set.checkpoint_durable);
            defer assert(grid.free_set.checkpoint_durable);

            grid.free_set.mark_checkpoint_durable();

            const callback = grid.callback.checkpoint_durable;
            grid.callback = .none;
            callback(grid);
        }

        pub fn cancel(grid: *Grid, callback: *const fn (*Grid) void) void {
            // grid.open() is cancellable the same way that read_block()/write_block() are.
            switch (grid.callback) {
                .none => {},
                .open => {},
                .checkpoint_durable => {},
                .checkpoint => unreachable,
                .cancel => unreachable,
            }

            grid.callback = .{ .cancel = callback };

            grid.blocks_missing.cancel();
            grid.read_queue.reset();
            grid.read_pending_queue.reset();
            grid.read_global_queue.reset();
            grid.write_queue.reset();
            grid.superblock.storage.reset_next_tick_lsm();
            grid.superblock.storage.on_next_tick(
                .vsr,
                cancel_tick_callback,
                &grid.canceling_tick_context,
            );
        }

        fn cancel_tick_callback(next_tick: *NextTick) void {
            const grid: *Grid = @alignCast(@fieldParentPtr("canceling_tick_context", next_tick));
            if (grid.callback != .cancel) return;

            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_global_queue.empty());
            assert(grid.write_queue.empty());

            grid.cancel_join_callback();
        }

        fn cancel_join_callback(grid: *Grid) void {
            assert(grid.callback == .cancel);
            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_global_queue.empty());
            assert(grid.write_queue.empty());

            if (grid.read_iops.executing() == 0 and
                grid.write_iops.executing() == 0)
            {
                const callback = grid.callback.cancel;
                grid.callback = .none;

                callback(grid);
            }
        }

        pub fn on_next_tick(
            grid: *Grid,
            callback: *const fn (*Grid.NextTick) void,
            next_tick: *Grid.NextTick,
        ) void {
            assert(grid.callback != .cancel);
            grid.superblock.storage.on_next_tick(.lsm, callback, next_tick);
        }

        /// Aborts if there are not enough free blocks to fill the reservation.
        /// Should a use case arise where a null return would be preferred, this can be split
        /// into panicking and non-panicking versions.
        pub fn reserve(grid: *Grid, blocks_count: usize) Reservation {
            assert(grid.callback == .none);
            return grid.free_set.reserve(blocks_count) orelse vsr.fatal(
                .storage_size_would_exceed_limit,
                "data file would become too large size={} + reservation={} > limit={}, " ++
                    "restart the replica increasing '--limit-storage'",
                .{
                    grid.superblock.working.vsr_state.checkpoint.storage_size,
                    blocks_count * constants.block_size,
                    grid.superblock.storage_size_limit,
                },
            );
        }

        /// Forfeit a reservation.
        pub fn forfeit(grid: *Grid, reservation: Reservation) void {
            assert(grid.callback == .none);
            return grid.free_set.forfeit(reservation);
        }

        /// Returns a just-allocated block.
        /// The caller is responsible for not acquiring more blocks than they reserved.
        pub fn acquire(grid: *Grid, reservation: Reservation) u64 {
            assert(grid.callback == .none);
            return grid.free_set.acquire(reservation).?;
        }

        /// This function should be used to release addresses, instead of release()
        /// on the free set directly, as this also demotes the address within the block cache.
        /// This reduces conflict misses in the block cache, by freeing ways soon after they are
        /// released.
        ///
        /// This does not remove the blocks from the cache — the blocks can be read until the next
        /// checkpoint.
        ///
        /// Asserts that the addresses are not currently being read from or written to.
        pub fn release(grid: *Grid, addresses: []const u64) void {
            assert(grid.callback == .none);
            for (addresses) |address| {
                assert(address > 0);

                // It's safe to release an address that is being read from or
                // written to, as it can only be overwritten in the next
                // checkpoint (when the address is freed and can be reacquired).
                maybe(grid.writing(address, null) == .create);

                grid.cache.demote(address);
                grid.free_set.release(address);
            }
        }

        const Writing = enum { create, repair, not_writing };

        /// If the address is being written to by a non-repair, return `.create`.
        /// If the address is being written to by a repair, return `.repair`.
        /// Otherwise return `.not_writing`.
        ///
        /// Assert that the block pointer is not being used for any write if non-null.
        pub fn writing(grid: *Grid, address: u64, block: ?BlockPtrConst) Writing {
            assert(address > 0);

            var result = Writing.not_writing;
            {
                var it = grid.write_queue.iterate();
                while (it.next()) |queued_write| {
                    assert(block != queued_write.block.*);
                    if (address == queued_write.address) {
                        assert(result == .not_writing);
                        result = if (queued_write.repair) .repair else .create;
                    }
                }
            }
            {
                var it = grid.write_iops.iterate();
                while (it.next()) |iop| {
                    assert(block != iop.write.block.*);
                    if (address == iop.write.address) {
                        assert(result == .not_writing);
                        result = if (iop.write.repair) .repair else .create;
                    }
                }
            }
            return result;
        }

        /// Assert that the address is not currently being read from (disregarding repairs).
        /// Assert that the block pointer is not being used for any read if non-null.
        fn assert_not_reading(grid: *Grid, address: u64, block: ?BlockPtrConst) void {
            assert(address > 0);

            for ([_]*const QueueType(Read){
                &grid.read_queue,
                &grid.read_global_queue,
            }) |queue| {
                var it = queue.iterate();
                while (it.next()) |queued_read| {
                    if (queued_read.coherent) {
                        assert(address != queued_read.address);
                    }
                }
            }
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    if (iop.read.coherent) {
                        assert(address != iop.read.address);
                    }

                    const iop_block = &grid.blocks[iop.location];
                    assert(block != iop_block);
                }
            }
        }

        pub fn assert_only_repairing(grid: *Grid) void {
            assert(grid.callback != .cancel);
            assert(grid.read_global_queue.empty());

            var read_queue_iterator = grid.read_queue.iterate();
            while (read_queue_iterator.next()) |read| {
                // Scrubber reads are independent from LSM operations.
                assert(!read.coherent);
            }

            var write_queue_iterator = grid.write_queue.iterate();
            while (write_queue_iterator.next()) |write| {
                assert(write.repair);
                assert(!grid.free_set.is_free(write.address));
            }

            var write_iops = grid.write_iops.iterate();
            while (write_iops.next()) |iop| {
                assert(iop.write.repair);
                assert(!grid.free_set.is_free(iop.write.address));
            }
        }

        /// Return a block from the stash which had no outstanding references.
        pub fn get_block(grid: *Grid) BlockPtr {
            assert(grid.superblock.opened);

            for (grid.cache_locations[grid.cache_blocks_count..]) |location| {
                // FIXME Or use a linked list of locations?
                if (grid.blocks_references[location] == 0) {
                    grid.blocks_references[location] += 1;

                    // We could overwrite all the block's data, but that would be more expensive.
                    const block = &grid.blocks[location];
                    @memset(block[0..@sizeOf(vsr.Header)], 0);
                    return block;
                }
            } else @panic("stash has no free blocks");
        }

        pub fn block_ref(grid: *Grid, block: BlockPtr) BlockPtr {
            const block_header = schema.header_from_block(block);
            assert(block_header.valid_checksum());

            const location = grid.location_from_block(block);
            maybe(grid.blocks_references[location] == 0); // FIXME explain

            // block_ref() is called by read_block callbacks, so the block is definitely in cache.
            const cache_index = grid.cache.get_index(block_header.address);
            if (cache_index != null and
                grid.cache_locations[cache_index.?] == location)
            {
                // Typical case.
            } else {
                // The caller invoked block_ref() on a block that is not in our cache.
                // Either:
                // - The caller read the block from our write queue. (It will be in the cache soon.)
                // - The caller read the block via fulfill_block().
            }

            grid.blocks_references[location] += 1;
            return block;
        }

        pub fn block_references(grid: *Grid, block: BlockPtrConst) u8 {
            return grid.blocks_references[grid.location_from_block(block)];
        }

        // FIXME return a zero-refrence block? won't work in 'fn init() { defer }' though
        pub fn block_unref(grid: *Grid, block: BlockPtrConst) void {
            const location = grid.location_from_block(block);
            assert(grid.blocks_references[location] > 0);
            grid.blocks_references[location] -= 1;
        }

        pub fn fulfill_block(grid: *Grid, block: BlockPtrConst) bool {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);

            const block_header = schema.header_from_block(block);
            assert(block_header.cluster == grid.superblock.working.cluster);

            var reads_iterator = grid.read_global_queue.iterate();
            while (reads_iterator.next()) |read| {
                if (read.checksum == block_header.checksum and
                    read.address == block_header.address)
                {
                    assert(block_header.release.value <=
                        grid.superblock.working.vsr_state.checkpoint.release.value);
                    grid.read_global_queue.remove(read);
                    grid.read_block_resolve(read, .{ .valid = block });
                    return true;
                }
            }
            return false;
        }

        pub fn repair_block_waiting(grid: *Grid, address: u64, checksum: u128) bool {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            return grid.blocks_missing.block_waiting(address, checksum);
        }

        /// Write a block that should already exist but (maybe) doesn't because of:
        /// - a disk fault, or
        /// - the block was missed due to state sync.
        ///
        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn repair_block(
            grid: *Grid,
            callback: *const fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
        ) void {
            const block_header = schema.header_from_block(block.*);
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(grid.writing(block_header.address, block.*) == .not_writing);
            assert(grid.blocks_missing.block_waiting(block_header.address, block_header.checksum));
            assert(!grid.free_set.is_free(block_header.address));

            grid.blocks_missing.write_commence(block_header.address, block_header.checksum);
            grid.write_block(callback, write, block, .repair);
        }

        /// Write a block for the first time.
        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn create_block(
            grid: *Grid,
            callback: *const fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
        ) void {
            const block_header = schema.header_from_block(block.*);
            assert(grid.superblock.opened);
            assert(grid.callback == .none or grid.callback == .checkpoint);
            assert((grid.callback == .checkpoint) == (block_header.block_type == .free_set));
            assert(grid.writing(block_header.address, block.*) == .not_writing);
            assert(!grid.blocks_missing.block_waiting(
                block_header.address,
                block_header.checksum,
            ));
            assert(!grid.free_set.is_free(block_header.address));
            grid.assert_not_reading(block_header.address, block.*);

            grid.write_block(callback, write, block, .create);
        }

        /// NOTE: This will consume `block` and replace it with a fresh block.
        fn write_block(
            grid: *Grid,
            callback: *const fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
            trigger: enum { create, repair },
        ) void {
            const header = schema.header_from_block(block.*);
            assert(header.cluster == grid.superblock.working.cluster);
            assert(header.release.value <=
                grid.superblock.working.vsr_state.checkpoint.release.value);

            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(grid.writing(header.address, block.*) == .not_writing);
            assert(!grid.free_set.is_free(header.address));
            grid.assert_coherent(header.address, header.checksum);

            assert(grid.blocks_references[grid.location_from_block(block.*)] > 0);
            if (grid.blocks_references[grid.location_from_block(block.*)] > 1) {
                // Extra references are due to fulfill_block().
                assert(trigger == .repair);
            }

            if (constants.verify) {
                // FIXME positive-check stash instead, since it is much smaller
                for (grid.cache_locations[0..grid.cache_blocks_count]) |location| {
                    assert(&grid.blocks[location] != block.*);
                }
            }

            // Zero sector padding.
            @memset(block.*[header.size..vsr.sector_ceil(header.size)], 0);

            write.* = .{
                .callback = callback,
                .address = header.address,
                .repair = trigger == .repair,
                .block = block,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
            };

            const iop = grid.write_iops.acquire() orelse {
                grid.write_queue.push(write);
                return;
            };

            grid.write_block_with(iop, write);
        }

        fn write_block_with(grid: *Grid, iop: *WriteIOP, write: *Write) void {
            assert(!grid.free_set.is_free(write.address));

            grid.trace.start(.{ .grid_write = .{ .iop = grid.write_iops.index(iop) } });

            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .write = write,
            };

            const write_header = schema.header_from_block(write.block.*);
            assert(write_header.size > @sizeOf(vsr.Header));
            assert(write_header.size <= constants.block_size);
            assert(stdx.zeroed(
                write.block.*[write_header.size..vsr.sector_ceil(write_header.size)],
            ));

            grid.superblock.storage.write_sectors(
                write_block_callback,
                &iop.completion,
                write.block.*[0..vsr.sector_ceil(write_header.size)],
                .grid,
                block_offset(write.address),
            );
        }

        fn write_block_callback(completion: *Storage.Write) void {
            const iop: *WriteIOP = @fieldParentPtr("completion", completion);

            // We must copy these values to the stack as they will be overwritten
            // when we release the iop and potentially start a queued write.
            const grid = iop.grid;
            const completed_write = iop.write;

            // We can only update the cache if the Grid is not resolving callbacks with a cache
            // block.
            assert(!grid.read_resolving);
            assert(!grid.free_set.is_free(completed_write.address));

            if (!completed_write.repair) {
                assert(grid.superblock.working.checkpoint_id() == completed_write.checkpoint_id);
            }

            // Insert the write block into the cache.
            // FIXME insert into cache _before_ writing?
            const block_written_location = grid.location_from_block(completed_write.block.*);
            grid.cache_upsert(.{
                .address = completed_write.address,
                .location = block_written_location,
            });

            // Usually references=1, but since reading from the write queue is possible, it may be
            // higher.
            assert(grid.blocks_references[block_written_location] > 0);
            grid.blocks_references[block_written_location] -= 1;

            const cache_block = &grid.blocks[block_written_location];
            completed_write.block.* = grid.get_block();

            const cache_block_header = schema.header_from_block(cache_block);
            assert(cache_block_header.address == completed_write.address);
            grid.assert_coherent(completed_write.address, cache_block_header.checksum);

            grid.trace.stop(.{ .grid_write = .{ .iop = grid.write_iops.index(iop) } });

            if (grid.callback == .cancel) {
                assert(grid.write_queue.empty());

                grid.write_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            // Start a queued write if possible *before* calling the completed
            // write's callback. This ensures that if the callback calls
            // Grid.write_block() it doesn't preempt the queue.
            //
            // (Don't pop from the write queue until after the read-repairs are resolved.
            // Otherwise their resolution might complete grid cancellation, but the replica has
            // not released its own write iop (via callback).)
            if (grid.write_queue.pop()) |queued_write| {
                grid.write_block_with(iop, queued_write);
            } else {
                grid.write_iops.release(iop);
            }

            // Precede the write's callback, since the callback takes back ownership of the block.
            if (completed_write.repair) grid.blocks_missing.write_complete(cache_block);
            // This call must come after (logically) releasing the IOP. Otherwise we risk tripping
            // assertions forbidding concurrent writes using the same block/address
            // if the callback calls write_block().
            completed_write.callback(completed_write);

            // We start awaiting pending repairs when the checkpoint becomes durable.
            if (grid.callback == .checkpoint_durable) grid.checkpoint_durable_join();
        }

        /// Fetch the block synchronously from the write queues, if possible.
        /// Note that we allow the creation of a block (and hence a write to
        /// an address) to span the entirety of the checkpoint when the write
        /// was initiated. This is safe to do as this address can only be
        /// freed and overwritten in the next checkpoint.
        ///
        /// It is possible to read an address while it's being written to, in the
        /// following scenarios:
        /// * Reading a block that is currenly being repaired.
        /// * Reading a block that is currently being created, if it is requested
        ///   by another replica, or by the replica itself.
        fn read_block_from_write_queues(
            grid: *const Grid,
            address: u64,
            checksum: u128,
        ) ?BlockPtrConst {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(address > 0);

            var block_found_count: u64 = 0;
            var block: ?BlockPtrConst = null;

            var write_queue_iterator = grid.write_queue.iterate();
            while (write_queue_iterator.next()) |queued_write| {
                const queued_write_header = mem.bytesAsValue(
                    vsr.Header.Block,
                    queued_write.block.*[0..@sizeOf(vsr.Header)],
                );

                if (address == queued_write_header.address and
                    checksum == queued_write_header.checksum)
                {
                    block_found_count += 1;
                    block = queued_write.block.*;
                }
            }

            var write_iops_iterator = grid.write_iops.iterate_const();
            while (write_iops_iterator.next()) |iop| {
                const queued_write_header = mem.bytesAsValue(
                    vsr.Header.Block,
                    iop.write.block.*[0..@sizeOf(vsr.Header)],
                );

                if (address == queued_write_header.address and
                    checksum == queued_write_header.checksum)
                {
                    block_found_count += 1;
                    block = iop.write.block.*;
                }
            }

            assert(block_found_count <= 1);
            return block;
        }

        /// Fetch the block synchronously from the write queues or grid cache, if possible.
        /// The returned block pointer is only valid until the next Grid write.
        pub fn read_block_from_cache(
            grid: *Grid,
            address: u64,
            checksum: u128,
            options: struct { coherent: bool },
        ) ?BlockPtrConst {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(address > 0);

            if (options.coherent) {
                assert(!grid.free_set.is_free(address));
                grid.assert_coherent(address, checksum);
            }

            const cache_index = grid.cache.get_index(address) orelse {
                if (grid.read_block_from_write_queues(address, checksum)) |block| {
                    grid.assert_coherent(address, checksum);
                    return block;
                }
                return null;
            };
            const cache_location = grid.cache_locations[cache_index];
            const cache_block = &grid.blocks[cache_location];
            const header = schema.header_from_block(cache_block);
            assert(header.address == address);
            assert(header.cluster == grid.superblock.working.cluster);
            assert(header.release.value <=
                grid.superblock.working.vsr_state.checkpoint.release.value);

            if (header.checksum == checksum) {
                if (constants.verify and
                    options.coherent and
                    grid.superblock.working.vsr_state.sync_op_max == 0)
                {
                    grid.verify_read_from_cache(address, cache_block);
                }

                return cache_block;
            } else {
                const write_queue_block = grid.read_block_from_write_queues(address, checksum);

                // For coherent reads, we can only find an old version of the
                // block in the cache if we either:
                // * Learnt about a new version of the block via state sync, or
                // * A new version of that block is currently being written
                //
                // This is because we evict the old version of the block from the
                // cache, as soon as it is written (see `write_block_callback`).
                if (options.coherent) {
                    assert(grid.superblock.working.vsr_state.sync_op_max > 0 or
                        write_queue_block != null);
                }

                if (write_queue_block) |block| {
                    grid.assert_coherent(address, checksum);
                    return block;
                }

                return null;
            }
        }

        pub fn read_block(
            grid: *Grid,
            callback: ReadBlockCallback,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
            options: struct {
                cache_read: bool,
                cache_write: bool,
            },
        ) void {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(address > 0);

            // It is possible to read an address while it's being written to
            // (see `read_block_from_write_queues`).
            maybe(grid.writing(address, null) == .create);

            switch (callback) {
                .from_local_storage => {
                    maybe(grid.callback == .checkpoint);
                    // We try to read the block even when it is free. If we
                    // recently released it, it might be found on disk anyway.
                    maybe(grid.free_set.is_free(address));
                },
                .from_local_or_global_storage => {
                    assert(grid.callback != .checkpoint);
                    assert(!grid.free_set.is_free(address));
                    grid.assert_coherent(address, checksum);
                },
            }

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .coherent = callback == .from_local_or_global_storage,
                .cache_read = options.cache_read,
                .cache_write = options.cache_write,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
                .checkpoint_durable = grid.free_set.checkpoint_durable,
                .grid = grid,
            };

            if (options.cache_read) {
                grid.on_next_tick(read_block_tick_callback, &read.next_tick);
            } else {
                read_block_tick_callback(&read.next_tick);
            }
        }

        fn read_block_tick_callback(next_tick: *Storage.NextTick) void {
            const read: *Grid.Read = @alignCast(@fieldParentPtr("next_tick", next_tick));
            const grid = read.grid;
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            if (read.coherent) {
                assert(!grid.free_set.is_free(read.address));
                maybe(grid.writing(read.address, null) == .create);
            }

            assert(read.address > 0);

            // Check the write queue before checking the read queue, since otherwise:
            // 1. Read block. (coherent=false, i.e. via repair)
            // 2. Create block. (start)
            // 3. Read block again. (coherent=true)
            // We must ensure that the second read succeeds, so that it doesn't just queue up behind
            // the first read.
            if (grid.read_block_from_write_queues(read.address, read.checksum)) |block| {
                grid.assert_coherent(read.address, read.checksum);
                grid.read_block_resolve(read, .{ .valid = block });
                return;
            }

            // Check if a read is already processing/recovering and merge with it.
            for ([_]*const QueueType(Read){
                &grid.read_queue,
                &grid.read_global_queue,
            }) |queue| {
                // Don't remote-repair repairs – the block may not belong in our current checkpoint.
                if (read.callback == .from_local_storage) {
                    if (queue == &grid.read_global_queue) continue;
                }

                var it = queue.iterate();
                while (it.next()) |queued_read| {
                    if (queued_read.address == read.address) {
                        // TODO check all read options match
                        if (queued_read.checksum == read.checksum) {
                            queued_read.resolves.push(&read.pending);
                            return;
                        } else {
                            assert(!queued_read.coherent or !read.coherent);
                        }
                    }
                }
            }

            // When Read.cache_read is set, the caller of read_block()
            // is responsible for calling us via next_tick().
            if (read.cache_read) {
                if (grid.read_block_from_cache(
                    read.address,
                    read.checksum,
                    .{ .coherent = read.coherent },
                )) |cache_block| {
                    grid.read_block_resolve(read, .{ .valid = cache_block });
                    return;
                }
            }

            // Become the "root" read that's fetching the block for the
            // given address. The fetch happens asynchronously to avoid
            // stack-overflow and nested cache invalidation.
            grid.read_queue.push(read);

            // Grab an IOP to resolve the block from storage.
            // Failure to do so means the read is queued to receive an
            // IOP when one finishes.
            const iop = grid.read_iops.acquire() orelse {
                grid.read_pending_queue.push(&read.pending);
                return;
            };

            grid.read_block_with(iop, read);
        }

        fn read_block_with(grid: *Grid, iop: *Grid.ReadIOP, read: *Grid.Read) void {
            const address = read.address;
            assert(address > 0);

            // We can only update the cache if the Grid is not resolving
            // callbacks with a cache block.
            assert(!grid.read_resolving);

            grid.trace.start(.{ .grid_read = .{ .iop = grid.read_iops.index(iop) } });

            const iop_location = location: {
                // FIXME O(n)
                for (grid.cache_locations[grid.cache_blocks_count..]) |location| {
                    if (grid.blocks_references[location] == 0) {
                        break :location location;
                    }
                } else unreachable;
            };

            iop.* = .{
                .completion = undefined,
                .read = read,
                .location = iop_location,
            };

            grid.blocks_references[iop_location] = 1;
            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                &grid.blocks[iop_location],
                .grid,
                block_offset(address),
            );
        }

        fn read_block_callback(completion: *Storage.Read) void {
            const iop: *ReadIOP = @fieldParentPtr("completion", completion);
            const read = iop.read;
            const grid = read.grid;
            const block = &grid.blocks[iop.location];
            const block_location = grid.location_from_block(block);
            assert(grid.blocks_references[block_location] == 1);
            defer grid.blocks_references[block_location] -= 1;

            grid.trace.stop(.{ .grid_read = .{ .iop = grid.read_iops.index(iop) } });

            if (grid.callback == .cancel) {
                grid.read_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            // FIXME or maybe call this after Handoff iop...
            const result = read_block_validate(block, .{
                .address = read.address,
                .checksum = read.checksum,
            });

            if (result == .valid) {
                grid.cache_upsert(.{ .address = read.address, .location = block_location });
            }

            // Handoff the iop to a pending read or release it before resolving the callbacks below.
            if (grid.read_pending_queue.pop()) |pending| {
                const queued_read: *Read = @alignCast(@fieldParentPtr("pending", pending));
                grid.read_block_with(iop, queued_read);
            } else {
                grid.read_iops.release(iop);
            }

            // Remove the "root" read so that the address is no longer actively reading / locked.
            grid.read_queue.remove(read);

            if (result != .valid) {
                const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);
                log.warn(
                    "{}: {s}: expected address={} checksum={x:0>32}, " ++
                        "found address={} checksum={x:0>32}",
                    .{
                        grid.superblock.replica_index.?,
                        @tagName(result),
                        read.address,
                        read.checksum,
                        header.address,
                        header.checksum,
                    },
                );

                if (constants.verify) grid.verify_read_fault(read);
            }

            grid.read_block_resolve(read, result);
        }

        fn read_block_validate(block: BlockPtrConst, expect: struct {
            address: u64,
            checksum: u128,
        }) ReadBlockResult {
            const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);

            if (!header.valid_checksum()) return .invalid_checksum;
            if (header.command != .block) return .unexpected_command;

            assert(header.size >= @sizeOf(vsr.Header));
            assert(header.size <= constants.block_size);

            const block_body = block[@sizeOf(vsr.Header)..header.size];
            if (!header.valid_checksum_body(block_body)) {
                return .invalid_checksum_body;
            }

            if (header.checksum != expect.checksum) return .unexpected_checksum;

            if (!stdx.zeroed(block[header.size..vsr.sector_ceil(header.size)])) {
                return .invalid_padding;
            }

            assert(header.address == expect.address);
            return .{ .valid = block };
        }

        fn read_block_resolve(grid: *Grid, read: *Grid.Read, result: ReadBlockResult) void {
            assert(grid.callback != .cancel);

            // Guard to make sure the cache cannot be updated by any read.callbacks() below.
            assert(!grid.read_resolving);
            grid.read_resolving = true;
            defer {
                assert(grid.read_resolving);
                grid.read_resolving = false;
            }

            if (read.coherent) {
                assert(!grid.free_set.is_free(read.address));
                assert(read.checkpoint_id == grid.superblock.working.checkpoint_id());
                grid.assert_coherent(read.address, read.checksum);
            }

            if (result == .valid) {
                const header = schema.header_from_block(result.valid);
                assert(header.cluster == grid.superblock.working.cluster);
                assert(header.release.value <=
                    grid.superblock.working.vsr_state.checkpoint.release.value);
                assert(header.address == read.address);
                assert(header.checksum == read.checksum);
            }

            var read_remote_resolves: QueueType(ReadPending) = QueueType(ReadPending).init(.{
                .name = read.resolves.any.name,
            });

            // Resolve all reads queued to the address with the block.
            while (read.resolves.pop()) |pending| {
                const pending_read: *Read = @alignCast(@fieldParentPtr("pending", pending));
                assert(pending_read.address == read.address);
                assert(pending_read.checksum == read.checksum);
                if (pending_read.coherent) {
                    assert(pending_read.checkpoint_id == grid.superblock.working.checkpoint_id());
                }

                switch (pending_read.callback) {
                    .from_local_storage => |callback| callback(pending_read, result),
                    .from_local_or_global_storage => |callback| {
                        if (result == .valid) {
                            callback(pending_read, result.valid);
                        } else {
                            read_remote_resolves.push(&pending_read.pending);
                        }
                    },
                }
            }

            // Then invoke the callback with the cache block (which should be valid for the duration
            // of the callback as any nested Grid calls cannot synchronously update the cache).
            switch (read.callback) {
                .from_local_storage => |callback| callback(read, result),
                .from_local_or_global_storage => |callback| {
                    if (result == .valid) {
                        callback(read, result.valid);
                    } else {
                        read_remote_resolves.push(&read.pending);
                    }
                },
            }

            // On the result of an invalid block, move the "root" read (and all others it
            // resolves) to recovery queue. Future reads on the same address will see the "root"
            // read in the recovery queue and enqueue to it.
            if (read_remote_resolves.pop()) |read_remote_head_pending| {
                const read_remote_head: *Read = @alignCast(
                    @fieldParentPtr("pending", read_remote_head_pending),
                );
                assert(read_remote_head.callback == .from_local_or_global_storage);
                assert(read_remote_head.coherent);

                log.debug("{}: read_block: fault: address={} checksum={x:0>32}", .{
                    grid.superblock.replica_index.?,
                    read_remote_head.address,
                    read_remote_head.checksum,
                });

                read_remote_head.resolves = read_remote_resolves;
                grid.read_global_queue.push(read_remote_head);

                if (grid.blocks_missing.repair_blocks_available() > 0) {
                    grid.blocks_missing.repair_block(
                        read_remote_head.address,
                        read_remote_head.checksum,
                    );
                }
            }
        }

        /// Insert the address into the cache, and swap the evicted block into the stash.
        fn cache_upsert(grid: *Grid, options: struct { address: u64, location: u32 }) void {
            assert(options.address != 0);
            assert(options.location < grid.blocks.len);

            // The location/block that is being moved from stash to cache.
            const block_cache_location = options.location;
            const block_cache = &grid.blocks[block_cache_location];
            const block_cache_header = schema.header_from_block(block_cache);
            assert(block_cache_header.address == options.address);

            const cache_index = grid.cache.upsert(&options.address).index;
            const stash_index = index: {
                for (
                    grid.cache_locations[grid.cache_blocks_count..],
                    grid.cache_blocks_count..,
                ) |location, i| {
                    if (location == block_cache_location) break :index i;
                } else unreachable;
            };
            assert(cache_index != stash_index);
            assert(cache_index < grid.cache_blocks_count);
            assert(stash_index >= grid.cache_blocks_count);

            // The location/block being moved from cache to stash.
            const block_stash_location = grid.cache_locations[cache_index];
            assert(block_stash_location != block_cache_location);

            assert(grid.blocks_references[block_cache_location] > 0);
            maybe(grid.blocks_references[block_stash_location] > 0);

            assert(grid.cache_locations[cache_index] == block_stash_location);
            assert(grid.cache_locations[stash_index] == block_cache_location);

            grid.cache_locations[cache_index] = block_cache_location;
            grid.cache_locations[stash_index] = block_stash_location;

            if (grid.blocks_references[block_stash_location] == 0) {
                // This block content won't be used again.
                // We could overwrite the entire thing, but that would be more expensive.
                const block_stashed = &grid.blocks[block_stash_location];
                @memset(block_stashed[0..@sizeOf(vsr.Header)], 0);
            }
        }

        fn location_from_block(grid: *const Grid, block: BlockPtrConst) u32 {
            assert(@intFromPtr(block.ptr) >= @intFromPtr(grid.blocks.ptr));
            assert(@intFromPtr(block.ptr) <
                @intFromPtr(grid.blocks.ptr) + grid.blocks.len * constants.block_size);

            const offset = @intFromPtr(block.ptr) - @intFromPtr(grid.blocks.ptr);
            return @intCast(@divExact(offset, constants.block_size));
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return (address - 1) * block_size;
        }

        /// Verify that the storage:
        /// - contains the given index block
        /// - contains every value block referenced by the index block
        pub fn verify_table(grid: *Grid, index_address: u64, index_checksum: u128) void {
            assert(index_address > 0);

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            const index_block = blk: {
                if (grid.read_block_from_write_queues(index_address, index_checksum)) |block| {
                    break :blk block;
                } else {
                    break :blk grid.superblock.storage.grid_block(index_address).?;
                }
            };
            const index_schema = schema.TableIndex.from(index_block);
            const index_block_header = schema.header_from_block(index_block);

            assert(index_block_header.address == index_address);
            assert(index_block_header.checksum == index_checksum);
            assert(index_block_header.block_type == .index);

            for (
                index_schema.value_addresses_used(index_block),
                index_schema.value_checksums_used(index_block),
            ) |value_address, value_checksum| {
                const value_block = blk: {
                    if (grid.read_block_from_write_queues(
                        value_address,
                        value_checksum.value,
                    )) |block| {
                        break :blk block;
                    } else {
                        break :blk grid.superblock.storage.grid_block(value_address).?;
                    }
                };
                const value_block_header = schema.header_from_block(value_block);

                assert(value_block_header.address == value_address);
                assert(value_block_header.checksum == value_checksum.value);
                assert(value_block_header.block_type == .value);
            }
        }

        fn assert_coherent(grid: *const Grid, address: u64, checksum: u128) void {
            assert(!grid.free_set.is_free(address));

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            if (grid.superblock.storage.options.grid_checker) |checker| {
                checker.assert_coherent(
                    &grid.superblock.working.vsr_state.checkpoint,
                    grid.free_set.checkpoint_durable,
                    address,
                    checksum,
                );

                checker.assert_coherent(
                    &grid.superblock.staging.vsr_state.checkpoint,
                    checkpoint_durable: {
                        if (grid.superblock.working.checkpoint_id() ==
                            grid.superblock.staging.checkpoint_id())
                        {
                            break :checkpoint_durable grid.free_set.checkpoint_durable;
                        } else {
                            // Checkpoint is currently being written to the superblock. Pass
                            // checkpoint_durable=False as we update free_set.checkpoint_durable
                            // only *after* the checkpoint is written to the superblock.
                            assert(grid.superblock.staging.parent_checkpoint_id() ==
                                grid.superblock.working.checkpoint_id());
                            assert(grid.free_set.checkpoint_durable);
                            break :checkpoint_durable false;
                        }
                    },
                    address,
                    checksum,
                );
            }
        }

        fn verify_read_from_cache(
            grid: *const Grid,
            address: u64,
            cached_block: BlockPtrConst,
        ) void {
            comptime assert(constants.verify);

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            const actual_block = grid.superblock.storage.grid_block(address).?;
            const actual_header = schema.header_from_block(actual_block);
            const cached_header = schema.header_from_block(cached_block);
            assert(cached_header.checksum == actual_header.checksum);

            assert(std.mem.eql(
                u8,
                cached_block[0..cached_header.size],
                actual_block[0..actual_header.size],
            ));
        }

        /// Called when we fail to read a block.
        fn verify_read_fault(grid: *const Grid, read: *const Read) void {
            comptime assert(constants.verify);

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            // Only check coherent reads -- i.e., when we know for certain that the read's
            // address/checksum belongs in our current checkpoint.
            if (!read.coherent) return;

            // Check our storage (bypassing faults).
            if (grid.superblock.storage.grid_block(read.address)) |actual_block| {
                const actual_header = schema.header_from_block(actual_block);
                if (actual_header.checksum == read.checksum) {
                    // Exact block found. Since the read failed anyway, it must have been a
                    // simulated read fault.
                    assert(grid.superblock.storage.area_faulty(.{
                        .grid = .{ .address = read.address },
                    }));
                } else {
                    // Different block found -- since this is a coherent read, we must be syncing.
                    assert(grid.superblock.working.vsr_state.sync_op_max > 0);
                }
            } else {
                // No block found -- since this is a coherent read, we must by syncing.
                assert(grid.superblock.working.vsr_state.sync_op_max > 0);
            }
        }

        /// Mark all blocks in the grid cache as MADV_DONTDUMP. Must be done after transitioning
        /// to static, as the combination of madvise() + mremap() can cause an EFAULT.
        ///
        /// It's OK that some blocks, such as the blocks used by compaction escape this -- this is
        /// not to stop sensitive data from appearing in core dumps, but rather to keep the core
        /// dump size manageable even with a large grid cache.
        /// FIXME call this from init() once we use memfds+mmap?
        pub fn madv_dont_dump(grid: *const Grid) !void {
            if (builtin.target.os.tag != .linux) return;

            assert(grid.blocks.len > 0);

            try std.posix.madvise(
                @ptrFromInt(@intFromPtr(grid.blocks.ptr)),
                grid.blocks.len * constants.block_size,
                std.posix.MADV.DONTDUMP,
            );

            log.debug("marked {} bytes as MADV_DONTDUMP", .{
                grid.blocks.len * constants.block_size,
            });
        }
    };
}
