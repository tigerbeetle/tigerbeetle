const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const BoundedArray = stdx.BoundedArray;

pub const Reservation = struct {
    round: u32,

    index: u32,
    remaining: u32,

    remaining_staging: u32,
};

pub const FreeList = struct {
    /// List of block address in the free list.
    ///
    /// In the superblock on disk and in memory at checkpoint, this is a sorted list of free blocks.
    ///
    /// Between checkpoints, the slice is subdivided into two parts at `staging_index`: "free" and
    /// "staging". Entries in range `0..<staging_index` are free blocks, entries in
    /// `staging_index..<blocks.len` are staging (released). Staging blocks are blocks released in
    /// the current checkpoint. They become free at the next checkpoint.
    ///
    /// The reason to store both free and released addresses in the same slice is that the sum of
    /// their counts must bounded, and a shared backing slice enforces that.
    blocks: []u64,
    /// Blocks before this index are free at the current checkpoint, blocks after this index become
    /// free in the next checkpoint.
    staging_index: u32,
    staging_count: u32,

    // First unallocated block in the grid.
    //
    // If the free list is empty, it is replenished by incrementing this counter.
    next_free: u64,

    // Count of reserving->forfeiting cycles within a single checkpoint, used to assert that
    // only reservations from the current round can be forfeited.
    round: u32,
    // Number of outstanding reservations in this round, used to check that all reservations are
    // forfeited.
    round_reservations: u32,

    // Total number of blocks reserved in the current round, used to check that the total size of
    // reservations in a single round is bounded.
    round_reserved: u32,
    // The upper bound on the number of outstanding reservations. This bound is critical to set the
    // `staging_index` correctly --- the upper bound on the number of allocations provides the lower
    // bound on the number of frees.
    //
    // `round_reserved_max` must be kept constant across rounds in  single checkpoint, but it can
    // vary between checkpoints. The way replica uses `FreeList`, `round_reserved_max` is a constant
    // determined by the shape of the forest.
    round_reserved_max: u32,
    // Total number of blocks reserved for the freeing in the current round.
    round_reserved_staging: u32,

    // Number of blocks actually allocated in the round.
    round_acquired: u32,
    // Number of blocks actually freed in the round.
    round_released: u32,

    state: enum { reserving, forfeiting },

    pub fn init(allocator: mem.Allocator, capacity: u32) !FreeList {
        const blocks = try allocator.alloc(u64, capacity);
        errdefer allocator.free(blocks);

        @memset(blocks, 0);
        return .{
            .blocks = blocks,
            .staging_index = 0,
            .staging_count = 0,

            .next_free = 1,

            .round = 0,
            .round_reservations = 0,

            .round_reserved = 0,
            .round_reserved_max = 0,
            .round_reserved_staging = 0,

            .round_acquired = 0,
            .round_released = 0,

            .state = .reserving,
        };
    }

    pub fn deinit(free_list: *FreeList, allocator: mem.Allocator) void {
        allocator.free(free_list.blocks);
    }

    pub fn reset(free_list: *FreeList) void {
        @memset(free_list.blocks, 0);
        free_list = .{
            .blocks = free_list.blocks,
            .staging_index = 0,
            .staging_count = 0,

            .next_free = 1,

            .round = 0,
            .round_reservations = 0,

            .round_reserved = 0,
            .round_reserved_max = 0,
            .round_reserved_staging = 0,

            .round_acquired = 0,
            .round_released = 0,

            .state = .reserving,
        };
    }

    pub fn decode(free_list: *FreeList, source: []u64) void {
        assert(free_list.state == .reserving);
        assert(free_list.staging_index == 0);
        assert(free_list.staging_count == 0);
        assert(free_list.next_free == 0);

        assert(source.len == free_list.free.len + 1);

        assert(free_list.next_free == 0);
        free_list.next_free = source[0];
        assert(free_list.next_free > 0);

        var trailing_zeroes = false;
        for (source[1..]) |address| {
            if (address == 0) {
                trailing_zeroes = true;
            } else {
                assert(!trailing_zeroes);
                assert(address < free_list.free_address);
                free_list.blocks[free_list.staging_index] = address;
                free_list.staging_index += 1;
            }
        }

        assert(free_list.round_reserved_max == 0);
    }

    pub fn encode(free_list: *const FreeList, target: []u64) void {
        assert(free_list.state == .reserving);
        assert(free_list.round_reservations == 0);
        assert(free_list.staging_count == 0);
        assert(target.len == free_list.free.len + 1);

        target[0] = free_list.free_address;
        stdx.copy_disjoint(
            .exact,
            u64,
            target[1 .. 1 + free_list.staging_index],
            free_list.free[0..free_list.staging_index],
        );
        @memset(target[1 + free_list.staging_index], 0);
    }

    pub fn set_round_reserved_max(free_list: *FreeList, reserved_max: u32) void {
        assert(free_list.state == .reserving);
        if (free_list.round == 0 and free_list.round_reservations == 0) {
            free_list.round_reserved_max = reserved_max;
            free_list.staging_index = @max(free_list.staging_index, reserved_max);

            assert(free_list.staging_index >= free_list.round_reserved_max);
        } else {
            assert(free_list.round_reserved_max == reserved_max);
        }
    }

    pub fn reserve(
        free_list: *FreeList,
        options: struct {
            acquire_blocks: u32,
            release_blocks: u32,
        },
    ) Reservation {
        assert(free_list.state == .reserving);

        if (free_list.round_reservations == 0) {
            // First reservation in a round, replenish the free list from the end of the grid.
            assert(free_list.round_released == 0);
            assert(free_list.round_released == 0);
            maybe(free_list.staging_count > 0);

            for (0..free_list.staging_index) |index| {
                if (free_list.blocks[index] == 0) {
                    free_list.blocks[index] = free_list.next_free;
                    free_list.next_free += 1;
                }
            }
        }
        free_list.round_reservations += 1;

        free_list.round_reserved += options.acquire_blocks;
        assert(free_list.round_reserved <= free_list.round_reserved_max);

        const staging_len = free_list.blocks.len - free_list.staging_index;
        const staging_spare_capacity = staging_len - free_list.staging_count;

        const staging_reservation = @min(
            options.release_blocks,
            staging_spare_capacity - free_list.round_reserved_staging,
        );
        free_list.round_reserved_staging += staging_reservation;
        assert(free_list.round_reserved_staging <= staging_spare_capacity);

        return .{
            .round = free_list.round,

            .index = free_list.round_reserved - options.acquire_blocks,
            .remaining = options.acquire_blocks,

            .remaining_staging = staging_reservation,
        };
    }

    pub fn acquire(free_list: *FreeList, reservation: *Reservation) ?u64 {
        assert(reservation.round == free_list.round);
        maybe(free_list.state == .reserving);

        if (reservation.remaining == 0) return null;
        assert(reservation.index < free_list.staging_index);

        free_list.round_acquired += 1;
        assert(free_list.round_acquired <= free_list.round_reserved);

        const block_address = free_list.blocks[reservation.index];
        free_list.blocks[reservation.index] = 0;
        assert(block_address != 0);

        reservation.index += 1;
        reservation.remaining -= 1;

        return block_address;
    }

    pub fn release(free_list: *FreeList, reservation: *Reservation, block_address: u64) void {
        assert(reservation.round == free_list.round);
        assert(free_list.state == .reserving);
        assert(block_address > 0);
        assert(reservation.remaining_staging > 0);

        free_list.round_released += 1;
        assert(free_list.round_released <= free_list.round_reserved_staging);

        const index = free_list.staging_index + free_list.staging_count;
        assert(index < free_list.blocks.len);
        assert(free_list.blocks[index] == 0);
        free_list.blocks[index] = block_address;
        free_list.staging_count += 1;

        reservation.remaining_staging -= 1;
    }

    pub fn forfeit(free_list: *FreeList, reservation: *Reservation) void {
        assert(reservation.round == free_list.round);
        maybe(free_list.state == .reserving);
        assert(free_list.round_reservations > 0);

        free_list.state = .forfeiting;

        free_list.round_reservations -= 1;
        free_list.round_reserved -= reservation.remaining;
        free_list.round_reserved_staging -= reservation.remaining_staging;
        reservation.* = undefined;

        if (free_list.round_reservations == 0) {
            assert(free_list.round_reserved == free_list.round_acquired);
            assert(free_list.round_reserved_staging == free_list.round_released);
            assert(free_list.round_released <= free_list.staging_count);

            maybe(free_list.round_acquired > 0);
            free_list.shift_acquired_blocks();
            assert(free_list.round_acquired == 0);

            free_list.round_reserved = 0;
            free_list.round_reserved_staging = 0;
            free_list.round_released = 0;

            free_list.round += 1;
            free_list.state = .reserving;
        }
    }

    fn shift_acquired_blocks(free_list: *FreeList) void {
        assert(free_list.state == .forfeiting);
        assert(free_list.round_reservations == 0);

        // Shift everything left to compact zeros.
        var gap: u32 = 0;
        for (0..free_list.staging_index) |index| {
            if (free_list.blocks[index] == 0) {
                gap += 1;
            } else {
                free_list.blocks[index - gap] = free_list.blocks[index];
            }
        }
        assert(gap == free_list.round_acquired);
        free_list.round_acquired = 0;
    }

    pub fn checkpoint(free_list: *FreeList) void {
        assert(free_list.state == .reserving);
        assert(free_list.round_reservations == 0);

        // Merge `staging` into free, and sort everything for determinism.
        for (free_list.staging[free_list.staging_count]) |address| {
            free_list.free[free_list.free_count] = address;
            free_list.free_count += 1;
        }
        free_list.staging_count = 0;

        // TODO: sort just `staging` and backwards-merge it into `free`.
        mem.sortUnstable(u64, free_list.free[0..free_list.free_count], {}, address_less_than);
        free_list.round = 0;
    }
};

fn address_less_than(_: void, lhs: u64, rhs: u64) bool {
    assert(lhs != rhs);
    return lhs < rhs;
}
