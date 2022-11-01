//! Fuzz FreeSet reserve/acquire/release flow.
//!
//! This fuzzer does *not* cover FreeSet encoding/decoding.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_vsr_superblock_free_set);

const FreeSet = @import("./superblock_free_set.zig").FreeSet;
const AddressReservation = @import("./superblock_free_set.zig").AddressReservation;
const fuzz = @import("../test/fuzz.zig");

pub fn main() !void {
    const allocator = std.testing.allocator;
    const args = try fuzz.parse_fuzz_args(allocator);

    var prng = std.rand.DefaultPrng.init(args.seed);

    const blocks_count = FreeSet.shard_size * (1 + prng.random().uintLessThan(usize, 10));
    const events = try generate_events(allocator, prng.random(), blocks_count);
    defer allocator.free(events);

    try run_fuzz(allocator, blocks_count, events);
}

fn run_fuzz(
    allocator: std.mem.Allocator,
    blocks_count: usize,
    events: []const FreeSetEvent,
) !void {
    var free_set = try FreeSet.init(allocator, blocks_count);
    defer free_set.deinit(allocator);

    var free_set_model = try FreeSetModel.init(allocator, blocks_count);
    defer free_set_model.deinit(allocator);

    var active_reservations = std.ArrayList(AddressReservation).init(allocator);
    defer active_reservations.deinit();

    var active_addresses = std.ArrayList(u64).init(allocator);
    defer active_addresses.deinit();

    for (events) |event| {
        log.debug("event={}", .{event});
        switch (event) {
            .reserve => |reserve| {
                const reservation_actual = free_set.reserve(reserve.blocks);
                const reservation_expect = free_set_model.reserve(reserve.blocks);
                assert(std.meta.eql(reservation_expect, reservation_actual));
                if (reservation_expect) |reservation| {
                    try active_reservations.append(reservation);
                }
            },
            .reserve_done => {
                free_set.reserve_done();
                free_set_model.reserve_done();
                active_reservations.clearRetainingCapacity();
            },
            .acquire => |data| {
                if (active_reservations.items.len == 0) continue;
                const reservation = active_reservations.items[
                    data.reservation % active_reservations.items.len
                ];
                const address_actual = free_set.acquire_reserved(reservation);
                const address_expect = free_set_model.acquire_reserved(reservation);
                assert(std.meta.eql(address_expect, address_actual));
                if (address_expect) |address| {
                    try active_addresses.append(address);
                }
            },
            .release => |data| {
                if (active_addresses.items.len == 0) continue;

                const address = active_addresses.swapRemove(data.address % active_addresses.items.len);
                free_set.release(address);
                free_set_model.release(address);
            },
            .checkpoint => {
                free_set.reserve_done();
                free_set_model.reserve_done();
                active_reservations.clearRetainingCapacity();

                free_set.checkpoint();
                free_set_model.checkpoint();
            },
        }

        assert(free_set_model.count_free() == free_set.count_free());
        assert(free_set_model.count_acquired() == free_set.count_acquired());
        assert(std.meta.eql(
            free_set_model.highest_address_acquired(),
            free_set.highest_address_acquired(),
        ));
    }
}

const FreeSetEventType = std.meta.Tag(FreeSetEvent);
const FreeSetEvent = union(enum) {
    reserve: struct { blocks: usize },
    reserve_done: void,
    acquire: struct { reservation: usize },
    release: struct { address: usize },
    checkpoint: void,
};

fn generate_events(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    blocks_count: usize,
) ![]const FreeSetEvent {
    const event_distribution = fuzz.Distribution(FreeSetEventType){
        .reserve = 1 + random.float(f64) * 100,
        .reserve_done = 1,
        .acquire = random.float(f64) * 1000,
        .release = if (random.boolean()) 0 else 500 * random.float(f64),
        .checkpoint = random.floatExp(f64) * 10,
    };

    const events = try allocator.alloc(FreeSetEvent, std.math.min(
        @as(usize, blocks_count * 1000),
        fuzz.random_int_exponential(random, usize, blocks_count * 100),
    ));
    errdefer allocator.free(events);

    log.info("event_distribution = {d:.2}", .{event_distribution});
    log.info("event_count = {d}", .{events.len});

    const reservation_blocks_mean = 1 + random.uintLessThan(usize, @divFloor(blocks_count, 20));
    for (events) |*event| {
        event.* = switch (fuzz.random_enum(random, FreeSetEventType, event_distribution)) {
            .reserve => FreeSetEvent{ .reserve = .{
                .blocks = 1 + fuzz.random_int_exponential(random, usize, reservation_blocks_mean),
            } },
            .reserve_done => FreeSetEvent{ .reserve_done = {} },
            .acquire => FreeSetEvent{ .acquire = .{ .reservation = random.int(usize) } },
            .release => FreeSetEvent{ .release = .{
                .address = random.int(usize),
            } },
            .checkpoint => FreeSetEvent{ .checkpoint = {} },
        };
    }
    return events;
}

const FreeSetModel = struct {
    /// Set bits indicate acquired blocks.
    blocks_acquired: std.DynamicBitSetUnmanaged,

    /// Set bits indicate blocks that will be released at the next checkpoint.
    blocks_released: std.DynamicBitSetUnmanaged,

    blocks_reserved: usize = 0,

    fn init(allocator: std.mem.Allocator, blocks_count: usize) !FreeSetModel {
        var blocks_acquired = try std.DynamicBitSetUnmanaged.initEmpty(allocator, blocks_count);
        errdefer blocks_acquired.deinit(allocator);

        var blocks_released = try std.DynamicBitSetUnmanaged.initEmpty(allocator, blocks_count);
        errdefer blocks_released.deinit(allocator);

        return FreeSetModel{
            .blocks_acquired = blocks_acquired,
            .blocks_released = blocks_released,
        };
    }

    fn deinit(set: *FreeSetModel, allocator: std.mem.Allocator) void {
        set.blocks_acquired.deinit(allocator);
        set.blocks_released.deinit(allocator);
    }

    pub fn count_free(set: FreeSetModel) u64 {
        return set.blocks_acquired.capacity() - set.blocks_acquired.count();
    }

    pub fn count_acquired(set: FreeSetModel) u64 {
        return set.blocks_acquired.count();
    }

    pub fn highest_address_acquired(set: FreeSetModel) ?u64 {
        const block = set.blocks_acquired.iterator(.{
            .direction = .reverse,
        }).next() orelse return null;
        return block + 1;
    }

    pub fn acquire_reserved(set: *FreeSetModel, reservation: AddressReservation) ?u64 {
        assert(reservation.block_count > 0);
        assert(reservation.block_base < set.blocks_acquired.capacity());
        assert(reservation.block_base < set.blocks_reserved);
        assert(reservation.block_base + reservation.block_count <= set.blocks_reserved);

        var iterator = set.blocks_acquired.iterator(.{ .kind = .unset });
        while (iterator.next()) |block| {
            if (block >= reservation.block_base and
                block < reservation.block_base + reservation.block_count)
            {
                assert(!set.blocks_acquired.isSet(block));
                set.blocks_acquired.set(block);

                const address = block + 1;
                return address;
            }
        }
        return null;
    }

    pub fn reserve(set: *FreeSetModel, reserve_count: usize) ?AddressReservation {
        assert(reserve_count > 0);

        var blocks_found_free: usize = 0;
        var iterator = set.blocks_acquired.iterator(.{ .kind = .unset });
        while (iterator.next()) |block| {
            if (block < set.blocks_reserved) continue;

            blocks_found_free += 1;
            if (blocks_found_free == reserve_count) {
                const block_count = block + 1 - set.blocks_reserved;
                const block_base = set.blocks_reserved;
                set.blocks_reserved += block_count;

                return AddressReservation{
                    .block_base = block_base,
                    .block_count = block_count,
                };
            }
        }
        return null;
    }

    pub fn reserve_done(set: *FreeSetModel) void {
        set.blocks_reserved = 0;
    }

    pub fn is_free(set: *FreeSetModel, address: u64) bool {
        return !set.blocks_acquired.isSet(address - 1);
    }

    pub fn release(set: *FreeSetModel, address: u64) void {
        const block = address - 1;
        set.blocks_released.set(block);
    }

    pub fn checkpoint(set: *FreeSetModel) void {
        assert(set.blocks_reserved == 0);

        var iterator = set.blocks_released.iterator(.{});
        while (iterator.next()) |block| {
            assert(set.blocks_released.isSet(block));
            assert(set.blocks_acquired.isSet(block));

            set.blocks_released.unset(block);
            set.blocks_acquired.unset(block);
        }
        assert(set.blocks_released.count() == 0);
    }
};
