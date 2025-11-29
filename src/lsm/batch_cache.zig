const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const fastrange = stdx.fastrange;

const SetAssociativeCacheType = @import("set_associative_cache.zig").SetAssociativeCacheType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

/// A batch-aware cache with two layers:
/// - A per-batch open-addressed hash table that never evicts within a batch.
/// - An optional eviction cache that carries hot items across batches.
///
/// At the start of a batch, the previous batch table is flushed into the eviction cache
/// (upserts or deletes) and then cleared. Lookups check the batch table first (including
/// tombstones to mask stale eviction entries) and fall back to the eviction cache.
pub fn BatchCacheType(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
    comptime hash_from_key: fn (Key) callconv(.@"inline") u64,
    comptime tombstone_from_key: fn (Key) callconv(.@"inline") Value,
    comptime tombstone: fn (*const Value) callconv(.@"inline") bool,
) type {
    return struct {
        const BatchCache = @This();

        pub const EvictionCache = SetAssociativeCacheType(
            Key,
            Value,
            key_from_value,
            hash_from_key,
            .{},
        );
        pub const Cache = EvictionCache;

        pub const Options = struct {
            eviction_value_count_max: u32,
            batch_value_count_max: u32,
            scope_value_count_max: u32,
            name: []const u8,
        };

        batch_table: BatchTable,
        eviction: ?EvictionCache,

        scope_is_active: bool = false,
        scope_rollback_log: std.ArrayListUnmanaged(Value),

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !BatchCache {
            assert(options.batch_value_count_max > 0);
            maybe(options.eviction_value_count_max == 0);
            maybe(options.scope_value_count_max == 0);

            var eviction: ?EvictionCache = if (options.eviction_value_count_max == 0) null else try EvictionCache.init(
                allocator,
                options.eviction_value_count_max,
                .{ .name = options.name },
            );
            errdefer if (eviction) |*e| e.deinit(allocator);

            var batch_table = try BatchTable.init(allocator, options.batch_value_count_max);
            errdefer batch_table.deinit(allocator);

            var scope_rollback_log = try std.ArrayListUnmanaged(Value).initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            var self = BatchCache{
                .batch_table = batch_table,
                .eviction = eviction,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };

            // Start with an empty batch.
            self.begin_batch();

            return self;
        }

        pub fn deinit(self: *BatchCache, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.batch_table.count <= self.options.batch_value_count_max);

            self.scope_rollback_log.deinit(allocator);
            self.batch_table.deinit(allocator);
            if (self.eviction) |*cache| cache.deinit(allocator);
        }

        pub fn reset(self: *BatchCache) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.batch_table.count <= self.options.batch_value_count_max);

            if (self.eviction) |*cache| cache.reset();
            self.batch_table.reset();

            self.* = .{
                .batch_table = self.batch_table,
                .eviction = self.eviction,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        /// Begins a new batch:
        /// - flushes the previous batch table into the eviction cache (if any),
        /// - clears the batch table,
        /// - clears any pending rollback log.
        pub fn begin_batch(self: *BatchCache) void {
            assert(!self.scope_is_active);
            if (self.eviction) |*cache| self.batch_table.flushToEviction(cache);

            // Clear batch table and rollback log.
            self.batch_table.begin_batch();
            self.scope_rollback_log.clearRetainingCapacity();
        }

        pub fn has(self: *const BatchCache, key: Key) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *const BatchCache, key: Key) ?*Value {
            if (self.batch_table.get(key)) |entry|
                return if (tombstone(entry)) null else entry;

            return if (self.eviction) |cache| cache.get(key) else null;
        }

        pub fn upsert(self: *BatchCache, value: *const Value) void {
            const old_value = self.batch_upsert(value);

            if (self.scope_is_active) {
                if (old_value) |old| {
                    self.scope_rollback_log.appendAssumeCapacity(old);
                } else {
                    const key = key_from_value(value);
                    const key_tombstone = tombstone_from_key(key);
                    self.scope_rollback_log.appendAssumeCapacity(key_tombstone);
                }
            }
        }

        fn batch_upsert(self: *BatchCache, value: *const Value) ?Value {
            assert(self.batch_table.count < self.options.batch_value_count_max);
            return self.batch_table.upsert(value);
        }

        pub fn remove(self: *BatchCache, key: Key) void {
            comptime assert(constants.verify);

            const tomb = tombstone_from_key(key);
            const old_value = self.batch_upsert(&tomb);

            if (self.scope_is_active) {
                if (old_value) |old| {
                    self.scope_rollback_log.appendAssumeCapacity(old);
                } else {
                    self.scope_rollback_log.appendAssumeCapacity(tomb);
                }
            }
        }

        pub fn scope_open(self: *BatchCache) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            self.scope_is_active = true;
        }

        pub fn scope_close(self: *BatchCache, mode: ScopeCloseMode) void {
            assert(self.scope_is_active);
            self.scope_is_active = false;

            if (mode == .persist) {
                self.scope_rollback_log.clearRetainingCapacity();
                return;
            }

            var i: usize = self.scope_rollback_log.items.len;
            while (i > 0) {
                i -= 1;

                const rollback_value = &self.scope_rollback_log.items[i];
                if (tombstone(rollback_value)) {
                    // The value didn't exist before this scope, so remove our change.
                    const key = key_from_value(rollback_value);
                    _ = self.batch_table.remove(key);
                } else {
                    // Restore the previous value; ignore the prior state.
                    _ = self.batch_upsert(rollback_value);
                }
            }

            self.scope_rollback_log.clearRetainingCapacity();
        }

        /// Currently a no-op; the batch table is already bounded and cleared per batch.
        pub fn compact(self: *BatchCache) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.batch_table.count <= self.options.batch_value_count_max);
        }

        const BatchTable = struct {
            const Slot = struct {
                value: Value = undefined,
                generation: u32 = 0,
            };

            slots: []Slot,
            capacity: u32,
            count: u32 = 0,
            current_generation: u32 = 1,
            indices: std.ArrayListUnmanaged(u32),

            fn init(allocator: std.mem.Allocator, value_count_max: u32) !BatchTable {
                // Keep load factor <= 0.5 for short probes.
                const capacity_hint: u64 = @as(u64, value_count_max) * 2;
                const capacity_pow2: u64 = try std.math.ceilPowerOfTwo(u64, capacity_hint);
                assert(capacity_pow2 <= std.math.maxInt(u32));
                const capacity: u32 = @intCast(capacity_pow2);

                const slots = try allocator.alloc(Slot, capacity);
                @memset(slots, .{});

                const indices = try std.ArrayListUnmanaged(u32).initCapacity(allocator, value_count_max);

                return .{
                    .slots = slots,
                    .capacity = capacity,
                    .indices = indices,
                };
            }

            fn deinit(self: *BatchTable, allocator: std.mem.Allocator) void {
                self.indices.deinit(allocator);
                allocator.free(self.slots);
            }

            fn reset(self: *BatchTable) void {
                @memset(self.slots, .{});
                self.count = 0;
                self.current_generation = 1;
                self.indices.clearRetainingCapacity();
            }

            fn begin_batch(self: *BatchTable) void {
                self.count = 0;
                self.indices.clearRetainingCapacity();
                self.current_generation +%= 1;
                if (self.current_generation == 0) {
                    // Wrapped: clear generations.
                    self.reset();
                }
            }

            fn upsert(self: *BatchTable, value: *const Value) ?Value {
                const key = key_from_value(value);
                var index: u32 = @intCast(fastrange(hash_from_key(key), self.capacity));
                const mask = self.capacity - 1;

                var probe: u32 = 0;
                var first_free: ?u32 = null;
                while (probe < self.capacity) : (probe += 1) {
                    const slot = &self.slots[index];
                    if (slot.generation != self.current_generation) {
                        if (first_free == null) first_free = index;
                        break;
                    }

                    if (key_from_value(&slot.value) == key) {
                        const old = slot.value;
                        slot.value = value.*;
                        return old;
                    }

                    index = (index + 1) & mask;
                }

                const dest_index = first_free orelse index;
                self.slots[dest_index] = .{
                    .value = value.*,
                    .generation = self.current_generation,
                };
                self.count +%= 1;
                self.indices.appendAssumeCapacity(dest_index);
                return null;
            }

            fn get(self: *const BatchTable, key: Key) ?*Value {
                var index: u32 = @intCast(fastrange(hash_from_key(key), self.capacity));
                const mask = self.capacity - 1;

                var probe: u32 = 0;
                while (probe < self.capacity) : (probe += 1) {
                    const slot = &self.slots[index];
                    if (slot.generation != self.current_generation) return null;
                    if (key_from_value(&slot.value) == key) return &slot.value;
                    index = (index + 1) & mask;
                }
                return null;
            }

            fn remove(self: *BatchTable, key: Key) ?Value {
                var index: u32 = @intCast(fastrange(hash_from_key(key), self.capacity));
                const mask = self.capacity - 1;

                var probe: u32 = 0;
                while (probe < self.capacity) : (probe += 1) {
                    const slot = &self.slots[index];
                    if (slot.generation != self.current_generation) return null;
                    if (key_from_value(&slot.value) == key) {
                        const old = slot.value;
                        slot.value = tombstone_from_key(key);
                        return old;
                    }
                    index = (index + 1) & mask;
                }
                return null;
            }

            fn flushToEviction(self: *BatchTable, eviction: *EvictionCache) void {
                for (self.indices.items) |slot_index| {
                    const slot = &self.slots[slot_index];
                    if (slot.generation != self.current_generation) continue;
                    const key = key_from_value(&slot.value);
                    if (tombstone(&slot.value)) {
                        _ = eviction.remove(key);
                    } else {
                        _ = eviction.upsert(&slot.value);
                    }
                }
            }
        };
    };
}

pub const TestTable = struct {
    pub const Key = u32;
    pub const Value = struct {
        key: Key,
        value: u32,
        tombstone: bool = false,
        padding: [7]u8 = undefined,
    };

    pub inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }

    pub inline fn compare_keys(a: Key, b: Key) std.math.Order {
        return std.math.order(a, b);
    }

    pub inline fn tombstone_from_key(a: Key) Value {
        return Value{ .key = a, .value = 0, .tombstone = true };
    }

    pub inline fn tombstone(a: *const TestTable.Value) bool {
        return a.tombstone;
    }

    pub inline fn hash(key: TestTable.Key) u64 {
        return stdx.hash_inline(key);
    }
};

pub const TestBatchCache = BatchCacheType(
    TestTable.Key,
    TestTable.Value,
    TestTable.key_from_value,
    TestTable.hash,
    TestTable.tombstone_from_key,
    TestTable.tombstone,
);

test "batch_cache: unit" {
    const testing = std.testing;

    const allocator = testing.allocator;

    var cache = try TestBatchCache.init(allocator, .{
        .eviction_value_count_max = 32,
        .batch_value_count_max = 32,
        .scope_value_count_max = 32,
        .name = "test batch cache",
    });
    defer cache.deinit(allocator);

    // Batch start is performed in init, so just operate.
    cache.upsert(&.{ .key = 1, .value = 10, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 10, .tombstone = false },
        cache.get(1).?.*,
    );

    // Scope discard on insert.
    cache.scope_open();
    cache.upsert(&.{ .key = 2, .value = 20, .tombstone = false });
    try testing.expect(cache.has(2));
    cache.scope_close(.discard);
    try testing.expect(!cache.has(2));

    // Scope discard on update.
    cache.scope_open();
    cache.upsert(&.{ .key = 1, .value = 11, .tombstone = false });
    try testing.expectEqual(@as(u32, 11), cache.get(1).?.value);
    cache.scope_close(.discard);
    try testing.expectEqual(@as(u32, 10), cache.get(1).?.value);

    // Batch transition flushes to eviction and clears batch map.
    cache.begin_batch();
    // The batch map is empty, but the eviction cache should still have the value.
    cache.scope_open();
    try testing.expectEqual(@as(u32, 10), cache.get(1).?.value);
    cache.scope_close(.persist);
}
