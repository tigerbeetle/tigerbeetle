const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const SetAssociativeCacheType = @import("set_associative_cache.zig").SetAssociativeCacheType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

/// A CacheMap is a hybrid between our SetAssociativeCache and a HashMap (stash). The
/// SetAssociativeCache sits on top and absorbs the majority of get / put requests. Below that,
/// lives a HashMap. Should an insert() cause an eviction (which can happen either because the Key
/// is the same, or because our Way is full), the evicted value is caught and put in the stash.
///
/// This allows for a potentially huge cache, with all the advantages of CLOCK Nth-Chance, while
/// still being able to give hard guarantees that values will be present. The stash will often be
/// significantly smaller, as the amount of values we're required to guarantee is less than what
/// we'd like to optimistically keep in memory.
///
/// Within our LSM, the CacheMap is the backing for the combined Groove prefetch + cache. The cache
/// part fills the use case of an object cache, while the stash ensures that prefetched values
/// are available in memory during their respective commit.
///
/// Cache invalidation for the stash is handled by `compact`.
pub fn CacheMapType(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime hash_from_key: fn (Key) callconv(.Inline) u64,
    comptime tombstone_from_key: fn (Key) callconv(.Inline) Value,
    comptime tombstone: fn (*const Value) callconv(.Inline) bool,
) type {
    return struct {
        const CacheMap = @This();

        const map_load_percentage_max = 50;
        const has_timestamp = @hasField(Value, "timestamp") and @hasField(Value, "id");

        pub const Cache = SetAssociativeCacheType(
            Key,
            Value,
            key_from_value,
            hash_from_key,
            .{},
        );

        pub const Map = std.HashMapUnmanaged(
            Value,
            void,
            struct {
                pub inline fn eql(_: @This(), a: Value, b: Value) bool {
                    return key_from_value(&a) == key_from_value(&b);
                }

                pub inline fn hash(_: @This(), value: Value) u64 {
                    return stdx.hash_inline(key_from_value(&value));
                }
            },
            map_load_percentage_max,
        );

        pub const MapByTimestamp = if (has_timestamp) std.AutoHashMapUnmanaged(
            u64,
            *Value,
        ) else void;

        pub const Options = struct {
            cache_value_count_max: u32,
            map_value_count_max: u32,
            scope_value_count_max: u32,
            name: []const u8,
        };

        // The hierarchy for lookups is cache (if present) -> stash -> immutable table -> lsm.
        // Lower levels _may_ have stale values, provided the correct value exists
        // in one of the levels above.
        // Evictions from the cache first flow into stash, with `.compact()` clearing it.
        // When cache is null, the stash mirrors the mutable table.
        cache: ?Cache,
        stash: Map,
        map_by_timestamp: MapByTimestamp,

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them.
        scope_is_active: bool = false,
        scope_rollback_log: std.ArrayListUnmanaged(Value),

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !CacheMap {
            assert(options.map_value_count_max > 0);
            maybe(options.cache_value_count_max == 0);
            maybe(options.scope_value_count_max == 0);

            var cache: ?Cache = if (options.cache_value_count_max == 0) null else try Cache.init(
                allocator,
                options.cache_value_count_max,
                .{ .name = options.name },
            );
            errdefer if (cache) |*cache_unwrapped| cache_unwrapped.deinit(allocator);

            var stash: Map = .{};
            try stash.ensureTotalCapacity(allocator, options.map_value_count_max);
            errdefer stash.deinit(allocator);

            var map_by_timestamp: MapByTimestamp =
                if (has_timestamp) .{} else {};
            if (has_timestamp) {
                try map_by_timestamp.ensureTotalCapacity(
                    allocator,
                    options.map_value_count_max + options.cache_value_count_max,
                );
            }
            errdefer if (has_timestamp) map_by_timestamp.deinit(allocator);

            var scope_rollback_log = try std.ArrayListUnmanaged(Value).initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return CacheMap{
                .cache = cache,
                .stash = stash,
                .map_by_timestamp = map_by_timestamp,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        pub fn deinit(self: *CacheMap, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.map_value_count_max);
            if (has_timestamp) {
                assert(self.map_by_timestamp.count() <=
                    self.options.map_value_count_max + self.options.cache_value_count_max);
            }

            self.scope_rollback_log.deinit(allocator);
            self.stash.deinit(allocator);
            if (has_timestamp) {
                self.map_by_timestamp.deinit(allocator);
            }
            if (self.cache) |*cache| cache.deinit(allocator);
        }

        pub fn reset(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.map_value_count_max);
            if (has_timestamp) {
                assert(self.map_by_timestamp.count() <=
                    self.options.map_value_count_max + self.options.cache_value_count_max);
            }

            if (self.cache) |*cache| cache.reset();
            self.stash.clearRetainingCapacity();
            if (has_timestamp) {
                self.map_by_timestamp.clearRetainingCapacity();
            }

            self.* = .{
                .cache = self.cache,
                .stash = self.stash,
                .map_by_timestamp = self.map_by_timestamp,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub fn has(self: *const CacheMap, key: Key) bool {
            return self.get(key) != null;
        }

        pub fn has_by_timestamp(self: *const CacheMap, timestamp: u64) bool {
            comptime assert(has_timestamp);
            return self.get_by_timestamp(timestamp) != null;
        }

        pub fn get(self: *const CacheMap, key: Key) ?*Value {
            return (if (self.cache) |*cache| cache.get(key) else null) orelse
                self.stash.getKeyPtr(tombstone_from_key(key));
        }

        pub fn get_by_timestamp(self: *const CacheMap, timestamp: u64) ?*Value {
            comptime assert(has_timestamp);
            return self.map_by_timestamp.get(timestamp);
        }

        pub fn upsert(self: *CacheMap, value: *const Value) void {
            const old_value_maybe = self.fetch_upsert(value);

            // When upserting into a scope:
            if (self.scope_is_active) {
                if (old_value_maybe) |old_value| {
                    // If it was updated, append the old value to the scope rollback log.
                    self.scope_rollback_log.appendAssumeCapacity(old_value);
                } else {
                    // If it was an insert, append a tombstone to the scope rollback log.
                    const key = key_from_value(value);
                    self.scope_rollback_log.appendAssumeCapacity(
                        tombstone_from_key(key),
                    );
                }
            }
        }

        // Upserts the cache and stash and returns the old value in case of
        // an update.
        fn fetch_upsert(self: *CacheMap, value: *const Value) ?Value {
            if (self.cache) |*cache| {
                const key = key_from_value(value);
                const result = cache.upsert(value);
                if (has_timestamp) {
                    self.map_by_timestamp.putAssumeCapacity(value.timestamp, result.value);
                }

                if (result.evicted) |*evicted| {
                    switch (result.updated) {
                        .update => {
                            assert(key_from_value(evicted) == key);

                            // There was an eviction because an item was updated,
                            // the evicted item is always its previous version.
                            return evicted.*;
                        },
                        .insert => {
                            assert(key_from_value(evicted) != key);

                            // There was an eviction because a new item was inserted,
                            // the evicted item will be added to the stash.
                            const stash_updated = self.stash_upsert(evicted);

                            // We don't expect stale values on the stash.
                            assert(stash_updated == null);
                        },
                    }
                } else {
                    // It must be an insert without eviction,
                    // since updates always evict the old version.
                    assert(result.updated == .insert);
                }

                // The stash may have the old value if nothing was evicted.
                return self.stash_remove(key);
            } else {
                // No cache.
                // Upserting the stash directly.
                return self.stash_upsert(value);
            }
        }

        fn stash_upsert(self: *CacheMap, value: *const Value) ?Value {
            // Using `getOrPutAssumeCapacity` instead of `putAssumeCapacity` is
            // critical, since we use HashMaps with no Value, `putAssumeCapacity`
            // _will not_ clobber the existing value.
            const gop = self.stash.getOrPutAssumeCapacity(value.*);
            defer {
                gop.key_ptr.* = value.*;
                if (has_timestamp) {
                    self.map_by_timestamp.putAssumeCapacity(value.timestamp, gop.key_ptr);
                }
            }

            return if (gop.found_existing)
                gop.key_ptr.*
            else
                null;
        }

        pub fn remove(self: *CacheMap, key: Key) void {
            // The only thing that tests this in any depth is the cache_map fuzz itself.
            // Make sure we aren't being called in regular code without another once over.
            assert(constants.verify);

            const cache_removed: ?Value = if (self.cache) |*cache| blk: {
                const value = cache.remove(key);
                if (has_timestamp) {
                    if (value != null) {
                        const removed = self.map_by_timestamp.remove(value.?.timestamp);
                        assert(removed);
                    }
                }

                break :blk value;
            } else null;

            // We don't allow stale values, so we need to remove from the stash as well,
            // since both can have different versions with the same key.
            const stash_removed: ?Value = self.stash_remove(key);

            if (self.scope_is_active) {
                // TODO: Actually, does the fuzz catch this...
                self.scope_rollback_log.appendAssumeCapacity(
                    cache_removed orelse stash_removed orelse return,
                );
            }
        }

        fn stash_remove(self: *CacheMap, key: Key) ?Value {
            if (self.stash.fetchRemove(tombstone_from_key(key))) |kv| {
                if (has_timestamp) {
                    const removed = self.map_by_timestamp.remove(kv.key.timestamp);
                    maybe(removed);
                }

                return kv.key;
            } else {
                return null;
            }
        }

        /// Start a new scope. Within a scope, changes can be persisted
        /// or discarded. At most one scope can be active at a time.
        pub fn scope_open(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            self.scope_is_active = true;
        }

        pub fn scope_close(self: *CacheMap, mode: ScopeCloseMode) void {
            assert(self.scope_is_active);
            self.scope_is_active = false;

            // We don't need to do anything to persist a scope.
            if (mode == .persist) {
                self.scope_rollback_log.clearRetainingCapacity();
                return;
            }

            // The scope_rollback_log stores the operations we need to reverse the changes a scope
            // made. They get replayed in reverse order.
            var i: usize = self.scope_rollback_log.items.len;
            while (i > 0) {
                i -= 1;

                const rollback_value = &self.scope_rollback_log.items[i];
                if (tombstone(rollback_value)) {
                    // Reverting an insert consists of a .remove call.
                    // The value in here will be a tombstone indicating the original value didn't
                    // exist.
                    const key = key_from_value(rollback_value);

                    // A tombstone in the rollback log can only occur when the value doesn't exist
                    // in _both_ the cache and stash on insert.
                    if (self.cache) |*cache| {
                        // If we have cache enabled, it must be there.
                        const value = cache.remove(key);
                        assert(value != null);

                        if (has_timestamp) {
                            const timestamp_removed = self.map_by_timestamp.remove(value.?.timestamp);
                            assert(timestamp_removed);
                        }
                    }

                    // It should be in the stash _iif_ we don't have cache enabled.
                    const stash_removed = self.stash_remove(key) != null;
                    assert(stash_removed == (self.cache == null));
                } else {
                    // Reverting an update or delete consists of an insert of the original value.
                    self.upsert(rollback_value);
                }
            }

            self.scope_rollback_log.clearRetainingCapacity();
        }

        pub fn compact(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            maybe(self.stash.count() <= self.options.map_value_count_max);

            if (has_timestamp) {
                var it = self.stash.keyIterator();
                while (it.next()) |value| {
                    const removed = self.map_by_timestamp.remove(value.timestamp);
                    assert(removed);
                }
            }
            self.stash.clearRetainingCapacity();
        }
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

pub const TestCacheMap = CacheMapType(
    TestTable.Key,
    TestTable.Value,
    TestTable.key_from_value,
    TestTable.hash,
    TestTable.tombstone_from_key,
    TestTable.tombstone,
);

test "cache_map: unit" {
    const testing = std.testing;

    const allocator = testing.allocator;

    var cache_map = try TestCacheMap.init(allocator, .{
        .cache_value_count_max = TestCacheMap.Cache.value_count_max_multiple,
        .scope_value_count_max = 32,
        .map_value_count_max = 32,
        .name = "test map",
    });
    defer cache_map.deinit(allocator);

    cache_map.upsert(&.{ .key = 1, .value = 1, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 1, .tombstone = false },
        cache_map.get(1).?.*,
    );

    // Test scope persisting
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 2, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 2, .value = 2, .tombstone = false },
        cache_map.get(2).?.*,
    );
    cache_map.scope_close(.persist);
    try testing.expectEqual(
        TestTable.Value{ .key = 2, .value = 2, .tombstone = false },
        cache_map.get(2).?.*,
    );

    // Test scope discard on updates
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 22, .tombstone = false });
    cache_map.upsert(&.{ .key = 2, .value = 222, .tombstone = false });
    cache_map.upsert(&.{ .key = 2, .value = 2222, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 2, .value = 2222, .tombstone = false },
        cache_map.get(2).?.*,
    );
    cache_map.scope_close(.discard);
    try testing.expectEqual(
        TestTable.Value{ .key = 2, .value = 2, .tombstone = false },
        cache_map.get(2).?.*,
    );

    // Test scope discard on inserts
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 3, .value = 3, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 3, .value = 3, .tombstone = false },
        cache_map.get(3).?.*,
    );
    cache_map.upsert(&.{ .key = 3, .value = 33, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 3, .value = 33, .tombstone = false },
        cache_map.get(3).?.*,
    );
    cache_map.scope_close(.discard);
    assert(!cache_map.has(3));
    assert(cache_map.get(3) == null);

    // Test scope discard on removes
    cache_map.scope_open();
    cache_map.remove(2);
    assert(!cache_map.has(2));
    assert(cache_map.get(2) == null);
    cache_map.scope_close(.discard);
    try testing.expectEqual(
        TestTable.Value{ .key = 2, .value = 2, .tombstone = false },
        cache_map.get(2).?.*,
    );
}
