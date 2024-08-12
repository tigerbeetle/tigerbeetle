const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const SetAssociativeCacheType = @import("set_associative_cache.zig").SetAssociativeCacheType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

pub fn ObjectHelperType(
    comptime Object: type,
) type {
    return struct {
        pub const tombstone_bit = 1 << (64 - 1);
        pub const primary_key: enum { id, timestamp } = primary_key: {
            assert(@hasField(Object, "timestamp"));
            assert(std.meta.FieldType(Object, .timestamp) == u64);

            if (@hasField(Object, "id")) {
                assert(std.meta.FieldType(Object, .id) == u128);
                break :primary_key .id;
            }

            break :primary_key .timestamp;
        };

        pub const PrimaryKey = switch (primary_key) {
            .id => u128,
            .timestamp => u64,
        };

        pub inline fn key_from_value(value: *const Object) PrimaryKey {
            return switch (primary_key) {
                .id => value.id,
                .timestamp => value.timestamp & ~@as(u64, tombstone_bit),
            };
        }

        pub inline fn hash(key: PrimaryKey) u64 {
            if (primary_key == .timestamp) assert((key & tombstone_bit) == 0);
            return stdx.hash_inline(key);
        }

        pub inline fn tombstone_from_key(a: PrimaryKey) Object {
            return switch (primary_key) {
                .id => std.mem.zeroInit(Object, .{
                    .id = a,
                    .timestamp = tombstone_bit,
                }),
                .timestamp => std.mem.zeroInit(Object, .{
                    .timestamp = a | tombstone_bit,
                }),
            };
        }

        pub inline fn tombstone(a: *const Object) bool {
            return (a.timestamp & tombstone_bit) != 0;
        }
    };
}

/// A ObjectCacheType is a hybrid between our SetAssociativeCache and a HashMap (stash). The
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
/// Also keeps an indirect lookup by `timestamp`.
///
/// Cache invalidation for the stash is handled by `compact`.
pub fn CacheMapType(
    comptime Object: type,
) type {
    return struct {
        const CacheMap = @This();
        const ObjectHelper = ObjectHelperType(Object);

        const map_load_percentage_max = 50;

        pub const Cache = SetAssociativeCacheType(
            ObjectHelper.PrimaryKey,
            Object,
            ObjectHelper.key_from_value,
            ObjectHelper.hash,
            .{},
        );

        pub const Map = std.HashMapUnmanaged(
            Object,
            void,
            struct {
                pub inline fn eql(_: @This(), a: Object, b: Object) bool {
                    return ObjectHelper.key_from_value(&a) == ObjectHelper.key_from_value(&b);
                }

                pub inline fn hash(_: @This(), value: Object) u64 {
                    return ObjectHelper.hash(ObjectHelper.key_from_value(&value));
                }
            },
            map_load_percentage_max,
        );

        pub const TimestampMap = std.HashMapUnmanaged(
            u64,
            ObjectHelper.PrimaryKey,
            struct {
                pub inline fn eql(_: @This(), a: u64, b: u64) bool {
                    return a == b;
                }

                pub inline fn hash(_: @This(), timestamp: u64) u64 {
                    return stdx.hash_inline(timestamp);
                }
            },
            map_load_percentage_max,
        );

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
        cache_timestamp: switch (ObjectHelper.primary_key) {
            .id => ?TimestampMap,
            .timestamp => void,
        },

        stash: Map,
        stash_timestamp: switch (ObjectHelper.primary_key) {
            .id => TimestampMap,
            .timestamp => void,
        },

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them.
        scope_is_active: bool = false,
        scope_rollback_log: std.ArrayListUnmanaged(Object),

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

            var cache_timestamp: switch (ObjectHelper.primary_key) {
                .id => ?TimestampMap,
                .timestamp => void,
            } = switch (ObjectHelper.primary_key) {
                .id => if (options.cache_value_count_max == 0)
                    null
                else cache_timestamp: {
                    var local: TimestampMap = .{};
                    try local.ensureTotalCapacity(allocator, options.cache_value_count_max);
                    break :cache_timestamp local;
                },
                .timestamp => {},
            };
            errdefer if (ObjectHelper.primary_key == .id) {
                if (cache_timestamp) |*cache_timestamp_unwrapped| {
                    cache_timestamp_unwrapped.deinit(allocator);
                }
            };

            var stash: Map = .{};
            try stash.ensureTotalCapacity(allocator, options.map_value_count_max);
            errdefer stash.deinit(allocator);

            var stash_timestamp: switch (ObjectHelper.primary_key) {
                .id => TimestampMap,
                .timestamp => void,
            } = switch (ObjectHelper.primary_key) {
                .id => stash_timestamp: {
                    var local: TimestampMap = .{};
                    try local.ensureTotalCapacity(allocator, options.map_value_count_max);
                    break :stash_timestamp local;
                },
                .timestamp => {},
            };
            errdefer if (ObjectHelper.primary_key == .id) {
                stash_timestamp.deinit(allocator);
            };

            var scope_rollback_log = try std.ArrayListUnmanaged(Object).initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return CacheMap{
                .cache = cache,
                .cache_timestamp = cache_timestamp,
                .stash = stash,
                .stash_timestamp = stash_timestamp,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        pub fn deinit(self: *CacheMap, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.map_value_count_max);
            if (ObjectHelper.primary_key == .id) {
                assert(self.stash.count() == self.stash_timestamp.count());
            }

            self.scope_rollback_log.deinit(allocator);
            self.stash.deinit(allocator);
            if (self.cache) |*cache| cache.deinit(allocator);

            if (ObjectHelper.primary_key == .id) {
                if (self.cache_timestamp) |*cache_timestamp| {
                    cache_timestamp.deinit(allocator);
                }
                self.stash_timestamp.deinit(allocator);
            }
        }

        pub fn reset(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.map_value_count_max);
            if (ObjectHelper.primary_key == .id) {
                assert(self.stash.count() == self.stash_timestamp.count());
            }

            if (self.cache) |*cache| cache.reset();
            self.stash.clearRetainingCapacity();

            if (ObjectHelper.primary_key == .id) {
                if (self.cache_timestamp) |*cache_timestamp| {
                    cache_timestamp.clearRetainingCapacity();
                }
                self.stash_timestamp.clearRetainingCapacity();
            }

            self.* = .{
                .cache = self.cache,
                .cache_timestamp = self.cache_timestamp,
                .stash = self.stash,
                .stash_timestamp = self.stash_timestamp,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub fn has(self: *const CacheMap, key: ObjectHelper.PrimaryKey) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *const CacheMap, key: ObjectHelper.PrimaryKey) ?*Object {
            return (if (self.cache) |*cache| cache.get(key) else null) orelse
                self.stash.getKeyPtr(ObjectHelper.tombstone_from_key(key));
        }

        pub fn key_from_timestamp(self: *const CacheMap, timestamp: u64) ?ObjectHelper.PrimaryKey {
            return switch (ObjectHelper.primary_key) {
                .id => (if (self.cache_timestamp) |*cache_timestamp|
                    cache_timestamp.get(timestamp)
                else
                    null) orelse
                    self.stash_timestamp.get(timestamp),
                .timestamp => timestamp,
            };
        }

        pub fn upsert(self: *CacheMap, value: *const Object) void {
            const old_value_maybe = self.fetch_upsert(value);

            // When upserting into a scope:
            if (self.scope_is_active) {
                if (old_value_maybe) |old_value| {
                    // If it was updated, append the old value to the scope rollback log.
                    self.scope_rollback_log.appendAssumeCapacity(old_value);
                } else {
                    // If it was an insert, append a tombstone to the scope rollback log.
                    const key = ObjectHelper.key_from_value(value);
                    self.scope_rollback_log.appendAssumeCapacity(
                        ObjectHelper.tombstone_from_key(key),
                    );
                }
            }
        }

        // Upserts the cache and stash and returns the old value in case of
        // an update.
        fn fetch_upsert(self: *CacheMap, value: *const Object) ?Object {
            if (self.cache) |*cache| {
                const key = ObjectHelper.key_from_value(value);
                const result = cache.upsert(value);

                if (result.evicted) |*evicted| {
                    switch (result.updated) {
                        .update => {
                            assert(ObjectHelper.key_from_value(evicted) == key);
                            if (ObjectHelper.primary_key == .id) {
                                assert(self.cache_timestamp != null);
                                assert(self.cache_timestamp.?.contains(value.timestamp));
                            }

                            // There was an eviction because an item was updated,
                            // the evicted item is always its previous version.
                            return evicted.*;
                        },
                        .insert => {
                            assert(ObjectHelper.key_from_value(evicted) != key);
                            if (ObjectHelper.primary_key == .id) {
                                assert(self.cache_timestamp != null);
                                const removed = self.cache_timestamp.?.remove(value.timestamp);
                                assert(removed);
                            }

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
                    if (ObjectHelper.primary_key == .id) {
                        assert(self.cache_timestamp != null);
                        self.cache_timestamp.?.putAssumeCapacityNoClobber(
                            value.timestamp,
                            value.id,
                        );
                    }
                }

                // The stash may have the old value if nothing was evicted.
                return self.stash_remove(key);
            } else {
                // No cache.
                // Upserting the stash directly.
                return self.stash_upsert(value);
            }
        }

        fn stash_upsert(self: *CacheMap, value: *const Object) ?Object {
            // Using `getOrPutAssumeCapacity` instead of `putAssumeCapacity` is
            // critical, since we use HashMaps with no Value, `putAssumeCapacity`
            // _will not_ clobber the existing value.
            const gop = self.stash.getOrPutAssumeCapacity(value.*);
            defer gop.key_ptr.* = value.*;

            if (gop.found_existing) {
                if (ObjectHelper.primary_key == .id) {
                    assert(self.stash_timestamp.contains(value.timestamp));
                }
                return gop.key_ptr.*;
            } else {
                if (ObjectHelper.primary_key == .id) {
                    self.stash_timestamp.putAssumeCapacityNoClobber(value.timestamp, value.id);
                }
                return null;
            }
        }

        pub fn remove(self: *CacheMap, key: ObjectHelper.PrimaryKey) void {
            // The only thing that tests this in any depth is the cache_map fuzz itself.
            // Make sure we aren't being called in regular code without another once over.
            assert(constants.verify);

            // We don't allow stale values, so we need to remove from the stash as well,
            // since both can have different versions with the same key.
            const cache_removed: ?Object = self.cache_remove(key);
            const stash_removed: ?Object = self.stash_remove(key);

            if (self.scope_is_active) {
                // TODO: Actually, does the fuzz catch this...
                self.scope_rollback_log.appendAssumeCapacity(
                    cache_removed orelse
                        stash_removed orelse return,
                );
            }
        }

        fn cache_remove(self: *CacheMap, key: ObjectHelper.PrimaryKey) ?Object {
            const cache_removed: ?Object = if (self.cache) |*cache|
                cache.remove(key)
            else
                null;
            if (ObjectHelper.primary_key == .id) {
                if (cache_removed) |value| {
                    assert(self.cache_timestamp != null);
                    const removed = self.cache_timestamp.?.remove(value.timestamp);
                    assert(removed);
                }
            }

            return cache_removed;
        }

        fn stash_remove(self: *CacheMap, key: ObjectHelper.PrimaryKey) ?Object {
            if (self.stash.fetchRemove(ObjectHelper.tombstone_from_key(key))) |kv| {
                if (ObjectHelper.primary_key == .id) {
                    const removed = self.stash_timestamp.remove(kv.key.timestamp);
                    assert(removed);
                }
                return kv.key;
            }

            return null;
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
                if (ObjectHelper.tombstone(rollback_value)) {
                    // Reverting an insert consists of a .remove call.
                    // The value in here will be a tombstone indicating the original value didn't
                    // exist.
                    const key = ObjectHelper.key_from_value(rollback_value);

                    // A tombstone in the rollback log can only occur when the value doesn't exist
                    // in _both_ the cache and stash on insert.
                    // If we have cache enabled, it must be there.
                    const cache_removed = self.cache_remove(key) != null;
                    assert(cache_removed == (self.cache != null));

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
            if (ObjectHelper.primary_key == .id) {
                assert(self.stash_timestamp.count() == self.stash.count());
                self.stash_timestamp.clearRetainingCapacity();
            }

            self.stash.clearRetainingCapacity();
        }
    };
}

pub const TestObject = struct {
    id: u128,
    timestamp: u64,
    value: u32,
    padding: [4]u8 = undefined,
};

pub const TestCacheMap = CacheMapType(TestObject);

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

    cache_map.upsert(&.{ .id = 1, .timestamp = 100, .value = 1 });
    try testing.expectEqual(
        TestObject{ .id = 1, .timestamp = 100, .value = 1 },
        cache_map.get(1).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 1, .timestamp = 100, .value = 1 },
        cache_map.get(cache_map.key_from_timestamp(100).?).?.*,
    );

    // Test scope persisting.
    cache_map.scope_open();
    cache_map.upsert(&.{ .id = 2, .timestamp = 200, .value = 2 });
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(2).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(cache_map.key_from_timestamp(200).?).?.*,
    );
    cache_map.scope_close(.persist);
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(2).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(cache_map.key_from_timestamp(200).?).?.*,
    );

    // Test scope discard on updates.
    cache_map.scope_open();
    cache_map.upsert(&.{ .id = 2, .timestamp = 200, .value = 22 });
    cache_map.upsert(&.{ .id = 2, .timestamp = 200, .value = 222 });
    cache_map.upsert(&.{ .id = 2, .timestamp = 200, .value = 2222 });
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2222 },
        cache_map.get(2).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2222 },
        cache_map.get(cache_map.key_from_timestamp(200).?).?.*,
    );
    cache_map.scope_close(.discard);
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(2).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(cache_map.key_from_timestamp(200).?).?.*,
    );

    // Test scope discard on inserts.
    cache_map.scope_open();
    cache_map.upsert(&.{ .id = 3, .timestamp = 300, .value = 3 });
    try testing.expectEqual(
        TestObject{ .id = 3, .timestamp = 300, .value = 3 },
        cache_map.get(3).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 3, .timestamp = 300, .value = 3 },
        cache_map.get(cache_map.key_from_timestamp(300).?).?.*,
    );
    cache_map.upsert(&.{ .id = 3, .timestamp = 300, .value = 33 });
    try testing.expectEqual(
        TestObject{ .id = 3, .timestamp = 300, .value = 33 },
        cache_map.get(3).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 3, .timestamp = 300, .value = 33 },
        cache_map.get(cache_map.key_from_timestamp(300).?).?.*,
    );
    cache_map.scope_close(.discard);
    assert(!cache_map.has(3));
    assert(cache_map.key_from_timestamp(300) == null);

    // Test scope discard on removes.
    cache_map.scope_open();
    cache_map.remove(2);
    assert(cache_map.get(2) == null);
    assert(cache_map.key_from_timestamp(200) == null);
    cache_map.scope_close(.discard);
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(2).?.*,
    );
    try testing.expectEqual(
        TestObject{ .id = 2, .timestamp = 200, .value = 2 },
        cache_map.get(cache_map.key_from_timestamp(200).?).?.*,
    );
}
