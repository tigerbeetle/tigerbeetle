const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("stdx");
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
    comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
    comptime hash_from_key: fn (Key) callconv(.@"inline") u64,
    comptime tombstone: fn (*const Value) callconv(.@"inline") bool,
) type {
    return struct {
        const CacheMap = @This();

        const map_load_percentage_max = 50;

        pub const Cache = SetAssociativeCacheType(
            Key,
            Value,
            key_from_value,
            hash_from_key,
            .{},
        );

        pub const Options = struct {
            cache_value_count_max: u32,
            scope_value_count_max: u32,
            name: []const u8,
        };

        const RollbackLogAction = union(enum) {
            /// The operation updated or deleted a value
            /// that needs to be restored on rollback.
            restore: Value,
            /// The operation inserted a value over a _tombstone_
            /// that needs to be restored on rollback.
            restore_tombstone: Key,
            /// The operation inserted a value that did not previously exist
            /// and must be removed on rollback.
            remove: Key,
        };
        const RollbackLog = std.ArrayListUnmanaged(RollbackLogAction);

        // The hierarchy for lookups is cache (if present) -> immutable table -> lsm.
        // Lower levels _may_ have stale values, provided the correct value exists
        // in one of the levels above.
        // Evictions from the cache first flow into stash, with `.compact()` clearing it.
        // When cache is null, the stash mirrors the mutable table.
        cache: Cache,

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them.
        scope_is_active: bool = false,
        scope_rollback_log: RollbackLog,

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !CacheMap {
            assert(options.cache_value_count_max > 0);
            maybe(options.scope_value_count_max == 0);

            var cache: Cache = try .init(
                allocator,
                options.cache_value_count_max,
                .{ .name = options.name },
            );
            errdefer cache.deinit(allocator);

            var scope_rollback_log: RollbackLog = try .initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return CacheMap{
                .cache = cache,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        pub fn deinit(self: *CacheMap, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);

            self.scope_rollback_log.deinit(allocator);
            self.cache.deinit(allocator);
        }

        pub fn reset(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);

            self.cache.reset();
            self.* = .{
                .cache = self.cache,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub fn has(self: *const CacheMap, key: Key) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *const CacheMap, key: Key) ?*Value {
            if (self.cache.get(key)) |object| {
                // Deleted keys are represented as tombstones.
                return if (tombstone(object)) null else object;
            }
            return null;
        }

        pub fn cache_entries(self: *const CacheMap) u64 {
            return self.cache.metrics.value_count;
        }

        pub fn cache_entries_max(self: *const CacheMap) u64 {
            return self.options.cache_value_count_max;
        }

        pub fn upsert(self: *CacheMap, value: *const Value) void {
            const updated = self.fetch_upsert(value);

            // When upserting into a scope:
            if (self.scope_is_active) {
                const rollback_action: RollbackLogAction = rollback_action: {
                    const old_value = updated orelse
                        break :rollback_action .{ .remove = key_from_value(value) };

                    if (tombstone(&old_value)) {
                        // Only unit tests and fuzzers call `remove`.
                        // Tombstones should never be present in production code.
                        assert(constants.verify);
                        break :rollback_action .{ .restore_tombstone = key_from_value(value) };
                    }

                    break :rollback_action .{ .restore = old_value };
                };
                self.scope_rollback_log.appendAssumeCapacity(rollback_action);
            }
        }

        // Upserts the cache and returns the old value in case of
        // an update.
        fn fetch_upsert(self: *CacheMap, value: *const Value) ?Value {
            const key = key_from_value(value);
            const result = self.cache.upsert(value);

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
                    },
                }
            } else {
                // It must be an insert without eviction,
                // since updates always evict the old version.
                assert(result.updated == .insert);
            }

            return null;
        }

        /// Removes a key from cache, adding a tombstone to record the action.
        /// Invariant: The key must be present in cache. // TODO: really?
        pub fn remove(self: *CacheMap, key: Key) void {
            // Only unit tests and fuzzers call this function.
            // Assert that it is not called from production code.
            comptime assert(constants.verify);

            const cache_removed = self.cache.remove(key).?;
            assert(key_from_value(&cache_removed) == key);

            // Cannot remove a value that has already been removed.
            assert(!tombstone(&cache_removed));
            if (self.scope_is_active) {
                self.scope_rollback_log.appendAssumeCapacity(.{
                    .restore = cache_removed,
                });
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
            maybe(self.scope_rollback_log.items.len > 0);
            defer assert(self.scope_rollback_log.items.len == 0);

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

                switch (self.scope_rollback_log.items[i]) {
                    .restore => |*rollback_value| {
                        // Reverting an update or delete
                        // consists of an insert of the original value.
                        assert(!tombstone(rollback_value));
                        self.upsert(rollback_value);
                    },
                    .restore_tombstone => |key| {
                        // Reverting an insert that overwrote a tombstone
                        // consists of restoring the tombstone to the stash.
                        if (constants.verify)
                            // Only unit tests and fuzzers call `remove`.
                            self.remove(key)
                        else
                            unreachable;
                    },
                    .remove => |key| {
                        // A tombstone in the rollback log can only occur when the value doesn't exist
                        // in the cache on insert.
                        const cache_removed: bool = self.cache.remove(key) != null;
                        assert(cache_removed);
                    },
                }
            }
            self.scope_rollback_log.clearRetainingCapacity();
        }

        pub fn compact(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
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
    TestTable.tombstone,
);

test "cache_map: unit" {
    const testing = std.testing;

    const allocator = testing.allocator;

    var cache_map = try TestCacheMap.init(allocator, .{
        .cache_value_count_max = TestCacheMap.Cache.value_count_max_multiple,
        .scope_value_count_max = 32,
        .name = "test map",
    });
    defer cache_map.deinit(allocator);

    try testing.expectEqual(@as(u64, 0), cache_map.cache_entries());
    try testing.expectEqual(
        @as(u64, cache_map.options.cache_value_count_max),
        cache_map.cache_entries_max(),
    );

    cache_map.upsert(&.{ .key = 1, .value = 1, .tombstone = false });
    try testing.expectEqual(@as(u64, 1), cache_map.cache_entries());
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 1, .tombstone = false },
        cache_map.get(1).?.*,
    );

    // Test scope persisting.
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

    // Test scope discard on updates.
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

    // Test scope discard on inserts.
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

    // Test scope discard on removes.
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
