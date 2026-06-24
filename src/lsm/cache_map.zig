const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const SetAssociativeCacheType = @import("set_associative_cache.zig").SetAssociativeCacheType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

/// A CacheMap is a hybrid between our SetAssociativeCache and a HashMap (stash). The stash is the
/// authoritative layer for values needed by the current commit; the SetAssociativeCache is a
/// best-effort recency cache for values from previous commits.
///
/// This allows for a potentially huge cache, with all the advantages of CLOCK Nth-Chance, while
/// still being able to give hard guarantees that values prefetched into the stash will be present.
/// The stash will often be significantly smaller, as the amount of values we're required to
/// guarantee is less than what we'd like to optimistically keep in memory.
///
/// Within our LSM, the CacheMap is the backing for the combined Groove prefetch + cache. The cache
/// part fills the use case of an object cache, while the stash ensures that prefetched values
/// are available in memory during their respective commit.
///
/// `compact()` promotes the stash into the recency cache, then clears the stash.
pub fn CacheMapType(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
    comptime hash_from_key: fn (Key) callconv(.@"inline") u64,
    comptime tombstone_from_key: fn (Key) callconv(.@"inline") Value,
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

        pub const Options = struct {
            cache_value_count_max: u32,
            stash_value_count_max: u32,
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

        // The hierarchy for lookups is stash -> cache (if present) -> immutable table -> lsm.
        // Lower levels _may_ have stale values, provided the correct value exists in one of the
        // levels above. Cache evictions are discarded; only `compact()` promotes stash values into
        // the recency cache.
        // When cache is null, the stash mirrors the mutable table.
        cache: ?Cache,
        stash: Map,

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them.
        scope_is_active: bool = false,
        scope_rollback_log: RollbackLog,

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !CacheMap {
            assert(options.stash_value_count_max > 0);
            maybe(options.cache_value_count_max == 0);
            maybe(options.scope_value_count_max == 0);

            var cache: ?Cache = if (options.cache_value_count_max == 0) null else try Cache.init(
                allocator,
                options.cache_value_count_max,
                .{ .name = options.name },
            );
            errdefer if (cache) |*cache_unwrapped| cache_unwrapped.deinit(allocator);

            var stash: Map = .{};
            try stash.ensureTotalCapacity(allocator, options.stash_value_count_max);
            errdefer stash.deinit(allocator);

            var scope_rollback_log: RollbackLog = try .initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return CacheMap{
                .cache = cache,
                .stash = stash,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        pub fn deinit(self: *CacheMap, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.stash_value_count_max);

            self.scope_rollback_log.deinit(allocator);
            self.stash.deinit(allocator);
            if (self.cache) |*cache| cache.deinit(allocator);
        }

        pub fn reset(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.stash_value_count_max);

            if (self.cache) |*cache| cache.reset();
            self.stash.clearRetainingCapacity();

            self.* = .{
                .cache = self.cache,
                .stash = self.stash,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub fn has(self: *const CacheMap, key: Key) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *const CacheMap, key: Key) ?*Value {
            if (self.stash.getKeyPtr(tombstone_from_key(key))) |object| {
                // Deleted keys are represented as tombstones in the stash.
                return if (tombstone(object)) null else object;
            }

            if (self.cache) |*cache| {
                if (cache.get(key)) |object| {
                    return if (tombstone(object)) null else object;
                }
            }

            return null;
        }

        pub fn get_or_tombstone(self: *const CacheMap, key: Key) union(enum) {
            found: *Value,
            not_found,
            tombstone,
        } {
            if (self.stash.getKeyPtr(tombstone_from_key(key))) |object| {
                // Deleted keys are represented as tombstones in the stash.
                return if (tombstone(object)) .tombstone else .{ .found = object };
            }

            if (self.cache) |*cache| {
                if (cache.get(key)) |object| {
                    return if (tombstone(object)) .tombstone else .{ .found = object };
                }
            }

            return .not_found;
        }

        pub fn cache_entries(self: *const CacheMap) u64 {
            return if (self.cache) |*cache|
                cache.metrics.value_count
            else
                0;
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

        // Upserts the stash and returns the old visible value in case of an update.
        fn fetch_upsert(self: *CacheMap, value: *const Value) ?Value {
            const key = key_from_value(value);
            const old_stash = self.stash_upsert(value);
            if (old_stash) |old| return old;

            if (self.cache) |*cache| {
                if (cache.get(key)) |old| return old.*;
            }

            return null;
        }

        fn stash_upsert(self: *CacheMap, value: *const Value) ?Value {
            defer assert(self.stash.count() <= self.options.stash_value_count_max);
            // Using `getOrPutAssumeCapacity` instead of `putAssumeCapacity` is
            // critical, since we use HashMaps with no Value, `putAssumeCapacity`
            // _will not_ clobber the existing value.
            const gop = self.stash.getOrPutAssumeCapacity(value.*);
            defer gop.key_ptr.* = value.*;

            return if (gop.found_existing)
                gop.key_ptr.*
            else
                null;
        }

        /// Removes a key by adding a tombstone to the stash.
        /// Invariant: The key must be visible through the cache map.
        pub fn remove(self: *CacheMap, key: Key) void {
            // Only unit tests and fuzzers call this function.
            // Assert that it is not called from production code.
            comptime assert(constants.verify);

            const old_value = (self.get(key) orelse unreachable).*;
            assert(!tombstone(&old_value));

            const stash_removed: ?Value = stash_removed: {
                assert(self.stash.count() <= self.options.stash_value_count_max);

                const tombstone_object = tombstone_from_key(key);
                const entry = self.stash.getOrPutAssumeCapacity(tombstone_object);

                // Add a tombstone in the stash, indicating that the
                // deletion happened and that the key should not be
                // looked up in the immutable table or LSM tree.
                defer entry.key_ptr.* = tombstone_object;

                break :stash_removed if (entry.found_existing) entry.key_ptr.* else null;
            };

            maybe(stash_removed != null);

            // Cannot remove a value that has already been removed.
            if (stash_removed) |stash_value| assert(!tombstone(&stash_value));
            if (self.scope_is_active) {
                self.scope_rollback_log.appendAssumeCapacity(.{
                    .restore = old_value,
                });
            }
        }

        fn stash_remove(self: *CacheMap, key: Key) ?Value {
            assert(self.stash.count() <= self.options.stash_value_count_max);
            return if (self.stash.fetchRemove(tombstone_from_key(key))) |kv|
                kv.key
            else
                null;
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
                        // Reverting an insert consists of removing the value.
                        const stash_value = self.stash_remove(key).?;
                        assert(!tombstone(&stash_value));
                    },
                }
            }

            self.scope_rollback_log.clearRetainingCapacity();
        }

        pub fn compact(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.stash_value_count_max);

            if (self.cache) |*cache| {
                var stash_iterator = self.stash.keyIterator();
                while (stash_iterator.next()) |object| {
                    const key = key_from_value(object);
                    if (tombstone(object)) {
                        _ = cache.remove(key);
                    } else {
                        _ = cache.upsert(object);
                    }
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
        .stash_value_count_max = 32,
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

    // Test scope discard on a sequence of insert->remove->insert.
    cache_map.upsert(&.{ .key = 4, .value = 4, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 4, .value = 4, .tombstone = false },
        cache_map.get(4).?.*,
    );

    cache_map.remove(4);
    assert(!cache_map.has(4));
    assert(cache_map.get(4) == null);
    assert(cache_map.get_or_tombstone(4) == .tombstone);

    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 4, .value = 4, .tombstone = false });
    cache_map.scope_close(.discard);

    assert(!cache_map.has(4));
    assert(cache_map.get(4) == null);
    assert(cache_map.get_or_tombstone(4) == .tombstone);
}

test "cache_map: stash shadows cache until compact" {
    const testing = std.testing;

    const allocator = testing.allocator;

    var cache_map = try TestCacheMap.init(allocator, .{
        .cache_value_count_max = TestCacheMap.Cache.value_count_max_multiple,
        .scope_value_count_max = 32,
        .stash_value_count_max = 32,
        .name = "test map",
    });
    defer cache_map.deinit(allocator);

    cache_map.upsert(&.{ .key = 1, .value = 1, .tombstone = false });
    try testing.expectEqual(@as(u64, 0), cache_map.cache_entries());
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 1, .tombstone = false },
        cache_map.get(1).?.*,
    );

    cache_map.compact();
    try testing.expectEqual(@as(u64, 1), cache_map.cache_entries());
    try testing.expectEqual(@as(u64, 0), cache_map.stash.count());
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 1, .tombstone = false },
        cache_map.cache.?.get(1).?.*,
    );

    cache_map.upsert(&.{ .key = 1, .value = 2, .tombstone = false });
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 1, .tombstone = false },
        cache_map.cache.?.get(1).?.*,
    );
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 2, .tombstone = false },
        cache_map.get(1).?.*,
    );

    cache_map.compact();
    try testing.expectEqual(@as(u64, 0), cache_map.stash.count());
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 2, .tombstone = false },
        cache_map.cache.?.get(1).?.*,
    );

    cache_map.remove(1);
    assert(cache_map.get(1) == null);
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 2, .tombstone = false },
        cache_map.cache.?.get(1).?.*,
    );

    cache_map.compact();
    assert(cache_map.get(1) == null);
    assert(cache_map.cache.?.get(1) == null);
}
