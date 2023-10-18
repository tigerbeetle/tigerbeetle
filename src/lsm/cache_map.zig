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
    const _Cache = SetAssociativeCacheType(
        Key,
        Value,
        key_from_value,
        hash_from_key,
        .{},
    );

    const HashMapContextValue = struct {
        const Self = @This();

        pub inline fn eql(_: Self, a: Value, b: Value) bool {
            return key_from_value(&a) == key_from_value(&b);
        }

        pub inline fn hash(_: Self, value: Value) u64 {
            return stdx.hash_inline(key_from_value(&value));
        }
    };

    const map_load_percentage_max = 50;
    const _Map = std.HashMapUnmanaged(
        Value,
        void,
        HashMapContextValue,
        map_load_percentage_max,
    );

    return struct {
        const Self = @This();

        pub const Cache = _Cache;
        pub const Map = _Map;

        pub const Options = struct {
            cache_value_count_max: u32,
            map_value_count_max: u32,
            scope_value_count_max: u32,
            name: []const u8,
        };

        // The hierarchy for lookups is cache -> stash_1 -> stash_2. Lower levels _may_ have stale
        // values, provided the correct value exists in one of the levels above. We have two
        // maps to implement our compact() support. Evictions from the cache first flow into
        // stash_1, with .compact() clearing stash_2 and swapping it.
        cache: Cache,
        stash_1: Map,
        stash_2: Map,

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them.
        scope_is_active: bool = false,
        scope_rollback_log: std.ArrayListUnmanaged(Value),

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !Self {
            assert(options.cache_value_count_max > 0);
            assert(options.map_value_count_max > 0);
            maybe(options.scope_value_count_max == 0);

            var cache: Cache = try Cache.init(
                allocator,
                options.cache_value_count_max,
                .{ .name = options.name },
            );
            errdefer cache.deinit(allocator);

            var stash_1: Map = .{};
            try stash_1.ensureTotalCapacity(allocator, options.map_value_count_max);
            errdefer stash_1.deinit(allocator);

            var stash_2: Map = .{};
            try stash_2.ensureTotalCapacity(allocator, options.map_value_count_max);
            errdefer stash_2.deinit(allocator);

            var scope_rollback_log = try std.ArrayListUnmanaged(Value).initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return Self{
                .cache = cache,
                .stash_1 = stash_1,
                .stash_2 = stash_2,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash_1.count() <= self.options.map_value_count_max);
            assert(self.stash_2.count() <= self.options.map_value_count_max);

            self.scope_rollback_log.deinit(allocator);
            self.stash_2.deinit(allocator);
            self.stash_1.deinit(allocator);
            self.cache.deinit(allocator);
        }

        pub fn reset(self: *Self) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash_1.count() <= self.options.map_value_count_max);
            assert(self.stash_2.count() <= self.options.map_value_count_max);

            self.cache.reset();
            self.stash_1.clearRetainingCapacity();
            self.stash_2.clearRetainingCapacity();

            self.* = .{
                .cache = self.cache,
                .stash_1 = self.stash_1,
                .stash_2 = self.stash_2,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub fn has(self: *const Self, key: Key) bool {
            return self.cache.get_index(key) != null or
                self.stash_1.getKeyPtr(tombstone_from_key(key)) != null or
                self.stash_2.getKeyPtr(tombstone_from_key(key)) != null;
        }

        pub fn get(self: *const Self, key: Key) ?*Value {
            return self.cache.get(key) orelse
                self.stash_1.getKeyPtr(tombstone_from_key(key)) orelse
                self.stash_2.getKeyPtr(tombstone_from_key(key));
        }

        pub fn upsert(self: *Self, value: *const Value) void {
            if (self.scope_is_active) {
                return self.upsert_scope_opened(value);
            } else {
                return self.upsert_scope_closed(value);
            }
        }

        fn upsert_scope_closed(self: *Self, value: *const Value) void {
            assert(!self.scope_is_active);

            const result = self.cache.upsert(value);

            if (result.evicted) |evicted| {
                switch (result.updated) {
                    .insert => {
                        // Here and in upsert_scope, putAssumeCapacity vs getOrPutAssumeCapacity is
                        // critical. Since we use HashMaps with no Value, putAssumeCapacity _will
                        // not_ clobber the existing value.
                        const gop = self.stash_1.getOrPutAssumeCapacity(evicted);
                        gop.key_ptr.* = evicted;
                    },
                    .update => {},
                }
            }
        }

        // When upserting into a scope, there are a few cases that must be handled:
        // 1. There was an eviction because an item was updated. Append the evicted item to the
        //    scope rollback log.
        // 2. There was an eviction because an item was inserted (eg, two different keys mapping to
        //    the same tags). Put the item in the stash, just like the no-scope case, and don't
        //    store anything in the scope rollback log yet. Case 3 will handle that.
        // 3. Regardless of eviction, there was an insert:
        //    a. If the item exists in the stash, it was really an update. Append the stash value
        //       to the scope rollback log.
        //    b. If the item doesn't exist in the stash, it was an insert. Append a tombstone to
        //       the scope rollback log.
        fn upsert_scope_opened(self: *Self, value: *const Value) void {
            assert(self.scope_is_active);

            const result = self.cache.upsert(value);

            if (result.evicted) |evicted| {
                switch (result.updated) {
                    .update => {
                        // Case 1.
                        self.scope_rollback_log.appendAssumeCapacity(evicted);
                    },
                    .insert => {
                        // Case 2.
                        const gop = self.stash_1.getOrPutAssumeCapacity(evicted);
                        gop.key_ptr.* = evicted;

                        // Case 3 below handles appending into the rollback log if needed.
                    },
                }
            }

            if (result.updated == .insert) {
                if (self.stash_1.getKey(value.*)) |stash_value| {
                    // Case 3a.
                    self.scope_rollback_log.appendAssumeCapacity(stash_value);
                } else {
                    // Case 3b.
                    self.scope_rollback_log.appendAssumeCapacity(
                        tombstone_from_key(key_from_value(value)),
                    );
                }
            }
        }

        pub fn remove(self: *Self, key: Key) void {
            // The only thing that tests this in any depth is the cache_map fuzz itself.
            // Make sure we aren't being called in regular code without another once over.
            assert(constants.verify);

            const maybe_removed = self.cache.remove(key);

            if (self.scope_is_active) {
                // TODO: Actually, does the fuzz catch this...
                // TODO: So if we delete from stash_2 and put to stash_1, there's a problem;
                //       because when we undo our scope we insert back to stash_1 :/
                self.scope_rollback_log.appendAssumeCapacity(
                    maybe_removed orelse
                        self.stash_1.getKey(tombstone_from_key(key)) orelse
                        self.stash_2.getKey(tombstone_from_key(key)) orelse return,
                );
            }

            // We always need to try remove from the stash; since it could have a stale value.
            // The early return above is OK - if it doesn't exist, there's nothing to remove.
            _ = self.stash_1.remove(tombstone_from_key(key));
            _ = self.stash_2.remove(tombstone_from_key(key));
        }

        /// Start a new scope. Within a scope, changes can be persisted
        /// or discarded. At most one scope can be active at a time.
        pub fn scope_open(self: *Self) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            self.scope_is_active = true;
        }

        pub fn scope_close(self: *Self, mode: ScopeCloseMode) void {
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
                    // Reverting an insert consists of a .remove call. The value in here will be a
                    // tombstone indicating the original value didn't exist. We don't touch stash_2;
                    // since we can never insert into it directly (only a .compact() can).
                    const key = key_from_value(rollback_value);

                    // A tombstone in the rollback log can only occur when the value doesn't exist
                    // in _both_ the cache and stash on insert (case 3b in upsert_scope_opened()).
                    // Since we replay the rollback operations backwards, the state of the cache
                    // and stash here will be identical to that of just after the insert, so it
                    // only needs to be removed from the cache.
                    const removed_from_cache = self.cache.remove(key) != null;
                    assert(removed_from_cache);
                } else {
                    // Reverting an update or delete consists of an insert of the original value.
                    self.upsert_scope_closed(rollback_value);
                }
            }

            self.scope_rollback_log.clearRetainingCapacity();
        }

        pub fn compact(self: *Self) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash_1.count() <= self.options.map_value_count_max);
            assert(self.stash_2.count() <= self.options.map_value_count_max);

            self.stash_2.clearRetainingCapacity();
            std.mem.swap(Map, &self.stash_1, &self.stash_2);
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
        .cache_value_count_max = 2048,
        .scope_value_count_max = 32,
        .map_value_count_max = 32,
        .name = "test map",
    });
    defer cache_map.deinit(allocator);

    cache_map.upsert(&.{ .key = 1, .value = 1, .tombstone = false });
    try testing.expectEqual(.{ .key = 1, .value = 1, .tombstone = false }, cache_map.get(1).?.*);

    // Test scope persisting
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 2, .tombstone = false });
    try testing.expectEqual(.{ .key = 2, .value = 2, .tombstone = false }, cache_map.get(2).?.*);
    cache_map.scope_close(.persist);
    try testing.expectEqual(.{ .key = 2, .value = 2, .tombstone = false }, cache_map.get(2).?.*);

    // Test scope discard on updates
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 22, .tombstone = false });
    cache_map.upsert(&.{ .key = 2, .value = 222, .tombstone = false });
    cache_map.upsert(&.{ .key = 2, .value = 2222, .tombstone = false });
    try testing.expectEqual(
        .{ .key = 2, .value = 2222, .tombstone = false },
        cache_map.get(2).?.*,
    );
    cache_map.scope_close(.discard);
    try testing.expectEqual(.{ .key = 2, .value = 2, .tombstone = false }, cache_map.get(2).?.*);

    // Test scope discard on inserts
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 3, .value = 3, .tombstone = false });
    try testing.expectEqual(.{ .key = 3, .value = 3, .tombstone = false }, cache_map.get(3).?.*);
    cache_map.upsert(&.{ .key = 3, .value = 33, .tombstone = false });
    try testing.expectEqual(.{ .key = 3, .value = 33, .tombstone = false }, cache_map.get(3).?.*);
    cache_map.scope_close(.discard);
    assert(!cache_map.has(3));
    assert(cache_map.get(3) == null);

    // Test scope discard on removes
    cache_map.scope_open();
    cache_map.remove(2);
    assert(!cache_map.has(2));
    assert(cache_map.get(2) == null);
    cache_map.scope_close(.discard);
    try testing.expectEqual(.{ .key = 2, .value = 2, .tombstone = false }, cache_map.get(2).?.*);
}
