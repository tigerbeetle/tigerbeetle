const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const SetAssociativeCacheType = @import("set_associative_cache.zig").SetAssociativeCacheType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

/// A CacheMap is a SetAssociativeCache over an optional `write_index` and a HashMap `stash`.
///
/// `write_index` maps a primary key to a slot in the parent tree's `table_mutable.values`,
/// so slot=some upserts don't duplicate the Value bytes between the mutable table and the
/// stash. When `Options.write_index_value_count_max == 0` the cache_map falls back to the
/// historical single-stash layout.
///
/// Lookups walk: cache -> write_index -> stash. `compact()` clears both write_index and
/// stash at the bar boundary, before the tree's mutable->immutable swap.
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

        // Maps a primary Key to a slot in `values_backing.*` (the parent tree's
        // `table_mutable.values`). Only allocated when `Options.write_index_value_count_max > 0`.
        pub const WriteIndex = std.AutoHashMapUnmanaged(Key, u32);

        pub const Options = struct {
            cache_value_count_max: u32,
            // The maximum number of slot=some entries the `write_index` must hold. Set to 0
            // to disable the write_index entirely (in which case the cache_map behaves like
            // its pre-optimization self: a single stash holding full Values).
            //
            // The intended sizing for write_index_value_count_max is the worst-case number
            // of `tree.put` operations that can land in `tree.table_mutable` between two
            // bar boundaries, which equals `lsm_compaction_ops * batch_value_count_limit`.
            write_index_value_count_max: u32 = 0,
            // The maximum number of values the `stash` must hold. With `write_index`
            // disabled this is `lsm_compaction_ops * (batch + prefetch)` (the historical
            // bound). With `write_index` enabled, the slot=some population moves into
            // `write_index`, so the stash only needs `lsm_compaction_ops * prefetch`.
            stash_value_count_max: u32,
            scope_value_count_max: u32,
            name: []const u8,
        };

        cache: ?Cache,
        // The slot-pointer index. Empty (zero capacity) when
        // `Options.write_index_value_count_max == 0`.
        write_index: WriteIndex,
        stash: Map,

        // Stable pointer to the slice header backing `write_index`'s slot indices. Set via
        // `attach_values_backing` after the parent tree is initialized. The slice header
        // (not the slice itself) is what's stable, since `tree.table_mutable.values` gets
        // `std.mem.swap`'d during `compact`/`absorb`. Always null when write_index is
        // disabled.
        values_backing: ?*const []Value,

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them. Slot-aware upserts ALSO append to the rollback log (we save the
        // pre-scope value, not the slot, since the rebuild path reconstructs the slot from
        // the post-rewind tree state).
        scope_is_active: bool = false,
        scope_rollback_log: std.ArrayListUnmanaged(Value),

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !CacheMap {
            assert(options.stash_value_count_max > 0);
            maybe(options.cache_value_count_max == 0);
            maybe(options.write_index_value_count_max == 0);
            maybe(options.scope_value_count_max == 0);

            var cache: ?Cache = if (options.cache_value_count_max == 0) null else try Cache.init(
                allocator,
                options.cache_value_count_max,
                .{ .name = options.name },
            );
            errdefer if (cache) |*cache_unwrapped| cache_unwrapped.deinit(allocator);

            var write_index: WriteIndex = .{};
            if (options.write_index_value_count_max > 0) {
                try write_index.ensureTotalCapacity(
                    allocator,
                    options.write_index_value_count_max,
                );
            }
            errdefer write_index.deinit(allocator);

            var stash: Map = .{};
            try stash.ensureTotalCapacity(allocator, options.stash_value_count_max);
            errdefer stash.deinit(allocator);

            var scope_rollback_log = try std.ArrayListUnmanaged(Value).initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return CacheMap{
                .cache = cache,
                .write_index = write_index,
                .stash = stash,
                .values_backing = null,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        // Attach the slice-header pointer that backs `write_index`'s slot indices. Must be
        // called once, after the parent tree's `table_mutable` has been initialized, and
        // only when `write_index_value_count_max > 0`.
        pub fn attach_values_backing(
            self: *CacheMap,
            values_backing: *const []Value,
        ) void {
            assert(self.write_index_enabled());
            assert(self.values_backing == null);
            self.values_backing = values_backing;
        }

        // Callback registered with the parent tree's `table_mutable` so the cache_map's
        // `write_index` is automatically re-aligned after every full or incremental sort
        // of `values_used`. The `*anyopaque` context is the `*CacheMap`.
        pub fn sort_callback(
            context: *anyopaque,
            values_used: []const Value,
            offset_after_sort: u32,
        ) void {
            const self: *CacheMap = @ptrCast(@alignCast(context));
            self.rebuild_write_index_suffix(values_used, offset_after_sort);
        }

        pub fn deinit(self: *CacheMap, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.stash_value_count_max);
            assert(self.write_index.count() <= self.options.write_index_value_count_max);

            self.scope_rollback_log.deinit(allocator);
            self.stash.deinit(allocator);
            self.write_index.deinit(allocator);
            if (self.cache) |*cache| cache.deinit(allocator);
        }

        pub fn reset(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.stash_value_count_max);
            assert(self.write_index.count() <= self.options.write_index_value_count_max);

            if (self.cache) |*cache| cache.reset();
            self.stash.clearRetainingCapacity();
            self.write_index.clearRetainingCapacity();

            self.* = .{
                .cache = self.cache,
                .write_index = self.write_index,
                .stash = self.stash,
                .values_backing = self.values_backing,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub inline fn write_index_enabled(self: *const CacheMap) bool {
            return self.options.write_index_value_count_max > 0;
        }

        pub fn has(self: *const CacheMap, key: Key) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *const CacheMap, key: Key) ?*Value {
            if (self.cache) |*cache| {
                if (cache.get(key)) |v| {
                    if (constants.verify) assert(key_from_value(v) == key);
                    return v;
                }
            }
            if (self.write_index_enabled()) {
                if (self.write_index.get(key)) |slot| {
                    const values = self.values_backing.?.*;
                    assert(slot < values.len);
                    const value_ptr = &values[slot];
                    // The slot must hold a value with the looked-up key, otherwise
                    // `write_index` is out of sync with `tree.table_mutable.values`.
                    assert(key_from_value(value_ptr) == key);
                    // A tombstone in the mutable table means "logically removed within
                    // this bar". Surface it as "not present" to match the semantics that
                    // `cache_map.remove(key)` followed by `cache_map.get(key)` returns null.
                    if (tombstone(value_ptr)) return null;
                    return value_ptr;
                }
            }
            return self.stash.getKeyPtr(tombstone_from_key(key));
        }

        // `slot` is the index in the parent tree's `table_mutable.values` where this value
        // also lives. Pass `slot = null` for values that are NOT mirrored in the mutable
        // table (prefetch loads, orphaned-id sentinels, SAC eviction recovery copies). When
        // `write_index_value_count_max == 0`, `slot` is required to be null.
        pub fn upsert(self: *CacheMap, value: *const Value, slot: ?u32) void {
            if (slot != null) assert(self.write_index_enabled());

            const old_value_maybe = self.fetch_upsert(value, slot);

            if (self.scope_is_active) {
                if (old_value_maybe) |old_value| {
                    // The previous version of this key (if any).
                    self.scope_rollback_log.appendAssumeCapacity(old_value);
                } else {
                    // No previous version: it was a fresh insert. Record a tombstone so the
                    // discard path knows to remove the key.
                    const key = key_from_value(value);
                    const key_tombstone = tombstone_from_key(key);
                    self.scope_rollback_log.appendAssumeCapacity(key_tombstone);
                }
            }
        }

        // Upserts the cache, write_index and stash. Returns the old value if this upsert
        // replaces an existing live entry.
        fn fetch_upsert(self: *CacheMap, value: *const Value, slot: ?u32) ?Value {
            if (self.cache != null) {
                return self.fetch_upsert_with_cache(value, slot);
            } else {
                return self.fetch_upsert_no_cache(value, slot);
            }
        }

        fn fetch_upsert_with_cache(
            self: *CacheMap,
            value: *const Value,
            slot: ?u32,
        ) ?Value {
            const key = key_from_value(value);
            const cache = &self.cache.?;
            const result = cache.upsert(value);

            var old_value: ?Value = null;
            if (result.evicted) |*evicted| switch (result.updated) {
                .update => {
                    assert(key_from_value(evicted) == key);
                    if (constants.verify) assert(!self.stash.contains(value.*));
                    // Previous version of THIS key.
                    old_value = evicted.*;
                },
                .insert => {
                    assert(key_from_value(evicted) != key);
                    // Eviction-time recovery: drop the eviction if the evicted key still
                    // has a live copy reachable through `write_index`; otherwise preserve
                    // it in the stash.
                    const evicted_key = key_from_value(evicted);
                    if (self.write_index_enabled() and
                        self.write_index.contains(evicted_key))
                    {
                        // Drop. Mutable table still has the latest version.
                    } else {
                        const stash_updated = self.stash_upsert(evicted);
                        assert(stash_updated == null);
                    }
                },
            } else {
                // No eviction means a fresh slot; updates always evict the old value.
                assert(result.updated == .insert);
            }

            // Record the new slot for slot=some callers so a later SAC eviction of this
            // key falls through to the live copy in `mutable.values`.
            if (slot) |s| self.write_index.putAssumeCapacity(key, s);

            // A stale prefetch entry for `key` (if any) is now superseded by the cache.
            const stash_old = self.stash_remove(key);
            return old_value orelse stash_old;
        }

        fn fetch_upsert_no_cache(
            self: *CacheMap,
            value: *const Value,
            slot: ?u32,
        ) ?Value {
            const key = key_from_value(value);
            if (slot) |s| {
                // Mirror in `write_index`. The previous slot for this key (if any) becomes
                // a shadow in `mutable.values` that the next `sort_suffix` dedup folds
                // out. Returning the previous-slot value satisfies the scope rollback
                // contract.
                const old_slot_kv = self.write_index.fetchPutAssumeCapacity(key, s);
                const stash_old = self.stash_remove(key);
                if (old_slot_kv) |kv| {
                    const values = self.values_backing.?.*;
                    assert(kv.value < values.len);
                    const prev = values[kv.value];
                    assert(key_from_value(&prev) == key);
                    return prev;
                }
                return stash_old;
            }
            // `slot=null` and no SAC: pure stash semantics, identical to the
            // pre-write_index code path.
            return self.stash_upsert(value);
        }

        fn stash_upsert(self: *CacheMap, value: *const Value) ?Value {
            defer assert(self.stash.count() <= self.options.stash_value_count_max);
            // Using `getOrPutAssumeCapacity` instead of `putAssumeCapacity` is critical,
            // since we use HashMaps with void Value, `putAssumeCapacity` _will not_ clobber
            // the existing key entry.
            const gop = self.stash.getOrPutAssumeCapacity(value.*);
            defer gop.key_ptr.* = value.*;

            return if (gop.found_existing)
                gop.key_ptr.*
            else
                null;
        }

        pub fn remove(self: *CacheMap, key: Key) void {
            // The only thing that tests this in any depth is the cache_map fuzz itself.
            // Make sure we aren't being called in regular code without another once over.
            comptime assert(constants.verify);

            const cache_removed: ?Value = if (self.cache) |*cache|
                cache.remove(key)
            else
                null;

            // Remove from write_index AND stash, since either may hold a different version
            // of the same key.
            const write_index_removed: ?Value = self.write_index_remove(key);
            const stash_removed: ?Value = self.stash_remove(key);

            if (self.scope_is_active) {
                self.scope_rollback_log.appendAssumeCapacity(
                    cache_removed orelse
                        write_index_removed orelse
                        stash_removed orelse return,
                );
            }
        }

        fn write_index_remove(self: *CacheMap, key: Key) ?Value {
            if (!self.write_index_enabled()) return null;
            const kv = self.write_index.fetchRemove(key) orelse return null;
            const values = self.values_backing.?.*;
            assert(kv.value < values.len);
            const value = values[kv.value];
            // Mirror the safety net in `get`: the slot must hold a value with the key we
            // removed, otherwise `write_index` is out of sync with `values_backing` and
            // whatever we return would silently corrupt the scope rollback log.
            assert(key_from_value(&value) == key);
            return value;
        }

        fn stash_remove(self: *CacheMap, key: Key) ?Value {
            assert(self.stash.count() <= self.options.stash_value_count_max);
            return if (self.stash.fetchRemove(tombstone_from_key(key))) |kv|
                kv.key
            else
                null;
        }

        /// Start a new scope. Within a scope, changes can be persisted or discarded.
        /// At most one scope can be active at a time.
        pub fn scope_open(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            self.scope_is_active = true;
        }

        pub fn scope_close(self: *CacheMap, mode: ScopeCloseMode) void {
            assert(self.scope_is_active);
            self.scope_is_active = false;

            if (mode == .persist) {
                self.scope_rollback_log.clearRetainingCapacity();
                return;
            }

            // The scope_rollback_log stores the operations we need to reverse the changes a
            // scope made. They get replayed in reverse order. The replays touch the SAC and
            // stash; for write_index entries, the replay is a no-op (slot=null upserts skip
            // write_index). After this loop, `Groove.scope_close` calls
            // `rebuild_write_index_full` to reconstruct write_index from the post-rewind
            // tree state.
            var i: usize = self.scope_rollback_log.items.len;
            while (i > 0) {
                i -= 1;

                const rollback_value = &self.scope_rollback_log.items[i];
                if (tombstone(rollback_value)) {
                    // Reverting an insert consists of a .remove call. The value here is a
                    // tombstone indicating the original value did not exist.
                    const key = key_from_value(rollback_value);

                    const cache_removed = if (self.cache) |*cache|
                        cache.remove(key) != null
                    else
                        false;
                    const write_index_removed = if (self.write_index_enabled())
                        self.write_index.remove(key)
                    else
                        false;
                    const stash_removed = self.stash_remove(key) != null;
                    // The key was in at least one of cache / write_index / stash before
                    // rollback. With both cache and write_index present, a slot=some insert
                    // populates both, so present_count may be 2.
                    assert(cache_removed or write_index_removed or stash_removed);
                } else {
                    // Reverting an update or delete consists of an insert of the original
                    // value. We don't know the slot of the original (it was lost when we
                    // appended the value to the rollback log), so we route via slot=null.
                    // The subsequent `rebuild_write_index_full` call from
                    // `Groove.scope_close` will reconstitute write_index from the
                    // post-rewind mutable values.
                    self.upsert(rollback_value, null);
                }
            }

            self.scope_rollback_log.clearRetainingCapacity();
        }

        // Rebuild `write_index` over the suffix `[offset..values_used.len)`. Used by
        // `Groove.compact` after each per-beat `tree.compact()` (which sorts and dedupes the
        // suffix). The walk is forward and last-wins per key:
        //   - tombstones erase any prior write_index entry for the key (preserving the
        //     existing semantic that a removed key is reported absent by `cache_map.has`),
        //   - non-tombstones overwrite.
        // The stable prefix `[0..offset)` is left untouched.
        pub fn rebuild_write_index_suffix(
            self: *CacheMap,
            values_used: []const Value,
            offset: u32,
        ) void {
            assert(self.write_index_enabled());
            assert(self.values_backing != null);
            assert(offset <= values_used.len);

            var i: u32 = offset;
            while (i < values_used.len) : (i += 1) {
                const v = &values_used[i];
                const k = key_from_value(v);
                if (tombstone(v)) {
                    _ = self.write_index.remove(k);
                } else {
                    self.write_index.putAssumeCapacity(k, i);
                }
            }
        }

        // Full rebuild over `[0..values_used.len)`. Used by `Groove.scope_close(.discard)`
        // after the tree rewinds `value_context.count`. Clears `write_index` first to drop
        // stale entries pointing past the rewind boundary, then walks forward with the
        // same tombstone-erases / non-tombstone-overwrites semantics as the suffix rebuild.
        pub fn rebuild_write_index_full(self: *CacheMap, values_used: []const Value) void {
            assert(self.write_index_enabled());
            assert(self.values_backing != null);

            self.write_index.clearRetainingCapacity();
            for (values_used, 0..) |*v, i| {
                const k = key_from_value(v);
                if (tombstone(v)) {
                    _ = self.write_index.remove(k);
                } else {
                    self.write_index.putAssumeCapacity(k, @intCast(i));
                }
            }
        }

        pub fn compact(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.stash.count() <= self.options.stash_value_count_max);
            assert(self.write_index.count() <= self.options.write_index_value_count_max);

            self.stash.clearRetainingCapacity();
            self.write_index.clearRetainingCapacity();
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

    // Unit test exercises the SAC + stash path with cache != null and write_index disabled,
    // matching the original semantics. Slot-aware coverage is exercised end-to-end via
    // forest_fuzz / VOPR through the real Account groove.
    var cache_map = try TestCacheMap.init(allocator, .{
        .cache_value_count_max = TestCacheMap.Cache.value_count_max_multiple,
        .scope_value_count_max = 32,
        .stash_value_count_max = 32,
        .name = "test map",
    });
    defer cache_map.deinit(allocator);

    cache_map.upsert(&.{ .key = 1, .value = 1, .tombstone = false }, null);
    try testing.expectEqual(
        TestTable.Value{ .key = 1, .value = 1, .tombstone = false },
        cache_map.get(1).?.*,
    );

    // Test scope persisting
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 2, .tombstone = false }, null);
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
    cache_map.upsert(&.{ .key = 2, .value = 22, .tombstone = false }, null);
    cache_map.upsert(&.{ .key = 2, .value = 222, .tombstone = false }, null);
    cache_map.upsert(&.{ .key = 2, .value = 2222, .tombstone = false }, null);
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
    cache_map.upsert(&.{ .key = 3, .value = 3, .tombstone = false }, null);
    try testing.expectEqual(
        TestTable.Value{ .key = 3, .value = 3, .tombstone = false },
        cache_map.get(3).?.*,
    );
    cache_map.upsert(&.{ .key = 3, .value = 33, .tombstone = false }, null);
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
