const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

const Fingerprint = packed struct(u32) {
    slot: u24,
    code: u8,
};

const capacity_ring = 1048576;
const capacity_hash = capacity_ring * 2;

fn FieldType(comptime T: type, comptime field_name: []const u8) type {
    const info = @typeInfo(T);
    if (info != .@"struct") {
        @compileError("KV must be a struct with .key and .value fields");
    }

    inline for (info.@"struct".fields) |field| {
        if (std.mem.eql(u8, field.name, field_name)) return field.type;
    }

    @compileError("KV must have a ." ++ field_name ++ " field");
}

fn capacity_power_of_two_min(capacity_min: usize) usize {
    assert(capacity_min > 0);

    var capacity: usize = 1;
    while (capacity < capacity_min) {
        assert(capacity <= std.math.maxInt(usize) / 2);
        capacity *= 2;
    }
    return capacity;
}

fn MicaType(
    comptime KV: type,
    comptime hash_from_key: fn (FieldType(KV, "key")) callconv(.@"inline") u64,
    comptime capacity_ring_min: usize,
    comptime capacity_hash_min: usize,
) type {
    const Key = FieldType(KV, "key");
    const Value = FieldType(KV, "value");

    return struct {
        const Self = @This();

        const back_invalid = std.math.maxInt(u32);
        const code_empty = std.math.maxInt(u8);
        const empty_fingerprint = Fingerprint{ .code = code_empty, .slot = 0 };

        // head = next element to read
        // tail = next slot to write
        //
        // empty when head == tail
        // full  when tail - head == capacity
        tail: usize,
        head: usize,

        capacity_ring: usize,
        capacity_hash: usize,
        mask_ring: usize,
        mask_hash: usize,

        ring: []KV,
        hash: []Fingerprint,
        dist: []u8,
        back: []u32,

        count: usize,

        pub fn init(allocator: std.mem.Allocator, ring_capacity_min: usize) !Self {
            const ring_capacity = @max(
                capacity_ring_min,
                capacity_power_of_two_min(ring_capacity_min),
            );
            const hash_capacity = @max(capacity_hash_min, ring_capacity * 2);

            assert(std.math.isPowerOfTwo(hash_capacity));
            assert(std.math.isPowerOfTwo(ring_capacity));
            assert(hash_capacity > ring_capacity);
            assert(ring_capacity - 1 <= std.math.maxInt(u24));
            assert(hash_capacity - 1 <= std.math.maxInt(u32));

            const ring = try allocator.alloc(KV, ring_capacity);
            errdefer allocator.free(ring);

            const hash = try allocator.alloc(Fingerprint, hash_capacity);
            errdefer allocator.free(hash);

            const dist = try allocator.alloc(u8, hash_capacity);
            errdefer allocator.free(dist);

            const back = try allocator.alloc(u32, ring_capacity);
            errdefer allocator.free(back);

            @memset(hash, empty_fingerprint);
            @memset(dist, 0);
            @memset(back, back_invalid);

            return Self{
                .tail = 0,
                .head = 0,
                .capacity_ring = ring_capacity,
                .capacity_hash = hash_capacity,
                .mask_ring = ring_capacity - 1,
                .mask_hash = hash_capacity - 1,
                .ring = ring,
                .hash = hash,
                .dist = dist,
                .back = back,
                .count = 0,
            };
        }

        pub fn deinit(mica: *Self, allocator: std.mem.Allocator) void {
            allocator.free(mica.back);
            allocator.free(mica.dist);
            allocator.free(mica.hash);
            allocator.free(mica.ring);
            mica.* = undefined;
        }

        pub fn reset(mica: *Self) void {
            mica.tail = 0;
            mica.head = 0;
            mica.count = 0;
            @memset(mica.hash, empty_fingerprint);
            @memset(mica.dist, 0);
            @memset(mica.back, back_invalid);
        }

        pub fn len(mica: *const Self) usize {
            return mica.tail - mica.head;
        }

        fn code(hash: u64) u8 {
            return @truncate(hash >> 57);
        }

        fn hash_slot(mica: *const Self, hash: u64) usize {
            return @as(usize, @truncate(hash)) & mica.mask_hash;
        }

        const Slot = union(enum) {
            found: usize,
            free: usize,
            full,
        };

        fn find_slot(mica: *const Self, key: Key) Slot {
            const hash = hash_from_key(key);
            const fingerprint = code(hash);
            var slot = mica.hash_slot(hash);

            for (0..mica.capacity_hash) |_| {
                const entry = mica.hash[slot];
                if (entry.code == code_empty) return .{ .free = slot };

                if (entry.code == fingerprint and
                    std.meta.eql(mica.ring[entry.slot].key, key))
                {
                    return .{ .found = slot };
                }

                slot = (slot + 1) & mica.mask_hash;
            }

            return .full;
        }

        fn delete_hash_slot(mica: *Self, slot_delete: usize) void {
            var i = slot_delete & mica.mask_hash;
            assert(mica.hash[i].code != code_empty);

            mica.back[mica.hash[i].slot] = back_invalid;
            mica.count -= 1;

            while (true) {
                var j = (i + 1) & mica.mask_hash;
                var off: usize = 1;
                while (true) {
                    const entry = mica.hash[j];
                    if (entry.code == code_empty) {
                        mica.hash[i] = empty_fingerprint;
                        mica.dist[i] = 0;
                        return;
                    }

                    if (mica.dist[j] >= off) break;
                    j = (j + 1) & mica.mask_hash;
                    off += 1;
                }

                mica.hash[i] = mica.hash[j];
                mica.dist[i] = @intCast(mica.dist[j] - off);
                mica.back[mica.hash[i].slot] = @intCast(i);
                i = j;
            }
        }

        fn evict_one(mica: *Self) void {
            assert(mica.len() == mica.capacity_ring);

            const slot_ring = mica.head & mica.mask_ring;
            const slot_hash = mica.back[slot_ring];
            if (slot_hash != back_invalid) {
                mica.delete_hash_slot(slot_hash);
            }

            mica.back[slot_ring] = back_invalid;
            mica.head += 1;
        }

        fn ensure_room(mica: *Self) void {
            if (mica.len() == mica.capacity_ring) mica.evict_one();
        }

        fn append_at(mica: *Self, slot_hash: usize, kv: KV) void {
            assert(mica.len() < mica.capacity_ring);
            assert(mica.hash[slot_hash].code != code_empty);

            const slot_ring = mica.tail & mica.mask_ring;
            mica.ring[slot_ring] = kv;
            mica.back[slot_ring] = @intCast(slot_hash);
            mica.hash[slot_hash].slot = @intCast(slot_ring);
            mica.tail += 1;
        }

        pub fn put(mica: *Self, kv: KV) void {
            mica.ensure_room();

            const hash = hash_from_key(kv.key);
            const fingerprint = code(hash);
            const home = mica.hash_slot(hash);
            var slot = home;

            while (true) {
                const entry = mica.hash[slot];
                if (entry.code == code_empty) {
                    const distance = (slot -% home) & mica.mask_hash;
                    assert(distance <= std.math.maxInt(u8));

                    mica.hash[slot] = Fingerprint{ .code = fingerprint, .slot = 0 };
                    mica.dist[slot] = @intCast(distance);
                    mica.count += 1;
                    mica.append_at(slot, kv);
                    return;
                }

                if (entry.code == fingerprint and
                    std.meta.eql(mica.ring[entry.slot].key, kv.key))
                {
                    mica.back[entry.slot] = back_invalid;
                    mica.append_at(slot, kv);
                    return;
                }

                slot = (slot + 1) & mica.mask_hash;
            }
        }

        pub fn insert(mica: *Self, kv: KV) void {
            mica.put(kv);
        }

        pub fn update(mica: *Self, kv: KV) void {
            // Append only to the ring buffer. This invalidates older versions, but
            // they can stay in the ring until natural FIFO eviction reaches them.
            mica.put(kv);
        }

        pub fn remove(mica: *Self, key: Key) ?KV {
            return switch (mica.find_slot(key)) {
                .found => |slot_hash| kv: {
                    const slot_ring = mica.hash[slot_hash].slot;
                    const kv = mica.ring[slot_ring];
                    mica.delete_hash_slot(slot_hash);
                    break :kv kv;
                },
                .free, .full => null,
            };
        }

        pub fn lookup(mica: *const Self, key: Key) ?Value {
            return switch (mica.find_slot(key)) {
                .found => |slot_hash| mica.ring[mica.hash[slot_hash].slot].value,
                .free, .full => null,
            };
        }

        pub fn lookup_kv(mica: *const Self, key: Key) ?KV {
            return switch (mica.find_slot(key)) {
                .found => |slot_hash| mica.ring[mica.hash[slot_hash].slot],
                .free, .full => null,
            };
        }

        pub fn lookup_value_ptr(mica: *const Self, key: Key) ?*Value {
            return switch (mica.find_slot(key)) {
                .found => |slot_hash| &@constCast(&mica.ring[mica.hash[slot_hash].slot]).value,
                .free, .full => null,
            };
        }

        pub fn contains(mica: *const Self, key: Key) bool {
            return mica.lookup(key) != null;
        }

        pub const Iterator = struct {
            mica: *const Self,
            index: usize = 0,

            pub fn next(it: *Iterator) ?*const KV {
                while (it.index < it.mica.capacity_hash) {
                    const index = it.index;
                    it.index += 1;

                    const entry = it.mica.hash[index];
                    if (entry.code != code_empty) {
                        return &it.mica.ring[entry.slot];
                    }
                }

                return null;
            }
        };

        pub fn iterator(mica: *const Self) Iterator {
            return .{ .mica = mica };
        }

        pub fn validate(mica: *const Self) void {
            var occupied: usize = 0;
            for (mica.hash, 0..) |entry, slot| {
                if (entry.code == code_empty) continue;
                occupied += 1;

                const ring_slot = entry.slot;
                assert(mica.back[ring_slot] == slot);

                const key = mica.ring[ring_slot].key;
                const hash = hash_from_key(key);
                assert(entry.code == code(hash));

                const home = mica.hash_slot(hash);
                assert(mica.dist[slot] == ((slot -% home) & mica.mask_hash));

                var s = home;
                while (s != slot) : (s = (s + 1) & mica.mask_hash) {
                    assert(mica.hash[s].code != code_empty);
                }
            }

            assert(occupied == mica.count);
        }
    };
}

pub fn Mica(
    comptime KV: type,
    comptime hash_from_key: fn (FieldType(KV, "key")) callconv(.@"inline") u64,
) type {
    return MicaType(KV, hash_from_key, capacity_ring, capacity_hash);
}

/// A CacheMap is a MICA-style ring cache backed by a linear-probed hash table. Updates append a
/// fresh copy into the ring and redirect the hash entry, leaving old versions to age out by FIFO
/// eviction. Tombstones are stored as ordinary values so point lookups can distinguish "not cached"
/// from "deleted".
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

        pub const Cache = struct {
            pub const value_count_max_multiple = 256;
        };

        pub const Options = struct {
            cache_value_count_max: u32,
            stash_value_count_max: u32,
            scope_value_count_max: u32,
            name: []const u8,
        };

        pub const Entry = struct {
            key: Key,
            value: Value,
        };

        pub const Table = Mica(Entry, hash_from_key);

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

        table: Table,

        // Scopes allow you to perform operations on the CacheMap before either persisting or
        // discarding them.
        scope_is_active: bool = false,
        scope_rollback_log: RollbackLog,

        options: Options,

        pub fn init(allocator: std.mem.Allocator, options: Options) !CacheMap {
            assert(options.stash_value_count_max > 0);
            maybe(options.cache_value_count_max == 0);
            maybe(options.scope_value_count_max == 0);

            const table_value_count_max: usize =
                @as(usize, options.cache_value_count_max) +
                @as(usize, options.stash_value_count_max) +
                @as(usize, options.scope_value_count_max);
            var table = try Table.init(allocator, table_value_count_max);
            errdefer table.deinit(allocator);

            var scope_rollback_log: RollbackLog = try .initCapacity(
                allocator,
                options.scope_value_count_max,
            );
            errdefer scope_rollback_log.deinit(allocator);

            return CacheMap{
                .table = table,
                .scope_rollback_log = scope_rollback_log,
                .options = options,
            };
        }

        pub fn deinit(self: *CacheMap, allocator: std.mem.Allocator) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.table.count <= self.table.capacity_ring);

            self.scope_rollback_log.deinit(allocator);
            self.table.deinit(allocator);
        }

        pub fn reset(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.table.count <= self.table.capacity_ring);

            self.table.reset();

            self.* = .{
                .table = self.table,
                .scope_rollback_log = self.scope_rollback_log,
                .options = self.options,
            };
        }

        pub fn has(self: *const CacheMap, key: Key) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *const CacheMap, key: Key) ?*Value {
            const object = self.table.lookup_value_ptr(key) orelse return null;
            return if (tombstone(object)) null else object;
        }

        pub fn get_or_tombstone(self: *const CacheMap, key: Key) union(enum) {
            found: *Value,
            not_found,
            tombstone,
        } {
            const object = self.table.lookup_value_ptr(key) orelse return .not_found;
            return if (tombstone(object)) .tombstone else .{ .found = object };
        }

        fn table_entry_count_for_metrics(self: *const CacheMap) u64 {
            if (self.options.cache_value_count_max == 0) return 0;
            return @min(
                @as(u64, self.table.count),
                @as(u64, self.options.cache_value_count_max),
            );
        }

        pub fn cache_entries(self: *const CacheMap) u64 {
            return self.table_entry_count_for_metrics();
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

        // Upserts the table and returns the old value in case of an update.
        fn fetch_upsert(self: *CacheMap, value: *const Value) ?Value {
            const key = key_from_value(value);
            const updated = if (self.table.lookup_kv(key)) |entry| entry.value else null;
            self.table.put(.{ .key = key, .value = value.* });
            return updated;
        }

        /// Removes a key from cache, adding a tombstone to record the action.
        /// Invariant: The key must be present in cache.
        pub fn remove(self: *CacheMap, key: Key) void {
            // Only unit tests and fuzzers call this function.
            // Assert that it is not called from production code.
            comptime assert(constants.verify);

            const old_value = (self.table.lookup_kv(key) orelse unreachable).value;
            // Cannot remove a value that has already been removed.
            assert(!tombstone(&old_value));

            const tombstone_object = tombstone_from_key(key);
            self.table.put(.{
                .key = key,
                .value = tombstone_object,
            });

            if (self.scope_is_active) {
                self.scope_rollback_log.appendAssumeCapacity(.{
                    .restore = old_value,
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
                        // consists of restoring the tombstone to the table.
                        if (constants.verify)
                            // Only unit tests and fuzzers call `remove`.
                            self.remove(key)
                        else
                            unreachable;
                    },
                    .remove => |key| {
                        // Reverting an insert consists of removing the value.
                        const removed = self.table.remove(key);
                        assert(removed != null);
                    },
                }
            }

            self.scope_rollback_log.clearRetainingCapacity();
        }

        pub fn compact(self: *CacheMap) void {
            assert(!self.scope_is_active);
            assert(self.scope_rollback_log.items.len == 0);
            assert(self.table.count <= self.table.capacity_ring);

            self.table.reset();
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
