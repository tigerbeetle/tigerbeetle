const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const mem = std.mem;

const config = @import("../config.zig");

comptime {
    assert(config.snapshot_page_size % config.sector_size == 0);
    assert(config.snapshot_page_size > 0);
}

const SnapshotPage = [config.snapshot_page_size]u8;

pub const ObjectStoreKind = enum {
    append_only,
    copy_on_write,
};

pub fn ObjectStore(comptime T: type, comptime kind: ObjectStoreKind) type {
    assert(config.snapshot_page_size % @sizeOf(T) == 0);
    assert(@sizeOf(T) > 0);

    return struct {
        const Self = @This();

        const Indexes = std.AutoHashMapUnmanaged(u128, u64);
        const IndexesDirty = std.bit_set.DynamicBitSetUnmanaged;

        objects: []align(config.sector_size) T,
        indexes: Indexes,

        /// Length of 0 indicates that we are not currently writing a snapshot.
        objects_dirty: if (kind == .copy_on_write) []T else void,
        indexes_dirty: if (kind == .copy_on_write) IndexesDirty else void,

        capacity: u64,

        snapshot_page: *align(config.sector_size) SnapshotPage,

        pub fn init(allocator: *mem.Allocator, capacity: u64) !Self {
            const objects_max_capacity = try allocator.allocAdvanced(
                T,
                config.sector_size,
                capacity,
                .exact,
            );
            errdefer allocator.free(objects_max_capacity);
            var objects = objects_max_capacity;
            objects.len = 0;

            var indexes: Indexes = .{};
            // TODO(zig): update to ensureTotalCapacity()
            try indexes.ensureCapacity(allocator, @intCast(u32, capacity));
            errdefer indexes.deinit(allocator);

            const snapshot_page = (try allocator.allocAdvanced(
                u8,
                config.sector_size,
                config.snapshot_page_size,
                .exact,
            ))[0..config.snapshot_page_size];
            errdefer allocator.free(snapshot_page);

            if (kind == .append_only) {
                return Self{
                    .objects = objects,
                    .objects_dirty = {},
                    .indexes = indexes,
                    .indexes_dirty = {},
                    .capacity = capacity,
                    .snapshot_page = snapshot_page,
                };
            } else {
                const objects_dirty_max_capacity = try allocator.alloc(T, capacity);
                errdefer allocator.free(objects_dirty_max_capacity);
                var objects_dirty = objects_dirty_max_capacity;
                objects_dirty.len = 0;

                const indexes_dirty = try IndexesDirty.initEmpty(capacity, allocator);
                errdefer indexes_dirty.deinit(allocator);

                return Self{
                    .objects = objects,
                    .indexes = indexes,
                    .objects_dirty = objects_dirty,
                    .indexes_dirty = indexes_dirty,
                    .capacity = capacity,
                    .snapshot_page = snapshot_page,
                };
            }
        }

        pub fn deinit(self: *Self, allocator: *mem.Allocator) void {
            allocator.free(self.objects.ptr[0..self.capacity]);
            self.indexes.deinit(allocator);

            if (kind == .copy_on_write) {
                allocator.free(self.objects_dirty.ptr[0..self.capacity]);
                self.indexes_dirty.deinit(allocator);
            }

            self.* = undefined;
        }

        pub fn count(self: *const Self) u64 {
            return self.objects.len;
        }

        const GetOrAppendResult = struct {
            object: *T,
            exists: bool,
        };

        pub fn get_or_append(self: *Self, id: u128) GetOrAppendResult {
            const index_get_or_put = self.indexes.getOrPutAssumeCapacity(id);

            if (index_get_or_put.found_existing) {
                const index = index_get_or_put.value_ptr.*;
                if (kind == .copy_on_write and index < self.objects_dirty.len) {
                    if (!self.indexes_dirty.isSet(index)) {
                        self.indexes_dirty.set(index);
                        self.objects_dirty[index] = self.objects[index];
                    }
                    return .{
                        .object = &self.objects_dirty[index],
                        .exists = true,
                    };
                } else {
                    if (kind == .copy_on_write) assert(!self.indexes_dirty.isSet(index));
                    return .{
                        .object = &self.objects[index],
                        .exists = true,
                    };
                }
            } else {
                assert(self.objects.len < self.capacity);
                if (kind == .copy_on_write) assert(self.objects_dirty.len <= self.objects.len);

                const index = self.objects.len;
                index_get_or_put.value_ptr.* = index;
                self.objects.len += 1;

                return .{
                    .object = &self.objects[index],
                    .exists = false,
                };
            }
        }

        pub fn append(self: *Self, id: u128) *T {
            const result = self.get_or_append(id);
            assert(!result.exists);
            return result.object;
        }

        pub fn get_const(self: *const Self, id: u128) ?*const T {
            const index = self.indexes.get(id) orelse return null;

            if (kind == .append_only) return &self.objects[index];

            if (index < self.objects_dirty.len and self.indexes_dirty.isSet(index)) {
                return &self.objects_dirty[index];
            } else {
                assert(!self.indexes_dirty.isSet(index));
                return &self.objects[index];
            }
        }

        pub fn get(self: *Self, id: u128) ?*T {
            comptime assert(kind == .copy_on_write);

            const index = self.indexes.get(id) orelse return null;
            if (index < self.objects_dirty.len) {
                if (!self.indexes_dirty.isSet(index)) {
                    self.indexes_dirty.set(index);
                    self.objects_dirty[index] = self.objects[index];
                }
                return &self.objects_dirty[index];
            } else {
                assert(!self.indexes_dirty.isSet(index));
                return &self.objects[index];
            }
        }

        pub fn rollback(self: *Self, id: u128) void {
            const index = self.indexes.fetchRemove(id).?.value;
            assert(index == self.objects.len - 1);

            if (builtin.mode == .Debug) self.objects[index] = undefined;

            self.objects.len -= 1;

            if (kind == .copy_on_write) {
                assert(!self.indexes_dirty.isSet(index));
                assert(index >= self.objects_dirty.len);
            }
        }

        // TODO: update this to return user_data including page number and a static type ID
        // from next_page() alongside the page.
        pub const Snapshot = struct {
            const objects_per_page = @divExact(config.snapshot_page_size, @sizeOf(T));

            self: *Self,
            index: u64 = 0,
            /// The value self.objects.len had when the snapshot was started.
            len: u64,
            released: bool = false,

            /// Return the snapshot pages in order.
            /// Once next_page() returns null once, it may not be called again.
            pub fn next_page(snapshot: *Snapshot) ?*align(config.sector_size) const SnapshotPage {
                const self = snapshot.self;

                if (snapshot.index == snapshot.len) {
                    assert(!snapshot.released);
                    snapshot.released = true;
                    if (kind == .copy_on_write) self.release();
                    return null;
                }
                assert(snapshot.index < snapshot.len);

                if (snapshot.index + objects_per_page > snapshot.len) {
                    assert(snapshot.len > snapshot.index);

                    mem.set(u8, self.snapshot_page, 0);
                    const objects = self.objects[snapshot.index..snapshot.len];
                    mem.copy(T, mem.bytesAsSlice(T, self.snapshot_page), objects);

                    snapshot.index += objects.len;
                    return self.snapshot_page;
                }

                defer snapshot.index += objects_per_page;
                return mem.sliceAsBytes(self.objects[snapshot.index..][0..objects_per_page]);
            }
        };

        pub fn snapshot(self: *Self) Snapshot {
            if (kind == .copy_on_write) self.acquire();
            return .{
                .self = self,
                .len = self.objects.len,
            };
        }

        fn acquire(self: *Self) void {
            comptime assert(kind == .copy_on_write);

            // assert that this object store is not currently acquired.
            assert(self.objects_dirty.len == 0);
            assert(self.indexes_dirty.count() == 0);

            self.objects_dirty.len = self.objects.len;
        }

        fn release(self: *Self) void {
            comptime assert(kind == .copy_on_write);

            // assert that this object store is currently acquired.
            assert(self.objects_dirty.len > 0);

            var it = self.indexes_dirty.iterator(.{ .kind = .set, .direction = .forward });
            while (it.next()) |index| {
                assert(self.indexes_dirty.isSet(index));

                self.objects[index] = self.objects_dirty[index];
                self.indexes_dirty.unset(index);
            }

            assert(self.indexes_dirty.count() == 0);
            self.objects_dirty.len = 0;
        }
    };
}
