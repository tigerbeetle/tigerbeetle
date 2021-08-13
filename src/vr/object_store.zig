const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const mem = std.mem;

pub const ObjectStoreKind = enum {
    append_only,
    copy_on_write,
};

pub fn ObjectStore(comptime T: type, comptime kind: ObjectStoreKind) type {
    return struct {
        const Self = @This();

        const Indexes = std.AutoHashMapUnmanaged(u128, u64);
        const IndexesDirty = std.bit_set.DynamicBitSetUnmanaged;

        objects: []T,
        indexes: Indexes,

        /// Length of 0 indicates that we are not currently writing a snapshot.
        objects_dirty: if (kind == .copy_on_write) []T else void,
        indexes_dirty: if (kind == .copy_on_write) IndexesDirty else void,

        capacity: u64,

        pub fn init(allocator: *mem.Allocator, capacity: u64) !Self {
            const objects_max_capacity = try allocator.alloc(T, capacity);
            errdefer allocator.free(objects_max_capacity);
            var objects = objects_max_capacity;
            objects.len = 0;

            var indexes: Indexes = .{};
            // TODO(zig): update to ensureTotalCapacity()
            try indexes.ensureCapacity(allocator, @intCast(u32, capacity));
            errdefer indexes.deinit(allocator);

            if (kind == .append_only) {
                return Self{
                    .objects = objects,
                    .objects_dirty = {},
                    .indexes = indexes,
                    .indexes_dirty = {},
                    .capacity = capacity,
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

        pub fn acquire(self: *Self) void {
            comptime assert(kind == .copy_on_write);

            // assert that this object store is not currently acquired.
            assert(self.objects_dirty.len == 0);
            assert(self.indexes_dirty.count() == 0);

            self.objects_dirty.len = self.objects.len;
        }

        pub fn release(self: *Self) void {
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
