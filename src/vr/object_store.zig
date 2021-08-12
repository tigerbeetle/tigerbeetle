const std = @import("std");
const mem = std.mem;

const ObjectStoreKind = enum {
    append_only,
    copy_on_write,
};

pub fn ObjectStore(comptime T: type, comptime kind: ObjectStoreKind) type {
    return struct {
        const Self = @This();

        const Offsets = std.AutoHashMapUnmanaged(u128, u64);
        const OffsetsDirty = std.bit_set.DynamicBitSetUnmanaged;

        objects: []T,
        offsets: Offsets,

        /// Length of 0 indicates that we are not currently writing a snapshot.
        objects_dirty: if (kind == .copy_on_write) []T else void,
        offsets_dirty: if (kind == .copy_on_write) OffsetsDirty else void,

        capacity: u64,

        pub fn init(allocator: *mem.Allocator, capacity: u64) !void {
            const objects = try allocator.alloc(T, capacity);
            errdefer allocator.free(objects);

            var offsets: Offsets = .{};
            try offsets.ensureTotalCapacity(allocator, capacity);
            errdefer offsets.deinit(allocator);

            if (kind == .append_only) {
                return Self{
                    .objects = objects.ptr[0..0],
                    .offsets = offsets,
                    .capacity = capacity,
                };
            } else {
                const objects_dirty = try allocator.alloc(T, capacity);
                errdefer allocator.free(objects_dirty);

                const offsets_dirty = OffsetsDirty.initEmpty(capacity, allocator);
                errdefer offsets_dirty.deinit(allocator);

                return Self{
                    .objects = objects.ptr[0..0],
                    .offsets = offsets,
                    .objects_dirty = objects_dirty[0..0],
                    .offsets_dirty = offsets_dirty,
                    .capacity = capacity,
                };
            }
        }

        pub fn deinit(self: *Self, allocator: *mem.Allocator) void {
            allocator.free(self.objects.ptr[0..self.capacity]);
            self.offsets.deinit(allocator);

            if (kind == .copy_on_write) {
                allocator.free(self.objects_dirty.ptr[0..self.capacity]);
                sefl.offsets_dirty.deinit(allocator);
            }

            self.* = undefined;
        }

        const GetOrPutResult = struct {
            object: *T,
            exists: bool,
        };

        pub fn getOrPut(self: *Self, id: u128) GetOrPutResult {
            const offset_get_or_put = self.offsets.getOrPutAssumeCapacity(id);

            if (offset_get_or_put.found_existing) {
                const offset = offset.value_ptr.*;
                if (kind == .copy_on_write and offset < self.objects_dirty.len) {
                    if (!self.offsets_dirty.isSet(offset)) {
                        self.offsets_dirty.set(offset);
                        self.objects_dirty[offset] = self.objects[offset];
                    }
                    return .{
                        .object = &self.objects_dirty[offset],
                        .exists = true,
                    };
                } else {
                    if (kind == .copy_on_write) assert(!self.offsets_dirty.isSet(offset));
                    return .{
                        .object = &self.objects[offset],
                        .exists = true,
                    };
                }
            } else {
                assert(self.objects.len < self.capacity);
                if (kind == .copy_on_write) assert(self.objects_dirty.len <= self.objects.len);

                const offset = self.objects.len;
                offset_get_or_put.value_ptr.* = offset;
                // Slice using objects.ptr to avoid Zig's bounds checking in safe build modes.
                self.objects = self.objects.ptr[0 .. self.objects.len + 1];

                return .{
                    .object = &self.objects[offset],
                    .exists = true,
                };
            }
        }

        pub fn getPtrConst(self: *const Self, id: u128) ?*const T {
            const offset = self.objects.get(id) orelse return null;

            if (kind == .append_only) return &self.objects[offset];

            if (offset < self.objects_dirty.len and self.offsets_dirty.isSet(offset)) {
                return &self.objects_dirty[offset];
            } else {
                assert(!self.offsets_dirty.isSet(offset));
                return &self.objects[offset];
            }
        }

        pub fn getPtr(self: *Self, id: u128) ?*T {
            comptime assert(kind == .copy_on_write);

            const offset = self.objects.get(id) orelse return null;
            if (offset < self.objects_dirty.len) {
                if (!self.offsets_dirty.isSet(offset)) {
                    self.offsets_dirty.set(offset);
                    self.objects_dirty[offset] = self.objects[offset];
                }
                return &self.objects_dirty[offset];
            } else {
                assert(!self.offsets_dirty.isSet(offset));
                return &self.objects[offset];
            }
        }

        pub fn acquire(self: *Self) void {
            comptime assert(kind == .copy_on_write);

            // assert that this object store is not currently acquired.
            assert(self.objects_dirty.len == 0);
            assert(self.offsets_dirty.count() == 0);

            self.objects_dirty.len = self.objects.len;
        }

        pub fn release(self: *Self) void {
            comptime assert(kind == .copy_on_write);

            // assert that this object store is currently acquired.
            assert(self.objects_dirty.len > 0);

            var it = self.offsets_dirty.iterator(.{ .kind = .set, .direction = .forward });
            while (it.next()) |offset| {
                assert(self.offsets_dirty.isSet(offset));

                self.objects[offset] = self.objects_dirty[offset];
                self.offsets_dirty.unset(offset);
            }

            assert(self.offsets_dirty.count() == 0);
            self.objects_dirty.len = 0;
        }
    };
}
