const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const OriginalAOF = @import("../aof.zig").AOF;
const AOFEntry = @import("../aof.zig").AOFEntry;

const Message = @import("../message_pool.zig").MessagePool.Message;
const log = std.log.scoped(.aof);

const backing_size = 10 * 1024 * 1024;

const InMemoryAOF = struct {
    const Self = @This();

    backing_store: []align(constants.sector_size) u8,
    index: usize,

    pub fn seekTo(self: *Self, to: usize) !void {
        self.index = to;
    }

    pub fn readAll(self: *Self, buf: []u8) !usize {
        stdx.copy_disjoint(.inexact, u8, buf, self.backing_store[self.index .. self.index + buf.len]);
        return buf.len;
    }

    pub fn close() void {}
};

pub const AOF = struct {
    index: usize,
    backing_store: []align(constants.sector_size) u8,
    validation_target: *AOFEntry,
    last_checksum: ?u128 = null,
    validation_checksums: std.AutoHashMap(u128, void) = undefined,

    pub fn init(allocator: std.mem.Allocator) !AOF {
        const memory = try allocator.allocAdvanced(u8, constants.sector_size, backing_size, .exact);
        errdefer allocator.free(memory);

        const target = try allocator.create(AOFEntry);
        errdefer allocator.free(target);

        log.debug("init. allocated {} bytes", .{backing_size});
        return AOF{
            .index = 0,
            .backing_store = memory,
            .validation_target = target,
            .validation_checksums = std.AutoHashMap(u128, void).init(allocator),
        };
    }

    pub fn deinit(self: *AOF, allocator: std.mem.Allocator) void {
        allocator.free(self.backing_store);
        allocator.destroy(self.validation_target);
        self.validation_checksums.deinit();
    }

    pub fn validate(self: *AOF, last_checksum: u128) !void {
        var it = self.iterator();

        // The iterator only does simple chain validation, but we can have backtracking
        // or duplicates, and still have a valid AOF. Handle this by keeping track of
        // every checksum we've seen so far, and considering it OK as long as we've seen
        // a parent.
        it.validate_chain = false;

        var last_entry: ?*AOFEntry = null;

        while (try it.next(self.validation_target)) |entry| {
            const header = entry.header();
            if (self.validation_checksums.count() != 0) {
                assert(self.validation_checksums.get(header.parent) != null);
            }
            try self.validation_checksums.put(header.checksum, {});

            last_entry = entry;
        }

        if (last_entry.?.header().checksum != last_checksum) {
            return error.ChecksumMismatch;
        }

        log.debug("validated all aof entries. last entry checksum {} matches supplied {}", .{ last_entry.?.header().checksum, last_checksum });
    }

    pub fn write(self: *AOF, message: *const Message, options: struct { replica: u8, primary: u8 }) !void {
        var entry: AOFEntry align(constants.sector_size) = undefined;
        entry.from_message(message, .{ .replica = options.replica, .primary = options.primary }, &self.last_checksum);

        const disk_size = entry.calculate_disk_size();
        stdx.copy_disjoint(.exact, u8, self.backing_store[self.index .. self.index + disk_size], std.mem.asBytes(&entry)[0..disk_size]);
        self.index += disk_size;

        log.debug("wrote {} bytes, {} used / {}", .{ disk_size, self.index, backing_size });
    }

    pub const Iterator = OriginalAOF.IteratorType(InMemoryAOF);

    pub fn iterator(self: *AOF) Iterator {
        const in_memory_aof = InMemoryAOF{ .backing_store = self.backing_store, .index = 0 };

        return Iterator{ .file = in_memory_aof, .size = self.index };
    }
};
