const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const OriginalAOF = @import("../aof.zig").AOF;
const AOFEntry = @import("../aof.zig").AOFEntry;

const Message = @import("../message_pool.zig").MessagePool.Message;
const log = std.log.scoped(.aof);

const BACKING_SIZE = 10 * 1024 * 1024;

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
    last_checksum: ?u128 = null,
    validation_checksums: std.AutoHashMap(u128, void) = undefined,

    pub fn init(allocator: std.mem.Allocator) !AOF {
        const memory = try allocator.allocAdvanced(u8, constants.sector_size, BACKING_SIZE, .exact);
        errdefer allocator.free(memory);

        log.info("testing aof: init. allocated {} bytes", .{BACKING_SIZE});
        return AOF{
            .index = 0,
            .backing_store = memory,
            .validation_checksums = std.AutoHashMap(u128, void).init(allocator),
        };
    }

    pub fn deinit(self: *AOF, allocator: std.mem.Allocator) void {
        allocator.free(self.backing_store);
        self.validation_checksums.deinit();
    }

    pub fn validate(self: *AOF, last_checksum: u128) !void {
        var it = self.iterator();

        // The iterator only does simple chain validation, but we can have backtracking
        // or duplicates, and still have a valid AOF. Handle this by keeping track of
        // every checksum we've seen so far, and considering it OK as long as we've seen
        // a parent.
        it.validate_chain = false;

        var last_entry: *AOFEntry = undefined;
        var target: AOFEntry = undefined;

        while (try it.next(&target)) |entry| {
            if (self.validation_checksums.count() != 0) {
                assert(self.validation_checksums.get(entry.metadata.vsr_header.parent) != null);
            }
            try self.validation_checksums.put(entry.metadata.vsr_header.checksum, {});

            last_entry = entry;
        }

        if (last_entry.metadata.vsr_header.checksum != last_checksum) {
            return error.ChecksumMismatch;
        }

        log.info("testing aof: validated all aof entries. last entry checksum {} matches supplied {}", .{ last_entry.metadata.vsr_header.checksum, last_checksum });
    }

    pub fn write(self: *AOF, message: *const Message, options: struct { replica: u8, primary: u8 }) !void {
        var entry: AOFEntry align(constants.sector_size) = undefined;
        OriginalAOF.prepare_entry(&self.last_checksum, message, .{ .replica = options.replica, .primary = options.primary }, &entry);

        const padded_size = entry.calculate_padded_size();
        stdx.copy_disjoint(.exact, u8, self.backing_store[self.index .. self.index + padded_size], std.mem.asBytes(&entry)[0..padded_size]);
        self.index += padded_size;

        log.info("testing aof: wrote {} bytes, {} used / {}", .{ padded_size, self.index, BACKING_SIZE });
    }

    pub const Iterator = OriginalAOF.IteratorType(InMemoryAOF);

    pub fn iterator(self: *AOF) Iterator {
        const in_memory_aof = InMemoryAOF{ .backing_store = self.backing_store, .index = 0 };

        return Iterator{ .file = in_memory_aof, .size = self.index };
    }
};
