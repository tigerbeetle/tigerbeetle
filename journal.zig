const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const log = std.log.scoped(.journal);
const mem = std.mem;
const os = std.os;

const config = @import("config.zig");

usingnamespace @import("types.zig");

pub const Journal = struct {
                    file: fs.File,
         hash_chain_root: u128,
    prev_hash_chain_root: u128,
                    size: u64,

    pub fn init() !Journal {
        const path = "journal";
        const file = try Journal.open(path);
        errdefer file.close();

        var self = Journal {
            .file = file,
            .hash_chain_root = 0,
            .prev_hash_chain_root = 0,
            .size = 0,
        };

        log.debug("opened fd={}", .{ self.file.handle });

        var buffer = [_]u8{0} ** 4194304;
        var offset: u64 = 0;
        while (true) {
            var bytes_read = try file.preadAll(buffer[0..], offset);
            log.debug("read[0..{}]={}", .{ buffer.len, bytes_read });
            // TODO Apply to state machine.
            if (bytes_read < buffer.len) break;
        }

        return self;
    }

    pub fn append(self: *Journal, request: *const NetworkHeader, buffer: []u8) !void {
        // TODO After AlphaBeetle, move fs syscalls to io_uring.

        assert(@sizeOf(JournalHeader) == @sizeOf(NetworkHeader));
        assert(buffer.len >= @sizeOf(NetworkHeader));
        assert(buffer.len >= request.size);

        // Copy the request header checksum so that we can detect use-after-free:
        // This can happen if the caller deserialized with a cast by reference instead of by value.
        const checksum_meta = request.checksum_meta;

        // Now reuse the request header memory for the journal header:
        var entry = mem.bytesAsValue(JournalHeader, buffer[0..@sizeOf(JournalHeader)]);
        entry.* = .{
            .prev_hash_chain_root = self.hash_chain_root,
            .command = request.command,
            .size = request.size
        };
        entry.set_checksum_and_hash_chain_root(buffer[0..entry.size]);

        // Guard against the request header being a pointer to the same memory:
        assert(request.checksum_meta == checksum_meta);
        
        // Round up to next sector size for sector-aligned disk writes:
        const entry_sector_size = Journal.round_up_to_sector(entry.size, config.sector_size);
        assert(entry_sector_size >= entry.size);
        assert(entry_sector_size < entry.size + config.sector_size);

        // Zero the padding after the entry:
        // TODO Does mem.set fail if slice range is 0 bytes?
        mem.set(u8, buffer[entry.size..entry_sector_size], 0);
        assert(entry.valid_checksum(buffer[0..entry.size]));

        // Write the journal entry to the end of the journal:
        log.debug("writing {} bytes to offset {}: {}", .{
            entry_sector_size,
            self.size,
            entry
        });
        try self.file.pwriteAll(buffer[0..entry_sector_size], self.size);

        // TODO Fsync and panic on fsync failure.

        // Update journal state:
        self.hash_chain_root = entry.hash_chain_root;
        self.prev_hash_chain_root = entry.prev_hash_chain_root;
        self.size += entry_sector_size;
    }

    pub fn deinit(self: *Journal) void {
        self.file.close();
    }

    fn open(path: []const u8) !fs.File {
        // TODO Figure out absolute path to journal file regardless of the server's cwd.
        log.debug("opening {}...", .{ path });
        return std.fs.cwd().openFile(path, .{
            .read = true,
            .write = true,
            .lock = .Exclusive,
        }) catch |err| switch (err) {
            error.FileNotFound => {
                log.debug("creating {}...", .{ path });
                return std.fs.cwd().createFile(path, .{
                    .read = true,
                    .exclusive = true,
                    .truncate = false,
                    .lock = .Exclusive,
                    .mode = 0o666,
                });
            },
            else => return err,
        };
    }

    fn round_up_to_sector(entry_size: u64, sector_size: u64) u64 {
        assert(entry_size > 0);
        assert(sector_size > 0);
        assert(std.math.isPowerOfTwo(sector_size));
        const sectors = std.math.divCeil(u64, entry_size, sector_size) catch unreachable;
        assert(sectors > 0);
        const rounded = sectors * sector_size;
        assert(rounded >= entry_size);
        assert(rounded < entry_size + sector_size);
        return rounded;
    }
};

const testing = std.testing;

test "round_up_to_sector()" {
    const sector_size: u64 = 4096;
    testing.expectEqual(sector_size, Journal.round_up_to_sector(1, sector_size));
    testing.expectEqual(sector_size, Journal.round_up_to_sector(sector_size - 1, sector_size));
    testing.expectEqual(sector_size, Journal.round_up_to_sector(sector_size, sector_size));
    testing.expectEqual(sector_size * 2, Journal.round_up_to_sector(sector_size + 1, sector_size));
}
