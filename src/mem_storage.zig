const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);

const config = @import("config.zig");

pub const MemStorage = struct {
    allocator: *Allocator,
    memory: []align(config.sector_size) u8,
    size: u64,

    pub fn init(allocator: *Allocator, size: u64) !MemStorage {
        var memory = try allocator.allocAdvanced(u8, config.sector_size, size, .exact);
        errdefer allocator.free(memory);
        std.mem.set(u8, memory, 0);

        return MemStorage{
            .allocator = allocator,
            .memory = memory,
            .size = size,
        };
    }

    pub fn deinit() void {
        self.allocator.free(self.memory);
    }

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
    pub fn fs_supports_direct_io(dir_fd: std.os.fd_t) !bool {
        if (!@hasDecl(std.os, "O_DIRECT")) return false;

        const os = std.os;
        const path = "fs_supports_direct_io";
        const dir = fs.Dir{ .fd = dir_fd };
        const fd = try os.openatZ(dir_fd, path, os.O_CLOEXEC | os.O_CREAT | os.O_TRUNC, 0o666);
        defer os.close(fd);
        defer dir.deleteFile(path) catch {};

        while (true) {
            const res = os.system.openat(dir_fd, path, os.O_CLOEXEC | os.O_RDONLY | os.O_DIRECT, 0);
            switch (linux.getErrno(res)) {
                0 => {
                    os.close(@intCast(os.fd_t, res));
                    return true;
                },
                linux.EINTR => continue,
                linux.EINVAL => return false,
                else => |err| return os.unexpectedErrno(err),
            }
        }
    }

    pub fn read(self: *MemStorage, buffer: []u8, offset: u64) void {
        self.assert_bounds_and_alignment(buffer, offset);

        if (self.read_all(buffer, offset)) |bytes_read| {
            if (bytes_read != buffer.len) {
                assert(bytes_read < buffer.len);
                log.emerg("short read: bytes_read={} buffer_len={} offset={}", .{
                    bytes_read,
                    buffer.len,
                    offset,
                });
                @panic("fs corruption: file inode size truncated");
            }
        } else |err| switch (err) {
            error.InputOutput => {
                // The disk was unable to read some sectors (an internal CRC or hardware failure):
                if (buffer.len > config.sector_size) {
                    log.err("latent sector error: offset={}, subdividing read...", .{offset});
                    // Subdivide the read into sectors to read around the faulty sector(s):
                    // This is considerably slower than doing a bulk read.
                    // By now we might have also experienced the disk's read timeout (in seconds).
                    // TODO Docs should instruct on why and how to reduce disk firmware timeouts.
                    var buffer_offset = 0;
                    while (buffer_offset < buffer.len) : (buffer_offset += config.sector_size) {
                        self.read(
                            buffer[buffer_offset..][0..config.sector_size],
                            offset + buffer_offset,
                        );
                    }
                    assert(buffer_offset == buffer.len);
                } else {
                    // Zero any remaining sectors that cannot be read:
                    // We treat these EIO errors the same as a checksum failure.
                    log.err("latent sector error: offset={}, zeroing buffer sector...", .{offset});
                    assert(buffer.len == config.sector_size);
                    mem.set(u8, buffer, 0);
                }
            },
            else => {
                log.emerg("impossible read: buffer_len={} offset={} error={}", .{
                    buffer_len,
                    offset,
                    err,
                });
                @panic("impossible read");
            },
        }
    }

    pub fn write(self: *MemStorage, buffer: []const u8, offset: u64) void {
        self.assert_bounds_and_alignment(buffer, offset);
        self.write_all(buffer, offset) catch |err| switch (err) {
            // We assume that the disk will attempt to reallocate a spare sector for any LSE.
            // TODO What if we receive an EIO error because of a faulty cable?
            error.InputOutput => @panic("latent sector error: no spare sectors to reallocate"),
            else => {
                log.emerg("write: buffer.len={} offset={} error={}", .{ buffer.len, offset, err });
                @panic("unrecoverable disk error");
            },
        };
    }

    fn assert_bounds_and_alignment(self: *MemStorage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= self.size);

        // Ensure that the read or write is aligned correctly for Direct I/O:
        // If this is not the case, the underlying syscall will return EINVAL.
        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(offset, config.sector_size) == 0);
    }

    fn read_all(self: *MemStorage, buffer: []u8, offset: u64) !u64 {
        std.mem.copy(u8, buffer, self.memory[offset .. offset + buffer.len]);
        return buffer.len;
    }

    fn write_all(self: *MemStorage, buffer: []const u8, offset: u64) !void {
        std.mem.copy(u8, self.memory[offset .. offset + buffer.len], buffer);
    }
};
