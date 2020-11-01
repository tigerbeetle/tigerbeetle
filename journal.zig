const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const linux = std.os.linux;
const log = std.log.scoped(.journal);
const mem = std.mem;
const os = std.os;

const config = @import("config.zig");

usingnamespace @import("types.zig");
usingnamespace @import("state.zig");

pub const Journal = struct {
                   state: *State,
                    file: fs.File,
         hash_chain_root: u128,
    prev_hash_chain_root: u128,
                 entries: u64,
                  offset: u64,

    pub fn init(state: *State) !Journal {
        const path = "journal";
        const file = try Journal.open(path);
        errdefer file.close();

        var self = Journal {
            .state = state,
            .file = file,
            .hash_chain_root = 0,
            .prev_hash_chain_root = 0,
            .entries = 0,
            .offset = 0,
        };

        log.debug("fd={}", .{ self.file.handle });

        try self.read();
        return self;
    }

    pub fn deinit(self: *Journal) void {
        self.file.close();
    }

    /// Append a batch of events to the journal:
    /// The journal will overwrite the 64-byte header in place at the front of the buffer.
    /// The journal will also write the 64-byte eof header to the last sector of the buffer.
    /// The buffer pointer address must be aligned to config.sector_size for direct I/O.
    /// The buffer length must similarly be a multiple of config.sector_size.
    /// `size` may be less than a sector multiple, but the remainder must already be zero padded.
    pub fn append(self: *Journal, command: Command, size: u32, buffer: []u8) !void {
        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@sizeOf(JournalHeader) == @sizeOf(NetworkHeader));
        assert(size >= @sizeOf(JournalHeader));
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(buffer.len == Journal.entry_size(size, config.sector_size));
        assert(buffer.len >= size + config.sector_size);
        assert(buffer.len < size + config.sector_size + config.sector_size);

        // TODO Snapshot before the journal file can overflow in entries or size.
        // TODO Wrap around the journal file when appending and parsing.
        if (self.entries + 1 > config.journal_entries_max) return error.JournalEntriesMax;
        if (self.offset + buffer.len > config.journal_size_max) return error.JournalSizeMax;

        // Write the entry header to the front of the buffer:
        var entry = mem.bytesAsValue(JournalHeader, buffer[0..@sizeOf(JournalHeader)]);
        entry.* = .{
            .prev_checksum_meta = self.hash_chain_root,
            .offset = self.offset,
            .command = command,
            .size = size
        };
        // Zero padding is not included in the checksum since it is not material except to prevent
        // buffer bleeds, which we assert against in Debug mode:
        entry.set_checksum_data(buffer[@sizeOf(JournalHeader)..size]);
        entry.set_checksum_meta();

        if (std.builtin.mode == .Debug) {
            // Assert that the sector padding is already zeroed:
            // There should be at least one sector for the eof header.
            var sum_of_sector_padding_bytes: u32 = 0;
            for (buffer[size..]) |byte| sum_of_sector_padding_bytes += byte;
            assert(sum_of_sector_padding_bytes == 0);
        }

        // Write the eof header to the last sector of the buffer:
        var eof_buffer = buffer[buffer.len - config.sector_size..][0..@sizeOf(JournalHeader)];
        const eof = mem.bytesAsValue(JournalHeader, eof_buffer);
        eof.* = .{
            .prev_checksum_meta = entry.checksum_meta,
            .offset = entry.offset + buffer.len - config.sector_size,
            .command = .eof,
            .size = @sizeOf(JournalHeader),
        };
        eof.set_checksum_data(eof_buffer[0..0]);
        eof.set_checksum_meta();

        // Write the journal entry to the end of the journal:
        log.debug("appending {} bytes at offset {}: {} {}", .{
            buffer.len,
            self.offset,
            entry,
            eof
        });
        try self.file.pwriteAll(buffer, self.offset);
        try os.fsync(self.file.handle);

        // Update journal state:
        self.hash_chain_root = entry.checksum_meta;
        self.prev_hash_chain_root = entry.prev_checksum_meta;
        self.entries += 1;
        self.offset += buffer.len - config.sector_size;

        assert(self.entries <= config.journal_entries_max);
        assert(self.offset <= config.journal_size_max);
    }

    pub fn entry_size(request_size: u64, sector_size: u64) u64 {
        assert(request_size > 0);
        assert(sector_size > 0);
        assert(std.math.isPowerOfTwo(sector_size));
        const sectors = std.math.divCeil(u64, request_size, sector_size) catch unreachable;
        assert(sectors > 0);
        const rounded = sectors * sector_size;
        assert(rounded >= request_size);
        assert(rounded < request_size + sector_size);
        // Now add another sector for the eof journal entry:
        return rounded + sector_size;
    }

    pub fn fs_supports_direct_io(dir_fd: os.fd_t) !bool {
        if (!@hasDecl(os, "O_DIRECT")) return false;

        const path = "fs_supports_direct_io";
        const dir = fs.Dir { .fd = dir_fd };
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

    pub fn read(self: *Journal) !void {
        assert(self.hash_chain_root == 0);
        assert(self.prev_hash_chain_root == 0);
        assert(self.entries == 0);
        assert(self.offset == 0);

        var buffer: [config.request_size_max]u8 align(config.sector_size) = undefined;
        // TODO Allocate bigger output buffer:
        var output: [16384]u8 = undefined;
        assert(@mod(@ptrToInt(&buffer), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);

        while (true) {
            // TODO Handle entries that straddle the buffer.
            var bytes_read = try self.file.preadAll(buffer[0..], self.offset);
            log.debug("read[0..{}]={}", .{ buffer.len, bytes_read });

            var buffer_offset: u64 = 0;
            while (buffer_offset < bytes_read) {
                const entry = mem.bytesAsValue(
                    JournalHeader,
                    buffer[buffer_offset..][0..@sizeOf(JournalHeader)]
                );
                log.debug("{}", .{ entry });

                if (!entry.valid_checksum_meta()) return error.JournalEntryHeaderCorrupt;
                const entry_data = buffer[buffer_offset..][@sizeOf(JournalHeader)..entry.size];
                if (!entry.valid_checksum_data(entry_data)) return error.JournalEntryDataCorrupt;
                if (entry.prev_checksum_meta != self.hash_chain_root) {
                    return error.JournalEntryMisdirected;
                }
                if (entry.offset != self.offset) return error.JournalEntryOffsetInvalid;

                if (entry.command == .eof) {
                    assert(entry.size == @sizeOf(JournalHeader));
                    log.info("hash_chain_root={} prev_hash_chain_root={}", .{
                        self.hash_chain_root,
                        self.prev_hash_chain_root,
                    });
                    log.info("entries={}/{} ({}%) offset={}/{} ({}%)", .{
                        self.entries,
                        config.journal_entries_max,
                        @divFloor(self.entries * 100, config.journal_entries_max),
                        self.offset,
                        config.journal_size_max,
                        @divFloor(self.offset * 100, config.journal_size_max),
                    });
                    return;
                }

                _ = self.state.apply(entry.command, entry_data, output[0..]);

                self.hash_chain_root = entry.checksum_meta;
                self.prev_hash_chain_root = entry.prev_checksum_meta;

                // TODO
                buffer_offset += Journal.entry_size(entry.size, config.sector_size) - config.sector_size;

                self.entries += 1;
                self.offset += Journal.entry_size(entry.size, config.sector_size) - config.sector_size;

                assert(self.entries <= config.journal_entries_max);
                assert(self.offset <= config.journal_size_max);
            }

            self.offset += buffer_offset;
            if (bytes_read < buffer.len) break;
        }
    }

    fn create(path: []const u8) !fs.File {
        log.debug("creating {}...", .{ path });

        const file = try Journal.openat(std.fs.cwd().fd, path, true);

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // Some virtual file systems will not support fallocate(), and that's fine.
        log.debug("pre-allocating {} bytes...", .{ config.journal_size_max });
        Journal.fallocate(file.handle, 0, 0, config.journal_size_max) catch |err| switch (err) {
            error.OperationNotSupported => {
                log.notice("fs does not support fallocate(), file may be non-contiguous", .{});
            },
            else => return err
        };

        // Dynamically allocate a buffer to zero the file:
        // This is done only at cluster initialization and not in the critical path.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var buffer = try arena.allocator.alloc(u8, config.request_size_max);
        mem.set(u8, buffer[0..], 0);
        
        // Write zeroes to the disk to improve performance:
        // These zeroes have no semantic meaning from a journal recovery point of view.
        // We use zeroes because we have to use something and we don't want a buffer bleed.
        log.debug("zeroing {} bytes...", .{ config.journal_size_max });
        assert(@mod(config.journal_size_max, buffer.len) == 0);
        var zeroing_progress: u64 = 0;
        var zeroing_offset: u64 = 0;
        while (zeroing_offset < config.journal_size_max) {
            try file.pwriteAll(buffer, zeroing_offset);
            zeroing_offset += buffer.len;

            const percent: u64 = @divTrunc(zeroing_offset * 100, config.journal_size_max);
            if (percent - zeroing_progress >= 20 or percent == 100) {
                log.debug("zeroing... {}%", .{ percent });
                zeroing_progress = percent;
            }
        }
        assert(zeroing_offset == config.journal_size_max);

        // Write the eof header:
        const eof = mem.bytesAsValue(JournalHeader, buffer[0..@sizeOf(JournalHeader)]);
        eof.* = .{
            .prev_checksum_meta = 0, // TODO Use unique initialization state.
            .offset = 0,
            .command = .eof,
            .size = @sizeOf(JournalHeader),
        };
        eof.set_checksum_data(buffer[0..0]);
        eof.set_checksum_meta();

        log.debug("appending {} bytes at offset {}: {}", .{
            config.sector_size,
            0,
            eof
        });
        try file.pwriteAll(buffer[0..config.sector_size], 0);
        
        log.debug("fsyncing...", .{});
        try os.fsync(file.handle);
        // TODO Open parent directory to fsync the directory inode.

        return file;
    }

    fn fallocate(fd: os.fd_t, mode: i32, offset: u64, len: u64) !void {
        switch (linux.getErrno(Journal.fallocate_syscall(fd, mode, offset, len))) {
            0 => {},
            linux.EBADF => return error.FileDescriptorInvalid,
            linux.EFBIG => return error.FileTooBig,
            linux.EINVAL => return error.ArgumentsInvalid,
            linux.EIO => return error.InputOutput,
            linux.ENODEV => return error.NoDevice,
            linux.ENOSPC => return error.NoSpaceLeft,
            linux.ENOSYS => return error.SystemOutdated,
            linux.EOPNOTSUPP => return error.OperationNotSupported,
            linux.EPERM => return error.PermissionDenied,
            linux.ESPIPE => return error.Unseekable,
            linux.ETXTBSY => return error.FileBusy,
            else => |errno| return os.unexpectedErrno(errno)
        }
    }

    fn fallocate_syscall(fd: os.fd_t, mode: i32, offset: u64, len: u64) usize {
        if (@sizeOf(usize) == 4) {
            return linux.syscall6(
                .fallocate,
                @bitCast(usize, @as(isize, fd)),
                @bitCast(usize, @as(isize, mode)),
                @truncate(usize, offset),
                @truncate(usize, offset >> 32),
                @truncate(usize, len),
                @truncate(usize, len >> 32),
            );
        } else {
            return linux.syscall4(
                .fallocate,
                @bitCast(usize, @as(isize, fd)),
                @bitCast(usize, @as(isize, mode)),
                offset,
                len,
            );
        }
    }

    fn open(path: []const u8) !fs.File {
        // TODO Figure out absolute path to journal file regardless of the server's cwd.
        log.debug("opening {}...", .{ path });
        return Journal.openat(std.fs.cwd().fd, path, false) catch |err| switch (err) {
            error.FileNotFound => return try Journal.create(path),
            else => return err,
        };
    }

    fn openat(dir_fd: os.fd_t, path: []const u8, creating: bool) !fs.File {
        var flags: u32 = os.O_CLOEXEC | os.O_RDWR;
        var mode: fs.File.Mode = 0;

        if (@hasDecl(os, "O_LARGEFILE")) flags |= os.O_LARGEFILE;

        if (config.direct_io) {
            const direct_io_supported = try Journal.fs_supports_direct_io(dir_fd);
            if (direct_io_supported) {
                flags |= os.O_DIRECT;
            } else if (config.deployment_environment == .development) {
                log.warn("file system does not support direct i/o", .{});
            } else {
                @panic("file system does not support direct i/o");
            }
        }

        if (creating) {
            flags |= os.O_CREAT;
            flags |= os.O_EXCL;
            mode = 0o666;
        }
        
        const path_c = try os.toPosixPath(path);
        const fd = try os.openatZ(dir_fd, &path_c, flags, mode);

        return fs.File {
            .handle = fd,
            .capable_io_mode = .blocking,
            .intended_io_mode = .blocking,
        };
    }
};

const testing = std.testing;

test "entry_size()" {
    const sector_size: u64 = 4096;
    testing.expectEqual(sector_size * 2, Journal.entry_size(1, sector_size));
    testing.expectEqual(sector_size * 2, Journal.entry_size(sector_size - 1, sector_size));
    testing.expectEqual(sector_size * 2, Journal.entry_size(sector_size, sector_size));
    testing.expectEqual(sector_size * 3, Journal.entry_size(sector_size + 1, sector_size));
}
