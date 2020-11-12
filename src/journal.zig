const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const linux = std.os.linux;
const log = std.log.scoped(.journal);
const mem = std.mem;
const Allocator = mem.Allocator;
const os = std.os;

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("state.zig");

pub const Journal = struct {
               allocator: *Allocator,
                   state: *State,
                    file: fs.File,
         hash_chain_root: u128,
    prev_hash_chain_root: u128,
                 headers: []JournalHeader align(config.sector_size),
                 entries: u64,
                  offset: u64,

    pub fn init(allocator: *Allocator, state: *State) !Journal {
        const path = "journal";
        const file = try Journal.open(path);
        errdefer file.close();

        var headers = try allocator.allocAdvanced(
            JournalHeader,
            config.sector_size,
            config.journal_entries_max,
            .exact
        );
        errdefer allocator.free(headers);
        mem.set(u8, mem.sliceAsBytes(headers), 0);
        
        var self = Journal {
            .allocator = allocator,
            .state = state,
            .file = file,
            .hash_chain_root = 0,
            .prev_hash_chain_root = 0,
            .headers = headers,
            .entries = 0,
            .offset = @sizeOf(JournalHeader) * headers.len,
        };
        assert(@mod(self.offset, config.sector_size) == 0);
        assert(@mod(@ptrToInt(&headers[0]), config.sector_size) == 0);

        log.debug("fd={}", .{ self.file.handle });

        try self.recover();
        return self;
    }

    pub fn deinit(self: *Journal) void {
        self.file.close();
    }

    /// Append a batch of events to the journal:
    /// - The journal will overwrite the 64-byte header in place at the front of the buffer.
    /// - The journal will also write the 64-byte EOF entry to the last sector of the buffer.
    /// - `size` may be less than a sector multiple, but the remainder must already be zero padded.
    /// - The buffer pointer address must be aligned to `config.sector_size` for direct I/O.
    /// - The buffer length must similarly be a multiple of `config.sector_size`.
    pub fn append(self: *Journal, command: Command, size: u32, buffer: []u8) !void {
        assert(command != .eof);
        assert(command != .ack);

        assert(@sizeOf(JournalHeader) == @sizeOf(NetworkHeader));
        assert(size >= @sizeOf(JournalHeader));
        
        assert(buffer.len == Journal.append_size(size));
        assert(buffer.len >= size + config.sector_size);
        assert(buffer.len < size + config.sector_size + config.sector_size);

        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);

        // TODO Snapshot before the journal file can overflow in entries or size.
        // TODO Wrap around the journal file when appending and parsing.
        if (self.entries + 2 > config.journal_entries_max) @panic("journal entries full");
        if (self.offset + buffer.len > config.journal_size_max) @panic("journal size full");

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
            var sum_of_sector_padding_bytes: u32 = 0;
            for (buffer[size..]) |byte| sum_of_sector_padding_bytes += byte;
            assert(sum_of_sector_padding_bytes == 0);
        }

        // Write the EOF entry to the last sector of the buffer:
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

        // Write the request entry and EOF entry to the tail of the journal:
        log.debug("appending: {}", .{ entry });
        log.debug("appending: {}", .{ eof });

        // Write the request entry and EOF entry headers to the head of the journal:
        assert(self.headers[self.entries].command == .eof);
        assert(self.headers[self.entries].prev_checksum_meta == entry.prev_checksum_meta);
        self.headers[self.entries + 0] = entry.*;
        self.headers[self.entries + 1] = eof.*;

        var headers_offset = Journal.sector_floor(self.entries * @sizeOf(JournalHeader));
        var headers_length = Journal.sector_ceil((self.entries + 2) * @sizeOf(JournalHeader));
        const headers = mem.sliceAsBytes(self.headers)[headers_offset..headers_length];

        // Submit these writes according to where the last write took place:
        // e.g. If the disk last wrote the headers then write the headers first for better locality.
        if (config.journal_disk_scheduler == .elevator and (self.entries & 1) == 0) {
            self.write(headers, headers_offset);
            self.write(buffer, self.offset);
        } else {
            self.write(buffer, self.offset);
            self.write(headers, headers_offset);
        }

        // Update journal state:
        self.hash_chain_root = entry.checksum_meta;
        self.prev_hash_chain_root = entry.prev_checksum_meta;
        self.entries += 1;
        self.offset += buffer.len - config.sector_size;

        assert(self.entries < config.journal_entries_max);
        assert(self.offset < config.journal_size_max);
    }

    fn sector_floor(offset: u64) u64 {
        const sectors = std.math.divFloor(u64, offset, config.sector_size) catch unreachable;
        return sectors * config.sector_size;
    }

    fn sector_ceil(offset: u64) u64 {
        const sectors = std.math.divCeil(u64, offset, config.sector_size) catch unreachable;
        return sectors * config.sector_size;
    }

    /// Returns the sector multiple size of a batch, plus a sector for the EOF entry.
    pub fn append_size(request_size: u64) u64 {
        assert(request_size > 0);
        const sector_multiple = Journal.sector_ceil(request_size);
        assert(sector_multiple >= request_size);
        assert(sector_multiple < request_size + config.sector_size);
        // Now add another sector for the EOF entry:
        return sector_multiple + config.sector_size;
    }

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
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

    fn read(self: *Journal, buffer: []u8, offset: u64) void {
        log.debug("read(buffer.len={} offset={})", .{ buffer.len, offset });

        assert(buffer.len > 0);
        assert(offset + buffer.len <= config.journal_size_max);

        // Ensure that the read is aligned correctly for Direct I/O:
        // If this is not the case, the underlying read syscall will return EINVAL.
        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(offset, config.sector_size) == 0);

        if (self.file.preadAll(buffer, offset)) |bytes_read| {
            if (bytes_read != buffer.len) {
                log.debug("short read: bytes_read={} buffer_len={}", .{ bytes_read, buffer.len });
                @panic("fs corruption: journal file size truncated");
            }
        } else |err| {
            if (err == error.InputOutput) {
                // The disk was unable to read some sectors (an internal crc or hardware failure):
                if (buffer.len > config.sector_size) {
                    log.warn("latent sector error, subdividing read...", .{});
                    // Subdivide the read into sectors to read around the faulty sector(s):
                    // This is considerably slower than doing a bulk read.
                    // By now we might also have experienced the disk's read timeout (in seconds).
                    var position = offset;
                    const length = offset + buffer.len;
                    while (position < length) : (position += config.sector_size) {
                        self.read(buffer[position..][0..config.sector_size], position);
                    }
                } else {
                    // Zero any remaining sectors that cannot be read:
                    // We treat these EIO errors the same as a checksum failure.
                    log.warn("latent sector error at offset {}, zeroing sector...", .{ offset });
                    mem.set(u8, buffer, 0);
                }
            } else {
                log.emerg("impossible read: err={}", .{ err });
                @panic("impossible read");
            }
        }
    }

    fn write(self: *Journal, buffer: []const u8, offset: u64) void {
        log.debug("write(buffer.len={} offset={})", .{ buffer.len, offset });

        assert(buffer.len > 0);
        assert(offset + buffer.len <= config.journal_size_max);

        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(offset, config.sector_size) == 0);

        self.file.pwriteAll(buffer, offset) catch |err| switch (err) {
            error.InputOutput => @panic("latent sector error: no spare sectors to reallocate"),
            else => {
                log.emerg("write: error={} buffer.len={} offset={}", .{ err, buffer.len, offset });
                @panic("unrecoverable disk error");
            }
        };
    }

    pub fn recover(self: *Journal) !void {
        assert(self.hash_chain_root == 0);
        assert(self.prev_hash_chain_root == 0);
        assert(self.entries == 0);
        assert(self.offset == config.journal_entries_max * @sizeOf(JournalHeader));

        var buffer = try self.allocator.allocAdvanced(
            u8,
            config.sector_size,
            config.request_size_max,
            .exact
        );
        defer self.allocator.free(buffer);
        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(config.journal_size_max, buffer.len) == 0);
        assert(buffer.len > @sizeOf(JournalHeader));

        var state_output = try self.allocator.alloc(u8, config.response_size_max);
        defer self.allocator.free(state_output);

        // Read entry headers from the head of the journal:
        self.read(mem.sliceAsBytes(self.headers), 0);

        // Read entry headers and entry data from the body of the journal:
        while (self.offset < config.journal_size_max) {
            self.read(buffer[0..], self.offset);

            var offset: u64 = 0;
            while (offset < buffer.len) {
                if (offset + @sizeOf(JournalHeader) > buffer.len) break;

                // TODO Repair headers at the head of the journal in memory.
                // TODO Repair headers at the head of the journal on disk.
                const d = &self.headers[self.entries];
                const e = mem.bytesAsValue(
                    JournalHeader,
                    buffer[offset..][0..@sizeOf(JournalHeader)]
                );

                log.debug("d = {}", .{ d });
                log.debug("e = {}", .{ e });

                if (!d.valid_checksum_meta()) @panic("corrupt header");
                if (d.prev_checksum_meta != self.hash_chain_root) @panic("misdirected");
                if (d.offset != self.offset) @panic("bad offset");

                if (self.entries > 0) {
                    const p = self.headers[self.entries - 1];
                    assert(d.prev_checksum_meta == p.checksum_meta);
                    assert(d.offset == p.offset + Journal.sector_ceil(p.size));
                }

                if (!e.valid_checksum_meta()) @panic("corrupt header");
                if (e.prev_checksum_meta != self.hash_chain_root) @panic("misdirected");
                if (e.offset != self.offset) @panic("bad offset");

                if (e.checksum_meta != d.checksum_meta) @panic("different headers");
                assert(e.command == d.command);
                assert(e.size == d.size);

                // Re-read the entry into the buffer, but starting from the beginning of the buffer:
                if (offset + e.size > buffer.len) {
                    // Assert that the buffer is sufficient for the entry to avoid an infinite loop:
                    assert(buffer.len >= e.size);
                    break;
                }

                const entry_data = buffer[offset..][@sizeOf(JournalHeader)..e.size];
                if (!e.valid_checksum_data(entry_data)) @panic("corrupt entry data");

                if (e.command == .eof) {
                    assert(e.size == @sizeOf(JournalHeader));
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
                    // We exclude the EOF entry from self.entries and from self.offset:
                    // This means the offset is ready for the next append, which overwrites the EOF.
                    return;
                }

                _ = self.state.apply(e.command, entry_data, state_output[0..]);

                self.hash_chain_root = e.checksum_meta;
                self.prev_hash_chain_root = e.prev_checksum_meta;

                self.entries += 1;
                self.offset += Journal.sector_ceil(e.size);
                offset += Journal.sector_ceil(e.size);

                // We have not yet encountered the EOF entry, so there must be free space remaining:
                assert(self.entries < config.journal_entries_max);
                assert(self.offset < config.journal_size_max);
            }
        }
        assert(self.offset == config.journal_size_max);
        @panic("eof entry not found");
    }

    /// Creates an empty journal file:
    /// - Calls fallocate() to allocate contiguous disk sectors (if possible).
    /// - Zeroes the entire file to force allocation and improve performance (e.g. on EBS volumes).
    /// - Writes an EOF entry.
    fn create(path: []const u8) !fs.File {
        log.info("creating {}...", .{ path });

        const file = try Journal.openat(std.fs.cwd().fd, path, true);

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // Some file systems will not support fallocate(), that's fine, but could mean more seeks.
        log.debug("pre-allocating {} bytes...", .{ config.journal_size_max });
        Journal.fallocate(file.handle, 0, 0, config.journal_size_max) catch |err| switch (err) {
            error.OperationNotSupported => {
                log.notice("file system does not support fallocate()", .{});
            },
            else => return err
        };

        // Dynamically allocate a buffer to zero the file:
        // This is done only at cluster initialization and not in the critical path.
        // TODO Use allocator passed to Journal.init() once we have support for cluster init.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var allocator = &arena.allocator;
        var buffer = try allocator.allocAdvanced(
            u8,
            config.sector_size,
            config.request_size_max,
            .exact
        );
        defer allocator.free(buffer);
        mem.set(u8, buffer[0..], 0);
        
        // Write zeroes to the disk to improve performance:
        // These zeroes have no semantic meaning from a journal recovery point of view.
        // We use zeroes because we have to use something and we don't want a buffer bleed.
        log.debug("zeroing {} bytes...", .{ config.journal_size_max });
        assert(@mod(config.journal_size_max, buffer.len) == 0);
        var zeroing_progress: u64 = 0;
        var zeroing_offset: u64 = 0;
        while (zeroing_offset < config.journal_size_max) {
            assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
            assert(@mod(buffer.len, config.sector_size) == 0);
            assert(@mod(zeroing_offset, config.sector_size) == 0);
            try file.pwriteAll(buffer, zeroing_offset);
            zeroing_offset += buffer.len;

            const zeroing_percent: u64 = @divTrunc(zeroing_offset * 100, config.journal_size_max);
            if (zeroing_percent - zeroing_progress >= 20 or zeroing_percent == 100) {
                log.debug("zeroing... {}%", .{ zeroing_percent });
                zeroing_progress = zeroing_percent;
            }
        }
        assert(zeroing_offset == config.journal_size_max);

        // Write the EOF entry to the head of the journal, and to the body of the journal:
        const eof_head_offset = 0;
        const eof_body_offset = config.journal_entries_max * @sizeOf(JournalHeader);
        assert(@mod(eof_body_offset, config.sector_size) == 0);
        const eof = mem.bytesAsValue(JournalHeader, buffer[0..@sizeOf(JournalHeader)]);
        eof.* = .{
            .prev_checksum_meta = 0, // TODO Use unique initialization state.
            .offset = eof_body_offset,
            .command = .eof,
            .size = @sizeOf(JournalHeader),
        };
        eof.set_checksum_data(buffer[0..0]);
        eof.set_checksum_meta();

        log.debug("write(buffer.len={} offset={}): {}", .{
            config.sector_size,
            eof_body_offset,
            eof
        });
        try file.pwriteAll(buffer[0..config.sector_size], eof_body_offset);
        
        log.debug("write(buffer.len={} offset={}): {}", .{
            config.sector_size,
            eof_head_offset,
            eof
        });
        try file.pwriteAll(buffer[0..config.sector_size], eof_head_offset);
        
        // TODO Open parent directory to fsync the directory inode (and recurse for all ancestors).

        return file;
    }

    /// Pending https://github.com/ziglang/zig/pull/6895
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

    /// Opens an existing journal file.
    fn open(path: []const u8) !fs.File {
        // TODO Figure out absolute path to journal file regardless of the server's cwd.
        log.debug("opening {}...", .{ path });
        return Journal.openat(std.fs.cwd().fd, path, false) catch |err| switch (err) {
            error.FileNotFound => return try Journal.create(path),
            else => return err,
        };
    }

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    fn openat(dir_fd: os.fd_t, path: []const u8, creating: bool) !fs.File {
        var flags: u32 = os.O_CLOEXEC | os.O_RDWR | os.O_DSYNC;
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
        
        // This is critical since we rely on O_DSYNC to fsync():
        assert((flags & os.O_DSYNC) > 0);

        const path_c = try os.toPosixPath(path);
        const fd = try os.openatZ(dir_fd, &path_c, flags, mode);
        errdefer os.close(fd);

        try os.flock(fd, os.LOCK_EX);

        return fs.File {
            .handle = fd,
            .capable_io_mode = .blocking,
            .intended_io_mode = .blocking,
        };
    }
};

const testing = std.testing;

test "append_size()" {
    const sector_size: u64 = config.sector_size;
    testing.expectEqual(sector_size * 2, Journal.append_size(1));
    testing.expectEqual(sector_size * 2, Journal.append_size(sector_size - 1));
    testing.expectEqual(sector_size * 2, Journal.append_size(sector_size));
    testing.expectEqual(sector_size * 3, Journal.append_size(sector_size + 1));
}
