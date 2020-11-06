const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const linux = std.os.linux;
const log = std.log.scoped(.journal);
const mem = std.mem;
const Allocator = mem.Allocator;
const os = std.os;

const config = @import("config.zig");

usingnamespace @import("types.zig");
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
    /// - The buffer pointer address must be aligned to `config.sector_size` for direct I/O.
    /// - The buffer length must similarly be a multiple of `config.sector_size`.
    /// - `size` may be less than a sector multiple, but the remainder must already be zero padded.
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
        log.debug("appending {} bytes at offset {}: {} {}", .{
            buffer.len,
            self.offset,
            entry,
            eof
        });
        try self.file.pwriteAll(buffer, self.offset);

        // Write the request entry and EOF entry headers to the head of the journal:
        // TODO Use a temporary buffer to avoid dirtying the buffer before it's on disk.
        assert(self.headers[self.entries].prev_checksum_meta == entry.prev_checksum_meta);
        assert(self.headers[self.entries].command == .eof);
        self.headers[self.entries] = entry.*;
        self.headers[self.entries + 1] = eof.*;

        var headers_start = self.entries * @sizeOf(JournalHeader);
        var headers_start_down = @divFloor(headers_start, config.sector_size);
        var headers_end = headers_start + @sizeOf(JournalHeader) + @sizeOf(JournalHeader);
        var headers_end_up = try std.math.divCeil(u64, headers_end, config.sector_size);
        headers_end_up = headers_end_up * config.sector_size;
        log.debug("headers_start={} headers_end={} entries={}", .{
            headers_start_down,
            headers_end_up,
            self.entries,
        });

        // TODO Fix Direct I/O alignment:
        //try self.file.pwriteAll(mem.sliceAsBytes(self.headers)[headers_start_down..headers_end_up], headers_start_down);

        try os.fsync(self.file.handle);

        // Update journal state:
        self.hash_chain_root = entry.checksum_meta;
        self.prev_hash_chain_root = entry.prev_checksum_meta;
        self.entries += 1;
        self.offset += buffer.len - config.sector_size;

        assert(self.entries < config.journal_entries_max);
        assert(self.offset < config.journal_size_max);
    }

    /// Returns the sector multiple size of a batch, plus a sector for the EOF entry.
    pub fn entry_size(request_size: u64, sector_size: u64) u64 {
        assert(request_size > 0);
        assert(sector_size > 0);
        assert(std.math.isPowerOfTwo(sector_size));
        const sectors = std.math.divCeil(u64, request_size, sector_size) catch unreachable;
        assert(sectors > 0);
        const rounded = sectors * sector_size;
        assert(rounded >= request_size);
        assert(rounded < request_size + sector_size);
        // Now add another sector for the EOF entry:
        return rounded + sector_size;
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

    pub fn read(self: *Journal, buffer: []u8, offset: u64) void {
        log.debug("read[{}..{}]", .{ offset, offset + buffer.len });

        assert(buffer.len > 0);
        assert(offset + buffer.len <= config.journal_size_max);
        // Ensure that the read is aligned correctly for Direct I/O:
        // If this is not the case, the underlying read(2) syscall will return EINVAL.
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

    pub fn recover(self: *Journal) !void {
        assert(self.hash_chain_root == 0);
        assert(self.prev_hash_chain_root == 0);
        assert(self.entries == 0);
        assert(self.offset == config.journal_entries_max * @sizeOf(JournalHeader));

        var buffer: [config.request_size_max]u8 align(config.sector_size) = undefined;
        // TODO Allocate bigger output buffer:
        var output: [16384]u8 = undefined;
        assert(@mod(@ptrToInt(&buffer), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(config.journal_size_max, buffer.len) == 0);

        // Read redundant entry headers from the head of the journal:
        self.read(mem.sliceAsBytes(self.headers), 0);

        // Read entry headers and data from the body of the journal:        
        while (self.offset < config.journal_size_max) {
            self.read(buffer[0..], self.offset);

            var offset = self.offset;
            var buffer_offset: u64 = 0;
            while (buffer_offset < buffer.len) {
                if (buffer_offset + @sizeOf(JournalHeader) > buffer.len) break;

                var header = &self.headers[self.entries];
                log.debug("header = {}", .{ header });

                const entry = mem.bytesAsValue(
                    JournalHeader,
                    buffer[buffer_offset..][0..@sizeOf(JournalHeader)]
                );
                log.debug("entry = {}", .{ entry });

                // TODO Fix Direct I/O alignment when writing to re-enable this:
                if (false) {
                    if (!header.valid_checksum_meta()) @panic("corrupt header");
                    if (self.entries > 0) {
                        const prev_header = self.headers[self.entries - 1];
                        if (header.prev_checksum_meta != prev_header.checksum_meta) {
                            @panic("misdirected header");
                        }
                        const prev_size = Journal.entry_size(prev_header.size, config.sector_size);
                        if (header.offset != prev_header.offset + prev_size - config.sector_size) {
                            @panic("invalid header offset");
                        }
                    }
                    if (header.prev_checksum_meta != self.hash_chain_root) @panic("misdirected header");
                    if (header.offset != self.offset) @panic("invalid header offset");
                }

                if (!entry.valid_checksum_meta()) @panic("corrupt entry");
                if (entry.prev_checksum_meta != self.hash_chain_root) @panic("misdirected entry");
                if (entry.offset != self.offset) @panic("invalid entry offset");

                //if (entry.checksum_meta != header.checksum_meta) @panic("different headers");

                if (buffer_offset + entry.size > buffer.len) break;

                const entry_data = buffer[buffer_offset..][@sizeOf(JournalHeader)..entry.size];
                if (!entry.valid_checksum_data(entry_data)) @panic("corrupt entry data");

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

                // TODO Do not include EOF size in entry_size() calculation:
                const advance = (
                    Journal.entry_size(entry.size, config.sector_size) - config.sector_size
                );

                self.entries += 1;
                assert(self.entries < config.journal_entries_max);
                
                self.offset += advance;
                assert(self.offset < config.journal_size_max);

                buffer_offset += advance;
            }
            if (self.offset == offset) @panic("offset was not advanced");
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

        // TODO Use Journal allocator:
        // Dynamically allocate a buffer to zero the file:
        // This is done only at cluster initialization and not in the critical path.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var buffer = try arena.allocator.allocAdvanced(
            u8,
            config.sector_size,
            config.request_size_max,
            .exact
        );
        defer arena.allocator.free(buffer);
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

        log.debug("writing {} bytes at offset {}: {}", .{
            config.sector_size,
            eof_body_offset,
            eof
        });
        try file.pwriteAll(buffer[0..config.sector_size], eof_body_offset);
        
        log.debug("writing {} bytes at offset {}: {}", .{
            config.sector_size,
            eof_head_offset,
            eof
        });
        try file.pwriteAll(buffer[0..config.sector_size], eof_head_offset);
        
        log.debug("fsyncing...", .{});
        try os.fsync(file.handle);

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
            // TODO Fail if FileNotFound, when we start explicitly initializing the cluster:
            error.FileNotFound => return try Journal.create(path),
            else => return err,
        };
    }

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
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

test "entry_size()" {
    const sector_size: u64 = 4096;
    testing.expectEqual(sector_size * 2, Journal.entry_size(1, sector_size));
    testing.expectEqual(sector_size * 2, Journal.entry_size(sector_size - 1, sector_size));
    testing.expectEqual(sector_size * 2, Journal.entry_size(sector_size, sector_size));
    testing.expectEqual(sector_size * 3, Journal.entry_size(sector_size + 1, sector_size));
}
