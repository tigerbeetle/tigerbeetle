const std = @import("std");
const os = std.os;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.storage);

const IO = @import("io.zig").IO;

const config = @import("config.zig");

pub const Storage = struct {
    pub const Read = struct {
        completion: IO.Completion,
        callback: fn (read: *Storage.Read) void,
        buffer: []u8,
        offset: u64,
        slice_max: u64,

        /// Returns the slice that we should read in the next IO operation
        fn slice(read: *Read) []u8 {
            var upper_bound = read.slice_max;
            // say we've had a partial read and the buffer is now
            // ptr = offset + 512
            // len = sector_size * 2 - 512 = 7680
            const partial_sector_read_remainder = read.buffer.len % config.sector_size;
            if (partial_sector_read_remainder != 0) {
                const partial_sector_read = config.sector_size - partial_sector_read_remainder;
                upper_bound -= partial_sector_read;
            }

            return read.buffer[0..std.math.min(read.buffer.len, upper_bound)];
        }
    };

    pub const Write = struct {
        completion: IO.Completion,
        callback: fn (write: *Storage.Write) void,
        buffer: []const u8,
        offset: u64,
    };

    io: *IO,
    fd: os.fd_t,
    size: u64,

    pub fn init(io: *IO, fd: os.fd_t, size: u64) !Storage {
        return Storage{
            .io = io,
            .fd = fd,
            .size = size,
        };
    }

    pub fn deinit() void {}

    pub fn read_sectors(
        self: *Storage,
        callback: fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        offset: u64,
    ) void {
        self.assert_bounds_and_alignment(buffer, offset);

        read.* = .{
            .completion = undefined,
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
            .slice_max = buffer.len,
        };

        self.start_read(read, 0);
    }

    fn start_read(self: *Storage, read: *Storage.Read, bytes_read: usize) void {
        read.offset += bytes_read;
        read.buffer = read.buffer[bytes_read..];

        if (read.slice().len == 0) {
            read.callback(read);
            return;
        }

        self.io.read(
            *Storage,
            self,
            on_read,
            &read.completion,
            self.fd,
            read.slice(),
            read.offset,
        );
    }

    fn on_read(self: *Storage, completion: *IO.Completion, result: IO.ReadError!usize) void {
        const read = @fieldParentPtr(Read, "completion", completion);

        const bytes_read = result catch |err| switch (err) {
            error.InputOutput => {
                // The disk was unable to read some sectors (an internal CRC or hardware failure):
                if (read.slice_max > config.sector_size) {
                    log.err("latent sector error: offset={}, subdividing read...", .{read.offset});
                    // Divide the buffer we are currently reading in half
                    // and try to read each half separately. With our
                    // recursion, this effectively creates a binary search for
                    // the sector(s) causing the EIO error.
                    // This is considerably slower than doing a bulk read.
                    // By now we might have also experienced the disk's read retry timeout (in seconds).
                    // TODO Docs should instruct on why and how to reduce disk firmware timeouts.
                    const sectors = @divExact(read.buffer.len, config.sector_size);
                    // This line implements ceiling division
                    read.slice_max = @divFloor(sectors - 1, 2) + 1;

                    // Pass 0 as bytes_read as we want to retry reading the data for which we got an EIO.
                    self.start_read(read, 0);
                    return;
                } else {
                    assert(read.slice_max == config.sector_size);
                    // We've tried to read at sector granularity and still failed.
                    // Zero this sector which can't be read:
                    // We treat these EIO errors the same as a checksum failure.
                    log.err("latent sector error: offset={}, zeroing buffer sector...", .{read.offset});
                    const read_slice = read.slice();
                    assert(read_slice.len <= config.sector_size);
                    std.mem.set(u8, read_slice, 0);
                    // We could set read.slice_max to Journal.sector_ceil(read.buffer.len) here
                    // in order to restart our pseudo-binary search on the rest of the sectors
                    // to be read, optimistically assuming this is the last failing sector.
                    // However, data corruption that causes EIO errors often has spacial locality.
                    // Therefore, restarting our pseudo-binary search would give us abysmal performance
                    // in this not uncommon case of many successive failing sectors.
                    self.start_read(read, read_slice.len);
                    return;
                }
            },

            error.WouldBlock,
            error.NotOpenForReading,
            error.ConnectionResetByPeer,
            error.Alignment,
            error.IsDir,
            error.SystemResources,
            error.Unseekable,
            error.Unexpected,
            => {
                log.emerg(
                    "impossible read: buffer_len={} offset={} error={s}",
                    .{ read.buffer.len, read.offset, @errorName(err) },
                );
                @panic("impossible read");
            },
        };

        if (bytes_read == 0) {
            log.emerg(
                "short read: bytes_read={} buffer_len={} offset={}",
                .{ bytes_read, read.buffer.len, read.offset },
            );
            @panic("fs corruption: file inode size truncated");
        }

        self.start_read(read, bytes_read);
    }

    pub fn write_sectors(
        self: *Storage,
        callback: fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        offset: u64,
    ) void {
        self.assert_bounds_and_alignment(buffer, offset);

        // TODO We can move this range queuing right into Storage and remove write_sectors entirely.
        // Our ConcurrentRange structure would also need to be weaned off of async/await but at
        // least then we can manage this all in one place (i.e. in Storage).
        //var range = Range{ .offset = offset, .len = buffer.len };
        //self.writing_sectors.acquire(&range);
        //defer self.writing_sectors.release(&range);

        write.* = .{
            .completion = undefined,
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
        };

        self.start_write(write);
    }

    fn start_write(self: *Storage, write: *Storage.Write) void {
        self.io.write(
            *Storage,
            self,
            on_write,
            &write.completion,
            self.fd,
            write.buffer,
            write.offset,
        );
    }

    fn on_write(self: *Storage, completion: *IO.Completion, result: IO.WriteError!usize) void {
        const write = @fieldParentPtr(Storage.Write, "completion", completion);
        const bytes_written = result catch |err| switch (err) {
            // We assume that the disk will attempt to reallocate a spare sector for any LSE.
            // TODO What if we receive an EIO error because of a faulty cable?
            error.InputOutput => @panic("latent sector error: no spare sectors to reallocate"),
            // TODO: It seems like it might be possible for some filesystems to return ETIMEDOUT
            // here. Consider handling this without panicking.
            else => {
                log.emerg(
                    "write: buffer.len={} offset={} error={}",
                    .{ write.buffer.len, write.offset, err },
                );
                @panic("unrecoverable disk error");
            },
        };

        if (bytes_written == 0) {
            // This should never happen if the kernel and filesystem are well behaved.
            // However, the internet tells us that block devices are known to exhibit
            // this behavior in the wild.
            // TODO: Consider retrying with a timeout if this panic proves problematic,
            // be careful to avoid logging in a busy loop.
            @panic("write operation returned 0 bytes written");
        }

        write.offset += bytes_written;
        write.buffer = write.buffer[bytes_written..];

        if (write.buffer.len == 0) {
            write.callback(write);
            return;
        }

        self.start_write(write);
    }

    fn assert_bounds_and_alignment(self: *Storage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= self.size);

        // Ensure that the read or write is aligned correctly for Direct I/O:
        // If this is not the case, the underlying syscall will return EINVAL.
        assert(@ptrToInt(buffer.ptr) % config.sector_size == 0);
        assert(buffer.len % config.sector_size == 0);
        assert(offset % config.sector_size == 0);
    }

    // Static helper functions to handle journal file creation/configuration:

    pub fn open_path(relative_path: [:0]const u8, allow_create: bool) !os.fd_t {
        assert(!std.fs.path.isAbsolute(relative_path));
        // TODO: Use config.data_directory when in release mode
        const fd = try open_or_create_with_options(std.fs.cwd().fd, relative_path, allow_create);
        // TODO Open parent directory to fsync the directory inode (and recurse for all ancestors).

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // Some file systems will not support fallocate(), that's fine, but could mean more seeks.
        if (allow_create) {
            log.debug("pre-allocating {Bi}...", .{config.journal_size_max});
            Storage.fallocate(fd, 0, 0, config.journal_size_max) catch |err| switch (err) {
                error.OperationNotSupported => {
                    log.notice("file system does not support fallocate()", .{});
                },
                else => |e| return e,
            };
        }

        return fd;
    }

    fn fallocate(fd: i32, mode: i32, offset: i64, length: i64) !void {
        while (true) {
            const rc = os.linux.fallocate(fd, 0, 0, config.journal_size_max);
            switch (os.linux.getErrno(rc)) {
                0 => return,
                os.linux.EBADF => return error.FileDescriptorInvalid,
                os.linux.EFBIG => return error.FileTooBig,
                os.linux.EINTR => continue,
                os.linux.EINVAL => return error.ArgumentsInvalid,
                os.linux.EIO => return error.InputOutput,
                os.linux.ENODEV => return error.NoDevice,
                os.linux.ENOSPC => return error.NoSpaceLeft,
                os.linux.ENOSYS => return error.SystemOutdated,
                os.linux.EOPNOTSUPP => return error.OperationNotSupported,
                os.linux.EPERM => return error.PermissionDenied,
                os.linux.ESPIPE => return error.Unseekable,
                os.linux.ETXTBSY => return error.FileBusy,
                else => |errno| return os.unexpectedErrno(errno),
            }
        }
    }

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    fn open_or_create_with_options(dir_fd: os.fd_t, path: [:0]const u8, allow_create: bool) !os.fd_t {
        // TODO: use O_EXCL when opening as a block device
        var flags: u32 = os.O_CLOEXEC | os.O_RDWR | os.O_DSYNC;
        var mode: os.mode_t = 0;

        if (@hasDecl(os, "O_LARGEFILE")) flags |= os.O_LARGEFILE;

        if (config.direct_io) {
            const direct_io_supported = try fs_supports_direct_io(dir_fd);
            if (direct_io_supported) {
                flags |= os.O_DIRECT;
            } else if (config.deployment_environment == .development) {
                log.warn("file system does not support direct i/o", .{});
            } else {
                @panic("file system does not support direct i/o");
            }
        }

        if (allow_create) {
            log.info("creating {s}...", .{path});
            flags |= os.O_CREAT;
            flags |= os.O_EXCL;
            mode = 0o666;
        }

        // This is critical since we rely on O_DSYNC to fsync():
        assert((flags & os.O_DSYNC) > 0);

        const fd = try os.openatZ(dir_fd, path, flags, mode);
        errdefer os.close(fd);

        try os.flock(fd, os.LOCK_EX);

        return fd;
    }

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
    pub fn fs_supports_direct_io(dir_fd: std.os.fd_t) !bool {
        if (!@hasDecl(std.os, "O_DIRECT")) return false;

        const path = "fs_supports_direct_io";
        const dir = std.fs.Dir{ .fd = dir_fd };
        const fd = try os.openatZ(dir_fd, path, os.O_CLOEXEC | os.O_CREAT | os.O_TRUNC, 0o666);
        defer os.close(fd);
        defer dir.deleteFile(path) catch {};

        while (true) {
            const res = os.system.openat(dir_fd, path, os.O_CLOEXEC | os.O_RDONLY | os.O_DIRECT, 0);
            switch (os.linux.getErrno(res)) {
                0 => {
                    os.close(@intCast(os.fd_t, res));
                    return true;
                },
                os.linux.EINTR => continue,
                os.linux.EINVAL => return false,
                else => |err| return os.unexpectedErrno(err),
            }
        }
    }
};
