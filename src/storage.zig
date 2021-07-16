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

    size: u64,
    fd: os.fd_t,
    io: *IO,

    pub fn init(size: u64, fd: os.fd_t, io: *IO) !Storage {
        return Storage{
            .size = size,
            .fd = fd,
            .io = io,
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

    // TODO Remove this when we add an explicit init command.
    // This is just here so long to allow TigerBeetle to know whether to create the data file.
    pub fn does_not_exist(dir_fd: os.fd_t, relative_path: [:0]const u8) !bool {
        assert(!std.fs.path.isAbsolute(relative_path));

        var flags: u32 = os.O_CLOEXEC | os.O_RDONLY;
        var mode: os.mode_t = 0;

        const fd = os.openatZ(dir_fd, relative_path, flags, mode) catch |err| switch (err) {
            error.FileNotFound => return true,
            else => return err,
        };
        defer os.close(fd);

        return false;
    }

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    /// - Allocates the file contiguously on disk if this is supported by the file system.
    /// - Ensures that the file data (and file inode in the parent directory) is durable on disk.
    ///   The caller is responsible for ensuring that the parent directory inode is durable.
    /// - Verifies that the file size matches the expected file size before returning.
    pub fn open(
        allocator: *std.mem.Allocator,
        dir_fd: os.fd_t,
        relative_path: [:0]const u8,
        size: u64,
        must_create: bool,
    ) !os.fd_t {
        assert(relative_path.len > 0);
        assert(size >= config.sector_size);
        assert(size % config.sector_size == 0);

        // TODO Use O_EXCL when opening as a block device to obtain a mandatory exclusive lock.
        // This is much stronger than an advisory exclusive lock, and is required on some platforms.

        var flags: u32 = os.O_CLOEXEC | os.O_RDWR | os.O_DSYNC;
        var mode: os.mode_t = 0;

        // TODO Document this and investigate whether this is in fact correct to set here.
        if (@hasDecl(os, "O_LARGEFILE")) flags |= os.O_LARGEFILE;

        if (config.direct_io) {
            const direct_io_supported = try Storage.fs_supports_direct_io(dir_fd);
            if (direct_io_supported) {
                flags |= os.O_DIRECT;
            } else if (config.deployment_environment == .development) {
                log.warn("file system does not support Direct I/O", .{});
            } else {
                // We require Direct I/O for safety to handle fsync failure correctly, and therefore
                // panic in production if it is not supported.
                @panic("file system does not support Direct I/O");
            }
        }

        if (must_create) {
            log.info("creating {s}...", .{relative_path});
            flags |= os.O_CREAT;
            flags |= os.O_EXCL;
            mode = 0o666;
        } else {
            log.info("opening {s}...", .{relative_path});
        }

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert((flags & os.O_DSYNC) > 0);

        // Be careful with openat(2): "If pathname is absolute, then dirfd is ignored." (man page)
        assert(!std.fs.path.isAbsolute(relative_path));
        const fd = try os.openatZ(dir_fd, relative_path, flags, mode);
        errdefer os.close(fd);

        // TODO Check that the file is actually a file.

        // Obtain an advisory exclusive lock that works only if all processes actually use flock().
        try os.flock(fd, os.LOCK_EX);

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // If the file system does not support `fallocate()`, then this could mean more seeks or a
        // panic if we run out of disk space (ENOSPC).
        if (must_create) try Storage.allocate(allocator, fd, size);

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try os.fsync(fd);

        // We fsync the parent directory to ensure that the file inode is durably written.
        // The caller is responsible for the parent directory inode stored under the grandparent.
        // We always do this when opening because we don't know if this was done before crashing.
        try os.fsync(dir_fd);

        const stat = try os.fstat(fd);
        if (stat.size != size) @panic("data file inode size was truncated or corrupted");

        return fd;
    }

    /// Allocates a file contiguously using fallocate() if supported.
    /// Alternatively, writes to the last sector so that at least the file size is correct.
    pub fn allocate(allocator: *std.mem.Allocator, fd: os.fd_t, size: u64) !void {
        log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
        Storage.fallocate(fd, 0, 0, @intCast(i64, size)) catch |err| switch (err) {
            error.OperationNotSupported => {
                log.warn("file system does not support fallocate(), an ENOSPC will panic", .{});
                log.notice("allocating by writing to the last sector of the file instead...", .{});

                const sector_size = config.sector_size;
                const sector: [sector_size]u8 align(sector_size) = [_]u8{0} ** sector_size;

                // Handle partial writes where the physical sector is less than a logical sector:
                const offset = size - sector.len;
                var written: usize = 0;
                while (written < sector.len) {
                    written += try os.pwrite(fd, sector[written..], offset + written);
                }
            },
            else => return err,
        };
    }

    fn fallocate(fd: i32, mode: i32, offset: i64, length: i64) !void {
        while (true) {
            const rc = os.linux.fallocate(fd, mode, offset, length);
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

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
    fn fs_supports_direct_io(dir_fd: std.os.fd_t) !bool {
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
