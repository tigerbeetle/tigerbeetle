const std = @import("std");
const os = std.os;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.storage);

const IO = @import("io.zig").IO;
const is_darwin = std.Target.current.isDarwin();

const config = @import("config.zig");
const vsr = @import("vsr.zig");

pub const Storage = struct {
    /// See usage in Journal.write_sectors() for details.
    pub const synchronicity: enum {
        always_synchronous,
        always_asynchronous,
    } = .always_asynchronous;

    pub const Read = struct {
        completion: IO.Completion,
        callback: fn (read: *Storage.Read) void,

        /// The buffer to read into, re-sliced and re-assigned as we go, e.g. after partial reads.
        buffer: []u8,

        /// The position into the file descriptor from where we should read, also adjusted as we go.
        offset: u64,

        /// The maximum amount of bytes to read per syscall. We use this to subdivide troublesome
        /// reads into smaller reads to work around latent sector errors (LSEs).
        target_max: u64,

        /// Returns a target slice into `buffer` to read into, capped by `target_max`.
        /// If the previous read was a partial read of physical sectors (e.g. 512 bytes) less than
        /// our logical sector size (e.g. 4 KiB), so that the remainder of the buffer is no longer
        /// aligned to a logical sector, then we further cap the slice to get back onto a logical
        /// sector boundary.
        fn target(read: *Read) []u8 {
            // A worked example of a partial read that leaves the rest of the buffer unaligned:
            // This could happen for non-Advanced Format disks with a physical sector of 512 bytes.
            // We want to read 8 KiB:
            //     buffer.ptr = 0
            //     buffer.len = 8192
            // ... and then experience a partial read of only 512 bytes:
            //     buffer.ptr = 512
            //     buffer.len = 7680
            // We can now see that `buffer.len` is no longer a sector multiple of 4 KiB and further
            // that we have 3584 bytes left of the partial sector read. If we subtract this amount
            // from our logical sector size of 4 KiB we get 512 bytes, which is the alignment error
            // that we need to subtract from `target_max` to get back onto the boundary.
            var max = read.target_max;

            const partial_sector_read_remainder = read.buffer.len % config.sector_size;
            if (partial_sector_read_remainder != 0) {
                // TODO log.debug() because this is interesting, and to ensure fuzz test coverage.
                const partial_sector_read = config.sector_size - partial_sector_read_remainder;
                max -= partial_sector_read;
            }

            return read.buffer[0..std.math.min(read.buffer.len, max)];
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
        self.assert_alignment(buffer, offset);

        read.* = .{
            .completion = undefined,
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
            .target_max = buffer.len,
        };

        self.start_read(read, 0);
    }

    fn start_read(self: *Storage, read: *Storage.Read, bytes_read: usize) void {
        assert(bytes_read <= read.target().len);

        read.offset += bytes_read;
        read.buffer = read.buffer[bytes_read..];

        const target = read.target();
        if (target.len == 0) {
            const callback = read.callback;
            read.* = undefined;
            callback(read);
            return;
        }

        self.assert_bounds(target, read.offset);
        self.io.read(
            *Storage,
            self,
            on_read,
            &read.completion,
            self.fd,
            target,
            read.offset,
        );
    }

    fn on_read(self: *Storage, completion: *IO.Completion, result: IO.ReadError!usize) void {
        const read = @fieldParentPtr(Storage.Read, "completion", completion);

        const bytes_read = result catch |err| switch (err) {
            error.InputOutput => {
                // The disk was unable to read some sectors (an internal CRC or hardware failure):
                // We may also have already experienced a partial unaligned read, reading less
                // physical sectors than the logical sector size, so we cannot expect `target.len`
                // to be an exact logical sector multiple.
                const target = read.target();
                if (target.len > config.sector_size) {
                    // We tried to read more than a logical sector and failed.
                    log.err("latent sector error: offset={}, subdividing read...", .{read.offset});

                    // Divide the buffer in half and try to read each half separately:
                    // This creates a recursive binary search for the sector(s) causing the error.
                    // This is considerably slower than doing a single bulk read and by now we might
                    // also have experienced the disk's read retry timeout (in seconds).
                    // TODO Our docs must instruct on why and how to reduce disk firmware timeouts.

                    // These lines both implement ceiling division e.g. `((3 - 1) / 2) + 1 == 2` and
                    // require that the numerator is always greater than zero:
                    assert(target.len > 0);
                    const target_sectors = @divFloor(target.len - 1, config.sector_size) + 1;
                    assert(target_sectors > 0);
                    read.target_max = (@divFloor(target_sectors - 1, 2) + 1) * config.sector_size;
                    assert(read.target_max >= config.sector_size);

                    // Pass 0 for `bytes_read`, we want to retry the read with smaller `target_max`:
                    self.start_read(read, 0);
                    return;
                } else {
                    // We tried to read at (or less than) logical sector granularity and failed.
                    log.err("latent sector error: offset={}, zeroing sector...", .{read.offset});

                    // Zero this logical sector which can't be read:
                    // We will treat these EIO errors the same as a checksum failure.
                    // TODO This could be an interesting avenue to explore further, whether
                    // temporary or permanent EIO errors should be conflated with checksum failures.
                    assert(target.len > 0);
                    std.mem.set(u8, target, 0);

                    // We could set `read.target_max` to `vsr.sector_ceil(read.buffer.len)` here
                    // in order to restart our pseudo-binary search on the rest of the sectors to be
                    // read, optimistically assuming that this is the last failing sector.
                    // However, data corruption that causes EIO errors often has spacial locality.
                    // Therefore, restarting our pseudo-binary search here might give us abysmal
                    // performance in the (not uncommon) case of many successive failing sectors.
                    self.start_read(read, target.len);
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
                    "impossible read: offset={} buffer.len={} error={s}",
                    .{ read.offset, read.buffer.len, @errorName(err) },
                );
                @panic("impossible read");
            },
        };

        if (bytes_read == 0) {
            // We tried to read more than there really is available to read.
            // In other words, we thought we could read beyond the end of the file descriptor.
            // This can happen if the data file inode `size` was truncated or corrupted.
            log.emerg(
                "short read: buffer.len={} offset={} bytes_read={}",
                .{ read.offset, read.buffer.len, bytes_read },
            );
            @panic("data file inode size was truncated or corrupted");
        }

        // If our target was limited to a single sector, perhaps because of a latent sector error,
        // then increase `target_max` according to AIMD now that we have read successfully and
        // hopefully cleared the faulty zone.
        // We assume that `target_max` may exceed `read.buffer.len` at any time.
        if (read.target_max == config.sector_size) {
            // TODO Add log.debug because this is interesting.
            read.target_max += config.sector_size;
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
        self.assert_alignment(buffer, offset);

        write.* = .{
            .completion = undefined,
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
        };

        self.start_write(write);
    }

    fn start_write(self: *Storage, write: *Storage.Write) void {
        self.assert_bounds(write.buffer, write.offset);
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
            // TODO What if we receive a temporary EIO error because of a faulty cable?
            error.InputOutput => @panic("latent sector error: no spare sectors to reallocate"),
            // TODO: It seems like it might be possible for some filesystems to return ETIMEDOUT
            // here. Consider handling this without panicking.
            else => {
                log.emerg(
                    "impossible write: offset={} buffer.len={} error={s}",
                    .{ write.offset, write.buffer.len, @errorName(err) },
                );
                @panic("impossible write");
            },
        };

        if (bytes_written == 0) {
            // This should never happen if the kernel and filesystem are well behaved.
            // However, block devices are known to exhibit this behavior in the wild.
            // TODO: Consider retrying with a timeout if this panic proves problematic, and be
            // careful to avoid logging in a busy loop. Perhaps a better approach might be to
            // return wrote = null here and let the protocol retry at a higher layer where there is
            // more context available to decide on how important this is or whether to cancel.
            @panic("write operation returned 0 bytes written");
        }

        write.offset += bytes_written;
        write.buffer = write.buffer[bytes_written..];

        if (write.buffer.len == 0) {
            const callback = write.callback;
            write.* = undefined;
            callback(write);
            return;
        }

        self.start_write(write);
    }

    /// Ensures that the read or write is aligned correctly for Direct I/O.
    /// If this is not the case, then the underlying syscall will return EINVAL.
    /// We check this only at the start of a read or write because the physical sector size may be
    /// less than our logical sector size so that partial IOs then leave us no longer aligned.
    fn assert_alignment(self: *Storage, buffer: []const u8, offset: u64) void {
        assert(@ptrToInt(buffer.ptr) % config.sector_size == 0);
        assert(buffer.len % config.sector_size == 0);
        assert(offset % config.sector_size == 0);
    }

    /// Ensures that the read or write is within bounds and intends to read or write some bytes.
    fn assert_bounds(self: *Storage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= self.size);
    }

    // Static helper functions to handle data file creation/opening/allocation:

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    /// - Allocates the file contiguously on disk if this is supported by the file system.
    /// - Ensures that the file data (and file inode in the parent directory) is durable on disk.
    ///   The caller is responsible for ensuring that the parent directory inode is durable.
    /// - Verifies that the file size matches the expected file size before returning.
    pub fn open(
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

        var direct_io_supported = false;
        if (config.direct_io) {
            direct_io_supported = try Storage.fs_supports_direct_io(dir_fd);
            if (direct_io_supported) {
                if (!is_darwin) flags |= os.O_DIRECT;
            } else if (config.deployment_environment == .development) {
                log.warn("file system does not support Direct I/O", .{});
            } else {
                // We require Direct I/O for safety to handle fsync failure correctly, and therefore
                // panic in production if it is not supported.
                @panic("file system does not support Direct I/O");
            }
        }

        if (must_create) {
            log.info("creating \"{s}\"...", .{relative_path});
            flags |= os.O_CREAT;
            flags |= os.O_EXCL;
            mode = 0o666;
        } else {
            log.info("opening \"{s}\"...", .{relative_path});
        }

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert((flags & os.O_DSYNC) > 0);

        // Be careful with openat(2): "If pathname is absolute, then dirfd is ignored." (man page)
        assert(!std.fs.path.isAbsolute(relative_path));
        const fd = try os.openatZ(dir_fd, relative_path, flags, mode);
        // TODO Return a proper error message when the path exists or does not exist (init/start).
        errdefer os.close(fd);

        // TODO Check that the file is actually a file.

        // On darwin, use F_NOCACHE on direct_io to disable the page cache as O_DIRECT doesn't exit.
        if (is_darwin and config.direct_io and direct_io_supported) {
            _ = try os.fcntl(fd, os.F_NOCACHE, 1);
        }

        // Obtain an advisory exclusive lock that works only if all processes actually use flock().
        // LOCK_NB means that we want to fail the lock without waiting if another process has it.
        os.flock(fd, os.LOCK_EX | os.LOCK_NB) catch |err| switch (err) {
            error.WouldBlock => @panic("another process holds the data file lock"),
            else => return err,
        };

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // If the file system does not support `fallocate()`, then this could mean more seeks or a
        // panic if we run out of disk space (ENOSPC).
        if (must_create) try Storage.allocate(fd, size);

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
    pub fn allocate(fd: os.fd_t, size: u64) !void {
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
        // https://stackoverflow.com/a/11497568
        // https://api.kde.org/frameworks/kcoreaddons/html/posix__fallocate__mac_8h_source.html
        // http://hg.mozilla.org/mozilla-central/file/3d846420a907/xpcom/glue/FileUtils.cpp#l61
        if (is_darwin) {
            const F_ALLOCATECONTIG = 0x2; // allocate contiguous space
            const F_ALLOCATEALL = 0x4; // allocate all or nothing
            const F_PEOFPOSMODE = 3; // use relative offset from the seek pos mode
            const F_VOLPOSMODE = 4; // use the specified volume offset
            const fstore_t = extern struct {
                fst_flags: c_uint,
                fst_posmode: c_int,
                fst_offset: os.off_t,
                fst_length: os.off_t,
                fst_bytesalloc: os.off_t,
            };

            var store = fstore_t{
                .fst_flags = F_ALLOCATECONTIG | F_ALLOCATEALL,
                .fst_posmode = F_PEOFPOSMODE,
                .fst_offset = 0,
                .fst_length = offset + length,
                .fst_bytesalloc = 0,
            };

            // try to pre-allocate contiguous space and fall back to default non-continugous
            var res = os.system.fcntl(fd, os.F_PREALLOCATE, @ptrToInt(&store));
            if (os.errno(res) != 0) {
                store.fst_flags = F_ALLOCATEALL;
                res = os.system.fcntl(fd, os.F_PREALLOCATE, @ptrToInt(&store));
            }

            switch (os.errno(res)) {
                0 => {},
                os.EACCES => unreachable, // F_SETLK or F_SETSIZE of F_WRITEBOOTSTRAP
                os.EBADF => return error.FileDescriptorInvalid,
                os.EDEADLK => unreachable, // F_SETLKW
                os.EINTR => unreachable, // F_SETLKW
                os.EINVAL => return error.ArgumentsInvalid, // for F_PREALLOCATE (offset invalid)
                os.EMFILE => unreachable, // F_DUPFD or F_DUPED
                os.ENOLCK => unreachable, // F_SETLK or F_SETLKW
                os.EOVERFLOW => return error.FileTooBig,
                os.ESRCH => unreachable, // F_SETOWN
                os.EOPNOTSUPP => return error.OperationNotSupported, // not reported but need same error union
                else => |errno| return os.unexpectedErrno(errno),
            }

            // now actually perform the allocation
            return os.ftruncate(fd, @intCast(u64, length)) catch |err| switch (err) {
                error.AccessDenied => error.PermissionDenied,
                else => |e| e,
            };
        }

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
        if (!@hasDecl(std.os, "O_DIRECT") and !is_darwin) return false;

        const path = "fs_supports_direct_io";
        const dir = std.fs.Dir{ .fd = dir_fd };
        const fd = try os.openatZ(dir_fd, path, os.O_CLOEXEC | os.O_CREAT | os.O_TRUNC, 0o666);
        defer os.close(fd);
        defer dir.deleteFile(path) catch {};

        // F_NOCACHE on darwin is the most similar option to O_DIRECT on linux.
        if (is_darwin) {
            _ = os.fcntl(fd, os.F_NOCACHE, 1) catch return false;
            return true;
        }

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
