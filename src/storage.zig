const std = @import("std");
const builtin = @import("builtin");
const os = std.os;
const assert = std.debug.assert;
const log = std.log.scoped(.storage);

const IO = @import("io.zig").IO;
const constants = @import("constants.zig");
const fatal = @import("cli.zig").fatal;
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

            const partial_sector_read_remainder = read.buffer.len % constants.sector_size;
            if (partial_sector_read_remainder != 0) {
                // TODO log.debug() because this is interesting, and to ensure fuzz test coverage.
                const partial_sector_read = constants.sector_size - partial_sector_read_remainder;
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

    io: *IO,
    fd: os.fd_t,

    pub fn init(io: *IO, fd: os.fd_t) !Storage {
        return Storage{
            .io = io,
            .fd = fd,
        };
    }

    pub fn deinit(storage: *Storage) void {
        assert(storage.fd != IO.INVALID_FILE);
        storage.fd = IO.INVALID_FILE;
    }

    pub fn tick(storage: *Storage) void {
        storage.io.tick() catch |err| {
            log.warn("tick: {}", .{err});
            std.debug.panic("io.tick(): {}", .{err});
        };
    }

    pub fn read_sectors(
        self: *Storage,
        callback: fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) void {
        if (zone.size()) |zone_size| {
            assert(offset_in_zone + buffer.len <= zone_size);
        }

        const offset_in_storage = zone.offset(offset_in_zone);
        assert_alignment(buffer, offset_in_storage);

        read.* = .{
            .completion = undefined,
            .callback = callback,
            .buffer = buffer,
            .offset = offset_in_storage,
            .target_max = buffer.len,
        };

        self.start_read(read, 0);
        assert(read.target().len > 0);
    }

    fn start_read(self: *Storage, read: *Storage.Read, bytes_read: usize) void {
        assert(bytes_read <= read.target().len);

        read.offset += bytes_read;
        read.buffer = read.buffer[bytes_read..];

        const target = read.target();
        if (target.len == 0) {
            read.callback(read);
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
                if (target.len > constants.sector_size) {
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
                    const target_sectors = @divFloor(target.len - 1, constants.sector_size) + 1;
                    assert(target_sectors > 0);
                    read.target_max = (@divFloor(target_sectors - 1, 2) + 1) * constants.sector_size;
                    assert(read.target_max >= constants.sector_size);

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
            error.ConnectionTimedOut,
            error.Unexpected,
            => {
                log.err(
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
            log.err(
                "short read: buffer.len={} offset={} bytes_read={}",
                .{ read.offset, read.buffer.len, bytes_read },
            );
            @panic("data file inode size was truncated or corrupted");
        }

        // If our target was limited to a single sector, perhaps because of a latent sector error,
        // then increase `target_max` according to AIMD now that we have read successfully and
        // hopefully cleared the faulty zone.
        // We assume that `target_max` may exceed `read.buffer.len` at any time.
        if (read.target_max == constants.sector_size) {
            // TODO Add log.debug because this is interesting.
            read.target_max += constants.sector_size;
        }

        self.start_read(read, bytes_read);
    }

    pub fn write_sectors(
        self: *Storage,
        callback: fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) void {
        if (zone.size()) |zone_size| {
            assert(offset_in_zone + buffer.len <= zone_size);
        }

        const offset_in_storage = zone.offset(offset_in_zone);
        assert_alignment(buffer, offset_in_storage);

        write.* = .{
            .completion = undefined,
            .callback = callback,
            .buffer = buffer,
            .offset = offset_in_storage,
        };

        self.start_write(write);
        // Assert that the callback is called asynchronously.
        assert(write.buffer.len > 0);
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
                log.err(
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
            write.callback(write);
            return;
        }

        self.start_write(write);
    }

    /// Ensures that the read or write is aligned correctly for Direct I/O.
    /// If this is not the case, then the underlying syscall will return EINVAL.
    /// We check this only at the start of a read or write because the physical sector size may be
    /// less than our logical sector size so that partial IOs then leave us no longer aligned.
    fn assert_alignment(buffer: []const u8, offset: u64) void {
        assert(@ptrToInt(buffer.ptr) % constants.sector_size == 0);
        assert(buffer.len % constants.sector_size == 0);
        assert(offset % constants.sector_size == 0);
    }

    /// Ensures that the read or write is within bounds and intends to read or write some bytes.
    fn assert_bounds(self: *Storage, buffer: []const u8, offset: u64) void {
        _ = self;
        _ = offset;

        assert(buffer.len > 0);
    }
};
