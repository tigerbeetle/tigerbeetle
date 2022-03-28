const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const math = std.math;

const config = @import("../config.zig");

const Message = @import("../message_pool.zig").MessagePool.Message;
const vsr = @import("../vsr.zig");
const Header = vsr.Header;

const log = std.log.scoped(.journal);

pub fn Journal(comptime Replica: type, comptime Storage: type) type {
    return struct {
        const Self = @This();

        pub const Read = struct {
            self: *Self,
            completion: Storage.Read,
            callback: fn (self: *Replica, prepare: ?*Message, destination_replica: ?u8) void,

            message: *Message,
            op: u64,
            checksum: u128,
            destination_replica: ?u8,
        };

        pub const Write = struct {
            pub const Trigger = enum { append, repair };

            self: *Self,
            callback: fn (self: *Replica, wrote: ?*Message, trigger: Trigger) void,

            message: *Message,
            trigger: Trigger,

            /// True if this Write has acquired a lock on a sector of headers.
            /// This also means that the Write is currently writing sectors or queuing to do so.
            header_sector_locked: bool = false,
            /// Linked list of Writes waiting to acquire the same header sector as this Write.
            header_sector_next: ?*Write = null,

            /// This is reset to undefined and reused for each Storage.write_sectors() call.
            range: Range,

            const Sector = *align(config.sector_size) [config.sector_size]u8;
            fn header_sector(write: *Self.Write, journal: *Self) Sector {
                assert(journal.writes.items.len == journal.headers_iops.len);
                const i = @divExact(@ptrToInt(write) - @ptrToInt(&journal.writes.items), @sizeOf(Self.Write));
                // TODO: the compiler should probably be smart enough to avoid needing this align cast
                // as the type of `headers_iops` ensures that each buffer is properly aligned.
                return @alignCast(config.sector_size, &journal.headers_iops[i]);
            }

            fn header_sector_same(write: *Self.Write, other: *Self.Write) bool {
                return write_prepare_header_offset(write.message) ==
                    write_prepare_header_offset(other.message);
            }
        };

        /// State that needs to be persisted while waiting for an overlapping
        /// concurrent write to complete. This is a range on the physical disk.
        const Range = struct {
            completion: Storage.Write,
            callback: fn (write: *Self.Write) void,
            buffer: []const u8,
            offset: u64,

            /// If other writes are waiting on this write to proceed, they will
            /// be queued up in this linked list.
            next: ?*Range = null,
            /// True if a Storage.write_sectors() operation is in progress for this buffer/offset.
            locked: bool,

            fn overlaps(self: *const Range, other: *const Range) bool {
                if (self.offset < other.offset) {
                    return self.offset + self.buffer.len > other.offset;
                } else {
                    return other.offset + other.buffer.len > self.offset;
                }
            }
        };

        storage: *Storage,
        replica: u8,
        size: u64,
        size_headers: u64,
        size_circular_buffer: u64,

        headers: []align(config.sector_size) Header,
        /// We copy-on-write to these buffers when writing, as in-memory headers may change concurrently.
        /// The buffers belong to the IOP at the corresponding index in IOPS.
        headers_iops: *align(config.sector_size) [config.io_depth_write][config.sector_size]u8,
        /// Apart from the header written with the entry, we also store two redundant copies of each
        /// header at different locations on disk, and we alternate between these for each append.
        /// This tracks which version (0 or 1) should be written to next:
        headers_version: u1 = 0,

        /// Statically allocated read IO operation context data.
        reads: IOPS(Read, config.io_depth_read) = .{},
        /// Statically allocated write IO operation context data.
        writes: IOPS(Write, config.io_depth_write) = .{},

        /// Whether an entry is in memory only and needs to be written or is being written:
        /// We use this in the same sense as a dirty bit in the kernel page cache.
        /// A dirty bit means that we have not yet prepared the entry, or need to repair a faulty entry.
        dirty: BitSet,

        /// Whether an entry was written to disk and this write was subsequently lost due to:
        /// * corruption,
        /// * a misdirected write (or a misdirected read, we do not distinguish), or else
        /// * a latent sector error, where the sector can no longer be read.
        /// A faulty bit means that we prepared and then lost the entry.
        /// A faulty bit requires the dirty bit to also be set so that functions need not check both.
        /// A faulty bit is used then only to qualify the severity of the dirty bit.
        faulty: BitSet,

        recovered: bool = true,
        recovering: bool = false,

        pub fn init(
            allocator: Allocator,
            storage: *Storage,
            replica: u8,
            size: u64,
            headers_count: u32,
            init_prepare: *Header,
        ) !Self {
            if (@mod(size, config.sector_size) != 0) return error.SizeMustBeAMultipleOfSectorSize;
            if (!math.isPowerOfTwo(headers_count)) return error.HeadersCountMustBeAPowerOfTwo;

            const headers_per_sector = @divExact(config.sector_size, @sizeOf(Header));
            assert(headers_per_sector > 0);
            assert(headers_count >= headers_per_sector);

            var headers = try allocator.allocAdvanced(
                Header,
                config.sector_size,
                headers_count,
                .exact,
            );
            errdefer allocator.free(headers);
            std.mem.set(Header, headers, Header.reserved());

            var dirty = try BitSet.init(allocator, headers.len);
            errdefer dirty.deinit(allocator);

            var faulty = try BitSet.init(allocator, headers.len);
            errdefer faulty.deinit(allocator);

            const headers_iops = (try allocator.allocAdvanced(
                [config.sector_size]u8,
                config.sector_size,
                config.io_depth_write,
                .exact,
            ))[0..config.io_depth_write];
            errdefer allocator.free(headers_iops);

            const header_copies = 2;
            const size_headers = headers.len * @sizeOf(Header);
            const size_headers_copies = size_headers * header_copies;
            if (size_headers_copies >= size) return error.SizeTooSmallForHeadersCount;

            const size_circular_buffer = size - size_headers_copies;
            if (size_circular_buffer < 64 * 1024 * 1024) return error.SizeTooSmallForCircularBuffer;

            log.debug("{}: size={} headers_len={} headers={} circular_buffer={}", .{
                replica,
                std.fmt.fmtIntSizeBin(size),
                headers.len,
                std.fmt.fmtIntSizeBin(size_headers),
                std.fmt.fmtIntSizeBin(size_circular_buffer),
            });

            var self = Self{
                .storage = storage,
                .replica = replica,
                .size = size,
                .size_headers = size_headers,
                .size_circular_buffer = size_circular_buffer,
                .headers = headers,
                .dirty = dirty,
                .faulty = faulty,
                .headers_iops = headers_iops,
            };

            assert(@mod(self.size_circular_buffer, config.sector_size) == 0);
            assert(@mod(@ptrToInt(&self.headers[0]), config.sector_size) == 0);
            assert(self.dirty.bits.len == self.headers.len);
            assert(self.faulty.bits.len == self.headers.len);

            // Op 0 is always the cluster initialization op.
            // TODO This will change when we implement synchronized incremental snapshots.
            assert(init_prepare.valid_checksum());
            assert(init_prepare.invalid() == null);
            self.headers[0] = init_prepare.*;
            self.assert_headers_reserved_from(init_prepare.op + 1);

            return self;
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            const replica = @fieldParentPtr(Replica, "journal", self);

            self.dirty.deinit(allocator);
            self.faulty.deinit(allocator);
            allocator.free(self.headers);
            allocator.free(self.headers_iops);

            {
                var it = self.reads.iterate();
                while (it.next()) |read| replica.message_bus.unref(read.message);
            }
            {
                var it = self.writes.iterate();
                while (it.next()) |write| replica.message_bus.unref(write.message);
            }
        }

        /// Asserts that headers are .reserved (zeroed) from `op_min` (inclusive).
        pub fn assert_headers_reserved_from(self: *Self, op_min: u64) void {
            // TODO Snapshots
            for (self.headers[op_min..]) |header| assert(header.command == .reserved);
        }

        /// Returns any existing entry at the location indicated by header.op.
        /// This existing entry may have an older or newer op number.
        pub fn entry(self: *Self, header: *const Header) ?*const Header {
            assert(header.command == .prepare);
            return self.entry_for_op(header.op);
        }

        /// We use the op number directly to index into the headers array and locate ops without a scan.
        /// Op numbers cycle through the headers array and do not wrap when offsets wrap. The reason for
        /// this is to prevent variable offsets from impacting the location of an op. Otherwise, the
        /// same op number but for different views could exist at multiple locations in the journal.
        pub fn entry_for_op(self: *Self, op: u64) ?*const Header {
            // TODO Snapshots
            const existing = &self.headers[op];
            if (existing.command == .reserved) return null;
            assert(existing.command == .prepare);
            return existing;
        }

        /// Returns the entry at `@mod(op)` location, but only if `entry.op == op`, else `null`.
        /// Be careful of using this without considering that there may still be an existing op.
        pub fn entry_for_op_exact(self: *Self, op: u64) ?*const Header {
            if (self.entry_for_op(op)) |existing| {
                if (existing.op == op) return existing;
            }
            return null;
        }

        /// As per `entry_for_op_exact()`, but only if there is an optional checksum match.
        pub fn entry_for_op_exact_with_checksum(
            self: *Self,
            op: u64,
            checksum: ?u128,
        ) ?*const Header {
            if (self.entry_for_op_exact(op)) |existing| {
                assert(existing.op == op);
                if (checksum == null or existing.checksum == checksum.?) return existing;
            }
            return null;
        }

        pub fn previous_entry(self: *Self, header: *const Header) ?*const Header {
            // TODO Snapshots
            if (header.op == 0) return null;
            return self.entry_for_op(header.op - 1);
        }

        pub fn next_entry(self: *Self, header: *const Header) ?*const Header {
            // TODO Snapshots
            if (header.op + 1 == self.headers.len) return null;
            return self.entry_for_op(header.op + 1);
        }

        pub fn next_offset(header: *const Header) u64 {
            // TODO Snapshots
            assert(header.command == .prepare);
            return header.offset + vsr.sector_ceil(header.size);
        }

        pub fn has(self: *Self, header: *const Header) bool {
            // TODO Snapshots
            const existing = &self.headers[header.op];
            if (existing.command == .reserved) {
                return false;
            } else {
                if (existing.checksum == header.checksum) {
                    assert(existing.checksum_body == header.checksum_body);
                    assert(existing.op == header.op);
                    return true;
                } else {
                    return false;
                }
            }
        }

        pub fn has_clean(self: *Self, header: *const Header) bool {
            // TODO Snapshots
            return self.has(header) and !self.dirty.bit(header.op);
        }

        pub fn has_dirty(self: *Self, header: *const Header) bool {
            // TODO Snapshots
            return self.has(header) and self.dirty.bit(header.op);
        }

        /// Copies latest headers between `op_min` and `op_max` (both inclusive) as will fit in `dest`.
        /// Reverses the order when copying so that latest headers are copied first, which also protects
        /// against the callsite slicing the buffer the wrong way and incorrectly.
        /// Skips .reserved headers (gaps between headers).
        /// Zeroes the `dest` buffer in case the copy would underflow and leave a buffer bleed.
        /// Returns the number of headers actually copied.
        pub fn copy_latest_headers_between(
            self: *Self,
            op_min: u64,
            op_max: u64,
            dest: []Header,
        ) usize {
            assert(op_min <= op_max);
            assert(dest.len > 0);

            var copied: usize = 0;
            std.mem.set(Header, dest, Header.reserved());

            // Start at op_max + 1 and do the decrement upfront to avoid overflow when op_min == 0:
            var op = op_max + 1;
            while (op > op_min) {
                op -= 1;

                if (self.entry_for_op_exact(op)) |header| {
                    dest[copied] = header.*;
                    assert(dest[copied].invalid() == null);
                    copied += 1;
                    if (copied == dest.len) break;
                }
            }

            log.debug("copy_latest_headers_between: op_min={} op_max={} dest.len={} copied={}", .{
                op_min,
                op_max,
                dest.len,
                copied,
            });

            return copied;
        }

        const HeaderRange = struct { op_min: u64, op_max: u64 };

        /// Finds the latest break in headers between `op_min` and `op_max` (both inclusive).
        /// A break is a missing header or a header not connected to the next header by hash chain.
        /// On finding the highest break, extends the range downwards to cover as much as possible.
        /// We expect that `op_min` and `op_max` (`replica.commit_min` and `replica.op`) must exist.
        /// A range will never include `op_min` because this is already committed.
        /// A range will never include `op_max` because this must be up to date as the latest op.
        /// We must therefore first resolve any view jump barrier so that we can trust `op_max`.
        ///
        /// For example: If ops 3, 9 and 10 are missing, returns: `{ .op_min = 9, .op_max = 10 }`.
        ///
        /// Another example: If op 17 is disconnected from op 18, 16 is connected to 17, and 12-15
        /// are missing, returns: `{ .op_min = 12, .op_max = 17 }`.
        pub fn find_latest_headers_break_between(
            self: *Self,
            op_min: u64,
            op_max: u64,
        ) ?HeaderRange {
            assert(op_min <= op_max);
            var range: ?HeaderRange = null;

            // We set B, the op after op_max, to null because we only examine breaks < op_max:
            var B: ?*const Header = null;

            var op = op_max + 1;
            while (op > op_min) {
                op -= 1;

                // Get the entry at @mod(op) location, but only if entry.op == op, else null:
                var A = self.entry_for_op_exact(op);
                if (A) |a| {
                    if (B) |b| {
                        // If A was reordered then A may have a newer op than B (but an older view).
                        // However, here we use entry_for_op_exact() to assert a.op + 1 == b.op:
                        assert(a.op + 1 == b.op);

                        // We do not assert a.view <= b.view here unless the chain is intact because
                        // repair_header() may put a newer view to the left of an older view.

                        // A exists and B exists:
                        if (range) |*r| {
                            assert(b.op == r.op_min);
                            if (a.op == op_min) {
                                // A is committed, because we pass `commit_min` as `op_min`:
                                // Do not add A to range because A cannot be a break if committed.
                                break;
                            } else if (a.checksum == b.parent) {
                                // A is connected to B, but B is disconnected, add A to range:
                                assert(a.view <= b.view);
                                assert(a.op > op_min);
                                r.op_min = a.op;
                            } else if (a.view < b.view) {
                                // A is not connected to B, and A is older than B, add A to range:
                                r.op_min = a.op;
                            } else if (a.view > b.view) {
                                // A is not connected to B, but A is newer than B, close range:
                                break;
                            } else {
                                // Op numbers in the same view must be connected.
                                unreachable;
                            }
                        } else if (a.checksum == b.parent) {
                            // A is connected to B, and B is connected or B is op_max.
                            assert(a.view <= b.view);
                        } else if (a.view < b.view) {
                            // A is not connected to B, and A is older than B, open range:
                            assert(a.op > op_min);
                            range = .{ .op_min = a.op, .op_max = a.op };
                        } else if (a.view > b.view) {
                            // A is not connected to B, but A is newer than B, open and close range:
                            assert(b.op < op_max);
                            range = .{ .op_min = b.op, .op_max = b.op };
                            break;
                        } else {
                            // Op numbers in the same view must be connected.
                            unreachable;
                        }
                    } else {
                        // A exists and B does not exist (or B has a older/newer op number):
                        if (range) |r| {
                            // We cannot compare A to B, A may be older/newer, close range:
                            assert(r.op_min == op + 1);
                            break;
                        } else {
                            // We expect a range if B does not exist, unless:
                            assert(a.op == op_max);
                        }
                    }
                } else {
                    assert(op > op_min);
                    assert(op < op_max);

                    // A does not exist, or A has an older (or newer if reordered) op number:
                    if (range) |*r| {
                        // Add A to range:
                        assert(r.op_min == op + 1);
                        r.op_min = op;
                    } else {
                        // Open range:
                        assert(B != null);
                        range = .{ .op_min = op, .op_max = op };
                    }
                }

                B = A;
            }

            if (range) |r| {
                // We can never repair op_min (replica.commit_min) since that is already committed:
                assert(r.op_min > op_min);
                // We can never repair op_max (replica.op) since that is the latest op:
                // We can assume this because any existing view jump barrier must first be resolved.
                assert(r.op_max < op_max);
            }

            return range;
        }

        pub fn read_prepare(
            self: *Self,
            callback: fn (replica: *Replica, prepare: ?*Message, destination_replica: ?u8) void,
            op: u64,
            checksum: u128,
            destination_replica: ?u8,
        ) void {
            const replica = @fieldParentPtr(Replica, "journal", self);
            if (op > replica.op) {
                self.read_prepare_log(op, checksum, "beyond replica.op");
                callback(replica, null, null);
                return;
            }

            // Do not use this pointer beyond this function's scope, as the
            // header memory may then change:
            const exact = self.entry_for_op_exact_with_checksum(op, checksum) orelse {
                self.read_prepare_log(op, checksum, "no entry exactly");
                callback(replica, null, null);
                return;
            };

            if (self.faulty.bit(op)) {
                assert(self.dirty.bit(op));

                self.read_prepare_log(op, checksum, "faulty");
                callback(replica, null, null);
                return;
            }

            if (self.dirty.bit(op)) {
                self.read_prepare_log(op, checksum, "dirty");
                callback(replica, null, null);
                return;
            }

            const physical_size = vsr.sector_ceil(exact.size);
            assert(physical_size >= exact.size);

            const message = replica.message_bus.get_message();
            defer replica.message_bus.unref(message);

            // Skip the disk read if the header is all we need:
            if (exact.size == @sizeOf(Header)) {
                message.header.* = exact.*;
                callback(replica, message, destination_replica);
                return;
            }

            const read = self.reads.acquire() orelse {
                self.read_prepare_log(op, checksum, "waiting for IOP");
                callback(replica, null, null);
                return;
            };

            read.* = .{
                .self = self,
                .completion = undefined,
                .message = message.ref(),
                .callback = callback,
                .op = op,
                .checksum = checksum,
                .destination_replica = destination_replica,
            };

            assert(exact.offset + physical_size <= self.size_circular_buffer);

            const buffer = message.buffer[0..physical_size];
            const offset = self.offset_in_circular_buffer(exact.offset);

            // Memory must not be owned by `self.headers` as these may be modified concurrently:
            assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
                @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

            log.debug(
                "{}: read_sectors: offset={} len={}",
                .{ replica.replica, offset, buffer.len },
            );

            self.storage.read_sectors(on_read, &read.completion, buffer, offset);
        }

        fn on_read(completion: *Storage.Read) void {
            const read = @fieldParentPtr(Self.Read, "completion", completion);
            const self = read.self;
            const replica = @fieldParentPtr(Replica, "journal", self);
            const op = read.op;
            const checksum = read.checksum;

            defer {
                replica.message_bus.unref(read.message);
                self.reads.release(read);
            }

            if (op > replica.op) {
                self.read_prepare_log(op, checksum, "beyond replica.op");
                read.callback(replica, null, null);
                return;
            }

            _ = replica.journal.entry_for_op_exact_with_checksum(op, checksum) orelse {
                self.read_prepare_log(op, checksum, "no entry exactly");
                read.callback(replica, null, null);
                return;
            };

            if (!read.message.header.valid_checksum()) {
                self.faulty.set(op);
                self.dirty.set(op);

                self.read_prepare_log(op, checksum, "corrupt header after read");
                read.callback(replica, null, null);
                return;
            }

            const body = read.message.buffer[@sizeOf(Header)..read.message.header.size];
            if (!read.message.header.valid_checksum_body(body)) {
                self.faulty.set(op);
                self.dirty.set(op);

                self.read_prepare_log(op, checksum, "corrupt body after read");
                read.callback(replica, null, null);
                return;
            }

            if (read.message.header.op != op) {
                self.read_prepare_log(op, checksum, "op changed during read");
                read.callback(replica, null, null);
                return;
            }

            if (read.message.header.checksum != checksum) {
                self.read_prepare_log(op, checksum, "checksum changed during read");
                read.callback(replica, null, null);
                return;
            }

            read.callback(replica, read.message, read.destination_replica);
        }

        fn read_prepare_log(self: *Self, op: u64, checksum: ?u128, notice: []const u8) void {
            log.info(
                "{}: read_prepare: op={} checksum={}: {s}",
                .{ self.replica, op, checksum, notice },
            );
        }

        pub fn recover(self: *Self) void {
            assert(!self.recovered);

            if (self.recovering) return;
            self.recovering = true;

            log.debug("{}: recover: recovering", .{self.replica});

            self.recover_headers(0, 0);
            self.recover_headers(0, 1);
        }

        fn recover_headers(self: *Self, offset: u64, version: u1) void {
            const replica = @fieldParentPtr(Replica, "journal", self);

            assert(!self.recovered);
            assert(self.recovering);

            if (offset == self.size_headers) {
                log.debug("{}: recover_headers: version={} recovered", .{
                    self.replica,
                    version,
                });
                if (self.reads.executing() == 0) {
                    log.debug("{}: recover_headers: both versions recovered", .{self.replica});
                    self.recovered = true;
                    self.recovering = false;
                    // The initialization op (TODO Snapshots):
                    assert(!self.dirty.bit(0));
                    assert(!self.faulty.bit(0));
                    // From here it's over to the Recovery protocol from VRR 2012.
                }
                return;
            }
            assert(offset < self.size_headers);

            const message = replica.message_bus.get_message();
            defer replica.message_bus.unref(message);

            // We use the count of reads executing to know when both versions have finished reading:
            // We expect that no other process is issuing reads while we are recovering.
            assert(self.reads.executing() < 2);

            const read = self.reads.acquire() orelse unreachable;
            read.* = .{
                .self = self,
                .completion = undefined,
                .message = message.ref(),
                .callback = undefined,
                .op = undefined,
                .checksum = offset,
                .destination_replica = version,
            };

            const buffer = self.recover_headers_buffer(message, offset);
            assert(buffer.len > 0);

            log.debug("{}: recover_headers: version={} offset={} size={} recovering", .{
                self.replica,
                version,
                offset,
                buffer.len,
            });

            self.storage.read_sectors(
                recover_headers_on_read,
                &read.completion,
                buffer,
                self.offset_in_headers_version(offset, version),
            );
        }

        fn recover_headers_buffer(self: *Self, message: *Message, offset: u64) []u8 {
            const max = std.math.min(message.buffer.len, self.size_headers - offset);
            assert(max % config.sector_size == 0);
            return message.buffer[0..max];
        }

        fn recover_headers_on_read(completion: *Storage.Read) void {
            const read = @fieldParentPtr(Self.Read, "completion", completion);
            const self = read.self;
            const replica = @fieldParentPtr(Replica, "journal", self);
            const message = read.message;

            const offset = @intCast(u64, read.checksum);
            const version = @intCast(u1, read.destination_replica.?);
            const buffer = self.recover_headers_buffer(message, offset);

            log.debug("{}: recover_headers: version={} offset={} size={} recovered", .{
                self.replica,
                version,
                offset,
                buffer.len,
            });

            assert(offset % @sizeOf(Header) == 0);
            assert(buffer.len >= @sizeOf(Header));
            assert(buffer.len % @sizeOf(Header) == 0);

            for (std.mem.bytesAsSlice(Header, buffer)) |*header, index| {
                const op = offset / @sizeOf(Header) + index;

                if (header.valid_checksum()) {
                    // This header is valid.
                    if (self.entry_for_op(op)) |existing| {
                        if (existing.checksum == header.checksum) {
                            // We also have the same header from the other version.
                            assert(!self.faulty.bit(op));
                        } else if (existing.command == .reserved) {
                            self.set_entry_as_dirty(header);
                            self.faulty.clear(op);
                        } else {
                            // Don't replace any existing op from the other version.
                            // First come, first served.
                            // We'll sort out the right order later when we recover higher up.
                            assert(!self.faulty.bit(op));
                        }
                    } else if (header.command == .reserved) {
                        self.dirty.set(op);
                        self.faulty.clear(op);
                    } else {
                        self.set_entry_as_dirty(header);
                    }
                } else {
                    // This header is corrupt.
                    if (self.entry_for_op(op)) |_| {
                        // However, we have a valid header from the other version.
                    } else {
                        self.dirty.set(op);
                        self.faulty.set(op);
                    }
                }
            }

            // We must release before we call `recover_headers()` in case Storage is synchronous.
            // Otherwise, we would run out of messages and reads.
            replica.message_bus.unref(read.message);
            self.reads.release(read);

            self.recover_headers(offset + buffer.len, version);
        }

        /// A safe way of removing an entry, where the header must match the current entry to succeed.
        fn remove_entry(self: *Self, header: *const Header) void {
            // Copy the header.op by value to avoid a reset() followed by undefined header.op usage:
            const op = header.op;
            log.debug("{}: remove_entry: op={} checksum={}", .{
                self.replica,
                op,
                header.checksum,
            });

            assert(self.entry(header).?.checksum == header.checksum);
            assert(self.headers[op].checksum == header.checksum); // TODO Snapshots

            defer self.headers[op] = Header.reserved();
            self.dirty.clear(op);
            self.faulty.clear(op);
        }

        /// Removes entries from `op_min` (inclusive) onwards.
        /// This is used after a view change to remove uncommitted entries discarded by the new leader.
        pub fn remove_entries_from(self: *Self, op_min: u64) void {
            // TODO Snapshots
            // TODO Optimize to jump directly to op:
            assert(op_min > 0);
            log.debug("{}: remove_entries_from: op_min={}", .{ self.replica, op_min });
            for (self.headers) |*header| {
                if (header.op >= op_min and header.command == .prepare) {
                    self.remove_entry(header);
                }
            }
            self.assert_headers_reserved_from(op_min);
            // TODO At startup we need to handle entries that may have been removed but now reappear.
            // This is because we do not call `write_headers_between()` here.
        }

        pub fn set_entry_as_dirty(self: *Self, header: *const Header) void {
            log.debug("{}: set_entry_as_dirty: op={} checksum={}", .{
                self.replica,
                header.op,
                header.checksum,
            });
            if (self.entry(header)) |existing| {
                if (existing.checksum != header.checksum) {
                    self.faulty.clear(header.op);
                }
            }
            self.headers[header.op] = header.*;
            self.dirty.set(header.op);
            // Do not clear any faulty bit for the same entry.
        }

        pub fn write_prepare(
            self: *Self,
            callback: fn (self: *Replica, wrote: ?*Message, trigger: Write.Trigger) void,
            message: *Message,
            trigger: Self.Write.Trigger,
        ) void {
            const replica = @fieldParentPtr(Replica, "journal", self);

            assert(message.header.command == .prepare);
            assert(message.header.size >= @sizeOf(Header));
            assert(message.header.size <= message.buffer.len);

            // The underlying header memory must be owned by the buffer and not by self.headers:
            // Otherwise, concurrent writes may modify the memory of the pointer while we write.
            assert(@ptrToInt(message.header) == @ptrToInt(message.buffer.ptr));

            if (!self.dirty.bit(message.header.op)) {
                // Any function that sets the faulty bit should also set the dirty bit:
                assert(!self.faulty.bit(message.header.op));
                self.write_prepare_debug(message.header, "skipping (clean)");
                callback(replica, message, trigger);
                return;
            }

            assert(self.has_dirty(message.header));

            const write = self.writes.acquire() orelse {
                self.write_prepare_debug(message.header, "waiting for IOP");
                callback(replica, null, trigger);
                return;
            };

            self.write_prepare_debug(message.header, "starting");

            write.* = .{
                .self = self,
                .callback = callback,
                .message = message.ref(),
                .trigger = trigger,
                .range = undefined,
            };

            // Slice the message to the nearest sector, we don't want to write the whole buffer:
            const sectors = message.buffer[0..vsr.sector_ceil(message.header.size)];
            assert(message.header.offset + sectors.len <= self.size_circular_buffer);

            if (builtin.mode == .Debug) {
                // Assert that any sector padding has already been zeroed:
                var sum_of_sector_padding_bytes: u32 = 0;
                for (sectors[message.header.size..]) |byte| sum_of_sector_padding_bytes += byte;
                assert(sum_of_sector_padding_bytes == 0);
            }

            self.write_sectors(
                write_prepare_header,
                write,
                sectors,
                self.offset_in_circular_buffer(message.header.offset),
            );
        }

        /// Attempt to lock the in-memory sector containing the header being written.
        /// If the sector is already locked, add this write to the wait queue.
        fn write_prepare_header(write: *Self.Write) void {
            const self = write.self;
            const message = write.message;

            if (!self.has(message.header)) {
                self.write_prepare_debug(message.header, "entry changed while writing sectors");
                self.write_prepare_release(write, null);
                return;
            }

            assert(!write.header_sector_locked);
            assert(write.header_sector_next == null);

            var it = self.writes.iterate();
            while (it.next()) |other| {
                if (other == write) continue;
                if (!other.header_sector_locked) continue;

                if (other.header_sector_same(write)) {
                    write.header_sector_next = other.header_sector_next;
                    other.header_sector_next = write;
                    return;
                }
            }

            write.header_sector_locked = true;
            self.write_prepare_on_lock_header_sector(write);
        }

        fn write_prepare_on_lock_header_sector(self: *Self, write: *Write) void {
            assert(write.header_sector_locked);

            // TODO It's possible within this section that the header has since been replaced but we
            // continue writing, even when the dirty bit is no longer set. This is not a problem
            // but it would be good to stop writing as soon as we see we no longer need to.
            // For this, we'll need to have a way to tweak write_prepare_release() to release locks.
            // At present, we don't return early here simply because it doesn't yet do that.

            const message = write.message;
            const offset = write_prepare_header_offset(write.message);
            std.mem.copy(
                u8,
                write.header_sector(self),
                std.mem.sliceAsBytes(self.headers)[offset..][0..config.sector_size],
            );

            log.debug("{}: write_header: op={} sectors[{}..{}]", .{
                self.replica,
                message.header.op,
                offset,
                offset + config.sector_size,
            });

            // TODO Snapshots
            if (self.write_prepare_header_once(message.header)) {
                const version = self.write_headers_increment_version();
                self.write_prepare_header_to_version(write, write_prepare_on_write_header, version, write.header_sector(self), offset);
            } else {
                // Versions must be incremented upfront:
                // If we don't increment upfront we could end up writing to the same copy twice.
                // We would then lose the redundancy required to locate headers or even overwrite all copies.
                const version = self.write_headers_increment_version();
                _ = self.write_headers_increment_version();
                switch (version) {
                    0 => self.write_prepare_header_to_version(write, write_prepare_on_write_header_version_0, 0, write.header_sector(self), offset),
                    1 => self.write_prepare_header_to_version(write, write_prepare_on_write_header_version_1, 1, write.header_sector(self), offset),
                }
            }
        }

        fn write_prepare_on_write_header_version_0(write: *Self.Write) void {
            const self = write.self;
            const offset = write_prepare_header_offset(write.message);
            // Pass the opposite version bit from the one we just finished writing.
            self.write_prepare_header_to_version(write, write_prepare_on_write_header, 1, write.header_sector(self), offset);
        }

        fn write_prepare_on_write_header_version_1(write: *Self.Write) void {
            const self = write.self;
            const offset = write_prepare_header_offset(write.message);
            // Pass the opposite version bit from the one we just finished writing.
            self.write_prepare_header_to_version(write, write_prepare_on_write_header, 0, write.header_sector(self), offset);
        }

        fn write_prepare_on_write_header(write: *Self.Write) void {
            const self = write.self;
            const message = write.message;

            assert(write.header_sector_locked);
            self.write_prepare_unlock_header_sector(write);

            if (!self.has(message.header)) {
                self.write_prepare_debug(message.header, "entry changed while writing headers");
                self.write_prepare_release(write, null);
                return;
            }

            self.write_prepare_debug(message.header, "complete, marking clean");
            // TODO Snapshots
            assert(self.has(message.header));
            self.dirty.clear(message.header.op);
            self.faulty.clear(message.header.op);

            self.write_prepare_release(write, message);
        }

        /// Release the lock held by a write on an in-memory header sector and pass
        /// it to a waiting Write, if any.
        fn write_prepare_unlock_header_sector(self: *Self, write: *Self.Write) void {
            assert(write.header_sector_locked);
            write.header_sector_locked = false;

            // Unlike the ranges of physical memory we lock when writing to disk,
            // these header sector locks are always an exact match, so there's no
            // need to re-check the waiting writes against all other writes.
            if (write.header_sector_next) |waiting| {
                write.header_sector_next = null;

                assert(waiting.header_sector_locked == false);
                waiting.header_sector_locked = true;
                self.write_prepare_on_lock_header_sector(waiting);
            }
            assert(write.header_sector_next == null);
        }

        fn write_prepare_release(self: *Self, write: *Self.Write, wrote: ?*Message) void {
            const replica = @fieldParentPtr(Replica, "journal", self);
            write.callback(replica, wrote, write.trigger);
            replica.message_bus.unref(write.message);
            self.writes.release(write);
        }

        fn write_prepare_debug(self: *Self, header: *const Header, status: []const u8) void {
            log.debug("{}: write: view={} op={} offset={} len={}: {} {s}", .{
                self.replica,
                header.view,
                header.op,
                header.offset,
                header.size,
                header.checksum,
                status,
            });
        }

        pub fn offset_in_circular_buffer(self: *Self, offset: u64) u64 {
            assert(offset < self.size_circular_buffer);
            return self.size_headers + offset;
        }

        fn offset_in_headers_version(self: *Self, offset: u64, version: u1) u64 {
            assert(offset < self.size_headers);
            return switch (version) {
                0 => offset,
                1 => self.size_headers + self.size_circular_buffer + offset,
            };
        }

        fn write_prepare_header_offset(message: *Message) u64 {
            comptime assert(config.sector_size % @sizeOf(Header) == 0);
            return vsr.sector_floor(message.header.op * @sizeOf(Header));
        }

        fn write_headers_increment_version(self: *Self) u1 {
            self.headers_version +%= 1;
            return self.headers_version;
        }

        /// Since we allow gaps in the journal, we may have to write our headers twice.
        /// If a dirty header is being written as reserved (empty) then write twice to make this clear.
        /// If a dirty header has no previous clean chained entry to give its offset then write twice.
        /// Otherwise, we only need to write the headers once because their other copy can be located in
        /// the body of the journal (using the previous entry's offset and size).
        fn write_prepare_header_once(self: *Self, header: *const Header) bool {
            // TODO Optimize this to decide whether to write once or twice once we add support to
            // recover from either header version at startup.
            const always_write_twice = true;
            if (always_write_twice) return false;

            // TODO Snapshots
            if (header.command == .reserved) {
                log.debug("{}: write_prepare_header_once: dirty reserved header", .{
                    self.replica,
                });
                return false;
            }
            if (self.previous_entry(header)) |previous| {
                assert(previous.command == .prepare);
                if (previous.checksum != header.parent) {
                    log.debug("{}: write_headers_once: no hash chain", .{self.replica});
                    return false;
                }
                // TODO Add is_dirty(header)
                // TODO Snapshots
                if (self.dirty.bit(previous.op)) {
                    log.debug("{}: write_prepare_header_once: previous entry is dirty", .{
                        self.replica,
                    });
                    return false;
                }
            } else {
                log.debug("{}: write_prepare_header_once: no previous entry", .{self.replica});
                return false;
            }
            return true;
        }

        fn write_prepare_header_to_version(
            self: *Self,
            write: *Self.Write,
            callback: fn (completion: *Self.Write) void,
            version: u1,
            buffer: []const u8,
            offset: u64,
        ) void {
            log.debug("{}: write_prepare_header_to_version: version={} offset={} len={}", .{
                self.replica,
                version,
                offset,
                buffer.len,
            });
            assert(offset + buffer.len <= self.size_headers);
            // Memory must not be owned by self.headers as self.headers may be modified concurrently:
            assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
                @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

            self.write_sectors(
                callback,
                write,
                buffer,
                self.offset_in_headers_version(offset, version),
            );
        }

        fn write_sectors(
            self: *Self,
            callback: fn (write: *Self.Write) void,
            write: *Self.Write,
            buffer: []const u8,
            offset: u64,
        ) void {
            write.range = .{
                .callback = callback,
                .completion = undefined,
                .buffer = buffer,
                .offset = offset,
                .locked = false,
            };
            self.lock_sectors(write);
        }

        /// Start the write on the current range or add it to the proper queue
        /// if an overlapping range is currently being written.
        fn lock_sectors(self: *Self, write: *Self.Write) void {
            assert(!write.range.locked);
            assert(write.range.next == null);

            var it = self.writes.iterate();
            while (it.next()) |other| {
                if (other == write) continue;
                if (!other.range.locked) continue;

                if (other.range.overlaps(&write.range)) {
                    write.range.next = other.range.next;
                    other.range.next = &write.range;
                    return;
                }
            }

            log.debug("{}: write_sectors: offset={} len={} locked", .{
                self.replica,
                write.range.offset,
                write.range.buffer.len,
            });

            write.range.locked = true;
            self.storage.write_sectors(
                write_sectors_on_write,
                &write.range.completion,
                write.range.buffer,
                write.range.offset,
            );
            // We rely on the Storage.write_sectors() implementation being either always synchronous,
            // in which case writes never actually need to be queued, or always always asynchronous,
            // in which case write_sectors_on_write() doesn't have to handle lock_sectors()
            // synchronously completing a write and making a nested write_sectors_on_write() call.
            //
            // We don't currently allow Storage implementations that are sometimes synchronous and
            // sometimes asynchronous as we don't have a use case for such a Storage implementation
            // and doing so would require a significant complexity increase.
            switch (Storage.synchronicity) {
                .always_synchronous => assert(!write.range.locked),
                .always_asynchronous => assert(write.range.locked),
            }
        }

        fn write_sectors_on_write(completion: *Storage.Write) void {
            const range = @fieldParentPtr(Range, "completion", completion);
            const write = @fieldParentPtr(Self.Write, "range", range);
            const self = write.self;

            assert(write.range.locked);
            write.range.locked = false;

            log.debug("{}: write_sectors: offset={} len={} unlocked", .{
                self.replica,
                write.range.offset,
                write.range.buffer.len,
            });

            // Drain the list of ranges that were waiting on this range to complete.
            var current = range.next;
            range.next = null;
            while (current) |waiting| {
                assert(waiting.locked == false);
                current = waiting.next;
                waiting.next = null;
                self.lock_sectors(@fieldParentPtr(Self.Write, "range", waiting));
            }

            // The callback may set range, so we can't set range to undefined after running the callback.
            const callback = range.callback;
            range.* = undefined;
            callback(write);
        }

        pub fn writing(self: *Self, op: u64, checksum: u128) bool {
            var it = self.writes.iterate();
            while (it.next()) |write| {
                // It's possible that we might be writing the same op but with a different checksum.
                // For example, if the op we are writing did not survive the view change and was
                // replaced by another op. We must therefore do the search primarily on checksum.
                // However, we compare against the 64-bit op first, since it's a cheap machine word.
                if (write.message.header.op == op and write.message.header.checksum == checksum) {
                    // If we truly are writing, then the dirty bit must be set:
                    assert(self.dirty.bit(op));
                    return true;
                }
            }
            return false;
        }
    };
}

// TODO Snapshots
pub const BitSet = struct {
    bits: []bool,

    /// The number of bits set (updated incrementally as bits are set or cleared):
    len: u64 = 0,

    fn init(allocator: Allocator, count: u64) !BitSet {
        const bits = try allocator.alloc(bool, count);
        errdefer allocator.free(bits);
        std.mem.set(bool, bits, false);

        return BitSet{ .bits = bits };
    }

    fn deinit(self: *BitSet, allocator: Allocator) void {
        allocator.free(self.bits);
    }

    /// Clear the bit for an op (idempotent):
    pub fn clear(self: *BitSet, op: u64) void {
        if (self.bits[op]) {
            self.bits[op] = false;
            self.len -= 1;
        }
    }

    /// Whether the bit for an op is set:
    pub fn bit(self: *BitSet, op: u64) bool {
        return self.bits[op];
    }

    /// Set the bit for an op (idempotent):
    pub fn set(self: *BitSet, op: u64) void {
        if (!self.bits[op]) {
            self.bits[op] = true;
            self.len += 1;
            assert(self.len <= self.bits.len);
        }
    }
};

/// Take a u6 to limit to 64 items max (2^6 = 64)
pub fn IOPS(comptime T: type, comptime size: u6) type {
    const Map = std.meta.Int(.unsigned, size);
    const MapLog2 = math.Log2Int(Map);
    return struct {
        const Self = @This();

        items: [size]T = undefined,
        /// 1 bits are free items
        free: Map = math.maxInt(Map),

        pub fn acquire(self: *Self) ?*T {
            const i = @ctz(Map, self.free);
            assert(i <= @bitSizeOf(Map));
            if (i == @bitSizeOf(Map)) return null;
            self.free &= ~(@as(Map, 1) << @intCast(MapLog2, i));
            return &self.items[i];
        }

        pub fn release(self: *Self, item: *T) void {
            item.* = undefined;
            const i = (@ptrToInt(item) - @ptrToInt(&self.items)) / @sizeOf(T);
            assert(self.free & (@as(Map, 1) << @intCast(MapLog2, i)) == 0);
            self.free |= (@as(Map, 1) << @intCast(MapLog2, i));
        }

        /// Returns true if there is at least one IOP available
        pub fn available(self: *const Self) math.Log2IntCeil(Map) {
            return @popCount(Map, self.free);
        }

        /// Returns true if there is at least one IOP in use
        pub fn executing(self: *const Self) math.Log2IntCeil(Map) {
            return @popCount(Map, math.maxInt(Map)) - @popCount(Map, self.free);
        }

        pub const Iterator = struct {
            iops: *Self,
            /// On iteration start this is a copy of the free map, but
            /// inverted so we can use @ctz() to find occupied instead of free slots.
            unseen: Map,

            pub fn next(iterator: *Iterator) ?*T {
                const i = @ctz(Map, iterator.unseen);
                assert(i <= @bitSizeOf(Map));
                if (i == @bitSizeOf(Map)) return null;
                // Set this bit of unseen to 1 to indicate this slot has been seen.
                iterator.unseen &= ~(@as(Map, 1) << @intCast(MapLog2, i));
                return &iterator.iops.items[i];
            }
        };

        pub fn iterate(self: *Self) Iterator {
            return .{ .iops = self, .unseen = ~self.free };
        }
    };
}

test {
    const testing = std.testing;
    var iops = IOPS(u32, 4){};

    try testing.expectEqual(@as(u4, 4), iops.available());
    try testing.expectEqual(@as(u4, 0), iops.executing());

    var one = iops.acquire().?;

    try testing.expectEqual(@as(u4, 3), iops.available());
    try testing.expectEqual(@as(u4, 1), iops.executing());

    var two = iops.acquire().?;
    var three = iops.acquire().?;

    try testing.expectEqual(@as(u4, 1), iops.available());
    try testing.expectEqual(@as(u4, 3), iops.executing());

    var four = iops.acquire().?;
    try testing.expectEqual(@as(?*u32, null), iops.acquire());

    try testing.expectEqual(@as(u4, 0), iops.available());
    try testing.expectEqual(@as(u4, 4), iops.executing());

    iops.release(two);

    try testing.expectEqual(@as(u4, 1), iops.available());
    try testing.expectEqual(@as(u4, 3), iops.executing());

    // there is only one slot free, so we will get the same pointer back.
    try testing.expectEqual(@as(?*u32, two), iops.acquire());

    iops.release(four);
    iops.release(two);
    iops.release(one);
    iops.release(three);

    try testing.expectEqual(@as(u4, 4), iops.available());
    try testing.expectEqual(@as(u4, 0), iops.executing());

    one = iops.acquire().?;
    two = iops.acquire().?;
    three = iops.acquire().?;
    four = iops.acquire().?;
    try testing.expectEqual(@as(?*u32, null), iops.acquire());
}
