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

/// There are two contiguous circular buffers on disk in the journal storage zone.
///
/// In both rings, the `op` for each reserved header is set to the slot index.
/// This helps WAL recovery detect misdirected reads/writes.
const Ring = enum {
    /// A circular buffer of prepare message headers.
    headers,
    /// A circular buffer of prepare messages. Each slot is padded to `config.message_size_max`.
    prepares,
};

const headers_per_sector = @divExact(config.sector_size, @sizeOf(Header));
comptime {
    assert(headers_per_sector > 0);
}

/// A slot is `op % config.journal_slot_count`.
const Slot = struct { index: u64 };

const slot_count = config.journal_slot_count;
const headers_size = config.journal_size_headers;
const prepares_size = config.journal_size_prepares;

pub const write_ahead_log_zone_size = headers_size + prepares_size;

comptime {
    assert(slot_count > 0);
    assert(slot_count % 2 == 0);
    assert(slot_count % headers_per_sector == 0);
    assert(slot_count >= headers_per_sector);
    // The length of the prepare pipeline is the upper bound on how many ops can be
    // reordered during a view change. See `recover_prepares_callback()` for more detail.
    assert(slot_count > config.pipelining_max);

    assert(headers_size > 0);
    assert(headers_size % config.sector_size == 0);

    assert(prepares_size > 0);
    assert(prepares_size % config.sector_size == 0);
    assert(prepares_size % config.message_size_max == 0);
}

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
            pub const Trigger = enum { append, repair, pipeline };

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
                const i = @divExact(
                    @ptrToInt(write) - @ptrToInt(&journal.writes.items),
                    @sizeOf(Self.Write),
                );
                // TODO The compiler should not need this align cast as the type of `headers_iops`
                // ensures that each buffer is properly aligned.
                return @alignCast(config.sector_size, &journal.headers_iops[i]);
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

        /// A header is located at `slot == header.op % headers.len`.
        ///
        /// Each slot's `header.command` is either `prepare` or `reserved`.
        /// When the slot's header is `reserved`, the header's `op` is the slot index.
        headers: []align(config.sector_size) Header,

        /// We copy-on-write to these buffers, as the in-memory headers may change while writing.
        /// The buffers belong to the IOP at the corresponding index in IOPS.
        headers_iops: *align(config.sector_size) [config.io_depth_write][config.sector_size]u8,

        /// Statically allocated read IO operation context data.
        reads: IOPS(Read, config.io_depth_read) = .{},

        /// Statically allocated write IO operation context data.
        writes: IOPS(Write, config.io_depth_write) = .{},

        /// Whether an entry is in memory only and needs to be written or is being written:
        /// We use this in the same sense as a dirty bit in the kernel page cache.
        /// A dirty bit means that we have not prepared the entry, or need to repair a faulty entry.
        dirty: BitSet,

        /// Whether an entry was written to disk and this write was subsequently lost due to:
        /// * corruption,
        /// * a misdirected write (or a misdirected read, we do not distinguish), or else
        /// * a latent sector error, where the sector can no longer be read.
        /// A faulty bit means that we prepared and then lost the entry.
        /// A faulty bit requires the dirty bit to also be set so that callers need not check both.
        /// A faulty bit is used then only to qualify the severity of the dirty bit.
        faulty: BitSet,

        /// The checksum of the prepare in the corresponding slot.
        /// This is used to respond to `request_prepare` messages even when the slot is faulty.
        /// For example, the slot may be faulty because the redundant header is faulty.
        ///
        /// The checksum will missing (`prepare_checksums[i]=0`, `prepare_inhabited[i]=false`) when:
        /// * the message in the slot is reserved,
        /// * the message in the slot is being written, or when
        /// * the message in the slot is corrupt.
        // TODO: `prepare_checksums` and `prepare_inhabited` should be combined into a []?u128,
        // but that type is currently unusable (as of Zig 0.9.1).
        // See: https://github.com/ziglang/zig/issues/9871
        prepare_checksums: []u128,
        /// When prepare_inhabited[i]==false, prepare_checksums[i]==0.
        /// (`undefined` would may more sense than `0`, but `0` allows it to be asserted).
        prepare_inhabited: []bool,

        recovered: bool = false,
        recovering: bool = false,

        pub fn init(allocator: Allocator, storage: *Storage, replica: u8) !Self {
            assert(write_ahead_log_zone_size <= storage.size);

            var headers = try allocator.allocAdvanced(
                Header,
                config.sector_size,
                slot_count,
                .exact,
            );
            errdefer allocator.free(headers);
            for (headers) |*header| header.* = undefined;

            var dirty = try BitSet.init(allocator, slot_count);
            errdefer dirty.deinit(allocator);

            var faulty = try BitSet.init(allocator, slot_count);
            errdefer faulty.deinit(allocator);

            var prepare_checksums = try allocator.alloc(u128, slot_count);
            errdefer allocator.free(prepare_checksums);
            std.mem.set(u128, prepare_checksums, 0);

            var prepare_inhabited = try allocator.alloc(bool, slot_count);
            errdefer allocator.free(prepare_inhabited);
            std.mem.set(bool, prepare_inhabited, false);

            const headers_iops = (try allocator.allocAdvanced(
                [config.sector_size]u8,
                config.sector_size,
                config.io_depth_write,
                .exact,
            ))[0..config.io_depth_write];
            errdefer allocator.free(headers_iops);

            log.debug("{}: slot_count={} size={} headers_size={} prepares_size={}", .{
                replica,
                slot_count,
                std.fmt.fmtIntSizeBin(write_ahead_log_zone_size),
                std.fmt.fmtIntSizeBin(headers_size),
                std.fmt.fmtIntSizeBin(prepares_size),
            });

            var self = Self{
                .storage = storage,
                .replica = replica,
                .headers = headers,
                .dirty = dirty,
                .faulty = faulty,
                .prepare_checksums = prepare_checksums,
                .prepare_inhabited = prepare_inhabited,
                .headers_iops = headers_iops,
            };

            assert(@mod(@ptrToInt(&self.headers[0]), config.sector_size) == 0);
            assert(self.dirty.bits.bit_length == slot_count);
            assert(self.faulty.bits.bit_length == slot_count);
            assert(self.prepare_checksums.len == slot_count);
            assert(self.prepare_inhabited.len == slot_count);

            return self;
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            const replica = @fieldParentPtr(Replica, "journal", self);

            self.dirty.deinit(allocator);
            self.faulty.deinit(allocator);
            allocator.free(self.headers);
            allocator.free(self.headers_iops);
            allocator.free(self.prepare_checksums);
            allocator.free(self.prepare_inhabited);

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
        pub fn assert_headers_reserved_from(self: *const Self, op_min: u64) void {
            // TODO Snapshots
            for (self.headers) |*header| {
                assert(header.command == .reserved or header.op < op_min);
            }
        }

        /// Returns whether this is a fresh database WAL; no prepares (except the root) have ever
        /// been written. This determines whether a replica can transition immediately to normal
        /// status, or if it needs to run recovery protocol.
        ///
        /// Called by the replica immediately after WAL recovery completes, but before the replica
        /// issues any I/O from handling messages.
        pub fn is_empty(self: *const Self) bool {
            assert(!self.recovering);
            assert(self.recovered);
            assert(self.writes.executing() == 0);

            const replica = @fieldParentPtr(Replica, "journal", self);
            if (self.headers[0].operation != .root) return false;

            assert(self.headers[0].checksum == Header.root_prepare(replica.cluster).checksum);
            assert(self.headers[0].checksum == self.prepare_checksums[0]);
            assert(self.prepare_inhabited[0]);

            // If any message is faulty, we must fall back to VSR recovery protocol (i.e. treat
            // this as a non-empty WAL) since that message may have been a prepare.
            if (self.faulty.count > 0) return false;
            assert(self.dirty.count == 0);

            for (self.headers[1..]) |*header| {
                if (header.command == .prepare) return false;
            }

            for (self.prepare_inhabited[1..]) |inhabited| {
                if (inhabited) return false;
            }

            return true;
        }

        pub fn slot_for_op(_: *const Self, op: u64) Slot {
            return Slot{ .index = op % slot_count };
        }

        pub fn slot_with_op(self: *const Self, op: u64) ?Slot {
            if (self.header_with_op(op)) |_| {
                return self.slot_for_op(op);
            } else {
                return null;
            }
        }

        pub fn slot_with_op_and_checksum(self: *const Self, op: u64, checksum: u128) ?Slot {
            if (self.header_with_op_and_checksum(op, checksum)) |_| {
                return self.slot_for_op(op);
            } else {
                return null;
            }
        }

        pub fn slot_for_header(self: *const Self, header: *const Header) Slot {
            assert(header.command == .prepare);
            return self.slot_for_op(header.op);
        }

        pub fn slot_with_header(self: *const Self, header: *const Header) ?Slot {
            assert(header.command == .prepare);
            return self.slot_with_op(header.op);
        }

        /// Returns any existing entry at the location indicated by header.op.
        /// This existing entry may have an older or newer op number.
        pub fn header_for_entry(self: *const Self, header: *const Header) ?*const Header {
            assert(header.command == .prepare);
            return self.header_for_op(header.op);
        }

        /// We use `op` directly to index into the headers array and locate ops without a scan.
        pub fn header_for_op(self: *const Self, op: u64) ?*const Header {
            // TODO Snapshots
            const slot = self.slot_for_op(op);
            const existing = &self.headers[slot.index];
            switch (existing.command) {
                .prepare => {
                    assert(self.slot_for_op(existing.op).index == slot.index);
                    return existing;
                },
                .reserved => {
                    assert(existing.op == slot.index);
                    return null;
                },
                else => unreachable,
            }
        }

        /// Returns the entry at `@mod(op)` location, but only if `entry.op == op`, else `null`.
        /// Be careful of using this without considering that there may still be an existing op.
        pub fn header_with_op(self: *const Self, op: u64) ?*const Header {
            if (self.header_for_op(op)) |existing| {
                if (existing.op == op) return existing;
            }
            return null;
        }

        /// As per `header_with_op()`, but only if there is an optional checksum match.
        pub fn header_with_op_and_checksum(
            self: *const Self,
            op: u64,
            checksum: ?u128,
        ) ?*const Header {
            if (self.header_with_op(op)) |existing| {
                assert(existing.op == op);
                if (checksum == null or existing.checksum == checksum.?) return existing;
            }
            return null;
        }

        // TODO How should we handle the case where the current header argument is the same as
        // op_checkpoint?
        pub fn previous_entry(self: *const Self, header: *const Header) ?*const Header {
            if (header.op == 0) {
                return null;
            } else {
                return self.header_for_op(header.op - 1);
            }
        }

        pub fn next_entry(self: *const Self, header: *const Header) ?*const Header {
            return self.header_for_op(header.op + 1);
        }

        /// Returns the highest op number prepared, in any slot without reference to the checkpoint.
        pub fn op_maximum(self: *const Self) u64 {
            assert(self.recovered);

            var op: u64 = 0;
            for (self.headers) |*header| {
                if (header.command == .prepare) {
                    if (header.op > op) op = header.op;
                } else {
                    assert(header.command == .reserved);
                }
            }
            return op;
        }

        /// Returns the highest op number prepared, after the checkpoint, without gaps.
        /// This must only be used for a cluster-of-one, where pipelining may create gaps.
        pub fn op_maximum_continuous(self: *const Self, op_checkpoint: u64) u64 {
            const replica = @fieldParentPtr(Replica, "journal", self);
            assert(replica.replica_count == 1);

            assert(self.recovered);
            assert(self.dirty.count == 0);
            assert(self.faulty.count == 0);

            // TODO Can we assert that op_checkpoint exists?

            var op: u64 = op_checkpoint;
            while (op < op_checkpoint + slot_count) : (op += 1) {
                const next = &self.headers[self.slot_for_op(op + 1).index];

                if (next.command == .reserved) break;
                assert(next.command == .prepare);

                if (next.op != op + 1) {
                    assert(next.op <= op_checkpoint);
                    break;
                }
            }

            const op_max = self.op_maximum();
            assert(op >= op_checkpoint);
            assert(op <= op_max);
            assert(op + config.pipelining_max >= op_max); // We expect gaps only in the pipeline.

            return op;
        }

        pub fn has(self: *const Self, header: *const Header) bool {
            assert(header.command == .prepare);
            // TODO Snapshots
            const slot = self.slot_for_op(header.op);
            const existing = &self.headers[slot.index];
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

        pub fn has_clean(self: *const Self, header: *const Header) bool {
            // TODO Snapshots
            return self.has(header) and !self.dirty.bit(self.slot_with_header(header).?);
        }

        pub fn has_dirty(self: *const Self, header: *const Header) bool {
            // TODO Snapshots
            return self.has(header) and self.dirty.bit(self.slot_with_header(header).?);
        }

        /// Copies latest headers between `op_min` and `op_max` (both inclusive) as fit in `dest`.
        /// Reverses the order when copying so that latest headers are copied first, which protects
        /// against the callsite slicing the buffer the wrong way and incorrectly.
        /// Skips .reserved headers (gaps between headers).
        /// Zeroes the `dest` buffer in case the copy would underflow and leave a buffer bleed.
        /// Returns the number of headers actually copied.
        pub fn copy_latest_headers_between(
            self: *const Self,
            op_min: u64,
            op_max: u64,
            dest: []Header,
        ) usize {
            assert(op_min <= op_max);
            assert(dest.len > 0);

            var copied: usize = 0;
            // Poison all slots; only slots less than `copied` are used.
            std.mem.set(Header, dest, undefined);

            // Start at op_max + 1 and do the decrement upfront to avoid overflow when op_min == 0:
            var op = op_max + 1;
            while (op > op_min) {
                op -= 1;

                if (self.header_with_op(op)) |header| {
                    dest[copied] = header.*;
                    assert(dest[copied].invalid() == null);
                    copied += 1;
                    if (copied == dest.len) break;
                }
            }

            log.debug(
                "{}: copy_latest_headers_between: op_min={} op_max={} dest.len={} copied={}",
                .{
                    self.replica,
                    op_min,
                    op_max,
                    dest.len,
                    copied,
                },
            );

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
                var A = self.header_with_op(op);
                if (A) |a| {
                    if (B) |b| {
                        // If A was reordered then A may have a newer op than B (but an older view).
                        // However, here we use header_with_op() to assert a.op + 1 == b.op:
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
            assert(checksum != 0);

            const replica = @fieldParentPtr(Replica, "journal", self);
            if (op > replica.op) {
                self.read_prepare_log(op, checksum, "beyond replica.op");
                callback(replica, null, null);
                return;
            }

            // Do not use this pointer beyond this function's scope, as the
            // header memory may then change:
            const exact = self.header_with_op_and_checksum(op, checksum) orelse {
                self.read_prepare_log(op, checksum, "no entry exactly");
                callback(replica, null, null);
                return;
            };

            const slot = self.slot_with_op_and_checksum(op, checksum).?;
            if (self.faulty.bit(slot)) {
                assert(self.dirty.bit(slot));

                self.read_prepare_log(op, checksum, "faulty");
                callback(replica, null, null);
                return;
            }

            if (self.dirty.bit(slot)) {
                self.read_prepare_log(op, checksum, "dirty");
                callback(replica, null, null);
                return;
            }

            // Skip the disk read if the header is all we need:
            if (exact.size == @sizeOf(Header)) {
                const message = replica.message_bus.get_message();
                defer replica.message_bus.unref(message);

                message.header.* = exact.*;
                callback(replica, message, destination_replica);
                return;
            }

            self.read_prepare_with_op_and_checksum(callback, op, checksum, destination_replica);
        }

        /// Read a prepare from disk. There may or may not be an in-memory header.
        pub fn read_prepare_with_op_and_checksum(
            self: *Self,
            callback: fn (replica: *Replica, prepare: ?*Message, destination_replica: ?u8) void,
            op: u64,
            checksum: u128,
            destination_replica: ?u8,
        ) void {
            const replica = @fieldParentPtr(Replica, "journal", self);
            const slot = self.slot_for_op(op);
            assert(self.prepare_inhabited[slot.index]);
            assert(self.prepare_checksums[slot.index] == checksum);

            const message = replica.message_bus.get_message();
            defer replica.message_bus.unref(message);

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

            const buffer: []u8 = message.buffer[0..config.message_size_max];
            const offset = offset_physical(.prepares, slot);

            log.debug(
                "{}: read_sectors: offset={} len={}",
                .{ replica.replica, offset, buffer.len },
            );

            // Memory must not be owned by `self.headers` as these may be modified concurrently:
            assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
                @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + headers_size);

            assert_bounds(.prepares, offset, buffer.len);
            self.storage.read_sectors(
                read_prepare_with_op_and_checksum_callback,
                &read.completion,
                buffer,
                offset,
            );
        }

        fn read_prepare_with_op_and_checksum_callback(completion: *Storage.Read) void {
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

            const checksum_inhabited = self.prepare_inhabited[self.slot_for_op(op).index];
            const checksum_match = self.prepare_checksums[self.slot_for_op(op).index] == checksum;
            if (!checksum_inhabited or !checksum_match) {
                self.read_prepare_log(op, checksum, "prepare changed during read");
                read.callback(replica, null, null);
                return;
            }

            // Check that the `headers` slot belongs to the same op that it did when the read began.
            // The slot may not match the Read's op/checksum due to either:
            // * The in-memory header changed since the read began.
            // * The in-memory header is reserved+faulty; the read was via `prepare_checksums`
            const slot = self.slot_with_op_and_checksum(op, checksum);

            if (!read.message.header.valid_checksum()) {
                if (slot) |s| {
                    self.faulty.set(s);
                    self.dirty.set(s);
                }

                self.read_prepare_log(op, checksum, "corrupt header after read");
                read.callback(replica, null, null);
                return;
            }

            if (read.message.header.cluster != replica.cluster) {
                // This could be caused by a misdirected read or write.
                // Though when a prepare spans multiple sectors, a misdirected read/write will
                // likely manifest as a checksum failure instead.
                if (slot) |s| {
                    self.faulty.set(s);
                    self.dirty.set(s);
                }

                self.read_prepare_log(op, checksum, "wrong cluster");
                read.callback(replica, null, null);
                return;
            }

            if (read.message.header.op != op) {
                // Possible causes:
                // * The prepare was rewritten since the read began.
                // * Misdirected read/write.
                // * The combination of:
                //   * The leader is responding to a `request_prepare`.
                //   * The `request_prepare` did not include a checksum.
                //   * The requested op's slot is faulty, but the prepare is valid. Since the
                //     prepare is valid, WAL recovery set `prepare_checksums[slot]`. But on reading
                //     this entry it turns out not to have the right op.
                //   (This case (and the accompanying unnessary read) could be prevented by storing
                //   the op along with the checksum in `prepare_checksums`.)
                assert(slot == null);

                self.read_prepare_log(op, checksum, "op changed during read");
                read.callback(replica, null, null);
                return;
            }

            if (read.message.header.checksum != checksum) {
                // This can also be caused by a misdirected read/write.
                assert(slot == null);

                self.read_prepare_log(op, checksum, "checksum changed during read");
                read.callback(replica, null, null);
                return;
            }

            if (!read.message.header.valid_checksum_body(read.message.body())) {
                if (slot) |s| {
                    self.faulty.set(s);
                    self.dirty.set(s);
                }

                self.read_prepare_log(op, checksum, "corrupt body after read");
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
            assert(!self.recovering);
            assert(self.dirty.count == 0);
            assert(self.faulty.count == 0);

            self.recovering = true;

            log.debug("{}: recover: recovering", .{self.replica});

            self.recover_headers(0);
        }

        fn recover_headers(self: *Self, offset: u64) void {
            const replica = @fieldParentPtr(Replica, "journal", self);

            assert(!self.recovered);
            assert(self.recovering);
            assert(self.dirty.count == 0);
            assert(self.faulty.count == 0);

            if (offset == headers_size) {
                log.debug("{}: recover_headers: complete", .{self.replica});
                self.recover_prepares(Slot{ .index = 0 });
                return;
            }
            assert(offset < headers_size);

            const message = replica.message_bus.get_message();
            defer replica.message_bus.unref(message);

            // We expect that no other process is issuing reads while we are recovering.
            assert(self.reads.executing() == 0);

            const read = self.reads.acquire() orelse unreachable;
            read.* = .{
                .self = self,
                .completion = undefined,
                .message = message.ref(),
                .callback = undefined,
                .op = undefined,
                .checksum = offset,
                .destination_replica = null,
            };

            const buffer = recover_headers_buffer(message, offset);
            assert(buffer.len > 0);

            log.debug("{}: recover_headers: offset={} size={} recovering", .{
                self.replica,
                offset,
                buffer.len,
            });

            self.storage.read_sectors(
                recover_headers_callback,
                &read.completion,
                buffer,
                offset_physical_for_logical(.headers, offset),
            );
        }

        fn recover_headers_callback(completion: *Storage.Read) void {
            const read = @fieldParentPtr(Self.Read, "completion", completion);
            const self = read.self;
            const replica = @fieldParentPtr(Replica, "journal", self);
            const message = read.message;

            const offset = @intCast(u64, read.checksum);
            const buffer = recover_headers_buffer(message, offset);

            log.debug("{}: recover_headers: offset={} size={} recovered", .{
                self.replica,
                offset,
                buffer.len,
            });

            assert(!self.recovered);
            assert(self.recovering);
            assert(offset % @sizeOf(Header) == 0);
            assert(buffer.len >= @sizeOf(Header));
            assert(buffer.len % @sizeOf(Header) == 0);
            assert(read.destination_replica == null);
            assert(self.dirty.count == 0);
            assert(self.faulty.count == 0);

            // Directly store all the redundant headers in `self.headers` (including any that are
            // invalid or corrupt). As the prepares are recovered, these will be replaced or
            // removed as necessary.
            const buffer_headers = std.mem.bytesAsSlice(Header, buffer);
            std.mem.copy(
                Header,
                self.headers[@divExact(offset, @sizeOf(Header))..][0..buffer_headers.len],
                buffer_headers,
            );

            const offset_next = offset + buffer.len;
            // We must release before we call `recover_headers()` in case Storage is synchronous.
            // Otherwise, we would run out of messages and reads.
            replica.message_bus.unref(read.message);
            self.reads.release(read);

            self.recover_headers(offset_next);
        }

        fn recover_headers_buffer(message: *Message, offset: u64) []u8 {
            const max = std.math.min(message.buffer.len, headers_size - offset);
            assert(max % config.sector_size == 0);
            assert(max % @sizeOf(Header) == 0);
            return message.buffer[0..max];
        }

        fn recover_prepares(self: *Self, slot: Slot) void {
            const replica = @fieldParentPtr(Replica, "journal", self);
            assert(!self.recovered);
            assert(self.recovering);
            // We expect that no other process is issuing reads while we are recovering.
            assert(self.reads.executing() == 0);

            if (slot.index == slot_count) {
                self.recover_done();
                return;
            }
            assert(slot.index < slot_count);
            assert(!self.dirty.bit(slot));
            assert(!self.faulty.bit(slot));

            const message = replica.message_bus.get_message();
            defer replica.message_bus.unref(message);

            const read = self.reads.acquire() orelse unreachable;
            read.* = .{
                .self = self,
                .completion = undefined,
                .message = message.ref(),
                .callback = undefined,
                .op = undefined,
                .checksum = slot.index,
                .destination_replica = null,
            };

            log.debug("{}: recover_prepares: recovering slot={}", .{
                self.replica,
                slot.index,
            });

            self.storage.read_sectors(
                recover_prepares_callback,
                &read.completion,
                // Only the header of the message needs to be read (128 bytes).
                // However, the minimum read is a 4 KiB sector.
                message.buffer[0..config.sector_size],
                offset_physical(.prepares, slot),
            );
        }

        /// When in doubt about whether a particular message was received, it must be marked as
        /// faulty to avoid nacking a prepare which was received then lost/misdirected/corrupted.
        ///
        ///
        /// There are two special cases where faulty slots must be carefully handled:
        ///
        /// A) Redundant headers are written in batches. Slots that are marked faulty are written
        /// as invalid (zeroed). This ensures that if the replica crashes and recovers, the
        /// entries are still faulty rather than reserved.
        /// The recovery process must be conservative about which headers are stored in
        /// `journal.headers`. To understand why this is important, consider what happens if it did
        /// load the faulty header into `journal.headers`, and then reads it back after a restart:
        ///
        /// 1. Suppose slot 8 is in case @D. Per the table below, mark slot 8 faulty.
        /// 2. Suppose slot 9 is also loaded as faulty.
        /// 3. Journal recovery finishes. The replica beings to repair its missing/broken messages.
        /// 4. VSR recovery protocol fetches the true prepare for slot 9.
        /// 5. The message from step 4 is written to slot 9 of the prepares.
        /// 6. The header from step 4 is written to slot 9 of the redundant headers.
        ///    But writes to the redundant headers are done in batches of `headers_per_sector`!
        ///    So if step 1 loaded slot 8's prepare header into `journal.headers`, slot 8's
        ///    redundant header would be updated at the same time (in the same write) as slot 9.
        /// 7! Immediately after step 6's write finishes, suppose the replica crashes (e.g. due to
        ///    power failure.
        /// 8! Journal recovery again — but now slot 8 is loaded *without* being marked faulty.
        ///    So we may incorrectly nack slot 8's message.
        ///
        /// Therefore, recovery will never load a header into a slot *and* mark that slot faulty.
        ///
        ///
        /// B) When replica_count=1, repairing broken/lost prepares over VSR is not an option,
        /// so if a message is faulty the replica will abort.
        ///
        ///
        /// Recovery decision table:
        ///
        ///   label                   @A  @B  @C  @D  @E  @F  @G  @H  @I  @J  @K  @L
        ///   header valid             0   1   1   0   0   1   1   1   1   1   1   1
        ///   header reserved          _   1   0   _   _   1   0   1   0   0   0   0
        ///   prepare valid            0   0   0   1   1   1   1   1   1   1   1   1
        ///   prepare reserved         _   _   _   1   0   0   1   1   0   0   0   0
        ///   match checksum           _   _   _   _   _   _   _  !1   0   0   0   1
        ///   match op                 _   _   _   _   _   _   _  !1   <   >   1  !1
        ///   match view               _   _   _   _   _   _   _  !1   _   _  !0  !1
        ///   decision (replicas>1)  vsr vsr vsr vsr vsr vsr vsr nil fix vsr vsr eql
        ///   decision (replicas=1)                  fix fix
        ///
        /// Legend:
        ///
        ///    0  false
        ///    1  true
        ///   !0  assert false
        ///   !1  assert true
        ///    _  ignore
        ///    <  header.op < prepare.op
        ///    >  header.op > prepare.op
        ///
        /// A "valid" header/prepare:
        /// 1. has a valid checksum
        /// 2. has the correct cluster
        /// 3. is in the correct slot (op % slot_count)
        /// 4. has command=reserved or command=prepare
        ///
        ///
        /// Regarding cases @E and @F for *single-replica clusters*:
        ///
        /// Most likely the redundant header write was corrupted/lost (respectively).
        /// Since single-replica clusters never rewrite ops, it can be locally repaired by copying
        /// over the prepare's header.
        ///
        /// However, another (astronomically low) possibility is this:
        ///
        /// 1. A prepare is written to slot 12.
        /// 2. A redundant header is written to slot 12.
        /// 3. Continue normal operation; wrap around the WAL.
        /// 4. A prepare is written to slot 12, but lost.
        /// 5. A redundant header is written to slot 12.
        /// 6. Replica crash.
        /// 7. On recovery, the old prepare (from step 1) is read and used to "repair" the corrupt
        ///    redundant header.
        ///
        /// This would be a safety violation — we assume that the likelihood of this event is
        /// negligible.
        fn recover_prepares_callback(completion: *Storage.Read) void {
            const read = @fieldParentPtr(Self.Read, "completion", completion);
            const self = read.self;
            const replica = @fieldParentPtr(Replica, "journal", self);

            assert(!self.recovered);
            assert(self.recovering);
            assert(read.destination_replica == null);

            const slot = Slot{ .index = @intCast(u64, read.checksum) };
            assert(slot.index < slot_count);
            assert(!self.dirty.bit(slot));
            assert(!self.faulty.bit(slot));

            const header: *const Header = &self.headers[slot.index];
            const prepare: *const Header = read.message.header;

            const case = recovery_case(replica.cluster, slot, header, read.message);
            const decision = case.decision(replica.replica_count);

            if (prepare.valid_checksum() and prepare.command == .prepare) {
                assert(!self.prepare_inhabited[slot.index]);

                // Store the message in `prepare_checksums` even if it belongs in a different slot.
                // This improves the availability of `request_prepare`.
                self.prepare_inhabited[slot.index] = true;
                self.prepare_checksums[slot.index] = prepare.checksum;
            }

            switch (decision) {
                .eql => {
                    assert(header.command == .prepare);
                    assert(prepare.command == .prepare);
                    assert(header.checksum == prepare.checksum);
                    assert(!self.dirty.bit(slot));
                    assert(!self.faulty.bit(slot));
                    assert(self.prepare_inhabited[slot.index]);
                    assert(self.prepare_checksums[slot.index] == prepare.checksum);
                    self.headers[slot.index] = header.*;
                },
                .nil => {
                    assert(header.command == .reserved);
                    assert(prepare.command == .reserved);
                    assert(!self.dirty.bit(slot));
                    assert(!self.faulty.bit(slot));
                    assert(!self.prepare_inhabited[slot.index]);
                    assert(self.prepare_checksums[slot.index] == 0);
                    self.headers[slot.index] = Header.reserved(replica.cluster, slot.index);
                },
                .fix => {
                    assert(prepare.command == .prepare);
                    assert(self.prepare_inhabited[slot.index]);
                    assert(self.prepare_checksums[slot.index] == prepare.checksum);
                    if (replica.replica_count == 1) {
                        self.headers[slot.index] = prepare.*;
                        self.dirty.clear(slot);
                    } else {
                        // TODO Repair without retrieving remotely (i.e. don't set dirty or faulty).
                        self.set_header_as_dirty(prepare);
                    }
                    self.faulty.clear(slot);
                },
                .vsr => {
                    self.headers[slot.index] = Header.reserved(replica.cluster, slot.index);
                    self.dirty.set(slot);
                    self.faulty.set(slot);
                },
            }

            switch (decision) {
                .eql, .nil => {
                    log.debug("{}: recover_prepares: recovered slot={} label={s} decision={s}", .{
                        self.replica,
                        slot.index,
                        case.label,
                        @tagName(decision),
                    });
                },
                .fix, .vsr => {
                    log.warn("{}: recover_prepares: recovered slot={} label={s} decision={s}", .{
                        self.replica,
                        slot.index,
                        case.label,
                        @tagName(decision),
                    });
                },
            }

            replica.message_bus.unref(read.message);
            self.reads.release(read);

            self.recover_prepares(Slot{ .index = slot.index + 1 });
        }

        fn recover_done(self: *Self) void {
            const replica = @fieldParentPtr(Replica, "journal", self);

            log.debug("{}: recover_done: dirty={} faulty={}", .{
                self.replica,
                self.dirty.count,
                self.faulty.count,
            });

            assert(self.reads.executing() == 0);
            assert(self.writes.executing() == 0);

            // A cluster-of-1 cannot recover from faults.
            assert(self.faulty.count == 0 or replica.replica_count > 1);
            assert(self.dirty.count <= slot_count);
            assert(self.faulty.count <= slot_count);
            assert(self.faulty.count <= self.dirty.count);
            if (self.faulty.count == slot_count) {
                // Abort if all slots are faulty, since something is very wrong.
                @panic("all WAL slots are faulty");
            }

            if (self.headers[0].op == 0 and self.headers[0].command == .prepare) {
                assert(self.headers[0].checksum == Header.root_prepare(replica.cluster).checksum);
                assert(!self.faulty.bit(Slot{ .index = 0 }));
                // We can only "fix" the root prepare for a cluster-of-1.
                // When `replica_count>1`, "fix" is only used when the log wraps.
                assert(!self.dirty.bit(Slot{ .index = 0 }) or replica.replica_count == 1);
            }

            {
                var op_min: ?u64 = null;
                var op_max: ?u64 = null;
                for (self.headers) |*header, slot| {
                    if (header.command == .reserved) {
                        assert(header.cluster == replica.cluster);
                        assert(header.op == slot);
                        continue;
                    }

                    assert(header.command == .prepare);
                    assert(header.op % slot_count == slot);
                    assert(!self.dirty.bit(Slot{ .index = slot }));
                    assert(!self.faulty.bit(Slot{ .index = slot }));
                    assert(self.prepare_inhabited[slot]);
                    assert(self.prepare_checksums[slot] == header.checksum);

                    if (op_min == null or op_min.? > header.op) op_min = header.op;
                    if (op_max == null or op_max.? < header.op) op_max = header.op;
                }

                if (op_max != null and op_max.? != 0) {
                    // Only committed ops are ever overwritten by a WAL wrap.
                    assert(op_max.? - op_min.? < 2 * slot_count - config.pipelining_max);
                }
            }

            self.recovered = true;
            self.recovering = false;
            // From here it's over to the Recovery protocol from VRR 2012.
        }

        /// A safe way of removing an entry, where the header must match the current entry.
        fn remove_entry(self: *Self, header: *const Header) void {
            const replica = @fieldParentPtr(Replica, "journal", self);
            // Copy the header.op by value to avoid a reset() followed by undefined header.op usage:
            const op = header.op;
            const slot = self.slot_with_header(header).?;
            log.debug("{}: remove_entry: op={} checksum={}", .{
                self.replica,
                op,
                header.checksum,
            });

            assert(self.header_for_entry(header).?.checksum == header.checksum);
            assert(self.headers[slot.index].checksum == header.checksum); // TODO Snapshots

            defer self.headers[slot.index] = Header.reserved(replica.cluster, slot.index);
            self.dirty.clear(slot);
            self.faulty.clear(slot);
        }

        /// Removes entries from `op_min` (inclusive) onwards.
        /// Used after a view change to remove uncommitted entries discarded by the new leader.
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

            // TODO At startup we need to handle removed entries that reappear.
            // This is because we do not call `write_headers_between()` here.
        }

        pub fn set_header_as_dirty(self: *Self, header: *const Header) void {
            log.debug("{}: set_header_as_dirty: op={} checksum={}", .{
                self.replica,
                header.op,
                header.checksum,
            });
            const slot = self.slot_for_header(header);

            if (self.has(header)) {
                assert(self.dirty.bit(slot));
                // Do not clear any faulty bit for the same entry.
            } else {
                self.headers[slot.index] = header.*;
                self.dirty.set(slot);
                self.faulty.clear(slot);
            }
        }

        /// `write_prepare` uses `write_sectors` to prevent concurrent disk writes.
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
            assert(self.has(message.header));

            // The underlying header memory must be owned by the buffer and not by self.headers:
            // Otherwise, concurrent writes may modify the memory of the pointer while we write.
            assert(@ptrToInt(message.header) == @ptrToInt(message.buffer.ptr));

            const slot = self.slot_with_header(message.header).?;

            if (!self.dirty.bit(slot)) {
                // Any function that sets the faulty bit should also set the dirty bit:
                assert(!self.faulty.bit(slot));
                assert(self.prepare_inhabited[slot.index]);
                assert(self.prepare_checksums[slot.index] == message.header.checksum);
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
            const buffer = message.buffer[0..vsr.sector_ceil(message.header.size)];
            const offset = offset_physical(.prepares, slot);

            if (builtin.mode == .Debug) {
                // Assert that any sector padding has already been zeroed:
                var sum_of_sector_padding_bytes: u8 = 0;
                for (buffer[message.header.size..]) |byte| sum_of_sector_padding_bytes |= byte;
                assert(sum_of_sector_padding_bytes == 0);
            }

            self.prepare_inhabited[slot.index] = false;
            self.prepare_checksums[slot.index] = 0;

            assert_bounds(.prepares, offset, buffer.len);
            self.write_sectors(write_prepare_header, write, buffer, offset);
        }

        /// Attempt to lock the in-memory sector containing the header being written.
        /// If the sector is already locked, add this write to the wait queue.
        fn write_prepare_header(write: *Self.Write) void {
            const self = write.self;
            const message = write.message;

            if (self.slot_with_op_and_checksum(message.header.op, message.header.checksum)) |slot| {
                assert(!self.prepare_inhabited[slot.index]);
                self.prepare_inhabited[slot.index] = true;
                self.prepare_checksums[slot.index] = message.header.checksum;
            } else {
                self.write_prepare_debug(message.header, "entry changed while writing sectors");
                self.write_prepare_release(write, null);
                return;
            }

            assert(!write.header_sector_locked);
            assert(write.header_sector_next == null);

            const write_offset = self.offset_logical_in_headers_for_message(message);

            var it = self.writes.iterate();
            while (it.next()) |other| {
                if (other == write) continue;
                if (!other.header_sector_locked) continue;

                const other_offset = self.offset_logical_in_headers_for_message(other.message);
                if (other_offset == write_offset) {
                    // The `other` and `write` target the same sector.
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

            const replica = @fieldParentPtr(Replica, "journal", self);
            const message = write.message;
            const slot_of_message = self.slot_for_header(message.header);
            const slot_first = Slot{
                .index = @divFloor(slot_of_message.index, headers_per_sector) * headers_per_sector,
            };

            const offset = offset_physical(.headers, slot_of_message);
            assert(offset % config.sector_size == 0);
            assert(offset == slot_first.index * @sizeOf(Header));

            const buffer: []u8 = write.header_sector(self);
            const buffer_headers = std.mem.bytesAsSlice(Header, buffer);
            assert(buffer_headers.len == headers_per_sector);

            var i: usize = 0;
            while (i < headers_per_sector) : (i += 1) {
                const slot = Slot{ .index = slot_first.index + i };

                if (self.faulty.bit(slot)) {
                    // Redundant faulty headers are deliberately written as invalid.
                    // This ensures that faulty headers are still faulty when they are read back
                    // from disk during recovery. This prevents faulty entries from changing to
                    // reserved (and clean) after a crash and restart (e.g. accidentally converting
                    // a case `@D` to a `@H` after a restart).
                    buffer_headers[i] = .{
                        .checksum = 0,
                        .cluster = replica.cluster,
                        .command = .reserved,
                    };
                    assert(!buffer_headers[i].valid_checksum());
                } else if (message.header.op < slot_count and
                    !self.prepare_inhabited[slot.index] and
                    message.header.command == .prepare and
                    self.dirty.bit(slot))
                {
                    // When:
                    // * this is the first wrap of the WAL, and
                    // * this prepare slot is not inhabited (never has been), and
                    // * this prepare slot is a dirty prepare,
                    // write a reserved header instead of the in-memory prepare header.
                    //
                    // This can be triggered by the follow sequence of events:
                    // 1. Ops 6 and 7 arrive.
                    // 2. The write of prepare 7 finishes (before prepare 6).
                    // 3. Op 7 continues on to write the redundant headers.
                    //    Because prepare 6 is not yet written, header 6 is written as reserved.
                    // 4. (If at this point the replica crashes & restarts, slot 6 is in case `@H`
                    //    (decision=nil) which can be locally repaired. In contrast, if op 6's
                    //    header was written in step 3, it would be case `@G`, which requires
                    //    remote repair.
                    //
                    // * When `replica_count=1`, case `@G`, is not recoverable.
                    // * When `replica_count>1` this marginally improves availability by enabling
                    //   local repair.
                    buffer_headers[i] = Header.reserved(replica.cluster, slot.index);
                } else {
                    buffer_headers[i] = self.headers[slot.index];
                }
            }

            log.debug("{}: write_header: op={} sectors[{}..{}]", .{
                self.replica,
                message.header.op,
                offset,
                offset + config.sector_size,
            });

            // Memory must not be owned by self.headers as these may be modified concurrently:
            assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
                @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + headers_size);

            assert_bounds(.headers, offset, buffer.len);
            self.write_sectors(write_prepare_on_write_header, write, buffer, offset);
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

            const slot = self.slot_with_header(message.header).?;
            self.dirty.clear(slot);
            self.faulty.clear(slot);

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

        fn write_prepare_debug(self: *const Self, header: *const Header, status: []const u8) void {
            log.debug("{}: write: view={} op={} len={}: {} {s}", .{
                self.replica,
                header.view,
                header.op,
                header.size,
                header.checksum,
                status,
            });
        }

        fn assert_bounds(ring: Ring, offset: u64, size: u64) void {
            switch (ring) {
                .headers => assert(offset + size <= headers_size),
                .prepares => {
                    assert(offset >= headers_size);
                    assert(offset + size <= headers_size + prepares_size);
                },
            }
        }

        fn offset_logical(ring: Ring, slot: Slot) u64 {
            assert(slot.index < slot_count);

            switch (ring) {
                .headers => {
                    comptime assert(config.sector_size % @sizeOf(Header) == 0);
                    const offset = vsr.sector_floor(slot.index * @sizeOf(Header));
                    assert(offset < headers_size);
                    return offset;
                },
                .prepares => {
                    const offset = config.message_size_max * slot.index;
                    assert(offset < prepares_size);
                    return offset;
                },
            }
        }

        fn offset_physical(ring: Ring, slot: Slot) u64 {
            return switch (ring) {
                .headers => offset_logical(.headers, slot),
                .prepares => headers_size + offset_logical(.prepares, slot),
            };
        }

        fn offset_logical_in_headers_for_message(self: *const Self, message: *Message) u64 {
            return offset_logical(.headers, self.slot_for_header(message.header));
        }

        /// Where `offset` is a logical offset relative to the start of the respective ring.
        fn offset_physical_for_logical(ring: Ring, offset: u64) u64 {
            switch (ring) {
                .headers => {
                    assert(offset < headers_size);
                    return offset;
                },
                .prepares => {
                    assert(offset < prepares_size);
                    return headers_size + offset;
                },
            }
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
            // We rely on the Storage.write_sectors() implementation being always synchronous,
            // in which case writes never actually need to be queued, or always asynchronous,
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

            // The callback may set range, so we can't set range to undefined after the callback.
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
                    assert(self.dirty.bit(self.slot_for_op(op)));
                    return true;
                }
            }
            return false;
        }
    };
}

// TODO Snapshots
pub const BitSet = struct {
    bits: std.DynamicBitSetUnmanaged,

    /// The number of bits set (updated incrementally as bits are set or cleared):
    count: u64 = 0,

    fn init(allocator: Allocator, count: usize) !BitSet {
        const bits = try std.DynamicBitSetUnmanaged.initEmpty(allocator, count);
        errdefer bits.deinit(allocator);

        return BitSet{ .bits = bits };
    }

    fn deinit(self: *BitSet, allocator: Allocator) void {
        self.bits.deinit(allocator);
    }

    /// Clear the bit for a slot (idempotent):
    pub fn clear(self: *BitSet, slot: Slot) void {
        if (self.bits.isSet(slot.index)) {
            self.bits.unset(slot.index);
            self.count -= 1;
        }
    }

    /// Whether the bit for a slot is set:
    pub fn bit(self: *const BitSet, slot: Slot) bool {
        return self.bits.isSet(slot.index);
    }

    /// Set the bit for a slot (idempotent):
    pub fn set(self: *BitSet, slot: Slot) void {
        if (!self.bits.isSet(slot.index)) {
            self.bits.set(slot.index);
            self.count += 1;
            assert(self.count <= self.bits.bit_length);
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

        /// Returns the count of IOPs available.
        pub fn available(self: *const Self) math.Log2IntCeil(Map) {
            return @popCount(Map, self.free);
        }

        /// Returns the count of IOPs in use.
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

test "IOPS" {
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

/// @B and @C:
/// This prepare header is corrupt.
/// We may have a valid redundant header, but need to recover the full message.
///
///
/// @F:
/// One of:
///
/// * A misdirected read to a reserved header.
/// * The replica is recovering from a crash after writing the prepare, but before writing the
///   redundant header.
/// * The redundant header's write was lost or misdirected.
///
/// The redundant header can be repaired using the prepare's header; it does not need to be
/// retrieved remotely.
///
///
/// @G:
/// The redundant header is present & valid, but the corresponding prepare was a lost or misdirected
/// read or write.
///
///
/// @H:
/// This slot is legitimately reserved — this may be the first fill of the log.
///
///
/// @I and @J:
/// When the redundant header & prepare header are both valid but distinct ops, always pick the
/// higher op.
///
/// For example, consider slot_count=10, the op to the left is 12, the op to the right is 14, and
/// the tiebreak is between an op=3 and op=13. Choosing op=13 over op=3 is safe because the op=3
/// must be from a previous wrap — it is too far back (>pipeline) to have been replaced by a view
/// change.
///
/// The length of the prepare pipeline is the upper bound on how many ops can be reordered during a
/// view change.
///
/// @I:
/// When the higher op belongs to the prepare, repair locally.
/// The most likely cause for this case is that the log wrapped, but the redundant header write was
/// lost.
///
/// @J:
/// When the higher op belongs to the header, mark faulty.
///
///
/// @K:
/// The message was rewritten due to a view change.
/// A single-replica cluster doesn't ever change views.
///
///
/// @L:
/// The redundant header matches the message's header.
/// This is the usual case: both the prepare and header are correct and equivalent.
const recovery_cases = table: {
    const __ = Matcher.any;
    const _0 = Matcher.is_false;
    const _1 = Matcher.is_true;
    // The replica will abort if any of these checks fail:
    const a0 = Matcher.assert_is_false;
    const a1 = Matcher.assert_is_true;

    break :table [_]Case{
        // Legend:
        //
        //    R>1  replica_count > 1
        //    R=1  replica_count = 1
        //     ok  valid checksum ∧ valid cluster ∧ valid slot ∧ valid command
        //    nil  command == reserved
        //     ✓∑  header.checksum == prepare.checksum
        //    op=  header.op == prepare.op
        //    op<  header.op <  prepare.op
        //   view  header.view == prepare.view
        //
        //        Label  Decision      Header  Prepare Compare
        //               R>1   R=1     ok  nil ok  nil ✓∑  op= op< view
        Case.init("@A", .vsr, .vsr, .{ _0, __, _0, __, __, __, __, __ }),
        Case.init("@B", .vsr, .vsr, .{ _1, _1, _0, __, __, __, __, __ }),
        Case.init("@C", .vsr, .vsr, .{ _1, _0, _0, __, __, __, __, __ }),
        Case.init("@D", .vsr, .vsr, .{ _0, __, _1, _1, __, __, __, __ }),
        Case.init("@E", .vsr, .fix, .{ _0, __, _1, _0, __, __, __, __ }),
        Case.init("@F", .vsr, .fix, .{ _1, _1, _1, _0, __, __, __, __ }),
        Case.init("@G", .vsr, .vsr, .{ _1, _0, _1, _1, __, __, __, __ }),
        Case.init("@H", .nil, .nil, .{ _1, _1, _1, _1, a1, a1, a0, a1 }), // normal path: reserved
        Case.init("@I", .fix, .fix, .{ _1, _0, _1, _0, _0, _0, _1, __ }), // header.op < prepare.op
        Case.init("@J", .vsr, .vsr, .{ _1, _0, _1, _0, _0, _0, _0, __ }), // header.op > prepare.op
        Case.init("@K", .vsr, .vsr, .{ _1, _0, _1, _0, _0, _1, a0, a0 }),
        Case.init("@L", .eql, .eql, .{ _1, _0, _1, _0, _1, a1, a0, a1 }), // normal path: prepare
    };
};

const RecoveryDecision = enum {
    /// The header and prepare are identical; no repair necessary.
    eql,
    /// Reserved; dirty/faulty are clear, no repair necessary.
    nil,
    /// If replica_count>1: Repair with VSR `request_prepare`. Mark dirty, clear faulty.
    /// If replica_count=1: Use intact prepare. Clear dirty, clear faulty.
    /// (Don't set faulty, because we have the valid message.)
    fix,
    /// If replica_count>1: Repair with VSR `request_prepare`. Mark dirty, mark faulty.
    /// If replica_count=1: Fail; cannot recover safely.
    vsr,
};

const Matcher = enum { any, is_false, is_true, assert_is_false, assert_is_true };

const Case = struct {
    label: []const u8,
    /// Decision when replica_count>1.
    decision_multiple: RecoveryDecision,
    /// Decision when replica_count=1.
    decision_single: RecoveryDecision,
    /// 0: header_ok(header)
    /// 1: header.command == reserved
    /// 2: header_ok(prepare)
    /// 3: prepare.command == reserved
    /// 4: header.checksum == prepare.checksum
    /// 5: header.op == prepare.op
    /// 6: header.op < prepare.op
    /// 7: header.view == prepare.view
    pattern: [8]Matcher,

    fn init(
        label: []const u8,
        decision_multiple: RecoveryDecision,
        decision_single: RecoveryDecision,
        pattern: [8]Matcher,
    ) Case {
        return .{
            .label = label,
            .decision_multiple = decision_multiple,
            .decision_single = decision_single,
            .pattern = pattern,
        };
    }

    fn check(self: *const Case, parameters: [8]bool) !bool {
        for (parameters) |b, i| {
            switch (self.pattern[i]) {
                .any => {},
                .is_false => if (b) return false,
                .is_true => if (!b) return false,
                .assert_is_false => if (b) return error.ExpectFalse,
                .assert_is_true => if (!b) return error.ExpectTrue,
            }
        }
        return true;
    }

    fn decision(self: *const Case, replica_count: u8) RecoveryDecision {
        assert(replica_count > 0);
        if (replica_count == 1) {
            return self.decision_single;
        } else {
            return self.decision_multiple;
        }
    }
};

fn recovery_case(
    cluster: u32,
    slot: Slot,
    header: *const Header,
    prepare_message: *const Message,
) *const Case {
    assert(slot.index < slot_count);

    const prepare: *const Header = prepare_message.header;
    const prepare_valid = header_ok(cluster, slot, prepare);
    const header_valid = header_ok(cluster, slot, header);
    if (prepare_valid) assert(prepare.invalid() == null);
    if (header_valid) assert(header.invalid() == null);

    const parameters = .{
        header_valid,
        header.command == .reserved,
        prepare_valid,
        prepare.command == .reserved,
        header.checksum == prepare.checksum,
        header.op == prepare.op,
        header.op < prepare.op,
        header.view == prepare.view,
    };

    var result: ?*const Case = null;
    for (recovery_cases) |*case| {
        const match = case.check(parameters) catch {
            log.err("recovery_case: impossible state: case={s} parameters={any}", .{
                case.label,
                parameters,
            });
            unreachable;
        };
        if (match) {
            assert(result == null);
            result = case;
        }
    }
    // The recovery table is exhaustive.
    // Every combination of parameters matches exactly one case.
    return result.?;
}

/// Returns whether the message header:
/// * has a valid checksum, and
/// * has the expected cluster, and
/// * has an expected command, and
/// * resides in the correct slot.
fn header_ok(cluster: u32, slot: Slot, header: *const Header) bool {
    // A header with the wrong cluster, or in the wrong slot, may indicate a misdirected read/write.
    //
    // All journalled headers should be reserved or else prepares.
    // A misdirected read/write to or from another storage zone may return the wrong message.
    const valid_command_and_slot = switch (header.command) {
        .prepare => slot.index == header.op % slot_count,
        .reserved => slot.index == header.op,
        else => false,
    };
    return header.valid_checksum() and header.cluster == cluster and valid_command_and_slot;
}

test "recovery_cases" {
    // Verify that every pattern matches exactly one case.
    //
    // Every possible combination of parameters must either:
    // * have a matching case
    // * have a case that fails (which would result in a panic).
    var i: usize = 0;
    while (i <= std.math.maxInt(u8)) : (i += 1) {
        var parameters: [8]bool = undefined;
        comptime var j: usize = 0;
        inline while (j < parameters.len) : (j += 1) {
            parameters[j] = i & (1 << j) != 0;
        }

        var case_match: ?*const Case = null;
        for (recovery_cases) |*case| {
            if (case.check(parameters) catch true) {
                try std.testing.expectEqual(case_match, null);
                case_match = case;
            }
        }
        if (case_match == null) @panic("no matching case");
    }
}
