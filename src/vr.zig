const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);
pub const log_level: std.log.Level = .debug;

const MessageBus = @import("message_bus.zig").MessageBus;
const Message = @import("message_bus.zig").Message;
usingnamespace @import("concurrent_ranges.zig");

// TODO Command for client to fetch its latest request number from the cluster.
/// Viewstamped Replication protocol commands:
pub const Command = packed enum(u8) {
    reserved,

    request,
    prepare,
    prepare_ok,
    reply,
    commit,

    start_view_change,
    do_view_change,
    start_view,

    request_headers,
    headers,
};

/// State machine operations:
pub const Operation = packed enum(u8) {
    reserved,
    init,
    noop,
};

/// Network message and journal entry header:
/// We reuse the same header for both so that prepare messages from the leader can simply be
/// journalled as is by the followers without requiring any further modification.
pub const Header = packed struct {
    /// A checksum covering only the rest of this header (but including checksum_data):
    /// This enables the header to be trusted without having to recv() or read() associated data.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum: u128 = 0,

    /// A checksum covering only associated data.
    checksum_data: u128 = 0,

    /// The checksum of the message to which this message refers, or a unique recovery nonce:
    /// We use this nonce in various ways, for example:
    /// * A prepare sets nonce to the checksum of the prior prepare to create a hash chain.
    /// * A prepare_ok sets nonce to the checksum of the prepare it wants to ack.
    /// * A commit sets nonce to the checksum of the latest committed op.
    /// This adds an additional cryptographic safety control beyond VR's op and commit numbers.
    nonce: u128 = 0,

    /// Each client records its own client id and a current request number. A client is allowed to
    /// have just one outstanding request at a time.
    client: u128 = 0,

    /// The cluster id binds intention into the header, so that a client or replica can indicate
    /// which cluster it thinks it's speaking to, instead of accidentally talking to the wrong
    /// cluster (for example, staging vs production).
    cluster: u128,

    /// Every message sent from one replica to another contains the sending replica's current view:
    view: u64,

    /// The op number:
    op: u64 = 0,

    /// The commit number:
    commit: u64 = 0,

    /// The journal offset to which this message relates:
    /// This enables direct access to a prepare, without requiring previous variable-length entries.
    /// While we use fixed-size data structures, a batch will contain a variable amount of them.
    offset: u64 = 0,

    /// The size of this message header and any associated data:
    /// This must be 0 for an empty header with command == .reserved.
    size: u32 = @sizeOf(Header),

    /// The cluster reconfiguration epoch number (for future use):
    epoch: u32 = 0,

    /// Each request is given a number by the client and later requests must have larger numbers
    /// than earlier ones. The request number is used by the replicas to avoid running requests more
    /// than once; it is also used by the client to discard duplicate responses to its requests.
    request: u32 = 0,

    /// The index of the replica in the cluster configuration array that sent this message:
    replica: u16 = 0,

    /// The VR protocol command for this message:
    command: Command,

    /// The state machine operation to apply:
    operation: Operation = .reserved,

    pub fn calculate_checksum(self: *const Header) u128 {
        // Reserved headers should be completely zeroed with a checksum also of 0:
        if (self.command == .reserved) {
            var sum: u128 = 0;
            for (std.mem.asBytes(self)) |byte| sum += byte;
            if (sum == 0) return 0;
        }

        const checksum_size = @sizeOf(@TypeOf(self.checksum));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(std.mem.asBytes(self)[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_data(self: *const Header, data: []const u8) u128 {
        // Reserved headers should be completely zeroed with a checksum_data also of 0:
        if (self.command == .reserved and self.size == 0 and data.len == 0) return 0;
        if (self.size == @sizeOf(Header) and data.len == 0) return 0;

        assert(self.size == @sizeOf(Header) + data.len);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_data));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(data[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    /// This must be called only after set_checksum_data() so that checksum_data is also covered:
    pub fn set_checksum(self: *Header) void {
        self.checksum = self.calculate_checksum();
    }

    pub fn set_checksum_data(self: *Header, data: []const u8) void {
        self.checksum_data = self.calculate_checksum_data(data);
    }

    pub fn valid_checksum(self: *const Header) bool {
        return self.checksum == self.calculate_checksum();
    }

    pub fn valid_checksum_data(self: *const Header, data: []const u8) bool {
        return self.checksum_data == self.calculate_checksum_data(data);
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum is valid, and expects that has already been done.
    pub fn bad(self: *const Header) ?[]const u8 {
        switch (self.command) {
            .reserved => if (self.size != 0) return "size != 0",
            else => if (self.size < @sizeOf(Header)) return "size < @sizeOf(Header)",
        }
        if (self.epoch != 0) return "epoch != 0";
        switch (self.command) {
            .reserved => {
                if (self.checksum != 0) return "checksum != 0";
                if (self.checksum_data != 0) return "checksum_data != 0";
                if (self.nonce != 0) return "nonce != 0";
                if (self.client != 0) return "client != 0";
                if (self.cluster != 0) return "cluster != 0";
                if (self.view != 0) return "view != 0";
                if (self.op != 0) return "op != 0";
                if (self.commit != 0) return "commit != 0";
                if (self.offset != 0) return "offset != 0";
                if (self.request != 0) return "request != 0";
                if (self.replica != 0) return "replica != 0";
                if (self.operation != .reserved) return "operation != .reserved";
            },
            .request => {
                if (self.nonce != 0) return "nonce != 0";
                if (self.client == 0) return "client == 0";
                if (self.cluster == 0) return "cluster == 0";
                if (self.view != 0) return "view != 0";
                if (self.op != 0) return "op != 0";
                if (self.commit != 0) return "commit != 0";
                if (self.offset != 0) return "offset != 0";
                if (self.request == 0) return "request == 0";
                if (self.replica != 0) return "replica != 0";
                if (self.operation == .reserved) return "operation == .reserved";
            },
            .prepare => {
                switch (self.operation) {
                    .reserved => return "operation == .reserved",
                    .init => {
                        if (self.checksum_data != 0) return "init: checksum_data != 0";
                        if (self.nonce != 0) return "init: nonce != 0";
                        if (self.client != 0) return "init: client != 0";
                        if (self.cluster == 0) return "init: cluster == 0";
                        if (self.view != 0) return "init: view != 0";
                        if (self.op != 0) return "init: op != 0";
                        if (self.commit != 0) return "init: commit != 0";
                        if (self.offset != 0) return "init: offset != 0";
                        if (self.size != @sizeOf(Header)) return "init: size != @sizeOf(Header)";
                        if (self.request != 0) return "init: request != 0";
                        if (self.replica != 0) return "init: replica != 0";
                    },
                    else => {
                        if (self.client == 0) return "client == 0";
                        if (self.cluster == 0) return "cluster == 0";
                        if (self.op == 0) return "op == 0";
                        if (self.op <= self.commit) return "op <= commit";
                        if (self.request == 0) return "request == 0";
                    },
                }
            },
            .prepare_ok => {
                switch (self.operation) {
                    .reserved => return "operation == .reserved",
                    .init => {
                        if (self.checksum_data != 0) return "init: checksum_data != 0";
                        if (self.nonce != 0) return "init: nonce != 0";
                        if (self.client != 0) return "init: client != 0";
                        if (self.cluster == 0) return "init: cluster == 0";
                        if (self.view != 0) return "init: view != 0";
                        if (self.op != 0) return "init: op != 0";
                        if (self.commit != 0) return "init: commit != 0";
                        if (self.offset != 0) return "init: offset != 0";
                        if (self.size != @sizeOf(Header)) return "init: size != @sizeOf(Header)";
                        if (self.request != 0) return "init: request != 0";
                        if (self.replica != 0) return "init: replica != 0";
                    },
                    else => {
                        if (self.checksum_data != 0) return "checksum_data != 0";
                        if (self.client == 0) return "client == 0";
                        if (self.cluster == 0) return "cluster == 0";
                        if (self.op == 0) return "op == 0";
                        if (self.op <= self.commit) return "op <= commit";
                        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
                        if (self.request == 0) return "request == 0";
                    },
                }
            },
            else => {}, // TODO Add validators for all commands.
        }
        return null;
    }

    pub fn zero(self: *Header) void {
        std.mem.set(u8, std.mem.asBytes(self), 0);

        assert(self.checksum == 0);
        assert(self.checksum_data == 0);
        assert(self.size == 0);
        assert(self.command == .reserved);
        assert(self.operation == .reserved);
    }
};

const HeaderRange = struct { op_min: u64, op_max: u64 };

const Client = struct {};

// TODO Client table should warn if the client's request number has wrapped past 32 bits.
// This is easy to detect.
// If a client has done a few billion requests, we don't expect to see request 0 come through.
const ClientTable = struct {};

pub const ConfigurationAddress = union(enum) {
    address: std.net.Address,
    replica: *Replica,
};

pub const sector_size = 4096;

pub const Journal = struct {
    allocator: *Allocator,
    replica: u16,
    size: u64,
    size_headers: u64,
    size_circular_buffer: u64,
    headers: []Header align(sector_size),
    dirty: []bool,

    /// We copy-on-write to this buffer when writing, as in-memory headers may change concurrently:
    write_headers_buffer: []u8 align(sector_size),

    /// Apart from the header written with the entry, we also store two redundant copies of each
    /// header at different locations on disk, and we alternate between these for each append.
    /// This tracks which version (0 or 1) should be written to next:
    write_headers_version: u1 = 0,

    /// These serialize concurrent writes but only for overlapping ranges:
    writing_headers: ConcurrentRanges = .{ .name = "write_headers" },
    writing_sectors: ConcurrentRanges = .{ .name = "write_sectors" },

    pub fn init(allocator: *Allocator, replica: u16, size: u64, headers_count: u32) !Journal {
        if (@mod(size, sector_size) != 0) return error.SizeMustBeAMultipleOfSectorSize;
        if (!std.math.isPowerOfTwo(headers_count)) return error.HeadersCountMustBeAPowerOfTwo;

        const headers_per_sector = @divExact(sector_size, @sizeOf(Header));
        assert(headers_per_sector > 0);
        assert(headers_count >= headers_per_sector);

        var headers = try allocator.allocAdvanced(Header, sector_size, headers_count, .exact);
        errdefer allocator.free(headers);
        for (headers) |*header| header.zero();

        var dirty = try allocator.alloc(bool, headers.len);
        errdefer allocator.free(dirty);
        std.mem.set(bool, dirty, false);

        var write_headers_buffer = try allocator.allocAdvanced(
            u8,
            sector_size,
            @sizeOf(Header) * headers.len,
            .exact,
        );
        errdefer allocator.free(write_headers_buffer);
        std.mem.set(u8, write_headers_buffer, 0);

        const header_copies = 2;
        const size_headers = headers.len * @sizeOf(Header);
        const size_headers_copies = size_headers * header_copies;
        if (size_headers_copies >= size) return error.SizeTooSmallForHeadersCount;

        const size_circular_buffer = size - size_headers_copies;
        if (size_circular_buffer < 64 * 1024 * 1024) return error.SizeTooSmallForCircularBuffer;

        log.debug("{}: journal: size={} headers_len={} headers={} circular_buffer={}", .{
            replica,
            std.fmt.fmtIntSizeBin(size),
            headers.len,
            std.fmt.fmtIntSizeBin(size_headers),
            std.fmt.fmtIntSizeBin(size_circular_buffer),
        });

        var self = Journal{
            .allocator = allocator,
            .replica = replica,
            .size = size,
            .size_headers = size_headers,
            .size_circular_buffer = size_circular_buffer,
            .headers = headers,
            .dirty = dirty,
            .write_headers_buffer = write_headers_buffer,
        };

        assert(@mod(self.size_circular_buffer, sector_size) == 0);
        assert(@mod(@ptrToInt(&self.headers[0]), sector_size) == 0);
        assert(self.dirty.len == self.headers.len);
        assert(self.write_headers_buffer.len == @sizeOf(Header) * self.headers.len);

        return self;
    }

    pub fn deinit(self: *Journal) void {}

    /// Asserts that headers are .reserved (zeroed) from `op_min` (inclusive).
    pub fn assert_headers_reserved_from(self: *Journal, op_min: u64) void {
        // TODO Snapshots
        for (self.headers[op_min..]) |header| assert(header.command == .reserved);
    }

    /// Returns any existing entry at the location indicated by header.op.
    /// This existing entry may have an older or newer op number.
    pub fn entry(self: *Journal, header: *const Header) ?*const Header {
        assert(header.command == .prepare);
        return self.entry_for_op(header.op);
    }

    /// We use the op number directly to index into the headers array and locate ops without a scan.
    /// Op numbers cycle through the headers array and do not wrap when offsets wrap. The reason for
    /// this is to prevent variable offsets from impacting the location of an op. Otherwise, the
    /// same op number but for different views could exist at multiple locations in the journal.
    pub fn entry_for_op(self: *Journal, op: u64) ?*const Header {
        // TODO Snapshots
        const existing = &self.headers[op];
        if (existing.command == .reserved) return null;
        assert(existing.command == .prepare);
        return existing;
    }

    /// Returns the entry at `@mod(op)` location, but only if `entry.op == op`, else `null`.
    /// Be careful of using this without checking if the existing op has a newer viewstamp.
    pub fn entry_for_op_exact(self: *Journal, op: u64) ?*const Header {
        if (self.entry_for_op(op)) |existing| {
            if (existing.op == op) return existing;
        }
        return null;
    }

    pub fn previous_entry(self: *Journal, header: *const Header) ?*const Header {
        // TODO Snapshots
        if (header.op == 0) return null;
        return self.entry_for_op(header.op - 1);
    }

    pub fn next_entry(self: *Journal, header: *const Header) ?*const Header {
        // TODO Snapshots
        if (header.op + 1 == self.headers.len) return null;
        return self.entry_for_op(header.op + 1);
    }

    pub fn next_offset(self: *Journal, header: *const Header) u64 {
        // TODO Snapshots
        assert(header.command == .prepare);
        return header.offset + Journal.sector_ceil(header.size);
    }

    pub fn has(self: *Journal, header: *const Header) bool {
        // TODO Snapshots
        const existing = &self.headers[header.op];
        if (existing.command == .reserved) {
            assert(existing.checksum == 0);
            assert(existing.checksum_data == 0);
            assert(existing.offset == 0);
            assert(existing.size == 0);
            return false;
        } else {
            if (existing.checksum == header.checksum) {
                assert(existing.checksum_data == header.checksum_data);
                assert(existing.op == header.op);
                return true;
            } else {
                return false;
            }
        }
    }

    pub fn has_clean(self: *Journal, header: *const Header) bool {
        // TODO Snapshots
        return self.has(header) and !self.dirty[header.op];
    }

    pub fn has_dirty(self: *Journal, header: *const Header) bool {
        // TODO Snapshots
        return self.has(header) and self.dirty[header.op];
    }

    /// Copies latest headers between `op_min` and `op_max` (both inclusive) as will fit in `dest`.
    /// Reverses the order when copying so that latest headers are copied first, which also protects
    /// against the callsite slicing the buffer the wrong way and incorrectly.
    /// Skips .reserved headers (gaps between headers).
    /// Zeroes the `dest` buffer in case the copy would underflow and leave a buffer bleed.
    /// Returns the number of headers actually copied.
    pub fn copy_latest_headers_between(
        self: *Journal,
        op_min: u64,
        op_max: u64,
        dest: []Header,
    ) usize {
        assert(op_min <= op_max);
        assert(dest.len > 0);

        var copied: usize = 0;
        for (dest) |*header| header.zero();

        // We start at op_max + 1 but front-load the decrement to avoid overflow when op_min == 0:
        var op = op_max + 1;
        while (op > op_min) {
            op -= 1;

            if (self.entry_for_op_exact(op)) |header| {
                dest[copied] = header.*;
                copied += 1;
            }
        }
        return copied;
    }

    /// Finds the latest break in headers, searching between `op_min` and `op_max` (both inclusive).
    /// A break is a missing header or a header not connected to the next header (by hash chain).
    /// Upon finding the highest break, extends the range downwards to cover as much as possible.
    ///
    /// For example: If ops 3, 9 and 10 are missing, returns: `{ .op_min = 9, .op_max = 10 }`.
    ///
    /// Another example: If op 17 is disconnected from op 18, 16 is connected to 17, and 12-15 are
    /// missing, returns: `{ .op_min = 12, .op_max = 17 }`.
    pub fn find_latest_headers_break_between(self: *Journal, op_min: u64, op_max: u64) ?HeaderRange {
        assert(op_min <= op_max);
        var range: ?HeaderRange = null;

        // We set B, the op after op_max, to null because we only examine breaks <= op_max:
        // In other words, we may report a missing header for op_max itself but not a broken chain.
        var B: ?*const Header = null;

        var op = op_max + 1;
        while (op > op_min) {
            op -= 1;

            // Get the entry at @mod(op) location, but only if entry.op == op, else null:
            var A = self.entry_for_op_exact(op);
            if (A) |a| {
                if (B) |b| {
                    // If A was reordered then A may have a newer op than B (but an older view).
                    // However, here we use entry_for_op_exact() so we can assert a.op + 1 == b.op:
                    assert(a.op + 1 == b.op);
                    // Further, while repair_headers() should never put an older view to the right
                    // of a newer view, it may put a newer view to the left of an older view.
                    // We therefore do not assert a.view <= b.view unless the hash chain is intact.

                    // A exists and B exists:
                    if (range) |*r| {
                        assert(b.op == r.op_min);
                        if (a.checksum == b.nonce) {
                            // A is connected to B, but B is disconnected, add A to range:
                            assert(a.view <= b.view);
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
                    } else if (a.checksum == b.nonce) {
                        // A is connected to B, and B is connected or B is op_max.
                        assert(a.view <= b.view);
                    } else if (a.view < b.view) {
                        // A is not connected to B, and A is older than B, open range:
                        range = .{ .op_min = a.op, .op_max = a.op };
                    } else if (a.view > b.view) {
                        // A is not connected to B, but A is newer than B, open and close range:
                        range = .{ .op_min = b.op, .op_max = b.op };
                        break;
                    } else {
                        // Op numbers in the same view must be connected.
                        unreachable;
                    }
                } else {
                    // A exists and B does not exist (or B has a lower op number):
                    if (range) |r| {
                        // We therefore cannot compare A to B, A may be older/newer, close range:
                        assert(r.op_min == op + 1);
                        break;
                    } else {
                        // We expect a range if B does not exist, unless:
                        assert(a.op == op_max);
                    }
                }
            } else {
                // A does not exist (or A has a lower op number):
                if (self.entry_for_op(op)) |wrapped_a| assert(wrapped_a.op < op);

                if (range) |*r| {
                    // Add A to range:
                    assert(r.op_min == op + 1);
                    r.op_min = op;
                } else {
                    // Open range:
                    assert(B != null or op == op_max);
                    range = .{ .op_min = op, .op_max = op };
                }
            }

            B = A;
        }

        return range;
    }

    /// A safe way of removing an entry, where the header must match the current entry to succeed.
    fn remove_entry(self: *Journal, header: *const Header) void {
        // TODO Add equality method to Header to do more comparisons:
        assert(self.entry(header).?.checksum == header.checksum);
        // TODO Snapshots
        assert(self.headers[header.op].checksum == header.checksum);
        self.headers[header.op].zero();
        self.dirty[header.op] = true;
    }

    /// Removes entries from `op_min` (inclusive) onwards.
    /// This is used after a view change to remove uncommitted entries discarded by the new leader.
    pub fn remove_entries_from(self: *Journal, op_min: u64) void {
        // TODO Snapshots
        // TODO Optimize to jump directly to op:
        for (self.headers) |*header| {
            if (header.op >= op_min and header.command == .prepare) {
                self.remove_entry(header);
            }
        }
        self.assert_headers_reserved_from(op_min);
        // TODO Be more precise and efficient:
        // TODO See if we can solve this rather at startup to avoid the need for a blocking write.
        self.write_headers_between(0, self.headers.len - 1);
    }

    pub fn set_entry_as_dirty(self: *Journal, header: *const Header) void {
        log.debug("{}: journal: set_entry_as_dirty: {}", .{ self.replica, header.checksum });
        if (self.entry(header)) |existing| {
            if (existing.checksum != header.checksum) {
                assert(existing.view <= header.view);
                assert(existing.op < header.op or existing.view < header.view);
            }
        }
        self.headers[header.op] = header.*;
        self.dirty[header.op] = true;
    }

    pub fn write(self: *Journal, header: *const Header, buffer: []const u8) void {
        assert(header.command == .prepare);
        assert(header.size >= @sizeOf(Header));
        assert(header.size <= buffer.len);
        assert(buffer.len == Journal.sector_ceil(header.size));
        assert(header.offset + buffer.len <= self.size_circular_buffer);

        // The underlying header memory must be owned by the buffer and not by self.headers:
        // Otherwise, concurrent writes may modify the memory of the pointer while we write.
        assert(@ptrToInt(header) == @ptrToInt(buffer.ptr));

        // There should be no concurrency between setting an entry as dirty and deciding to write:
        assert(self.has_dirty(header));

        self.write_debug(header, "starting");

        self.write_sectors(buffer, self.offset_in_circular_buffer(header.offset));
        if (!self.has(header)) {
            self.write_debug(header, "entry changed while writing sectors");
            return;
        }

        // TODO Snapshots
        self.write_headers_between(header.op, header.op);
        if (!self.has(header)) {
            self.write_debug(header, "entry changed while writing headers");
            return;
        }

        self.write_debug(header, "complete, marking clean");
        // TODO Snapshots
        self.dirty[header.op] = false;
    }

    fn write_debug(self: *Journal, header: *const Header, status: []const u8) void {
        log.debug("{}: journal: write: view={} op={} offset={} len={}: {} {s}", .{
            self.replica,
            header.view,
            header.op,
            header.offset,
            header.size,
            header.checksum,
            status,
        });
    }

    fn offset_in_circular_buffer(self: *Journal, offset: u64) u64 {
        assert(offset < self.size_circular_buffer);
        return self.size_headers + offset;
    }

    fn offset_in_headers_version(self: *Journal, offset: u64, version: u1) u64 {
        assert(offset < self.size_headers);
        return switch (version) {
            0 => offset,
            1 => self.size_headers + self.size_circular_buffer + offset,
        };
    }

    /// Writes headers between `op_min` and `op_max` (both inclusive).
    fn write_headers_between(self: *Journal, op_min: u64, op_max: u64) void {
        // TODO Snapshots
        assert(op_min <= op_max);

        const offset = Journal.sector_floor(op_min * @sizeOf(Header));
        const len = Journal.sector_ceil((op_max - op_min + 1) * @sizeOf(Header));
        assert(len > 0);

        // We must acquire the concurrent range using the sector offset and len:
        // Different headers may share the same sector without any op_min or op_max overlap.
        // TODO Use a callback to acquire the range instead of suspend/resume:
        var range = Range{ .offset = offset, .len = len };
        self.writing_headers.acquire(&range);
        defer self.writing_headers.release(&range);

        const source = std.mem.sliceAsBytes(self.headers)[offset .. offset + len];
        var slice = self.write_headers_buffer[offset .. offset + len];
        assert(slice.len == source.len);
        assert(slice.len == len);
        std.mem.copy(u8, slice, source);

        log.debug("{}: journal: write_headers: op_min={} op_max={} sectors[{}..{}]", .{
            self.replica,
            op_min,
            op_max,
            offset,
            offset + len,
        });

        // Versions must be incremented upfront:
        // write_headers_to_version() will block while other calls may proceed concurrently.
        // If we don't increment upfront we could end up writing to the same copy twice.
        // We would then lose the redundancy required to locate headers or overwrite all copies.
        // TODO Snapshots
        if (self.write_headers_once(self.headers[op_min .. op_max + 1])) {
            const version_a = self.write_headers_increment_version();
            self.write_headers_to_version(version_a, slice, offset);
        } else {
            const version_a = self.write_headers_increment_version();
            const version_b = self.write_headers_increment_version();
            self.write_headers_to_version(version_a, slice, offset);
            self.write_headers_to_version(version_b, slice, offset);
        }
    }

    fn write_headers_increment_version(self: *Journal) u1 {
        self.write_headers_version +%= 1;
        return self.write_headers_version;
    }

    /// Since we allow gaps in the journal, we may have to write our headers twice.
    /// If a dirty header is being written as reserved (empty) then write twice to make this clear.
    /// If a dirty header has no previous clean chained entry to give its offset then write twice.
    /// Otherwise, we only need to write the headers once because their other copy can be located in
    /// the body of the journal (using the previous entry's offset and size).
    fn write_headers_once(self: *Journal, headers: []const Header) bool {
        for (headers) |*header| {
            // TODO Snapshots
            // We must use header.op and not the loop index as we are working from a slice:
            if (!self.dirty[header.op]) continue;
            if (header.command == .reserved) {
                log.debug("{}: journal: write_headers_once: dirty reserved header", .{
                    self.replica,
                });
                return false;
            }
            if (self.previous_entry(header)) |previous| {
                assert(previous.command == .prepare);
                if (previous.checksum != header.nonce) {
                    log.debug("{}: journal: write_headers_once: no hash chain", .{
                        self.replica,
                    });
                    return false;
                }
                // TODO Add is_dirty(header)
                // TODO Snapshots
                if (self.dirty[previous.op]) {
                    log.debug("{}: journal: write_headers_once: previous entry is dirty", .{
                        self.replica,
                    });
                    return false;
                }
            } else {
                log.debug("{}: journal: write_headers_once: no previous entry", .{
                    self.replica,
                });
                return false;
            }
        }
        return true;
    }

    fn write_headers_to_version(self: *Journal, version: u1, buffer: []const u8, offset: u64) void {
        log.debug("{}: journal: write_headers_to_version: version={} offset={} len={}", .{
            self.replica,
            version,
            offset,
            buffer.len,
        });
        assert(offset + buffer.len <= self.size_headers);
        self.write_sectors(buffer, self.offset_in_headers_version(offset, version));
    }

    fn write_sectors(self: *Journal, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= self.size);

        assert(@mod(@ptrToInt(buffer.ptr), sector_size) == 0);
        assert(@mod(buffer.len, sector_size) == 0);
        assert(@mod(offset, sector_size) == 0);

        // Memory must not be owned by self.headers as self.headers may be modified concurrently:
        assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
            @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

        var range = Range{ .offset = offset, .len = buffer.len };
        self.writing_sectors.acquire(&range);
        defer self.writing_sectors.release(&range);

        log.debug("{}: journal: write_sectors: offset={} len={}", .{
            self.replica,
            offset,
            buffer.len,
        });

        // TODO Write to underlying storage abstraction layer.
        // pwriteAll(buffer, offset) catch |err| switch (err) {
        //     error.InputOutput => @panic("latent sector error: no spare sectors to reallocate"),
        //     else => {
        //         log.emerg("write: error={} buffer.len={} offset={}", .{ err, buffer.len, offset });
        //         @panic("unrecoverable disk error");
        //     },
        // };
    }

    fn sector_floor(offset: u64) u64 {
        const sectors = std.math.divFloor(u64, offset, sector_size) catch unreachable;
        return sectors * sector_size;
    }

    fn sector_ceil(offset: u64) u64 {
        const sectors = std.math.divCeil(u64, offset, sector_size) catch unreachable;
        return sectors * sector_size;
    }
};

pub const StateMachine = struct {
    allocator: *Allocator,

    pub fn init(allocator: *Allocator) !StateMachine {
        var self = StateMachine{
            .allocator = allocator,
        };

        return self;
    }

    pub fn deinit(self: *StateMachine) void {}
};

const Status = enum {
    normal,
    view_change,
    recovering,
};

const Timeout = struct {
    name: []const u8,
    replica: u16,
    after: u64,
    ticks: u64 = 0,
    ticking: bool = false,

    /// It's important to check that when fired() is acted on that the timeout is stopped/started,
    /// otherwise further ticks around the event loop may trigger a thundering herd of messages.
    pub fn fired(self: *Timeout) bool {
        if (self.ticking and self.ticks >= self.after) {
            log.debug("{}: {s} fired", .{ self.replica, self.name });
            if (self.ticks > self.after) {
                log.emerg("{}: {s} is firing every tick", .{ self.replica, self.name });
                @panic("timeout was not reset correctly");
            }
            return true;
        } else {
            return false;
        }
    }

    pub fn reset(self: *Timeout) void {
        assert(self.ticking);
        self.ticks = 0;
        log.debug("{}: {s} reset", .{ self.replica, self.name });
    }

    pub fn start(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = true;
        log.debug("{}: {s} started", .{ self.replica, self.name });
    }

    pub fn stop(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = false;
        log.debug("{}: {s} stopped", .{ self.replica, self.name });
    }

    pub fn tick(self: *Timeout) void {
        if (self.ticking) self.ticks += 1;
    }
};

pub const Replica = struct {
    allocator: *Allocator,

    /// The id of the cluster to which this replica belongs:
    cluster: u128,

    /// The maximum number of replicas that may be faulty:
    f: u32,

    /// An array containing the remote or local addresses of each of the 2f + 1 replicas:
    /// Unlike the VRR paper, we do not sort the array but leave the order explicitly to the user.
    /// There are several advantages to this:
    /// * The operator may deploy a cluster with proximity in mind since replication follows order.
    /// * A replica's IP address may be changed without reconfiguration.
    /// This does require that the user specify the same order to all replicas.
    configuration: []ConfigurationAddress,

    /// An abstraction to send messages from the replica to itself or another replica or client.
    /// The recipient replica or client may be a local in-memory pointer or network-addressable.
    /// The message bus will also deliver messages to this replica by calling Replica.on_message().
    message_bus: *MessageBus,

    /// The persistent log of hash-chained journal entries:
    journal: *Journal,

    /// For executing service up-calls after an operation has been committed:
    state_machine: *StateMachine,

    /// The index into the configuration where this replica's IP address is stored:
    replica: u16,

    /// The current view, initially 0:
    view: u64,

    /// The current status, either normal, view_change, or recovering:
    /// TODO Don't default to normal, set the starting status according to the journal's health.
    status: Status = .normal,

    /// The op number assigned to the most recently prepared operation:
    op: u64,

    /// The op number of the latest committed and executed operation (according to the replica):
    /// The replica may have to wait for repairs to complete before commit_min reaches commit_max.
    commit_min: u64,

    /// The op number of the latest committed operation (according to the cluster):
    /// This is the commit number in terms of the VRR paper.
    commit_max: u64,

    /// The current request's checksum (used for now to enforce one-at-a-time request processing):
    request_checksum: ?u128 = null,

    /// The current prepare message (used to cross-check prepare_ok messages, and for resending):
    prepare_message: ?*Message = null,
    prepare_attempt: u64 = 0,

    appending: bool = false,
    appending_frame: @Frame(write_to_journal) = undefined,

    repairing: bool = false,
    repairing_frame: @Frame(write_to_journal) = undefined,

    repair_queue: ?*Message = null,
    repair_queue_len: usize = 0,
    repair_queue_max: usize = 3,

    /// Unique prepare_ok messages for the same view, op number and checksum from ALL replicas:
    prepare_ok_from_all_replicas: []?*Message,

    /// Unique start_view_change messages for the same view from OTHER replicas (excluding ourself):
    start_view_change_from_other_replicas: []?*Message,

    /// Unique do_view_change messages for the same view from ALL replicas (including ourself):
    do_view_change_from_all_replicas: []?*Message,

    /// The number of ticks without enough prepare_ok's before the leader resends a prepare:
    /// TODO Adjust this dynamically to match sliding window EWMA of recent network latencies.
    prepare_timeout: Timeout,

    /// The number of ticks before the leader sends a commit heartbeat:
    /// The leader always sends a commit heartbeat irrespective of when it last sent a prepare.
    /// This improves liveness when prepare messages cannot be replicated fully due to partitions.
    commit_timeout: Timeout,

    /// The number of ticks without hearing from the leader before a follower starts a view change:
    /// This transitions from .normal status to .view_change status.
    normal_timeout: Timeout,

    /// The number of ticks before a view change is timed out:
    /// This transitions from .view_change status to .view_change status but for a higher view.
    view_change_timeout: Timeout,

    /// The number of ticks before repairing missing/disconnected headers and/or dirty data:
    repair_timeout: Timeout,

    // TODO Limit integer types for `f` and `replica` to match their upper bounds in practice.
    pub fn init(
        allocator: *Allocator,
        cluster: u128,
        f: u32,
        configuration: []ConfigurationAddress,
        message_bus: *MessageBus,
        journal: *Journal,
        state_machine: *StateMachine,
        replica: u16,
    ) !Replica {
        assert(cluster > 0);
        assert(configuration.len > 0);
        assert(configuration.len > f);
        assert(replica < configuration.len);

        var prepare_ok = try allocator.alloc(?*Message, configuration.len);
        for (prepare_ok) |*received| received.* = null;
        errdefer allocator.free(prepare_ok);

        var start_view_change = try allocator.alloc(?*Message, configuration.len);
        for (start_view_change) |*received| received.* = null;
        errdefer allocator.free(start_view_change);

        var do_view_change = try allocator.alloc(?*Message, configuration.len);
        for (do_view_change) |*received| received.* = null;
        errdefer allocator.free(do_view_change);

        // TODO Only initialize the journal when initializing the cluster:
        var init_prepare = Header{
            .checksum_data = 0,
            .nonce = 0,
            .client = 0,
            .cluster = cluster,
            .view = 0,
            .op = 0,
            .commit = 0,
            .offset = 0,
            .size = @sizeOf(Header),
            .epoch = 0,
            .request = 0,
            .replica = 0,
            .command = .prepare,
            .operation = .init,
        };
        init_prepare.set_checksum();
        assert(init_prepare.valid_checksum());
        assert(init_prepare.bad() == null);
        journal.headers[0] = init_prepare;
        journal.assert_headers_reserved_from(init_prepare.op + 1);

        var self = Replica{
            .allocator = allocator,
            .cluster = cluster,
            .f = f,
            .configuration = configuration,
            .replica = replica,
            .view = init_prepare.view,
            .op = init_prepare.op,
            .commit_min = init_prepare.commit,
            .commit_max = init_prepare.commit,
            .message_bus = message_bus,
            .journal = journal,
            .state_machine = state_machine,
            .prepare_ok_from_all_replicas = prepare_ok,
            .start_view_change_from_other_replicas = start_view_change,
            .do_view_change_from_all_replicas = do_view_change,
            .prepare_timeout = Timeout{
                .name = "prepare_timeout",
                .replica = replica,
                .after = 10,
            },
            .commit_timeout = Timeout{
                .name = "commit_timeout",
                .replica = replica,
                .after = 30,
            },
            .normal_timeout = Timeout{
                .name = "normal_timeout",
                .replica = replica,
                .after = 500,
            },
            .view_change_timeout = Timeout{
                .name = "view_change_timeout",
                .replica = replica,
                .after = 6000,
            },
            .repair_timeout = Timeout{
                .name = "repair_timeout",
                .replica = replica,
                .after = 3, // TODO
            },
        };

        // We must initialize timeouts here, not in tick() on the first tick, because on_message()
        // can race with tick()... before timeouts have been initialized:
        assert(self.status == .normal);
        if (self.leader()) {
            log.debug("{}: init: leader", .{self.replica});
            self.commit_timeout.start();
            self.repair_timeout.start();
        } else {
            log.debug("{}: init: follower", .{self.replica});
            self.normal_timeout.start();
            self.repair_timeout.start();
        }

        return self;
    }

    pub fn deinit(self: *Replica) void {
        self.allocator.free(self.prepare_ok_from_all_replicas);
        self.allocator.free(self.start_view_change_from_other_replicas);
        self.allocator.free(self.do_view_change_from_all_replicas);
    }

    /// Returns whether the replica is a follower for the current view.
    /// This may be used only when the replica status is normal.
    pub fn follower(self: *Replica) bool {
        return !self.leader();
    }

    /// Returns whether the replica is the leader for the current view.
    /// This may be used only when the replica status is normal.
    pub fn leader(self: *Replica) bool {
        assert(self.status == .normal);
        return self.leader_index(self.view) == self.replica;
    }

    /// Returns the index into the configuration of the leader for a given view.
    pub fn leader_index(self: *Replica, view: u64) u16 {
        return @intCast(u16, @mod(view, self.configuration.len));
    }

    /// Time is measured in logical ticks that are incremented on every call to tick().
    /// This eliminates a dependency on the system time and enables deterministic testing.
    pub fn tick(self: *Replica) void {
        self.prepare_timeout.tick();
        self.commit_timeout.tick();
        self.normal_timeout.tick();
        self.view_change_timeout.tick();
        self.repair_timeout.tick();

        if (self.prepare_timeout.fired()) self.on_prepare_timeout();
        if (self.commit_timeout.fired()) self.on_commit_timeout();
        if (self.normal_timeout.fired()) self.on_normal_timeout();
        if (self.view_change_timeout.fired()) self.on_view_change_timeout();
        if (self.repair_timeout.fired()) self.on_repair_timeout();

        self.repair_last_queued_message_if_any();
    }

    /// Called by the MessageBus to deliver a message to the replica.
    pub fn on_message(self: *Replica, message: *Message) void {
        log.debug("{}: on_message: view={} status={s} {}", .{
            self.replica,
            self.view,
            @tagName(self.status),
            message.header,
        });
        if (message.header.bad()) |reason| {
            log.debug("{}: on_message: bad ({s})", .{ self.replica, reason });
            return;
        }
        if (message.header.cluster != self.cluster) {
            log.warn("{}: on_message: wrong cluster (message.header.cluster={} instead of {})", .{
                self.replica,
                message.header.cluster,
                self.cluster,
            });
            return;
        }
        assert(message.header.replica < self.configuration.len);
        switch (message.header.command) {
            .request => self.on_request(message),
            .prepare => self.on_prepare(message),
            .prepare_ok => self.on_prepare_ok(message),
            .commit => self.on_commit(message),
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            .request_headers => self.on_request_headers(message),
            .headers => self.on_headers(message),
            else => unreachable,
        }
    }

    fn on_request(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_request: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (self.follower()) {
            // TODO Add an optimization to tell the client the view (and leader) ahead of a timeout.
            // This would prevent thundering herds where clients suddenly broadcast the cluster.
            log.debug("{}: on_request: ignoring (follower)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(self.leader());

        // TODO Check the client table to see if this is a duplicate request and reply if so.
        // TODO If request is pending then this will also reflect in client table and we can ignore.
        // TODO Add client information to client table.

        if (self.request_checksum) |request_checksum| {
            assert(message.header.command == .request);
            if (message.header.checksum == request_checksum) {
                log.debug("{}: on_request: ignoring (already preparing)", .{self.replica});
                return;
            }
        }

        // TODO Queue (or drop client requests after a limit) to handle one request at a time:
        // TODO Clear this queue if we lose our leadership (critical for correctness).
        assert(self.commit_min == self.commit_max and self.commit_max == self.op);
        assert(self.request_checksum == null);
        self.request_checksum = message.header.checksum;

        log.debug("{}: on_request: request {}", .{ self.replica, message.header.checksum });

        // The primary advances op-number, adds the request to the end of the log, and updates the
        // information for this client in the client-table to contain the new request number, s.
        // Then it sends a ⟨PREPARE v, m, n, k⟩ message to the other replicas, where v is the
        // current view-number, m is the message it received from the client, n is the op-number it
        // assigned to the request, and k is the commit-number.

        var data = message.buffer[@sizeOf(Header)..message.header.size];
        // TODO Assign timestamps to structs in associated data.

        var latest_entry = self.journal.entry_for_op_exact(self.op).?;

        message.header.nonce = latest_entry.checksum;
        message.header.view = self.view;
        message.header.op = self.op + 1;
        message.header.commit = self.commit_max;
        message.header.offset = self.journal.next_offset(latest_entry);
        message.header.replica = self.replica;
        message.header.command = .prepare;

        message.header.set_checksum_data(data);
        message.header.set_checksum();

        assert(message.header.checksum != self.request_checksum.?);

        log.debug("{}: on_request: prepare {}", .{ self.replica, message.header.checksum });

        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
        assert(self.prepare_timeout.ticking == false);

        message.references += 1;
        self.prepare_message = message;
        self.prepare_attempt = 0;
        self.prepare_timeout.start();

        // Use the same replication code path for the leader and followers:
        self.send_message_to_replica(self.replica, message);
    }

    /// Replication is simple, with a single code path for the leader and followers:
    ///
    /// The leader starts by sending a prepare message to itself.
    ///
    /// Each replica (including the leader) then forwards this prepare message to the next replica
    /// in the configuration, in parallel to writing to its own journal, closing the circle until
    /// the next replica is back to the leader, in which case the replica does not forward.
    ///
    /// This keeps the leader's outgoing bandwidth limited (one-for-one) to incoming bandwidth,
    /// since the leader need only replicate to the next replica. Otherwise, the leader would need
    /// to replicate to multiple followers, dividing available bandwidth.
    ///
    /// This does not impact latency, since with Flexible Paxos we need only one remote prepare_ok.
    /// It is ideal if this synchronous replication to one remote replica is to the next replica,
    /// since that is the replica next in line to be leader, which will need to be up-to-date before
    /// it can start the next view.
    ///
    /// At the same time, asynchronous replication keeps going, so that if our local disk is slow
    /// then any latency spike will be masked by more remote prepare_ok messages as they come in.
    /// This gives automatic tail latency tolerance for storage latency spikes.
    ///
    /// The remaining problem then is tail latency tolerance for network latency spikes.
    /// If the next replica is down or partitioned, then the leader's prepare timeout will fire,
    /// and the leader will resend but to another replica, until it receives enough prepare_ok's.
    fn on_prepare(self: *Replica, message: *Message) void {
        if (self.is_repair(message)) {
            log.debug("{}: on_prepare: ignoring (repair)", .{self.replica});
            self.on_repair(message);
            return;
        }

        if (self.status != .normal) {
            log.debug("{}: on_prepare: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_prepare: newer view", .{self.replica});
            self.jump_to_newer_view(message.header.view);
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader() or self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));
        assert(message.header.op > self.op);
        assert(message.header.op > self.commit_min);

        if (self.follower()) self.normal_timeout.reset();

        if (message.header.op > self.op + 1) {
            log.debug("{}: on_prepare: newer op", .{self.replica});
            self.jump_to_newer_op(message.header);
        }

        if (self.journal.previous_entry(message.header)) |previous| {
            // Any previous entry may be a whole journal's worth of ops behind due to wrapping.
            // We therefore do not do any further op, offset or checksum assertions beyond this:
            self.panic_if_hash_chain_would_break_in_the_same_view(previous, message.header);
        }

        // We must advance our op and set the header as dirty before replicating and journalling.
        // The leader needs this before its journal is outrun by any prepare_ok quorum:
        log.debug("{}: on_prepare: advancing: op={}..{} checksum={}..{}", .{
            self.replica,
            self.op,
            message.header.op,
            message.header.nonce,
            message.header.checksum,
        });
        assert(message.header.op == self.op + 1);
        self.op = message.header.op;
        self.journal.set_entry_as_dirty(message.header);

        // TODO Update client's information in the client table.

        self.replicate(message);
        self.append(message);

        if (self.follower()) self.commit_ops_through(message.header.commit);
    }

    fn on_prepare_ok(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_prepare_ok: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_prepare_ok: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view > self.view) {
            // Another replica is treating us as the leader for a view we do not know about.
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_prepare_ok: ignoring (newer view)", .{self.replica});
            return;
        }

        if (self.follower()) {
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_prepare_ok: ignoring (follower)", .{self.replica});
            return;
        }

        if (self.prepare_message) |prepare_message| {
            if (message.header.nonce != prepare_message.header.checksum) {
                log.debug("{}: on_prepare_ok: ignoring (different nonce)", .{self.replica});
                return;
            }
        } else {
            log.debug("{}: on_prepare_ok: ignoring (not preparing)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader());

        assert(message.header.command == .prepare_ok);
        assert(message.header.nonce == self.prepare_message.?.header.checksum);
        assert(message.header.client == self.prepare_message.?.header.client);
        assert(message.header.cluster == self.prepare_message.?.header.cluster);
        assert(message.header.view == self.prepare_message.?.header.view);
        assert(message.header.op == self.prepare_message.?.header.op);
        assert(message.header.commit == self.prepare_message.?.header.commit);
        assert(message.header.offset == self.prepare_message.?.header.offset);
        assert(message.header.epoch == self.prepare_message.?.header.epoch);
        assert(message.header.request == self.prepare_message.?.header.request);
        assert(message.header.operation == self.prepare_message.?.header.operation);
        assert(message.header.op == self.op);
        assert(message.header.op == self.commit_max + 1);

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.prepare_ok_from_all_replicas,
            message,
            self.f + 1,
        ) orelse return;

        assert(count == self.f + 1);
        log.debug("{}: on_prepare_ok: quorum received", .{self.replica});

        self.commit_ops_through(message.header.op);

        self.reset_prepare();
    }

    fn on_commit(self: *Replica, message: *const Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_commit: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_commit: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_commit: newer view", .{self.replica});
            self.jump_to_newer_view(message.header.view);
        }

        if (self.leader()) {
            log.warn("{}: on_commit: ignoring (leader)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));

        // We may not always have the latest commit entry but if we do its checksum must match:
        if (self.journal.entry_for_op_exact(message.header.commit)) |commit_entry| {
            if (commit_entry.checksum == message.header.nonce) {
                log.debug("{}: on_commit: verified commit checksum", .{self.replica});
            } else {
                @panic("commit checksum verification failed");
            }
        }

        self.normal_timeout.reset();

        self.commit_ops_through(message.header.commit);
    }

    fn on_repair(self: *Replica, message: *Message) void {
        assert(message.header.command == .prepare);

        if (self.status != .normal and self.status != .view_change) {
            log.debug("{}: on_repair: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_repair: ignoring (newer view)", .{self.replica});
            return;
        }

        if (self.status == .view_change and message.header.view == self.view) {
            log.debug("{}: on_repair: ignoring (view already started)", .{self.replica});
            return;
        }

        if (self.status == .view_change and self.leader_index(self.view) != self.replica) {
            log.debug("{}: on_repair: ignoring (follower and view change)", .{self.replica});
            return;
        }

        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op or message.header.view < self.view);

        if (self.journal.has_clean(message.header)) {
            log.debug("{}: on_repair: duplicate", .{self.replica});
            self.send_prepare_ok(message);
            return;
        }

        if (self.repair_header(message.header)) {
            assert(self.journal.has_dirty(message.header));

            if (self.repairing) return self.repair_later(message);

            log.debug("{}: on_repair: repairing journal", .{self.replica});
            self.repairing_frame = async self.write_to_journal(message, &self.repairing);
        }
    }

    fn on_start_view_change(self: *Replica, message: *Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(message.header.replica != self.replica);

        if (message.header.view > self.view) {
            log.debug("{}: on_start_view_change: changing to newer view", .{self.replica});
            self.transition_to_view_change_status(message.header.view);
        }

        assert(self.status == .view_change);
        assert(message.header.view == self.view);

        // Wait until we have `f` messages (excluding ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.start_view_change_from_other_replicas,
            message,
            self.f,
        ) orelse return;

        assert(count == self.f);
        log.debug("{}: on_start_view_change: quorum received", .{self.replica});

        // TODO Resend after timeout:
        self.send_do_view_change();
    }

    fn on_do_view_change(self: *Replica, message: *Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(self.leader_index(message.header.view) == self.replica);

        if (message.header.view > self.view) {
            log.debug("{}: on_do_view_change: changing to newer view", .{self.replica});
            // At first glance, it might seem that sending start_view_change messages out now after
            // receiving a do_view_change here would be superfluous. However, this is essential,
            // especially in the presence of asymmetric network faults. At this point, we may not
            // yet have received a do_view_change quorum, and another replica might need our
            // start_view_change message in order to get its do_view_change message through to us.
            // We therefore do not special case the start_view_change function, and we always send
            // start_view_change messages, regardless of the message that initiated the view change:
            self.transition_to_view_change_status(message.header.view);
        }

        assert(self.status == .view_change);
        assert(message.header.view == self.view);

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.do_view_change_from_all_replicas,
            message,
            self.f + 1,
        ) orelse return;

        assert(count == self.f + 1);
        log.debug("{}: on_do_view_change: quorum received", .{self.replica});

        // When the new primary receives f + 1 do_view_change messages from different replicas
        // (including itself), it sets its view number to that in the messages and selects as the
        // new log the one contained in the message with the largest v′; if several messages have
        // the same v′ it selects the one among them with the largest n. It sets its op number to
        // that of the topmost entry in the new log, sets its commit number to the largest such
        // number it received in the do_view_change messages, changes its status to normal, and
        // informs the other replicas of the completion of the view change by sending
        // ⟨start_view v, l, n, k⟩ messages to the other replicas, where l is the new log, n is the
        // op number, and k is the commit number.

        var latest: Header = std.mem.zeroInit(Header, .{});
        var k: u64 = 0;

        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                assert(m.header.command == .do_view_change);
                assert(m.header.cluster == self.cluster);
                assert(m.header.replica == replica);
                assert(m.header.view == self.view);

                if (m.header.commit > k) k = m.header.commit;

                for (std.mem.bytesAsSlice(Header, m.buffer[@sizeOf(Header)..m.header.size])) |h| {
                    assert(h.command == .prepare);
                    assert(h.commit <= k);

                    if (latest.command == .reserved) {
                        latest = h;
                    } else if (h.view > latest.view) {
                        latest = h;
                    } else if (h.view == latest.view and h.op > latest.op) {
                        latest = h;
                    }
                }
            }
        }

        log.debug("{}: on_do_view_change: latest: view={} op={} commit={} checksum={} offset={}", .{
            self.replica,
            latest.view,
            latest.op,
            k,
            latest.checksum,
            latest.offset,
        });

        assert(latest.valid_checksum());
        assert(latest.command == .prepare);
        assert(latest.cluster == self.cluster);
        assert(latest.view < self.view); // Latest normal view before this view change.
        assert(latest.op >= self.commit_max); // Ops may be rewound through a prior view change.
        assert(k >= latest.commit);
        assert(k >= self.commit_max);

        self.op = latest.op;
        self.commit_max = k;
        self.journal.set_entry_as_dirty(&latest);

        // Now that we have the latest HEAD in place, repair any other headers from these messages:
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                for (std.mem.bytesAsSlice(Header, m.buffer[@sizeOf(Header)..m.header.size])) |*h| {
                    _ = self.repair_header(h);
                }
            }
        }

        // Verify that our latest entry was not overwritten by a bug in `repair_header()` above:
        assert(self.op == latest.op);
        if (self.journal.entry_for_op_exact(self.op)) |entry| {
            assert(entry.checksum == latest.checksum);
            assert(entry.view == latest.view);
            assert(entry.op == latest.op);
            assert(entry.offset == latest.offset);
        } else {
            @panic("failed to set latest entry correctly");
        }

        self.repair_headers();

        if (true) return;

        // TODO Repair according to CTRL protocol.

        // TODO Ensure self.commit_max == self.op
        // Next prepare needs this to hold: assert(message.header.op == self.commit_max + 1);

        self.commit_ops_through(self.commit_max);
        assert(self.commit_min == k);
        assert(self.commit_max == k);

        self.transition_to_normal_status(self.view);

        assert(self.status == .normal);
        assert(self.leader());

        // TODO Add journal entries to start_view message.
        self.send_header_to_other_replicas(.{
            .command = .start_view,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit_max,
        });
    }

    fn on_start_view(self: *Replica, message: *const Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(message.header.replica != self.replica);
        assert(message.header.replica == self.leader_index(message.header.view));

        // TODO Assert that start_view message matches what we expect if our journal is empty.
        // TODO Assert that start_view message's oldest op overlaps with our last commit number.
        // TODO Update the journal.

        self.transition_to_normal_status(message.header.view);

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.follower());

        // TODO Update our last op number according to message.

        // TODO self.commit(msg.lastcommitted());
        // TODO self.send_prepare_oks(oldLastOp);
    }

    fn on_request_headers(self: *Replica, message: *const Message) void {
        // TODO Add debug logs.
        if (self.status != .normal and self.status != .view_change) {
            return;
        }

        if (message.header.view != self.view) {
            return;
        }

        if (message.header.replica == self.replica) {
            return;
        }

        const op_min = message.header.commit;
        const op_max = message.header.op;
        assert(op_max >= op_min); // TODO Add Header.bad validator for this.

        // We must add 1 because op_max and op_min are both inclusive:
        const count_max = @intCast(u32, std.math.min(64, op_max - op_min + 1));
        assert(count_max > 0);

        const size_max = @sizeOf(Header) + @sizeOf(Header) * count_max;

        var m = self.message_bus.create_message(size_max) catch unreachable;
        m.header.* = .{
            .command = .headers,
            .nonce = message.header.checksum,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
        };

        var dest = std.mem.bytesAsSlice(Header, m.buffer[@sizeOf(Header)..size_max]);
        const count = self.journal.copy_latest_headers_between(op_min, op_max, dest);
        log.debug("copied {} headers", .{count});

        m.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);
        const data = m.buffer[@sizeOf(Header)..m.header.size];

        m.header.set_checksum_data(data);
        m.header.set_checksum();

        assert(m.references == 0);
        self.send_message_to_replica(message.header.replica, m);
    }

    fn on_headers(self: *Replica, message: *const Message) void {
        if (self.status != .normal and self.status != .view_change) {
            return;
        }

        if (message.header.view != self.view) {
            return;
        }

        if (message.header.replica == self.replica) {
            return;
        }

        log.debug("{}: received headers from {}", .{ self.replica, message.header.replica });

        for (std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..message.header.size])) |*h| {
            if (h.command == .reserved) continue;

            log.debug("{}: {}", .{ self.replica, h });

            _ = self.repair_header(h);
        }
    }

    fn on_prepare_timeout(self: *Replica) void {
        assert(self.status == .normal);
        assert(self.leader());
        assert(self.request_checksum != null);
        assert(self.prepare_message != null);

        var message = self.prepare_message.?;
        assert(message.header.view == self.view);

        // TODO Exponential backoff.
        // TODO Prevent flooding the network due to multiple concurrent rounds of replication.
        self.prepare_attempt += 1;
        self.prepare_timeout.reset();

        // The list of remote replicas yet to send a prepare_ok:
        var waiting: [32]u16 = undefined;
        var waiting_len: usize = 0;
        for (self.prepare_ok_from_all_replicas) |received, replica| {
            if (received == null and replica != self.replica) {
                waiting[waiting_len] = @intCast(u16, replica);
                waiting_len += 1;
                if (waiting_len == waiting.len) break;
            }
        }

        if (waiting_len == 0) {
            log.debug("{}: on_prepare_timeout: waiting for journal", .{self.replica});
            assert(self.prepare_ok_from_all_replicas[self.replica] == null);
            return;
        }

        for (waiting[0..waiting_len]) |replica| {
            log.debug("{}: on_prepare_timeout: waiting for replica {}", .{ self.replica, replica });
        }

        // Cycle through the list for each attempt to reach live replicas and get around partitions:
        // If only the first replica in the list was chosen... liveness would suffer if it was down!
        var replica = waiting[@mod(self.prepare_attempt, waiting_len)];
        assert(replica != self.replica);

        log.debug("{}: on_prepare_timeout: replicating to replica {}", .{ self.replica, replica });
        self.send_message_to_replica(replica, message);
    }

    fn on_commit_timeout(self: *Replica) void {
        self.commit_timeout.reset();

        assert(self.status == .normal);
        assert(self.leader());
        assert(self.commit_min == self.commit_max);

        // TODO Snapshots: Use snapshot checksum if commit is no longer in journal.
        const latest_committed_entry = self.journal.entry_for_op_exact(self.commit_max).?;

        self.send_header_to_other_replicas(.{
            .command = .commit,
            .nonce = latest_committed_entry.checksum,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .commit = self.commit_max,
        });
    }

    fn on_normal_timeout(self: *Replica) void {
        assert(self.status == .normal);
        assert(self.follower());
        self.transition_to_view_change_status(self.view + 1);
    }

    fn on_view_change_timeout(self: *Replica) void {
        assert(self.status == .view_change);
        self.transition_to_view_change_status(self.view + 1);
    }

    fn on_repair_timeout(self: *Replica) void {
        assert(self.status == .normal or self.status == .view_change);
        self.repair_headers();
    }

    fn add_message_and_receive_quorum_exactly_once(
        self: *Replica,
        messages: []?*Message,
        message: *Message,
        threshold: u32,
    ) ?usize {
        assert(messages.len == self.configuration.len);
        assert(message.header.cluster == self.cluster);
        assert(message.header.view == self.view);

        switch (message.header.command) {
            .prepare_ok => {
                assert(self.status == .normal);
                assert(self.leader());
                assert(message.header.nonce == self.prepare_message.?.header.checksum);
            },
            .start_view_change => assert(self.status == .view_change),
            .do_view_change => {
                assert(self.status == .view_change);
                assert(self.leader_index(self.view) == self.replica);
            },
            else => unreachable,
        }

        // TODO Improve this to work for "a cluster of one":
        assert(threshold >= 1);
        assert(threshold <= self.configuration.len);

        // Do not allow duplicate messages to trigger multiple passes through a state transition:
        if (messages[message.header.replica]) |m| {
            // Assert that this truly is a duplicate message and not a different message:
            // TODO Review that all message fields are compared for equality:
            assert(m.header.command == message.header.command);
            assert(m.header.replica == message.header.replica);
            assert(m.header.view == message.header.view);
            assert(m.header.op == message.header.op);
            assert(m.header.commit == message.header.commit);
            assert(m.header.checksum_data == message.header.checksum_data);
            assert(m.header.checksum == message.header.checksum);
            log.debug("{}: on_{s}: ignoring (duplicate message)", .{
                self.replica,
                @tagName(message.header.command),
            });
            return null;
        }

        // Record the first receipt of this message:
        assert(messages[message.header.replica] == null);
        messages[message.header.replica] = message;
        assert(message.references == 1);
        message.references += 1;

        // Count the number of unique messages now received:
        var count: usize = 0;
        for (messages) |received, replica| {
            if (received) |m| {
                assert(m.header.command == message.header.command);
                assert(m.header.replica == replica);
                assert(m.header.view == self.view);
                switch (message.header.command) {
                    .prepare_ok => {
                        assert(m.header.nonce == message.header.nonce);
                    },
                    .start_view_change => {
                        assert(m.header.replica != self.replica);
                    },
                    .do_view_change => {},
                    else => unreachable,
                }
                count += 1;
            }
        }
        log.debug("{}: on_{s}: {} message(s)", .{
            self.replica,
            @tagName(message.header.command),
            count,
        });

        // Wait until we have exactly `threshold` messages for quorum:
        if (count < threshold) {
            log.debug("{}: on_{s}: waiting for quorum", .{
                self.replica,
                @tagName(message.header.command),
            });
            return null;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > threshold) {
            log.debug("{}: on_{s}: ignoring (quorum received already)", .{
                self.replica,
                @tagName(message.header.command),
            });
            return null;
        }

        assert(count == threshold);
        return count;
    }

    fn append(self: *Replica, message: *Message) void {
        assert(self.status == .normal);
        assert(message.header.command == .prepare);
        assert(message.header.view == self.view);
        assert(message.header.op == self.op);

        if (self.appending) {
            log.debug("{}: append: skipping (slow journal outrun by quorum)", .{self.replica});
            self.repair_later(message);
            return;
        }

        log.debug("{}: append: appending to journal", .{self.replica});
        self.appending_frame = async self.write_to_journal(message, &self.appending);
    }

    /// Returns whether `b` succeeds `a` by having a newer view or same view and newer op.
    fn ascending_viewstamps(
        self: *Replica,
        a: *const Header,
        b: *const Header,
    ) bool {
        assert(a.command == .prepare);
        assert(b.command == .prepare);

        if (a.view < b.view) {
            // We do not assert b.op >= a.op, ops may be reordered during a view change.
            return true;
        } else if (a.view > b.view) {
            // We do not assert b.op <= a.op, ops may be reordered during a view change.
            return false;
        } else if (a.op < b.op) {
            assert(a.view == b.view);
            return true;
        } else if (a.op > b.op) {
            assert(a.view == b.view);
            return false;
        } else {
            unreachable;
        }
    }

    fn commit_ops_through(self: *Replica, commit: u64) void {
        assert(self.commit_min <= self.commit_max);
        assert(self.commit_min <= self.op);
        assert(self.commit_max <= self.op or self.commit_max > self.op);
        assert(commit <= self.op or commit > self.op);

        // We have already committed this far:
        if (commit <= self.commit_min) return;

        if (commit > self.commit_max) {
            log.debug("{}: commit_ops_through: advancing commit_max={}..{}", .{
                self.replica,
                self.commit_max,
                commit,
            });
            self.commit_max = commit;
        }

        // If we know we could validate the hash chain even further then wait until we can:
        // This is partial defense-in-depth in case self.op is ever advanced by a stale prepare.
        if (self.op < self.commit_max) {
            log.notice("{}: commit_ops_through: waiting for repair (op < commit)", .{self.replica});
            return;
        }

        // We must validate the hash chain as far as possible, since self.op may disclose a fork:
        // This is defense-in-depth in case jump_to_newer_view() fails.
        if (!self.valid_hash_chain_between(self.commit_min, self.op)) {
            log.notice("{}: commit_ops_through: waiting for repair (hash chain)", .{self.replica});
            return;
        }

        // We may receive commit numbers for ops we do not yet have:
        // Even a naive state transfer may fail to correct for this.
        while (self.commit_min < self.commit_max and self.commit_min < self.op) {
            self.commit_min += 1;
            assert(self.commit_min <= self.op);

            const entry = self.journal.entry_for_op_exact(self.commit_min).?;
            assert(entry.op == self.commit_min);

            // TODO See if we are able to read from Journal before incrementing self.commit_min.

            // TODO Apply to State Machine:
            log.debug("{}: commit_ops_through: executing op={} checksum={} ({s})", .{
                self.replica,
                entry.op,
                entry.checksum,
                @tagName(entry.operation),
            });

            // TODO Add reply to the client table to answer future duplicate requests idempotently.
            // Lookup client table entry using client id.
            // If client's last request id is <= this request id, then update client table entry.
            // Otherwise the client is already ahead of us, and we don't need to update the entry.

            // TODO Now that the reply is in the client table, trigger this message to be sent.
            // TODO Do not reply to client if we are a follower.
        }
    }

    fn ignore_view_change_message(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .start_view_change or
            message.header.command == .do_view_change or
            message.header.command == .start_view);
        assert(message.header.view > 0); // The initial view is already zero.

        const command: []const u8 = @tagName(message.header.command);

        // 4.3 Recovery
        // While a replica's status is recovering it does not participate in either the request
        // processing protocol or the view change protocol.
        // This is critical for correctness (to avoid data loss):
        if (self.status == .recovering) {
            log.debug("{}: on_{s}: ignoring (recovering)", .{ self.replica, command });
            return true;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
            return true;
        }

        if (message.header.view == self.view and self.status == .normal) {
            log.debug("{}: on_{s}: ignoring (view already started)", .{ self.replica, command });
            return true;
        }

        // These may be caused by faults in the network topology.
        switch (message.header.command) {
            .start_view_change, .start_view => {
                if (message.header.replica == self.replica) {
                    log.warn("{}: on_{s}: ignoring (self)", .{ self.replica, command });
                    return true;
                }
            },
            .do_view_change => {
                if (self.leader_index(message.header.view) != self.replica) {
                    log.warn("{}: on_{s}: ignoring (follower)", .{ self.replica, command });
                    return true;
                }
            },
            else => unreachable,
        }

        return false;
    }

    fn is_repair(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .prepare);

        if (self.status == .normal) {
            if (message.header.view < self.view) return true;
            if (message.header.view == self.view and message.header.op <= self.op) return true;
        } else if (self.status == .view_change) {
            if (message.header.view < self.view) return true;
            // The view has already started.
            // TODO Think through scenarios of what could happen if we didn't make this distinction.
        }

        return false;
    }

    fn jump_to_newer_view(self: *Replica, new_view: u64) void {
        log.debug("{}: jump_to_newer_view: advancing: view={}..{}", .{
            self.replica,
            self.view,
            new_view,
        });
        assert(self.status == .normal);
        assert(self.leader() or self.follower());
        assert(new_view > self.view);
        assert(self.leader_index(new_view) != self.replica);

        // 5.2 State Transfer
        // There are two cases, depending on whether the slow node has learned that it is missing
        // requests in its current view, or has heard about a later view. In the former case it only
        // needs to learn about requests after its op-number. In the latter it needs to learn about
        // requests after the latest committed request in its log, since requests after that might
        // have been reordered in the view change, so in this case it sets its op-number to its
        // commit-number and removes all entries after this from its log.

        // It is critical for correctness that we discard any ops here that were not involved in the
        // view change(s), since they may have been replaced in the view change(s) by other ops.
        // If we fail to do this immediately then we may commit the wrong op and diverge state when
        // we process a commit message or when we process a commit number in a prepare message.

        // TODO Critical: Rewind any ops in repair queue.

        if (self.op > self.commit_max) {
            log.debug("{}: jump_to_newer_view: rewinding: op={}..{}", .{
                self.replica,
                self.op,
                self.commit_max,
            });
            self.op = self.commit_max;
            self.journal.remove_entries_from(self.op + 1);
            assert(self.journal.entry_for_op_exact(self.op) != null);
        } else {
            log.debug("{}: jump_to_newer_view: op == commit, no need to rewind", .{self.replica});
        }

        assert(self.op >= self.commit_min);
        assert(self.op <= self.commit_max);
        self.transition_to_normal_status(new_view);
        assert(self.view == new_view);
        assert(self.follower());
    }

    /// Advances `op` to where we need to be before `header` can be processed as a prepare:
    fn jump_to_newer_op(self: *Replica, header: *const Header) void {
        assert(self.status == .normal);
        assert(self.follower());
        assert(header.view == self.view);
        assert(header.op > self.op + 1);
        // We may have learned of a higher commit_max through a commit message before jumping to a
        // newer op that is less than commit_max but greater than commit_min:
        assert(header.op > self.commit_min);

        const latest_entry = self.journal.entry_for_op_exact(self.op).?;

        log.debug("{}: jump_to_newer_op: advancing: op={}..{} checksum={}..{}", .{
            self.replica,
            self.op,
            header.op - 1,
            latest_entry.checksum,
            header.nonce,
        });
        self.op = header.op - 1;
        assert(self.op >= self.commit_min);
        assert(self.op + 1 == header.op);
    }

    /// Panics if immediate neighbors in the same view would have a broken hash chain.
    /// Assumes gaps and does not require that a preceeds b.
    fn panic_if_hash_chain_would_break_in_the_same_view(
        self: *Replica,
        a: *const Header,
        b: *const Header,
    ) void {
        assert(a.command == .prepare);
        assert(b.command == .prepare);
        if (a.view == b.view and a.op + 1 == b.op and a.checksum != b.nonce) {
            assert(a.valid_checksum());
            assert(b.valid_checksum());
            log.emerg("{}: panic_if_hash_chain_would_break: a: {}", .{ self.replica, a });
            log.emerg("{}: panic_if_hash_chain_would_break: b: {}", .{ self.replica, b });
            @panic("hash chain would break");
        }
    }

    fn repair_dirty(self: *Replica) void {
        // TODO Add a flag to avoid scanning all dirty bits.
        // TODO Avoid requesting data if we have already just requested it.
        // TODO Avoid requesting data if we are busy writing it (writes could take 10 seconds).
    }

    /// Repairs must always backfill in behind self.op and may never advance past self.op (HEAD).
    /// Otherwise, a split-brain leader may reapply an op that was removed by jump_to_newer_view(),
    /// which could then be committed by a higher commit_max number received in a commit message.
    /// Since we always wait to commit until the hash chain between self.commit_min and self.op is
    /// fully intact, and since self.op is never advanced except in the current view, we can be sure
    /// that we do not fork the hash chain.
    fn repair_header(self: *Replica, header: *const Header) bool {
        assert(self.status == .normal or self.status == .view_change);
        assert(header.command == .prepare);
        assert(header.size >= @sizeOf(Header));
        assert(header.view <= self.view);

        if (self.status == .normal and header.op > self.op and header.view < self.view) {
            // This only applies for normal status:
            // Within a view change, self.view will always be greater.
            // For example, we may have jumped from view 5 to 10 on receiving a start_view_change.
            // If we're the new leader processing do_view_change messages, our self.op may be old.
            // It's critical for correctness that we don't consider newer ops as reordered.
            // Otherwise, catastrophic data loss would occur.
            assert(self.status != .view_change);
            // For example, an op reordered through a view change (section 5.2 in the VRR paper).
            log.debug("{}: repair_header: ignoring (reordered op)", .{self.replica});
            return false;
        }

        if (self.journal.entry(header)) |existing| {
            if (existing.view > header.view) {
                log.debug("{}: repair_header: ignoring (newer view)", .{self.replica});
                return false;
            }

            if (existing.view == header.view and existing.op > header.op) {
                // For example, the journal wrapped.
                log.debug("{}: repair_header: ignoring (newer op)", .{self.replica});
                return false;
            }

            if (existing.checksum == header.checksum) {
                if (self.journal.dirty[header.op]) {
                    log.debug("{}: repair_header: exists (dirty checksum)", .{self.replica});
                    // We still want to run the chain and overlap checks below before deciding...
                } else {
                    log.debug("{}: repair_header: ignoring (clean checksum)", .{self.replica});
                    return false;
                }
            } else if (existing.view == header.view) {
                // We expect that the same view and op must have the same checksum:
                assert(existing.op < header.op);
                log.debug("{}: repair_header: exists (same view, older op)", .{self.replica});
            } else if (existing.op >= header.op) {
                assert(existing.view < header.view);
                log.debug("{}: repair_header: exists (older view, reordered op)", .{self.replica});
            } else {
                assert(existing.view < header.view);
                assert(existing.op < header.op);
                log.debug("{}: repair_header: exists (older view, older op)", .{self.replica});
            }

            assert(existing.view <= header.view);
            assert(existing.op <= header.op or existing.view < header.view);
        } else {
            log.debug("{}: repair_header: gap", .{self.replica});
        }

        if (self.repair_header_would_break_chain_with_newer_neighbor(header)) {
            log.debug("{}: repair_header: would break chain with newer neighbor", .{self.replica});
            return false;
        }

        if (self.repair_header_would_succeed_newer_neighbor(header)) {
            log.debug("{}: repair_header: would succeed newer neighbor", .{self.replica});
            return false;
        }

        if (self.repair_header_would_overlap_newer_neighbor(header)) {
            log.debug("{}: repair_header: would overlap newer neighbor", .{self.replica});
            return false;
        }

        // TODO Snapshots: Skip if this header is already snapshotted.

        self.journal.set_entry_as_dirty(header);
        return true;
    }

    /// If we repair this header would we introduce a break in the chain with a newer neighbor?
    /// Ignores breaks in the chain if the neighboring entry is older and would in turn be repaired.
    /// Panics if the hash chain would break for immediate neighbors in the same view.
    fn repair_header_would_break_chain_with_newer_neighbor(
        self: *Replica,
        header: *const Header,
    ) bool {
        if (self.journal.previous_entry(header)) |previous| {
            if (previous.checksum == header.nonce) {
                assert(previous.view <= header.view);
                assert(previous.op + 1 == header.op);
                return false;
            }
            self.panic_if_hash_chain_would_break_in_the_same_view(previous, header);
            return self.ascending_viewstamps(header, previous);
        }
        if (self.journal.next_entry(header)) |next| {
            if (header.checksum == next.nonce) {
                assert(header.view <= next.view);
                assert(header.op + 1 == next.op);
                return false;
            }
            self.panic_if_hash_chain_would_break_in_the_same_view(header, next);
            return self.ascending_viewstamps(header, next);
        }
        return false;
    }

    /// If we repair this header would we overwrite a newer neighbor (immediate or distant)?
    /// Ignores overlap if the neighboring entry is older and would in turn be repaired.
    fn repair_header_would_overlap_newer_neighbor(self: *Replica, header: *const Header) bool {
        // TODO Snapshots: Handle journal wrap around.
        {
            // Look behind this entry for any preceeding entry that this would overwrite.
            var op: usize = header.op;
            while (op > 0) {
                op -= 1;
                if (self.journal.entry_for_op(op)) |neighbor| {
                    if (self.journal.next_offset(neighbor) > header.offset) {
                        if (self.ascending_viewstamps(header, neighbor)) return true;
                    } else {
                        break;
                    }
                }
            }
        }
        {
            // Look beyond this entry for any succeeding entry that this would overwrite.
            var op: usize = header.op + 1;
            while (op <= self.op) : (op += 1) {
                if (self.journal.entry_for_op(op)) |neighbor| {
                    if (self.journal.next_offset(header) > neighbor.offset) {
                        if (self.ascending_viewstamps(header, neighbor)) return true;
                    } else {
                        break;
                    }
                }
            }
        }
        return false;
    }

    /// If we repair this header would we succeed a newer neighbor (immediate or distant)?
    /// Looks behind this entry for any preceeding entry that would be newer.
    fn repair_header_would_succeed_newer_neighbor(self: *Replica, header: *const Header) bool {
        // TODO Snapshots: Handle journal wrap around:
        // We may not know the journal start position (if we do not yet have the snapshot).
        // Run from header.op - 1 down through zero to start?
        var op: usize = header.op;
        while (op > 0) {
            op -= 1;
            if (self.journal.entry_for_op(op)) |neighbor| {
                return self.ascending_viewstamps(header, neighbor);
            }
        }
        return false;
    }

    /// Starting from the latest journal entry, backfill any missing or disconnected headers.
    /// A header is disconnected if it breaks the hash chain with its newer neighbor to the right.
    /// Since we work backwards from the latest entry, we should always be able to fix the chain.
    fn repair_headers(self: *Replica) void {
        self.repair_timeout.reset();

        if (self.status != .normal and self.status != .view_change) return;
        assert(self.commit_min <= self.op);
        assert(self.commit_min <= self.commit_max);

        // TODO Handle case where we are requesting reordered headers that no longer exist.

        // We expect these always to exist (and to be correct):
        assert(self.journal.entry_for_op_exact(self.commit_min) != null);
        assert(self.journal.entry_for_op_exact(self.op) != null);

        // Request missing committed headers:
        if (self.op < self.commit_max) {
            log.notice("{}: repair_headers: op={} < commit_max={}", .{
                self.replica,
                self.op,
                self.commit_max,
            });
            // TODO This must go to the leader of the view?
            self.send_header_to_other_replicas(.{
                .command = .request_headers,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .commit = self.op + 1,
                .op = self.commit_max,
            });
            return;
        }

        // Request any missing or disconnected headers:
        // TODO Snapshots: Ensure that self.commit_min op always exists in the journal.
        var broken = self.journal.find_latest_headers_break_between(self.commit_min, self.op);
        if (broken) |range| {
            log.notice("{}: repair_headers: latest break: {}", .{ self.replica, range });
            assert(range.op_min > self.commit_min);
            assert(range.op_max < self.op);
            // TODO Ask any N random replicas excluding ourself (to reduce traffic).
            self.send_header_to_other_replicas(.{
                .command = .request_headers,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .commit = range.op_min,
                .op = range.op_max,
            });
            return;
        }

        // Assert that all headers are now present and connected with a perfect hash chain:
        assert(self.op >= self.commit_max);
        assert(self.valid_hash_chain_between(self.commit_min, self.op));
    }

    fn repair_last_queued_message_if_any(self: *Replica) void {
        if (self.status != .normal and self.status != .view_change) return;

        while (!self.repairing) {
            if (self.repair_queue) |message| {
                assert(self.repair_queue_len > 0);
                self.repair_queue = message.next;
                self.repair_queue_len -= 1;

                message.references -= 1;
                message.next = null;
                self.on_repair(message);
                assert(self.repair_queue != message); // Catch an accidental requeue by on_repair().
                self.message_bus.gc(message);
            } else {
                assert(self.repair_queue_len == 0);
                break;
            }
        }
    }

    fn repair_later(self: *Replica, message: *Message) void {
        assert(self.appending or self.repairing);
        assert(message.references > 0);
        assert(message.header.command == .prepare);
        assert(message.next == null);

        if (!self.repairing) {
            log.debug("{}: repair_later: repairing immediately", .{self.replica});
            self.on_repair(message);
            return;
        }

        log.debug("{}: repair_later: {} message(s)", .{ self.replica, self.repair_queue_len });

        if (self.repair_queue_len >= self.repair_queue_max) {
            log.debug("{}: repair_later: dropping", .{self.replica});
            return;
        }

        log.debug("{}: repair_later: queueing", .{self.replica});

        message.references += 1;
        message.next = self.repair_queue;
        self.repair_queue = message;
        self.repair_queue_len += 1;
    }

    /// Replicates to the next replica in the configuration (until we get back to the leader):
    /// Replication starts and ends with the leader, we never forward back to the leader.
    /// Does not flood the network with prepares that have already committed.
    /// TODO Use recent heartbeat data for next replica to leapfrog if faulty.
    fn replicate(self: *Replica, message: *Message) void {
        assert(self.status == .normal);
        assert(message.header.command == .prepare);
        assert(message.header.view == self.view);
        assert(message.header.op == self.op);

        if (message.header.op <= self.commit_max) {
            log.debug("{}: replicate: skipping (already committed by cluster)", .{self.replica});
            return;
        }

        const next = @mod(self.replica + 1, @intCast(u16, self.configuration.len));
        if (next == self.leader_index(message.header.view)) {
            log.debug("{}: replicate: replication complete", .{self.replica});
            return;
        }

        log.debug("{}: replicate: replicating to replica {}", .{ self.replica, next });
        self.send_message_to_replica(next, message);
    }

    fn reset_prepare(self: *Replica) void {
        if (self.prepare_message) |message| {
            self.request_checksum = null;
            message.references -= 1;
            self.message_bus.gc(message);
            self.prepare_message = null;
            self.prepare_attempt = 0;
            self.prepare_timeout.stop();
            self.reset_quorum_counter(self.prepare_ok_from_all_replicas, .prepare_ok);
        }
        assert(self.request_checksum == null);
        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        assert(self.prepare_timeout.ticking == false);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
    }

    fn reset_quorum_counter(self: *Replica, messages: []?*Message, command: Command) void {
        var count: usize = 0;
        for (messages) |*received, replica| {
            if (received.*) |message| {
                assert(message.header.command == command);
                assert(message.header.replica == replica);
                assert(message.header.view <= self.view);
                message.references -= 1;
                self.message_bus.gc(message);
                count += 1;
            }
            received.* = null;
        }
        log.debug("{}: reset {} {s} message(s)", .{ self.replica, count, @tagName(command) });
    }

    fn send_prepare_ok(self: *Replica, message: *Message) void {
        assert(message.references > 0);
        assert(message.header.command == .prepare);
        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op or message.header.view < self.view);

        if (self.status != .normal) {
            log.debug("{}: send_prepare_ok: not sending ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: send_prepare_ok: not sending (older view)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(message.header.op <= self.op);

        if (message.header.op <= self.commit_max) {
            log.debug("{}: send_prepare_ok: not sending (committed)", .{self.replica});
            return;
        }

        if (self.journal.has_clean(message.header)) {
            assert(message.header.replica == self.leader_index(message.header.view));
            self.send_header_to_replica(message.header.replica, .{
                .command = .prepare_ok,
                .nonce = message.header.checksum,
                .client = message.header.client,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = message.header.view,
                .op = message.header.op,
                .commit = message.header.commit,
                .offset = message.header.offset,
                .epoch = message.header.epoch,
                .request = message.header.request,
                .operation = message.header.operation,
            });
        } else {
            log.debug("{}: send_prepare_ok: not sending (dirty)", .{self.replica});
            return;
        }
    }

    /// When replica i receives start_view_change messages for its view from f other replicas,
    /// it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
    /// primary in the new view. Here v is its view, l is its log, v′ is the view number of the
    /// latest view in which its status was normal, n is the op number, and k is the commit
    /// number.
    fn send_do_view_change(self: *Replica) void {
        assert(self.status == .view_change);

        const size_max = @sizeOf(Header) * 8;

        // TODO Add a method to MessageBus to provide a pre-allocated message:
        var message = self.message_bus.create_message(size_max) catch unreachable;
        message.header.* = .{
            .command = .do_view_change,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit_max,
        };

        var dest = std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..size_max]);
        const count = self.journal.copy_latest_headers_between(0, self.op, dest);

        message.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);
        const data = message.buffer[@sizeOf(Header)..message.header.size];

        message.header.set_checksum_data(data);
        message.header.set_checksum();

        const new_leader = self.leader_index(self.view);

        assert(message.references == 0);
        self.send_message_to_replica(new_leader, message);
    }

    fn send_header_to_other_replicas(self: *Replica, header: Header) void {
        for (self.configuration) |_, replica| {
            if (replica != self.replica) {
                self.send_header_to_replica(@intCast(u16, replica), header);
            }
        }
    }

    // TODO Work out the maximum number of messages a replica may output per tick() or on_message().
    fn send_header_to_replica(self: *Replica, replica: u16, header: Header) void {
        log.debug("{}: sending {s} to replica {}: {}", .{
            self.replica,
            @tagName(header.command),
            replica,
            header,
        });
        assert(header.replica == self.replica);
        assert(header.view == self.view);

        self.message_bus.send_header_to_replica(replica, header);
    }

    fn send_message_to_replica(self: *Replica, replica: u16, message: *Message) void {
        log.debug("{}: sending {s} to replica {}: {}", .{
            self.replica,
            @tagName(message.header.command),
            replica,
            message.header,
        });
        switch (message.header.command) {
            .prepare => {
                // We do not assert message.header.replica as we would for send_header_to_replica().
                // We may forward messages sent by another replica (e.g. prepares from the leader).
                assert(self.status == .normal);
            },
            .do_view_change => {
                assert(self.status == .view_change);
            },
            .headers => {
                assert(self.status == .normal or self.status == .view_change);
                assert(message.header.replica == self.replica);
            },
            else => unreachable,
        }
        assert(message.header.cluster == self.cluster);
        assert(message.header.view == self.view);
        self.message_bus.send_message_to_replica(replica, message);
    }

    fn transition_to_normal_status(self: *Replica, new_view: u64) void {
        log.debug("{}: transition_to_normal_status: view {}", .{ self.replica, new_view });
        // In the VRR paper it's possible to transition from .normal to .normal for the same view.
        // For example, this could happen after a state transfer triggered by an op jump.
        assert(new_view >= self.view);
        self.view = new_view;
        self.status = .normal;

        if (self.leader()) {
            log.debug("{}: transition_to_normal_status: leader", .{self.replica});

            self.commit_timeout.start();
            self.normal_timeout.stop();
            self.view_change_timeout.stop();
        } else {
            log.debug("{}: transition_to_normal_status: follower", .{self.replica});

            self.commit_timeout.stop();
            self.normal_timeout.start();
            self.view_change_timeout.stop();
        }

        // This is essential for correctness:
        self.reset_prepare();

        // Reset and garbage collect all view change messages (if any):
        // This is not essential for correctness, only efficiency.
        // We just don't want to tie them up until the next view change (when they must be reset):
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);
    }

    /// A replica i that notices the need for a view change advances its view, sets its status to
    /// view_change, and sends a ⟨start_view_change v, i⟩ message to all the other replicas,
    /// where v identifies the new view. A replica notices the need for a view change either based
    /// on its own timer, or because it receives a start_view_change or do_view_change message for
    /// a view with a larger number than its own view.
    fn transition_to_view_change_status(self: *Replica, new_view: u64) void {
        log.debug("{}: transition_to_view_change_status: view {}", .{ self.replica, new_view });
        assert(new_view > self.view);
        self.view = new_view;
        self.status = .view_change;

        self.commit_timeout.stop();
        self.normal_timeout.stop();
        self.view_change_timeout.start();

        self.reset_prepare();

        // Some VR implementations reset their counters only on entering a view, perhaps assuming
        // the view will be followed only by a single subsequent view change to the next view.
        // However, multiple successive view changes can fail, e.g. after a view change timeout.
        // We must therefore reset our counters here to avoid counting messages from an older view,
        // which would violate the quorum intersection property essential for correctness.
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);

        // Send only to other replicas (and not to ourself) to avoid a quorum off-by-one error:
        // This could happen if the replica mistakenly counts its own message in the quorum.
        self.send_header_to_other_replicas(.{
            .command = .start_view_change,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = new_view,
        });
        // TODO Resend after timeout.
    }

    /// Returns true if all operations are present, correctly ordered and connected by hash chain,
    /// between `op_min` and `op_max` (both inclusive).
    fn valid_hash_chain_between(self: *Replica, op_min: u64, op_max: u64) bool {
        assert(op_min <= op_max);

        // If we use anything less than self.op then we may commit ops for a forked hash chain that
        // has since been reordered by a new leader.
        assert(op_max == self.op);
        var b = self.journal.entry_for_op_exact(op_max).?;

        var op = op_max;
        while (op > op_min) {
            op -= 1;

            if (self.journal.entry_for_op_exact(op)) |a| {
                assert(a.op + 1 == b.op);
                if (a.checksum == b.nonce) {
                    assert(self.ascending_viewstamps(a, b));
                    b = a;
                } else {
                    log.notice("{}: valid_hash_chain_between: break: A: {}", .{ self.replica, a });
                    log.notice("{}: valid_hash_chain_between: break: B: {}", .{ self.replica, b });
                    return false;
                }
            } else {
                log.notice("{}: valid_hash_chain_between: missing op={}", .{ self.replica, op });
                return false;
            }
        }
        assert(b.op == op_min);
        return true;
    }

    fn write_to_journal(self: *Replica, message: *Message, lock: *bool) void {
        assert(lock.* == false);
        lock.* = true;
        defer lock.* = false;

        assert(message.references > 0);
        assert(message.header.command == .prepare);
        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op or message.header.view < self.view);

        if (!self.journal.has(message.header)) {
            log.debug("{}: write_to_journal: ignoring (header changed)", .{self.replica});
            return;
        }

        message.references += 1;

        if (self.journal.has_dirty(message.header)) {
            self.journal.write(message.header, message.buffer);
        } else {
            log.debug("{}: write_to_journal: skipping (already clean)", .{self.replica});
        }

        self.send_prepare_ok(message);

        message.references -= 1;
        self.message_bus.gc(message);
    }
};

pub fn run() !void {
    assert(@sizeOf(Header) == 128);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const f = 1;
    const journal_size = 512 * 1024 * 1024;
    const journal_headers = 1048576;
    const cluster = 123456789;

    var configuration: [3]ConfigurationAddress = undefined;
    var message_bus = try MessageBus.init(allocator, &configuration);

    var journals: [2 * f + 1]Journal = undefined;
    for (journals) |*journal, index| {
        journal.* = try Journal.init(
            allocator,
            @intCast(u16, index),
            journal_size,
            journal_headers,
        );
    }

    var state_machines: [2 * f + 1]StateMachine = undefined;
    for (state_machines) |*state_machine| {
        state_machine.* = try StateMachine.init(allocator);
    }

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        replica.* = try Replica.init(
            allocator,
            cluster,
            f,
            &configuration,
            &message_bus,
            &journals[index],
            &state_machines[index],
            @intCast(u16, index),
        );
        configuration[index] = .{ .replica = replica };
    }

    var ticks: usize = 0;
    while (true) : (ticks += 1) {
        message_bus.send_queued_messages();

        std.debug.print("\n", .{});
        log.debug("ticking (message_bus has {} messages allocated)", .{message_bus.allocated});
        std.debug.print("\n", .{});

        var leader: ?*Replica = null;
        for (replicas) |*replica| {
            if (replica.status == .normal and replica.leader()) leader = replica;
            replica.tick();
        }

        if (leader) |replica| {
            if (ticks == 1 or ticks == 5) {
                var request = message_bus.create_message(sector_size) catch unreachable;
                request.header.* = .{
                    .cluster = cluster,
                    .client = 1,
                    .view = 0,
                    .request = @intCast(u32, ticks),
                    .command = .request,
                    .operation = .noop,
                };
                request.header.set_checksum_data(request.buffer[0..0]);
                request.header.set_checksum();

                message_bus.send_message_to_replica(replica.replica, request);
            }
        }

        message_bus.send_queued_messages();
        std.time.sleep(std.time.ns_per_ms * 1000);
    }
}

pub fn main() !void {
    var frame = async run();
    nosuspend try await frame;
}
