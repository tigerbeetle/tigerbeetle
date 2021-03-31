const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);
pub const log_level: std.log.Level = .debug;

const conf = @import("tigerbeetle.conf");

// TODO: This currently needs to be switched out manually.
const MessageBus = @import("message_bus.zig").MessageBus;
//const MessageBus = @import("test_message_bus.zig").MessageBus;
const Message = MessageBus.Message;

const ConcurrentRanges = @import("concurrent_ranges.zig").ConcurrentRanges;
const Range = @import("concurrent_ranges.zig").Range;

const Operation = @import("state_machine.zig").Operation;
const StateMachine = @import("state_machine.zig").StateMachine;

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

    request_start_view,
    request_prepare,
    request_headers,
    headers,
};

/// Network message and journal entry header:
/// We reuse the same header for both so that prepare messages from the leader can simply be
/// journalled as is by the followers without requiring any further modification.
pub const Header = packed struct {
    comptime {
        assert(@sizeOf(Header) == 128);
    }
    /// A checksum covering only the rest of this header (but including checksum_body):
    /// This enables the header to be trusted without having to recv() or read() associated body.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum: u128 = 0,

    /// A checksum covering only associated body.
    checksum_body: u128 = 0,

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

    /// The size of this message header and any associated body:
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

    pub fn calculate_checksum_body(self: *const Header, body: []const u8) u128 {
        // Reserved headers should be completely zeroed with a checksum_body also of 0:
        if (self.command == .reserved and self.size == 0 and body.len == 0) return 0;
        if (self.size == @sizeOf(Header) and body.len == 0) return 0;

        assert(self.size == @sizeOf(Header) + body.len);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_body));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(body[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    /// This must be called only after set_checksum_body() so that checksum_body is also covered:
    pub fn set_checksum(self: *Header) void {
        self.checksum = self.calculate_checksum();
    }

    pub fn set_checksum_body(self: *Header, body: []const u8) void {
        self.checksum_body = self.calculate_checksum_body(body);
    }

    pub fn valid_checksum(self: *const Header) bool {
        return self.checksum == self.calculate_checksum();
    }

    pub fn valid_checksum_body(self: *const Header, body: []const u8) bool {
        return self.checksum_body == self.calculate_checksum_body(body);
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum is valid, and expects that has already been done.
    pub fn invalid(self: *const Header) ?[]const u8 {
        switch (self.command) {
            .reserved => if (self.size != 0) return "size != 0",
            else => if (self.size < @sizeOf(Header)) return "size < @sizeOf(Header)",
        }
        if (self.epoch != 0) return "epoch != 0";
        switch (self.command) {
            .reserved => {
                if (self.checksum != 0) return "checksum != 0";
                if (self.checksum_body != 0) return "checksum_body != 0";
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
                        if (self.checksum_body != 0) return "init: checksum_body != 0";
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
                        if (self.checksum_body != 0) return "init: checksum_body != 0";
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
                        if (self.checksum_body != 0) return "checksum_body != 0";
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
        assert(self.checksum_body == 0);
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

/// TODO Use IO and callbacks:
pub const Storage = struct {
    allocator: *Allocator,
    memory: []u8 align(conf.sector_size),
    size: u64,

    pub fn init(allocator: *Allocator, size: u64) !Storage {
        var memory = try allocator.allocAdvanced(u8, conf.sector_size, size, .exact);
        errdefer allocator.free(memory);
        std.mem.set(u8, memory, 0);

        return Storage{
            .allocator = allocator,
            .memory = memory,
            .size = size,
        };
    }

    fn deinit() void {
        self.allocator.free(self.memory);
    }

    fn assert_bounds_and_alignment(self: *Storage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= self.size);

        assert(@mod(@ptrToInt(buffer.ptr), conf.sector_size) == 0);
        assert(@mod(buffer.len, conf.sector_size) == 0);
        assert(@mod(offset, conf.sector_size) == 0);
    }

    fn read(self: *Storage, buffer: []u8, offset: u64) void {
        self.assert_bounds_and_alignment(buffer, offset);
        // TODO Subdivide read into individual sectors if a we get an EIO (latent sector error).
        std.mem.copy(u8, buffer, self.memory[offset .. offset + buffer.len]);
    }

    fn write(self: *Storage, buffer: []const u8, offset: u64) void {
        self.assert_bounds_and_alignment(buffer, offset);
        std.mem.copy(u8, self.memory[offset .. offset + buffer.len], buffer);
        // pwriteAll(buffer, offset) catch |err| switch (err) {
        //     error.InputOutput => @panic("latent sector error: no spare sectors to reallocate"),
        //     else => {
        //         log.emerg("write: error={} buffer.len={} offset={}", .{ err, buffer.len, offset });
        //         @panic("unrecoverable disk error");
        //     },
        // };
    }
};

pub const Journal = struct {
    allocator: *Allocator,
    storage: *Storage,
    replica: u16,
    size: u64,
    size_headers: u64,
    size_circular_buffer: u64,
    headers: []Header align(conf.sector_size),
    dirty: []bool,

    /// We copy-on-write to this buffer when writing, as in-memory headers may change concurrently:
    write_headers_buffer: []u8 align(conf.sector_size),

    /// Apart from the header written with the entry, we also store two redundant copies of each
    /// header at different locations on disk, and we alternate between these for each append.
    /// This tracks which version (0 or 1) should be written to next:
    write_headers_version: u1 = 0,

    /// These serialize concurrent writes but only for overlapping ranges:
    writing_headers: ConcurrentRanges = .{ .name = "write_headers" },
    writing_sectors: ConcurrentRanges = .{ .name = "write_sectors" },

    pub fn init(
        allocator: *Allocator,
        storage: *Storage,
        replica: u16,
        size: u64,
        headers_count: u32,
    ) !Journal {
        if (@mod(size, conf.sector_size) != 0) return error.SizeMustBeAMultipleOfSectorSize;
        if (!std.math.isPowerOfTwo(headers_count)) return error.HeadersCountMustBeAPowerOfTwo;
        assert(storage.size == size);

        const headers_per_sector = @divExact(conf.sector_size, @sizeOf(Header));
        assert(headers_per_sector > 0);
        assert(headers_count >= headers_per_sector);

        var headers = try allocator.allocAdvanced(Header, conf.sector_size, headers_count, .exact);
        errdefer allocator.free(headers);
        for (headers) |*header| header.zero();

        var dirty = try allocator.alloc(bool, headers.len);
        errdefer allocator.free(dirty);
        std.mem.set(bool, dirty, false);

        var write_headers_buffer = try allocator.allocAdvanced(
            u8,
            conf.sector_size,
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
            .storage = storage,
            .replica = replica,
            .size = size,
            .size_headers = size_headers,
            .size_circular_buffer = size_circular_buffer,
            .headers = headers,
            .dirty = dirty,
            .write_headers_buffer = write_headers_buffer,
        };

        assert(@mod(self.size_circular_buffer, conf.sector_size) == 0);
        assert(@mod(@ptrToInt(&self.headers[0]), conf.sector_size) == 0);
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
            assert(existing.checksum_body == 0);
            assert(existing.offset == 0);
            assert(existing.size == 0);
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
                    // Further, while repair_header() should never put an older view to the right
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
                        // TODO Add unit test especially for this.
                        // This is important if we see `self.op < self.commit_max` then request
                        // prepares and then later receive a newer view to the left of `self.op`.
                        // We must then repair `self.op` which was reordered through a view change.
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

    fn read_sectors(self: *Journal, buffer: []u8, offset: u64) void {
        // Memory must not be owned by self.headers as self.headers may be modified concurrently:
        assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
            @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

        log.debug("{}: journal: read_sectors: offset={} len={}", .{
            self.replica,
            offset,
            buffer.len,
        });

        self.storage.read(buffer, offset);
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

    pub fn write(self: *Journal, message: *const Message) void {
        assert(message.header.command == .prepare);
        assert(message.header.size >= @sizeOf(Header));
        assert(message.header.size <= message.buffer.len);
        assert(message.buffer.len == Journal.sector_ceil(message.header.size));
        assert(message.header.offset + message.buffer.len <= self.size_circular_buffer);

        // The underlying header memory must be owned by the buffer and not by self.headers:
        // Otherwise, concurrent writes may modify the memory of the pointer while we write.
        assert(@ptrToInt(message.header) == @ptrToInt(message.buffer.ptr));

        // There should be no concurrency between setting an entry as dirty and deciding to write:
        assert(self.has_dirty(message.header));

        self.write_debug(message.header, "starting");

        self.write_sectors(message.buffer, self.offset_in_circular_buffer(message.header.offset));
        if (!self.has(message.header)) {
            self.write_debug(message.header, "entry changed while writing sectors");
            return;
        }

        // TODO Snapshots
        self.write_headers_between(message.header.op, message.header.op);
        if (!self.has(message.header)) {
            self.write_debug(message.header, "entry changed while writing headers");
            return;
        }

        self.write_debug(message.header, "complete, marking clean");
        // TODO Snapshots
        self.dirty[message.header.op] = false;
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
        // Memory must not be owned by self.headers as self.headers may be modified concurrently:
        assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
            @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

        // TODO We can move this range queuing right into Storage and remove write_sectors entirely.
        // Our ConcurrentRange structure would also need to be weaned off of async/await but at
        // least then we can manage this all in one place (i.e. in Storage).
        var range = Range{ .offset = offset, .len = buffer.len };
        self.writing_sectors.acquire(&range);
        defer self.writing_sectors.release(&range);

        log.debug("{}: journal: write_sectors: offset={} len={}", .{
            self.replica,
            offset,
            buffer.len,
        });

        self.storage.write(buffer, offset);
    }

    pub fn sector_floor(offset: u64) u64 {
        const sectors = std.math.divFloor(u64, offset, conf.sector_size) catch unreachable;
        return sectors * conf.sector_size;
    }

    pub fn sector_ceil(offset: u64) u64 {
        const sectors = std.math.divCeil(u64, offset, conf.sector_size) catch unreachable;
        return sectors * conf.sector_size;
    }
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
    configuration: []MessageBus.Address,

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

    /// Whether we have experienced a view jump:
    /// If this is true then we must request a start_view message from the leader before committing.
    /// This prevents us from committing ops that may have been reordered through a view change.
    view_jump_barrier: bool = false,

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

    /// Used to read a journal entry before committing operations to the state machine:
    commit_buffer: []u8 align(conf.sector_size),

    /// The current request's checksum (used for now to enforce one-at-a-time request processing):
    request_checksum: ?u128 = null,

    /// The current prepare message (used to cross-check prepare_ok messages, and for resending):
    prepare_message: ?*Message = null,
    prepare_attempt: u64 = 0,

    appending: bool = false,
    appending_frame: @Frame(write_to_journal) = undefined,

    repairing: bool = false,
    repairing_frame: @Frame(write_to_journal) = undefined,

    sending_prepare: bool = false,
    sending_prepare_frame: @Frame(send_prepare_to_replica) = undefined,

    repair_queue: ?*Message = null,
    repair_queue_len: usize = 0,
    repair_queue_max: usize = 3,

    /// Unique prepare_ok messages for the same view, op number and checksum from ALL replicas:
    prepare_ok_from_all_replicas: []?*Message,

    /// Unique start_view_change messages for the same view from OTHER replicas (excluding ourself):
    start_view_change_from_other_replicas: []?*Message,

    /// Unique do_view_change messages for the same view from ALL replicas (including ourself):
    do_view_change_from_all_replicas: []?*Message,

    /// Whether the leader has received a quorum of do_view_change messages for the view change:
    /// Determines whether the leader may effect repairs according to the CTRL protocol.
    do_view_change_quorum: bool = false,

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

    /// The number of ticks before repairing missing/disconnected headers and/or dirty entries:
    repair_timeout: Timeout,

    /// The number of ticks elapsed in total:
    ticks: u64 = 0,

    /// Used to provide deterministic entropy to `choose_any_other_replica()`.
    /// Incremented whenever `choose_any_other_replica()` is called.
    choose_any_other_replica_ticks: u64 = 0,

    // TODO Limit integer types for `f` and `replica` to match their upper bounds in practice.
    pub fn init(
        allocator: *Allocator,
        cluster: u128,
        f: u32,
        configuration: []MessageBus.Address,
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
            .checksum_body = 0,
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
        assert(init_prepare.invalid() == null);
        journal.headers[0] = init_prepare;
        journal.assert_headers_reserved_from(init_prepare.op + 1);

        var commit_buffer = try allocator.allocAdvanced(u8, conf.sector_size, conf.response_size_max, .exact);
        errdefer allocator.free(commit_buffer);
        std.mem.set(u8, commit_buffer, 0);

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
            .commit_buffer = commit_buffer,
            .message_bus = message_bus,
            .journal = journal,
            .state_machine = state_machine,
            .prepare_ok_from_all_replicas = prepare_ok,
            .start_view_change_from_other_replicas = start_view_change,
            .do_view_change_from_all_replicas = do_view_change,

            .prepare_timeout = Timeout{
                .name = "prepare_timeout",
                .replica = replica,
                .after = 20,
            },
            .commit_timeout = Timeout{
                .name = "commit_timeout",
                .replica = replica,
                .after = 100,
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
                .after = 20,
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
        self.ticks += 1;

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

        self.message_bus.tick();
    }

    /// Called by the MessageBus to deliver a message to the replica.
    pub fn on_message(self: *Replica, message: *Message) void {
        log.debug("{}:", .{self.replica});
        log.debug("{}: on_message: view={} status={s} {}", .{
            self.replica,
            self.view,
            @tagName(self.status),
            message.header,
        });
        if (message.header.invalid()) |reason| {
            log.debug("{}: on_message: invalid ({s})", .{ self.replica, reason });
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
            .request_start_view => self.on_request_start_view(message),
            .request_prepare => self.on_request_prepare(message),
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

        var body = message.buffer[@sizeOf(Header)..message.header.size];
        self.state_machine.prepare(message.header.operation, body);

        var latest_entry = self.journal.entry_for_op_exact(self.op).?;

        message.header.nonce = latest_entry.checksum;
        message.header.view = self.view;
        message.header.op = self.op + 1;
        message.header.commit = self.commit_max;
        message.header.offset = self.journal.next_offset(latest_entry);
        message.header.replica = self.replica;
        message.header.command = .prepare;

        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.header.checksum != self.request_checksum.?);

        log.debug("{}: on_request: prepare {}", .{ self.replica, message.header.checksum });

        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
        assert(self.prepare_timeout.ticking == false);

        self.prepare_message = self.message_bus.ref(message);
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
        self.view_jump(message.header);

        if (self.is_repair(message)) {
            log.debug("{}: on_prepare: ignoring (repair)", .{self.replica});
            self.on_repair(message);
            return;
        }

        if (self.status != .normal) {
            log.debug("{}: on_prepare: ignoring ({})", .{ self.replica, self.status });
            return;
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
            self.jump_to_newer_op_in_normal_status(message.header);
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

        // We have the latest op from the leader and have therefore cleared the view jump barrier:
        if (self.view_jump_barrier) {
            self.view_jump_barrier = false;
            log.notice("{}: on_prepare: cleared view jump barrier", .{self.replica});
        }

        // TODO Update client's information in the client table.

        self.replicate(message);
        self.append(message);

        if (self.follower()) {
            // A prepare may already be committed if requested by repair() so take the max:
            self.commit_ops_through(std.math.max(message.header.commit, self.commit_max));
        }
    }

    fn on_prepare_ok(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.warn("{}: on_prepare_ok: ignoring ({})", .{ self.replica, self.status });
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
        self.view_jump(message.header);

        if (self.status != .normal) {
            log.debug("{}: on_commit: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_commit: ignoring (older view)", .{self.replica});
            return;
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
            log.debug("{}: on_repair: ignoring (view started)", .{self.replica});
            return;
        }

        if (self.status == .view_change and self.leader_index(self.view) != self.replica) {
            log.debug("{}: on_repair: ignoring (view change, follower)", .{self.replica});
            return;
        }

        if (self.status == .view_change and !self.do_view_change_quorum) {
            log.debug("{}: on_repair: ignoring (view change, waiting for quorum)", .{self.replica});
            return;
        }

        assert(self.status == .normal or self.status == .view_change);
        assert(self.repairs_allowed());
        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op or message.header.view < self.view);

        if (self.journal.has_clean(message.header)) {
            log.debug("{}: on_repair: duplicate", .{self.replica});
            self.send_prepare_ok(message);
            return;
        }

        if (self.repair_header(message.header)) {
            assert(self.journal.has_dirty(message.header));
            assert(message.header.op <= self.op); // Repairs may never advance `self.op`.

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

        self.view_jump(message.header);

        assert(!self.view_jump_barrier);
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

        assert(!self.do_view_change_quorum);

        // When replica i receives start_view_change messages for its view from f other replicas,
        // it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
        // primary in the new view. Here v is its view, l is its log, v′ is the view number of the
        // latest view in which its status was normal, n is the op number, and k is the commit
        // number.
        const do_view_change = self.create_do_view_change_or_start_view_message(.do_view_change);
        defer self.message_bus.unref(do_view_change);

        assert(do_view_change.references == 1);
        assert(do_view_change.header.command == .do_view_change);
        assert(do_view_change.header.view == self.view);
        assert(do_view_change.header.op == self.op);
        assert(do_view_change.header.commit == self.commit_max);

        // TODO Assert that latest header in message body matches self.op.

        // TODO Resend after timeout:
        self.send_message_to_replica(self.leader_index(self.view), do_view_change);
    }

    fn on_do_view_change(self: *Replica, message: *Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(self.leader_index(message.header.view) == self.replica);

        self.view_jump(message.header);

        assert(!self.view_jump_barrier);
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
                self.set_latest_header(self.message_body_as_headers(m), &latest);
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
        // Ops may be rewound through a view change so we use `self.commit_max` and not `self.op`:
        assert(latest.op >= self.commit_max);
        assert(k >= latest.commit);
        assert(k >= self.commit_max);

        self.op = latest.op;
        self.commit_max = k;
        self.journal.set_entry_as_dirty(&latest);

        // Now that we have the latest op in place, repair any other headers from these messages:
        for (self.do_view_change_from_all_replicas) |received| {
            if (received) |m| {
                for (self.message_body_as_headers(m)) |*h| {
                    _ = self.repair_header(h);
                }
            }
        }

        // Start repairs according to the CTRL protocol:
        assert(self.op == latest.op);
        if (self.journal.entry_for_op_exact(self.op)) |entry| {
            // Verify that our latest entry was not overwritten by a bug in `repair_header()` above:
            assert(entry.checksum == latest.checksum);
            assert(entry.view == latest.view);
            assert(entry.op == latest.op);
            assert(entry.offset == latest.offset);

            assert(!self.do_view_change_quorum);
            self.do_view_change_quorum = true;

            self.repair_timeout.start();
            self.repair();
        } else {
            unreachable;
        }

        if (true) return;

        // TODO Ensure self.commit_max == self.op
        // Next prepare needs this to hold: assert(message.header.op == self.commit_max + 1);

        // The new primary starts accepting client requests. It also executes (in order) any
        // committed operations that it hadn’t executed previously, updates its client table, and
        // sends the replies to the clients.

        self.transition_to_normal_status(self.view);

        assert(self.status == .normal);
        assert(self.leader());

        self.commit_ops_through(self.commit_max);
        assert(self.commit_min == k);
        assert(self.commit_max == k);

        const start_view = self.create_do_view_change_or_start_view_message(.start_view);
        defer self.message_bus.unref(start_view);

        assert(start_view.references == 1);
        assert(start_view.header.command == .start_view);
        assert(start_view.header.view == self.view);
        assert(start_view.header.op == self.op);
        assert(start_view.header.commit == self.commit_max);

        self.send_message_to_other_replicas(start_view);
    }

    fn on_start_view(self: *Replica, message: *const Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(message.header.replica != self.replica);
        assert(message.header.replica == self.leader_index(message.header.view));

        self.view_jump(message.header);

        assert(!self.view_jump_barrier or self.status == .normal);
        assert(self.status == .view_change or self.view_jump_barrier);
        assert(message.header.view == self.view);

        // TODO Assert that start_view message matches what we expect if our journal is empty.

        // When other replicas receive the start_view message, they replace their log with the one
        // in the message, set their op number to that of the latest entry in the log, set their
        // view number to the view number in the message, change their status to normal, and update
        // the information in their client table. If there are non-committed operations in the log,
        // they send a ⟨prepare_ok v, n, i⟩ message to the primary; here n is the op-number. Then
        // they execute all operations known to be committed that they haven’t executed previously,
        // advance their commit number, and update the information in their client table.

        var latest: Header = std.mem.zeroInit(Header, .{});
        self.set_latest_header(self.message_body_as_headers(message), &latest);

        assert(latest.command == .prepare);
        assert(latest.op == message.header.op);
        assert(latest.commit <= message.header.commit);

        assert(latest.op >= self.commit_min);
        assert(latest.op >= self.commit_max);
        assert(latest.commit >= self.commit_min);
        assert(latest.commit >= self.commit_max);

        self.op = message.header.op;
        self.commit_max = message.header.commit;
        self.journal.set_entry_as_dirty(&latest);

        self.journal.remove_entries_from(self.op + 1);
        assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);

        // Now that we have the latest op in place, repair any other headers:
        for (self.message_body_as_headers(message)) |*h| {
            _ = self.repair_header(h);
        }

        if (self.view_jump_barrier) {
            assert(self.status == .normal);
            self.view_jump_barrier = false;
            log.notice("{}: on_start_view: resolved view jump barrier", .{self.replica});
        } else {
            assert(self.status == .view_change);
            self.transition_to_normal_status(message.header.view);
        }

        assert(!self.view_jump_barrier);
        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.follower());

        // TODO Send prepare_ok messages for uncommitted ops.

        self.commit_ops_through(self.commit_max);
    }

    fn on_request_start_view(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(message.header.replica != self.replica);
        assert(self.leader());

        const start_view = self.create_do_view_change_or_start_view_message(.start_view);
        defer self.message_bus.unref(start_view);

        assert(start_view.references == 1);
        assert(start_view.header.command == .start_view);
        assert(start_view.header.view == self.view);
        assert(start_view.header.op == self.op);
        assert(start_view.header.commit == self.commit_max);

        self.send_message_to_replica(message.header.replica, start_view);
    }

    fn on_request_prepare(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view == self.view);

        var nonce: ?u128 = message.header.nonce;
        if (self.leader_index(self.view) == self.replica and message.header.nonce == 0) {
            nonce = null;
        }

        if (self.sending_prepare) return;
        self.sending_prepare_frame = async self.send_prepare_to_replica(
            message.header.replica,
            message.header.op,
            nonce,
        );
    }

    fn on_request_headers(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view == self.view);

        const op_min = message.header.commit;
        const op_max = message.header.op;
        assert(op_max >= op_min);

        // We must add 1 because op_max and op_min are both inclusive:
        const count_max = @intCast(u32, std.math.min(64, op_max - op_min + 1));
        assert(count_max > 0);

        const size_max = @sizeOf(Header) + @sizeOf(Header) * count_max;

        const response = self.message_bus.create_message(size_max) catch unreachable;
        defer self.message_bus.unref(response);
        response.header.* = .{
            .command = .headers,
            // We echo the nonce back to the replica so that they can match up our response:
            .nonce = message.header.nonce,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
        };

        const count = self.journal.copy_latest_headers_between(
            op_min,
            op_max,
            std.mem.bytesAsSlice(Header, response.buffer[@sizeOf(Header)..size_max]),
        );

        response.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);
        const body = response.buffer[@sizeOf(Header)..response.header.size];

        response.header.set_checksum_body(body);
        response.header.set_checksum();

        self.send_message_to_replica(message.header.replica, response);
    }

    fn on_headers(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view == self.view);

        var op_min: ?u64 = null;
        var op_max: ?u64 = null;
        for (self.message_body_as_headers(message)) |*h| {
            if (op_min == null or h.op < op_min.?) op_min = h.op;
            if (op_max == null or h.op > op_max.?) op_max = h.op;
            _ = self.repair_header(h);
        }
        assert(op_max.? >= op_min.?);
    }

    fn on_prepare_timeout(self: *Replica) void {
        // TODO Exponential backoff.
        // TODO Prevent flooding the network due to multiple concurrent rounds of replication.
        self.prepare_timeout.reset();
        self.prepare_attempt += 1;

        assert(self.status == .normal);
        assert(self.leader());
        assert(self.request_checksum != null);
        assert(self.prepare_message != null);

        var message = self.prepare_message.?;
        assert(message.header.view == self.view);

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
        self.repair();
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
            assert(m.header.checksum_body == message.header.checksum_body);
            assert(m.header.checksum == message.header.checksum);
            log.debug("{}: on_{s}: ignoring (duplicate message)", .{
                self.replica,
                @tagName(message.header.command),
            });
            return null;
        }

        // Record the first receipt of this message:
        assert(messages[message.header.replica] == null);
        messages[message.header.replica] = self.message_bus.ref(message);

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

    /// Choose a different replica each time if possible (excluding ourself).
    /// The choice of replica is a deterministic function of:
    /// 1. `choose_any_other_replica_ticks`, and
    /// 2. whether the replica is connected and ready for sending in the MessageBus.
    fn choose_any_other_replica(self: *Replica) ?u16 {
        var count: usize = 0;
        while (count < self.configuration.len) : (count += 1) {
            self.choose_any_other_replica_ticks += 1;
            const replica = @mod(
                self.replica + self.choose_any_other_replica_ticks,
                self.configuration.len,
            );
            if (replica == self.replica) continue;
            // TODO if (!MessageBus.can_send_to_replica(replica)) continue;
            return @intCast(u16, replica);
        }
        return null;
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

        // If we know we have uncommitted ops that may have been reordered through a view change
        // then wait until the latest of these has been resolved with the leader:
        if (self.view_jump_barrier) {
            log.notice("{}: commit_ops_through: waiting to resolve view jump barrier", .{
                self.replica,
            });
            return;
        }

        // If we know we could validate the hash chain even further, then wait until we can:
        // This is partial defense-in-depth in case `self.op` is ever advanced by a reordered op.
        if (self.op < self.commit_max) {
            log.notice("{}: commit_ops_through: waiting for repair (op={} < commit={})", .{
                self.replica,
                self.op,
                self.commit_max,
            });
            return;
        }

        // We must validate the hash chain as far as possible, since `self.op` may disclose a fork:
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
            assert(entry.valid_checksum());
            assert(entry.op == self.commit_min);
            assert(entry.operation != .init);

            log.debug("{}: commit_ops_through: executing op={} checksum={} ({s})", .{
                self.replica,
                entry.op,
                entry.checksum,
                @tagName(entry.operation),
            });

            // TODO Refactor this loop block into a separate method.
            // TODO Then pass the buffer in, having read it and validated checksum already.
            // This will also enable the leader to commit with the prepare buffer it has in memory
            // without having to hit the disk again.
            self.journal.read_sectors(
                self.commit_buffer[0..@intCast(u32, Journal.sector_ceil(entry.size))],
                self.journal.offset_in_circular_buffer(entry.offset),
            );

            // TODO See if entry body has a valid checksum before we increment self.commit_min:
            // We cannot fail after we have incremented self.commit_min.
            const entry_body = self.commit_buffer[@sizeOf(Header)..entry.size];
            assert(entry.valid_checksum_body(entry_body));

            const reply = self.message_bus.create_message(conf.response_size_max) catch unreachable;
            defer self.message_bus.unref(reply);

            var reply_body_size = @intCast(u32, self.state_machine.commit(
                entry.operation,
                entry_body,
                reply.buffer[@sizeOf(Header)..],
            ));

            reply.header.command = .reply;
            reply.header.operation = entry.operation;
            reply.header.nonce = entry.checksum;
            reply.header.client = entry.client;
            reply.header.request = entry.request;
            reply.header.cluster = self.cluster;
            reply.header.replica = self.replica;
            reply.header.view = self.view;
            reply.header.op = self.op;
            reply.header.size = @sizeOf(Header) + reply_body_size;

            reply.header.set_checksum_body(reply.buffer[@sizeOf(Header)..reply.header.size]);
            reply.header.set_checksum();

            // TODO Add reply to the client table to answer future duplicate requests idempotently.
            // Lookup client table entry using client id.
            // If client's last request id is <= this request id, then update client table entry.
            // Otherwise the client is already ahead of us, and we don't need to update the entry.

            if (self.leader()) {
                log.debug("{}: commit_ops_through: sending reply to client: {}", .{
                    self.replica,
                    reply.header,
                });
                self.message_bus.send_message_to_client(reply.header.client, reply);
            }
        }
    }

    /// The returned message has exactly 1 reference.
    fn create_do_view_change_or_start_view_message(self: *Replica, command: Command) *Message {
        assert(command == .do_view_change or command == .start_view);

        // We may also send a start_view message in normal status to resolve a follower's view jump:
        assert(self.status == .normal or self.status == .view_change);

        const size_max = @sizeOf(Header) * 8;

        const message = self.message_bus.create_message(size_max) catch unreachable;
        message.header.* = .{
            .command = command,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit_max,
        };

        var dest = std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..size_max]);
        const count = self.journal.copy_latest_headers_between(0, self.op, dest);
        assert(count > 0);

        message.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);
        const body = message.buffer[@sizeOf(Header)..message.header.size];

        message.header.set_checksum_body(body);
        message.header.set_checksum();

        return message;
    }

    fn discard_repair_queue(self: *Replica) void {
        while (self.repair_queue) |message| {
            log.notice("{}: discard_repair_queue: op={}", .{ self.replica, message.header.op });
            assert(self.repair_queue_len > 0);
            self.repair_queue = message.next;
            self.repair_queue_len -= 1;

            message.next = null;
            self.message_bus.unref(message);
        }
        assert(self.repair_queue_len == 0);
    }

    fn ignore_repair_message(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .request_start_view or
            message.header.command == .request_headers or
            message.header.command == .headers or
            message.header.command == .request_prepare);

        const command: []const u8 = @tagName(message.header.command);

        if (self.status != .normal and self.status != .view_change) {
            log.debug("{}: on_{s}: ignoring ({})", .{ self.replica, command, self.status });
            return true;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
            return true;
        }

        // We should never view jump unless we know what our status should be after the jump:
        // Otherwise we may be normal before the leader, or in a view change that has completed.
        // Since we do not know the status of the other replica, we ignore and do not jump.
        if (message.header.view > self.view) {
            log.debug("{}: on_{s}: ignoring (newer view)", .{ self.replica, command });
            return true;
        }

        if (self.status == .view_change) {
            switch (message.header.command) {
                .request_start_view => {
                    log.debug("{}: on_{s}: ignoring (view change)", .{ self.replica, command });
                    return true;
                },
                .request_headers, .request_prepare => {
                    if (self.leader_index(self.view) != message.header.replica) {
                        log.debug("{}: on_{s}: ignoring (view change, requested by follower)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                .headers => {
                    if (self.leader_index(self.view) != self.replica) {
                        log.debug("{}: on_{s}: ignoring (view change, received by follower)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    } else if (!self.do_view_change_quorum) {
                        log.debug("{}: on_{s}: ignoring (view change, waiting for quorum)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                else => unreachable,
            }
        }

        if (message.header.replica == self.replica) {
            log.warn("{}: on_{s}: ignoring (self)", .{ self.replica, command });
            return true;
        }

        if (message.header.command == .request_start_view) {
            if (self.leader_index(self.view) != self.replica) {
                log.debug("{}: on_{s}: ignoring (follower)", .{ self.replica, command });
                return true;
            }
        }

        if (message.header.command == .request_prepare) {
            // Only the leader may answer a request for a prepare that does not specify the nonce:
            if (message.header.nonce == 0 and self.leader_index(self.view) != self.replica) {
                log.warn("{}: on_{s}: ignoring (nonce=0, follower)", .{ self.replica, command });
                return true;
            }
        }

        // Only allow repairs for same view as defense-in-depth:
        assert(message.header.view == self.view);
        return false;
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
            if (message.header.command != .start_view or !self.view_jump_barrier) {
                log.debug("{}: on_{s}: ignoring (view started)", .{ self.replica, command });
                return true;
            }
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
            // The view has already started or is newer.
        }

        return false;
    }

    /// Advances `op` to where we need to be before `header` can be processed as a prepare:
    fn jump_to_newer_op_in_normal_status(self: *Replica, header: *const Header) void {
        assert(self.status == .normal);
        assert(self.follower());
        assert(header.view == self.view);
        assert(header.op > self.op + 1);
        // We may have learned of a higher `commit_max` through a commit message before jumping to a
        // newer op that is less than `commit_max` but greater than `commit_min`:
        assert(header.op > self.commit_min);

        log.debug("{}: jump_to_newer_op: advancing: op={}..{} checksum={}..{}", .{
            self.replica,
            self.op,
            header.op - 1,
            self.journal.entry_for_op_exact(self.op).?.checksum,
            header.nonce,
        });

        self.op = header.op - 1;
        assert(self.op >= self.commit_min);
        assert(self.op + 1 == header.op);
    }

    fn message_body_as_headers(self: *Replica, message: *const Message) []Header {
        // TODO Assert message commands that we expect this to be called for.
        assert(message.header.size > @sizeOf(Header)); // Body must contain at least one header.
        return std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..message.header.size]);
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
        assert(a.cluster == b.cluster);
        if (a.view == b.view and a.op + 1 == b.op and a.checksum != b.nonce) {
            assert(a.valid_checksum());
            assert(b.valid_checksum());
            log.emerg("{}: panic_if_hash_chain_would_break: a: {}", .{ self.replica, a });
            log.emerg("{}: panic_if_hash_chain_would_break: b: {}", .{ self.replica, b });
            @panic("hash chain would break");
        }
    }

    /// Starting from the latest journal entry, backfill any missing or disconnected headers.
    /// A header is disconnected if it breaks the hash chain with its newer neighbor to the right.
    /// Since we work backwards from the latest entry, we should always be able to fix the chain.
    /// Once headers are connected, backfill any dirty or faulty prepares.
    fn repair(self: *Replica) void {
        self.repair_timeout.reset();

        assert(self.status == .normal or self.status == .view_change);
        assert(self.repairs_allowed());
        assert(self.commit_min <= self.op);
        assert(self.commit_min <= self.commit_max);

        // TODO Handle case where we are requesting reordered headers that no longer exist.

        // We expect these always to exist:
        assert(self.journal.entry_for_op_exact(self.commit_min) != null);
        assert(self.journal.entry_for_op_exact(self.op) != null);

        // Resolve any view jump by requesting the leader's latest op:
        if (self.view_jump_barrier) {
            assert(self.status == .normal);
            assert(self.follower());
            log.notice("{}: repair: resolving view jump barrier", .{self.replica});
            self.send_header_to_replica(self.leader_index(self.view), .{
                .command = .request_start_view,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            });
            return;
        }

        // Request outstanding committed prepares to advance our op number:
        // This handles the case of an idle cluster, where a follower will not otherwise advance.
        // This is not required for correctness, but for durability.
        if (self.op < self.commit_max) {
            // If the leader repairs during a view change, it will have already advanced `self.op`
            // to the latest op according to the quorum of `do_view_change` messages received, so we
            // must therefore be in normal status:
            assert(self.status == .normal);
            assert(self.follower());
            log.notice("{}: repair: op={} < commit_max={}", .{
                self.replica,
                self.op,
                self.commit_max,
            });
            // We need to advance our op number and therefore have to `request_prepare`,
            // since only `on_prepare()` can do this, not `repair_header()` in `on_headers()`.
            // TODO Explore danger scenarios if we were to request from a follower instead.
            self.send_header_to_replica(self.leader_index(self.view), .{
                .command = .request_prepare,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .op = self.commit_max,
                // We cannot yet know the nonce so we set it to 0:
                // The nonce is optional when requesting from the leader but required otherwise.
                .nonce = 0,
            });
            return;
        }

        // Request any missing or disconnected headers:
        // TODO Snapshots: Ensure that self.commit_min op always exists in the journal.
        var broken = self.journal.find_latest_headers_break_between(self.commit_min, self.op);
        if (broken) |range| {
            log.notice("{}: repair: latest break: {}", .{ self.replica, range });
            assert(range.op_min > self.commit_min);
            assert(range.op_max < self.op);
            // A range of `op_min=0` or `op_max=0` should be impossible as a header break:
            // This is the init op that is prepared when the cluster is initialized.
            assert(range.op_min > 0);
            assert(range.op_max > 0);
            if (self.choose_any_other_replica()) |replica| {
                self.send_header_to_replica(replica, .{
                    .command = .request_headers,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .view = self.view,
                    .commit = range.op_min,
                    .op = range.op_max,
                });
            }
            return;
        }

        // Assert that all headers are now present and connected with a perfect hash chain:
        assert(!self.view_jump_barrier);
        assert(self.op >= self.commit_max);
        assert(self.valid_hash_chain_between(self.commit_min, self.op));

        // Request the latest dirty or faulty prepare:
        var op = self.op;
        while (op > self.commit_min) : (op -= 1) {
            if (self.journal.dirty[op]) {
                log.debug("we need to request_prepare for op={}", .{op});
                return;
            }
        }
    }

    fn repair_dirty(self: *Replica) void {
        // TODO Add a flag to avoid scanning all dirty bits.
        // TODO Avoid requesting data if we have already just requested it.
        // TODO Avoid requesting data if we are busy writing it (writes could take 10 seconds).
    }

    fn repair_header(self: *Replica, header: *const Header) bool {
        assert(self.status == .normal or self.status == .view_change);
        assert(header.command == .prepare);
        assert(header.size >= @sizeOf(Header));

        if (self.status == .normal) {
            assert(header.view <= self.view);
        } else if (self.status == .view_change) {
            assert(header.view < self.view);
        }

        if (header.op >= self.op) {
            // A repair may never advance or replace `self.op` (critical for correctness):
            //
            // Repairs must always backfill in behind `self.op` but may never advance `self.op`.
            // Otherwise, a split-brain leader may reapply an op that was removed through a view
            // change, which could be committed by a higher `commit_max` number in a commit message.
            //
            // See this commit message for an example:
            // https://github.com/coilhq/tigerbeetle/commit/6119c7f759f924d09c088422d5c60ac6334d03de
            //
            // Our guiding principles around repairs in general:
            //
            // * Our latest op makes sense of everything else and must not be replaced or advanced
            // except by the leader in the current view.
            //
            // * Do not jump to a view in normal status without imposing a view jump barrier.
            //
            // * Do not commit before resolving the view jump barrier with the leader.
            //
            // * Do not commit until the hash chain between `self.commit_min` and `self.op` is
            // fully connected, to ensure that all the ops in this range are correct.
            //
            // * Ensure that `self.commit_max` is never advanced for a newer view without first
            // imposing a view jump barrier, otherwise `self.commit_max` may refer to different ops.
            //
            // * Ensure that `self.op` is never advanced by a repair since repairs may occur in a
            // view change where the view has not started.
            log.debug("{}: repair_header: ignoring (would replace or advance op)", .{self.replica});
            return false;
        }

        // See Figure 3.7 on page 41 in Diego Ongaro's Raft thesis for an example of where an op
        // with an older view number may be committed instead of an op with a newer view number:
        // http://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
        //
        // We therefore only compare ops in the same view or with reference to our hash chain:

        if (self.journal.entry(header)) |existing| {
            // Do not replace any existing op lightly as doing so may impair durability and even
            // violate correctness by undoing a prepare already acknowledged to the leader:
            if (existing.checksum == header.checksum) {
                if (self.journal.dirty[header.op]) {
                    // We may safely replace this existing op (with hash chain and overlap caveats):
                    log.debug("{}: repair_header: exists (dirty checksum)", .{self.replica});
                } else {
                    log.debug("{}: repair_header: ignoring (clean checksum)", .{self.replica});
                    return false;
                }
            } else if (existing.view == header.view) {
                // We expect that the same view and op must have the same checksum:
                assert(existing.op != header.op);

                // The journal must have wrapped:
                if (existing.op < header.op) {
                    // We may safely replace this existing op (with hash chain and overlap caveats):
                    log.debug("{}: repair_header: exists (same view, older op)", .{self.replica});
                } else if (existing.op > header.op) {
                    log.debug("{}: repair_header: ignoring (same view, newer op)", .{self.replica});
                    return false;
                } else {
                    unreachable;
                }
            } else {
                assert(existing.view != header.view);
                assert(existing.op == header.op or existing.op != header.op);

                if (self.repair_header_would_connect_hash_chain(header)) {
                    // We may safely replace this existing op:
                    log.debug("{}: repair_header: exists (hash chain break)", .{self.replica});
                } else {
                    // We cannot replace this existing op until we are sure that doing so would not
                    // violate any prior commitments made to the leader.
                    log.debug("{}: repair_header: ignoring (hash chain doubt)", .{self.replica});
                    return false;
                }
            }
        } else {
            // We may repair the gap (with hash chain and overlap caveats):
            log.debug("{}: repair_header: gap", .{self.replica});
        }

        // Caveat: Do not repair an existing op or gap if doing so would break the hash chain:
        if (self.repair_header_would_break_hash_chain_with_next_entry(header)) {
            log.debug("{}: repair_header: ignoring (would break hash chain with next entry)", .{
                self.replica,
            });
            return false;
        }

        // Caveat: Do not repair an existing op or gap if doing so would overlap another:
        if (self.repair_header_would_overlap_another(header)) {
            if (self.repair_header_would_connect_hash_chain(header)) {
                // We may overlap previous entries in order to connect the hash chain:
                log.debug("{}: repair_header: overlap (would connect hash chain)", .{self.replica});
            } else {
                log.debug("{}: repair_header: ignoring (would overlap another)", .{self.replica});
                return false;
            }
        }

        // TODO Snapshots: Skip if this header is already snapshotted.

        assert(header.op < self.op);
        self.journal.set_entry_as_dirty(header);
        return true;
    }

    /// If we repair this header, then would this break the hash chain only to our immediate right?
    /// This offers a weak guarantee compared to `repair_header_would_connect_hash_chain()` below.
    /// However, this is useful for allowing repairs when the hash chain is sparse.
    fn repair_header_would_break_hash_chain_with_next_entry(
        self: *Replica,
        header: *const Header,
    ) bool {
        if (self.journal.previous_entry(header)) |previous| {
            self.panic_if_hash_chain_would_break_in_the_same_view(previous, header);
        }

        if (self.journal.next_entry(header)) |next| {
            self.panic_if_hash_chain_would_break_in_the_same_view(header, next);

            if (header.checksum == next.nonce) {
                assert(header.view <= next.view);
                assert(header.op + 1 == next.op);
                // We don't break with `next` but this is no guarantee that `next` does not break.
                return false;
            } else {
                // If the journal has wrapped, then err in favor of a break regardless of op order:
                return true;
            }
        }

        // We are not completely sure since there is no entry to the immediate right:
        return false;
    }

    /// If we repair this header, then would this connect the hash chain through to the latest op?
    /// This offers a strong guarantee that may be used to replace or overlap an existing op.
    ///
    /// Here is an example of what could go wrong if we did not check for complete connection:
    ///
    /// 1. We do a prepare that's going to be committed.
    /// 2. We do a stale prepare to the right of this, ignoring the hash chain break to the left.
    /// 3. We do another stale prepare that replaces the first op because it connects to the second.
    ///
    /// This would violate our quorum replication commitment to the leader.
    /// The mistake in this example was not that we ignored the break to the left, which we must do
    /// to repair reordered ops, but that we did not check for complete connection to the right.
    fn repair_header_would_connect_hash_chain(self: *Replica, header: *const Header) bool {
        var entry = header;
        assert(entry.op < self.op);

        while (entry.op < self.op) {
            if (self.journal.next_entry(entry)) |next| {
                if (entry.checksum == next.nonce) {
                    assert(entry.view <= next.view);
                    assert(entry.op + 1 == next.op);
                    entry = next;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        assert(entry.op == self.op);
        assert(entry.checksum == self.journal.entry_for_op_exact(self.op).?.checksum);
        return true;
    }

    /// If we repair this header, then would this overlap and overwrite part of another batch?
    /// Journal entries have variable-sized batches that may overlap if entries are disconnected.
    fn repair_header_would_overlap_another(self: *Replica, header: *const Header) bool {
        // TODO Snapshots: Handle journal wrap around.
        {
            // Look behind this entry for any preceeding entry that this would overlap:
            var op: u64 = header.op;
            while (op > 0) {
                op -= 1;
                if (self.journal.entry_for_op(op)) |neighbor| {
                    if (self.journal.next_offset(neighbor) > header.offset) return true;
                    break;
                }
            }
        }
        {
            // Look beyond this entry for any succeeding entry that this would overlap:
            var op: u64 = header.op + 1;
            while (op <= self.op) : (op += 1) {
                if (self.journal.entry_for_op(op)) |neighbor| {
                    if (self.journal.next_offset(header) > neighbor.offset) return true;
                    break;
                }
            }
        }
        return false;
    }

    fn repair_last_queued_message_if_any(self: *Replica) void {
        if (self.status != .normal and self.status != .view_change) return;
        if (!self.repairs_allowed()) return;

        while (!self.repairing) {
            if (self.repair_queue) |message| {
                assert(self.repair_queue_len > 0);
                self.repair_queue = message.next;
                self.repair_queue_len -= 1;

                message.next = null;
                self.on_repair(message);
                assert(self.repair_queue != message); // Catch an accidental requeue by on_repair().
                self.message_bus.unref(message);
            } else {
                assert(self.repair_queue_len == 0);
                break;
            }
        }
    }

    fn repair_later(self: *Replica, message: *Message) void {
        assert(self.repairs_allowed());
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

        message.next = self.repair_queue;
        self.repair_queue = self.message_bus.ref(message);
        self.repair_queue_len += 1;
    }

    fn repairs_allowed(self: *Replica) bool {
        switch (self.status) {
            .view_change => {
                if (self.do_view_change_quorum) {
                    assert(self.leader_index(self.view) == self.replica);
                    return true;
                } else {
                    return false;
                }
            },
            .normal => return true,
            else => return false,
        }
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
            log.debug("{}: replicate: not replicating (committed)", .{self.replica});
            return;
        }

        const next = @mod(self.replica + 1, @intCast(u16, self.configuration.len));
        if (next == self.leader_index(message.header.view)) {
            log.debug("{}: replicate: not replicating (completed)", .{self.replica});
            return;
        }

        log.debug("{}: replicate: replicating to replica {}", .{ self.replica, next });
        self.send_message_to_replica(next, message);
    }

    fn reset_prepare(self: *Replica) void {
        if (self.prepare_message) |message| {
            self.request_checksum = null;
            self.message_bus.unref(message);
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
                self.message_bus.unref(message);
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

    fn send_prepare_to_replica(self: *Replica, replica: u16, op: u64, nonce: ?u128) void {
        assert(self.status == .normal or self.status == .view_change);

        if (op > self.op) {
            log.debug("{}: send_prepare_to_replica: op={} > self.op={}", .{
                self.replica,
                op,
                self.op,
            });
            return;
        }

        const optional_entry = self.journal.entry_for_op_exact(op);
        if (optional_entry == null) {
            log.debug("{}: send_prepare_to_replica: op={} missing", .{ self.replica, op });
            return;
        }

        const entry = optional_entry.?;

        if (nonce) |checksum| {
            if (entry.checksum != checksum) {
                log.debug("{}: send_prepare_to_replica: checksum={} != nonce={}", .{
                    self.replica,
                    entry.checksum,
                    checksum,
                });
                return;
            }
        }

        if (!self.journal.has_clean(entry)) {
            log.debug("{}: send_prepare_to_replica: op={} dirty", .{ self.replica, op });
            return;
        }

        assert(!self.sending_prepare);
        self.sending_prepare = true;
        defer self.sending_prepare = false;

        const size = @intCast(u32, Journal.sector_ceil(entry.size));
        assert(size >= entry.size);

        var message = self.message_bus.create_message(size) catch unreachable;
        defer self.message_bus.unref(message);

        assert(message.header.offset + size <= self.journal.size_circular_buffer);
        self.journal.read_sectors(
            message.buffer[0..size],
            self.journal.offset_in_circular_buffer(entry.offset),
        );

        if (message.header.op != op) {
            log.warn("{}: send_prepare_to_replica: op={} changed", .{ self.replica, op });
            return;
        }

        self.send_message_to_replica(replica, message);
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

    fn send_message_to_other_replicas(self: *Replica, message: *Message) void {
        for (self.configuration) |_, replica| {
            if (replica != self.replica) {
                self.send_message_to_replica(@intCast(u16, replica), message);
            }
        }
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
                // We do not assert message.header.replica as we would for send_header_to_replica()
                // because we typically forward messages sent by another replica (i.e. the leader).
                assert(self.status == .normal or self.status == .view_change);
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

    fn set_latest_header(self: *Replica, headers: []Header, latest: *Header) void {
        switch (latest.command) {
            .reserved, .prepare => assert(latest.valid_checksum()),
            else => unreachable,
        }

        for (headers) |header| {
            assert(header.command == .prepare);
            assert(header.valid_checksum());

            if (latest.command == .reserved) {
                latest.* = header;
            } else if (header.view > latest.view) {
                latest.* = header;
            } else if (header.view == latest.view and header.op > latest.op) {
                latest.* = header;
            }
        }
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
            self.repair_timeout.start();
        } else {
            log.debug("{}: transition_to_normal_status: follower", .{self.replica});

            self.commit_timeout.stop();
            self.normal_timeout.start();
            self.view_change_timeout.stop();
            self.repair_timeout.start();
        }

        // This is essential for correctness:
        self.reset_prepare();

        // Reset and garbage collect all view change messages (if any):
        // This is not essential for correctness, only efficiency.
        // We just don't want to tie them up until the next view change (when they must be reset):
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);

        self.do_view_change_quorum = false;
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
        self.repair_timeout.stop();

        self.reset_prepare();

        // Some VR implementations reset their counters only on entering a view, perhaps assuming
        // the view will be followed only by a single subsequent view change to the next view.
        // However, multiple successive view changes can fail, e.g. after a view change timeout.
        // We must therefore reset our counters here to avoid counting messages from an older view,
        // which would violate the quorum intersection property essential for correctness.
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);

        self.do_view_change_quorum = false;

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

    fn view_jump(self: *Replica, header: *const Header) void {
        const to_status: Status = switch (header.command) {
            .prepare, .commit => .normal,
            .start_view_change, .do_view_change, .start_view => .view_change,
            else => unreachable,
        };

        if (self.status != .normal and self.status != .view_change) return;

        // If this is for an older view, then ignore:
        if (header.view < self.view) return;

        // Compare status transitions and decide whether to view jump or ignore:
        switch (self.status) {
            .normal => switch (to_status) {
                // If the transition is to `.normal`, then ignore if this is for the same view:
                .normal => if (header.view == self.view) return,
                // If the transition is to `.view_change`, then ignore if the view has started:
                .view_change => if (header.view == self.view) return,
                else => unreachable,
            },
            .view_change => switch (to_status) {
                // This is an interesting special case:
                // If the transition is to `.normal` in the same view, then we missed the
                // `start_view` message and we must also consider this a view jump:
                // If we don't view jump here, then our `view_change_timeout` will fire and we will
                // disrupt the cluster by starting another view change for a newer view.
                .normal => {},
                // If the transition is to `.view_change`, then ignore if this is for the same view:
                .view_change => if (header.view == self.view) return,
                else => unreachable,
            },
            else => unreachable,
        }

        if (to_status == .normal) {
            assert(header.view >= self.view);

            const command = @tagName(header.command);
            if (header.view == self.view) {
                assert(self.status == .view_change and to_status == .normal);
                log.debug("{}: view_jump: exiting view change and starting view", .{self.replica});
            } else {
                log.debug("{}: view_jump: jumping to newer view", .{self.replica});
            }

            if (self.op > self.commit_max) {
                // We have uncommitted ops, and these may have been removed or replaced by the new
                // leader through a view change in which we were not involved.
                //
                // In Section 5.2, the VR paper simply removes these uncommitted ops and does a
                // state transfer. However, while strictly safe, this impairs safety in terms of
                // durability, and adds unnecessary repair overhead if the ops were committed.
                //
                // We rather impose a view jump barrier to keep `commit_ops_through()` from
                // committing. This preserves and maximizes durability and minimizes repair traffic.
                //
                // This view jump barrier is cleared or may be resolved, respectively, as soon as:
                // 1. we receive a new prepare from the leader that advances our latest op, or
                // 2. we request and receive a `start_view` message from the leader for this view.
                //
                // This is safe because advancing our latest op in the current view or receiving the
                // latest op from the leader both ensure that we have the latest hash chain head.
                log.notice("{}: view_jump: imposing view jump barrier", .{self.replica});
                self.view_jump_barrier = true;
            } else {
                assert(self.op == self.commit_max);

                // We may still need to resolve any prior view jump barrier:
                // For example, if we jump to view 3 and jump again to view 7 both in normal status.
                assert(self.view_jump_barrier == true or self.view_jump_barrier == false);
            }
        } else if (to_status == .view_change) {
            assert(header.view > self.view);

            // The view change will set the latest op in on_do_view_change() or on_start_view():
            // There is no need to impose a view jump barrier and any existing barrier is cleared.
            // We only need to transition to view change status.
            if (self.view_jump_barrier) {
                log.notice("{}: view_jump: clearing view jump barrier", .{self.replica});
                self.view_jump_barrier = false;
            }
        } else {
            unreachable;
        }

        switch (to_status) {
            .normal => self.transition_to_normal_status(header.view),
            .view_change => self.transition_to_view_change_status(header.view),
            else => unreachable,
        }
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

        if (self.journal.has_dirty(message.header)) {
            self.journal.write(message);
        } else {
            log.debug("{}: write_to_journal: skipping (clean)", .{self.replica});
        }

        self.send_prepare_ok(message);
    }
};
