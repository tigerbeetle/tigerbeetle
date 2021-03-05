const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);
pub const log_level: std.log.Level = .debug;

usingnamespace @import("concurrent_ranges.zig");
/// Viewstamped Replication protocol commands:
// TODO Command for client to fetch its latest request_number from the cluster.
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
};

/// State machine operations:
pub const Operation = packed enum(u8) {
    reserved,

    noop,
};

/// Network message and journal entry header:
/// We reuse the same header for both so that prepare messages from the leader can simply be
/// journalled as is by the followers without requiring any further modification.
pub const Header = packed struct {
    /// A checksum covering only the rest of this header (but including checksum_data):
    /// This enables the header to be trusted without having to recv() or read() associated data.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum_meta: u128 = 0,

    /// A checksum covering only associated data.
    checksum_data: u128 = 0,

    /// The checksum_meta of the message to which this message refers, or a unique recovery nonce:
    /// We use this nonce in various ways, for example:
    /// * A prepare sets nonce to the checksum_meta of the prior prepare to hash-chain the journal.
    /// * A prepare_ok sets nonce to the checksum_meta of the prepare it wants to ack.
    /// * A commit sets nonce to the checksum_meta of the latest committed op.
    /// This adds an additional cryptographic safety control beyond VR's op and commit numbers.
    nonce: u128 = 0,

    /// Each client records its own client_id and a current request_number. A client is allowed to
    /// have just one outstanding request at a time. Each request is given a number by the client
    /// and later requests must have larger numbers than earlier ones. The request_number is used
    /// by the replicas to avoid running requests more than once; it is also used by the client to
    /// discard duplicate responses to its requests.
    client_id: u128 = 0,
    request_number: u64 = 0,

    /// Every message sent from one replica to another contains the sending replica's current view:
    view: u64 = 0,

    /// The latest view for which the replica's status was normal:
    latest_normal_view: u64 = 0,

    /// The op number:
    op: u64 = 0,

    /// The commit number:
    commit: u64 = 0,

    /// The journal offset to which this message relates:
    /// This enables direct access to an entry, without requiring previous variable-length entries.
    offset: u64 = 0,

    /// The journal index to which this message relates:
    /// Similarly, this enables direct access to an entry's header in the journal's header array.
    index: u32 = 0,

    /// The size of this message header and any associated data:
    size: u32 = @sizeOf(Header),

    /// The cluster reconfiguration epoch number (for future use):
    epoch: u32 = 0,

    /// The index of the replica that sent this message:
    replica: u16 = 0,

    /// The VR protocol command for this message:
    command: Command,

    /// The state machine operation to apply:
    operation: Operation = .reserved,

    pub fn calculate_checksum_meta(self: *const Header) u128 {
        // Reserved headers should be completely zeroed with a checksum_meta also of 0:
        if (self.command == .reserved) {
            var sum: u128 = 0;
            for (std.mem.asBytes(self)) |byte| sum += byte;
            if (sum == 0) return 0;
        }

        const checksum_size = @sizeOf(@TypeOf(self.checksum_meta));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(std.mem.asBytes(self)[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_data(self: *const Header, data: []const u8) u128 {
        // Reserved headers should be completely zeroed with a checksum_data also of 0:
        if (self.command == .reserved and self.size == 0 and data.len == 0) return 0;

        assert(self.size == @sizeOf(Header) + data.len);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_data));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(data[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    /// This must be called only after set_checksum_data() so that checksum_data is also covered:
    pub fn set_checksum_meta(self: *Header) void {
        self.checksum_meta = self.calculate_checksum_meta();
    }

    pub fn set_checksum_data(self: *Header, data: []const u8) void {
        self.checksum_data = self.calculate_checksum_data(data);
    }

    pub fn valid_checksum_meta(self: *const Header) bool {
        return self.checksum_meta == self.calculate_checksum_meta();
    }

    pub fn valid_checksum_data(self: *const Header, data: []const u8) bool {
        return self.checksum_data == self.calculate_checksum_data(data);
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum_meta is valid, and expects that has already been done.
    pub fn bad(self: *const Header) ?[]const u8 {
        switch (self.command) {
            .reserved => {
                if (self.checksum_meta != 0) return "checksum_meta != 0";
                if (self.checksum_data != 0) return "checksum_data != 0";
                if (self.nonce != 0) return "nonce != 0";
                if (self.client_id != 0) return "client_id != 0";
                if (self.request_number != 0) return "request_number != 0";
                if (self.view != 0) return "view != 0";
                if (self.latest_normal_view != 0) return "latest_normal_view != 0";
                if (self.op != 0) return "op != 0";
                if (self.commit != 0) return "commit != 0";
                if (self.offset != 0) return "offset != 0";
                if (self.index != 0) return "index != 0";
                if (self.size != 0) return "size != 0";
                if (self.epoch != 0) return "epoch != 0";
                if (self.replica != 0) return "replica != 0";
                if (self.operation != .reserved) return "operation != .reserved";
            },
            .request => {
                if (self.nonce != 0) return "nonce != 0";
                if (self.client_id == 0) return "client_id == 0";
                if (self.request_number == 0) return "request_number == 0";
                if (self.view != 0) return "view != 0";
                if (self.latest_normal_view != 0) return "latest_normal_view != 0";
                if (self.op != 0) return "op != 0";
                if (self.commit != 0) return "commit != 0";
                if (self.offset != 0) return "offset != 0";
                if (self.index != 0) return "index != 0";
                if (self.epoch != 0) return "epoch != 0";
                if (self.replica != 0) return "replica != 0";
                if (self.operation == .reserved) return "operation == .reserved";
            },
            .prepare, .prepare_ok => {
                if (self.client_id == 0) return "client_id == 0";
                if (self.request_number == 0) return "request_number == 0";
                if (self.latest_normal_view != 0) return "latest_normal_view != 0";
                if (self.op == 0) return "op == 0";
                if (self.op <= self.commit) return "op <= commit";
                if (self.epoch != 0) return "epoch != 0";
                if (self.operation == .reserved) return "operation == .reserved";
            },
            else => {}, // TODO Add validators for all commands.
        }
        return null;
    }

    pub fn zero(self: *Header) void {
        std.mem.set(u8, std.mem.asBytes(self), 0);

        assert(self.checksum_meta == 0);
        assert(self.checksum_data == 0);
        assert(self.size == 0);
        assert(self.command == .reserved);
        assert(self.operation == .reserved);
    }
};

const Client = struct {};

const ConfigurationAddress = union(enum) {
    address: *std.net.Address,
    replica: *Replica,
};

pub const Message = struct {
    header: *Header,
    buffer: []u8 align(sector_size),
    references: usize = 0,
    next: ?*Message = null,
};

pub const MessageBus = struct {
    allocator: *Allocator,
    allocated: usize = 0,
    configuration: []ConfigurationAddress,

    /// A linked list of messages that are ready to send (FIFO):
    head: ?*Envelope = null,
    tail: ?*Envelope = null,

    const Address = union(enum) {
        replica: *Replica,
    };

    const Envelope = struct {
        address: Address,
        message: *Message,
        next: ?*Envelope,
    };

    pub fn init(allocator: *Allocator, configuration: []ConfigurationAddress) !MessageBus {
        var self = MessageBus{
            .allocator = allocator,
            .configuration = configuration,
        };
        return self;
    }

    pub fn deinit(self: *MessageBus) void {}

    /// TODO Detect if gc() called multiple times for message.references == 0.
    pub fn gc(self: *MessageBus, message: *Message) void {
        if (message.references == 0) {
            log.debug("message_bus: freeing {}", .{message.header});
            self.destroy_message(message);
        }
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u16, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const data = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum_meta depends on checksum_data:
        message.header.set_checksum_data(data);
        message.header.set_checksum_meta();

        assert(message.references == 0);
        self.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(self: *MessageBus, replica: u16, message: *Message) void {
        message.references += 1;

        // TODO Pre-allocate envelopes at startup.
        var envelope = self.allocator.create(Envelope) catch unreachable;
        envelope.* = .{
            .address = .{ .replica = self.configuration[replica].replica },
            .message = message,
            .next = null,
        };
        self.enqueue_message(envelope);
    }

    pub fn send_queued_messages(self: *MessageBus) void {
        while (self.head != null) {
            // Loop on a copy of the linked list, having reset the linked list first, so that any
            // synchronous append in on_message() is executed the next iteration of the outer loop.
            // This is not critical for safety here, but see run() in src/io.zig where it is.
            var head = self.head;
            self.head = null;
            self.tail = null;
            while (head) |envelope| {
                head = envelope.next;
                envelope.next = null;
                assert(envelope.message.references > 0);
                switch (envelope.address) {
                    .replica => |r| r.on_message(envelope.message),
                }
                envelope.message.references -= 1;
                self.gc(envelope.message);
                // The lifetime of an envelope will often be shorter than that of the message.
                self.allocator.destroy(envelope);
            }
        }
    }

    fn create_message(self: *MessageBus, size: u32) !*Message {
        assert(size >= @sizeOf(Header));

        var buffer = try self.allocator.allocAdvanced(u8, sector_size, size, .exact);
        errdefer self.allocator.free(buffer);
        std.mem.set(u8, buffer, 0);

        var message = try self.allocator.create(Message);
        errdefer self.allocator.destroy(message);

        self.allocated += 1;

        message.* = .{
            .header = std.mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
            .buffer = buffer,
            .references = 0,
        };

        return message;
    }

    fn destroy_message(self: *MessageBus, message: *Message) void {
        assert(message.references == 0);
        self.allocator.free(message.buffer);
        self.allocator.destroy(message);
        self.allocated -= 1;
    }

    fn enqueue_message(self: *MessageBus, envelope: *Envelope) void {
        assert(envelope.message.references > 0);
        assert(envelope.next == null);
        if (self.head == null) {
            assert(self.tail == null);
            self.head = envelope;
            self.tail = envelope;
        } else {
            self.tail.?.next = envelope;
            self.tail = envelope;
        }
    }
};

const sector_size = 4096;

pub const Journal = struct {
    allocator: *Allocator,
    replica: u16,
    size: u64,
    size_headers: u64,
    size_circular_buffer: u64,
    headers: []Header align(sector_size),
    dirty: []bool,
    nonce: u128,
    offset: u64,
    index: u32,

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

        log.debug("{}: journal: size={Bi} headers_len={} headers={Bi} circular_buffer={Bi}", .{
            replica,
            size,
            headers.len,
            size_headers,
            size_circular_buffer,
        });

        var self = Journal{
            .allocator = allocator,
            .replica = replica,
            .size = size,
            .size_headers = size_headers,
            .size_circular_buffer = size_circular_buffer,
            .nonce = 0,
            .headers = headers,
            .dirty = dirty,
            .offset = 0,
            .index = 0,
            .write_headers_buffer = write_headers_buffer,
        };

        assert(@mod(self.size_circular_buffer, sector_size) == 0);
        assert(@mod(@ptrToInt(&self.headers[0]), sector_size) == 0);
        assert(self.dirty.len == self.headers.len);
        assert(@mod(self.offset, sector_size) == 0);
        assert(self.write_headers_buffer.len == @sizeOf(Header) * self.headers.len);

        return self;
    }

    pub fn deinit(self: *Journal) void {}

    /// Advances the hash chain nonce, index and offset to after a header.
    /// The header's checksum_meta becomes the journal's nonce for the next entry.
    /// The header's index and offset must match the journal's current position (as a safety check).
    /// The header does not need to have been written yet.
    pub fn advance_nonce_offset_and_index_to_after(self: *Journal, header: *const Header) void {
        assert(header.command == .prepare);
        assert(header.nonce == self.nonce);
        assert(header.offset == self.offset);
        assert(header.index == self.index);

        // TODO Snapshots: Wrap offset and index.
        const offset = header.offset + Journal.sector_ceil(header.size);
        // TODO Assert against offset overflow.
        const index = header.index + 1;
        assert(index < self.headers.len);

        log.debug("{}: journal: advancing: nonce={}..{} offset={}..{} index={}..{}", .{
            self.replica,
            self.nonce,
            header.checksum_meta,
            self.offset,
            offset,
            self.index,
            index,
        });

        self.nonce = header.checksum_meta;
        self.offset = offset;
        self.index = index;
    }

    /// Sets the index and offset to after a header.
    pub fn set_nonce_offset_and_index_to_after(self: *Journal, header: *const Header) void {
        assert(header.command == .prepare);
        self.nonce = header.nonce;
        self.offset = header.offset;
        self.index = header.index;
        self.advance_nonce_offset_and_index_to_after(header);
    }

    pub fn set_nonce_offset_and_index_to_empty(self: *Journal) void {
        self.assert_all_headers_are_reserved_from_index(0);
        // TODO Use initial nonce state.
        self.nonce = 0;
        // TODO Snapshots: We need to set this to the snapshot index and offset.
        self.offset = 0;
        self.index = 0;
    }

    pub fn assert_all_headers_are_reserved_from_index(self: *Journal, index: u32) void {
        // TODO Snapshots: Adjust slices to stop before starting index.
        for (self.headers[index..]) |*header| assert(header.command == .reserved);
    }

    /// Returns a pointer to the header with the matching op number, or null if not found.
    /// Asserts that at most one such header exists.
    pub fn find_header_for_op(self: *Journal, op: u64) ?*const Header {
        assert(op > 0);
        var result: ?*const Header = null;
        for (self.headers) |*header, index| {
            if (header.op == op and header.command == .prepare) {
                assert(result == null);
                assert(header.index == index);
                result = header;
            }
        }
        return result;
    }

    pub fn entry(self: *Journal, index: u32) ?*const Header {
        const header = &self.headers[index];
        if (header.command == .reserved) return null;
        return header;
    }

    pub fn latest_entry(self: *Journal) ?*const Header {
        return self.previous_entry(self.index);
    }

    pub fn next_entry(self: *Journal, index: u32) ?*const Header {
        // TODO Snapshots: This will wrap and then be null only when we get back to the head.
        if (index + 1 == self.headers.len) return null;
        return self.entry(index + 1);
    }

    pub fn previous_entry(self: *Journal, index: u32) ?*const Header {
        // TODO Snapshots: This will wrap and then be null only when we get back to the head.
        if (index == 0) return null;
        return self.entry(index - 1);
    }

    pub fn has(self: *Journal, header: *const Header) bool {
        const existing = &self.headers[header.index];
        if (existing.command == .reserved) {
            assert(existing.checksum_meta == 0);
            assert(existing.offset == 0);
            assert(existing.index == 0);
            return false;
        } else {
            assert(existing.index == header.index);
            return existing.checksum_meta == header.checksum_meta;
        }
    }

    pub fn has_clean(self: *Journal, header: *const Header) bool {
        return self.has(header) and !self.dirty[header.index];
    }

    pub fn has_dirty(self: *Journal, header: *const Header) bool {
        return self.has(header) and self.dirty[header.index];
    }

    pub fn copy_latest_headers_to(self: *Journal, dest: []Header) usize {
        assert(dest.len > 0);
        for (dest) |*header| header.zero();
        // TODO Snapshots: Handle wrapping.
        const n = std.math.min(dest.len, self.index);
        const source = self.headers[self.index - n .. self.index];
        std.mem.copy(Header, dest, source);
        return source.len;
    }

    /// Removes entries after op number (exclusive), i.e. with a higher op number.
    /// This is used after a view change to prune uncommitted entries discarded by the new leader.
    pub fn remove_entries_after_op(self: *Journal, op: u64) void {
        assert(op > 0);
        for (self.headers) |*header, index| {
            if (header.op > op and header.command == .prepare) {
                self.remove_entry(header);
            }
        }
    }

    /// A safe way of removing an entry, where the header must match the current entry to succeed.
    pub fn remove_entry(self: *Journal, header: *const Header) void {
        assert(header.command == .prepare);
        const existing = &self.headers[header.index];
        assert(existing.checksum_meta == header.checksum_meta);
        assert(existing.offset == header.offset);
        assert(existing.index == header.index);
        self.remove_entry_at_index(header.index);
    }

    fn remove_entry_at_index(self: *Journal, index: u64) void {
        var header = &self.headers[index];
        assert(header.index == index or header.command == .reserved);
        header.zero();
        assert(self.headers[index].command == .reserved);
        self.dirty[index] = true;
    }

    pub fn set_entry_as_dirty(self: *Journal, header: *const Header) void {
        assert(header.command == .prepare);
        log.debug("{}: journal: set_entry_as_dirty: {}", .{ self.replica, header.checksum_meta });
        if (self.entry(header.index)) |existing| {
            if (existing.checksum_meta != header.checksum_meta) {
                assert(existing.view <= header.view);
                assert(existing.op < header.op or existing.view < header.view);
            }
        }
        self.headers[header.index] = header.*;
        self.dirty[header.index] = true;
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

        self.write_headers(header.index, 1);
        if (!self.has(header)) {
            self.write_debug(header, "entry changed while writing headers");
            return;
        }

        self.write_debug(header, "complete, marking clean");
        self.dirty[header.index] = false;
    }

    fn write_debug(self: *Journal, header: *const Header, status: []const u8) void {
        log.debug("{}: journal: write: index={} offset={} len={}: {} {}", .{
            self.replica,
            header.index,
            header.offset,
            header.size,
            header.checksum_meta,
            status,
        });
    }

    pub fn write_headers(self: *Journal, index: u32, len: u32) void {
        assert(index < self.headers.len);
        assert(len > 0);
        assert(index + len <= self.headers.len);

        const sector_offset = Journal.sector_floor(index * @sizeOf(Header));
        const sector_len = Journal.sector_ceil((index + len) * @sizeOf(Header));

        // We must acquire the concurrent range using the sector offset and len:
        // Different headers may share the same sector without any index or len overlap.
        var range = Range{ .offset = sector_offset, .len = sector_len };
        self.writing_headers.acquire(&range);
        defer self.writing_headers.release(&range);

        const source = std.mem.sliceAsBytes(self.headers)[sector_offset..sector_len];
        var slice = self.write_headers_buffer[sector_offset..sector_len];
        assert(slice.len == source.len);
        std.mem.copy(u8, slice, source);

        log.debug("{}: journal: write_headers: index={} len={} sectors[{}..{}]", .{
            self.replica,
            index,
            len,
            sector_offset,
            sector_len,
        });

        // Versions must be incremented upfront:
        // write_headers_to_version() will block while other calls may proceed concurrently.
        // If we don't increment upfront we could end up writing to the same copy twice.
        // We would then lose the redundancy required to locate headers or overwrite all copies.
        if (self.write_headers_once(self.headers[index .. index + len])) {
            const version_a = self.write_headers_increment_version();
            self.write_headers_to_version(version_a, slice, sector_offset);
        } else {
            const version_a = self.write_headers_increment_version();
            const version_b = self.write_headers_increment_version();
            self.write_headers_to_version(version_a, slice, sector_offset);
            self.write_headers_to_version(version_b, slice, sector_offset);
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
        // We must use header.index below (and not the loop index) as we are working on a slice.
        for (headers) |header| {
            if (self.dirty[header.index]) {
                if (header.command == .reserved) {
                    log.debug("{}: journal: write_headers_once: dirty reserved header", .{
                        self.replica,
                    });
                    return false;
                }
                if (self.previous_entry(header.index)) |previous| {
                    assert(previous.command == .prepare);
                    if (previous.checksum_meta != header.nonce) {
                        log.debug("{}: journal: write_headers_once: no hash chain", .{
                            self.replica,
                        });
                        return false;
                    }
                    if (self.dirty[previous.index]) {
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

    pub fn sector_floor(offset: u64) u64 {
        const sectors = std.math.divFloor(u64, offset, sector_size) catch unreachable;
        return sectors * sector_size;
    }

    pub fn sector_ceil(offset: u64) u64 {
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
            log.debug("{}: {} fired", .{ self.replica, self.name });
            return true;
        } else {
            return false;
        }
    }

    pub fn reset(self: *Timeout) void {
        assert(self.ticking);
        self.ticks = 0;
        log.debug("{}: {} reset", .{ self.replica, self.name });
    }

    pub fn start(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = true;
        log.debug("{}: {} started", .{ self.replica, self.name });
    }

    pub fn stop(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = false;
        log.debug("{}: {} stopped", .{ self.replica, self.name });
    }

    pub fn tick(self: *Timeout) void {
        if (self.ticking) self.ticks += 1;
    }
};

pub const Replica = struct {
    allocator: *Allocator,

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
    view: u64 = 0,

    /// The current status, either normal, view_change, or recovering:
    /// TODO Don't default to normal, set the starting status according to the journal's health.
    status: Status = .normal,

    /// The op number assigned to the most recently received request:
    /// The op number means only the highest op seen, not the highest op prepared, as we allow gaps.
    /// The first op will be assigned op number 1.
    op: u64 = 0,

    /// The op number of the most recently committed operation:
    commit: u64 = 0,

    /// The current request's checksum (used for now to enforce one-at-a-time request processing):
    request_checksum_meta: ?u128 = null,

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

    // TODO Limit integer types for `f` and `replica` to match their upper bounds in practice.
    pub fn init(
        allocator: *Allocator,
        f: u32,
        configuration: []ConfigurationAddress,
        message_bus: *MessageBus,
        journal: *Journal,
        state_machine: *StateMachine,
        replica: u16,
    ) !Replica {
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

        var self = Replica{
            .allocator = allocator,
            .f = f,
            .configuration = configuration,
            .replica = replica,
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
        };

        // We must initialize timeouts here, not in tick() on the first tick, because on_message()
        // can race with tick()... before timeouts have been initialized:
        assert(self.status == .normal);
        if (self.leader()) {
            log.debug("{}: init: leader", .{self.replica});
            self.commit_timeout.start();
        } else {
            log.debug("{}: init: follower", .{self.replica});
            self.normal_timeout.start();
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

    /// Returns the index into the configuration of the next replica for a given view,
    /// or null if the next replica is the view's leader.
    /// Replication starts and ends with the leader, we never forward back to the leader.
    pub fn next_replica(self: *Replica, view: u64) ?u16 {
        const next = @mod(self.replica + 1, @intCast(u16, self.configuration.len));
        if (next == self.leader_index(view)) return null;
        return next;
    }

    /// Time is measured in logical ticks that are incremented on every call to tick().
    /// This eliminates a dependency on the system time and enables deterministic testing.
    pub fn tick(self: *Replica) void {
        self.prepare_timeout.tick();
        self.commit_timeout.tick();
        self.normal_timeout.tick();
        self.view_change_timeout.tick();

        if (self.prepare_timeout.fired()) self.on_prepare_timeout();
        if (self.commit_timeout.fired()) self.on_commit_timeout();
        if (self.normal_timeout.fired()) self.on_normal_timeout();
        if (self.view_change_timeout.fired()) self.on_view_change_timeout();

        self.repair_last_queued_message_if_any();
    }

    /// Called by the MessageBus to deliver a message to the replica.
    pub fn on_message(self: *Replica, message: *Message) void {
        log.debug("{}: on_message: view={} status={} {}", .{ self.replica, self.view, @tagName(self.status), message.header });
        if (message.header.bad()) |reason| {
            log.debug("{}: on_message: bad ({})", .{ self.replica, reason });
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

        if (self.request_checksum_meta) |request_checksum_meta| {
            assert(message.header.command == .request);
            if (message.header.checksum_meta == request_checksum_meta) {
                log.debug("{}: on_request: ignoring (already preparing)", .{self.replica});
                return;
            }
        }

        // TODO Queue (or drop client requests after a limit) to handle one request at a time:
        // TODO Clear this queue if we lose our leadership (critical for correctness).
        assert(self.request_checksum_meta == null);
        self.request_checksum_meta = message.header.checksum_meta;

        log.debug("{}: on_request: request {}", .{ self.replica, message.header.checksum_meta });

        // The primary advances op-number, adds the request to the end of the log, and updates the
        // information for this client in the client-table to contain the new request number, s.
        // Then it sends a ⟨PREPARE v, m, n, k⟩ message to the other replicas, where v is the
        // current view-number, m is the message it received from the client, n is the op-number it
        // assigned to the request, and k is the commit-number.

        var data = message.buffer[@sizeOf(Header)..message.header.size];
        // TODO Assign timestamps to structs in associated data.

        message.header.nonce = self.journal.nonce;
        message.header.view = self.view;
        message.header.op = self.op + 1;
        message.header.commit = self.commit;
        message.header.offset = self.journal.offset;
        message.header.index = self.journal.index;
        message.header.replica = self.replica;
        message.header.command = .prepare;

        message.header.set_checksum_data(data);
        message.header.set_checksum_meta();

        assert(message.header.checksum_meta != self.request_checksum_meta.?);

        log.debug("{}: on_request: prepare {}", .{ self.replica, message.header.checksum_meta });

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

        if (message.header.view > self.view) {
            log.debug("{}: on_prepare: newer view", .{self.replica});
            self.jump_to_newer_view(message.header.view);
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader() or self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));
        assert(message.header.op > self.op);

        if (self.follower()) self.normal_timeout.reset();

        if (message.header.op > self.op + 1) {
            log.debug("{}: on_prepare: newer op", .{self.replica});
            self.jump_to_newer_op(message.header);
        }

        // We must advance our op, nonce, offset and index before replicating and journalling.
        // The leader needs this before its journal is outrun by any prepare_ok quorum:
        log.debug("{}: on_prepare: advancing: op={}..{}", .{
            self.replica,
            self.op,
            message.header.op,
        });
        assert(message.header.op == self.op + 1);
        self.op = message.header.op;
        self.journal.advance_nonce_offset_and_index_to_after(message.header);
        self.journal.set_entry_as_dirty(message.header);

        // TODO Update client's information in the client table.

        // Replicate to the next replica in the configuration (until we get back to the leader):
        if (self.next_replica(self.view)) |next| {
            log.debug("{}: on_prepare: replicating to replica {}", .{ self.replica, next });
            self.send_message_to_replica(next, message);
        }

        if (self.appending) {
            log.debug("{}: on_prepare: skipping (slow journal outrun by quorum)", .{self.replica});
            self.repair_later(message);
            return;
        }

        log.debug("{}: on_prepare: appending to journal", .{self.replica});
        self.appending_frame = async self.write_to_journal(message, &self.appending);

        if (self.follower()) self.commit_ops_through(message.header.commit);
    }

    fn is_repair(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .prepare);

        if (self.status != .normal) return true;
        if (message.header.view < self.view) return true;
        if (message.header.view == self.view and message.header.op <= self.op) return true;
        return false;
    }

    fn jump_to_newer_view(self: *Replica, new_view: u64) void {
        log.debug("{}: jump_to_newer_view: from {} to {}", .{ self.replica, self.view, new_view });
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
        if (self.op > self.commit) {
            log.debug("{}: jump_to_newer_view: setting op number {} to commit number {}", .{
                self.replica,
                self.op,
                self.commit,
            });
            self.op = self.commit;
            log.debug("{}: jump_to_newer_view: removing journal entries after op number {}", .{
                self.replica,
                self.op,
            });
            const journal_index = self.journal.index;
            self.journal.remove_entries_after_op(self.op);
            if (self.journal.find_header_for_op(self.op)) |header| {
                self.journal.set_nonce_offset_and_index_to_after(header);
            } else {
                // TODO This is dangerous, add more checks before we set to empty.
                // We want to double-check and be sure that the journal really is empty.

                // TODO Snapshots: We must update the assertion below as the last committed op may
                // have been included in the snapshot and removed from the journal.
                // Our op number may then be greater than zero.
                assert(self.op == 0);
                self.journal.set_nonce_offset_and_index_to_empty();
            }
            self.journal.assert_all_headers_are_reserved_from_index(self.journal.index);
            assert(journal_index > self.journal.index); // TODO Snapshots: Handle wrapping.
            self.journal.write_headers(self.journal.index, journal_index - self.journal.index);
        } else {
            log.debug("{}: jump_to_newer_view: no journal entries to remove", .{self.replica});
        }

        assert(self.op == self.commit);
        self.transition_to_normal_status(new_view);
        assert(self.view == new_view);
        assert(self.follower());
    }

    fn jump_to_newer_op(self: *Replica, header: *const Header) void {
        log.debug("{}: jump_to_newer_op: op={}..{} nonce={}..{} offset={}..{} index={}..{}", .{
            self.replica,
            self.op,
            header.op,
            self.journal.nonce,
            header.nonce,
            self.journal.offset,
            header.offset,
            self.journal.index,
            header.index,
        });
        assert(self.status == .normal);
        assert(self.follower());
        assert(header.op > self.op);
        assert(header.op > self.op + 1);
        assert(header.op > self.commit);
        // TODO Assert offset.
        self.op = header.op - 1;
        assert(self.op >= self.commit);
        assert(self.op + 1 == header.op);
        self.journal.nonce = header.nonce;
        self.journal.index = header.index;
        self.journal.offset = header.offset;
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

        if (message.header.op <= self.commit) {
            log.debug("{}: send_prepare_ok: not sending (committed)", .{self.replica});
            return;
        }

        if (self.journal.has_clean(message.header)) {
            assert(message.header.replica == self.leader_index(message.header.view));
            // Sending extra fields back in the ack such as offset allows the leader to cross-check.
            // For example, a follower may ack the correct checksum but write to the wrong offset.
            self.send_header_to_replica(message.header.replica, .{
                .command = .prepare_ok,
                .nonce = message.header.checksum_meta,
                .client_id = message.header.client_id,
                .request_number = message.header.request_number,
                .replica = self.replica,
                .view = message.header.view,
                .op = message.header.op,
                .commit = message.header.commit,
                .offset = message.header.offset,
                .index = message.header.index,
                .operation = message.header.operation,
            });
        } else {
            log.debug("{}: send_prepare_ok: not sending (dirty)", .{self.replica});
            return;
        }
    }

    fn on_prepare_timeout(self: *Replica) void {
        assert(self.status == .normal);
        assert(self.leader());
        assert(self.request_checksum_meta != null);
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
            if (message.header.nonce != prepare_message.header.checksum_meta) {
                log.debug("{}: on_prepare_ok: ignoring (different nonce)", .{self.replica});
                return;
            }

            assert(message.header.command == .prepare_ok);
            assert(message.header.client_id == prepare_message.header.client_id);
            assert(message.header.request_number == prepare_message.header.request_number);
            assert(message.header.view == prepare_message.header.view);
            assert(message.header.op == prepare_message.header.op);
            assert(message.header.commit == prepare_message.header.commit);
            assert(message.header.offset == prepare_message.header.offset);
            assert(message.header.index == prepare_message.header.index);
            assert(message.header.operation == prepare_message.header.operation);
        } else {
            log.debug("{}: on_prepare_ok: ignoring (not preparing)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader());
        assert(message.header.nonce == self.prepare_message.?.header.checksum_meta);
        assert(message.header.op == self.op);
        assert(message.header.op == self.commit + 1);

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

    fn reset_prepare(self: *Replica) void {
        if (self.prepare_message) |message| {
            self.request_checksum_meta = null;
            message.references -= 1;
            self.message_bus.gc(message);
            self.prepare_message = null;
            self.prepare_attempt = 0;
            self.prepare_timeout.stop();
            self.reset_quorum_counter(self.prepare_ok_from_all_replicas, .prepare_ok);
        }
        assert(self.request_checksum_meta == null);
        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        assert(self.prepare_timeout.ticking == false);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
    }

    fn on_commit_timeout(self: *Replica) void {
        assert(self.status == .normal);
        assert(self.leader());

        self.commit_timeout.reset();

        // TODO Add checksum_meta (as nonce) of latest committed entry so that followers can verify.
        self.send_header_to_other_replicas(.{
            .command = .commit,
            .replica = self.replica,
            .view = self.view,
            .commit = self.commit,
        });
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
        assert(self.commit <= self.op);

        self.normal_timeout.reset();

        self.commit_ops_through(message.header.commit);
    }

    fn on_repair(self: *Replica, message: *Message) void {
        assert(message.header.command == .prepare);
        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op or message.header.view < self.view);

        if (self.status != .normal and self.status != .view_change) return;

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

    fn repair_header(self: *Replica, header: *const Header) bool {
        assert(header.command == .prepare);
        assert(header.view <= self.view);
        assert(header.op <= self.op or header.view < self.view);

        if (header.op > self.op and header.view < self.view) {
            // For example, an op reordered through a view change (section 5.2 in the VRR paper).
            log.debug("{}: repair_header: ignoring (reordered op)", .{self.replica});
            return false;
        }

        if (self.journal.entry(header.index)) |existing| {
            if (existing.view > header.view) {
                log.debug("{}: repair_header: ignoring (newer view)", .{self.replica});
                return false;
            }

            if (existing.view == header.view and existing.op > header.op) {
                // For example, the journal wrapped.
                log.debug("{}: repair_header: ignoring (newer op)", .{self.replica});
                return false;
            }

            if (existing.checksum_meta == header.checksum_meta) {
                if (self.journal.dirty[header.index]) {
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

        if (self.repair_header_would_overlap_with_newer_neighbor(header)) {
            log.debug("{}: repair_header: would overlap with newer neighbor", .{self.replica});
            return false;
        }

        // TODO Snapshots: Skip if this header is already snapshotted.

        // While we could also skip this header if its view is less than preceeding entries in the
        // journal, this requires first knowing where the journal begins, is CPU-intensive, and is
        // not essential for correctness here, where our goal is simply to not overwrite newer data.

        self.journal.set_entry_as_dirty(header);
        return true;
    }

    /// If we repair this header would we introduce a break in the chain with a newer neighbor?
    /// Ignores breaks in the chain if the neighboring entry is older and would in turn be repaired.
    fn repair_header_would_break_chain_with_newer_neighbor(
        self: *Replica,
        header: *const Header,
    ) bool {
        if (self.journal.previous_entry(header.index)) |previous| {
            if (previous.checksum_meta == header.nonce) {
                assert(previous.view <= header.view);
                assert(previous.op + 1 == header.op);
                return false;
            }
            return self.repair_header_has_newer_neighbor(header, previous);
        }
        if (self.journal.next_entry(header.index)) |next| {
            if (header.checksum_meta == next.nonce) {
                assert(header.view <= next.view);
                assert(header.op + 1 == next.op);
                return false;
            }
            return self.repair_header_has_newer_neighbor(header, next);
        }
        return false;
    }

    /// If we repair this header would we overwrite a newer neighbor (immediate or distant)?
    /// Ignores overlap if the neighboring entry is older and would in turn be repaired.
    fn repair_header_would_overlap_with_newer_neighbor(self: *Replica, header: *const Header) bool {
        // TODO Snapshots: Handle journal wrap around.
        {
            // Look behind this entry for any preceeding entry that this would overwrite.
            var i: usize = header.index;
            while (i > 0) {
                i -= 1;
                const neighbor = &self.journal.headers[i];
                if (neighbor.command == .reserved) continue;
                if (neighbor.offset + Journal.sector_ceil(neighbor.size) <= header.offset) break;
                if (neighbor.offset + Journal.sector_ceil(neighbor.size) > header.offset) {
                    if (self.repair_header_has_newer_neighbor(header, neighbor)) return true;
                }
            }
        }
        {
            // Look beyond this entry for any succeeding entry that this would overwrite.
            var i: usize = header.index + 1;
            while (i < self.journal.index) : (i += 1) {
                const neighbor = &self.journal.headers[i];
                if (neighbor.command == .reserved) continue;
                if (header.offset + Journal.sector_ceil(header.size) <= neighbor.offset) break;
                if (header.offset + Journal.sector_ceil(header.size) > neighbor.offset) {
                    if (self.repair_header_has_newer_neighbor(header, neighbor)) return true;
                }
            }
        }
        return false;
    }

    /// If we repair this header would we succeed a newer neighbor (immediate or distant)?
    /// Looks behind this entry for any preceeding entry that would be newer.
    fn repair_header_would_succeed_newer_neighbor(self: *Replica, header: *const Header) bool {
        // TODO Snapshots: Handle journal wrap around:
        // We may not know the journal start index (if we do not yet have the snapshot).
        // Run from header.index - 1 down through zero to self.index?
        var i: usize = header.index;
        while (i > 0) {
            i -= 1;
            const neighbor = &self.journal.headers[i];
            if (neighbor.command == .reserved) continue;
            return self.repair_header_has_newer_neighbor(header, neighbor);
        }
        return false;
    }

    /// A neighbor may be an immediate neighbor (left or right of the index) or further away.
    /// Returns whether the neighbor has a higher view or else same view and higher op.
    fn repair_header_has_newer_neighbor(
        self: *Replica,
        header: *const Header,
        neighbor: *const Header,
    ) bool {
        assert(header.command == .prepare);
        assert(neighbor.command == .prepare);
        assert(neighbor.index != header.index);

        // Check if we haven't accidentally swapped the header and neighbor arguments above:
        // The neighbor should at least exist in the journal.
        assert(self.journal.headers[neighbor.index].checksum_meta == neighbor.checksum_meta);

        if (neighbor.view > header.view) {
            // We do not assert neighbor.op >= header.op, ops may be reordered during a view change.
            return true;
        } else if (neighbor.view < header.view) {
            // We do not assert neighbor.op <= header.op, ops may be reordered during a view change.
            return false;
        } else if (neighbor.op > header.op) {
            assert(neighbor.view == header.view);
            return true;
        } else if (neighbor.op < header.op) {
            assert(neighbor.view == header.view);
            return false;
        } else {
            assert(neighbor.view == header.view);
            assert(neighbor.op == header.op);
            unreachable;
        }
        return false;
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
            .replica = self.replica,
            .view = new_view,
        });
        // TODO Resend after timeout.
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

        // When replica i receives start_view_change messages for its view from f other replicas,
        // it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
        // primary in the new view. Here v is its view, l is its log, v′ is the view number of the
        // latest view in which its status was normal, n is the op number, and k is the commit
        // number.
        const new_leader = self.leader_index(self.view);
        self.send_header_to_replica(new_leader, .{
            .command = .do_view_change,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit,
        });

        // TODO Add latest normal view.
        // TODO Add N most recent journal entries.
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
            log.debug("{}: on_{}: ignoring (recovering)", .{ self.replica, command });
            return true;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_{}: ignoring (older view)", .{ self.replica, command });
            return true;
        }

        if (message.header.view == self.view and self.status == .normal) {
            log.debug("{}: on_{}: ignoring (view already started)", .{ self.replica, command });
            return true;
        }

        // These may be caused by faults in the network topology.
        switch (message.header.command) {
            .start_view_change, .start_view => {
                if (message.header.replica == self.replica) {
                    log.warn("{}: on_{}: ignoring (self)", .{ self.replica, command });
                    return true;
                }
            },
            .do_view_change => {
                if (self.leader_index(message.header.view) != self.replica) {
                    log.warn("{}: on_{}: ignoring (follower)", .{ self.replica, command });
                    return true;
                }
            },
            else => unreachable,
        }

        return false;
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

        // It's possible for the first view change to not have anything newer than these initial values:
        // Use real values here, rather than introducing anything artificial.
        var r: u16 = self.replica;
        var v: u64 = 0;
        var n: u64 = 0;
        var k: u64 = 0;
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                if ((m.header.latest_normal_view > v) or
                    (m.header.latest_normal_view == v and m.header.op > n))
                {
                    r = @intCast(u16, replica);
                    v = m.header.latest_normal_view;
                    n = m.header.op;
                    k = m.header.commit;
                }
            }
        }

        // We do not assert that v must be non-zero, because v represents the latest normal view,
        // which may be zero if this is our first view change.

        log.debug("{}: replica={} has the latest log: op={} commit={}", .{ self.replica, r, n, k });

        // TODO Update journal
        // TODO Calculate how much of the journal to send in start_view message.
        self.transition_to_normal_status(self.view);

        assert(self.status == .normal);
        assert(self.leader());

        self.op = n;
        self.commit_ops_through(k);
        assert(self.commit == k);

        // TODO Add journal entries to start_view message:
        self.send_header_to_other_replicas(.{
            .command = .start_view,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit,
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

    fn add_message_and_receive_quorum_exactly_once(
        self: *Replica,
        messages: []?*Message,
        message: *Message,
        threshold: u32,
    ) ?usize {
        assert(messages.len == self.configuration.len);

        assert(message.header.view == self.view);
        switch (message.header.command) {
            .prepare_ok => {
                assert(self.status == .normal);
                assert(self.leader());
                assert(message.header.nonce == self.prepare_message.?.header.checksum_meta);
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
            assert(m.header.latest_normal_view == message.header.latest_normal_view);
            log.debug(
                "{}: on_{}: ignoring (duplicate message)",
                .{ self.replica, @tagName(message.header.command) },
            );
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
                        assert(m.header.latest_normal_view <= self.view);
                        assert(m.header.nonce == message.header.nonce);
                    },
                    .start_view_change => {
                        assert(m.header.latest_normal_view < self.view);
                        assert(m.header.replica != self.replica);
                    },
                    .do_view_change => {
                        assert(m.header.latest_normal_view < self.view);
                    },
                    else => unreachable,
                }
                count += 1;
            }
        }
        log.debug("{}: on_{}: {} message(s)", .{ self.replica, @tagName(message.header.command), count });

        // Wait until we have exactly `threshold` messages for quorum:
        if (count < threshold) {
            log.debug("{}: on_{}: waiting for quorum", .{ self.replica, @tagName(message.header.command) });
            return null;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > threshold) {
            log.debug("{}: on_{}: ignoring (quorum received already)", .{ self.replica, @tagName(message.header.command) });
            return null;
        }

        assert(count == threshold);
        return count;
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
        log.debug("{}: reset {} {} message(s)", .{ self.replica, count, @tagName(command) });
    }

    fn commit_ops_through(self: *Replica, commit: u64) void {
        // TODO Wait until our journal chain from self.commit to self.op is completely connected.
        // This will serve as another defense against not removing ops after a view jump.

        // We may receive commit messages for ops we don't yet have:
        // Even a naive state transfer may fail to correct for this.
        while (self.commit < commit and self.commit < self.op) {
            self.commit += 1;
            assert(self.commit <= self.op);
            // Find operation in journal:
            // TODO Journal should have a fast path where current operation is already in memory.
            // var entry = self.journal.find(self.commit) orelse @panic("operation not found in log");

            // TODO Apply to State Machine:
            log.debug("{}: executing op {}", .{ self.replica, self.commit });

            // TODO Add reply to the client table to answer future duplicate requests idempotently.
            // Lookup client table entry using client id.
            // If client's last request id is <= this request id, then update client table entry.
            // Otherwise the client is already ahead of us, and we don't need to update the entry.

            // TODO Now that the reply is in the client table, trigger this message to be sent.
            // TODO Do not reply to client if we are a follower.
        }
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
        log.debug("{}: sending {} to replica {}: {}", .{
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
        log.debug("{}: sending {} to replica {}: {}", .{
            self.replica,
            @tagName(message.header.command),
            replica,
            message.header,
        });
        // We may forward messages sent by another replica (e.g. prepares from the leader).
        // We therefore do not assert message.header.replica as we do for send_header_to_replica().
        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(message.header.command == .prepare);

        self.message_bus.send_message_to_replica(replica, message);
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

    var configuration: [3]ConfigurationAddress = undefined;
    var message_bus = try MessageBus.init(allocator, &configuration);

    var journals: [2 * f + 1]Journal = undefined;
    for (journals) |*journal, replica_index| {
        journal.* = try Journal.init(
            allocator,
            @intCast(u16, replica_index),
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
                    .client_id = 1,
                    .request_number = ticks,
                    .command = .request,
                    .operation = .noop,
                };
                request.header.set_checksum_data(request.buffer[0..0]);
                request.header.set_checksum_meta();

                message_bus.send_message_to_replica(replica.replica, request);
            }
        }

        message_bus.send_queued_messages();
        std.time.sleep(std.time.ns_per_s);
    }
}
pub fn main() !void {
    var frame = async run();
    nosuspend try await frame;
}
