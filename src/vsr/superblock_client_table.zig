const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;

pub const ClientTable = struct {
    /// We found two bugs in the VRR paper relating to the client table:
    ///
    /// 1. a correctness bug, where successive client crashes may cause request numbers to collide for
    /// different request payloads, resulting in requests receiving the wrong reply, and
    ///
    /// 2. a liveness bug, where if the client table is updated for request and prepare messages with
    /// the client's latest request number, then the client may be locked out from the cluster if the
    /// request is ever reordered through a view change.
    ///
    /// We therefore take a different approach with the implementation of our client table, to:
    ///
    /// 1. register client sessions explicitly through the state machine to ensure that client session
    /// numbers always increase, and
    ///
    /// 2. make a more careful distinction between uncommitted and committed request numbers,
    /// considering that uncommitted requests may not survive a view change.
    pub const Entry = struct {
        /// The client's session number as committed to the cluster by a register request.
        session: u64,

        /// The reply sent to the client's latest committed request.
        reply: *MessagePool.Message,
    };

    const Entries = std.AutoHashMapUnmanaged(u128, Entry);

    entries: Entries,
    sorted: []*const Entry,
    message_pool: *MessagePool,

    pub fn init(allocator: mem.Allocator, message_pool: *MessagePool) !ClientTable {
        var entries: Entries = .{};
        errdefer entries.deinit(allocator);

        try entries.ensureTotalCapacity(allocator, @intCast(u32, constants.clients_max));
        assert(entries.capacity() >= constants.clients_max);

        const sorted = try allocator.alloc(*const Entry, constants.clients_max);
        errdefer allocator.free(sorted);

        return ClientTable{
            .entries = entries,
            .sorted = sorted,
            .message_pool = message_pool,
        };
    }

    pub fn deinit(client_table: *ClientTable, allocator: mem.Allocator) void {
        {
            var it = client_table.iterator();
            while (it.next()) |entry| {
                client_table.message_pool.unref(entry.reply);
            }
        }

        client_table.entries.deinit(allocator);
        allocator.free(client_table.sorted);
    }

    fn sort_entries_less_than(context: void, a: *const Entry, b: *const Entry) bool {
        _ = context;
        assert(a.reply.header.client != b.reply.header.client);
        return std.math.order(
            a.reply.header.client,
            b.reply.header.client,
        ) == .lt;
    }

    /// Maximum size of the buffer needed to encode the client table on disk.
    pub const encode_size_max = blk: {
        var size_max: usize = 0;

        // First goes the vsr headers for the entries.
        // This takes advantage of the buffer alignment to avoid adding padding for the headers.
        size_max = std.mem.alignForward(size_max, @alignOf(vsr.Header));
        size_max += @sizeOf(vsr.Header) * constants.clients_max;

        // Then follows the session values for the entries.
        size_max = std.mem.alignForward(size_max, @alignOf(u64));
        size_max += @sizeOf(u64) * constants.clients_max;

        // Followed by the message bodies for the entries.
        size_max = std.mem.alignForward(size_max, @alignOf(u8));
        size_max += constants.message_size_max * constants.clients_max;

        // Finally the entry count at the end
        size_max = std.mem.alignForward(size_max, @alignOf(u32));
        size_max += @sizeOf(u32);

        break :blk size_max;
    };

    pub fn encode(client_table: *const ClientTable, target: []align(@alignOf(vsr.Header)) u8) u64 {
        // The entries must be collected and sorted into a separate buffer first before iteration.
        // This avoids relying on iteration order of AutoHashMapUnmanaged which may change between
        // zig versions.
        var entries_count: u32 = 0;
        {
            var it = client_table.entries.valueIterator();
            while (it.next()) |entry| : (entries_count += 1) {
                assert(entries_count < client_table.sorted.len);
                assert(entry.reply.header.command == .reply);
                client_table.sorted[entries_count] = entry;
            }
        }

        assert(entries_count <= client_table.sorted.len);
        const entries = client_table.sorted[0..entries_count];
        std.sort.sort(*const Entry, entries, {}, sort_entries_less_than);

        var size: u64 = 0;
        assert(target.len >= encode_size_max);

        // Write all headers:
        var new_size = std.mem.alignForward(size, @alignOf(vsr.Header));
        std.mem.set(u8, target[size..new_size], 0);
        size = new_size;

        for (entries) |entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(entry.reply.header));
            size += @sizeOf(vsr.Header);
        }

        // Write all sessions:
        new_size = std.mem.alignForward(size, @alignOf(u64));
        std.mem.set(u8, target[size..new_size], 0);
        size = new_size;

        for (entries) |entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.session));
            size += @sizeOf(u64);
        }

        // Write all messages:
        new_size = std.mem.alignForward(size, @alignOf(u8));
        std.mem.set(u8, target[size..new_size], 0);
        size = new_size;

        for (entries) |entry| {
            const body = entry.reply.body();
            assert(body.len == (entry.reply.header.size - @sizeOf(vsr.Header)));
            stdx.copy_disjoint(.inexact, u8, target[size..], body);
            size += body.len;
        }

        // Finally write the entry count:
        new_size = std.mem.alignForward(size, @alignOf(u32));
        std.mem.set(u8, target[size..new_size], 0);
        size = new_size;

        stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entries_count));
        size += @sizeOf(u32);

        assert(size <= encode_size_max);
        return size;
    }

    pub fn decode(client_table: *ClientTable, source: []align(@alignOf(vsr.Header)) const u8) void {
        // Read the entry count at the end of the buffer to determine how many there are.
        var entries_count: u32 = undefined;
        stdx.copy_disjoint(.exact, u8, mem.asBytes(&entries_count), source[source.len - @sizeOf(u32) ..]);
        assert(entries_count <= client_table.sorted.len);

        assert(client_table.count() == 0);
        defer assert(client_table.count() == entries_count);

        // Skip decoding if there aren't any entries.
        if (entries_count == 0) return;

        var size: u64 = 0;
        assert(source.len > 0);
        assert(source.len <= encode_size_max);

        size = std.mem.alignForward(size, @alignOf(vsr.Header));
        const headers = mem.bytesAsSlice(
            vsr.Header,
            source[size..][0 .. entries_count * @sizeOf(vsr.Header)],
        );
        size += mem.sliceAsBytes(headers).len;

        size = std.mem.alignForward(size, @alignOf(u64));
        const sessions = mem.bytesAsSlice(u64, source[size..][0 .. entries_count * @sizeOf(u64)]);
        size += mem.sliceAsBytes(sessions).len;

        size = std.mem.alignForward(size, @alignOf(u8));
        var bodies = source[size .. source.len - @sizeOf(u32)];

        for (headers) |header, i| {
            // Prepare the entry with a message.
            var entry: Entry = undefined;
            entry.reply = client_table.message_pool.get_message();

            // Read the header and session for the entry.
            entry.session = sessions[i];
            entry.reply.header.* = header;
            assert(entry.reply.header.valid_checksum());
            assert(entry.reply.header.command == .reply);
            assert(entry.reply.header.commit >= entry.session);

            // Get the message body buffer for the entry.
            const body_size = entry.reply.header.size - @sizeOf(vsr.Header);
            const body = entry.reply.body()[0..body_size];

            // Read the message body for the entry.
            assert(bodies.len >= body_size);
            stdx.copy_disjoint(.exact, u8, body, bodies[0..body_size]);
            bodies = bodies[body_size..];
            {
                vsr.checksum_context = "SuperBlockClientTable.decode";
                defer vsr.checksum_context = null;
                assert(entry.reply.header.valid_checksum_body(body));
            }

            // Insert into the client table
            client_table.put(&entry);
        }
    }

    pub fn count(client_table: *const ClientTable) usize {
        return client_table.entries.count();
    }

    pub fn capacity(client_table: *const ClientTable) usize {
        return client_table.sorted.len;
    }

    pub fn get(client_table: *ClientTable, client: u128) ?*Entry {
        return client_table.entries.getPtr(client);
    }

    pub fn put(client_table: *ClientTable, entry: *const Entry) void {
        const client = entry.reply.header.client;
        client_table.entries.putAssumeCapacityNoClobber(client, entry.*);

        if (constants.verify) assert(client_table.entries.contains(client));
    }

    pub fn remove(client_table: *ClientTable, client: u128) void {
        assert(client_table.entries.remove(client));

        if (constants.verify) assert(!client_table.entries.contains(client));
    }

    pub const Iterator = Entries.ValueIterator;

    pub fn iterator(client_table: *ClientTable) Iterator {
        return client_table.entries.valueIterator();
    }
};
