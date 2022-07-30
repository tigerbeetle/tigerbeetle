const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

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

        try entries.ensureTotalCapacity(allocator, @intCast(u32, config.clients_max));
        assert(entries.capacity() >= config.clients_max);

        const sorted = try allocator.alloc(*const Entry, entries.capacity());
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

    /// Creates a sortable key for a given entry using the client id + request number.
    /// The request should be unique under a client and clients should be unique between each other.
    fn sort_entries_key(entry: *const Entry) u256 {
        const header = entry.reply.header;
        const key_parts = [_]u128{ header.client, header.request };
        return @bitCast(u256, key_parts);
    }

    fn sort_entries_less_than(context: void, entry_a: *const Entry, entry_b: *const Entry) bool {
        _ = context;
        return std.math.order(
            sort_entries_key(entry_a),
            sort_entries_key(entry_b),
        ) == .lt;
    }

    /// Maximum size of the buffer needed to encode the client table on disk.
    pub const encode_size_max = blk: {
        var size_max: usize = 0;

        // First goes the vsr headers for the entries.
        // This takes advantage of the buffer alignment to avoid adding padding for the headers.
        size_max = std.mem.alignForward(size_max, @alignOf(vsr.Header));
        size_max += @sizeOf(vsr.Header) * config.clients_max;

        // Then follows the session values for the entries.
        size_max = std.mem.alignForward(size_max, @alignOf(u64));
        size_max += @sizeOf(u64) * config.clients_max;

        // Followed by the message bodies for the entries.
        size_max = std.mem.alignForward(size_max, @alignOf(u8));
        size_max += config.message_size_max * config.clients_max;

        // Finally the entry count at the end
        size_max = std.mem.alignForward(size_max, @alignOf(u32));
        size_max += @sizeOf(u32);

        break :blk size_max;
    };

    pub fn encode(client_table: *const ClientTable, target: []align(@alignOf(vsr.Header)) u8) u64 {
        // The entries must be collected and sorted into a separate buffer first before iteration.
        // This avoids relying on iteration order of AutoHashMapUnmanaged which may change between 
        // zig versions.
        var num_entries: u32 = 0;
        {
            var it = client_table.iterator();
            while (it.next()) |entry| : (num_entries += 1) {
                assert(num_entries < client_table.sorted.len);
                client_table.sorted[num_entries] = entry;
                num_entries += 1;
            }
        }

        assert(num_entries <= client_table.sorted.len);
        const entries = client_table.sorted[0..num_entries];
        std.sort.sort(*const Entry, entries, {}, sort_entries_less_than);

        var size: u64 = 0;
        assert(target.len >= encode_size_max);

        // Write all headers
        size = std.mem.alignForward(size, @alignOf(vsr.Header));
        for (entries) |entry| {
            mem.copy(u8, target[size..], mem.asBytes(entry.reply.header));
            size += @sizeOf(vsr.Header);
        }

        // Write all sessions
        size = std.mem.alignForward(size, @alignOf(u64));
        for (entries) |entry| {
            mem.copy(u8, target[size..], mem.asBytes(entry.session));
            size += @sizeOf(u64);
        }
        
        // Write all messages
        for (entries) |entry| {
            const body = entry.reply.body();
            assert(body.len == (entry.reply.header.size - @sizeOf(vsr.Header)));
            mem.copy(u8, target[size..], body);
            size += body.len;
        }

        // Finally write the entry count
        mem.copy(u8, target[size..], mem.asBytes(&num_entries));
        size += @sizeOf(u32);

        assert(size <= encode_size_max);
        return size;
    }

    pub fn decode(client_table: *ClientTable, source: []align(@alignOf(vsr.Header)) const u8) void {
        // Read the entry count at the end of the buffer to determine how many there are.
        var num_entries: u32 = undefined;
        mem.copy(u8, mem.asBytes(&num_entries), source[source.len - @sizeOf(u32)..]);

        assert(num_entries <= client_table.sorted.len);
        if (num_entries == 0) return;

        var size: u64 = 0;
        assert(source.len > 0);
        assert(source.len <= encode_size_max);

        var headers = mem.bytesAsSlice(vsr.Header, source[size..num_entries * @sizeOf(vsr.Header)]);
        size += mem.sliceAsBytes(headers).len;

        var sessions = mem.bytesAsSlice(u64, source[size..num_entries * @sizeOf(u64)]);
        size += mem.sliceAsBytes(sessions).len;

        var bodies = source[size..];
        assert(bodies.len > 0);

        var i: u32 = 0;
        while (i < num_entries) : (i += 1) {
            // Prepare the entry with a message.
            var entry: Entry = undefined;
            entry.reply = client_table.message_pool.get_message();

            // Read the header and session for the entry.
            entry.reply.header.* = headers[i];
            entry.session = sessions[i];

            // Get the message body buffer for the entry.
            const body_size = entry.reply.header.size - @sizeOf(vsr.Header);
            const body = entry.reply.body()[0..body_size];

            // Read the message body for the entry.
            assert(bodies.len >= body_size);
            mem.copy(u8, body, bodies[0..body_size]);
            bodies = bodies[body_size..];
            
            // Insert into the client table
            client_table.put(&entry);
        }
    }

    pub fn count(client_table: *const ClientTable) usize {
        return client_table.entries.count();
    } 

    pub fn capacity(client_table: *const ClientTable) usize {
        return client_table.entries.capacity();
    }

    pub fn get(client_table: *const ClientTable, client: u128) ?*const Entry {
        return client_table.entries.getPtr(client);
    }

    pub fn put(client_table: *ClientTable, entry: *const Entry) void {
        const client = entry.reply.header.client;
        client_table.entries.putAssumeCapacityNoClobber(client, entry.*);

        if (config.verify) assert(client_table.entries.contains(client));
    }

    pub fn remove(client_table: *ClientTable, client: u128) void {
        client_table.entries.remove(client);

        if (config.verify) assert(!client_table.entries.contains(client));
    }

    pub const Iterator = Entries.ValueIterator;

    pub fn iterator(client_table: *ClientTable) Iterator {
        return client_table.entries.valueIterator();
    }
};
