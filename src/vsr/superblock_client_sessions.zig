const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");

/// There is a slot corresponding to every active client (i.e. a total of clients_max slots).
pub const ReplySlot = struct { index: usize };

pub const ClientSessions = struct {
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

        /// The header of the reply corresponding to the client's latest committed request.
        header: vsr.Header,
    };

    /// Values are indexes into `entries`.
    const EntriesByClient = std.AutoHashMapUnmanaged(u128, usize);

    entries_by_client: EntriesByClient,
    /// Free entries are zeroed, both in `entries` and on-disk.
    entries: []Entry,

    pub fn init(allocator: mem.Allocator) !ClientSessions {
        var entries_by_client: EntriesByClient = .{};
        errdefer entries_by_client.deinit(allocator);

        try entries_by_client.ensureTotalCapacity(allocator, @intCast(u32, constants.clients_max));
        assert(entries_by_client.capacity() >= constants.clients_max);

        var entries = try allocator.alloc(Entry, constants.clients_max);
        errdefer allocator.free(entries);
        std.mem.set(Entry, entries, std.mem.zeroes(Entry));

        return ClientSessions{
            .entries_by_client = entries_by_client,
            .entries = entries,
        };
    }

    pub fn deinit(client_sessions: *ClientSessions, allocator: mem.Allocator) void {
        client_sessions.entries_by_client.deinit(allocator);
        allocator.free(client_sessions.entries);
    }

    /// Maximum size of the buffer needed to encode the client sessions on disk.
    /// (Not rounded up to a sector boundary).
    pub const encode_size_max = blk: {
        var size_max: usize = 0;

        // First goes the vsr headers for the entries.
        // This takes advantage of the buffer alignment to avoid adding padding for the headers.
        size_max = std.mem.alignForward(size_max, @alignOf(vsr.Header));
        size_max += @sizeOf(vsr.Header) * constants.clients_max;

        // Then follows the session values for the entries.
        size_max = std.mem.alignForward(size_max, @alignOf(u64));
        size_max += @sizeOf(u64) * constants.clients_max;

        break :blk size_max;
    };

    pub fn encode(client_sessions: *const ClientSessions, target: []align(@alignOf(vsr.Header)) u8) u64 {
        assert(target.len >= encode_size_max);

        var size: u64 = 0;

        // Write all headers:
        var new_size = std.mem.alignForward(size, @alignOf(vsr.Header));
        std.mem.set(u8, target[size..new_size], 0);
        size = new_size;

        for (client_sessions.entries) |entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.header));
            size += @sizeOf(vsr.Header);
        }

        // Write all sessions:
        new_size = std.mem.alignForward(size, @alignOf(u64));
        std.mem.set(u8, target[size..new_size], 0);
        size = new_size;

        for (client_sessions.entries) |entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.session));
            size += @sizeOf(u64);
        }

        assert(size == encode_size_max);
        return size;
    }

    pub fn decode(client_sessions: *ClientSessions, source: []align(@alignOf(vsr.Header)) const u8) void {
        assert(client_sessions.count() == 0);
        for (client_sessions.entries) |*entry| {
            assert(entry.session == 0);
            assert(std.meta.eql(entry.header, std.mem.zeroes(vsr.Header)));
        }

        var size: u64 = 0;
        assert(source.len > 0);
        assert(source.len <= encode_size_max);

        size = std.mem.alignForward(size, @alignOf(vsr.Header));
        const headers = mem.bytesAsSlice(
            vsr.Header,
            source[size..][0 .. constants.clients_max * @sizeOf(vsr.Header)],
        );
        size += mem.sliceAsBytes(headers).len;

        size = std.mem.alignForward(size, @alignOf(u64));
        const sessions = mem.bytesAsSlice(
            u64,
            source[size..][0 .. constants.clients_max * @sizeOf(u64)],
        );
        size += mem.sliceAsBytes(sessions).len;

        assert(size == encode_size_max);

        for (headers) |*header, i| {
            const session = sessions[i];
            if (session == 0) {
                assert(std.meta.eql(header.*, std.mem.zeroes(vsr.Header)));
            } else {
                assert(header.valid_checksum());
                assert(header.command == .reply);
                assert(header.commit >= session);

                client_sessions.entries_by_client.putAssumeCapacityNoClobber(header.client, i);
                client_sessions.entries[i] = .{
                    .session = session,
                    .header = header.*,
                };
            }
        }
    }

    pub fn count(client_sessions: *const ClientSessions) usize {
        return client_sessions.entries_by_client.count();
    }

    pub fn capacity(client_sessions: *const ClientSessions) usize {
        _ = client_sessions;
        return constants.clients_max;
    }

    pub fn get(client_sessions: *ClientSessions, client: u128) ?*Entry {
        const entry_index = client_sessions.entries_by_client.get(client) orelse return null;
        const entry = &client_sessions.entries[entry_index];
        assert(entry.session != 0);
        assert(entry.header.command == .reply);
        assert(entry.header.client == client);
        return entry;
    }

    pub fn get_slot(client_sessions: *ClientSessions, client: u128) ?ReplySlot {
        const index = client_sessions.entries_by_client.get(client) orelse return null;
        return ReplySlot{ .index = index };
    }

    /// If the entry is from a newly-registered client, the caller is responsible for ensuring
    /// the ClientSessions has available capacity.
    pub fn put(client_sessions: *ClientSessions, entry: *const Entry) ReplySlot {
        assert(entry.session != 0);
        assert(entry.header.command == .reply);
        const client = entry.header.client;

        defer if (constants.verify) assert(client_sessions.entries_by_client.contains(client));

        if (client_sessions.entries_by_client.get(client)) |entry_index| {
            const existing = &client_sessions.entries[entry_index];
            assert(existing.session == entry.session);
            assert(existing.header.client == client);
            assert(existing.header.commit > entry.header.commit);

            existing.header = entry.header;
            return ReplySlot{ .index = entry_index };
        } else {
            for (client_sessions.entries) |*e, i| {
                if (e.session == 0) {
                    e.* = entry.*;
                    client_sessions.entries_by_client.putAssumeCapacityNoClobber(client, i);
                    return ReplySlot{ .index = i };
                }
            } else unreachable;
        }
    }

    pub fn remove(client_sessions: *ClientSessions, client: u128) void {
        const entry_index = client_sessions.entries_by_client.get(client).?;

        const removed = client_sessions.entries_by_client.remove(client);
        assert(removed);

        assert(client_sessions.entries[entry_index].header.client == client);
        client_sessions.entries[entry_index] = std.mem.zeroes(Entry);

        if (constants.verify) assert(!client_sessions.entries_by_client.contains(client));
    }

    pub const Iterator = struct {
        client_sessions: *const ClientSessions,
        index: usize = 0,

        pub fn next(it: *Iterator) ?*const Entry {
            while (it.index < it.client_sessions.entries.len) {
                defer it.index += 1;

                const entry = &it.client_sessions.entries[it.index];
                if (entry.session != 0) {
                    return entry;
                }
            }
            return null;
        }
    };

    pub fn iterator(client_sessions: *const ClientSessions) Iterator {
        return .{ .client_sessions = client_sessions };
    }
};
