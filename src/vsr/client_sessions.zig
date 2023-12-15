const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");

/// There is a slot corresponding to every active client (i.e. a total of clients_max slots).
pub const ReplySlot = struct { index: usize };

/// Track the headers of the latest reply for each active client.
/// Serialized/deserialized to/from the trailer on-disk.
/// For the reply bodies, see ClientReplies.
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
        header: vsr.Header.Reply,
    };

    /// Values are indexes into `entries`.
    const EntriesByClient = std.AutoHashMapUnmanaged(u128, usize);

    const EntriesFree = std.StaticBitSet(constants.clients_max);

    /// Free entries are zeroed, both in `entries` and on-disk.
    entries: []Entry,
    entries_by_client: EntriesByClient,
    entries_free: EntriesFree = EntriesFree.initFull(),

    pub fn init(allocator: mem.Allocator) !ClientSessions {
        var entries_by_client: EntriesByClient = .{};
        errdefer entries_by_client.deinit(allocator);

        try entries_by_client.ensureTotalCapacity(allocator, @as(u32, @intCast(constants.clients_max)));
        assert(entries_by_client.capacity() >= constants.clients_max);

        var entries = try allocator.alloc(Entry, constants.clients_max);
        errdefer allocator.free(entries);
        @memset(entries, std.mem.zeroes(Entry));

        return ClientSessions{
            .entries_by_client = entries_by_client,
            .entries = entries,
        };
    }

    pub fn deinit(client_sessions: *ClientSessions, allocator: mem.Allocator) void {
        client_sessions.entries_by_client.deinit(allocator);
        allocator.free(client_sessions.entries);
    }

    pub fn reset(client_sessions: *ClientSessions) void {
        @memset(client_sessions.entries, std.mem.zeroes(Entry));
        client_sessions.entries_by_client.clearRetainingCapacity();
        client_sessions.entries_free = EntriesFree.initFull();
    }

    /// Size of the buffer needed to encode the client sessions on disk.
    /// (Not rounded up to a sector boundary).
    pub const encode_size = blk: {
        var size_max: usize = 0;

        // First goes the vsr headers for the entries.
        // This takes advantage of the buffer alignment to avoid adding padding for the headers.
        assert(@alignOf(vsr.Header) == 16);
        size_max = std.mem.alignForward(usize, size_max, 16);
        size_max += @sizeOf(vsr.Header) * constants.clients_max;

        // Then follows the session values for the entries.
        assert(@alignOf(u64) == 8);
        size_max = std.mem.alignForward(usize, size_max, 8);
        size_max += @sizeOf(u64) * constants.clients_max;

        break :blk size_max;
    };

    pub fn encode(client_sessions: *const ClientSessions, target: []align(@alignOf(vsr.Header)) u8) u64 {
        assert(target.len >= encode_size);

        var size: u64 = 0;

        // Write all headers:
        assert(@alignOf(vsr.Header) == 16);
        var new_size = std.mem.alignForward(usize, size, 16);
        @memset(target[size..new_size], 0);
        size = new_size;

        for (client_sessions.entries) |*entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.header));
            size += @sizeOf(vsr.Header);
        }

        // Write all sessions:
        assert(@alignOf(u64) == 8);
        new_size = std.mem.alignForward(usize, size, 8);
        @memset(target[size..new_size], 0);
        size = new_size;

        for (client_sessions.entries) |*entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.session));
            size += @sizeOf(u64);
        }

        assert(size == encode_size);
        return size;
    }

    pub fn decode(client_sessions: *ClientSessions, source: []align(@alignOf(vsr.Header)) const u8) void {
        assert(client_sessions.count() == 0);
        assert(client_sessions.entries_free.count() == constants.clients_max);
        for (client_sessions.entries) |*entry| {
            assert(entry.session == 0);
            assert(stdx.zeroed(std.mem.asBytes(&entry.header)));
        }

        var size: u64 = 0;
        assert(source.len > 0);
        assert(source.len <= encode_size);

        assert(@alignOf(vsr.Header) == 16);
        size = std.mem.alignForward(usize, size, 16);
        const headers: []const vsr.Header.Reply = @alignCast(mem.bytesAsSlice(
            vsr.Header.Reply,
            source[size..][0 .. constants.clients_max * @sizeOf(vsr.Header)],
        ));
        size += mem.sliceAsBytes(headers).len;

        assert(@alignOf(u64) == 8);
        size = std.mem.alignForward(usize, size, 8);
        const sessions = mem.bytesAsSlice(
            u64,
            source[size..][0 .. constants.clients_max * @sizeOf(u64)],
        );
        size += mem.sliceAsBytes(sessions).len;

        assert(size == encode_size);

        for (headers, 0..) |*header, i| {
            const session = sessions[i];
            if (session == 0) {
                assert(stdx.zeroed(std.mem.asBytes(header)));
            } else {
                assert(header.valid_checksum());
                assert(header.command == .reply);
                assert(header.commit >= session);

                client_sessions.entries_by_client.putAssumeCapacityNoClobber(header.client, i);
                client_sessions.entries_free.unset(i);
                client_sessions.entries[i] = .{
                    .session = session,
                    .header = header.*,
                };
            }
        }

        assert(constants.clients_max - client_sessions.entries_free.count() ==
            client_sessions.entries_by_client.count());
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

    pub fn get_slot_for_client(client_sessions: *const ClientSessions, client: u128) ?ReplySlot {
        const index = client_sessions.entries_by_client.get(client) orelse return null;
        return ReplySlot{ .index = index };
    }

    pub fn get_slot_for_header(
        client_sessions: *const ClientSessions,
        header: *const vsr.Header.Reply,
    ) ?ReplySlot {
        if (client_sessions.entries_by_client.get(header.client)) |entry_index| {
            const entry = &client_sessions.entries[entry_index];
            if (entry.header.checksum == header.checksum) {
                return ReplySlot{ .index = entry_index };
            }
        }
        return null;
    }

    /// If the entry is from a newly-registered client, the caller is responsible for ensuring
    /// the ClientSessions has available capacity.
    pub fn put(
        client_sessions: *ClientSessions,
        session: u64,
        header: *const vsr.Header.Reply,
    ) ReplySlot {
        assert(session != 0);
        assert(header.command == .reply);
        const client = header.client;

        defer if (constants.verify) assert(client_sessions.entries_by_client.contains(client));

        const entry_gop = client_sessions.entries_by_client.getOrPutAssumeCapacity(client);
        if (entry_gop.found_existing) {
            const entry_index = entry_gop.value_ptr.*;
            assert(!client_sessions.entries_free.isSet(entry_index));

            const existing = &client_sessions.entries[entry_index];
            assert(existing.session == session);
            assert(existing.header.cluster == header.cluster);
            assert(existing.header.client == header.client);
            assert(existing.header.commit < header.commit);

            existing.header = header.*;
            return ReplySlot{ .index = entry_index };
        } else {
            const entry_index = client_sessions.entries_free.findFirstSet().?;
            client_sessions.entries_free.unset(entry_index);

            const e = &client_sessions.entries[entry_index];
            assert(e.session == 0);

            entry_gop.value_ptr.* = entry_index;
            e.session = session;
            e.header = header.*;
            return ReplySlot{ .index = entry_index };
        }
    }

    /// For correctness, it's critical that all replicas evict deterministically:
    /// We cannot depend on `HashMap.capacity()` since `HashMap.ensureTotalCapacity()` may
    /// change across versions of the Zig std lib. We therefore rely on
    /// `constants.clients_max`, which must be the same across all replicas, and must not
    /// change after initializing a cluster.
    /// We also do not depend on `HashMap.valueIterator()` being deterministic here. However,
    /// we do require that all entries have different commit numbers and are iterated.
    /// This ensures that we will always pick the entry with the oldest commit number.
    /// We also check that a client has only one entry in the hash map (or it's buggy).
    pub fn evictee(client_sessions: *const ClientSessions) u128 {
        assert(client_sessions.entries_free.count() == 0);
        assert(client_sessions.count() == constants.clients_max);

        var evictee_: ?*const vsr.Header.Reply = null;
        var iterated: usize = 0;
        var entries = client_sessions.iterator();
        while (entries.next()) |entry| : (iterated += 1) {
            assert(entry.header.command == .reply);
            assert(entry.header.op == entry.header.commit);
            assert(entry.header.commit >= entry.session);

            if (evictee_) |evictee_reply| {
                assert(entry.header.client != evictee_reply.client);
                assert(entry.header.commit != evictee_reply.commit);

                if (entry.header.commit < evictee_reply.commit) {
                    evictee_ = &entry.header;
                }
            } else {
                evictee_ = &entry.header;
            }
        }
        assert(iterated == constants.clients_max);

        return evictee_.?.client;
    }

    pub fn remove(client_sessions: *ClientSessions, client: u128) void {
        const entry_index = client_sessions.entries_by_client.fetchRemove(client).?.value;

        assert(!client_sessions.entries_free.isSet(entry_index));
        client_sessions.entries_free.set(entry_index);

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
                if (entry.session == 0) {
                    assert(it.client_sessions.entries_free.isSet(it.index));
                } else {
                    assert(!it.client_sessions.entries_free.isSet(it.index));
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
