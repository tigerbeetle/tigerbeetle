const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("stdx");

/// There is a slot corresponding to every active client (i.e. a total of clients_max slots).
pub const ReplySlot = struct { index: usize };

/// Track the headers of the latest reply for each active client.
/// Serialized/deserialized to/from the trailer on-disk.
/// For the reply bodies, see ClientReplies.
pub const ClientSessions = struct {
    /// We found two bugs in the VRR paper relating to the client table:
    ///
    /// 1. a correctness bug, where successive client crashes may cause request numbers to collide
    /// for different request payloads, resulting in requests receiving the wrong reply, and
    ///
    /// 2. a liveness bug, where if the client table is updated for request and prepare messages
    /// with the client's latest request number, then the client may be locked out from the cluster
    /// if the request is ever reordered through a view change.
    ///
    /// We therefore take a different approach with the implementation of our client table, to:
    ///
    /// 1. register client sessions explicitly through the state machine to ensure that
    ///    session numbers always increase, and
    ///
    /// 2. make a more careful distinction between uncommitted and committed request numbers,
    ///    considering that uncommitted requests may not survive a view change.
    pub const Entry = struct {
        /// The client's session number as committed to the cluster by a register request.
        session: u64,

        /// The header of the reply corresponding to the client's latest committed request.
        header: vsr.Header.Reply,
    };

    /// Values are indexes into `entries`.
    const EntriesByClient = std.AutoHashMapUnmanaged(u128, usize);

    const EntriesPresent = stdx.BitSetType(constants.clients_max);

    /// Free entries are zeroed, both in `entries` and on-disk.
    entries: []Entry,
    entries_by_client: EntriesByClient,
    entries_present: EntriesPresent = .{},

    pub fn init(allocator: mem.Allocator) !ClientSessions {
        var entries_by_client: EntriesByClient = .{};
        errdefer entries_by_client.deinit(allocator);

        try entries_by_client.ensureTotalCapacity(allocator, @intCast(constants.clients_max));
        assert(entries_by_client.capacity() >= constants.clients_max);

        const entries = try allocator.alloc(Entry, constants.clients_max);
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
        client_sessions.entries_present = .{};
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

        // For encoding/decoding simplicity, the ClientSessions always fits in a single block.
        assert(size_max <= constants.block_size - @sizeOf(vsr.Header));

        break :blk size_max;
    };

    pub fn encode(
        client_sessions: *const ClientSessions,
        target: []align(@alignOf(vsr.Header)) u8,
    ) u64 {
        assert(target.len >= encode_size);

        var size: u64 = 0;

        // Write all headers:
        comptime assert(@alignOf(vsr.Header) == 16);
        var new_size = std.mem.alignForward(usize, size, @alignOf(vsr.Header));
        @memset(target[size..new_size], 0);
        size = new_size;

        for (client_sessions.entries) |*entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.header));
            size += @sizeOf(vsr.Header);
        }

        // Write all sessions:
        comptime assert(@alignOf(u64) == 8);
        new_size = std.mem.alignForward(usize, size, @alignOf(u64));
        @memset(target[size..new_size], 0);
        size = new_size;

        for (client_sessions.entries) |*entry| {
            stdx.copy_disjoint(.inexact, u8, target[size..], mem.asBytes(&entry.session));
            size += @sizeOf(u64);
        }

        assert(size == encode_size);
        return size;
    }

    pub fn decode(
        client_sessions: *ClientSessions,
        source: []align(@alignOf(vsr.Header)) const u8,
    ) void {
        assert(client_sessions.count() == 0);
        assert(client_sessions.entries_present.empty());
        for (client_sessions.entries) |*entry| {
            assert(entry.session == 0);
            assert(stdx.zeroed(std.mem.asBytes(&entry.header)));
        }

        var size: u64 = 0;
        assert(source.len > 0);
        assert(source.len <= encode_size);

        comptime assert(@alignOf(vsr.Header) == 16);
        size = std.mem.alignForward(usize, size, @alignOf(vsr.Header));
        const headers: []const vsr.Header.Reply = @alignCast(mem.bytesAsSlice(
            vsr.Header.Reply,
            source[size..][0 .. constants.clients_max * @sizeOf(vsr.Header)],
        ));
        size += mem.sliceAsBytes(headers).len;

        comptime assert(@alignOf(u64) == 8);
        size = std.mem.alignForward(usize, size, @alignOf(u64));
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
                client_sessions.entries_present.set(i);
                client_sessions.entries[i] = .{
                    .session = session,
                    .header = header.*,
                };
            }
        }

        assert(
            client_sessions.entries_present.count() == client_sessions.entries_by_client.count(),
        );
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

    /// Returns the client's slot, if the client has a session and it is `session`.
    /// Messages from another incarnation of the client id (for example, the pings that a client
    /// sends with session=0 before it registers) do not match.
    pub fn get_slot_for_session(
        client_sessions: *const ClientSessions,
        client: u128,
        session: u64,
    ) ?ReplySlot {
        assert(client != 0);

        const index = client_sessions.entries_by_client.get(client) orelse return null;
        const entry = &client_sessions.entries[index];
        assert(entry.session != 0);
        assert(entry.header.client == client);

        if (entry.session != session) return null;
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

        defer assert(client_sessions.entries_by_client.contains(client));

        const entry_gop = client_sessions.entries_by_client.getOrPutAssumeCapacity(client);
        if (entry_gop.found_existing) {
            const entry_index = entry_gop.value_ptr.*;
            assert(client_sessions.entries_present.is_set(entry_index));

            const existing = &client_sessions.entries[entry_index];
            assert(existing.session == session);
            assert(existing.header.cluster == header.cluster);
            assert(existing.header.client == header.client);
            assert(existing.header.commit < header.commit);

            existing.header = header.*;
            return ReplySlot{ .index = entry_index };
        } else {
            const entry_index = client_sessions.entries_present.first_unset().?;
            client_sessions.entries_present.set(entry_index);

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
        assert(client_sessions.entries_present.full());
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

    pub const EvictionPath = enum {
        /// The register's recorded evictee (see `RegisterRequest.evictee`).
        hint,
        /// The register has no recorded evictee: `evictee()`.
        fallback_no_hint,
        /// The register's recorded evictee no longer has a session: `evictee()`.
        fallback_hint_gone,
    };

    pub const Eviction = struct {
        client: u128,
        path: EvictionPath,
    };

    /// Returns the session that a register evicts from the full client table, at commit.
    /// This depends only on the prepare (`hint` is its `RegisterRequest.evictee`) and on
    /// replicated state, so every replica makes the same choice.
    pub fn evictee_for_register(client_sessions: *const ClientSessions, hint: u128) Eviction {
        assert(client_sessions.entries_present.full());
        assert(client_sessions.count() == constants.clients_max);

        if (hint == 0) {
            return .{ .client = client_sessions.evictee(), .path = .fallback_no_hint };
        }
        if (client_sessions.entries_by_client.contains(hint)) {
            return .{ .client = hint, .path = .hint };
        }
        return .{ .client = client_sessions.evictee(), .path = .fallback_hint_gone };
    }

    /// Returns the client whose session the primary should record as the evictee of a register
    /// that it is preparing, or null to leave the choice to `evictee()` at commit.
    ///
    /// Unlike `evictee()`, this uses replica-local information, so it must only be called by the
    /// primary when preparing, and its result must reach other replicas through the prepare.
    ///
    /// The candidates are the sessions that will still exist when the register commits, after the
    /// prepares ahead of it in the pipeline (`projection`). The candidate whose client has been
    /// silent the longest (`ClientLiveness.silence()`) is chosen: a client that is alive keeps
    /// pinging every replica, while a client that has died without closing its session falls
    /// silent forever. Ties are broken by the oldest commit, like `evictee()`. Commits are unique,
    /// so the choice does not depend on the iteration order.
    ///
    /// Returns null if no candidate has a liveness sample (for example, a primary that has just
    /// restarted): then every candidate is silent since the epoch, the tie-break decides, and
    /// that is the choice that `evictee()` makes at commit anyway.
    pub fn evictee_by_liveness(
        client_sessions: *const ClientSessions,
        client_liveness: *const ClientLiveness,
        options: struct {
            projection: *const Projection,
            /// The local monotonic time, comparable with `ClientLiveness` samples.
            now: stdx.Instant,
        },
    ) ?u128 {
        const projection = options.projection;
        assert(projection.client_sessions == client_sessions);
        assert(projection.count() == constants.clients_max);

        var evictee_index: ?usize = null;
        var evictee_silence_ns: u64 = 0;
        var sampled_any: bool = false;
        for (client_sessions.entries, 0..) |*entry, index| {
            if (!projection.present.is_set(index)) continue;
            assert(client_sessions.entries_present.is_set(index));
            assert(entry.session != 0);
            assert(entry.header.command == .reply);

            const client = entry.header.client;
            assert(client != 0);
            // Clients that registers ahead add have no session yet, so they have no liveness
            // sample: if they were candidates, they would read as silent since the epoch, and the
            // newest registration would be chosen first.
            assert(!projection.added_contains(client));

            const silence = client_liveness.silence(.{ .index = index }, client, options.now);
            sampled_any = sampled_any or silence.sampled;

            const commit = projection.commits[index];
            if (evictee_index) |evictee_index_| {
                const evictee_commit = projection.commits[evictee_index_];
                assert(evictee_commit != commit);

                if (silence.ns > evictee_silence_ns or
                    (silence.ns == evictee_silence_ns and commit < evictee_commit))
                {
                    evictee_index = index;
                    evictee_silence_ns = silence.ns;
                }
            } else {
                evictee_index = index;
                evictee_silence_ns = silence.ns;
            }
        }

        if (!sampled_any) return null;
        const evictee_entry = &client_sessions.entries[evictee_index.?];
        assert(projection.present.is_set(evictee_index.?));
        return evictee_entry.header.client;
    }

    /// The client table as it will be once the prepares in the primary's pipeline have
    /// committed, in op order. It tracks occupancy and membership, and the commit order that
    /// `evictee()` uses, so that the primary only chooses an evictee for a register that will
    /// evict, and never one that a register ahead already evicts.
    ///
    /// `apply()` mirrors the client table updates of `commit_op()` prong for prong, so the
    /// projection is exact, given the same prepares.
    pub const Projection = struct {
        client_sessions: *const ClientSessions,
        /// The entries of `client_sessions` that are still present.
        present: EntriesPresent,
        /// The commit of the latest request of each entry, indexed like `entries`.
        commits: [constants.clients_max]u64,
        /// The clients that registers ahead add. They have no slot (nor a session) yet.
        added: stdx.BoundedArrayType(Added, constants.pipeline_prepare_queue_max) = .{},

        const Added = struct { client: u128, commit: u64 };

        pub fn init(client_sessions: *const ClientSessions) Projection {
            var commits: [constants.clients_max]u64 = @splat(0);
            for (client_sessions.entries, 0..) |*entry, index| {
                if (client_sessions.entries_present.is_set(index)) {
                    assert(entry.session != 0);
                    assert(entry.header.commit >= entry.session);
                    commits[index] = entry.header.commit;
                } else {
                    assert(entry.session == 0);
                }
            }
            return .{
                .client_sessions = client_sessions,
                .present = client_sessions.entries_present,
                .commits = commits,
            };
        }

        pub fn count(projection: *const Projection) usize {
            const result = projection.present.count() + projection.added.count();
            assert(result <= constants.clients_max);
            return result;
        }

        pub fn contains(projection: *const Projection, client: u128) bool {
            assert(client != 0);
            return projection.present_index(client) != null or projection.added_contains(client);
        }

        /// Applies the prepare of the next op. `register_request` is the prepare's body if it is
        /// a register, and null otherwise.
        pub fn apply(
            projection: *Projection,
            header: *const vsr.Header.Prepare,
            register_request: ?*const vsr.RegisterRequest,
        ) void {
            assert(header.command == .prepare);
            assert(header.op > 0);
            assert((header.operation == .register) == (register_request != null));

            // Every named operation is listed (and there is no `else`), so that a new one fails to
            // compile until it is projected. Mirrors the client table switch in `commit_op()`.
            switch (header.operation) {
                .reserved, .root => unreachable,
                .register => projection.apply_register(header, register_request.?),
                .pulse, .upgrade => assert(header.client == 0),
                .reconfigure => projection.apply_request(header),
                .noop => projection.apply_request(header),
                _ => projection.apply_request(header),
            }
        }

        fn apply_register(
            projection: *Projection,
            header: *const vsr.Header.Prepare,
            register_request: *const vsr.RegisterRequest,
        ) void {
            assert(header.operation == .register);
            assert(header.client != 0);
            assert(register_request.evictee != header.client);
            // The primary never prepares a register for a client with a session (it resends the
            // reply instead), and drops duplicates of the requests that are being prepared. After
            // a view change, the pipeline is a suffix of the log, so any eviction of this client
            // that an older primary relied on is in the pipeline too, and is projected first.
            assert(!projection.contains(header.client));

            // Like `client_table_entry_create()`, with `evictee_for_register()`.
            if (projection.count() == constants.clients_max) {
                const hint = register_request.evictee;
                const victim = if (hint != 0 and projection.contains(hint))
                    hint
                else
                    projection.fallback();
                projection.remove(victim);
            }
            assert(projection.count() < constants.clients_max);

            projection.added.push(.{ .client = header.client, .commit = header.op });
        }

        fn apply_request(projection: *Projection, header: *const vsr.Header.Prepare) void {
            assert(header.operation != .register);
            assert(header.client != 0);

            // Like `client_table_entry_update()`.
            if (projection.present_index(header.client)) |index| {
                assert(projection.commits[index] < header.op);
                projection.commits[index] = header.op;
                return;
            }
            for (projection.added.slice()) |*added| {
                if (added.client == header.client) {
                    assert(added.commit < header.op);
                    added.commit = header.op;
                    return;
                }
            }
            // The session was evicted (by a register ahead) while this request was prepared.
        }

        /// The session with the oldest commit, like `evictee()`.
        fn fallback(projection: *const Projection) u128 {
            assert(projection.count() == constants.clients_max);

            var victim: ?Added = null;
            for (projection.client_sessions.entries, 0..) |*entry, index| {
                if (!projection.present.is_set(index)) continue;
                const candidate: Added = .{
                    .client = entry.header.client,
                    .commit = projection.commits[index],
                };
                if (victim == null or candidate.commit < victim.?.commit) victim = candidate;
            }
            for (projection.added.const_slice()) |candidate| {
                assert(victim == null or candidate.commit != victim.?.commit);
                if (victim == null or candidate.commit < victim.?.commit) victim = candidate;
            }
            return victim.?.client;
        }

        fn remove(projection: *Projection, client: u128) void {
            assert(projection.contains(client));

            if (projection.present_index(client)) |index| {
                projection.present.unset(index);
            } else {
                for (projection.added.const_slice(), 0..) |added, index| {
                    if (added.client == client) {
                        _ = projection.added.swap_remove(index);
                        break;
                    }
                } else unreachable;
            }
            assert(!projection.contains(client));
        }

        fn present_index(projection: *const Projection, client: u128) ?usize {
            const index = projection.client_sessions.entries_by_client.get(client) orelse
                return null;
            if (!projection.present.is_set(index)) return null;
            return index;
        }

        fn added_contains(projection: *const Projection, client: u128) bool {
            for (projection.added.const_slice()) |added| {
                if (added.client == client) return true;
            }
            return false;
        }
    };

    pub fn remove(client_sessions: *ClientSessions, client: u128) void {
        const entry_index = client_sessions.entries_by_client.fetchRemove(client).?.value;

        assert(client_sessions.entries_present.is_set(entry_index));
        client_sessions.entries_present.unset(entry_index);

        assert(client_sessions.entries[entry_index].header.client == client);
        client_sessions.entries[entry_index] = std.mem.zeroes(Entry);

        assert(!client_sessions.entries_by_client.contains(client));
    }

    pub const Iterator = struct {
        client_sessions: *const ClientSessions,
        index: usize = 0,

        pub fn next(it: *Iterator) ?*const Entry {
            while (it.index < it.client_sessions.entries.len) {
                defer it.index += 1;

                const entry = &it.client_sessions.entries[it.index];
                if (entry.session == 0) {
                    assert(!it.client_sessions.entries_present.is_set(it.index));
                } else {
                    assert(it.client_sessions.entries_present.is_set(it.index));
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

/// Replica-local record of when this replica last heard from the client of each session, on its
/// monotonic clock. Indexed by `ReplySlot`, so it is bounded by `clients_max`.
///
/// This is neither replicated nor persisted: replicas hear different messages at different times.
/// Therefore it must never influence the result of a commit directly. Only the primary reads it,
/// when it chooses the evictee to record in the body of a register prepare.
///
/// Every replica (not only the primary) tracks liveness, since clients ping every replica: a
/// backup that becomes primary can then choose evictees from its first prepare, rather than only
/// after a full ping interval.
///
/// The samples are:
/// - a request or a ping_client from the client, in its current session (`heard()`), and
/// - a commit of the client's request (`committed()`), so that a client which has just registered
///   is protected before its first ping. Ops up to `op_replay_max`, the journal head when the
///   client table was loaded, are replayed from the WAL rather than committed live, so they are
///   not samples: a replay of a long-dead client's requests must not make it look alive.
///
/// A session without a sample is treated as if its client was heard at `epoch`, the instant at
/// which the client table was loaded (when the replica opened, or after state sync). This biases
/// towards protecting sessions.
///
/// Two asymmetries are deliberate, and both only bias towards "no sample":
/// - After state sync, ops in (sync checkpoint, op_replay_max] are committed live by
///   `commit_journal()`, but are not samples, while ops repaired afterwards are.
/// - `op` may advance between the sync superblock update and the reset, so a few more ops may stay
///   without a sample.
pub const ClientLiveness = struct {
    entries: [constants.clients_max]Entry = @splat(.{ .client = 0, .heard = .{ .ns = 0 } }),
    /// The instant at which the client table was loaded.
    epoch: stdx.Instant = .{ .ns = 0 },
    /// Commits of ops up to (and including) this op are replayed from the WAL, so they are not
    /// signs of life.
    op_replay_max: u64 = 0,

    pub const Entry = struct {
        /// The client that `heard` belongs to, or 0 if there is no sample in this slot.
        /// Slots are reused by later sessions, so a sample is only valid for the same client.
        client: u128,
        heard: stdx.Instant,
    };

    pub const Silence = struct {
        ns: u64,
        /// Whether the silence is measured from a sample, rather than from `epoch`.
        sampled: bool,
    };

    /// Called whenever the client table is loaded, since the slots may now belong to different
    /// sessions.
    pub fn reset(client_liveness: *ClientLiveness, options: struct {
        now: stdx.Instant,
        op_replay_max: u64,
        commit_min: u64,
    }) void {
        assert(options.now.ns >= client_liveness.epoch.ns);
        assert(options.op_replay_max >= options.commit_min);

        client_liveness.* = .{
            .epoch = options.now,
            .op_replay_max = options.op_replay_max,
        };
    }

    /// Records a message (a request or a ping_client) from the client.
    pub fn heard(
        client_liveness: *ClientLiveness,
        slot: ReplySlot,
        client: u128,
        now: stdx.Instant,
    ) void {
        assert(slot.index < constants.clients_max);
        assert(client != 0);
        assert(now.ns >= client_liveness.epoch.ns);

        const entry = &client_liveness.entries[slot.index];
        if (entry.client == client) assert(entry.heard.ns <= now.ns);
        entry.* = .{ .client = client, .heard = now };
    }

    /// Records the commit of the client's request (including its register) at `op`.
    pub fn committed(
        client_liveness: *ClientLiveness,
        slot: ReplySlot,
        client: u128,
        op: u64,
        now: stdx.Instant,
    ) void {
        assert(slot.index < constants.clients_max);
        assert(client != 0);
        assert(op > 0);

        if (op <= client_liveness.op_replay_max) return; // Replayed from the WAL.
        client_liveness.heard(slot, client, now);
    }

    /// Returns how long the client of the session in `slot` has been silent.
    pub fn silence(
        client_liveness: *const ClientLiveness,
        slot: ReplySlot,
        client: u128,
        now: stdx.Instant,
    ) Silence {
        assert(slot.index < constants.clients_max);
        assert(client != 0);
        assert(now.ns >= client_liveness.epoch.ns);

        const entry = &client_liveness.entries[slot.index];
        if (entry.client == client) {
            assert(entry.heard.ns >= client_liveness.epoch.ns);
            assert(entry.heard.ns <= now.ns);
            return .{ .ns = entry.heard.elapsed(now).ns, .sampled = true };
        } else {
            return .{ .ns = client_liveness.epoch.elapsed(now).ns, .sampled = false };
        }
    }
};

const TestSessions = struct {
    client_sessions: ClientSessions,
    client_liveness: ClientLiveness = .{},

    // The local monotonic clock, in seconds since the client table was loaded (the epoch).
    const epoch_s = 1_000;

    fn init(options: struct { op_replay_max: u64 = 0 }) !TestSessions {
        var t: TestSessions = .{
            .client_sessions = try ClientSessions.init(std.testing.allocator),
        };
        t.client_liveness.reset(.{
            .now = instant(0),
            .op_replay_max = options.op_replay_max,
            .commit_min = 0,
        });
        return t;
    }

    fn deinit(t: *TestSessions) void {
        t.client_sessions.deinit(std.testing.allocator);
    }

    fn instant(since_epoch_s: u64) stdx.Instant {
        return .{ .ns = (epoch_s + since_epoch_s) * std.time.ns_per_s };
    }

    /// Adds a session for `client`, whose latest request committed at `commit`.
    fn session(t: *TestSessions, client: u128, commit: u64) void {
        var header: vsr.Header.Reply = .{
            .command = .reply,
            .cluster = 0,
            .view = 0,
            .release = .{ .value = 1 },
            .replica = 0,
            .request_checksum = 0,
            .client = client,
            .op = commit,
            .commit = commit,
            .timestamp = commit,
            .request = 0,
            .operation = .register,
        };
        header.set_checksum();
        _ = t.client_sessions.put(commit, &header);
    }

    fn slot(t: *const TestSessions, client: u128) ReplySlot {
        return t.client_sessions.get_slot_for_client(client).?;
    }

    fn heard(t: *TestSessions, client: u128, since_epoch_s: u64) void {
        t.client_liveness.heard(t.slot(client), client, instant(since_epoch_s));
    }

    fn committed(t: *TestSessions, client: u128, op: u64, since_epoch_s: u64) void {
        t.client_liveness.committed(t.slot(client), client, op, instant(since_epoch_s));
    }

    fn silence(t: *const TestSessions, client: u128, since_epoch_s: u64) ClientLiveness.Silence {
        return t.client_liveness.silence(t.slot(client), client, instant(since_epoch_s));
    }

    /// Fills the rest of the table with live clients (client = commit, from `client_first`), which
    /// were heard at `since_epoch_s`.
    fn fill(t: *TestSessions, options: struct { client_first: u128, heard_s: u64 }) void {
        var client = options.client_first;
        while (t.client_sessions.count() < constants.clients_max) : (client += 1) {
            t.session(client, @intCast(client));
            t.heard(client, options.heard_s);
        }
        assert(t.client_sessions.entries_present.full());
    }

    fn evictee(t: *const TestSessions, projection: *const ClientSessions.Projection) ?u128 {
        return t.client_sessions.evictee_by_liveness(&t.client_liveness, .{
            .projection = projection,
            .now = instant(now_s),
        });
    }

    fn evictee_now(t: *const TestSessions) ?u128 {
        const projection = ClientSessions.Projection.init(&t.client_sessions);
        return t.evictee(&projection);
    }

    const now_s = 60;
};

fn test_prepare(options: struct {
    op: u64,
    operation: vsr.Operation,
    client: u128,
}) vsr.Header.Prepare {
    return .{
        .cluster = 0,
        .view = 0,
        .release = .{ .value = 1 },
        .command = .prepare,
        .parent = 0,
        .request_checksum = 0,
        .checkpoint_id = 0,
        .client = options.client,
        .op = options.op,
        .commit = options.op - 1,
        .timestamp = options.op,
        .request = if (options.operation == .register) 0 else 1,
        .operation = options.operation,
    };
}

fn test_apply_register(
    projection: *ClientSessions.Projection,
    options: struct { op: u64, client: u128, evictee: u128 },
) void {
    const header = test_prepare(.{
        .op = options.op,
        .operation = .register,
        .client = options.client,
    });
    const register_request: vsr.RegisterRequest = .{
        .batch_size_limit = 1,
        .evictee = options.evictee,
    };
    projection.apply(&header, &register_request);
}

fn test_apply_request(
    projection: *ClientSessions.Projection,
    options: struct { op: u64, operation: vsr.Operation, client: u128 },
) void {
    assert(options.operation != .register);
    const header = test_prepare(.{
        .op = options.op,
        .operation = options.operation,
        .client = options.client,
    });
    projection.apply(&header, null);
}

test "ClientSessions.evictee_by_liveness: live idle client survives dead sessions" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    // Client 1 is alive but idle: it committed first, but it is still pinging.
    t.session(1, 1);
    t.heard(1, 40);
    // Clients 2 and 3 died without closing their sessions, after committing more recently.
    t.session(2, 2);
    t.heard(2, 10);
    t.session(3, 3);
    t.heard(3, 20);
    t.fill(.{ .client_first = 4, .heard_s = 59 });

    // `evictee()` would choose client 1 (the oldest commit), but client 2 is silent the longest.
    try std.testing.expectEqual(1, t.client_sessions.evictee());
    try std.testing.expectEqual(2, t.evictee_now());
}

test "ClientSessions.evictee_by_liveness: fresh register protected by its commit sample" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    // Client 1 was heard 50s ago.
    t.session(1, 1);
    t.heard(1, 10);
    // Client 2 has just registered (committed live, 10s ago), and has not pinged yet.
    t.session(2, 2);
    t.committed(2, 2, 50);
    t.fill(.{ .client_first = 3, .heard_s = 59 });

    try std.testing.expectEqual(1, t.evictee_now());
}

test "ClientSessions.evictee_by_liveness: WAL replay does not stamp" {
    var t = try TestSessions.init(.{ .op_replay_max = 100 });
    defer t.deinit();

    // Client 1 is alive: it pinged after the replica opened.
    t.session(1, 1);
    t.heard(1, 5);
    // Client 2 died long ago, but its last request was replayed from the WAL after open.
    t.session(2, 2);
    t.committed(2, 2, 8);
    t.fill(.{ .client_first = 3, .heard_s = 59 });

    try std.testing.expectEqual(2, t.evictee_now());
}

test "ClientSessions.evictee_by_liveness: nothing sampled returns null" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    for (1..constants.clients_max + 1) |client| t.session(client, client);
    // Every candidate is silent since the epoch: leave the choice to `evictee()` at commit.
    try std.testing.expectEqual(null, t.evictee_now());

    // A sample of a different client in the same slot (the slot was reused) does not count.
    t.client_liveness.heard(t.slot(1), constants.clients_max + 1, TestSessions.instant(59));
    try std.testing.expectEqual(null, t.evictee_now());

    t.heard(3, 59);
    try std.testing.expectEqual(1, t.evictee_now());
}

test "ClientSessions.evictee_by_liveness: ties break by the oldest commit" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    // Add the sessions in descending commit order, so that slot order and commit order disagree.
    var commit: u64 = constants.clients_max;
    while (commit > 0) : (commit -= 1) {
        const client: u128 = 100 + commit;
        t.session(client, commit);
        t.heard(client, 30);
    }
    try std.testing.expectEqual(101, t.evictee_now());
}

test "ClientSessions.evictee_by_liveness: a reused slot does not inherit liveness" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    // Every client is alive, apart from the one in the middle.
    const client_dead: u128 = @divFloor(constants.clients_max, 2) + 1;
    for (1..constants.clients_max + 1) |client| {
        t.session(client, client);
        if (client != client_dead) t.heard(client, 50);
    }
    try std.testing.expectEqual(client_dead, t.evictee_now());

    // Once the dead session is evicted, its slot is reused by a new client, which must not
    // inherit the liveness of the dead one. (Nor does the new client have a sample yet.)
    t.client_sessions.remove(client_dead);
    t.session(constants.clients_max + 1, constants.clients_max + 1);
    try std.testing.expectEqual(t.slot(constants.clients_max + 1).index, client_dead - 1);
    try std.testing.expectEqual(constants.clients_max + 1, t.evictee_now());
}

test "ClientSessions.evictee_for_register: hint gone falls back" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    for (1..constants.clients_max + 1) |client| t.session(client, client);

    try std.testing.expectEqual(
        ClientSessions.Eviction{ .client = 3, .path = .hint },
        t.client_sessions.evictee_for_register(3),
    );
    try std.testing.expectEqual(
        ClientSessions.Eviction{ .client = 1, .path = .fallback_no_hint },
        t.client_sessions.evictee_for_register(0),
    );
    try std.testing.expectEqual(
        ClientSessions.Eviction{ .client = 1, .path = .fallback_hint_gone },
        t.client_sessions.evictee_for_register(constants.clients_max + 1),
    );
}

test "Projection: fallback victim of a hint-less register ahead is excluded" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    // Client 1 has the oldest commit, and is also silent the longest.
    t.session(1, 1);
    t.heard(1, 1);
    t.session(2, 2);
    t.heard(2, 2);
    t.fill(.{ .client_first = 3, .heard_s = 59 });

    var projection = ClientSessions.Projection.init(&t.client_sessions);
    try std.testing.expectEqual(1, t.evictee(&projection));

    // A register ahead (for which this replica recorded no evictee) evicts client 1 at commit.
    test_apply_register(&projection, .{ .op = 100, .client = 1000, .evictee = 0 });
    try std.testing.expectEqual(constants.clients_max, projection.count());
    try std.testing.expect(!projection.contains(1));
    try std.testing.expect(projection.contains(1000));
    try std.testing.expectEqual(2, t.evictee(&projection));
}

test "Projection: requests ahead reorder the projected fallback" {
    // Every operation that `commit_op()` passes to `client_table_entry_update()`.
    const operations = [_]vsr.Operation{ @enumFromInt(200), .noop, .reconfigure };
    for (operations) |operation| {
        var t = try TestSessions.init(.{});
        defer t.deinit();

        for (1..constants.clients_max + 1) |client| t.session(client, client);

        var projection = ClientSessions.Projection.init(&t.client_sessions);
        // Client 1 (the oldest commit) commits a request ahead, so client 2 is the oldest.
        test_apply_request(&projection, .{ .op = 100, .operation = operation, .client = 1 });
        test_apply_register(&projection, .{ .op = 101, .client = 1000, .evictee = 0 });
        try std.testing.expect(projection.contains(1));
        try std.testing.expect(!projection.contains(2));
        try std.testing.expectEqual(constants.clients_max, projection.count());
    }
}

test "Projection: request of a client evicted ahead is a no-op" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    for (1..constants.clients_max + 1) |client| t.session(client, client);

    var projection = ClientSessions.Projection.init(&t.client_sessions);
    test_apply_register(&projection, .{ .op = 100, .client = 1000, .evictee = 3 });
    try std.testing.expect(!projection.contains(3));
    // The session of client 3 was evicted while its request was being prepared.
    test_apply_request(&projection, .{ .op = 101, .operation = @enumFromInt(200), .client = 3 });
    try std.testing.expect(!projection.contains(3));
    try std.testing.expectEqual(constants.clients_max, projection.count());

    test_apply_register(&projection, .{ .op = 102, .client = 1001, .evictee = 0 });
    try std.testing.expect(!projection.contains(1));
    try std.testing.expect(projection.contains(2));
    try std.testing.expectEqual(constants.clients_max, projection.count());
}

test "Projection: added clients are never candidates" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    // Every session is alive, so any session without a sample would be chosen first.
    for (1..constants.clients_max + 1) |client| {
        t.session(client, client);
        t.heard(client, 50 + @as(u64, @intCast(client)));
    }

    var projection = ClientSessions.Projection.init(&t.client_sessions);
    test_apply_register(&projection, .{ .op = 100, .client = 1000, .evictee = 1 });
    try std.testing.expect(projection.contains(1000));
    try std.testing.expect(!projection.contains(1));
    // Client 1000 is not a candidate, but it is part of the fallback.
    try std.testing.expectEqual(2, t.evictee(&projection));
    test_apply_register(&projection, .{ .op = 101, .client = 1001, .evictee = 0 });
    try std.testing.expect(!projection.contains(2));
    try std.testing.expect(projection.contains(1000));
}

test "ClientLiveness: silence" {
    var t = try TestSessions.init(.{ .op_replay_max = 20 });
    defer t.deinit();

    t.session(1, 10);
    t.session(2, 30);
    t.session(3, 40);

    // Without a sample, a session is silent since the epoch.
    try std.testing.expectEqual(
        ClientLiveness.Silence{ .ns = 60 * std.time.ns_per_s, .sampled = false },
        t.silence(1, 60),
    );

    t.heard(1, 50);
    try std.testing.expectEqual(
        ClientLiveness.Silence{ .ns = 10 * std.time.ns_per_s, .sampled = true },
        t.silence(1, 60),
    );

    // A live commit is a sample, but an op replayed from the WAL is not.
    t.committed(2, 30, 40);
    t.committed(3, 20, 40);
    try std.testing.expectEqual(
        ClientLiveness.Silence{ .ns = 20 * std.time.ns_per_s, .sampled = true },
        t.silence(2, 60),
    );
    try std.testing.expectEqual(
        ClientLiveness.Silence{ .ns = 60 * std.time.ns_per_s, .sampled = false },
        t.silence(3, 60),
    );

    // A sample of a different client in the same slot (the slot was reused) does not count.
    t.client_sessions.remove(1);
    t.session(4, 50);
    try std.testing.expectEqual(t.slot(4).index, 0);
    try std.testing.expectEqual(
        ClientLiveness.Silence{ .ns = 60 * std.time.ns_per_s, .sampled = false },
        t.silence(4, 60),
    );
}

test "ClientLiveness: reset clears samples" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    t.session(1, 10);
    t.heard(1, 50);
    try std.testing.expect(t.silence(1, 60).sampled);

    // The client table is loaded again (state sync): the slot may now hold another session of
    // the same client id, which the sample must not carry over to.
    t.client_liveness.reset(.{
        .now = TestSessions.instant(60),
        .op_replay_max = 10,
        .commit_min = 10,
    });
    try std.testing.expectEqual(
        ClientLiveness.Silence{ .ns = 10 * std.time.ns_per_s, .sampled = false },
        t.silence(1, 70),
    );
}

test "ClientLiveness: a message from another session is not a sample" {
    var t = try TestSessions.init(.{});
    defer t.deinit();

    t.session(1, 10);
    // The session number is the commit of the register.
    try std.testing.expectEqual(t.client_sessions.get_slot_for_session(1, 10), t.slot(1));
    // For example, a ping that the client sent with session=0 before it registered, or a late
    // message from an older (evicted) session.
    try std.testing.expectEqual(t.client_sessions.get_slot_for_session(1, 0), null);
    try std.testing.expectEqual(t.client_sessions.get_slot_for_session(1, 9), null);
    try std.testing.expectEqual(t.client_sessions.get_slot_for_session(2, 10), null);
}
