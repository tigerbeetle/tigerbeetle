const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const vr = @import("vr.zig");
const Header = vr.Header;

const MessageBus = @import("message_bus.zig").MessageBus;
const Message = @import("message_bus.zig").Message;
const Operation = @import("state_machine.zig").Operation;
const FixedArrayList = @import("fixed_array_list.zig").FixedArrayList;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

// TODO Be explicit with what we import:
usingnamespace @import("tigerbeetle.zig");

const log = std.log;

pub const Client = struct {
    allocator: *mem.Allocator,
    id: u128,
    cluster: u128,
    configuration: []MessageBus.Address,
    message_bus: *MessageBus,

    // TODO Ask the cluster for our last request number.
    request: u32 = 0,

    batch_manager: BatchManager,

    pub fn init(
        allocator: *mem.Allocator,
        id: u128,
        cluster: u128,
        configuration_raw: []const u8,
        message_bus: *MessageBus,
    ) !Client {
        assert(id > 0);
        assert(cluster > 0);

        const configuration = try vr.parse_configuration(allocator, configuration_raw);
        errdefer allocator.free(configuration);
        assert(configuration.len > 0);

        var self = Client{
            .allocator = allocator,
            .id = id,
            .cluster = cluster,
            .configuration = configuration,
            .message_bus = message_bus,
            .batch_manager = undefined,
        };
        self.batch_manager = try BatchManager.init(self);

        return self;
    }

    pub fn deinit(self: *Client) void {}

    pub fn tick(self: *Client) void {
        self.message_bus.tick();
        self.batch_manager.send_if_none_inflight();
    }

    pub fn batch(
        self: *Client,
        user_data: u64,
        callback: BatchCallback,
        operation: Operation,
        data: []const u8,
    ) void {
        batch_manager.batch(user_data, callback, operation, data) catch {
            // TODO return error
        };
    }

    pub fn on_message(self: *Client, message: *Message) void {
        log.debug("{}: on_message: {}", .{ self.id, message.header });
        if (message.header.invalid()) |reason| {
            log.debug("{}: on_message: invalid ({s})", .{ self.id, reason });
            return;
        }
        if (message.header.cluster != self.cluster) {
            log.warn("{}: on_message: wrong cluster (message.header.cluster={} instead of {})", .{
                self.id,
                message.header.cluster,
                self.cluster,
            });
            return;
        }
        if (message.header.command != .reply) {
            log.warn("{}: on_message: unexpected command {}", .{ self.id, message.header.command });
            return;
        }
        if (message.header.request < self.request) {
            log.debug("{}: on_message: duplicate reply {}", .{ self.id, message.header.request });
            return;
        }

        self.batch_manager.deliver(self.message_bus, message);
    }

    fn init_header(self: Client, header: *Header, operation: Operation) void {
        header.* = .{
            .client = self.id,
            .cluster = self.cluster,
            .request = 1, // TODO: use request numbers properly
            .command = .request,
            .operation = operation,
        };
    }

    fn send_message_to_replicas(self: *Client, message: *Message) void {
        var replica: u16 = 0;
        while (replica < self.configuration.len) : (replica += 1) {
            self.message_bus.send_message_to_replica(message);
        }
    }
};

const BatchManager = struct {
    const groups_max = 1000;
    const batches_per_op = 3;

    const AnyBatch = union(enum) {
        create_accounts: *Batch(.create_accounts, groups_max),
        create_transfers: *Batch(.create_transfers, groups_max),
        commit_transfers: *Batch(.commit_transfers, groups_max),
        lookup_accounts: *Batch(.lookup_accounts, groups_max),
    };

    /// The currently inflight message (if any) is always at the front of this queue.
    send_queue: RingBuffer(AnyBatch, 4 * batches_per_op) = .{},

    create_accounts: BatchRing(Batch(.create_accounts, groups_max), batches_per_op) = .{},
    create_transfers: BatchRing(Batch(.create_transfers, groups_max), batches_per_op) = .{},
    commit_transfers: BatchRing(Batch(.commit_transfers, groups_max), batches_per_op) = .{},
    lookup_accounts: BatchRing(Batch(.lookup_accounts, groups_max), batches_per_op) = .{},

    fn init(client: Client) !BatchManager {
        var self = BatchManager{};

        for (self.create_accounts.batches) |*batch| {
            batch.* = try Batch(.create_accounts, groups_max).init(client);
        }
        for (self.create_transfers.batches) |*batch| {
            batch.* = try Batch(.create_transfers, groups_max).init(client);
        }
        for (self.commit_transfers.batches) |*batch| {
            batch.* = try Batch(.commit_transfers, groups_max).init(client);
        }
        for (self.lookup_accounts.batches) |*batch| {
            batch.* = try Batch(.lookup_accounts, groups_max).init(client);
        }

        return self;
    }

    fn push(
        self: *BatchManager,
        client: *Client,
        user_data: u64,
        callback: BatchCallback,
        operation: Operation,
        data: []const u8,
    ) error{NoSpaceLeft}!void {
        switch (operation) {
            .reserved, .init => unreachable,
            .create_accounts => {
                const batch = self.create_accounts.current() orelse return error.NoSpaceLeft;
                batch.push(user_data, callback, mem.bytesAsSlice(Account, data)) catch {
                    // The current batch is full, so mark it as complete and push it to the send queue.
                    self.create_accounts.mark_current_complete();
                    self.push_to_send_queue(client, .{ .create_accounts = batch });
                    const new_batch = self.create_accounts.current() orelse return error.NoSpaceLeft;
                    // TODO: reject Client.batch calls with data > message_size_max.
                    new_batch.push(user_data, callback, mem.bytesAsSlice(Account, data)) catch unreachable;
                };
            },
            .create_transfers => {
                const batch = self.create_transfers.current() orelse return error.NoSpaceLeft;
                batch.push(user_data, callback, mem.bytesAsSlice(Transfer, data)) catch {
                    // The current batch is full, so mark it as complete and push it to the send queue.
                    self.create_transfers.mark_current_complete();
                    self.push_to_send_queue(client, .{ .create_transfers = batch });
                    const new_batch = self.create_transfers.current() orelse return error.NoSpaceLeft;
                    // TODO: reject Client.batch calls with data > message_size_max.
                    new_batch.push(user_data, callback, mem.bytesAsSlice(Transfer, data)) catch unreachable;
                };
            },
            .commit_transfers => {
                const batch = self.commit_transfers.current() orelse return error.NoSpaceLeft;
                batch.push(user_data, callback, mem.bytesAsSlice(Commit, data)) catch {
                    // The current batch is full, so mark it as complete and push it to the send queue.
                    self.commit_transfers.mark_current_complete();
                    self.push_to_send_queue(client, .{ .commit_transfers = batch });
                    const new_batch = self.commit_transfers.current() orelse return error.NoSpaceLeft;
                    // TODO: reject Client.batch calls with data > message_size_max.
                    new_batch.push(user_data, callback, mem.bytesAsSlice(Commit, data)) catch unreachable;
                };
            },
            .lookup_accounts => {
                const batch = self.lookup_accounts.current() orelse return error.NoSpaceLeft;
                batch.push(user_data, callback, mem.bytesAsSlice(u128, data)) catch {
                    // The current batch is full, so mark it as complete and push it to the send queue.
                    self.lookup_accounts.mark_current_complete();
                    self.push_to_send_queue(client, .{ .lookup_accounts = batch });
                    const new_batch = self.lookup_accounts.current() orelse return error.NoSpaceLeft;
                    // TODO: reject Client.batch calls with data > message_size_max.
                    new_batch.push(user_data, callback, mem.bytesAsSlice(u128, data)) catch unreachable;
                };
            },
        }
    }

    fn push_to_send_queue(self: *BatchManager, client: *Client, any_batch: AnyBatch) void {
        const was_empty = self.send_queue.empty();
        // the send_queue is large enough to hold all batches
        self.send_queue.push(any_batch) catch unreachable;

        if (was_empty) {
            const message = switch (any_batch) {
                .create_accounts => |batch| batch.message,
            };

            const body = message.buffer[@sizeOf(Header)..message.header.size];
            message.header.set_checksum_body(body);
            message.header.set_checksum();
            client.send_message_to_replicas(message);
        }
    }

    fn send_if_none_inflight(self: *BatchManager, client: *Client) void {
        if (!self.send_queue.empty()) return;

        // TODO: instead of sending the first non-empty batch we could send the largest one.
        if (self.create_accounts.current().?.group.items.len > 0) {
            self.create_accounts.mark_current_complete();
            self.push_to_send_queue(client, .{ .create_accounts = batch });
            return;
        }
        if (self.create_transfers.current().?.group.items.len > 0) {
            self.create_transfers.mark_current_complete();
            self.push_to_send_queue(client, .{ .create_transfers = batch });
            return;
        }
        if (self.commit_transfers.current().?.group.items.len > 0) {
            self.commit_transfers.mark_current_complete();
            self.push_to_send_queue(client, .{ .commit_transfers = batch });
            return;
        }
        if (self.lookup_accounts.current().?.group.items.len > 0) {
            self.lookup_accounts.mark_current_complete();
            self.push_to_send_queue(client, .{ .lookup_accounts = batch });
            return;
        }
    }

    fn deliver(self: *BatchManager, client: *Client, message: *Message) void {
        switch (self.send_queue.pop().?) {
            .create_accounts => |*batch| {
                batch.deliver(message);
                assert(self.create_accounts.pop_complete() == batch);
                batch.clear(client);
            },
        }
    }
};

fn BatchRing(comptime T: type, comptime size: usize) type {
    return struct {
        const Self = @This();

        batches: [size]T = undefined,
        // The index of the slot with the first complete/queued batch, if any
        index: usize = 0,
        // The number of complete/queued batches
        count: usize = 0,

        /// Returns the non-queued/inflight batch that is currently accumulating data, if any.
        fn current(self: *Self) ?*T {
            if (self.count == size) return null;
            return &self.buffer[(self.index + self.count) % self.buffer.len];
        }

        fn mark_current_complete(self: *Self) void {
            assert(self.count != size);
            self.count += 1;
        }

        /// Mark the first complete/queued batch no longer complete/queued and return a pointer
        /// to it if any.
        fn pop_complete(self: *Self) ?*T {
            if (self.count == 0) return null;
            const ret = self.buffer[self.index];
            self.index = (self.index + 1) % self.buffer.len;
            self.count -= 1;
            return ret;
        }
    };
}

const BatchCallback = fn (user_data: u64, operation: Operation, results: []const u8) void;

/// This aggregates groups of events into a batch, containing many events, sent as a single message.
/// Each group of events within the batch has a callback.
/// The idea is to have several of these structs, one for each Operation, and perhaps even more
/// than one for each Operation if we want to queue more events while a message is inflight.
fn Batch(comptime operation: Operation, comptime groups_max: usize) type {
    const Event = switch (operation) {
        .init, .reserved => @compileError("invalid operation"),
        .create_accounts => Account,
        .create_transfers => Transfer,
        .commit_transfers => Commit,
        .lookup_accounts => u128,
    };

    const Result = switch (operation) {
        .init, .reserved => @compileError("invalid operation"),
        .create_accounts => CreateAccountResults,
        .create_transfers => CreateTransferResults,
        .commit_transfers => CommitTransferResults,
        .lookup_accounts => Account,
    };

    return struct {
        const Self = @This();

        const Group = struct {
            user_data: u64,
            callback: BatchCallback,
            /// The number of events in the message buffer associated with this group:
            len: u32,
        };

        const Groups = FixedArrayList(Group, groups_max);

        groups: Groups,
        message: *Message,

        fn init(client: Client) !Self {
            const self = Self{
                .groups = try Groups.init(client.allocator),
                .message = client.message_bus.get_message().?,
            };
            client.init_header(self.message.header, operation);
            return self;
        }

        fn deinit(self: *Self, allocator: *mem.Allocator) void {
            self.groups.deinit(allocator);
        }

        /// Clears all state, keeping the allocated memory.
        fn clear(self: *Self, client: *Client) void {
            self.groups.clear();
            client.message_bus.unref(self.message);
            // We just released what should be the last reference to our previous message.
            // TODO: make sure this actually never fails.
            self.message = client.message_bus.get_message().?;
            client.init_header(self.message.header, operation);
        }

        fn push(self: *Self, user_data: u64, callback: BatchCallback, events: []Event) error{NoSpaceLeft}!void {
            const data = mem.sliceAsBytes(events);
            if (self.message.header.size + data.len > config.message_size_max) {
                return error.NoSpaceLeft;
            }
            self.groups.append(.{
                .user_data = user_data,
                .callback = callback,
                .len = @intCast(u32, events.len),
            }) catch return error.NoSpaceLeft; // TODO Use exhaustive switch.
            mem.copy(u8, self.message.buffer[self.message.header.size..], data);
            self.message.header.size += data.len;
        }

        fn deliver(self: *Self, message: *Message) void {
            if (Result == Account) {
                const accounts = mem.bytesAsSlice(Account, message.buffer);
                var account_idx: usize = 0;
                for (self.groups.items) |group| {
                    const group_accounts = accounts[account_idx..][0..group.len];
                    group.callback(group.user_data, operation, mem.sliceAsBytes(group_accounts));
                    account_idx += group.len;
                }
                assert(account_idx == accounts.len);
            } else {
                // Results are sorted by index field.
                const results = mem.bytesAsSlice(Result, message.buffer);
                var result_idx: u32 = 0;
                var item_idx: u32 = 0;
                for (self.groups.items) |group| {
                    const group_first_result = result_idx;
                    while (results[result_idx].index < item_idx + group.len) : (result_idx += 1) {
                        // Mutate the result index in-place to be relative to this group:
                        results[result_idx].index -= item_idx;
                    }
                    group.callback(group.user_data, operation, mem.sliceAsBytes(results[group_first_result..result_idx]));
                    item_idx += group.len;
                }
                assert(result_idx == results.len);
            }
        }
    };
}
