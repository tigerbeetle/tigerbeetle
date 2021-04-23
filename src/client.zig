const std = @import("std");
const assert = std.debug.assert;

const vr = @import("vr.zig");

const MessageBus = @import("message_bus.zig").MessageBus;
const Message = @import("message_bus.zig").Message;
const Operation = @import("state_machine.zig").Operation;
const FixedArrayList = @import("fixed_array_list.zig").FixedArrayList;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

// TODO Be explicit with what we import:
usingnamespace @import("tigerbeetle.zig");

const log = std.log;

pub const Client = struct {
    allocator: *std.mem.Allocator,
    id: u128,
    cluster: u128,
    configuration: []MessageBus.Address,
    message_bus: *MessageBus,

    // TODO Ask the cluster for our last request number.
    request: u32 = 0,

    batch_manager: BatchManager,

    pub fn init(
        allocator: *std.mem.Allocator,
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

        const self = Client{
            .allocator = allocator,
            .id = id,
            .cluster = cluster,
            .configuration = configuration,
            .message_bus = message_bus,
            .batch_manager = try BatchManager.init(allocator, message_bus),
        };

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
};

const BatchManager = struct {
    const groups_max = 1000;
    const batches_per_op = 3;

    const AnyBatch = union(enum) {
        create_accounts: *Batch(.create_accounts, groups_max),
        // TODO
    };

    /// The currently inflight message (if any) is always at the front of this queue.
    send_queue: RingBuffer(AnyBatch, 4 * batches_per_op) = .{},

    create_accounts: BatchRing(Batch(.create_accounts, groups_max), batches_per_op) = .{},
    // TODO: other ops

    fn init(allocator: *std.mem.Allocator, message_bus: *MessageBus) !BatchManager {
        var self = BatchManager{};

        for (self.create_accounts.batches) |*batch| {
            batch.* = try Batch(.create_accounts, groups_max).init(allocator, message_bus.get_message().?);
        }
        // TODO: other ops

        return self;
    }

    fn push(
        self: *BatchManager,
        user_data: u64,
        callback: BatchCallback,
        operation: Operation,
        data: []const u8,
    ) error{NoSpaceLeft}!void {
        switch (operation) {
            .reserved, .init => unreachable,
            .create_accounts => {
                const batch = self.create_accounts.current() orelse return error.NoSpaceLeft;
                batch.push(user_data, callback, std.mem.bytesAsSlice(Account, data)) catch {
                    // The current batch is full, so mark it as complete and push it to the send queue.
                    self.create_accounts.mark_current_complete();
                    self.push_to_send_queue(.{ .create_accounts = batch });
                    const new_batch = self.create_accounts.current() orelse return error.NoSpaceLeft;
                    // TODO: reject Client.batch calls with data > message_size_max.
                    new_batch.push(user_data, callback, std.mem.bytesAsSlice(Account, data)) catch unreachable;
                };
            },
            else => unreachable, // TODO: other operations
        }
    }

    fn push_to_send_queue(self: *BatchManager, any_batch: AnyBatch) void {
        const was_empty = self.send_queue.empty();
        // the send_queue is large enough to hold all batches
        self.send_queue.push(any_batch) catch unreachable;

        if (was_empty) {
            // TODO send message
        }
    }

    fn send_if_none_inflight(self: *BatchManager) void {
        if (!self.send_queue.empty()) return;

        if (self.create_accounts.current().?.group.items.len > 0) {
            self.create_accounts.mark_current_complete();
            self.push_to_send_queue(.{ .create_accounts = batch });
            return;
        }
        // TODO: other operations
    }

    fn deliver(self: *BatchManager, message_bus: *MessageBus, message: *Message) void {
        switch (self.send_queue.pop().?) {
            .create_accounts => |*batch| {
                batch.deliver(message);
                assert(self.create_accounts.pop_complete() == batch);
                // TODO: can this fail? can we just reuse the old message stored in the Batch?
                batch.clear(message_bus, message_bus.get_message().?);
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
        .create_accounts => Account,
        else => unreachable, // TODO
    };

    const Result = switch (operation) {
        .create_accounts => CreateAccountResults,
        else => unreachable, // TODO
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

        fn init(allocator: *std.mem.Allocator, message: *Message) !Self {
            return Self{
                .groups = try Groups.init(allocator),
                .message = message.ref(),
            };
        }

        fn deinit(self: *Self, allocator: *std.mem.Allocator) void {
            self.groups.deinit(allocator);
        }

        /// Clears all state, keeping the allocated memory.
        fn clear(self: *Self, message_bus: *MessageBus, new_message: *Message) void {
            message_bus.unref(self.message);
            self.message = new_message.ref();
            self.entries.clear();
        }

        fn push(self: *Self, user_data: u64, callback: BatchCallback, events: []Event) error{NoSpaceLeft}!void {
            const data = mem.sliceAsBytes(events);
            if (self.message.header.size + data.len > config.message_size_max) {
                return error.NoSpaceLeft;
            }
            self.entries.append(.{
                .user_data = user_data,
                .callback = callback,
                .len = @intCast(u32, events.len),
            }) catch return error.NoSpaceLeft; // TODO Use exhaustive switch.
        }

        fn deliver(self: *Self, message: *Message) void {
            // Results are sorted by index field.
            const results = std.mem.bytesAsSlice(Result, message.buffer);
            var result_idx: u32 = 0;
            var item_idx: u32 = 0;
            for (self.groups.items) |group| {
                const group_first_result = result_idx;
                while (results[result_idx].index < item_idx + group.len) : (result_idx += 1) {
                    // Mutate the result index in-place to be relative to this group:
                    results[result_idx].index -= item_idx;
                }
                group.callback(group.user_data, operation, std.mem.sliceAsBytes(results[group_first_result..result_idx]));
                item_idx += group.len;
            }
            assert(result_idx == results.len);
        }
    };
}
