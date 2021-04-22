const std = @import("std");
const assert = std.debug.assert;

const MessageBus = @import("message_bus.zig").MessageBus;
const Message = @import("message_bus.zig").Message;
const Operation = @import("state_machine.zig").Operation;
const FixedArrayList = @import("fixed_array_list.zig").FixedArrayList;

// TODO Be explicit with what we import:
usingnamespace @import("tigerbeetle.zig");

const log = std.log;

pub const Client = struct {
    allocator: *std.mem.Allocator,
    id: u128,
    cluster: u128,
    configuration: []MessageBus.Address,
    message_bus: *MessageBus,

    batch_create_accounts: Batch(.create_accounts, 1000),

    pub fn init(
        allocator: *std.mem.Allocator,
        id: u128,
        cluster: u128,
        configuration: []MessageBus.Address,
        message_bus: *MessageBus,
    ) !Client {
        assert(id > 0);
        assert(cluster > 0);
        assert(configuration.len > 0);

        const self = Client{
            .allocator = allocator,
            .id = id,
            .cluster = cluster,
            .configuration = configuration,
            .message_bus = message_bus,
            .batch_create_accounts = try Batch(.create_accounts, 1000).init(
                allocator,
                message_bus.get_message().?,
            ),
        };

        return self;
    }

    pub fn deinit(self: *Client) void {}

    pub fn tick(self: *Client) void {
        self.message_bus.tick();
    }

    pub fn batch(
        self: *Client,
        user_data: u64,
        calback: BatchCallback,
        operation: Operation,
        data: []const u8,
    ) void {
        switch (operation) {
            .reserved, .init => unreachable,

            .create_accounts => {
                self.create_accounts_batch.push(user_data, calback, mem.bytesAsSlice(Account, data)) catch {
                    // TODO return error
                };
            },

            // TODO
            .create_transfers => {},
            .commit_transfers => {},
            .lookup_accounts => {},
        }
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
    }
};

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

        const Callback = fn (user_data: u64, operation: Operation, results: []const u8) void;

        const Group = struct {
            user_data: u64,
            callback: Callback,
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

        fn push(self: *Self, user_data: u64, callback: Callback, events: []Event) error{NoSpaceLeft}!void {
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
                    result.index -= item_idx;
                }
                group.callback(group.user_data, operation, std.mem.sliceAsBytes(results[group_first_result..result_idx]));
                item_idx += group.len;
            }
            assert(result_idx == results.len);
        }
    };
}
