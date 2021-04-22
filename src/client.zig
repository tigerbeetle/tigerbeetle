const std = @import("std");
const assert = std.debug.assert;

const MessageBus = @import("message_bus.zig").MessageBus;
const Message = @import("message_bus.zig").Message;
const Operation = @import("state_machine.zig").Operation;
const FixedArrayList = @import("fixed_array_list.zig").FixedArrayList;

const log = std.log;

pub const Client = struct {
    allocator: *std.mem.Allocator,
    id: u128,
    cluster: u128,
    configuration: []MessageBus.Address,
    message_bus: *MessageBus,

    create_accounts_batch: Batch(.create_accounts, 1000),

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

        const batch = try Batch(.create_accounts, 1000).init(allocator, message_bus.get_message().?);
        const self = Client{
            .allocator = allocator,
            .id = id,
            .cluster = cluster,
            .configuration = configuration,
            .message_bus = message_bus,
            .create_accounts_batch = batch,
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

/// This allows building a batch to be sent as a single message and handles
/// callback state. The idea is to have multiple of these structs for different
/// Operations and perhaps even multiple for the same opertion if we want to queue
/// more entries while a message is in flight.
fn Batch(comptime operation: Operation, comptime size: usize) type {
    const T = switch (operation) {
        .create_accounts => Account,
        else => unreachable, // TODO
    };
    return struct {
        const Self = @This();

        const Callback = fn (user_data: u64, operation: Operation, data: []const u8) void;

        const Entry = struct {
            user_data: u64,
            callback: Callback,
            /// Number of items in the Message buffer associated with the entry.
            items: u32,
        };

        entries: FixedArrayList(Entry, size),
        message: *Message,

        fn init(allocator: *std.mem.Allocator, message: *Message) !Self {
            return Self{
                .entries = try FixedArrayList(Entry, size).init(allocator),
                .message = message.ref(),
            };
        }

        /// Clear all state but keep the allocated memory
        fn reset(self: *Self, new_message: *Message, message_bus: *MessageBus) void {
            message_bus.unref(self.message);
            self.message = new_message.ref();
            self.entries.clear();
        }

        fn push(self: *Self, user_data: u64, callback: Callback, items: []T) error{NoSpaceLeft}!void {
            const data = mem.sliceAsBytes(items);
            if (self.message.header.size + data.len > config.message_size_max) {
                return error.NoSpaceLeft;
            }
            self.entries.append(.{
                .user_data = user_data,
                .callback = callback,
                .items = @intCast(u32, items.len),
            }) orelse return error.NoSpaceLeft;
        }

        fn deliver(self: *Self, message: *Message) void {
            // This assumes the results are sorted by their index field, if not we need to sort them here
            const results = mem.bytesAsSlice(T.Results, message.buffer);
            var result_idx: u32 = 0;
            var item_idx: u32 = 0;
            for (self.entries.items) |entry| {
                const entry_first_result = result_idx;
                while (results[result_idx].index < item_idx + entry.items) : (result_idx += 1) {
                    // Mutate the result index to be relative to this entry in-place
                    result.index -= item_idx;
                }
                entry.callback(entry.user_data, operation, results[entry_first_result..result_idx]);
                item_idx += entry.items;
            }
            assert(result_idx == results.len);
        }
    };
}
