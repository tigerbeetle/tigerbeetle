const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

usingnamespace @import("types.zig");

const HashMapAccounts = std.AutoHashMap(u128, Account);
const HashMapTransfers = std.AutoHashMap(u128, Transfer);

pub const State = struct {
    allocator: *Allocator,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,

    pub fn init(allocator: *Allocator, accounts_max: usize, transfers_max: usize) !State {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureCapacity(@intCast(u32, transfers_max));

        return State {
            .allocator = allocator,
            .accounts = accounts,
            .transfers = transfers
        };
    }

    pub fn deinit(self: *State) void {
        self.accounts.deinit();
        self.transfers.deinit();
    }

    pub fn apply(self: *State, command: Command, data: []u8) void {
        switch (command) {
            .reserved => unreachable, // TODO
            .ack => unreachable, // TODO
            .create_accounts => self.apply_create_accounts(data),
            .create_transfers => unreachable, // TODO
            .commit_transfers => unreachable, // TODO
        }
    }

    pub fn apply_create_accounts(self: *State, data: []u8) void {
        var batch = mem.bytesAsSlice(Account, data);
        for (batch) |account| {
            std.debug.print("{}\n", .{account});
        }
    }
};
