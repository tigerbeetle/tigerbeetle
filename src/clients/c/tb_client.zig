const std = @import("std");

pub const vsr = @import("../../vsr.zig");
pub const exports = @import("tb_client_exports.zig");

const MessageBus = @import("../../message_bus.zig").MessageBusType(@import("../../io.zig").IO);

pub const InitError = @import("tb_client/context.zig").InitError;
pub const InitParameters = @import("tb_client/context.zig").InitParameters;
pub const ClientInterface = @import("tb_client/context.zig").ClientInterface;
pub const CompletionCallback = @import("tb_client/context.zig").CompletionCallback;
pub const Packet = @import("tb_client/packet.zig").Packet.Extern;
pub const PacketStatus = @import("tb_client/packet.zig").Packet.Status;
pub const Operation = vsr.tigerbeetle.Operation;

/// Creates the `tb_client` context for the accounting state machine.
pub const Context = blk: {
    const ContextType = @import("tb_client/context.zig").ContextType;
    const ClientType = @import("../../vsr/client.zig").ClientType;
    const Client = ClientType(Operation, MessageBus);

    const allowed_operations = [_]Operation{
        .create_accounts,
        .create_transfers,
        .lookup_accounts,
        .lookup_transfers,
        .get_account_transfers,
        .get_account_balances,
        .query_accounts,
        .query_transfers,
        .get_change_events,
    };

    break :blk ContextType(Client, &allowed_operations);
};

test {
    std.testing.refAllDecls(Context);
}

// Consistency of U128 across Zig and the language clients.
// It must be kept in sync with all platforms.
test "u128 consistency test" {
    const decimal: u128 = 214850178493633095719753766415838275046;
    const binary = [16]u8{
        0xe6, 0xe5, 0xe4, 0xe3, 0xe2, 0xe1,
        0xd2, 0xd1, 0xc2, 0xc1, 0xb2, 0xb1,
        0xa4, 0xa3, 0xa2, 0xa1,
    };
    const pair: extern struct { lower: u64, upper: u64 } = .{
        .lower = 15119395263638463974,
        .upper = 11647051514084770242,
    };

    try std.testing.expectEqual(decimal, @as(u128, @bitCast(binary)));
    try std.testing.expectEqual(binary, @as([16]u8, @bitCast(decimal)));

    try std.testing.expectEqual(decimal, @as(u128, @bitCast(pair)));
    try std.testing.expectEqual(pair, @as(@TypeOf(pair), @bitCast(decimal)));
}
