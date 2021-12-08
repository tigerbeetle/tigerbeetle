const std = @import("std");
const assert = std.debug.assert;
const tb = @import("../tigerbeetle/src/tigerbeetle.zig");

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const Commit = tb.Commit;
const CommitFlags = tb.CommitFlags;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;
const CommitTransfersResult = tb.CommitTransfersResult;

pub const StateMachine = @import("../tigerbeetle/src/state_machine.zig").StateMachine;
pub const Operation = StateMachine.Operation;
pub const MessageBus = @import("../tigerbeetle/src/message_bus.zig").MessageBusClient;
pub const IO = @import("../tigerbeetle/src/io.zig").IO;

const vsr = @import("../tigerbeetle/src/vsr.zig");
pub const Header = vsr.Header;
pub const Client = vsr.Client(StateMachine, MessageBus);

pub const log_level: std.log.Level = .info;

const Temp = struct {
    id: u32 = 11,
    pub fn hi(self: *Temp) u32 {
        return self.id;
    }
};

fn clientPtrFromCtx(ctx: *TB_Context) !*Temp {
    return @ptrCast(*Temp, @alignCast(@alignOf(Temp), &ctx.*.client));
}

pub const TB_Context = extern struct {
    client: [@sizeOf(Temp)]u8,
};

export fn init(ctx: *TB_Context) c_int {
    var client = clientPtrFromCtx(ctx) catch return 1;
    client.* = Temp{};

    return 0;
}

const Callback = fn (u32) void;

export fn request(ctx: *TB_Context, user_data: *c_void) c_int {
    std.debug.print("zig request\n", .{});
    var client = clientPtrFromCtx(ctx) catch return 1;
    const cb = @ptrCast(Callback, user_data);
    cb(client.hi());

    return 0;
}
