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

const IoThreadType = @import("tb_client/context.zig").IoThreadType;
const DefaultIoThread = blk: {
    const ClientType = @import("../../vsr/client.zig").ClientType;
    const Client = ClientType(Operation, MessageBus);
    break :blk IoThreadType(Client);
};

const TestingIoThread = blk: {
    const EchoClientType = @import("tb_client/echo_client.zig").EchoClientType;
    const EchoClient = EchoClientType(MessageBus);
    break :blk IoThreadType(EchoClient);
};

pub const init = DefaultIoThread.init;
pub const init_echo = TestingIoThread.init;

test {
    std.testing.refAllDecls(DefaultIoThread);
}
