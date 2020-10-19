const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;
const testing = std.testing;

usingnamespace @import("types.zig");

const port = 3001;

pub fn main() !void {
    var addr = try net.Address.parseIp4("127.0.0.1", port);
    var connection = try net.tcpConnectToAddress(addr);
    errdefer os.close(connection.handle);

    var accounts = [_]Account {
        Account {
            .id = 1,
            .custom = 0,
            .flags = 0,
            .unit = 2,
            .debit_accepted = 1_000_000_000,
            .debit_reserved = 0,
            .credit_accepted = 10_000,
            .credit_reserved = 0,
            .limit_debit_accepted = 0,
            .limit_debit_reserved = 0,
            .limit_credit_accepted = 1_000_000,
            .limit_credit_reserved = 1_000,
            .padding = 0,
            .timestamp = 0,
        },
        Account {
            .id = 2,
            .custom = 0,
            .flags = 0,
            .unit = 2,
            .debit_accepted = 0,
            .debit_reserved = 0,
            .credit_accepted = 0,
            .credit_reserved = 0,
            .limit_debit_accepted = 1_000_000,
            .limit_debit_reserved = 1_000,
            .limit_credit_accepted = 0,
            .limit_credit_reserved = 0,
            .padding = 0,
            .timestamp = 0,
        }
    };

    var data = mem.asBytes(accounts[0..]);
    var header = NetworkHeader {
        .id = 0,
        .command = .create_accounts,
        .data_size = data.len
    };
    header.set_checksum_data(data[0..]);
    header.set_checksum_meta();
    const meta = mem.asBytes(&header);

    _ = try os.sendto(connection.handle, meta[0..], 0, null, 0);
    if (data.len > 0) _ = try os.sendto(connection.handle, data[0..], 0, null, 0);
    var recv: [64]u8 = undefined;
    _ = try os.recvfrom(connection.handle, recv[0..], 0, null, null);
    std.debug.print("ack: {x}\n", .{ recv });
    os.close(connection.handle);
}
