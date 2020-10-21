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

    var timestamp = @intCast(u64, std.time.nanoTimestamp());

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
            .debit_accepted_limit = 0,
            .debit_reserved_limit = 0,
            .credit_accepted_limit = 1_000_000,
            .credit_reserved_limit = 1_000,
            .padding = 0,
            .timestamp = timestamp + 0,
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
            .debit_accepted_limit = 1_000_000,
            .debit_reserved_limit = 1_000_000,
            .credit_accepted_limit = 0,
            .credit_reserved_limit = 0,
            .padding = 0,
            .timestamp = timestamp + 1,
        }
    };

    var data = mem.asBytes(accounts[0..]);
    var header = Header {
        .id = 0,
        .command = .create_accounts,
        .size = @sizeOf(Header) + data.len
    };
    header.set_checksum_data(data[0..]);
    header.set_checksum_meta();
    const meta = mem.asBytes(&header);

    _ = try os.sendto(connection.handle, meta[0..], 0, null, 0);
    if (data.len > 0) _ = try os.sendto(connection.handle, data[0..], 0, null, 0);

    var recv: [1024 * 1024]u8 = undefined;
    var recv_bytes = try os.recvfrom(connection.handle, recv[0..], 0, null, null);

    var response = mem.bytesAsValue(Header, recv[0..@sizeOf(Header)]);
    std.debug.print("{}\n", .{ response });
    assert(response.valid_checksum_meta());

    const response_data = recv[@sizeOf(Header)..response.size];
    assert(response.valid_checksum_data(response_data));

    for (mem.bytesAsSlice(AccountResults, response_data)) |result| {
        std.debug.print("{}\n", .{ result });
    }

    os.close(connection.handle);
}
