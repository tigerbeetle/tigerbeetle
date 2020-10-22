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

var id: u128 = 0;

fn send(fd: os.fd_t, command: Command, data: []u8, Result: anytype) !void {
    id += 1;

    var request = Header {
        .id = id,
        .command = command,
        .size = @intCast(u32, @sizeOf(Header) + data.len)
    };
    request.set_checksum_data(data[0..]);
    request.set_checksum_meta();

    const meta = mem.asBytes(&request);
    assert((try os.sendto(fd, meta[0..], 0, null, 0)) == meta.len);
    if (data.len > 0) {
        assert((try os.sendto(fd, data[0..], 0, null, 0)) == data.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    const recv_bytes = try os.recvfrom(fd, recv[0..], 0, null, null);
    assert(recv_bytes >= @sizeOf(Header));
    
    var response = mem.bytesAsValue(Header, recv[0..@sizeOf(Header)]);
    std.debug.print("{}\n", .{ response });
    assert(response.magic == Magic);
    assert(response.valid_checksum_meta());
    assert(response.valid_size());
    assert(recv_bytes >= response.size);

    const response_data = recv[@sizeOf(Header)..response.size];
    assert(response.valid_checksum_data(response_data));

    for (mem.bytesAsSlice(Result, response_data)) |result| {
        std.debug.print("{}\n", .{ result });
    }
}

pub fn main() !void {
    var addr = try net.Address.parseIp4("127.0.0.1", port);
    var connection = try net.tcpConnectToAddress(addr);
    const fd = connection.handle;
    defer os.close(fd);

    var timestamp = @intCast(u64, std.time.nanoTimestamp());

    var accounts = [_]Account {
        Account {
            .id = 1,
            .custom = 0,
            .flags = .{},
            .unit = 2,
            .debit_reserved = 0,
            .debit_accepted = 0,
            .credit_reserved = 0,
            .credit_accepted = 0,
            .debit_reserved_limit = 1_000_000,
            .debit_accepted_limit = 1_000_000,
            .credit_reserved_limit = 0,
            .credit_accepted_limit = 0,
            .padding = 0,
            .timestamp = timestamp + 0,
        },
        Account {
            .id = 2,
            .custom = 0,
            .flags = .{},
            .unit = 2,
            .debit_reserved = 0,
            .debit_accepted = 1_000_000_000,
            .credit_reserved = 0,
            .credit_accepted = 900_000,
            .debit_reserved_limit = 0,
            .debit_accepted_limit = 0,
            .credit_reserved_limit = 1_000_000,
            .credit_accepted_limit = 1_000_000,
            .padding = 0,
            .timestamp = timestamp + 1,
        },
    };

    try send(fd, .create_accounts, mem.asBytes(accounts[0..]), CreateAccountResults);
    
    var transfers = [_]Transfer {
        Transfer {
            .id = 4,
            .debit_account_id = accounts[0].id,
            .credit_account_id = accounts[1].id,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{ .accept = true, .auto_commit = true },
            .amount = 100,
            .timeout = 0,
            .timestamp = timestamp + 2,
        }
    };

    try send(fd, .create_transfers, mem.asBytes(transfers[0..]), CreateTransferResults);
}
