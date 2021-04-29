const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

pub const Header = @import("vr.zig").Header;
pub const Operation = @import("state_machine.zig").Operation;

var accounts = [_]Account{
    Account{
        .id = 1,
        .custom = 0,
        .flags = .{},
        .unit = 2,
        .debit_reserved = 0,
        .debit_accepted = 0,
        .credit_reserved = 0,
        .credit_accepted = 0,
        .debit_reserved_limit = 1000_000_000,
        .debit_accepted_limit = 1000_000_000,
        .credit_reserved_limit = 0,
        .credit_accepted_limit = 0,
    },
    Account{
        .id = 2,
        .custom = 0,
        .flags = .{},
        .unit = 2,
        .debit_reserved = 0,
        .debit_accepted = 0,
        .credit_reserved = 0,
        .credit_accepted = 0,
        .debit_reserved_limit = 0,
        .debit_accepted_limit = 0,
        .credit_reserved_limit = 1000_000_000,
        .credit_accepted_limit = 1000_000_000,
    },
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // Pre-allocate a million transfers:
    var transfers = try arena.allocator.alloc(Transfer, 1_000_000);
    for (transfers) |*transfer, index| {
        transfer.* = .{
            .id = index,
            .debit_account_id = accounts[0].id,
            .credit_account_id = accounts[1].id,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{},
            .amount = 1,
            .timeout = 0,
        };
    }

    // Pre-allocate a million commits:
    var commits = try arena.allocator.alloc(Commit, 1_000_000);
    for (commits) |*commit, index| {
        commit.* = .{
            .id = index,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{ .accept = true },
        };
    }

    var addr = try std.net.Address.parseIp4("127.0.0.1", config.port);
    std.debug.print("connecting to {}...\n", .{addr});
    var connection = try std.net.tcpConnectToAddress(addr);
    const fd = connection.handle;
    defer std.os.close(fd);
    std.debug.print("connected to tigerbeetle\n", .{});

    // Create our two accounts if necessary:
    std.debug.print("creating accounts...\n", .{});
    try send(fd, .create_accounts, std.mem.asBytes(accounts[0..]), CreateAccountResults);

    // Start the benchmark:
    const start = std.time.milliTimestamp();
    var max_create_transfers_latency: i64 = 0;
    var max_commit_transfers_latency: i64 = 0;
    var offset: u64 = 0;
    while (offset < transfers.len) {
        // Create a batch of 10,000 transfers:
        const ms1 = std.time.milliTimestamp();
        var batch_transfers = transfers[offset..][0..10000];
        try send(
            fd,
            .create_transfers,
            std.mem.asBytes(batch_transfers[0..]),
            CreateTransferResults,
        );

        const ms2 = std.time.milliTimestamp();
        var create_transfers_latency = ms2 - ms1;
        if (create_transfers_latency > max_create_transfers_latency) {
            max_create_transfers_latency = create_transfers_latency;
        }

        // Commit this batch:
        var batch_commits = commits[offset..][0..10000];
        try send(fd, .commit_transfers, std.mem.asBytes(batch_commits[0..]), CommitTransferResults);

        const ms3 = std.time.milliTimestamp();
        var commit_transfers_latency = ms3 - ms2;
        if (commit_transfers_latency > max_commit_transfers_latency) {
            max_commit_transfers_latency = commit_transfers_latency;
        }

        offset += batch_transfers.len;
        if (@mod(offset, 100000) == 0) {
            var space = if (offset == 1000000) "" else " ";
            std.debug.print("{s}{} transfers...\n", .{ space, offset });
        }
    }

    const ms = std.time.milliTimestamp() - start;
    std.debug.print("=============================\n", .{});
    std.debug.print("{} transfers per second\n\n", .{
        @divFloor(@intCast(i64, transfers.len * 1000), ms),
    });
    std.debug.print("create_transfers max p100 latency per 10,000 transfers = {}ms\n", .{
        max_create_transfers_latency,
    });
    std.debug.print("commit_transfers max p100 latency per 10,000 transfers = {}ms\n", .{
        max_commit_transfers_latency,
    });
}

const cluster_id: u128 = 746649394563965214; // a5ca1ab1ebee11e
const client_id: u128 = 123;
var request_number: u32 = 0;

fn send(fd: std.os.fd_t, operation: Operation, body: []u8, comptime Result: anytype) !void {
    request_number += 1;

    var request = Header{
        .cluster = cluster_id,
        .client = client_id,
        .view = 0,
        .request = request_number,
        .command = .request,
        .operation = operation,
        .size = @intCast(u32, @sizeOf(Header) + body.len),
    };
    request.set_checksum_body(body[0..]);
    request.set_checksum();

    const header = std.mem.asBytes(&request);
    assert((try std.os.sendto(fd, header[0..], 0, null, 0)) == header.len);
    if (body.len > 0) {
        assert((try std.os.sendto(fd, body[0..], 0, null, 0)) == body.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    var recv_size: u64 = 0;
    while (recv_size < @sizeOf(Header)) {
        var recv_bytes = try std.os.recvfrom(fd, recv[recv_size..], 0, null, null);
        if (recv_bytes == 0) @panic("server closed the connection (while waiting for header)");
        recv_size += recv_bytes;
    }

    var response = std.mem.bytesAsValue(Header, recv[0..@sizeOf(Header)]);
    var print_response = response.size > @sizeOf(Header);
    if (print_response) std.debug.print("{}\n", .{response});
    assert(response.valid_checksum());

    while (recv_size < response.size) {
        var recv_bytes = try std.os.recvfrom(fd, recv[recv_size..], 0, null, null);
        if (recv_bytes == 0) @panic("server closed the connection (while waiting for body)");
        recv_size += recv_bytes;
    }

    const response_body = recv[@sizeOf(Header)..response.size];
    assert(response.valid_checksum_body(response_body));

    if (print_response) {
        for (std.mem.bytesAsSlice(Result, response_body)) |result| {
            std.debug.print("{}\n", .{result});
        }
        @panic("restart the cluster to do a 'clean slate' benchmark");
    }
}
