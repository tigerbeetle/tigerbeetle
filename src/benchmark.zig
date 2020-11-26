const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;
const testing = std.testing;

usingnamespace @import("tigerbeetle.zig");

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

    var addr = try net.Address.parseIp4("127.0.0.1", config.port);
    std.debug.print("connecting to {}...\n", .{addr});
    var connection = try net.tcpConnectToAddress(addr);
    const fd = connection.handle;
    defer os.close(fd);
    std.debug.print("connected to tigerbeetle\n", .{});

    // Create our two accounts if necessary:
    std.debug.print("creating accounts...\n", .{});
    try send(fd, .create_accounts, mem.asBytes(accounts[0..]), CreateAccountResults);

    // Start the benchmark:
    const start = std.time.milliTimestamp();
    var max_create_transfers_latency: i64 = 0;
    var max_commit_transfers_latency: i64 = 0;
    var offset: u64 = 0;
    while (offset < transfers.len) {
        // Create a batch of 10,000 transfers:
        const ms1 = std.time.milliTimestamp();
        var batch_transfers = transfers[offset..][0..10000];
        try send(fd, .create_transfers, mem.asBytes(batch_transfers[0..]), CreateTransferResults);

        const ms2 = std.time.milliTimestamp();
        var create_transfers_latency = ms2 - ms1;
        if (create_transfers_latency > max_create_transfers_latency) {
            max_create_transfers_latency = create_transfers_latency;
        }

        // Commit this batch:
        var batch_commits = commits[offset..][0..10000];
        try send(fd, .commit_transfers, mem.asBytes(batch_commits[0..]), CommitTransferResults);

        const ms3 = std.time.milliTimestamp();
        var commit_transfers_latency = ms3 - ms2;
        if (commit_transfers_latency > max_commit_transfers_latency) {
            max_commit_transfers_latency = commit_transfers_latency;
        }

        offset += batch_transfers.len;
        if (@mod(offset, 100000) == 0) {
            var space = if (offset == 1000000) "" else " ";
            std.debug.print("{}{} transfers...\n", .{ space, offset });
        }
    }

    const ms = std.time.milliTimestamp() - start;
    std.debug.print("=============================\n", .{});
    std.debug.print("{} transfers per second\n", .{@divFloor(@intCast(i64, transfers.len * 1000), ms)});
    std.debug.print("max create_transfers latency = {}ms\n", .{max_create_transfers_latency});
    std.debug.print("max commit_transfers latency = {}ms\n", .{max_commit_transfers_latency});
}

var request_id: u128 = 0;

fn send(fd: os.fd_t, command: Command, data: []u8, comptime Result: anytype) !void {
    request_id += 1;
    var request = NetworkHeader{
        .id = request_id,
        .command = command,
        .size = @intCast(u32, @sizeOf(NetworkHeader) + data.len),
    };
    request.set_checksum_data(data[0..]);
    request.set_checksum_meta();

    const meta = mem.asBytes(&request);
    assert((try os.sendto(fd, meta[0..], 0, null, 0)) == meta.len);
    if (data.len > 0) {
        assert((try os.sendto(fd, data[0..], 0, null, 0)) == data.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    var recv_size: u64 = 0;
    while (recv_size < @sizeOf(NetworkHeader)) {
        var recv_bytes = try os.recvfrom(fd, recv[recv_size..], 0, null, null);
        if (recv_bytes == 0) @panic("server closed the connection (while waiting for header)");
        recv_size += recv_bytes;
    }

    var response = mem.bytesAsValue(NetworkHeader, recv[0..@sizeOf(NetworkHeader)]);
    var print_response = response.size > @sizeOf(NetworkHeader);
    if (print_response) std.debug.print("{}\n", .{response});
    assert(response.magic == Magic);
    assert(response.valid_checksum_meta());
    assert(response.valid_size());

    while (recv_size < response.size) {
        var recv_bytes = try os.recvfrom(fd, recv[recv_size..], 0, null, null);
        if (recv_bytes == 0) @panic("server closed the connection (while waiting for data)");
        recv_size += recv_bytes;
    }

    const response_data = recv[@sizeOf(NetworkHeader)..response.size];
    assert(response.valid_checksum_data(response_data));

    if (print_response) {
        for (mem.bytesAsSlice(Result, response_data)) |result| {
            std.debug.print("{}\n", .{result});
        }
        @panic("run 'rm -rf journal' and restart the server to do a fresh benchmark");
    }
}
