const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

pub const Header = @import("vr.zig").Header;
pub const Operation = @import("state_machine.zig").Operation;

const MAX_TRANSFERS = 1_000_000;
const BATCH_SIZE = 10_000;
const IS_TWO_PHASE_COMMIT = false;
const BENCHMARK = if (IS_TWO_PHASE_COMMIT) 500_000 else 1_000_000;
const RESULT_TOLERANCE = 10; // percent

var accounts = [_]Account{
    Account{
        .id = 1,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .unit = 2,
        .code = 0,
        .flags = .{},
        .debits_reserved = 0,
        .debits_accepted = 0,
        .credits_reserved = 0,
        .credits_accepted = 0,
    },
    Account{
        .id = 2,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .unit = 2,
        .code = 0,
        .flags = .{},
        .debits_reserved = 0,
        .debits_accepted = 0,
        .credits_reserved = 0,
        .credits_accepted = 0,
    },
};

pub fn main() !void {
    if (std.builtin.mode != .ReleaseFast or std.builtin.mode != .ReleaseSafe) {
        std.debug.warn("The client has not been built in ReleaseSafe or ReleaseFast mode.\n", .{});
    }
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // Pre-allocate a million transfers:
    var transfers = try arena.allocator.alloc(Transfer, MAX_TRANSFERS);
    for (transfers) |*transfer, index| {
        transfer.* = .{
            .id = index,
            .debit_account_id = accounts[0].id,
            .credit_account_id = accounts[1].id,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = if (IS_TWO_PHASE_COMMIT) .{ .two_phase_commit = true } else .{},
            .amount = 1,
            .timeout = if (IS_TWO_PHASE_COMMIT) std.time.ns_per_hour else 0,
        };
    }

    // Pre-allocate a million commits:
    var commits: ?[]Commit = if (IS_TWO_PHASE_COMMIT) try arena.allocator.alloc(Commit, MAX_TRANSFERS) else null;
    if (commits) |all_commits| {
        for (all_commits) |*commit, index| {
            commit.* = .{
                .id = index,
                .reserved = [_]u8{0} ** 32,
                .code = 0,
                .flags = .{},
            };
        }
    }

    var addr = try std.net.Address.parseIp4("127.0.0.1", config.port);
    std.debug.print("connecting to {}...\n", .{addr});
    var connection = try std.net.tcpConnectToAddress(addr);
    const fd = connection.handle;
    defer std.os.close(fd);
    std.debug.print("connected to tigerbeetle\n", .{});

    // Create our two accounts if necessary:
    std.debug.print("creating accounts...\n", .{});
    try send(fd, .create_accounts, std.mem.asBytes(accounts[0..]), CreateAccountsResult);

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
            CreateTransfersResult,
        );

        const ms2 = std.time.milliTimestamp();
        var create_transfers_latency = ms2 - ms1;
        if (create_transfers_latency > max_create_transfers_latency) {
            max_create_transfers_latency = create_transfers_latency;
        }

        if (commits) |all_commits| {
            // Commit this batch:
            var batch_commits = all_commits[offset..][0..10000];
            try send(
                fd,
                .commit_transfers,
                std.mem.asBytes(batch_commits[0..]),
                CommitTransfersResult,
            );

            const ms3 = std.time.milliTimestamp();
            var commit_transfers_latency = ms3 - ms2;
            if (commit_transfers_latency > max_commit_transfers_latency) {
                max_commit_transfers_latency = commit_transfers_latency;
            }
        }

        offset += batch_transfers.len;
        if (@mod(offset, 100000) == 0) {
            var space = if (offset == 1000000) "" else " ";
            std.debug.print("{s}{} transfers...\n", .{ space, offset });
        }
    }

    const ms = std.time.milliTimestamp() - start;
    std.debug.print("============================================\n", .{});
    const transfer_type = if (IS_TWO_PHASE_COMMIT) "two-phase commit" else "";
    const result: i64 = @divFloor(@intCast(i64, transfers.len * 1000), ms);
    std.debug.print("{} {s} transfers per second\n\n", .{
        result,
        transfer_type,
    });
    std.debug.print("create_transfers max p100 latency per 10,000 transfers = {}ms\n", .{
        max_create_transfers_latency,
    });
    std.debug.print("commit_transfers max p100 latency per 10,000 transfers = {}ms\n", .{
        max_commit_transfers_latency,
    });

    if (result < @divFloor(@intCast(i64, BENCHMARK * (100 - RESULT_TOLERANCE)), 100)) {
        std.debug.warn("There has been a performance regression. previous benchmark={}\n", .{BENCHMARK});
    }
}

const cluster_id: u128 = 746649394563965214; // a5ca1ab1ebee11e
const client_id: u128 = 123;
var request_number: u32 = 0;
var sent_ping = false;

fn send(fd: std.os.fd_t, operation: Operation, body: []u8, comptime Result: anytype) !void {
    // This is required to greet the cluster and identify the connection as being from a client:
    if (!sent_ping) {
        sent_ping = true;

        var ping = Header{
            .command = .ping,
            .cluster = cluster_id,
            .client = client_id,
            .view = 0,
        };
        ping.set_checksum_body(&[0]u8{});
        ping.set_checksum();

        assert((try std.os.sendto(fd, std.mem.asBytes(&ping), 0, null, 0)) == @sizeOf(Header));

        var pong: [@sizeOf(Header)]u8 = undefined;
        var pong_size: u64 = 0;
        while (pong_size < @sizeOf(Header)) {
            var pong_bytes = try std.os.recvfrom(fd, pong[pong_size..], 0, null, null);
            if (pong_bytes == 0) @panic("server closed the connection (while waiting for pong)");
            pong_size += pong_bytes;
        }
    }

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
