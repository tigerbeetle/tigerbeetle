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
const config = @import("../tigerbeetle/src/config.zig");

const vsr = @import("../tigerbeetle/src/vsr.zig");
pub const Header = vsr.Header;
pub const Client = vsr.Client(StateMachine, MessageBus);

pub const log_level: std.log.Level = .info;
const log = std.log.scoped(.c_client);

fn clientPtrFromCtx(ctx: *TB_Context) *Client {
    return @ptrCast(*Client, @alignCast(@alignOf(Client), &ctx.*.client));
}

fn messageBusPtrFromCtx(ctx: *TB_Context) *MessageBus {
    return @ptrCast(*MessageBus, @alignCast(@alignOf(MessageBus), &ctx.*.message_bus));
}

// Error set
const InvalidOperation: c_int = 2;
const TooManyOutstandingRequests: c_int = InvalidOperation + 1; // 3
const EmptyBatch: c_int = TooManyOutstandingRequests + 1; // 4
const MaximumBatchSizeExceeded: c_int = EmptyBatch + 1; // 5
const InvalidBatchSize: c_int = MaximumBatchSizeExceeded + 1; // 6
const FailedToTickIO: c_int = InvalidBatchSize + 1; // 7

pub const TB_Context = extern struct {
    client: [@sizeOf(Client)]u8,
    message_bus: [@sizeOf(MessageBus)]u8,
    io: usize, // All contexts share a global io.
    client_id: u128,

    fn init(
        self: *TB_Context,
        allocator: *std.mem.Allocator,
        client_id: u128,
        io: *IO,
        cluster: u32,
        addresses_raw: []const u8,
    ) !void {
        var addresses = vsr.parse_addresses(allocator, addresses_raw) catch |err| {
            log.err("Failed to parse addresses.", .{});
            return err;
        };
        errdefer allocator.free(addresses);

        const message_bus = messageBusPtrFromCtx(self);
        log.debug("init: Initialising message bus.", .{});
        message_bus.* = MessageBus.init(
            allocator,
            cluster,
            addresses,
            client_id,
            io,
        ) catch |err| {
            log.err("Failed to init message bus.", .{});
            return err;
        };
        errdefer message_bus.deinit();

        const client = clientPtrFromCtx(self);
        log.debug("init: Initialising client. cluster_id={d} client_id={d} addresses={o}", .{
            cluster,
            client_id,
            addresses,
        });
        client.* = Client.init(
            allocator,
            client_id,
            cluster,
            @intCast(u8, addresses.len),
            message_bus,
        ) catch |err| {
            log.err("Failed to initialize zig client.", .{});
            return err;
        };
        errdefer client.deinit();

        message_bus.set_on_message(*Client, client, Client.on_message);
    }
};

// All TB_Contexts will share the same IO.
var globalIo: ?IO = null;

export fn init(ctx: *TB_Context, cluster: u32, addresses: [*c]u8, addresses_length: u32) c_int {
    const client_id = std.crypto.random.int(u128);
    log.debug("init: initializing client_id={}", .{client_id});

    if (globalIo == null) {
        // Be careful to size the SQ ring to only a few SQE entries and to share a single IO
        // instance across multiple clients to stay under kernel limits:
        //
        // The memory required by io_uring is accounted under the rlimit memlocked option, which can
        // be quite low on some setups (64K). The default is usually enough for most use cases, but
        // bigger rings or things like registered buffers deplete it quickly. Root isn't under this
        // restriction, but regular users are.
        //
        // Check `/etc/security/limits.conf` for user settings, or `/etc/systemd/user.conf` and
        // `/etc/systemd/system.conf` for systemd setups.
        globalIo = IO.init(32, 0) catch {
            log.err("Failed to initialize io.", .{});
            return 1;
        };
    }

    const allocator = std.heap.c_allocator;
    var addresses_raw = @ptrCast([*]u8, addresses)[0..addresses_length];
    ctx.init(allocator, client_id, &globalIo.?, cluster, addresses_raw) catch {
        log.err("Failed to initialize context.", .{});
        return 1;
    };
    return 0;
}

const ResultsCallback = fn (
    user_data: *c_void,
    err: u8,
    operation: u8,
    results: [*c]const u8,
    results_length: u32,
) void;

const UserData = packed struct {
    ctx: *c_void,
    callback: *ResultsCallback,
};

fn EventSize(operation: Operation) usize {
    return switch (operation) {
        Operation.create_accounts => @sizeOf(Account),
        Operation.create_transfers => @sizeOf(Transfer),
        Operation.commit_transfers => @sizeOf(Commit),
        Operation.lookup_accounts => @sizeOf(u128),
        Operation.lookup_transfers => @sizeOf(u128),
        else => unreachable,
    };
}

export fn request(
    ctx: *TB_Context,
    user_data_raw: *c_void,
    callback: *ResultsCallback,
    operation_int: c_uint,
    batch_raw: [*c]u8,
    batch_raw_length: c_uint,
) c_int {
    const client = clientPtrFromCtx(ctx);

    if (operation_int >= @typeInfo(Operation).Enum.fields.len) return InvalidOperation;
    const operation = @intToEnum(Operation, @intCast(u8, operation_int));
    log.debug("{}: request: operation={s}\n", .{ ctx.client_id, operation });

    const message = client.get_message() orelse return TooManyOutstandingRequests;
    defer client.unref(message);

    const event_size = EventSize(operation);
    const batch_length = @intCast(u32, batch_raw_length);
    if (batch_length < 1) return EmptyBatch;
    if (@sizeOf(Header) + batch_length > config.message_size_max) return MaximumBatchSizeExceeded;
    if (@mod(batch_length, event_size) != 0) return InvalidBatchSize;
    const batch = @ptrCast([*]u8, batch_raw)[0..batch_length];

    // Copy batch into message buffer as data lifetime is not guaranteed.
    std.mem.copy(u8, message.buffer[@sizeOf(Header)..], batch[0..]);

    const user_data = UserData{
        .ctx = user_data_raw,
        .callback = callback,
    };

    client.request(@bitCast(u128, user_data), on_result, operation, message, batch_length);

    return 0;
}

fn on_result(user_data: u128, operation: Operation, results: Client.Error![]const u8) void {
    const ctx = @bitCast(UserData, user_data).ctx;
    const user_callback = @ptrCast(ResultsCallback, @bitCast(UserData, user_data).callback);
    const operation_int = @enumToInt(operation);
    log.debug("on_result: operation={} results={o} callback={}", .{ operation, results, user_callback });

    if (results) |value| {
        log.debug("on_result: returning results={o}.", .{value});
        user_callback(
            ctx,
            0,
            operation_int,
            value.ptr,
            @intCast(u32, value.len),
        );
    } else |err| {
        log.err("on_result: returning error. {}", .{err});
        const c_error: c_int = 1; // TODO: handle error cases
        user_callback(
            ctx,
            c_error,
            operation_int,
            0,
            0,
        );
    }
}

export fn tick(ctx: *TB_Context) c_int {
    const client = clientPtrFromCtx(ctx);
    client.tick();
    var c_err: c_int = 0;
    globalIo.?.tick() catch |err| switch (err) {
        // TODO exhaustive switch
        else => {
            c_err = FailedToTickIO;
        },
    };

    return c_err;
}

export fn deinit(ctx: *TB_Context) void {
    const client = clientPtrFromCtx(ctx);
    client.deinit();

    const message_bus = messageBusPtrFromCtx(ctx);
    message_bus.deinit();
}
