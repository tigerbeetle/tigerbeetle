const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../../vsr.zig");
const tb = vsr.tb_client;
const stdx = vsr.stdx;

pub const tb_packet_t = tb.Packet;
pub const tb_packet_status = tb.PacketStatus;

pub const tb_client_t = extern struct {
    @"opaque": [4]u64,

    pub inline fn cast(self: *tb_client_t) *tb.ClientInterface {
        return @ptrCast(self);
    }

    comptime {
        assert(@sizeOf(tb_client_t) == @sizeOf(tb.ClientInterface));
        assert(@bitSizeOf(tb_client_t) == @bitSizeOf(tb.ClientInterface));
        assert(@alignOf(tb_client_t) == @alignOf(tb.ClientInterface));
    }
};

pub const tb_init_status = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    address_invalid,
    address_limit_exceeded,
    system_resources,
    network_subsystem,
};

pub const tb_client_status = enum(c_int) {
    ok = 0,
    invalid,
};

pub const tb_register_log_callback_status = enum(c_int) {
    success = 0,
    already_registered,
    not_registered,
};

pub const tb_log_level = enum(c_int) {
    err = @intFromEnum(std.log.Level.err),
    warn = @intFromEnum(std.log.Level.warn),
    info = @intFromEnum(std.log.Level.info),
    debug = @intFromEnum(std.log.Level.debug),

    comptime {
        assert(std.enums.values(std.log.Level).len == std.enums.values(tb_log_level).len);
        for (std.enums.values(std.log.Level)) |std_level| {
            const level: tb_log_level = @enumFromInt(@intFromEnum(std_level));
            assert(std.mem.eql(u8, @tagName(std_level), @tagName(level)));
        }
    }
};

pub const tb_operation = tb.Operation;
pub const tb_completion_t = tb.CompletionCallback;
pub const tb_init_parameters = tb.InitParameters;

pub const tb_account_t = vsr.tigerbeetle.Account;
pub const tb_transfer_t = vsr.tigerbeetle.Transfer;
pub const tb_account_flags = vsr.tigerbeetle.AccountFlags;
pub const tb_transfer_flags = vsr.tigerbeetle.TransferFlags;
pub const tb_create_account_result = vsr.tigerbeetle.CreateAccountResult;
pub const tb_create_transfer_result = vsr.tigerbeetle.CreateTransferResult;
pub const tb_create_accounts_result_t = vsr.tigerbeetle.CreateAccountsResult;
pub const tb_create_transfers_result_t = vsr.tigerbeetle.CreateTransfersResult;
pub const tb_account_filter_t = vsr.tigerbeetle.AccountFilter;
pub const tb_account_filter_flags = vsr.tigerbeetle.AccountFilterFlags;
pub const tb_account_balance_t = vsr.tigerbeetle.AccountBalance;
pub const tb_query_filter_t = vsr.tigerbeetle.QueryFilter;
pub const tb_query_filter_flags = vsr.tigerbeetle.QueryFilterFlags;

pub fn init_error_to_status(err: tb.InitError) tb_init_status {
    return switch (err) {
        error.Unexpected => .unexpected,
        error.OutOfMemory => .out_of_memory,
        error.AddressInvalid => .address_invalid,
        error.AddressLimitExceeded => .address_limit_exceeded,
        error.SystemResources => .system_resources,
        error.NetworkSubsystemFailed => .network_subsystem,
    };
}

pub fn init(
    tb_client_out: *tb_client_t,
    cluster_id_ptr: *const [16]u8,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    completion_ctx: usize,
    completion_callback: tb_completion_t,
) callconv(.c) tb_init_status {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];

    // Passing u128 by value is prone to ABI issues. Pass as a [16]u8, and explicitly copy into
    // memory we know will be aligned correctly. Don't just use bytesToValue here, as that keeps
    // pointer alignment, and will result in a potentially unaligned access of a
    // `*align(1) const u128`.
    const cluster_id: u128 = blk: {
        var cluster_id: u128 = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&cluster_id), cluster_id_ptr);

        break :blk cluster_id;
    };

    tb.init(
        std.heap.c_allocator,
        tb_client_out.cast(),
        cluster_id,
        addresses,
        completion_ctx,
        completion_callback,
    ) catch |err| return init_error_to_status(err);
    return .success;
}

pub fn init_echo(
    tb_client_out: *tb_client_t,
    cluster_id_ptr: *const [16]u8,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    completion_ctx: usize,
    completion_callback: tb_completion_t,
) callconv(.c) tb_init_status {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];

    // See explanation in init().
    const cluster_id: u128 = blk: {
        var cluster_id: u128 = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&cluster_id), cluster_id_ptr);

        break :blk cluster_id;
    };

    tb.init_echo(
        std.heap.c_allocator,
        tb_client_out.cast(),
        cluster_id,
        addresses,
        completion_ctx,
        completion_callback,
    ) catch |err| return init_error_to_status(err);
    return .success;
}

pub fn submit(tb_client: ?*tb_client_t, packet: *tb_packet_t) callconv(.c) tb_client_status {
    const client: *tb.ClientInterface = if (tb_client) |ptr| ptr.cast() else return .invalid;
    client.submit(packet) catch |err| switch (err) {
        error.ClientInvalid => return .invalid,
    };
    return .ok;
}

pub fn deinit(tb_client: ?*tb_client_t) callconv(.c) tb_client_status {
    const client: *tb.ClientInterface = if (tb_client) |ptr| ptr.cast() else return .invalid;
    client.deinit() catch |err| switch (err) {
        error.ClientInvalid => return .invalid,
    };
    return .ok;
}

pub fn init_parameters(
    tb_client: ?*tb_client_t,
    out_parameters: *tb_init_parameters,
) callconv(.c) tb_client_status {
    const client: *tb.ClientInterface = if (tb_client) |ptr| ptr.cast() else return .invalid;
    client.init_parameters(out_parameters) catch |err| switch (err) {
        error.ClientInvalid => return .invalid,
    };
    return .ok;
}

pub fn completion_context(
    tb_client: ?*tb_client_t,
    completion_ctx_out: *usize,
) callconv(.c) tb_client_status {
    const client: *tb.ClientInterface = if (tb_client) |ptr| ptr.cast() else return .invalid;
    completion_ctx_out.* = client.completion_context() catch |err| switch (err) {
        error.ClientInvalid => return .invalid,
    };
    return .ok;
}

pub fn register_log_callback(
    callback_maybe: ?Logging.Callback,
    debug: bool,
) callconv(.c) tb_register_log_callback_status {
    Logging.global.mutex.lock();
    defer Logging.global.mutex.unlock();
    if (Logging.global.callback == null) {
        if (callback_maybe) |callback| {
            Logging.global.callback = callback;
            Logging.global.debug = debug;
            return .success;
        } else {
            return .not_registered;
        }
    } else {
        if (callback_maybe == null) {
            Logging.global.callback = null;
            Logging.global.debug = debug;
            return .success;
        } else {
            return .already_registered;
        }
    }
}

pub const Logging = struct {
    const Callback = *const fn (
        message_level: tb_log_level,
        message_ptr: [*]const u8,
        message_len: u32,
    ) callconv(.c) void;

    const log_line_max = 8192;

    /// Logging is global per process; it would be nice to be able to define a different logger
    /// for each client instance, though.
    var global: Logging = .{};

    callback: ?Callback = null,
    mutex: std.Thread.Mutex = .{},
    buffer: [log_line_max]u8 = undefined,
    debug: bool = false,

    /// A logger which defers to an application provided handler.
    pub fn application_logger(
        comptime message_level: std.log.Level,
        comptime scope: @Type(.enum_literal),
        comptime format: []const u8,
        args: anytype,
    ) void {
        // Debug logs are dropped here unless debug is set, because of the potential penalty in
        // crossing FFI to drop them.
        if (message_level == .debug and !Logging.global.debug) {
            return;
        }

        // Other messages are silently dropped if no logging callback is specified - unless they're
        // warn or err. The value in having those for debugging is too high to silence them, even
        // until client libraries catch up and implement a callback handler.
        if (Logging.global.callback == null and (message_level == .warn or message_level == .err)) {
            std.log.defaultLog(message_level, scope, format, args);
            return;
        }

        // Protect everything with a mutex - logging can be called from different threads
        // simultaneously, and there's only one buffer for now.
        Logging.global.mutex.lock();
        defer Logging.global.mutex.unlock();

        const callback = Logging.global.callback orelse return;

        const tb_message_level: tb_log_level = @enumFromInt(@intFromEnum(message_level));
        const prefix = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
        const output = std.fmt.bufPrint(
            &Logging.global.buffer,
            prefix ++ format,
            args,
        ) catch |err| switch (err) {
            error.NoSpaceLeft => blk: {
                // Print an error indicating the log message has been truncated, before the
                // truncated log itself.
                const message = "the following log message has been truncated:";
                callback(tb_message_level, message.ptr, message.len);

                break :blk &Logging.global.buffer;
            },
            else => unreachable,
        };

        callback(tb_message_level, output.ptr, @intCast(output.len));
    }
};
