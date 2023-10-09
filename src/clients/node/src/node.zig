const std = @import("std");
const assert = std.debug.assert;

const c = @import("c.zig");
const translate = @import("translate.zig");
const tb = @import("../../../tigerbeetle.zig");

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const Storage = @import("../../../storage.zig").Storage;
const StateMachine = @import("../../../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const Operation = StateMachine.Operation;
const MessageBus = @import("../../../message_bus.zig").MessageBusClient;
const MessagePool = @import("../../../message_pool.zig").MessagePool;
const IO = @import("../../../io.zig").IO;
const constants = @import("../../../constants.zig");

const vsr = @import("../../../vsr.zig");
const Header = vsr.Header;
const Client = vsr.Client(StateMachine, MessageBus);

pub const std_options = struct {
    // Since this is running in application space, log only critical messages to reduce noise.
    pub const log_level: std.log.Level = .err;
};

/// N-API will call this constructor automatically to register the module.
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    translate.register_function(env, exports, "init", init) catch return null;
    translate.register_function(env, exports, "deinit", deinit) catch return null;
    translate.register_function(env, exports, "request", request) catch return null;
    translate.register_function(env, exports, "raw_request", raw_request) catch return null;
    translate.register_function(env, exports, "tick", tick) catch return null;

    translate.u32_into_object(
        env,
        exports,
        "tick_ms",
        constants.tick_ms,
        "failed to add tick_ms to exports",
    ) catch return null;

    const allocator = std.heap.c_allocator;
    var global = Globals.init(allocator, env) catch {
        std.log.err("Failed to initialise environment.\n", .{});
        return null;
    };
    errdefer global.deinit();

    // Tie the global state to this Node.js environment. This allows us to be thread safe.
    // See https://nodejs.org/api/n-api.html#n_api_environment_life_cycle_apis.
    // A cleanup function is registered as well that Node will call when the environment
    // is torn down. Be careful not to call this function again as it will overwrite the global
    // state.
    translate.set_instance_data(
        env,
        @ptrCast(@alignCast(global)),
        Globals.destroy,
    ) catch {
        global.deinit();
        return null;
    };

    return exports;
}

const Globals = struct {
    allocator: std.mem.Allocator,
    io: IO,
    napi_undefined: c.napi_value,

    pub fn init(allocator: std.mem.Allocator, env: c.napi_env) !*Globals {
        const self = try allocator.create(Globals);
        errdefer allocator.destroy(self);

        self.allocator = allocator;

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
        self.io = IO.init(32, 0) catch {
            return translate.throw(env, "Failed to initialize io_uring");
        };
        errdefer self.io.deinit();

        if (c.napi_get_undefined(env, &self.napi_undefined) != c.napi_ok) {
            return translate.throw(env, "Failed to capture the value of \"undefined\".");
        }

        return self;
    }

    pub fn deinit(self: *Globals) void {
        self.io.deinit();
        self.allocator.destroy(self);
    }

    pub fn destroy(env: c.napi_env, data: ?*anyopaque, hint: ?*anyopaque) callconv(.C) void {
        _ = env;
        _ = hint;

        const self = globalsCast(data.?);
        self.deinit();
    }
};

fn globalsCast(globals_raw: *anyopaque) *Globals {
    return @ptrCast(@alignCast(globals_raw));
}

const Context = struct {
    io: *IO,
    addresses: []std.net.Address,
    client: Client,
    message_pool: MessagePool,

    fn create(
        env: c.napi_env,
        allocator: std.mem.Allocator,
        io: *IO,
        cluster: u32,
        addresses_raw: []const u8,
    ) !c.napi_value {
        const context = try allocator.create(Context);
        errdefer allocator.destroy(context);

        context.io = io;
        context.message_pool = try MessagePool.init(allocator, .client);
        errdefer context.message_pool.deinit(allocator);

        context.addresses = try vsr.parse_addresses(allocator, addresses_raw, constants.replicas_max);
        errdefer allocator.free(context.addresses);
        assert(context.addresses.len > 0);

        const client_id = std.crypto.random.int(u128);
        context.client = try Client.init(
            allocator,
            client_id,
            cluster,
            @as(u8, @intCast(context.addresses.len)),
            &context.message_pool,
            .{
                .configuration = context.addresses,
                .io = context.io,
            },
        );
        errdefer context.client.deinit(allocator);

        return try translate.create_external(env, context);
    }
};

fn contextCast(context_raw: *anyopaque) !*Context {
    return @ptrCast(@alignCast(context_raw));
}

fn validate_timestamp(env: c.napi_env, object: c.napi_value) !u64 {
    const timestamp = try translate.u64_from_object(env, object, "timestamp");
    if (timestamp != 0) {
        return translate.throw(
            env,
            "Timestamp should be set as 0 as this will be set correctly by the Server.",
        );
    }

    return timestamp;
}

fn decode_from_object(comptime T: type, env: c.napi_env, object: c.napi_value) !T {
    return switch (T) {
        Transfer => Transfer{
            .id = try translate.u128_from_object(env, object, "id"),
            .debit_account_id = try translate.u128_from_object(env, object, "debit_account_id"),
            .credit_account_id = try translate.u128_from_object(env, object, "credit_account_id"),
            .amount = try translate.u128_from_object(env, object, "amount"),
            .pending_id = try translate.u128_from_object(env, object, "pending_id"),
            .user_data_128 = try translate.u128_from_object(env, object, "user_data_128"),
            .user_data_64 = try translate.u64_from_object(env, object, "user_data_64"),
            .user_data_32 = try translate.u32_from_object(env, object, "user_data_32"),
            .timeout = try translate.u32_from_object(env, object, "timeout"),
            .ledger = try translate.u32_from_object(env, object, "ledger"),
            .code = try translate.u16_from_object(env, object, "code"),
            .flags = @as(TransferFlags, @bitCast(try translate.u16_from_object(env, object, "flags"))),
            .timestamp = try validate_timestamp(env, object),
        },
        Account => Account{
            .id = try translate.u128_from_object(env, object, "id"),
            .debits_pending = try translate.u128_from_object(env, object, "debits_pending"),
            .debits_posted = try translate.u128_from_object(env, object, "debits_posted"),
            .credits_pending = try translate.u128_from_object(env, object, "credits_pending"),
            .credits_posted = try translate.u128_from_object(env, object, "credits_posted"),
            .user_data_128 = try translate.u128_from_object(env, object, "user_data_128"),
            .user_data_64 = try translate.u64_from_object(env, object, "user_data_64"),
            .user_data_32 = try translate.u32_from_object(env, object, "user_data_32"),
            .reserved = try translate.u32_from_object(env, object, "reserved"),
            .ledger = try translate.u32_from_object(env, object, "ledger"),
            .code = try translate.u16_from_object(env, object, "code"),
            .flags = @bitCast(try translate.u16_from_object(env, object, "flags")),
            .timestamp = try validate_timestamp(env, object),
        },
        u128 => try translate.u128_from_value(env, object, "lookup"),
        else => unreachable,
    };
}

pub fn decode_events(
    env: c.napi_env,
    array: c.napi_value,
    operation: Operation,
    output: []u8,
) !usize {
    return switch (operation) {
        .create_accounts => try decode_events_from_array(env, array, Account, output),
        .create_transfers => try decode_events_from_array(env, array, Transfer, output),
        .lookup_accounts => try decode_events_from_array(env, array, u128, output),
        .lookup_transfers => try decode_events_from_array(env, array, u128, output),
    };
}

fn decode_events_from_array(
    env: c.napi_env,
    array: c.napi_value,
    comptime T: type,
    output: []u8,
) !usize {
    const array_length = try translate.array_length(env, array);
    if (array_length < 1) return translate.throw(env, "Batch must contain at least one event.");

    const body_length = @sizeOf(T) * array_length;
    if (@sizeOf(Header) + body_length > constants.message_size_max) {
        return translate.throw(env, "Batch is larger than the maximum message size.");
    }

    // We take a slice on `output` to ensure that its length is a multiple of @sizeOf(T) to prevent
    // a safety-checked runtime panic from `bytesAsSlice` for non-multiple sizes.
    var results = std.mem.bytesAsSlice(T, output[0..body_length]);

    var i: u32 = 0;
    while (i < array_length) : (i += 1) {
        const entry = try translate.array_element(env, array, i);
        results[i] = try decode_from_object(T, env, entry);
    }

    return body_length;
}

fn encode_napi_results_array(
    comptime Result: type,
    env: c.napi_env,
    data: []const u8,
) !c.napi_value {
    const results = std.mem.bytesAsSlice(Result, data);
    const napi_array = try translate.create_array(
        env,
        @as(u32, @intCast(results.len)),
        "Failed to allocate array for results.",
    );

    switch (Result) {
        CreateAccountsResult, CreateTransfersResult => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(
                    env,
                    "Failed to create result object",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "index",
                    result.index,
                    "Failed to set property \"index\" of result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "result",
                    @intFromEnum(result.result),
                    "Failed to set property \"result\" of result.",
                );

                try translate.set_array_element(
                    env,
                    napi_array,
                    i,
                    napi_object,
                    "Failed to set element in results array.",
                );
            }
        },
        Account => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(
                    env,
                    "Failed to create account lookup result object.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "id",
                    result.id,
                    "Failed to set property \"id\" of account lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "debits_pending",
                    result.debits_pending,
                    "Failed to set property \"debits_pending\" of account lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "debits_posted",
                    result.debits_posted,
                    "Failed to set property \"debits_posted\" of account lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "credits_pending",
                    result.credits_pending,
                    "Failed to set property \"credits_pending\" of account lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "credits_posted",
                    result.credits_posted,
                    "Failed to set property \"credits_posted\" of account lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "user_data_128",
                    result.user_data_128,
                    "Failed to set property \"user_data_128\" of account lookup result.",
                );

                try translate.u64_into_object(
                    env,
                    napi_object,
                    "user_data_64",
                    result.user_data_64,
                    "Failed to set property \"user_data_64\" of account lookup result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "user_data_32",
                    result.user_data_32,
                    "Failed to set property \"user_data_32\" of account lookup result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "reserved",
                    result.reserved,
                    "Failed to set property \"reserved\" of account lookup result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "ledger",
                    @as(u32, @intCast(result.ledger)),
                    "Failed to set property \"ledger\" of account lookup result.",
                );

                try translate.u16_into_object(
                    env,
                    napi_object,
                    "code",
                    @as(u16, @intCast(result.code)),
                    "Failed to set property \"code\" of account lookup result.",
                );

                try translate.u16_into_object(
                    env,
                    napi_object,
                    "flags",
                    @as(u16, @bitCast(result.flags)),
                    "Failed to set property \"flags\" of account lookup result.",
                );

                try translate.u64_into_object(
                    env,
                    napi_object,
                    "timestamp",
                    result.timestamp,
                    "Failed to set property \"timestamp\" of account lookup result.",
                );

                try translate.set_array_element(
                    env,
                    napi_array,
                    i,
                    napi_object,
                    "Failed to set element in results array.",
                );
            }
        },
        Transfer => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(
                    env,
                    "Failed to create transfer lookup result object.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "id",
                    result.id,
                    "Failed to set property \"id\" of transfer lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "debit_account_id",
                    result.debit_account_id,
                    "Failed to set property \"debit_account_id\" of transfer lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "credit_account_id",
                    result.credit_account_id,
                    "Failed to set property \"credit_account_id\" of transfer lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "amount",
                    result.amount,
                    "Failed to set property \"amount\" of transfer lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "pending_id",
                    result.pending_id,
                    "Failed to set property \"pending_id\" of transfer lookup result.",
                );

                try translate.u128_into_object(
                    env,
                    napi_object,
                    "user_data_128",
                    result.user_data_128,
                    "Failed to set property \"user_data_128\" of transfer lookup result.",
                );

                try translate.u64_into_object(
                    env,
                    napi_object,
                    "user_data_64",
                    result.user_data_64,
                    "Failed to set property \"user_data_64\" of transfer lookup result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "user_data_32",
                    result.user_data_32,
                    "Failed to set property \"user_data_32\" of transfer lookup result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "timeout",
                    result.timeout,
                    "Failed to set property \"timeout\" of transfer lookup result.",
                );

                try translate.u32_into_object(
                    env,
                    napi_object,
                    "ledger",
                    @as(u32, @intCast(result.ledger)),
                    "Failed to set property \"ledger\" of transfer lookup result.",
                );

                try translate.u16_into_object(
                    env,
                    napi_object,
                    "code",
                    @as(u16, @intCast(result.code)),
                    "Failed to set property \"code\" of transfer lookup result.",
                );

                try translate.u16_into_object(
                    env,
                    napi_object,
                    "flags",
                    @as(u16, @bitCast(result.flags)),
                    "Failed to set property \"flags\" of transfer lookup result.",
                );

                try translate.u64_into_object(
                    env,
                    napi_object,
                    "timestamp",
                    result.timestamp,
                    "Failed to set property \"timestamp\" of transfer lookup result.",
                );

                try translate.set_array_element(
                    env,
                    napi_array,
                    i,
                    napi_object,
                    "Failed to set element in results array.",
                );
            }
        },
        else => unreachable,
    }

    return napi_array;
}

/// Add-on code
fn init(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 1;
    var argv: [1]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }
    if (argc != 1) translate.throw(
        env,
        "Function init() must receive 1 argument exactly.",
    ) catch return null;

    const cluster = translate.u32_from_object(env, argv[0], "cluster_id") catch return null;
    const addresses = translate.slice_from_object(
        env,
        argv[0],
        "replica_addresses",
    ) catch return null;

    const allocator = std.heap.c_allocator;

    const globals_raw = translate.globals(env) catch return null;
    const globals = globalsCast(globals_raw.?);

    const context = Context.create(env, allocator, &globals.io, cluster, addresses) catch {
        // TODO: switch on err and provide more detailed messages
        translate.throw(env, "Failed to initialize Client.") catch return null;
    };

    return context;
}

/// This function decodes and validates an array of Node objects, one-by-one, directly into an
/// available message before requesting the client to send it.
fn request(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 4;
    var argv: [4]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    if (argc != 4) translate.throw(
        env,
        "Function request() requires 4 arguments exactly.",
    ) catch return null;

    const context_raw = translate.value_external(
        env,
        argv[0],
        "Failed to get Client Context pointer.",
    ) catch return null;
    const context = contextCast(context_raw.?) catch return null;
    const operation_int = translate.u32_from_value(env, argv[1], "operation") catch return null;

    if (!@as(vsr.Operation, @enumFromInt(operation_int)).valid(StateMachine)) {
        translate.throw(env, "Unknown operation.") catch return null;
    }

    if (context.client.messages_available == 0) {
        translate.throw(
            env,
            "Too many outstanding requests - message pool exhausted.",
        ) catch return null;
    }
    const message = context.client.get_message();
    errdefer context.client.release(message);

    const operation = @as(Operation, @enumFromInt(@as(u8, @intCast(operation_int))));
    const body_length = decode_events(
        env,
        argv[2],
        operation,
        message.buffer[@sizeOf(Header)..],
    ) catch |err| switch (err) {
        error.ExceptionThrown => return null,
    };

    // This will create a reference (in V8) to the user's JS callback that we must eventually also
    // free in order to avoid a leak. We therefore do this last to ensure we cannot fail after
    // taking this reference.
    const user_data = translate.user_data_from_value(env, argv[3]) catch return null;
    context.client.request(@as(u128, @bitCast(user_data)), on_result, operation, message, body_length);

    return null;
}

/// The batch has already been encoded into a byte slice. This means that we only have to do one
/// copy directly into an available message. No validation of the encoded data is performed except
/// that it will fit into the message buffer.
fn raw_request(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 4;
    var argv: [4]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    if (argc != 4) translate.throw(
        env,
        "Function request() requires 4 arguments exactly.",
    ) catch return null;

    const context_raw = translate.value_external(
        env,
        argv[0],
        "Failed to get Client Context pointer.",
    ) catch return null;
    const context = contextCast(context_raw.?) catch return null;
    const operation_int = translate.u32_from_value(env, argv[1], "operation") catch return null;

    if (!@as(vsr.Operation, @enumFromInt(operation_int)).valid(StateMachine)) {
        translate.throw(env, "Unknown operation.") catch return null;
    }
    const operation = @as(Operation, @enumFromInt(@as(u8, @intCast(operation_int))));

    if (context.client.messages_available == 0) {
        translate.throw(
            env,
            "Too many outstanding requests - message pool exhausted.",
        ) catch return null;
    }
    const message = context.client.get_message();
    errdefer context.client.release(message);

    const body_length = translate.bytes_from_buffer(
        env,
        argv[2],
        message.buffer[@sizeOf(Header)..],
        "raw_batch",
    ) catch |err| switch (err) {
        error.ExceptionThrown => return null,
    };

    // This will create a reference (in V8) to the user's JS callback that we must eventually also
    // free in order to avoid a leak. We therefore do this last to ensure we cannot fail after
    // taking this reference.
    const user_data = translate.user_data_from_value(env, argv[3]) catch return null;
    context.client.request(@as(u128, @bitCast(user_data)), on_result, operation, message, body_length);

    return null;
}

fn on_result(user_data: u128, operation: Operation, results: []const u8) void {
    // A reference to the user's JS callback was made in `request` or `raw_request`. This MUST be
    // cleaned up regardless of the result of this function.
    const env = @as(translate.UserData, @bitCast(user_data)).env;
    const callback_reference = @as(translate.UserData, @bitCast(user_data)).callback_reference;
    defer translate.delete_reference(env, callback_reference) catch {
        std.log.warn("on_result: Failed to delete reference to user's JS callback.", .{});
    };

    const napi_callback = translate.reference_value(
        env,
        callback_reference,
        "Failed to get callback reference.",
    ) catch return;
    const scope = translate.scope(
        env,
        "Failed to get \"this\" for results callback.",
    ) catch return;
    const globals_raw = translate.globals(env) catch return;
    const globals = globalsCast(globals_raw.?);
    const argc: usize = 2;
    var argv: [argc]c.napi_value = undefined;

    const napi_results = switch (operation) {
        .create_accounts => encode_napi_results_array(
            CreateAccountsResult,
            env,
            results,
        ) catch return,
        .create_transfers => encode_napi_results_array(
            CreateTransfersResult,
            env,
            results,
        ) catch return,
        .lookup_accounts => encode_napi_results_array(Account, env, results) catch return,
        .lookup_transfers => encode_napi_results_array(Transfer, env, results) catch return,
    };

    argv[0] = globals.napi_undefined;
    argv[1] = napi_results;

    translate.call_function(env, scope, napi_callback, argc, argv[0..]) catch {
        translate.throw(env, "Failed to call JS results callback.") catch return;
    };
}

fn tick(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 1;
    var argv: [1]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    if (argc != 1) translate.throw(
        env,
        "Function tick() requires 1 argument exactly.",
    ) catch return null;

    const context_raw = translate.value_external(
        env,
        argv[0],
        "Failed to get Client Context pointer.",
    ) catch return null;
    const context = contextCast(context_raw.?) catch return null;

    context.client.tick();
    context.io.tick() catch |err| switch (err) {
        // TODO exhaustive switch
        else => {
            translate.throw(env, "Failed to tick IO.") catch return null;
        },
    };
    return null;
}

fn deinit(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 1;
    var argv: [1]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    if (argc != 1) translate.throw(
        env,
        "Function deinit() requires 1 argument exactly.",
    ) catch return null;

    const context_raw = translate.value_external(
        env,
        argv[0],
        "Failed to get Client Context pointer.",
    ) catch return null;
    const context = contextCast(context_raw.?) catch return null;

    const allocator = std.heap.c_allocator;
    context.client.deinit(allocator);
    context.message_pool.deinit(allocator);
    allocator.free(context.addresses);

    return null;
}
