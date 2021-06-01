const std = @import("std");
const assert = std.debug.assert;

const c = @import("c.zig");
const translate = @import("translate.zig");
const tb = @import("tigerbeetle/src/tigerbeetle.zig");

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const Commit = tb.Commit;
const CommitFlags = tb.CommitFlags;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;
const CommitTransfersResult = tb.CommitTransfersResult;

const Operation = @import("tigerbeetle/src/state_machine.zig").Operation;
const MessageBus = @import("tigerbeetle/src/message_bus.zig").MessageBusClient;
const Client = @import("tigerbeetle/src/client.zig").Client;
const IO = @import("tigerbeetle/src/io.zig").IO;

const vr = @import("tigerbeetle/src/vr.zig");

pub const log_level: std.log.Level = .info;

/// N-API will call this constructor automatically to register the module.
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    translate.register_function(env, exports, "init", init) catch return null;
    translate.register_function(env, exports, "deinit", deinit) catch return null;
    translate.register_function(env, exports, "request", request) catch return null;
    translate.register_function(env, exports, "tick", tick) catch return null;

    const allocator = std.heap.c_allocator;
    var global = Globals.init(allocator, env) catch {
        std.log.emerg("Failed to initialise environment.\n", .{});
        return null;
    };
    errdefer global.deinit();

    // Tie the global state to this Node.js environment. This allows us to be thread safe.
    // See https://nodejs.org/api/n-api.html#n_api_environment_life_cycle_apis.
    // A cleanup function is registered as well that Node will call when the environment
    // is torn down. Be careful not to call this function again as it will overwrite the global state.
    translate.set_instance_data(
        env,
        @ptrCast(*c_void, @alignCast(@alignOf(u8), global)),
        Globals.destroy,
    ) catch {
        global.deinit();
        return null;
    };

    return exports;
}

const Globals = struct {
    allocator: *std.mem.Allocator,
    io: IO,
    napi_undefined: c.napi_value,

    pub fn init(allocator: *std.mem.Allocator, env: c.napi_env) !*Globals {
        const self = try allocator.create(Globals);
        errdefer allocator.destroy(self);

        self.allocator = allocator;

        // Be careful to size the SQ ring to only a few SQE entries and to share a single IO instance
        // across multiple clients to stay under kernel limits:
        //
        // The memory required by io_uring is accounted under the rlimit memlocked option, which can be
        // quite low on some setups (64K). The default is usually enough for most use cases, but
        // bigger rings or things like registered buffers deplete it quickly. Root isn't under this
        // restriction, but regular users are.
        //
        // Check `/etc/security/limits.conf` for user settings, or `/etc/systemd/user.conf` and
        // `/etc/systemd/system.conf` for systemd setups.
        self.io = IO.init(32, 0) catch {
            return translate.throw(env, "Failed to initialize io_uring");
        };
        errdefer self.io.deinit();

        if (c.napi_get_undefined(env, &self.napi_undefined) != .napi_ok) {
            return translate.throw(env, "Failed to capture the value of \"undefined\".");
        }

        return self;
    }

    pub fn deinit(self: *Globals) void {
        self.io.deinit();
        self.allocator.destroy(self);
    }

    pub fn destroy(env: c.napi_env, data: ?*c_void, hint: ?*c_void) callconv(.C) void {
        const self = globalsCast(data.?);
        self.deinit();
    }
};

fn globalsCast(globals_raw: *c_void) *Globals {
    return @ptrCast(*Globals, @alignCast(@alignOf(Globals), globals_raw));
}

const Context = struct {
    io: *IO,
    configuration: [32]std.net.Address,
    message_bus: MessageBus,
    client: Client,

    fn create(env: c.napi_env, allocator: *std.mem.Allocator, io: *IO, cluster: u128, configuration_raw: []const u8) !c.napi_value {
        const context = try allocator.create(Context);
        errdefer allocator.destroy(context);

        context.io = io;

        const configuration = try vr.parse_configuration(allocator, configuration_raw);
        errdefer allocator.free(configuration);
        assert(configuration.len > 0);
        for (configuration) |address, index| context.configuration[index] = address;

        context.message_bus = try MessageBus.init(
            allocator,
            cluster,
            context.configuration[0..configuration.len],
            1, // value for client from ProcessType enum (see message_bus.zig)
            context.io,
        );
        errdefer context.message_bus.deinit();

        context.client = try Client.init(
            allocator,
            cluster,
            @intCast(u16, configuration.len),
            &context.message_bus,
        );
        errdefer context.client.deinit();
        context.message_bus.process = .{ .client = &context.client };

        const ret = try translate.create_external(env, context);

        std.log.debug("Created context\n", .{});

        return ret;
    }
};

fn contextCast(context_raw: *c_void) !*Context {
    return @ptrCast(*Context, @alignCast(@alignOf(Context), context_raw));
}

fn decode_from_object(comptime T: type, env: c.napi_env, object: c.napi_value) !T {
    return switch (T) {
        Commit => Commit{
            .id = try translate.u128_from_object(env, object, "id"),
            .reserved = try translate.copy_buffer_of_length_from_object(env, object, 32, "reserved"),
            .code = try translate.u32_from_object(env, object, "code"),
            .flags = @bitCast(CommitFlags, try translate.u32_from_object(env, object, "flags")),
        },
        Transfer => Transfer{
            .id = try translate.u128_from_object(env, object, "id"),
            .debit_account_id = try translate.u128_from_object(env, object, "debit_account_id"),
            .credit_account_id = try translate.u128_from_object(env, object, "credit_account_id"),
            .user_data = try translate.u128_from_object(env, object, "user_data"),
            .reserved = try translate.copy_buffer_of_length_from_object(env, object, 32, "reserved"),
            .timeout = try translate.u64_from_object(env, object, "timeout"),
            .code = try translate.u32_from_object(env, object, "code"),
            .flags = @bitCast(TransferFlags, try translate.u32_from_object(env, object, "flags")),
            .amount = try translate.u64_from_object(env, object, "amount"),
        },
        Account => Account{
            .id = try translate.u128_from_object(env, object, "id"),
            .user_data = try translate.u128_from_object(env, object, "user_data"),
            .reserved = try translate.copy_buffer_of_length_from_object(env, object, 48, "reserved"),
            .unit = @intCast(u16, try translate.u32_from_object(env, object, "unit")),
            .code = @intCast(u16, try translate.u32_from_object(env, object, "code")),
            .flags = @bitCast(AccountFlags, try translate.u32_from_object(env, object, "flags")),
            .debits_reserved = try translate.u64_from_object(env, object, "debits_reserved"),
            .debits_accepted = try translate.u64_from_object(env, object, "debits_accepted"),
            .credits_reserved = try translate.u64_from_object(env, object, "credits_reserved"),
            .credits_accepted = try translate.u64_from_object(env, object, "credits_accepted"),
        },
        u128 => try translate.u128_from_value(env, object, "Account lookup"),
        else => unreachable,
    };
}

fn decode_from_array(comptime T: type, env: c.napi_env, array: c.napi_value) ![]u8 {
    const array_length = try translate.array_length(env, array);
    if (array_length < 1) return translate.throw(env, "Batch must contain at least one event.");

    const allocator = std.heap.c_allocator;
    const events = allocator.alloc(T, array_length) catch {
        return translate.throw(env, "Failed to allocate memory for batch in client.");
    };
    errdefer allocator.free(events);

    var i: u32 = 0;
    while (i < array_length) : (i += 1) {
        const entry = try translate.array_element(env, array, i);
        events[i] = try decode_from_object(T, env, entry);
    }

    for (events) |event| std.log.debug("Decoded event {}\n", .{event});

    return std.mem.sliceAsBytes(events);
}

fn decode_slice_from_array(env: c.napi_env, object: c.napi_value, operation: Operation) ![]u8 {
    const allocator = std.heap.c_allocator;
    return switch (operation) {
        .reserved, .init => {
            std.log.err("Reserved operation {}", .{operation});
            return translate.throw(env, "Reserved operation.");
        },
        .create_accounts => try decode_from_array(Account, env, object),
        .create_transfers => try decode_from_array(Transfer, env, object),
        .commit_transfers => try decode_from_array(Commit, env, object),
        .lookup_accounts => try decode_from_array(u128, env, object),
    };
}

fn encode_napi_results_array(
    comptime Result: type,
    env: c.napi_env,
    data: []const u8,
) !c.napi_value {
    const results = std.mem.bytesAsSlice(Result, data);
    const napi_array = try translate.create_array(env, @intCast(u32, results.len), "Failed to allocate array for results.");

    switch (Result) {
        CreateAccountsResult, CreateTransfersResult, CommitTransfersResult => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(env, "Failed to create result object");

                try translate.u32_into_object(env, napi_object, "index", result.index, "Failed to set property \"index\" of result.");

                try translate.u32_into_object(env, napi_object, "error", @enumToInt(result.result), "Failed to set property \"error\" of result.");

                try translate.set_array_element(env, napi_array, i, napi_object, "Failed to set element in results array.");
            }
        },
        else => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(env, "Failed to create account lookup result object.");

                try translate.u128_into_object(env, napi_object, "id", result.id, "Failed to set property \"id\" of account lookup result.");

                try translate.u128_into_object(env, napi_object, "user_data", result.user_data, "Failed to set property \"user_data\" of account lookup result.");
                
                try translate.byte_slice_into_object(env, napi_object, "reserved", &result.reserved, "Failed to set property \"reserved\" of account lookup result.");

                try translate.u32_into_object(env, napi_object, "unit", @intCast(u32, result.unit), "Failed to set property \"unit\" of account lookup result.");

                try translate.u32_into_object(env, napi_object, "code", @intCast(u32, result.code), "Failed to set property \"code\" of account lookup result.");

                try translate.u32_into_object(env, napi_object, "flags", @bitCast(u32, result.flags), "Failed to set property \"flags\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "debits_reserved", result.debits_reserved, "Failed to set property \"debits_reserved\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "debits_accepted", result.debits_accepted, "Failed to set property \"debits_accepted\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "credits_reserved", result.credits_reserved, "Failed to set property \"credits_reserved\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "credits_accepted", result.credits_accepted, "Failed to set property \"credits_accepted\" of account lookup result.");

                try translate.set_array_element(env, napi_array, i, napi_object, "Failed to set element in results array.");
            }
        },
    }

    return napi_array;
}

/// Add-on code
fn init(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }
    if (argc != 1) translate.throw(env, "Function init() must receive 1 argument exactly.") catch return null;

    const cluster = translate.u128_from_object(env, argv[0], "cluster_id") catch return null;
    const configuration = translate.buffer_from_object(env, argv[0], "replica_addresses") catch return null;

    const allocator = std.heap.c_allocator;

    const globals_raw = translate.globals(env) catch return null;
    const globals = globalsCast(globals_raw.?);
    const context = Context.create(env, allocator, &globals.io, cluster, configuration) catch {
        // TODO: switch on err and provide more detailed messages
        translate.throw(env, "Failed to initialize Client.") catch return null;
    };

    return context;
}

fn request(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    const allocator = std.heap.c_allocator;
    if (argc != 4) translate.throw(env, "Function request() requires 4 arguments exactly.") catch return null;

    const context_raw = translate.value_external(env, argv[0], "Failed to get Client Context pointer.") catch return null;
    const context = contextCast(context_raw.?) catch return null;
    const operation_int = translate.u32_from_value(env, argv[1], "operation") catch return null;
    const user_data = translate.user_data_from_value(env, argv[3]) catch return null;

    if (operation_int >= @typeInfo(Operation).Enum.fields.len) {
        translate.throw(env, "Unknown operation.") catch return null;
    }

    const operation = @intToEnum(Operation, @intCast(u8, operation_int));
    const events = decode_slice_from_array(env, argv[2], operation) catch |err| switch (err) {
        error.ExceptionThrown => {
            return null;
        },
    };
    defer allocator.free(events);

    context.client.request(@bitCast(u128, user_data), on_result, operation, events);

    return null;
}

fn on_result(user_data: u128, operation: Operation, results: []const u8) void {
    const env = @bitCast(translate.UserData, user_data).env;
    const callback_reference = @bitCast(translate.UserData, user_data).callback_reference;
    const napi_callback = translate.reference_value(env, callback_reference, "Failed to get callback reference.") catch return;
    const scope = translate.scope(env, "Failed to get \"this\" for results callback.") catch return;
    const globals_raw = translate.globals(env) catch return;
    const globals = globalsCast(globals_raw.?);
    const argc: usize = 2;
    var argv: [argc]c.napi_value = undefined;

    // if (client_error != null) {
    //     // TODO: determine message according to error
    //     const napi_error = create_napi_error(env, "Client could not send batch.") catch return;
    //     argv[0] = napi_error;
    //     argv[1] = napi_undefined;
    // } else {
    //     const napi_results = encode_napi_results_array(env, @bitCast([]Result, results)) catch return;
    //     argv[0] = napi_undefined;
    //     argv[1] = napi_results;
    // }

    const napi_results = switch (operation) {
        .reserved, .init => {
            translate.throw(env, "Reserved operation.") catch return;
        },
        .create_accounts => encode_napi_results_array(CreateAccountsResult, env, results) catch return,
        .create_transfers => encode_napi_results_array(CreateTransfersResult, env, results) catch return,
        .commit_transfers => encode_napi_results_array(CommitTransfersResult, env, results) catch return,
        .lookup_accounts => encode_napi_results_array(Account, env, results) catch return,
    };
    argv[0] = globals.napi_undefined;
    argv[1] = napi_results;

    // TODO: add error handling (but allow JS function to throw an exception)
    translate.call_function(env, scope, napi_callback, argc, argv[0..]) catch return;

    translate.delete_reference(env, callback_reference) catch return;
}

fn tick(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    const allocator = std.heap.c_allocator;
    if (argc != 1) translate.throw(env, "Function tick() requires 1 argument exactly.") catch return null;

    const context_raw = translate.value_external(env, argv[0], "Failed to get Client Context pointer.") catch return null;
    const context = contextCast(context_raw.?) catch return null;

    context.client.tick();
    context.io.tick() catch unreachable; // TODO Use a block to throw an exception including the Zig error.
    return null;
}

fn deinit(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    if (argc != 1) translate.throw(env, "Function deinit() requires 1 argument exactly.") catch return null;

    const context_raw = translate.value_external(env, argv[0], "Failed to get Client Context pointer.") catch return null;
    const context = contextCast(context_raw.?) catch return null;
    context.client.deinit();
    context.message_bus.deinit();

    return null;
}
