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
const CreateAccountResults = tb.CreateAccountResults;
const CreateTransferResults = tb.CreateTransferResults;
const CommitTransferResults = tb.CommitTransferResults;

const Operation = @import("tigerbeetle/src/state_machine.zig").Operation;
const MessageBus = @import("tigerbeetle/src/message_bus.zig").MessageBusClient;
const Client = @import("tigerbeetle/src/client.zig").Client;
const IO = @import("tigerbeetle/src/io.zig").IO;

const vr = @import("tigerbeetle/src/vr.zig");

var globals: Globals = undefined;

/// N-API will call this constructor automatically to register the module.
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    translate.register_function(env, exports, "init", init) catch return null;
    translate.register_function(env, exports, "deinit", deinit) catch return null;
    translate.register_function(env, exports, "batch", batch) catch return null;
    translate.register_function(env, exports, "tick", tick) catch return null;

    globals.init(env) catch return null;

    if (c.napi_add_env_cleanup_hook(env, cleanup_globals_hook, null) != .napi_ok) {
        translate.throw(env, "Failed to add cleanup hook") catch {};
        return null;
    }

    return exports;
}

fn cleanup_globals_hook(arg: ?*c_void) callconv(.C) void {
    assert(arg == null);
    globals.deinit();
}

const Globals = struct {
    io: IO,
    napi_undefined: c.napi_value,

    pub fn init(self: *Globals, env: c.napi_env) !void {
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
    }

    pub fn deinit(self: *Globals) void {
        self.io.deinit();
    }
};

const Context = struct {
    io: *IO,
    configuration: [32]std.net.Address,
    message_bus: MessageBus,
    client: Client,

    fn create(env: c.napi_env, allocator: *std.mem.Allocator, io: *IO, id: u128, cluster: u128, configuration_raw: []const u8) !c.napi_value {
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
            id,
            context.io,
        );
        errdefer context.message_bus.deinit();

        context.client = try Client.init(
            allocator,
            id,
            cluster,
            @intCast(u16, configuration.len),
            &context.message_bus,
        );
        errdefer context.client.deinit();
        context.message_bus.process = .{ .client = &context.client };

        const ret = try translate.create_external(env, context);

        return ret;
    }
};

fn contextCast(context_raw: *c_void) !*Context {
    return @ptrCast(*Context, @alignCast(@alignOf(Context), context_raw)); // TODO: @Isaac please review.
}

fn decode_from_object(comptime T: type, env: c.napi_env, object: c.napi_value) !T {
    return switch (T) {
        Commit => Commit{
            .id = try translate.u128_from_object(env, object, "id"),
            .custom_1 = try translate.u128_from_object(env, object, "custom_1"),
            .custom_2 = try translate.u128_from_object(env, object, "custom_2"),
            .custom_3 = try translate.u128_from_object(env, object, "custom_3"),
            .flags = @bitCast(CommitFlags, try translate.u64_from_object(env, object, "flags")),
        },
        Transfer => Transfer{
            .id = try translate.u128_from_object(env, object, "id"),
            .debit_account_id = try translate.u128_from_object(env, object, "debit_account_id"),
            .credit_account_id = try translate.u128_from_object(env, object, "credit_account_id"),
            .custom_1 = try translate.u128_from_object(env, object, "custom_1"),
            .custom_2 = try translate.u128_from_object(env, object, "custom_2"),
            .custom_3 = try translate.u128_from_object(env, object, "custom_3"),
            .flags = @bitCast(TransferFlags, try translate.u64_from_object(env, object, "flags")),
            .amount = try translate.u64_from_object(env, object, "amount"),
            .timeout = try translate.u64_from_object(env, object, "timeout"),
        },
        Account => Account{
            .id = try translate.u128_from_object(env, object, "id"),
            .custom = try translate.u128_from_object(env, object, "custom"),
            .flags = @bitCast(AccountFlags, try translate.u64_from_object(env, object, "flags")),
            .unit = try translate.u64_from_object(env, object, "unit"),
            .debit_reserved = try translate.u64_from_object(env, object, "debit_reserved"),
            .debit_accepted = try translate.u64_from_object(env, object, "debit_accepted"),
            .credit_reserved = try translate.u64_from_object(env, object, "credit_reserved"),
            .credit_accepted = try translate.u64_from_object(env, object, "credit_accepted"),
            .debit_reserved_limit = try translate.u64_from_object(env, object, "debit_reserved_limit"),
            .debit_accepted_limit = try translate.u64_from_object(env, object, "debit_accepted_limit"),
            .credit_reserved_limit = try translate.u64_from_object(env, object, "credit_reserved_limit"),
            .credit_accepted_limit = try translate.u64_from_object(env, object, "credit_accepted_limit"),
        },
        u128 => try translate.u128_from_value(env, object, "Account lookup must be an id."),
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

    for (events) |event| std.debug.print("Decoded event {}\n", .{event});

    return std.mem.sliceAsBytes(events);
}

fn decode_slice_from_array(env: c.napi_env, object: c.napi_value, operation: Operation) ![]u8 {
    const allocator = std.heap.c_allocator;
    return switch (operation) {
        .reserved, .init => {
            std.debug.print("Reserved operation {}", .{operation});
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
        CreateAccountResults, CreateTransferResults, CommitTransferResults => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(env, "Failed to create result object");

                try translate.u32_into_object(env, napi_object, "index", result.index, "Failed to set property \"index\" of result.");

                try translate.u32_into_object(env, napi_object, "result", @enumToInt(result.result), "Failed to set property \"result\" of result.");

                try translate.set_array_element(env, napi_array, i, napi_object, "Failed to set element in results array.");
            }
        },
        else => {
            var i: u32 = 0;
            while (i < results.len) : (i += 1) {
                const result = results[i];
                const napi_object = try translate.create_object(env, "Failed to create account lookup result object.");

                try translate.u128_into_object(env, napi_object, "id", result.id, "Failed to set property \"id\" of account lookup result.");

                try translate.u128_into_object(env, napi_object, "custom", result.custom, "Failed to set property \"custom\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "flags", @bitCast(u64, result.flags), "Failed to set property \"flags\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "unit", result.unit, "Failed to set property \"unit\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "debit_reserved", result.debit_reserved, "Failed to set property \"debit_reserved\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "debit_accepted", result.debit_accepted, "Failed to set property \"debit_accepted\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "credit_reserved", result.credit_reserved, "Failed to set property \"credit_reserved\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "credit_accepted", result.credit_accepted, "Failed to set property \"credit_accepted\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "debit_reserved_limit", result.debit_reserved_limit, "Failed to set property \"debit_reserved_limit\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "debit_accepted_limit", result.debit_accepted_limit, "Failed to set property \"debit_accepted_limit\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "credit_reserved_limit", result.credit_reserved_limit, "Failed to set property \"credit_reserved_limit\" of account lookup result.");

                try translate.u64_into_object(env, napi_object, "credit_accepted_limit", result.credit_accepted_limit, "Failed to set property \"credit_accepted_limit\" of account lookup result.");

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

    const id = translate.u128_from_object(env, argv[0], "client_id") catch return null;
    const cluster = translate.u128_from_object(env, argv[0], "cluster_id") catch return null;
    const configuration = translate.buffer_from_object(env, argv[0], "replica_addresses") catch return null;

    const allocator = std.heap.c_allocator;

    const globals_raw = translate.globals(env) catch return null;
    const context = Context.create(env, allocator, &globals.io, id, cluster, configuration) catch {
        // TODO: switch on err and provide more detailed messages
        translate.throw(env, "Failed to initialize Client.") catch return null;
    };

    return context;
}

fn batch(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }

    const allocator = std.heap.c_allocator;
    if (argc != 4) translate.throw(env, "Function batch() requires 4 arguments exactly.") catch return null;

    const context_raw = translate.value_external(env, argv[0], "Failed to get Client Context pointer.") catch return null;
    const context = contextCast(context_raw.?) catch return null; // TODO: @Isaac, please review
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

    context.client.batch(@bitCast(u128, user_data), on_result, operation, events);

    return null;
}

fn on_result(user_data: u128, operation: Operation, results: []const u8) void {
    const env = @bitCast(translate.UserData, user_data).env;
    const callback_reference = @bitCast(translate.UserData, user_data).callback_reference;
    const napi_callback = translate.reference_value(env, callback_reference, "Failed to get callback reference.") catch return;
    const scope = translate.scope(env, "Failed to get \"this\" for results callback.") catch return;
    const globals_raw = translate.globals(env) catch return;
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
        .create_accounts => encode_napi_results_array(CreateAccountResults, env, results) catch return,
        .create_transfers => encode_napi_results_array(CreateTransferResults, env, results) catch return,
        .commit_transfers => encode_napi_results_array(CommitTransferResults, env, results) catch return,
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
    const context = contextCast(context_raw.?) catch return null; // TODO: @Isaac, please review

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
    const context = contextCast(context_raw.?) catch return null; // TODO: @Isaac, please review
    context.client.deinit();
    context.message_bus.deinit();

    return null;
}
