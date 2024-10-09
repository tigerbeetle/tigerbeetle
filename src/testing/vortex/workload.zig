const std = @import("std");
const Driver = @import("./driver.zig");
const arbitrary = @import("./arbitrary.zig");

const log = std.log.scoped(.workload);
const assert = std.debug.assert;
const expect = std.testing.expect;

const events_count_max = Driver.events_count_max;

pub const CLIArgs = struct {
    @"cluster-id": u128,
    addresses: []const u8,
    @"driver-command": []const u8,
};

pub fn main(allocator: std.mem.Allocator, args: CLIArgs) !void {
    var driver = try start_driver(allocator, args);
    defer {
        if (driver.state() == .running) {
            _ = driver.terminate() catch {};
        }
        driver.destroy(allocator);
    }

    var model = Model{};
    const seed = std.crypto.random.int(u64);
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    for (0..std.math.maxInt(u64)) |i| {
        const command = try random_command(random, &model);
        const result = try execute(command, driver) orelse break;
        try reconcile(result, &command, &model);

        log.info("accounts created = {d}, commands run = {d}", .{ model.accounts_id_next, i });
    }
}

const Command = union(enum) {
    create_accounts: []Driver.Event(.create_accounts),
    create_transfers: []Driver.Event(.create_transfers),
    lookup_accounts: []Driver.Event(.lookup_accounts),
};

const CommandBuffers = FixedSizeBuffers(Command);
var command_buffers: CommandBuffers = std.mem.zeroes(CommandBuffers);

const Result = union(enum) {
    create_accounts: []Driver.Result(.create_accounts),
    create_transfers: []Driver.Result(.create_transfers),
    lookup_accounts: []Driver.Result(.lookup_accounts),
};
const ResultBuffers = FixedSizeBuffers(Result);
var result_buffers: ResultBuffers = std.mem.zeroes(ResultBuffers);

fn execute(command: Command, driver: *const Driver) !?Result {
    switch (command) {
        .create_accounts => |events| return try execute_regular(.create_accounts, events, driver),
        .create_transfers => |events| return try execute_regular(.create_transfers, events, driver),
        .lookup_accounts => |events| return try execute_regular(.lookup_accounts, events, driver),
    }
}

fn execute_regular(
    comptime operation: Driver.Operation,
    events: []Driver.Event(operation),
    driver: *const Driver,
) !?Result {
    try driver.send(operation, events);

    const buffer = @field(result_buffers, @tagName(operation))[0..events.len];
    const results = driver.receive(operation, buffer) catch |err| {
        switch (err) {
            error.EndOfStream => return null,
            else => return err,
        }
    };
    return @unionInit(Result, @tagName(operation), results);
}

fn reconcile(result: Result, command: *const Command, model: *Model) !void {
    switch (result) {
        .create_accounts => |entries| {
            for (entries) |entry| {
                if (entry.result != .ok) {
                    log.err("got result {s} for event {d}: {any}", .{
                        @tagName(entry.result),
                        entry.index,
                        command.create_accounts[entry.index],
                    });
                    return error.TestFailed;
                }
            }
            model.accounts_id_next += @intCast(command.create_accounts.len);
        },
        .create_transfers => |entries| {
            const transfers = command.create_transfers;

            for (entries, 0..) |entry, e| {
                // Check that linked transfers fail together.
                if (entry.index > 0) {
                    const preceeding_transfer = transfers[entry.index - 1];
                    if (preceeding_transfer.flags.linked) {
                        try expect(e > 0);
                        const preceeding_entry = entries[e - 1];
                        try expect(preceeding_entry.index == entry.index - 1);
                        try expect(preceeding_entry.result != .ok);
                    }
                }
                // No further validation needed for failed transfers.
                if (entry.result != .ok) {
                    continue;
                }
            }
        },
        else => {}, // TODO
    }
}

const Model = struct {
    accounts_id_next: u128 = 1,
};

fn random_command(random: std.Random, model: *const Model) !Command {
    const command_tag = arbitrary.weighted(random, std.meta.Tag(Command), .{
        .create_accounts = if (model.accounts_id_next < 100) 1 else 0,
        .create_transfers = if (model.accounts_id_next > 2) 10 else 0,
        .lookup_accounts = 0,
    }).?;
    switch (command_tag) {
        .create_accounts => return random_create_accounts(random, model),
        .create_transfers => return random_create_transfers(random, model),
        .lookup_accounts => unreachable,
    }
}

fn random_create_accounts(random: std.Random, model: *const Model) !Command {
    const events_count = random.intRangeAtMost(usize, 1, Driver.events_count_max);
    assert(events_count <= events_count_max);

    var buffer = command_buffers.create_accounts[0..events_count];
    var i: usize = 0;
    for (buffer) |*event| {
        event.* = std.mem.zeroInit(Driver.Event(.create_accounts), .{
            .id = model.accounts_id_next + i,
            .ledger = 1,
            .code = random.intRangeAtMost(u16, 1, 100),
        });
        i += 1;
    }

    return .{ .create_accounts = buffer[0..events_count] };
}

fn random_create_transfers(random: std.Random, model: *const Model) !Command {
    const events_count = random.intRangeAtMost(usize, 1, Driver.events_count_max);
    assert(events_count <= events_count_max);

    var buffer = command_buffers.create_transfers[0..events_count];
    var i: usize = 0;
    for (buffer) |*event| {
        const debit_account_id = random.intRangeLessThan(u128, 1, model.accounts_id_next);
        var credit_account_id: u128 = 0;
        while (credit_account_id == 0 or credit_account_id == debit_account_id) {
            credit_account_id = random.intRangeLessThan(u128, 1, model.accounts_id_next);
        }

        // TODO: flags

        event.* = std.mem.zeroInit(Driver.Event(.create_transfers), .{
            .id = random.intRangeAtMost(u128, 1, std.math.maxInt(u128)),
            .ledger = 1,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = random.uintAtMost(u128, std.math.maxInt(u128)),
            .code = random.intRangeAtMost(u16, 1, 100),
        });
        i += 1;
    }

    return .{ .create_transfers = buffer[0..events_count] };
}

fn start_driver(allocator: std.mem.Allocator, args: CLIArgs) !*Driver {
    var argv = std.ArrayList([]const u8).init(allocator);
    defer argv.deinit();

    assert(std.mem.indexOf(u8, args.@"driver-command", "\"") == null);
    var cmd_parts = std.mem.split(u8, args.@"driver-command", " ");

    while (cmd_parts.next()) |part| {
        try argv.append(part);
    }

    var cluster_id_argument: [32]u8 = undefined;
    const cluster_id = try std.fmt.bufPrint(cluster_id_argument[0..], "{d}", .{args.@"cluster-id"});

    try argv.append(cluster_id);
    try argv.append(args.addresses);

    return try Driver.spawn(allocator, argv.items);
}

/// Converts a union type, where each field is of a slice type, into a struct of arrays of the
/// corresponding type, with the maximum count of driver events as its len. These buffers are used
/// to hold commands and results in the workload loop.
fn FixedSizeBuffers(Union: type) type {
    const union_fields = @typeInfo(Union).Union.fields;
    var struct_fields: [union_fields.len]std.builtin.Type.StructField = undefined;

    var i = 0;
    for (union_fields) |union_field| {
        const info = @typeInfo(union_field.type);
        const field_type = [events_count_max]info.Pointer.child;
        struct_fields[i] = .{
            .name = union_field.name,
            .type = field_type,
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(field_type),
        };
        i += 1;
    }

    return @Type(.{ .Struct = .{
        .is_tuple = false,
        .fields = &struct_fields,
        .layout = .auto,
        .decls = &.{},
    } });
}
