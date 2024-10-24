const std = @import("std");
const constants = @import("../../constants.zig");
const StateMachineType = @import("../../state_machine.zig").StateMachineType;
const StateMachine = StateMachineType(void, constants.state_machine_config);

const c = @cImport({
    _ = @import("../../tb_client_exports.zig"); // Needed for the @export()'ed C ffi functions.
    @cInclude("tb_client.h");
});

const assert = std.debug.assert;

const log = std.log.scoped(.zig_driver);
const events_count_max = 8190;

pub const CLIArgs = struct {
    positional: struct {
        @"cluster-id": u128,
        addresses: []const u8,
    },
};

pub fn main(_: std.mem.Allocator, args: CLIArgs) !void {
    log.info("addresses: {s}", .{args.positional.addresses});

    const cluster_id = args.positional.@"cluster-id";
    var tb_client: c.tb_client_t = undefined;
    const result = c.tb_client_init(
        &tb_client,
        cluster_id,
        args.positional.addresses.ptr,
        @intCast(args.positional.addresses.len),
        0,
        on_complete,
    );
    if (result != c.TB_STATUS_SUCCESS) {
        return error.ClientInitError;
    }
    defer c.tb_client_deinit(tb_client);

    const stdin = std.io.getStdIn().reader().any();
    const stdout = std.io.getStdOut().writer().any();

    while (true) {
        const operation, const events_buffer = receive(stdin) catch |err| {
            switch (err) {
                error.EndOfStream => break,
                else => return err,
            }
        };

        var packet: c.tb_packet_t = undefined;
        packet.operation = @intFromEnum(operation);
        packet.user_data = @constCast(@ptrCast(&stdout));
        packet.data = @constCast(events_buffer.ptr);
        packet.data_size = @intCast(events_buffer.len);
        packet.next = null;
        packet.status = c.TB_PACKET_OK;

        c.tb_client_submit(tb_client, &packet);
    }
}

pub fn on_complete(
    tb_context: usize,
    tb_client: c.tb_client_t,
    tb_packet: [*c]c.tb_packet_t,
    result_ptr: [*c]const u8,
    result_len: u32,
) callconv(.C) void {
    _ = tb_context;
    _ = tb_client;
    const writer: *std.io.AnyWriter = @ptrCast(@alignCast(tb_packet.*.user_data.?));
    const operation: StateMachine.Operation = @enumFromInt(tb_packet.*.operation);

    // We might want to move this off the callback thread eventually, at least of we want
    // multithreading within this driver. But for now it's OK.
    if (result_ptr) |result| {
        write_results(writer, operation, result[0..result_len]) catch |err| {
            switch (err) {
                error.BrokenPipe => {},
                else => log.err("error when writing back results: {any}", .{err}),
            }
        };
    }
}

fn write_results(
    writer: *std.io.AnyWriter,
    operation: StateMachine.Operation,
    result: []const u8,
) !void {
    switch (operation) {
        inline else => |comptime_operation| {
            const size = @sizeOf(StateMachine.Result(comptime_operation));
            if (size > 0) {
                const count = @divExact(result.len, size);
                try writer.writeInt(u32, @intCast(count), .little);
                try writer.writeAll(result);
            } else {
                log.err(
                    "unexpected size {d} for op: {s}",
                    .{ size, @tagName(comptime_operation) },
                );
            }
        },
    }
}

fn receive(reader: std.io.AnyReader) !struct { StateMachine.Operation, []const u8 } {
    const operation = try reader.readEnum(StateMachine.Operation, .little);
    const count = try reader.readInt(u32, .little);

    return switch (operation) {
        inline else => |comptime_operation| {
            assert(count <= events_count_max);
            const buffer_size_max =
                @sizeOf(StateMachine.Event(comptime_operation)) * events_count_max;

            var buffer: [buffer_size_max]u8 = undefined;

            const buffer_size = @sizeOf(StateMachine.Event(comptime_operation)) * count;
            assert(try reader.readAtLeast(buffer[0..buffer_size], buffer_size) == buffer_size);

            return .{ comptime_operation, buffer[0..buffer_size] };
        },
    };
}
