const std = @import("std");
const Driver = @import("driver.zig");

const assert = std.debug.assert;

const log = std.log.scoped(.zig_driver);

pub const CLIArgs = struct {
    positional: struct { addresses: []const u8 },
};

pub fn main(allocator: std.mem.Allocator, args: CLIArgs) !void {
    _ = allocator;

    log.info("addresses: {s}", .{args.positional.addresses});

    const stdin = std.io.getStdIn().reader().any();
    while (true) {
        const request = receive(stdin) catch |err| {
            switch (err) {
                error.EndOfStream => break,
                else => return err,
            }
        };
        log.info("received: {any}", .{request});
    }
}

fn receive(stdin: std.io.AnyReader) !Driver.Request {
    const op = try stdin.readEnum(Driver.Operation, .little);
    const count = try stdin.readInt(u32, .little);

    return switch (op) {
        .create_accounts => try receive_events(stdin, .create_accounts, count),
        .create_transfers => try receive_events(stdin, .create_transfers, count),
        .query_accounts => try receive_events(stdin, .query_accounts, count),
        else => {
            log.err("not supported: {s}", .{@tagName(op)});
            unreachable;
        },
    };
}

fn receive_events(
    stdin: std.io.AnyReader,
    comptime op: anytype,
    count: u32,
) !Driver.Request {
    assert(count <= Driver.events_count_max);

    var buffer: [@sizeOf(Driver.Event(op)) * Driver.events_count_max]u8 = undefined;

    const length = @sizeOf(Driver.Event(op)) * count;
    assert(try stdin.readAtLeast(buffer[0..length], length) == length);

    const events: []const Driver.Event(op) = @alignCast(
        std.mem.bytesAsSlice(Driver.Event(op), buffer[0..length]),
    );

    return @unionInit(Driver.Request, @tagName(op), events);
}
