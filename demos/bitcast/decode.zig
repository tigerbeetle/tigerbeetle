const std = @import("std");
const assert = std.debug.assert;

const Transfer = packed struct {
    id: u128,
    debit_id: u128,
    credit_id: u128,
    custom_1: u128,
    custom_2: u128,
    custom_3: u128,
    flags: u64,
    amount: u64,
    timeout: u64,
    timestamp: u64,
};

pub fn main() !void {
    // Read a serialized transfer from a file:
    const file = try std.fs.cwd().openFile("transfer", std.fs.File.OpenFlags{});
    defer file.close();
    var bytes: [@sizeOf(Transfer)]u8 = undefined;
    var bytes_read = try file.read(bytes[0..]);
    assert(bytes_read == bytes.len);

    // "Deserialize" this transfer from off the wire (without doing any parsing):
    const transfer = @bitCast(Transfer, bytes);

    // Log to the console... formatting the ID as lower case hexadecimal:
    std.debug.print("id={x} flags={} amount={} timeout={} timestamp={}\n", .{
        transfer.id,
        transfer.flags,
        transfer.amount,
        transfer.timeout,
        transfer.timestamp,
    });
}
