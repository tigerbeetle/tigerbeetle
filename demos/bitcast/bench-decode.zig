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
    // Read transfers from a file:
    const file = try std.fs.cwd().openFile("transfers", std.fs.File.OpenFlags{});
    defer file.close();
    var bytes: [2097152]u8 = undefined;
    var bytes_read = try file.read(bytes[0..]);
    assert(bytes_read == bytes.len);

    var start = std.time.nanoTimestamp();
    var sum: u64 = 0;
    var offset: u64 = 0;
    while (offset < bytes.len) {
        // Get a comptime length slice at a runtime offset:
        // Tip: https://github.com/ziglang/zig/issues/863#issuecomment-556711748
        const array: [128]u8 = bytes[offset..][0..128].*;

        // Deserialize without any overhead:
        const transfer = @bitCast(Transfer, array);

        // Sum transfer amounts to keep compiler from optimizing everything away:
        sum += transfer.amount;
        offset += 128;
    }
    var ns = std.time.nanoTimestamp() - start;
    std.debug.print("zig: sum of transfer amounts={} ns={}\n", .{ sum, ns });
}
