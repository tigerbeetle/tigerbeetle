const std = @import("std");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const runs: usize = 5;
    const insertions: usize = 1000000;
    const transfer: [128]u8 = [_]u8{ 0 } ** 128;

    var transfers = std.AutoHashMap(u128, @TypeOf(transfer)).init(allocator);
    try transfers.ensureCapacity(insertions * runs);

    var id: u128 = 0;
    var run: usize = 0;
    while (run < runs) : (run += 1) {
        var start = std.time.milliTimestamp();
        var count: usize = 0;
        while (count < insertions) : (count += 1) {
            transfers.putAssumeCapacity(id, transfer);
            id += 1;
        }
        std.debug.print(
            "{} hash table insertions in {}ms\n",
            .{ insertions, std.time.milliTimestamp() - start }
        );
    }
}
