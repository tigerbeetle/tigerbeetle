const std = @import("std");

const fuzz = @import("../testing/fuzz.zig");
const segmented_array = @import("segmented_array.zig");

pub fn main(fuzz_args: fuzz.FuzzArgs) !void {
    const allocator = fuzz.allocator;
    try segmented_array.run_fuzz(allocator, fuzz_args.seed, .{ .verify = true });
}
