const std = @import("std");

const fuzz = @import("../testing/fuzz.zig");
const segmented_array = @import("segmented_array.zig");

pub fn main() !void {
    const allocator = fuzz.allocator;
    const fuzz_args = try fuzz.parse_fuzz_args(allocator);
    try segmented_array.run_tests(allocator, fuzz_args.seed, .{ .verify = true });
}
