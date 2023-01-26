const std = @import("std");

const fuzz = @import("../testing/fuzz.zig");
const segmented_array = @import("segmented_array.zig");

pub fn main() !void {
    const fuzz_args = try fuzz.parse_fuzz_args(std.testing.allocator);
    try segmented_array.run_tests(fuzz_args.seed, .{ .verify = true });
}
