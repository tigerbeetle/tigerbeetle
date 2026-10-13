const std = @import("std");
const fuzz = @import("stdx").fuzz;
const segmented_array = @import("segmented_array.zig");

pub fn main(gpa: std.mem.Allocator, fuzz_args: fuzz.FuzzArgs) !void {
    try segmented_array.run_fuzz(gpa, fuzz_args.seed, .{ .verify = true });
}
