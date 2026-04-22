const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

pub const Error = error{
    UnsupportedPlatform,
    InvalidCpu,
} || std.posix.UnexpectedError;

/// Pins the calling thread to the CPU with the given zero-based index.
///
/// Returns `error.UnsupportedPlatform` on non-Linux systems.
/// Returns `error.InvalidCpu` if `cpu_index` exceeds the platform's cpu_set_t capacity.
pub fn pin_current_thread(cpu_index: u16) Error!void {
    if (comptime builtin.os.tag != .linux) return error.UnsupportedPlatform;

    comptime assert(builtin.os.tag == .linux);

    return pin_current_thread_linux(cpu_index);
}

fn pin_current_thread_linux(cpu_index: u16) Error!void {
    comptime assert(builtin.os.tag == .linux);

    const linux = std.os.linux;
    var cpu_set = std.mem.zeroes(linux.cpu_set_t);

    const bits_per_word = @bitSizeOf(usize);
    const word_index: usize = @intCast(cpu_index / bits_per_word);
    if (word_index >= cpu_set.len) return error.InvalidCpu;

    const bit_index: usize = @intCast(cpu_index % bits_per_word);
    const bit_shift: u6 = @intCast(bit_index);
    cpu_set[word_index] = (@as(usize, 1) << bit_shift);

    try linux.sched_setaffinity(0, &cpu_set);
}
