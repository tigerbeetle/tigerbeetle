const std = @import("std");

const Shell = @import("../shell.zig");

pub const CliArgs = struct {};

/// The CFO will run the simulator this percent of the time, and alternate between all the other
/// fuzzers the remaining time.
const simulator_chance = 80;

const POLLIN = 1;
const POLLHUP = 0x010;

const FuzzOutput = struct {
    fuzzer: []const u8,
    seed: []const u8,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    _ = cli_args;

    const num_cores = 3;

    var prng = std.rand.DefaultPrng.init(std.crypto.random.int(u64));

    var children = try gpa.alloc(std.ChildProcess, num_cores);
    defer gpa.free(children);

    for (children) |*child| {
        const choice = prng.random().uintLessThan(u64, 100);
        _ = choice;
        //choice < simulator_chance
        child.* = if (true)
            try shell.spawn("zig/zig build simulator_run -Drelease", .{})
        else
            try shell.spawn("zig/zig build fuzz  -Drelease -- random", .{});
    }

    var fds = try gpa.alloc(std.os.pollfd, num_cores);
    defer gpa.free(fds);

    for (fds, children) |*fd, *child| {
        fd.* = .{
            .revents = 0,
            .events = 1,
            .fd = child.stdout.?.handle,
        };
    }

    var output_buffers = try gpa.alloc([512 * 1024]u8, num_cores);
    defer gpa.free(output_buffers);

    var output_positions = try gpa.alloc(usize, num_cores);
    defer gpa.free(output_positions);
    @memset(output_positions, 0);

    while (true) {
        const r = try std.os.poll(fds, 0);
        std.debug.assert(r >= 0);
        var done: usize = 0;

        for (fds, children, output_buffers, output_positions) |
            *fd,
            *child,
            *output_buffer,
            *output_position,
        | {
            if (fd.revents & POLLIN > 0) {
                const read = try child.stdout.?.read(output_buffer[output_position.*..]);

                // We should always read more than 0, otherwise either poll is lying to us or
                // we've run out of buffer space.
                std.debug.assert(read > 0);

                output_position.* += read;
            } else if (fd.revents & POLLHUP > 0) {
                done += 1;
            }
        }

        if (done == fds.len) {
            break;
        }
    }

    for (children) |*child| {
        _ = try child.wait();
    }

    for (output_buffers, output_positions) |*output_buffer, output_position| {
        const fuzz = try parse_output(output_buffer[0..output_position]);
        std.log.info("Fuzzer: {s} Seed: {s}", .{ fuzz.fuzzer, fuzz.seed });
    }
}

/// buffer still owns the memory.
fn parse_output(buffer: []u8) !FuzzOutput {
    const fuzzer = "Fuzzer = ";
    const fuzzer_start = (std.mem.indexOfPos(u8, buffer, 0, fuzzer) orelse
        return error.InvalidFuzzOutput) + fuzzer.len;
    const fuzzer_end = std.mem.indexOfPos(u8, buffer, fuzzer_start, "\n") orelse
        return error.InvalidFuzzOutput;

    const fuzz_seed = "Fuzz seed = ";
    const fuzz_seed_start = (std.mem.indexOfPos(u8, buffer, 0, fuzz_seed) orelse
        return error.InvalidFuzzOutput) + fuzz_seed.len;
    const fuzz_seed_end = std.mem.indexOfPos(u8, buffer, fuzz_seed_start, "\n") orelse
        return error.InvalidFuzzOutput;

    return .{
        .fuzzer = buffer[fuzzer_start..fuzzer_end],
        .seed = buffer[fuzz_seed_start..fuzz_seed_end],
    };
}
