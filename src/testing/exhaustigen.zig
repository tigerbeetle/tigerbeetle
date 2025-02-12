//! An utility for exhaustive generation of arbitrary data
//! <https://matklad.github.io/2021/11/07/generate-all-the-things.html>
test "generate all permutations" {
    var g: Gen = .{};
    var permutation_count: u32 = 0;

    // This loop imperatively generates all permutations of "abcd":
    while (!g.done()) {
        var pool_buffer: [4]u8 = "abcd".*;
        var permutation: [4]u8 = undefined;

        for (0..permutation.len) |index| {
            const pool = pool_buffer[0 .. pool_buffer.len - index];

            // Pick a "random" number from the pool, append it to the permutation,
            // and swap-remove it from the pool.
            const pool_index = g.int_inclusive(usize, pool.len - 1);
            permutation[index] = pool[pool_index];
            pool[pool_index] = pool[pool.len - 1];
        }
        // Here, `permutation` enumerates all permutations of `abcd`:
        // std.debug.print("permutation: {s}\n", .{permutation});
        permutation_count += 1;
    }

    // Verify that we indeed generated n! permutations.
    var factorial: usize = 1;
    for (1..5) |n| factorial *= n;

    assert(permutation_count == factorial);
}

const std = @import("std");
const assert = std.debug.assert;

// The implementation is tricky, refer to the post for details.
//
// On each iteration of `while (!g.done())` loop, Gen generates a sequence of numbers.
// Internally, it remembers this sequence  together with bounds the user requested:
//
// value:  3 1 4 4
// bound:  5 4 4 4
//
// To advance to the next iteration, Gen finds the smallest sequence of values which is larger than
// the current one, but still satisfies all the bounds. "Smallest" means that Gen tries to increment
// the rightmost number.
//
// In the above example, the last two "4"s already match the bound, so we can't increment them.
// However, we can increment the second number, "1", to get 3 2 4 4. This isn’t the smallest
// sequence though, 3 2 0 0 is be smaller. So, after incrementing the rightmost number possible,
// we zero the rest.
const Gen = @This();

start: bool = true,
index: usize = 0,
count: usize = 0,
value: [32]u32 = undefined,
bound: [32]u32 = undefined,

pub fn done(g: *@This()) bool {
    if (g.start) {
        g.start = false;
        return false;
    }
    var i = g.count;
    while (i > 0) {
        i -= 1;
        if (g.value[i] < g.bound[i]) {
            g.value[i] += 1;
            g.count = i + 1;
            g.index = 0;
            return false;
        }
    }
    return true;
}

fn gen(g: *Gen, bound: u32) u32 {
    assert(g.index < g.value.len);
    if (g.index == g.count) {
        g.value[g.count] = 0;
        g.bound[g.count] = 0;
        g.count += 1;
    }
    defer g.index += 1;
    g.bound[g.index] = bound;
    return g.value[g.index];
}

pub fn int_inclusive(g: *Gen, Int: type, bound: Int) Int {
    return @intCast(g.gen(@intCast(bound)));
}

pub fn enum_value(g: *Gen, Enum: type) Enum {
    const values = std.enums.values(Enum);
    return values[g.int_inclusive(usize, values.len - 1)];
}
