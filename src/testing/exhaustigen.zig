//! An utility for exhaustive generation of arbitrary data
//! <https://matklad.github.io/2021/11/07/generate-all-the-things.html>
test "generate all permutations" {
    var g: Gen = .{};
    var permutation_count: u32 = 0;

    // This loop imperatively generates all permutations of "abcd":
    while (!g.done()) {
        var pool_buffer: [4]u8 = "abcd".*;
        var permutation: [4]u8 = undefined;

        for (0..permutation.len) |i| {
            const pool = pool_buffer[0 .. pool_buffer.len - i];

            // Pick a "random" number from the pool, append it to the permutation,
            // and swap-remove it from the pool.
            const pool_index = g.index(pool);
            permutation[i] = pool[pool_index];
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

started: bool = false,
v: [32]struct { value: u32, bound: u32 } = undefined,
p: usize = 0,
p_max: usize = 0,

pub fn done(g: *@This()) bool {
    if (!g.started) {
        g.started = true;
        return false;
    }
    var i = g.p_max;
    while (i > 0) {
        i -= 1;
        if (g.v[i].value < g.v[i].bound) {
            g.v[i].value += 1;
            g.p_max = i + 1;
            g.p = 0;
            return false;
        }
    }
    return true;
}

fn gen(g: *Gen, bound: u32) u32 {
    assert(g.p < g.v.len);
    if (g.p == g.p_max) {
        g.v[g.p] = .{ .value = 0, .bound = 0 };
        g.p_max += 1;
    }
    g.p += 1;
    g.v[g.p - 1].bound = bound;
    return g.v[g.p - 1].value;
}

pub fn int_inclusive(g: *Gen, Int: type, bound: Int) Int {
    return @intCast(g.gen(@intCast(bound)));
}

pub fn range_inclusive(g: *Gen, Int: type, min: Int, max: Int) Int {
    comptime assert(@typeInfo(Int).int.signedness == .unsigned);
    assert(min <= max);
    return min + g.int_inclusive(Int, max - min);
}

pub fn shuffle(g: *Gen, T: type, slice: []T) void {
    for (0..slice.len) |i| {
        const j = g.int_inclusive(u64, i);
        std.mem.swap(T, &slice[i], &slice[j]);
    }
}

test shuffle {
    var n_factorial: u32 = 1;
    inline for (0..5) |n| {
        var g: Gen = .{};
        var count: u32 = 0;
        while (!g.done()) {
            var array: [n]u8 = @splat(0);
            g.shuffle(u8, &array);
            count += 1;
        }
        assert(count == n_factorial);
        n_factorial *= (n + 1);
    }
}

pub fn index(g: *Gen, slice: anytype) usize {
    assert(slice.len > 0);
    return g.int_inclusive(usize, slice.len - 1);
}

pub fn enum_value(g: *Gen, Enum: type) Enum {
    const values = std.enums.values(Enum);
    return values[g.index(values)];
}
