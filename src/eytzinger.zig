const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

pub fn eytzinger(comptime keys_count: u32, comptime values_count: u32) type {
    // This is not strictly necessary, but having less than 8 keys in the
    // Eytzinger layout would make having the layout at all somewhat pointless.
    assert(keys_count >= 4);
    assert(math.isPowerOfTwo(keys_count));
    assert(values_count >= keys_count);

    return struct {
        const eytzinger_tree: [keys_count - 1]u32 = blk: {
            var tree: [keys_count - 1]u32 = undefined;

            // n = 7:
            //   sorted values: 0 1 2 3 4 5 6
            //
            //   binary search tree:
            //          3
            //      1       5
            //    0   2   4   6
            //
            //   Eytzinger layout:
            //   3 1 5 0 2 4 6
            //
            // Our Eytzinger layout construction exactly matches the indexes
            // used during a binary search, unlike common recursive implementations.
            // This gives us more consistent performance and it's easy to verify.
            //
            // n = 21, X = padding:
            //   sorted values: 0 1 2 3 ... 20
            //
            //   binary search tree:
            //                  10
            //          4                    15
            //      1       7         12            18
            //    0   2   5   8    11    13     16     19
            //   X X X 3 X 6 X 9  X  X  X  14  X  17  X  20
            //
            //   Eytzinger layout:
            //    0 1  2 3 4  5 6 7 8 9 10 11 12 13 14
            //   10 4 15 1 7 12 1 0 2 5  8 11 13 16 19
            //
            // Note, we only support building a full Eytzinger tree of 2^k-1 keys.
            // The last layer in the binary search tree for n=21 is not included
            // in the Eytzinger layout. If we were to include it, our perfectly
            // balanced construction would require sentinel values for the padding
            // due to negative lookups.

            var node: u32 = 0;
            while (node < tree.len) : (node += 1) {
                // Left and right inclusive bounds for the children of this node,
                // as if we were doing a binary search.
                const l = if (left_ancestor(&tree, node)) |l| tree[l] + 1 else 0;
                const r = if (right_ancestor(&tree, node)) |r| tree[r] - 1 else values_count - 1;

                // The binary search index into source for this node in the Eytzinger layout.
                // This is (r + l) / 2 ... but without overflow bugs.
                tree[node] = l + (r - l) / 2;
            }

            break :blk tree;
        };

        fn left_ancestor(tree: []const u32, node: u32) ?u32 {
            var n = node;
            while (!is_right_child(n)) {
                n = parent(tree, n) orelse return null;
            }
            return parent(tree, n).?;
        }

        fn right_ancestor(tree: []const u32, node: u32) ?u32 {
            var n = node;
            while (!is_left_child(n)) {
                n = parent(tree, n) orelse return null;
            }
            return parent(tree, n).?;
        }

        fn parent(tree: []const u32, node: u32) ?u32 {
            if (node == 0) return null;
            return (node - 1) / 2;
        }

        fn is_right_child(node: u32) bool {
            return node != 0 and node % 2 == 0;
        }

        fn is_left_child(node: u32) bool {
            return node != 0 and node % 2 != 0;
        }

        pub fn layout(
            comptime Key: type,
            comptime Value: type,
            comptime key_from_value: fn (Value) Key,
            keys: *[keys_count]Key,
            values: *const [values_count]Value,
        ) void {
            comptime assert(eytzinger_tree.len + 1 == keys.len);

            // We leave the first slot in keys empty for purposes of alignment.
            // If we did not do this, the level in the tree with 16 great great
            // grand childern would be split across cache lines with one child
            // in the first cache line and the other 15 in the second.
            mem.set(u8, mem.asBytes(&keys[0]), 0);
            // 0 8 4 12 2 6 10 14 1 3 5 7 9 11 13 15
            // ^
            // padding element

            for (eytzinger_tree) |values_index, i| {
                keys[i + 1] = key_from_value(values[values_index]);
            }
        }

        pub const Bounds = struct {
            lower: ?u32,
            upper: ?u32,
        };

        // TODO: examine the generated machine code for this function
        inline fn search_keys(
            comptime Key: type,
            comptime compare_keys: fn (Key, Key) math.Order,
            keys: *[eytzinger_tree.len]Key,
            key: Key,
        ) Bounds {
            // "Array Layouts for Comparison-Based Search"
            //
            // Example using the n=21 tree above:
            //
            // If we search for 12.5 then we would have gone left at 13:
            //   i = 25 = 0b0...0011001
            //   i + 1  = 0b0...0011010
            //
            // Upper bound:
            //   ~(i + 1) = 0b1...1100101
            //   ffs(~(i + 1)) = index of least significant set bit + 1 = 0 + 1
            //   j = (i + 1) >> ffs(~(i + 1)) = 0b11010 >> 1 = 0b1101 = 13
            //   upper = if (j == 0) keys.len else j - 1 = 12
            //
            // Lower bound:
            //   ffs(i + 1) = index of least significant set bit + 1 = 1 + 1 = 2
            //   k = (i + 1) >> ffs(i + 1) = 0b11010 >> 2 = 0b110 = 6
            //   lower = if (k == 0) -1 else k - 1 = 5;
            //
            // Search for 3 in the tree 10 4 15:
            //   i = 2 * 1 + 1 = 3
            //   i + 1 = 0b100
            //
            // Upper bound:
            //   ~(i + 1) = 0b1...11011
            //   ffs(~(i + 1)) = 0 + 1 = 1
            //   j = (i + 1) >> ffs(~(i + 1)) = 0b100 >> 1 = 0b10 = 2
            //   upper = if (j == 0) keys.len else j - 1 = 1
            //
            // Lower bound:
            //   ffs(i + 1) = 2 + 1 = 3
            //   k = (i + 1) >> ffs(i + 1) = 0b100 >> 3 = 0
            //   lower = if (k == 0) -1 else k - 1 = -1;

            var i: u32 = 0;
            while (i < keys.len) {
                // TODO use @prefetch when available: https://github.com/ziglang/zig/issues/3600
                i = if (compare_keys(key, keys[i]) == .gt) 2 * i + 2 else 2 * i + 1;
            }

            // The upper_bound is the smallest key that is greater than or equal to the
            // target. This is due to the greater than comparison in the loop above.
            const upper = @as(u64, i + 1) >> ffs(~(i + 1));
            const upper_bound: ?u32 = if (upper == 0) null else @intCast(u32, upper - 1);

            // Because of the comparison used in the loop above, the lower bound is a < bound
            // not a <= bound. Therefore in the case of an exact match we must use the upper bound.
            const lower_bound: ?u32 = blk: {
                if (upper_bound) |u| {
                    if (compare_keys(key, keys[u]) == .eq) break :blk u;
                }
                const lower = @as(u64, i + 1) >> ffs((i + 1));
                break :blk if (lower == 0) null else @intCast(u32, lower - 1);
            };

            return .{
                .lower = lower_bound,
                .upper = upper_bound,
            };
        }

        // Returns one plus the index of the least significant 1-bit of x.
        // If x is zero, returns zero.
        inline fn ffs(x: u32) u6 {
            // clang __builtin_ffs() output:
            //   bsf     ecx, edi
            //   mov     eax, -1
            //   cmovne  eax, ecx
            //   add     eax, 1
            //   ret
            //
            // zig direct translation:
            //   export fn ffs(num: i32) i32 {
            //       return if (num == 0) 0 else @ctz(i32, num) + 1;
            //   }
            //
            // zig ffs output:
            //   tzcnt   ecx, edi
            //   mov     eax, -1
            //   cmovae  eax, ecx
            //   inc     eax
            //   ret
            //
            // tzcnt is essentially bsf but is defined to return the size
            // of the operand if the operand is 0, whereas the output of
            // bsf is undefined if the operand is 0.

            // Since x will never be 0 in practice, we can drop the cmove
            // above by not checking for 0.
            // This function is always called with argument ~(i + 1) or (i + 1).
            // Since i is unsigned, (i + 1) is always greater than 0. Therefore
            // we only need to check for the case where ~(i + 1) == 0 which happens
            // exactly when i + 1 is the maximum value for a u32.
            comptime {
                // Max i after the while loop in search_keys() terminates.
                const max_i = 2 * (eytzinger_tree.len - 1) + 2;
                assert(max_i + 1 < math.maxInt(u32));
                assert(~(max_i + 1) != 0);
            }
            assert(x != 0);

            // Since we assert that x is not 0, we know that @ctz() returns
            // a value in the range 0 to 31 inclusive. This means that the return
            // value of @ctz() fits in a u5, but since we add 1 the return type of
            // our function must be a u6.
            comptime assert(31 + 1 <= math.maxInt(u6));
            return @ctz(u32, x) + 1;
        }

        /// Returns a smaller slice into values where the target key may be found.
        /// If the target key is present in values, the returned slice is guaranteed to contain it.
        /// May return a slice of length one if an exact result is found.
        /// May return a slice of length zero if the key is definitely not in values.
        /// Otherwise, the caller will likely want to preform a binary search on the result.
        pub fn search(
            comptime Key: type,
            comptime Value: type,
            comptime key_from_value: fn (Value) Key,
            comptime compare_keys: fn (Key, Key) math.Order,
            keys: *[keys_count]Key,
            values: *const [values_count]Value,
            key: Key,
        ) []const Value {
            const bounds = search_keys(Key, compare_keys, keys[1..], key);
            // We want to exclude the bounding keys to avoid re-checking them, except in the case
            // of an exact match. This condition checks for an inexact match.
            const exclusion = @boolToInt(bounds.lower == null or bounds.upper == null or
                bounds.lower.? != bounds.upper.?);
            // The exclusion alone may result in an upper bound one less than the lower bound.
            // However, we add one to the upper bound to make it exclusive for slicing.
            // This case indicates that key is not present in values.
            const lower = if (bounds.lower) |l| eytzinger_tree[l] + exclusion else 0;
            // This must be an exclusive upper bound but bounds.upper is inclusive. Thus, add 1.
            const upper = if (bounds.upper) |u| eytzinger_tree[u] + 1 - exclusion else values.len;
            return values[lower..upper];
        }
    };
}

const test_eytzinger = struct {
    const Value = extern struct {
        key: u32,

        fn to_key(value: Value) u32 {
            return value.key;
        }
    };

    fn compare_keys(a: u32, b: u32) math.Order {
        return math.order(a, b);
    }

    fn layout(
        comptime keys_count: usize,
        comptime values_count: usize,
        expect: []const u32,
    ) !void {
        const e = eytzinger(keys_count, values_count);

        var values: [values_count]Value = undefined;
        for (values) |*v, i| v.* = .{ .key = @intCast(u32, i) };

        var keys: [keys_count]u32 = undefined;

        e.layout(u32, Value, Value.to_key, &keys, &values);

        try std.testing.expectEqualSlices(u32, expect, &keys);
    }

    fn search(comptime keys_count: usize, comptime values_count: usize) !void {
        const e = eytzinger(keys_count, values_count);

        var values: [values_count]Value = undefined;
        // This 3 * i + 7 ensures that keys don't line up perfectly with indexes
        // which could potentially catch bugs.
        for (values) |*v, i| v.* = .{ .key = @intCast(u32, 3 * i + 7) };

        var keys_aligned: [keys_count]u32 = undefined;

        e.layout(u32, Value, Value.to_key, &keys_aligned, &values);

        const keys = keys_aligned[1..];

        // This is a regression test for our test code. We added this because we originally failed
        // to test exactly the case this is checking for.
        var at_least_one_target_key_not_in_values = false;

        var target_key: u32 = 0;
        while (target_key < values[values.len - 1].to_key() + 13) : (target_key += 1) {
            var expect_keys: e.Bounds = .{
                .lower = null,
                .upper = null,
            };
            for (keys) |key, index| {
                const i = @intCast(u32, index);
                switch (compare_keys(key, target_key)) {
                    .eq => {
                        expect_keys.lower = i;
                        expect_keys.upper = i;
                        break;
                    },
                    .gt => if (expect_keys.upper == null or
                        compare_keys(key, keys[expect_keys.upper.?]) == .lt)
                    {
                        expect_keys.upper = i;
                    },
                    .lt => if (expect_keys.lower == null or
                        compare_keys(key, keys[expect_keys.lower.?]) == .gt)
                    {
                        expect_keys.lower = i;
                    },
                }
            }
            assert(!(expect_keys.lower == null and expect_keys.upper == null));

            const actual_keys = e.search_keys(u32, compare_keys, keys, target_key);
            try std.testing.expectEqual(expect_keys.lower, actual_keys.lower);
            try std.testing.expectEqual(expect_keys.upper, actual_keys.upper);

            var expect_values: e.Bounds = .{
                .lower = null,
                .upper = null,
            };
            var target_key_found = false;
            for (values) |value, i| {
                if (compare_keys(value.to_key(), target_key) == .eq) target_key_found = true;

                if (expect_keys.lower) |l| {
                    if (compare_keys(value.to_key(), keys[l]) == .eq) {
                        expect_values.lower = @intCast(u32, i);
                    }
                }
                if (expect_keys.upper) |u| {
                    if (compare_keys(value.to_key(), keys[u]) == .eq) {
                        expect_values.upper = @intCast(u32, i);
                    }
                }
            }
            if (!target_key_found) at_least_one_target_key_not_in_values = true;
            assert((expect_keys.lower == null) == (expect_values.lower == null));
            assert((expect_keys.upper == null) == (expect_values.upper == null));
            assert(!(expect_values.lower == null and expect_values.upper == null));

            if (expect_values.lower != null) {
                if (compare_keys(values[expect_values.lower.?].to_key(), target_key) == .lt) {
                    expect_values.lower.? += 1;
                } else {
                    assert(compare_keys(values[expect_values.lower.?].to_key(), target_key) == .eq);
                }
            }
            if (expect_values.upper != null) {
                var exclusion: u32 = undefined;
                if (compare_keys(values[expect_values.upper.?].to_key(), target_key) == .gt) {
                    exclusion = 1;
                } else {
                    exclusion = 0;
                    assert(compare_keys(values[expect_values.upper.?].to_key(), target_key) == .eq);
                }
                // Convert the inclusive upper bound to an exclusive upper bound for slicing.
                expect_values.upper.? += 1;
                // Now, apply the conditional exclusion. The order is important here to avoid
                // underflow if expect_values.upper is 0.
                expect_values.upper.? -= exclusion;
            }

            const expect_slice_lower = expect_values.lower orelse 0;
            const expect_slice_upper = expect_values.upper orelse values.len;
            const expect_slice = values[expect_slice_lower..expect_slice_upper];
            if (target_key_found and expect_slice.len == 1) {
                assert(compare_keys(expect_slice[0].to_key(), target_key) == .eq);
            }

            const actual_slice = e.search(
                u32,
                Value,
                Value.to_key,
                compare_keys,
                &keys_aligned,
                &values,
                target_key,
            );

            try std.testing.expectEqual(@as([*]const Value, expect_slice.ptr), actual_slice.ptr);
            try std.testing.expectEqual(expect_slice.len, actual_slice.len);
        }

        assert(at_least_one_target_key_not_in_values);
    }
};

test "eytzinger: equal key and value count" {
    // zig fmt: off
    try test_eytzinger.layout(4, 4, &[_]u32{ 0,
          1,
        0, 2,
    });
    try test_eytzinger.layout(8, 8, &[_]u32{ 0,
             3,
          1,    5,
        0, 2, 4, 6,
    });
    try test_eytzinger.layout(16, 16, &[_]u32{ 0,
                   7,
             3,          11,
          1,    5,    9,     13,
        0, 2, 4, 6, 8, 10, 12, 14,
    });
    // zig fmt: on
}

test "eytzinger: power of two value count > key count" {
    // zig fmt: off
    try test_eytzinger.layout(4, 8, &[_]u32{ 0,
          3,
        1, 5,
    });
    try test_eytzinger.layout(4, 16, &[_]u32{ 0,
          7,
        3, 11,
    });
    try test_eytzinger.layout(8, 16, &[_]u32{ 0,
              7,
          3,     11,
        1,  5, 9,  13,
    });
    try test_eytzinger.layout(16, 32, &[_]u32{ 0,
                   15,
             7,             23,
          3,   11,      19,     27,
        1, 5, 9, 13, 17,  21, 25, 29,
    });
    // zig fmt: on
}

test "eytzinger: non power of two value count" {
    // zig fmt: off
    try test_eytzinger.layout(8, 13, &[_]u32{ 0,
             6,
          2,    9,
        0, 4, 7, 11,
    });
    try test_eytzinger.layout(8, 19, &[_]u32{ 0,
             9,
          4,    14,
        1, 6, 11, 16,
    });
    try test_eytzinger.layout(8, 20, &[_]u32{ 0,
             9,
          4,    14,
        1, 6, 11, 17,
    });
    try test_eytzinger.layout(16, 21, &[_]u32{ 0,
                  10,
             4,           15,
          1,    7,    12,     18,
        0, 2, 5, 8, 11, 13, 16, 19,
    });
    try test_eytzinger.layout(8, 7919, &[_]u32{ 0,
                   3959,
           1979,           5939,
        989,   2969,   4949,   6929,
    });
    try test_eytzinger.layout(8, 7920, &[_]u32{ 0,
                   3959,
           1979,           5939,
        989,   2969,   4949,   6929,
    });
    try test_eytzinger.layout(8, 7921, &[_]u32{ 0,
                   3960,
           1979,           5940,
        989,   2969,   4950,   6930,
    });
    // zig fmt: on
}

test "eytzinger: handle classic binary search overflow" {
    // Many binary search implementations do mid = (l + r) / 2
    // However, if l + r is outside of the range of the integer type
    // used, this calculation will needlessly overflow.
    // https://en.wikipedia.org/wiki/Binary_search_algorithm#Implementation_issues

    // We don't use the utilities in test_eytzinger as they place the key/value arrays
    // on the stack, which the OS doesn't like if you have 2^32 values.
    const actual = eytzinger(4, math.maxInt(u32)).eytzinger_tree;
    const expect = [_]u32{ 2_147_483_647, 1_073_741_823, 3_221_225_471 };
    try std.testing.expectEqualSlices(u32, &expect, &actual);
}

test "eytzinger: search" {
    comptime var keys_count: u32 = 4;
    inline while (keys_count <= 32) : (keys_count <<= 1) {
        comptime var values_count = keys_count;
        inline while (values_count <= 64) : (values_count += 1) {
            try test_eytzinger.search(keys_count, values_count);
        }
    }
}
