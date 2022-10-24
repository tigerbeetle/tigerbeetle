const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

/// keys_count must be one less than a power of two. This allows us to align the layout
/// such that great great grandchildren of a node are not unnecessarily split across cache lines.
pub fn eytzinger(comptime keys_count: u32, comptime values_max: u32) type {
    // This is not strictly necessary, but having less than 8 keys in the
    // Eytzinger layout would make having the layout at all somewhat pointless.
    assert(keys_count >= 3);
    assert(math.isPowerOfTwo(keys_count + 1));
    assert(values_max >= keys_count);

    return struct {
        const tree: [keys_count]u32 = blk: {
            @setEvalBranchQuota((keys_count + 1) * 4 * math.log2(keys_count));
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

            var nodes: [keys_count]u32 = undefined;
            var node: u32 = 0;
            while (node < nodes.len) : (node += 1) {
                // Left and right inclusive bounds for the children of this node,
                // as if we were doing a binary search.
                const l = if (left_ancestor(node)) |l| nodes[l] + 1 else 0;
                const r = if (right_ancestor(node)) |r| nodes[r] - 1 else values_max - 1;

                // The binary search index into source for this node in the Eytzinger layout.
                // This is (r + l) / 2 ... but without overflow bugs.
                nodes[node] = l + (r - l) / 2;
            }

            break :blk nodes;
        };

        fn left_ancestor(node: u32) ?u32 {
            var n = node;
            while (!is_right_child(n)) {
                n = parent(n) orelse return null;
            }
            return parent(n).?;
        }

        fn right_ancestor(node: u32) ?u32 {
            var n = node;
            while (!is_left_child(n)) {
                n = parent(n) orelse return null;
            }
            return parent(n).?;
        }

        fn parent(node: u32) ?u32 {
            if (node == 0) return null;
            return (node - 1) / 2;
        }

        fn is_right_child(node: u32) bool {
            return node != 0 and node % 2 == 0;
        }

        fn is_left_child(node: u32) bool {
            return node != 0 and node % 2 != 0;
        }

        /// Writes the Eytzinger layout to the passed layout buffer.
        /// The values slice must be sorted by key in ascending order.
        pub fn layout_from_keys_or_values(
            comptime Key: type,
            comptime Value: type,
            comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
            /// This sentinel must compare greater than all actual keys.
            comptime sentinel_key: Key,
            values: []const Value,
            layout: *[keys_count + 1]Key,
        ) void {
            comptime assert(tree.len + 1 == layout.len);
            assert(values.len > 0);
            assert(values.len <= values_max);

            // We leave the first slot in layout empty for purposes of alignment.
            // If we did not do this, the level in the tree with 16 great great
            // grand childern would be split across cache lines with one child
            // in the first cache line and the other 15 in the second.
            mem.set(u8, mem.asBytes(&layout[0]), 0);
            // 0 8 4 12 2 6 10 14 1 3 5 7 9 11 13 15
            // ^
            // padding element

            for (tree) |values_index, i| {
                if (values_index < values.len) {
                    layout[i + 1] = key_from_value(&values[values_index]);
                } else {
                    layout[i + 1] = sentinel_key;
                }
            }
        }

        /// Returns a smaller slice into values where the target key may be found.
        /// If the target key is present in values, the returned slice is guaranteed to contain it.
        /// May return a slice of length one if an exact result is found.
        /// May return a slice of length zero if the key is definitely not in values.
        /// Otherwise, the caller will likely want to perform a binary search on the result.
        /// TODO examine the generated machine code for this function
        pub fn search_values(
            comptime Key: type,
            comptime Value: type,
            comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
            layout: *const [keys_count + 1]Key,
            values: []const Value,
            key: Key,
        ) []const Value {
            assert(values.len > 0);
            assert(values.len <= values_max);

            const keys = layout[1..];

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

            // We want to exclude the bounding keys to avoid re-checking them, except in the case
            // of an exact match. This condition checks for an inexact match.
            const exclusion = @boolToInt(lower_bound == null or upper_bound == null or
                lower_bound.? != upper_bound.?);

            // The exclusion alone may result in an upper bound one less than the lower bound.
            // However, we add one to the upper bound to make it exclusive for slicing.
            // This case indicates that key is not present in values.
            const values_lower = if (lower_bound) |l| tree[l] + exclusion else 0;
            // This must be an exclusive upper bound but upper_bound is inclusive. Thus, add 1.
            const values_upper = if (upper_bound) |u| tree[u] + 1 - exclusion else values.len;

            return values[values_lower..math.min(values.len, values_upper)];
        }

        /// Returns an upper bound index into the corresponding values. The returned index is
        /// less than values_count, or null to indicate that all keys are less than the target key.
        /// TODO examine the generated machine code for this function
        pub fn search_keys(
            comptime Key: type,
            comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
            layout: *const [keys_count + 1]Key,
            values_count: u32,
            key: Key,
        ) ?u32 {
            // See search_values() for the explanation and full implementation of the algorithm.
            // This code is duplicated here to avoid unnecessary computation when only searching
            // for an upper bound and to keep search_values() as readable as possible. Using helper
            // functions would fragment the logic.
            const keys = layout[1..];
            var i: u32 = 0;
            while (i < keys.len) {
                // TODO use @prefetch when available: https://github.com/ziglang/zig/issues/3600
                i = if (compare_keys(key, keys[i]) == .gt) 2 * i + 2 else 2 * i + 1;
            }
            const upper = @as(u64, i + 1) >> ffs(~(i + 1));

            const out_of_bounds = upper == 0 or tree[upper - 1] >= values_count;
            return if (out_of_bounds) null else tree[upper - 1];
        }

        /// Returns one plus the index of the least significant 1-bit of x.
        /// Asserts that x is not 0.
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
            //       return if (num == 0) 0 else @ctz(num) + 1;
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
                // Max i after the while loop in search_values() terminates.
                const max_i = 2 * (tree.len - 1) + 2;
                assert(max_i + 1 < math.maxInt(u32));
                assert(~(max_i + 1) != 0);
            }
            assert(x != 0);

            // Since we assert that x is not 0, we know that @ctz(u32) returns
            // a value in the range 0 to 31 inclusive. This means that the return
            // value of @ctz(u32) fits in a u5, but since we add 1 the return type of
            // our function must be a u6.
            comptime assert(31 + 1 <= math.maxInt(u6));
            return @ctz(x) + 1;
        }
    };
}

const test_eytzinger = struct {
    const log = false;

    const Value = extern struct {
        key: u32,

        inline fn to_key(value: *const Value) u32 {
            return value.key;
        }
    };

    inline fn compare_keys(a: u32, b: u32) math.Order {
        return math.order(a, b);
    }

    const sentinel_key = math.maxInt(u32);

    pub const Bounds = struct {
        lower: ?u32,
        upper: ?u32,
    };

    fn layout_from_keys_or_values(
        comptime keys_count: usize,
        comptime values_max: usize,
        expect: []const u32,
    ) !void {
        const e = eytzinger(keys_count, values_max);

        var values: [values_max]Value = undefined;
        for (values) |*v, i| v.* = .{ .key = @intCast(u32, i) };

        var layout: [keys_count + 1]u32 = undefined;

        e.layout_from_keys_or_values(u32, Value, Value.to_key, sentinel_key, &values, &layout);

        try std.testing.expectEqualSlices(u32, expect, &layout);
    }

    fn search(comptime keys_count: usize, comptime values_max: usize, sentinels_max: usize) !void {
        assert(sentinels_max < values_max);
        const e = eytzinger(keys_count, values_max);

        var values_full: [values_max]Value = undefined;
        // This 3 * i + 7 ensures that keys don't line up perfectly with indexes
        // which could potentially catch bugs.
        for (values_full) |*v, i| v.* = .{ .key = @intCast(u32, 3 * i + 7) };

        var layout: [keys_count + 1]u32 = undefined;

        var sentinels: usize = 0;
        while (sentinels < sentinels_max) : (sentinels += 1) {
            const values = values_full[0 .. values_full.len - sentinels];
            e.layout_from_keys_or_values(u32, Value, Value.to_key, sentinel_key, values, &layout);

            const keys = layout[1..];

            if (log) {
                std.debug.print("keys count: {}, values max: {}, sentinels: {}\n", .{
                    keys_count,
                    values_max,
                    sentinels,
                });
                std.debug.print("values: {any}\n", .{values});
                std.debug.print("eytzinger layout: {any}\n", .{e.tree});
                std.debug.print("keys: {any}\n", .{@as([]u32, keys)});
            }

            // This is a regression test for our test code. We added this after we originally failed
            // to test exactly the case this is checking for.
            var at_least_one_target_key_not_in_values = false;

            var target_key: u32 = 0;
            while (target_key < values[values.len - 1].to_key() + 13) : (target_key += 1) {
                if (log) std.debug.print("target key: {}\n", .{target_key});
                var expect_keys: Bounds = .{
                    .lower = null,
                    .upper = null,
                };
                for (keys) |key, index| {
                    const i = @intCast(u32, index);
                    if (key == sentinel_key) continue;
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

                const expect_upper_bound = if (expect_keys.upper) |u| e.tree[u] else null;
                var actual_upper_bound = e.search_keys(
                    u32,
                    compare_keys,
                    &layout,
                    @intCast(u32, values.len),
                    target_key,
                );
                try std.testing.expectEqual(expect_upper_bound, actual_upper_bound);

                var expect_values: Bounds = .{
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

                const actual_slice = e.search_values(
                    u32,
                    Value,
                    compare_keys,
                    &layout,
                    values,
                    target_key,
                );

                try std.testing.expectEqual(
                    @as([*]const Value, expect_slice.ptr),
                    actual_slice.ptr,
                );
                try std.testing.expectEqual(expect_slice.len, actual_slice.len);
            }

            assert(at_least_one_target_key_not_in_values);
        }
    }
};

test "eytzinger: equal key and value count" {
    // zig fmt: off
    try test_eytzinger.layout_from_keys_or_values(3, 3, &[_]u32{ 0,
          1,
        0, 2,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 7, &[_]u32{ 0,
             3,
          1,    5,
        0, 2, 4, 6,
    });
    try test_eytzinger.layout_from_keys_or_values(15, 15, &[_]u32{ 0,
                   7,
             3,          11,
          1,    5,    9,     13,
        0, 2, 4, 6, 8, 10, 12, 14,
    });
    // zig fmt: on
}

test "eytzinger: power of two value count > key count" {
    // zig fmt: off
    try test_eytzinger.layout_from_keys_or_values(3, 8, &[_]u32{ 0,
          3,
        1, 5,
    });
    try test_eytzinger.layout_from_keys_or_values(3, 16, &[_]u32{ 0,
          7,
        3, 11,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 16, &[_]u32{ 0,
              7,
          3,     11,
        1,  5, 9,  13,
    });
    try test_eytzinger.layout_from_keys_or_values(15, 32, &[_]u32{ 0,
                   15,
             7,             23,
          3,   11,      19,     27,
        1, 5, 9, 13, 17,  21, 25, 29,
    });
    // zig fmt: on
}

test "eytzinger: non power of two value count" {
    // zig fmt: off
    try test_eytzinger.layout_from_keys_or_values(7, 13, &[_]u32{ 0,
             6,
          2,    9,
        0, 4, 7, 11,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 19, &[_]u32{ 0,
             9,
          4,    14,
        1, 6, 11, 16,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 20, &[_]u32{ 0,
             9,
          4,    14,
        1, 6, 11, 17,
    });
    try test_eytzinger.layout_from_keys_or_values(15, 21, &[_]u32{ 0,
                  10,
             4,           15,
          1,    7,    12,     18,
        0, 2, 5, 8, 11, 13, 16, 19,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 7919, &[_]u32{ 0,
                   3959,
           1979,           5939,
        989,   2969,   4949,   6929,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 7920, &[_]u32{ 0,
                   3959,
           1979,           5939,
        989,   2969,   4949,   6929,
    });
    try test_eytzinger.layout_from_keys_or_values(7, 7921, &[_]u32{ 0,
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
    const actual = eytzinger(3, math.maxInt(u32)).tree;
    const expect = [_]u32{ 2_147_483_647, 1_073_741_823, 3_221_225_471 };
    try std.testing.expectEqualSlices(u32, &expect, &actual);
}

test "eytzinger: search" {
    comptime var power_of_2: u32 = 4;
    inline while (power_of_2 <= 32) : (power_of_2 <<= 1) {
        comptime var values_count = power_of_2;
        inline while (values_count <= 64) : (values_count += 1) {
            const keys_count = power_of_2 - 1;
            try test_eytzinger.search(keys_count, values_count, values_count - 1);
        }
    }
}
