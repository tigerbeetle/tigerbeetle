const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

// TODO Add prefeching when @prefetch is available: https://github.com/ziglang/zig/issues/3600.
//
// TODO The Zig self hosted compiler will implement inlining itself before passing the IR to llvm,
// which should eliminate the current poor codegen of key_from_value/compare_keys.

/// Doesn't preform the extra key comparison to determine if the match is exact
pub fn binary_search_values_raw(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (Value) Key,
    comptime compare_keys: fn (Key, Key) math.Order,
    values: []const Value,
    key: Key,
) usize {
    assert(values.len > 0);

    var offset: usize = 0;
    var length: usize = values.len;
    while (length > 1) {
        const half = length / 2;
        const mid = offset + half;

        // This trick seems to be what's needed to get llvm to emit branchless code for this,
        // a ternay-style if expression was generated as a jump here for whatever reason.
        const next_offsets = [_]usize{ offset, mid };
        offset = next_offsets[@boolToInt(compare_keys(key_from_value(values[mid]), key) == .lt)];

        length -= half;
    }

    return offset + @boolToInt(compare_keys(key_from_value(values[offset]), key) == .lt);
}

const BinarySearchResult = struct {
    index: usize,
    exact: bool,
};

pub inline fn binary_search_values(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (Value) Key,
    comptime compare_keys: fn (Key, Key) math.Order,
    values: []const Value,
    key: Key,
) BinarySearchResult {
    const index = binary_search_values_raw(Key, Value, key_from_value, compare_keys, values, key);
    return .{
        .index = index,
        .exact = index < values.len and compare_keys(key_from_value(values[index]), key) == .eq,
    };
}

pub inline fn binary_search_keys(
    comptime Key: type,
    comptime compare_keys: fn (Key, Key) math.Order,
    keys: []const Key,
    key: Key,
) BinarySearchResult {
    return binary_search_values(
        Key,
        Key,
        struct {
            fn key_from_key(k: Key) Key {
                return k;
            }
        }.key_from_key,
        compare_keys,
        keys,
        key,
    );
}

const test_binary_search = struct {
    const log = false;

    const gpa = std.testing.allocator;

    fn compare_keys(a: u32, b: u32) math.Order {
        return math.order(a, b);
    }

    fn exhaustive_search(keys_count: u32) !void {
        const keys = try gpa.alloc(u32, keys_count);
        defer gpa.free(keys);

        for (keys) |*key, i| key.* = @intCast(u32, 7 * i + 3);

        var target_key: u32 = 0;
        while (target_key < keys_count + 13) : (target_key += 1) {
            var expect: BinarySearchResult = .{ .index = 0, .exact = false };
            for (keys) |key, i| {
                switch (compare_keys(key, target_key)) {
                    .lt => expect.index = i + 1,
                    .eq => {
                        expect.exact = true;
                        break;
                    },
                    .gt => break,
                }
            }

            if (log) {
                std.debug.print("keys:", .{});
                for (keys) |k| std.debug.print("{},", .{k});
                std.debug.print("\n", .{});
                std.debug.print("target key: {}\n", .{target_key});
            }

            const actual = binary_search_keys(
                u32,
                compare_keys,
                keys,
                target_key,
            );

            if (log) std.debug.print("expected: {}, actual: {}\n", .{ expect, actual });
            try std.testing.expectEqual(expect.index, actual.index);
            try std.testing.expectEqual(expect.exact, actual.exact);
        }
    }

    fn explicit_search(
        keys: []const u32,
        target_keys: []const u32,
        expected_results: []const BinarySearchResult,
    ) !void {
        assert(target_keys.len == expected_results.len);

        for (target_keys) |target_key, i| {
            if (log) {
                std.debug.print("keys:", .{});
                for (keys) |k| std.debug.print("{},", .{k});
                std.debug.print("\n", .{});
                std.debug.print("target key: {}\n", .{target_key});
            }
            const expect = expected_results[i];
            const actual = binary_search_keys(
                u32,
                compare_keys,
                keys,
                target_key,
            );
            try std.testing.expectEqual(expect.index, actual.index);
            try std.testing.expectEqual(expect.exact, actual.exact);
        }
    }
};

// TODO test search on empty slice
test "binary search: exhaustive" {
    if (test_binary_search.log) std.debug.print("\n", .{});
    var i: u32 = 1;
    while (i < 300) : (i += 1) {
        try test_binary_search.exhaustive_search(i);
    }
}

test "binary search: explicit" {
    if (test_binary_search.log) std.debug.print("\n", .{});
    try test_binary_search.explicit_search(
        &[_]u32{ 0, 3, 5, 8, 9, 11 },
        &[_]u32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 },
        &[_]BinarySearchResult{
            .{ .index = 0, .exact = true },
            .{ .index = 1, .exact = false },
            .{ .index = 1, .exact = false },
            .{ .index = 1, .exact = true },
            .{ .index = 2, .exact = false },
            .{ .index = 2, .exact = true },
            .{ .index = 3, .exact = false },
            .{ .index = 3, .exact = false },
            .{ .index = 3, .exact = true },
            .{ .index = 4, .exact = true },
            .{ .index = 5, .exact = false },
            .{ .index = 5, .exact = true },
            .{ .index = 6, .exact = false },
            .{ .index = 6, .exact = false },
        },
    );
}

// Our ManifestLevel implementation requires that if the target key is not found,
// the index of the next greatest key is returned even in the presence of duplicates.
test "binary search: duplicates" {
    if (test_binary_search.log) std.debug.print("\n", .{});
    try test_binary_search.explicit_search(
        &[_]u32{ 0, 0, 3, 3, 3, 5, 5, 5, 5 },
        &[_]u32{ 1, 2, 4, 6 },
        &[_]BinarySearchResult{
            .{ .index = 2, .exact = false },
            .{ .index = 2, .exact = false },
            .{ .index = 5, .exact = false },
            .{ .index = 9, .exact = false },
        },
    );
}
