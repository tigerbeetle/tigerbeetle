const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

pub const Config = struct {
    verify: bool = false,
};

// TODO Add prefeching when @prefetch is available: https://github.com/ziglang/zig/issues/3600.
//
// TODO The Zig self hosted compiler will implement inlining itself before passing the IR to llvm,
// which should eliminate the current poor codegen of key_from_value/compare_keys.

/// Returns either the index of the first value equal to `key`,
/// or if there is no such value then the index where `key` would be inserted.
///
/// In other words, return `i` such that both:
/// * key_from_value(values[i])  >= key or i == values.len
/// * key_value_from(values[i-1]) < key or i == 0
///
/// Doesn't perform the extra key comparison to determine if the match is exact.
pub fn binary_search_values_raw(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    values: []const Value,
    key: Key,
    comptime config: Config,
) u32 {
    if (values.len == 0) return 0;

    if (config.verify) {
        // Input must be sorted by key.
        for (values) |_, i| {
            assert(i == 0 or
                compare_keys(key_from_value(&values[i - 1]), key_from_value(&values[i])) != .gt);
        }
    }

    var offset: usize = 0;
    var length: usize = values.len;
    while (length > 1) {
        if (config.verify) {
            assert(offset == 0 or
                compare_keys(key_from_value(&values[offset - 1]), key) != .gt);
            assert(offset + length == values.len or
                compare_keys(key_from_value(&values[offset + length]), key) != .lt);
        }

        const half = length / 2;
        const mid = offset + half;

        // This trick seems to be what's needed to get llvm to emit branchless code for this,
        // a ternary-style if expression was generated as a jump here for whatever reason.
        const next_offsets = [_]usize{ offset, mid };
        offset = next_offsets[@boolToInt(compare_keys(key_from_value(&values[mid]), key) == .lt)];

        length -= half;
    }

    if (config.verify) {
        assert(length == 1);
        assert(offset == 0 or
            compare_keys(key_from_value(&values[offset - 1]), key) != .gt);
        assert(offset + length == values.len or
            compare_keys(key_from_value(&values[offset + length]), key) != .lt);
    }

    offset += @boolToInt(compare_keys(key_from_value(&values[offset]), key) == .lt);

    if (config.verify) {
        assert(offset == 0 or
            compare_keys(key_from_value(&values[offset - 1]), key) == .lt);
        assert(offset == values.len or
            compare_keys(key_from_value(&values[offset]), key) != .lt);
    }

    return @intCast(u32, offset);
}

pub inline fn binary_search_keys_raw(
    comptime Key: type,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    keys: []const Key,
    key: Key,
    comptime config: Config,
) u32 {
    return binary_search_values_raw(
        Key,
        Key,
        struct {
            inline fn key_from_key(k: *const Key) Key {
                return k.*;
            }
        }.key_from_key,
        compare_keys,
        keys,
        key,
        config,
    );
}

const BinarySearchResult = struct {
    index: u32,
    exact: bool,
};

pub inline fn binary_search_values(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    values: []const Value,
    key: Key,
    comptime config: Config,
) BinarySearchResult {
    const index = binary_search_values_raw(Key, Value, key_from_value, compare_keys, values, key, config);
    return .{
        .index = index,
        .exact = index < values.len and compare_keys(key_from_value(&values[index]), key) == .eq,
    };
}

pub inline fn binary_search_keys(
    comptime Key: type,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    keys: []const Key,
    key: Key,
    comptime config: Config,
) BinarySearchResult {
    const index = binary_search_keys_raw(Key, compare_keys, keys, key, config);
    return .{
        .index = index,
        .exact = index < keys.len and compare_keys(keys[index], key) == .eq,
    };
}

const test_binary_search = struct {
    const fuzz = @import("../test/fuzz.zig");

    const log = false;

    const gpa = std.testing.allocator;

    inline fn compare_keys(a: u32, b: u32) math.Order {
        return math.order(a, b);
    }

    fn less_than_key(_: void, a: u32, b: u32) bool {
        return a < b;
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
                    .lt => expect.index = @intCast(u32, i) + 1,
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
                .{ .verify = true },
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
                .{ .verify = true },
            );
            try std.testing.expectEqual(expect.index, actual.index);
            try std.testing.expectEqual(expect.exact, actual.exact);
        }
    }

    fn random_search(random: std.rand.Random, iter: usize) !void {
        const keys_count = @min(
            @as(usize, 1E6),
            fuzz.random_int_exponential(random, usize, iter),
        );

        const keys = try std.testing.allocator.alloc(u32, keys_count);
        defer std.testing.allocator.free(keys);

        for (keys) |*key| key.* = fuzz.random_int_exponential(random, u32, 100);
        std.sort.sort(u32, keys, {}, less_than_key);
        const target_key = fuzz.random_int_exponential(random, u32, 100);

        var expect: BinarySearchResult = .{ .index = 0, .exact = false };
        for (keys) |key, i| {
            switch (compare_keys(key, target_key)) {
                .lt => expect.index = @intCast(u32, i) + 1,
                .eq => {
                    expect.exact = true;
                    break;
                },
                .gt => break,
            }
        }

        const actual = binary_search_keys(
            u32,
            compare_keys,
            keys,
            target_key,
            .{ .verify = true },
        );

        if (log) std.debug.print("expected: {}, actual: {}\n", .{ expect, actual });
        try std.testing.expectEqual(expect.index, actual.index);
        try std.testing.expectEqual(expect.exact, actual.exact);
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
        &[_]u32{},
        &[_]u32{0},
        &[_]BinarySearchResult{
            .{ .index = 0, .exact = false },
        },
    );
    try test_binary_search.explicit_search(
        &[_]u32{1},
        &[_]u32{ 0, 1, 2 },
        &[_]BinarySearchResult{
            .{ .index = 0, .exact = false },
            .{ .index = 0, .exact = true },
            .{ .index = 1, .exact = false },
        },
    );
    try test_binary_search.explicit_search(
        &[_]u32{ 1, 3 },
        &[_]u32{ 0, 1, 2, 3, 4 },
        &[_]BinarySearchResult{
            .{ .index = 0, .exact = false },
            .{ .index = 0, .exact = true },
            .{ .index = 1, .exact = false },
            .{ .index = 1, .exact = true },
            .{ .index = 2, .exact = false },
        },
    );
    try test_binary_search.explicit_search(
        &[_]u32{ 1, 3, 5, 8, 9, 11 },
        &[_]u32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 },
        &[_]BinarySearchResult{
            .{ .index = 0, .exact = false },
            .{ .index = 0, .exact = true },
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

test "binary search: random" {
    var rng = std.rand.DefaultPrng.init(42);
    var i: usize = 0;
    while (i < 2048) : (i += 1) {
        try test_binary_search.random_search(rng.random(), i);
    }
}
