const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("../stdx.zig");

const constants = @import("../constants.zig");

pub const Config = struct {
    mode: enum { lower_bound, upper_bound } = .lower_bound,
    prefetch: bool = true,
};

// TODO The Zig self hosted compiler will implement inlining itself before passing the IR to llvm,
// which should eliminate the current poor codegen of key_from_value.

/// Returns either the index of the value equal to `key`,
/// or if there is no such value then the index where `key` would be inserted.
///
/// In other words, return `i` such that both:
/// * key <= key_from_value(values[i]) or i == values.len
/// * key_value_from(values[i-1]) <= key or i == 0
///
/// If `values` contains duplicated matches, then returns
/// the first index when `Config.mode == .lower_bound`,
/// or the last index when `Config.mode == .upper_bound`.
/// This invariant can be expressed as:
/// * key_value_from(values[i-1]) < key or i == 0 when Config.mode == .lower_bound.
/// * key < key_value_from(values[i+1]) or i == values.len when Config.mode == .upper_bound.
///
/// Expects `values` to be sorted by key.
/// Doesn't perform the extra key comparison to determine if the match is exact.
pub fn binary_search_values_upsert_index(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    values: []const Value,
    key: Key,
    comptime config: Config,
) u32 {
    if (values.len == 0) return 0;

    var offset: usize = 0;
    var length: usize = values.len;
    while (length > 1) {
        if (constants.verify) {
            assert(offset == 0 or switch (comptime config.mode) {
                .lower_bound => key_from_value(&values[offset - 1]) < key,
                .upper_bound => key_from_value(&values[offset - 1]) <= key,
            });

            assert(offset + length == values.len or switch (comptime config.mode) {
                .lower_bound => key <= key_from_value(&values[offset + length]),
                .upper_bound => key < key_from_value(&values[offset + length]),
            });
        }

        const half = length / 2;
        if (config.prefetch) {
            // Prefetching:
            // ARRAY LAYOUTS FOR COMPARISON-BASED SEARCHING, page 18.
            // https://arxiv.org/abs/1509.05053.
            //
            // Prefetching the two possible positions we'd need for the next iteration:
            //
            // [....................mid....................]
            //           ^                       ^
            //       one quarter          three quarters.

            // We need to use pointer arithmetic and disable runtime safety to avoid bounds checks,
            // otherwise prefetching would harm the performance instead of improving it.
            // Since these pointers are never dereferenced, it's safe to dismiss this extra cost here.
            @setRuntimeSafety(constants.verify);
            const one_quarter = values.ptr + offset + half / 2;
            const three_quarters = one_quarter + half;

            // @sizeOf(Value) can be greater than a single cache line.
            // In that case, we need to prefetch multiple cache lines for a single value:
            comptime stdx.maybe(@sizeOf(Value) > constants.cache_line_size);
            const CacheLineBytes = [*]const [constants.cache_line_size]u8;
            const cache_lines_per_value = comptime stdx.div_ceil(
                @sizeOf(Value),
                constants.cache_line_size,
            );
            inline for (0..cache_lines_per_value) |i| {
                // Locality = 0 means no temporal locality. That is, the data can be immediately
                // dropped from the cache after it is accessed.
                const options = .{
                    .rw = .read,
                    .locality = 0,
                    .cache = .data,
                };

                @prefetch(@as(CacheLineBytes, @ptrCast(@alignCast(one_quarter))) + i, options);
                @prefetch(@as(CacheLineBytes, @ptrCast(@alignCast(three_quarters))) + i, options);
            }
        }

        const mid = offset + half;

        // This trick seems to be what's needed to get llvm to emit branchless code for this,
        // a ternary-style if expression was generated as a jump here for whatever reason.
        const next_offsets = [_]usize{ offset, mid };
        offset = next_offsets[
            // For exact matches, takes the first half if `mode == .lower_bound`,
            // or the second half if `mode == .upper_bound`.
            @intFromBool(switch (comptime config.mode) {
                .lower_bound => key_from_value(&values[mid]) < key,
                .upper_bound => key_from_value(&values[mid]) <= key,
            })
        ];

        length -= half;
    }

    if (constants.verify) {
        assert(length == 1);

        assert(offset == 0 or switch (comptime config.mode) {
            .lower_bound => key_from_value(&values[offset - 1]) < key,
            .upper_bound => key_from_value(&values[offset - 1]) <= key,
        });

        assert(offset + length == values.len or switch (comptime config.mode) {
            .lower_bound => key <= key_from_value(&values[offset + length]),
            .upper_bound => key < key_from_value(&values[offset + length]),
        });
    }

    offset += @intFromBool(key_from_value(&values[offset]) < key);

    if (constants.verify) {
        assert(offset == 0 or switch (config.mode) {
            .lower_bound => key_from_value(&values[offset - 1]) < key,
            .upper_bound => key_from_value(&values[offset - 1]) <= key,
        });
        assert(offset >= values.len - 1 or switch (config.mode) {
            .lower_bound => key <= key_from_value(&values[offset + 1]),
            .upper_bound => key < key_from_value(&values[offset + 1]),
        });
        assert(offset == values.len or
            key <= key_from_value(&values[offset]));
    }

    return @as(u32, @intCast(offset));
}

pub inline fn binary_search_keys_upsert_index(
    comptime Key: type,
    keys: []const Key,
    key: Key,
    comptime config: Config,
) u32 {
    return binary_search_values_upsert_index(
        Key,
        Key,
        struct {
            inline fn key_from_key(k: *const Key) Key {
                return k.*;
            }
        }.key_from_key,
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
    values: []const Value,
    key: Key,
    comptime config: Config,
) ?*const Value {
    const index = binary_search_values_upsert_index(
        Key,
        Value,
        key_from_value,
        values,
        key,
        config,
    );
    const exact = index < values.len and key_from_value(&values[index]) == key;

    if (exact) {
        const value = &values[index];
        if (constants.verify) {
            assert(key == key_from_value(value));
        }
        return value;
    } else {
        // TODO: Figure out how to fuzz this without causing asymptotic
        //       slowdown in all fuzzers
        return null;
    }
}

pub inline fn binary_search_keys(
    comptime Key: type,
    keys: []const Key,
    key: Key,
    comptime config: Config,
) BinarySearchResult {
    const index = binary_search_keys_upsert_index(Key, keys, key, config);
    return .{
        .index = index,
        .exact = index < keys.len and keys[index] == key,
    };
}

pub const BinarySearchRangeUpsertIndexes = struct {
    start: u32,
    end: u32,
};

/// Same semantics of `binary_search_values_upsert_indexes`:
/// Returns either the indexes of the values equal to `key_min` and `key_max`,
/// or the indexes where they would be inserted.
///
/// Expects `values` to be sorted by key.
/// If `values` contains duplicated matches, then returns
/// the first index for `key_min` and the last index for `key_max`.
///
/// Doesn't perform the extra key comparison to determine if the match is exact.
pub inline fn binary_search_values_range_upsert_indexes(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    values: []const Value,
    key_min: Key,
    key_max: Key,
) BinarySearchRangeUpsertIndexes {
    assert(key_min <= key_max);

    const start = binary_search_values_upsert_index(
        Key,
        Value,
        key_from_value,
        values,
        key_min,
        .{ .mode = .lower_bound },
    );

    if (start == values.len) return .{
        .start = start,
        .end = start,
    };

    const end = binary_search_values_upsert_index(
        Key,
        Value,
        key_from_value,
        values[start..],
        key_max,
        .{ .mode = .upper_bound },
    );

    return .{
        .start = start,
        .end = start + end,
    };
}

pub inline fn binary_search_keys_range_upsert_indexes(
    comptime Key: type,
    keys: []const Key,
    key_min: Key,
    key_max: Key,
) BinarySearchRangeUpsertIndexes {
    return binary_search_values_range_upsert_indexes(
        Key,
        Key,
        struct {
            inline fn key_from_key(k: *const Key) Key {
                return k.*;
            }
        }.key_from_key,
        keys,
        key_min,
        key_max,
    );
}

pub const BinarySearchRange = struct {
    start: u32,
    count: u32,
};

/// Returns the index of the first value greater than or equal to `key_min` and
/// the count of elements until the last value less than or equal to `key_max`.
///
/// Expects `values` to be sorted by key.
/// The result is always safe for slicing using the `values[start..][0..count]` idiom,
/// even when no elements are matched.
pub inline fn binary_search_values_range(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    values: []const Value,
    key_min: Key,
    key_max: Key,
) BinarySearchRange {
    const upsert_indexes = binary_search_values_range_upsert_indexes(
        Key,
        Value,
        key_from_value,
        values,
        key_min,
        key_max,
    );

    if (upsert_indexes.start == values.len) return .{
        .start = upsert_indexes.start -| 1,
        .count = 0,
    };

    const inclusive = @intFromBool(
        upsert_indexes.end < values.len and
            key_max == key_from_value(&values[upsert_indexes.end]),
    );
    return .{
        .start = upsert_indexes.start,
        .count = upsert_indexes.end - upsert_indexes.start + inclusive,
    };
}

pub inline fn binary_search_keys_range(
    comptime Key: type,
    keys: []const Key,
    key_min: Key,
    key_max: Key,
) BinarySearchRange {
    return binary_search_values_range(
        Key,
        Key,
        struct {
            inline fn key_from_key(k: *const Key) Key {
                return k.*;
            }
        }.key_from_key,
        keys,
        key_min,
        key_max,
    );
}

const test_binary_search = struct {
    const fuzz = @import("../testing/fuzz.zig");

    const log = false;

    const gpa = std.testing.allocator;

    fn less_than_key(_: void, a: u32, b: u32) bool {
        return a < b;
    }

    fn exhaustive_search(keys_count: u32, comptime mode: anytype) !void {
        const keys = try gpa.alloc(u32, keys_count);
        defer gpa.free(keys);

        for (keys, 0..) |*key, i| key.* = @as(u32, @intCast(7 * i + 3));

        var target_key: u32 = 0;
        while (target_key < keys_count + 13) : (target_key += 1) {
            var expect: BinarySearchResult = .{ .index = 0, .exact = false };
            for (keys, 0..) |key, i| {
                switch (std.math.order(key, target_key)) {
                    .lt => expect.index = @as(u32, @intCast(i)) + 1,
                    .eq => {
                        expect.index = @as(u32, @intCast(i));
                        expect.exact = true;
                        if (mode == .lower_bound) break;
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
                keys,
                target_key,
                .{ .mode = mode },
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
        comptime mode: anytype,
    ) !void {
        assert(target_keys.len == expected_results.len);

        for (target_keys, 0..) |target_key, i| {
            if (log) {
                std.debug.print("keys:", .{});
                for (keys) |k| std.debug.print("{},", .{k});
                std.debug.print("\n", .{});
                std.debug.print("target key: {}\n", .{target_key});
            }
            const expect = expected_results[i];
            const actual = binary_search_keys(
                u32,
                keys,
                target_key,
                .{ .mode = mode },
            );
            try std.testing.expectEqual(expect.index, actual.index);
            try std.testing.expectEqual(expect.exact, actual.exact);
        }
    }

    fn random_sequence(allocator: std.mem.Allocator, random: std.rand.Random, iter: usize) ![]const u32 {
        const keys_count = @min(
            @as(usize, 1E6),
            fuzz.random_int_exponential(random, usize, iter),
        );

        const keys = try allocator.alloc(u32, keys_count);
        for (keys) |*key| key.* = fuzz.random_int_exponential(random, u32, 100);
        std.mem.sort(u32, keys, {}, less_than_key);

        return keys;
    }

    fn random_search(random: std.rand.Random, iter: usize, comptime mode: anytype) !void {
        const keys = try random_sequence(std.testing.allocator, random, iter);
        defer std.testing.allocator.free(keys);

        const target_key = fuzz.random_int_exponential(random, u32, 100);

        var expect: BinarySearchResult = .{ .index = 0, .exact = false };
        for (keys, 0..) |key, i| {
            switch (std.math.order(key, target_key)) {
                .lt => expect.index = @as(u32, @intCast(i)) + 1,
                .eq => {
                    expect.index = @as(u32, @intCast(i));
                    expect.exact = true;
                    if (mode == .lower_bound) break;
                },
                .gt => break,
            }
        }

        const actual = binary_search_keys(
            u32,
            keys,
            target_key,
            .{ .mode = mode },
        );

        if (log) std.debug.print("expected: {}, actual: {}\n", .{ expect, actual });
        try std.testing.expectEqual(expect.index, actual.index);
        try std.testing.expectEqual(expect.exact, actual.exact);
    }

    pub fn explicit_range_search(
        sequence: []const u32,
        key_min: u32,
        key_max: u32,
        expected: BinarySearchRange,
    ) !void {
        const actual = binary_search_keys_range(
            u32,
            sequence,
            key_min,
            key_max,
        );

        try std.testing.expectEqual(expected.start, actual.start);
        try std.testing.expectEqual(expected.count, actual.count);

        // Make sure that the index is valid for slicing using the [start..][0..count] idiom:
        const expected_slice = sequence[expected.start..][0..expected.count];
        const actual_slice = sequence[actual.start..][0..actual.count];
        try std.testing.expectEqualSlices(u32, expected_slice, actual_slice);
    }

    fn random_range_search(random: std.rand.Random, iter: usize) !void {
        const keys = try random_sequence(std.testing.allocator, random, iter);
        defer std.testing.allocator.free(keys);

        const target_range = blk: {
            // Cover many combinations of key_min, key_max:
            var key_min = if (keys.len > 0 and random.boolean())
                random.intRangeAtMostBiased(u32, keys[0], keys[keys.len - 1])
            else
                fuzz.random_int_exponential(random, u32, 100);

            var key_max = if (keys.len > 0 and random.boolean())
                random.intRangeAtMostBiased(u32, keys[0], keys[keys.len - 1])
            else if (random.boolean())
                key_min
            else
                fuzz.random_int_exponential(random, u32, 100);

            if (key_max < key_min) std.mem.swap(u32, &key_min, &key_max);
            assert(key_min <= key_max);

            break :blk .{
                .key_min = key_min,
                .key_max = key_max,
            };
        };

        var expect: BinarySearchRange = .{ .start = 0, .count = 0 };
        var key_target: enum { key_min, key_max } = .key_min;
        for (keys) |key| {
            if (key_target == .key_min) {
                switch (std.math.order(key, target_range.key_min)) {
                    .lt => if (expect.start < keys.len - 1) {
                        expect.start += 1;
                    },
                    .gt, .eq => key_target = .key_max,
                }
            }

            if (key_target == .key_max) {
                switch (std.math.order(key, target_range.key_max)) {
                    .lt, .eq => expect.count += 1,
                    .gt => break,
                }
            }
        }

        const actual = binary_search_keys_range(
            u32,
            keys,
            target_range.key_min,
            target_range.key_max,
        );

        if (log) std.debug.print("expected: {?}, actual: {?}\n", .{ expect, actual });
        try std.testing.expectEqual(expect.start, actual.start);
        try std.testing.expectEqual(expect.count, actual.count);
    }
};

test "binary search: exhaustive" {
    if (test_binary_search.log) std.debug.print("\n", .{});
    inline for (.{ .lower_bound, .upper_bound }) |mode| {
        var i: u32 = 1;
        while (i < 300) : (i += 1) {
            try test_binary_search.exhaustive_search(i, mode);
        }
    }
}

test "binary search: explicit" {
    if (test_binary_search.log) std.debug.print("\n", .{});

    inline for (.{ .lower_bound, .upper_bound }) |mode| {
        try test_binary_search.explicit_search(
            &[_]u32{},
            &[_]u32{0},
            &[_]BinarySearchResult{
                .{ .index = 0, .exact = false },
            },
            mode,
        );

        try test_binary_search.explicit_search(
            &[_]u32{4} ** 10,
            &[_]u32{4},
            &[_]BinarySearchResult{
                .{
                    .index = if (mode == .lower_bound) 0 else 9,
                    .exact = true,
                },
            },
            mode,
        );

        try test_binary_search.explicit_search(
            &[_]u32{},
            &[_]u32{0},
            &[_]BinarySearchResult{
                .{ .index = 0, .exact = false },
            },
            mode,
        );

        try test_binary_search.explicit_search(
            &[_]u32{1},
            &[_]u32{ 0, 1, 2 },
            &[_]BinarySearchResult{
                .{ .index = 0, .exact = false },
                .{ .index = 0, .exact = true },
                .{ .index = 1, .exact = false },
            },
            mode,
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
            mode,
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
            mode,
        );
    }
}

test "binary search: duplicates" {
    if (test_binary_search.log) std.debug.print("\n", .{});
    try test_binary_search.explicit_search(
        &[_]u32{ 0, 0, 3, 3, 3, 5, 5, 5, 5 },
        &[_]u32{ 0, 1, 2, 3, 4, 5, 6 },
        &[_]BinarySearchResult{
            .{ .index = 0, .exact = true },
            .{ .index = 2, .exact = false },
            .{ .index = 2, .exact = false },
            .{ .index = 2, .exact = true },
            .{ .index = 5, .exact = false },
            .{ .index = 5, .exact = true },
            .{ .index = 9, .exact = false },
        },
        .lower_bound,
    );
    try test_binary_search.explicit_search(
        &[_]u32{ 0, 0, 3, 3, 3, 5, 5, 5, 5 },
        &[_]u32{ 0, 1, 2, 3, 4, 5, 6 },
        &[_]BinarySearchResult{
            .{ .index = 1, .exact = true },
            .{ .index = 2, .exact = false },
            .{ .index = 2, .exact = false },
            .{ .index = 4, .exact = true },
            .{ .index = 5, .exact = false },
            .{ .index = 8, .exact = true },
            .{ .index = 9, .exact = false },
        },
        .upper_bound,
    );
}

test "binary search: random" {
    var rng = std.rand.DefaultPrng.init(42);
    inline for (.{ .lower_bound, .upper_bound }) |mode| {
        var i: usize = 0;
        while (i < 2048) : (i += 1) {
            try test_binary_search.random_search(rng.random(), i, mode);
        }
    }
}

test "binary search: explicit range" {
    if (test_binary_search.log) std.debug.print("\n", .{});

    // Exact inverval:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        3,
        1000,
        .{
            .start = 0,
            .count = 9,
        },
    );

    // Larger inverval:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        2,
        1001,
        .{
            .start = 0,
            .count = 9,
        },
    );

    // Inclusive key_min and exclusive key_max:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        3,
        9,
        .{
            .start = 0,
            .count = 2,
        },
    );

    // Exclusive key_min and inclusive key_max:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        5,
        10,
        .{
            .start = 2,
            .count = 1,
        },
    );

    // Exclusive interval:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        5,
        14,
        .{
            .start = 2,
            .count = 1,
        },
    );

    // Inclusive interval:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        15,
        100,
        .{
            .start = 3,
            .count = 5,
        },
    );

    // Where key_min == key_max:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        10,
        10,
        .{
            .start = 2,
            .count = 1,
        },
    );

    // Interval smaller than the first element:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        1,
        2,
        .{
            .start = 0,
            .count = 0,
        },
    );

    // Interval greater than the last element:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        1_001,
        10_000,
        .{
            .start = 8,
            .count = 0,
        },
    );

    // Nonexistent interval in the middle:
    try test_binary_search.explicit_range_search(
        &[_]u32{ 3, 4, 10, 15, 20, 25, 30, 100, 1000 },
        31,
        99,
        .{
            .start = 7,
            .count = 0,
        },
    );

    // Empty slice:
    try test_binary_search.explicit_range_search(
        &[_]u32{},
        1,
        2,
        .{
            .start = 0,
            .count = 0,
        },
    );
}

test "binary search: duplicated range" {
    if (test_binary_search.log) std.debug.print("\n", .{});
    try test_binary_search.explicit_range_search(
        &[_]u32{ 1, 3, 3, 3, 5, 5, 5, 7 },
        3,
        5,
        .{
            .start = 1,
            .count = 6,
        },
    );
    try test_binary_search.explicit_range_search(
        &[_]u32{ 1, 1, 1, 3, 5, 7 },
        1,
        1,
        .{
            .start = 0,
            .count = 3,
        },
    );
}

test "binary search: random range" {
    var rng = std.rand.DefaultPrng.init(42);
    var i: usize = 0;
    while (i < 2048) : (i += 1) {
        try test_binary_search.random_range_search(rng.random(), i);
    }
}
