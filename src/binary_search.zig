const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

// TODO Add prefeching when @prefetch is available: https://github.com/ziglang/zig/issues/3600.
//
// TODO The Zig self hosted compiler will implement inlining itself before passing the IR to llvm,
// which should eliminate the current poor codegen of key_from_value/compare_keys.
pub fn binary_search(
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

const test_binary_search = struct {
    const log = false;

    const gpa = std.testing.allocator;

    const Value = struct {
        key: u32,
        fn to_key(v: Value) u32 {
            return v.key;
        }
    };

    fn compare_keys(a: u32, b: u32) math.Order {
        return math.order(a, b);
    }

    fn exhaustive_search(values_count: u32) !void {
        const values = try gpa.alloc(Value, values_count);
        defer gpa.free(values);

        for (values) |*value, i| value.* = .{ .key = @intCast(u32, 7 * i + 3) };

        var target_key: u32 = 0;
        while (target_key < values_count + 13) : (target_key += 1) {
            var expect_index: usize = 0;
            for (values) |value, i| {
                if (compare_keys(value.to_key(), target_key) != .lt) break;
                expect_index = i + 1;
            }

            if (log) {
                std.debug.print("values:", .{});
                for (values) |v| std.debug.print("{},", .{v.key});
                std.debug.print("\n", .{});
                std.debug.print("target key: {}\n", .{target_key});
            }

            const actual_index = binary_search(
                u32,
                Value,
                Value.to_key,
                compare_keys,
                values,
                target_key,
            );

            if (log) std.debug.print("expected: {}, actual: {}\n", .{ expect_index, actual_index });
            try std.testing.expectEqual(expect_index, actual_index);
        }
    }

    fn explicit_search(
        keys: []const u32,
        target_keys: []const u32,
        expect_indexes: []const usize,
    ) !void {
        assert(target_keys.len == expect_indexes.len);

        const values = try gpa.alloc(Value, keys.len);
        defer gpa.free(values);

        for (values) |*value, i| value.* = .{ .key = keys[i] };

        for (target_keys) |target_key, i| {
            if (log) {
                std.debug.print("values:", .{});
                for (values) |v| std.debug.print("{},", .{v.key});
                std.debug.print("\n", .{});
                std.debug.print("target key: {}\n", .{target_key});
            }
            const expect_index = expect_indexes[i];
            const actual_index = binary_search(
                u32,
                Value,
                Value.to_key,
                compare_keys,
                values,
                target_key,
            );
            try std.testing.expectEqual(expect_index, actual_index);
        }
    }
};

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
        &[_]usize{ 0, 1, 1, 1, 2, 2, 3, 3, 3, 4, 5, 5, 6, 6 },
    );
}
