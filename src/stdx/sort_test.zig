const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");
const ratio = stdx.PRNG.ratio;

test "sort_stable" {
    const Value = struct {
        const Value = @This();

        x: u32, // x determines the order of the values.
        y: u32, // y ensures that values are distinct for the purpose of checking stability.

        fn compare_x_ascending(_: void, a: Value, b: Value) bool {
            return a.x < b.x;
        }

        fn compare_x_descending(_: void, a: Value, b: Value) bool {
            return a.x > b.x;
        }

        fn compare_xy_ascending(_: void, a: Value, b: Value) bool {
            if (a.x < b.x) return true;
            if (a.x > b.x) return false;
            return a.y < b.y;
        }
    };

    const allocator = std.testing.allocator;

    var prng = stdx.PRNG.from_seed_testing();

    const values_max = 1 << 15;
    const values_all = try allocator.alloc(Value, values_max);
    defer allocator.free(values_all);

    const values_all_expected = try allocator.alloc(Value, values_max);
    defer allocator.free(values_all_expected);

    for (0..256) |_| {
        const values_count = prng.range_inclusive(u32, 2, values_max);
        const values_expected = values_all_expected[0..values_count];
        const values = values_all[0..values_count];

        {
            // Set up `values`.

            for (values) |*value| {
                value.* = .{
                    .x = prng.int_inclusive(u32, values_count * 2 - 1),
                    .y = undefined,
                };
            }

            // Sort algorithms often optimize the case of already-sorted (or already-reverse-sorted)
            // sub-arrays.
            const partitions_count = prng.range_inclusive(u32, 1, @max(values_count, 64) - 1);
            // The `partition_reverse_probability` is a subset of the partitions sorted by
            // `partition_sort_percent`.
            const partition_sort_probability = ratio(prng.int_inclusive(u8, 100), 100);
            const partition_reverse_probability = ratio(prng.int_inclusive(u8, 100), 100);

            var partitions_remaining: u32 = partitions_count;
            var partition_offset: u32 = 0;
            while (partition_offset < values_count) {
                const partition_size = size: {
                    if (partitions_remaining == 1) {
                        break :size values_count - partition_offset;
                    } else {
                        break :size prng.range_inclusive(u32, 1, values_count - partition_offset);
                    }
                };

                if (prng.chance(partition_sort_probability)) {
                    const partition = values[partition_offset..][0..partition_size];
                    if (prng.chance(partition_reverse_probability)) {
                        std.mem.sortUnstable(Value, partition, {}, Value.compare_x_descending);
                    } else {
                        std.mem.sortUnstable(Value, partition, {}, Value.compare_x_ascending);
                    }
                }

                partitions_remaining -= 1;
                partition_offset += partition_size;
            }

            for (values, 0..) |*value, i| value.y = @intCast(i);
        }

        {
            // Set up `values_expected`.
            stdx.copy_disjoint(.exact, Value, values_expected, values);
            std.mem.sortUnstable(Value, values_expected, {}, Value.compare_xy_ascending);

            // Sanity-check the expected values' order.
            for (
                values_expected[0 .. values_count - 1],
                values_expected[1..],
            ) |a, b| {
                assert(a.x <= b.x);
                if (a.x == b.x) assert(a.y < b.y);
            }
        }

        std.mem.sort(Value, values, {}, Value.compare_x_ascending);

        for (values, values_expected) |value, value_expected| {
            try std.testing.expectEqual(value.x, value_expected.x);
            try std.testing.expectEqual(value.y, value_expected.y);
        }
    }
}
