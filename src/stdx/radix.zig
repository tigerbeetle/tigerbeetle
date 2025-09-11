//! Stable, non-allocating, out-of-place LSD radix sort over unsigned integer keys.
//! Sorts `values` in ascending order by `key_from_value`, using `values_scratch`
//! as an equally sized, disjoint swap buffer. Keys must be an unsigned `Int`.
//! The sorted result is in the original buffer `values`.
//! The implementation builds per-pass histograms, skips trivial passes (all items
//! in one bucket), and uses a fixed digit width (8 or 11 bits based on `Value` size)
//! to reduce the number of passes. Buffers are swapped after each non-trivial pass;
//! if the number of such passes is odd, results are copied back so `values` holds
//! the output on return.

const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx.zig");

/// Stable, ascending radix sort for unsigned intergers. Sorted result will be in `values`.
pub fn sort(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    values: []Value,
    values_scratch: []Value,
) void {
    comptime {
        assert(@typeInfo(Key) == .int);
        assert(@typeInfo(Key).int.signedness == .unsigned);
    }

    assert(stdx.disjoint_slices(Value, Value, values, values_scratch));
    assert(values.len == values_scratch.len);
    assert(values.len <= std.math.maxInt(u32)); // At the moment we use u32 for the histogram.

    const count: u32 = @intCast(values.len);

    if (count == 0) return;
    // TODO: insertion sort fallback.

    // Heuristic: use wider digits for larger vlaue sizes to reduce the number of passes.
    const radix_bits_heuristic = if (@sizeOf(Value) >= 128) 11 else 8;
    //const radix_bits_heuristic = 8;
    //const radix_bits_heuristic = if (@sizeOf(Value) >= 128 or @sizeOf(Key) >= 32) 11 else 8;
    const radix_bits = @min(@bitSizeOf(Key), radix_bits_heuristic);
    const radix_partitions = 1 << radix_bits;
    const radix_mask = radix_partitions - 1;
    const radix_passes = stdx.div_ceil(@bitSizeOf(Key), radix_bits);

    const BitsKey = std.math.Log2Int(Key);

    // Create the histograms per pass in a single pass over `values`.
    const histograms = blk: {
        var histogram: [radix_passes][radix_partitions]u32 align(64) = @splat(@splat(0));
        for (values) |*value| {
            const key = key_from_value(value);
            inline for (0..radix_passes) |pass| {
                const pass_bit_offset: BitsKey = @intCast(pass * radix_bits);
                const partition_id: u32 = @intCast((key >> pass_bit_offset) & radix_mask);
                histogram[pass][partition_id] += 1;
            }
        }
        break :blk histogram;
    };

    var source: []Value = values;
    var target: []Value = values_scratch;
    var target_offsets: [radix_partitions]u32 = @splat(0);
    var partition_passes: u32 = 0;

    // This hot loop is unrolled as it gives ~10% performance improvement.
    inline for (0..radix_passes) |pass| {
        const pass_bit_offset: BitsKey = @intCast(pass * radix_bits);
        // Determine if a pass is trivial if exactly one bucket has all `count` elements.
        const pass_trivial: bool = trivial: {
            for (histograms[pass]) |partition_count| {
                if (partition_count == 0) continue;
                break :trivial partition_count == count;
            }
            unreachable;
        };

        // Skip trival passes.
        if (!pass_trivial) {
            partition_passes += 1;

            // Build prefix sums to determin the partition target offsets.
            var next_offset: u32 = 0;
            for (0..radix_partitions) |partition_id| {
                target_offsets[partition_id] = next_offset;
                next_offset += histograms[pass][partition_id];
            }

            // Partition pass.
            for (source) |*value| {
                const key: Key = key_from_value(value);
                const partition_id: u32 = @intCast((key >> pass_bit_offset) & radix_mask);

                target[target_offsets[partition_id]] = value.*;
                target_offsets[partition_id] += 1;
            }
            std.mem.swap([]Value, &source, &target);
        }
    }

    //std.debug.print("partition_passes {} radix bits {} key {} bits value {} bytes \n", .{
    //partition_passes,
    //radix_bits,
    //@bitSizeOf(Key),
    //@sizeOf(Value),
    //});
    // Copy the values back in the input buffer `values`.
    if (partition_passes % 2 != 0) {
        stdx.copy_disjoint(.exact, Value, values, values_scratch);
    }
}

const ratio = stdx.PRNG.ratio;

// TODO: test more of the keys (in particular edge case).
//       e.g. large keys
//       what if we sort u6?

pub fn TestValueType(comptime Key: type, comptime value_length: usize) type {
    return struct {
        const Value = @This();

        x: Key, // TODO: rename to key.
        y: u32, // y ensures that values are distinct for the purpose of checking stability.
        padding: [value_length]u8 = @splat(0),

        inline fn key_from_value(value: *const Value) Key {
            return value.x;
        }

        // Those functions are only required for the comparision based helpers.
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
}

test "radix_sort_stable" {
    inline for (.{
        u3, //  Smaller than radix bits.
        u64, // Requires multiple passes.
    }) |Key| {
        inline for (.{
            0,
            130, // Test heuristic with larger values.
        }) |value_size_min| {
            const allocator = std.testing.allocator;

            const Value = TestValueType(Key, value_size_min);

            var prng = stdx.PRNG.from_seed(0);

            const values_max = 1 << 18; // Explores uneven and even passes to test copy back.
            const values_all = try allocator.alloc(Value, values_max);
            defer allocator.free(values_all);

            const values_all_expected = try allocator.alloc(Value, values_max);
            defer allocator.free(values_all_expected);

            const values_all_scratch = try allocator.alloc(Value, values_max);
            defer allocator.free(values_all_scratch);

            for (0..256) |_| {
                const values_count = prng.range_inclusive(u32, 2, values_max);
                const values_expected = values_all_expected[0..values_count];
                const values = values_all[0..values_count];
                const values_scratch = values_all_scratch[0..values_count];

                {
                    // Set up `values`.

                    for (values) |*value| {
                        value.* = .{
                            .x = prng.int_inclusive(Key, @min(std.math.maxInt(Key), values_count * 2 - 1)),
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

                //std.mem.sort(Value, values, {}, Value.compare_x_ascending);
                sort(Key, Value, Value.key_from_value, values, values_scratch);

                for (values, values_expected) |value, value_expected| {
                    try std.testing.expectEqual(value.x, value_expected.x);
                    try std.testing.expectEqual(value.y, value_expected.y);
                }
            }
        }
    }
}
