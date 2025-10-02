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

/// Stable, ascending radix sort for unsigned integers. The sorted result will be in `values`.
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
    assert(values.len <= std.math.maxInt(u32));

    if (values.len == 0) return;
    if (values.len <= 32) {
        return std.sort.insertion(Value, values, {}, struct {
            fn lessThan(_: void, a: Value, b: Value) bool {
                return key_from_value(&a) < key_from_value(&b);
            }
        }.lessThan);
    }
    radix_sort(Key, Value, key_from_value, values, values_scratch);
}

fn radix_sort(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    values: []Value,
    values_scratch: []Value,
) void {
    const count: u32 = @intCast(values.len);
    // Heuristic: use more bits for larger value sizes to reduce the number of passes.
    const radix_bits_heuristic = if (@sizeOf(Value) >= 128) 11 else 8;
    const radix_bits = @min(@bitSizeOf(Key), radix_bits_heuristic);
    const radix_passes = stdx.div_ceil(@bitSizeOf(Key), radix_bits);
    const radix_partitions = 1 << radix_bits;
    const radix_mask: u32 = radix_partitions - 1;

    const BitsKey = std.math.Log2Int(Key); // Used to shift the key for each pass.
    const Histograms: type = [radix_passes][radix_partitions]u32;
    comptime assert(@sizeOf(Histograms) <= 200 * stdx.KiB);

    // Create histograms per radix pass in a single iteration over `values`.
    const histograms = blk: {
        var histograms: Histograms align(64) = @splat(@splat(0));

        for (values) |*value| {
            const key = key_from_value(value);
            inline for (0..radix_passes) |pass| {
                const pass_bit_offset: BitsKey = @intCast(pass * radix_bits);
                const partition_id: u32 = @intCast((key >> pass_bit_offset) & radix_mask);
                histograms[pass][partition_id] += 1;
            }
        }
        break :blk histograms;
    };

    var source: []Value = values;
    var target: []Value = values_scratch;
    var target_offsets: [radix_partitions]u32 = @splat(0);

    inline for (0..radix_passes) |pass| {
        // Determine if a pass is trivial if exactly one partition has all `count` elements.
        const pass_trivial: bool = for (histograms[pass]) |partition_count| {
            if (partition_count == count) break true;
        } else false;

        if (!pass_trivial) {
            // Build prefix sums.
            var next_offset: u32 = 0;
            for (0..radix_partitions) |partition_id| {
                target_offsets[partition_id] = next_offset;
                next_offset += histograms[pass][partition_id];
            }

            // Partitioning pass.
            const pass_bit_offset: BitsKey = @intCast(pass * radix_bits);
            for (source) |*value| {
                const key: Key = key_from_value(value);
                const partition_id: u32 = @intCast((key >> pass_bit_offset) & radix_mask);

                target[target_offsets[partition_id]] = value.*;
                target_offsets[partition_id] += 1;
            }
            std.mem.swap([]Value, &source, &target);
        }
    }

    // Copy the values back into the input buffer `values`.
    if (values.ptr != source.ptr) {
        stdx.copy_disjoint(.exact, Value, values, values_scratch);
    }
}

const ratio = stdx.PRNG.ratio;

pub fn TestValueType(comptime Key: type, comptime value_length: usize) type {
    return struct {
        const Value = @This();

        x: Key,
        y: u32, // y ensures that values are distinct for the purpose of checking stability.
        padding: [value_length]u8 = @splat(0),

        inline fn key_from_value(value: *const Value) Key {
            return value.x;
        }

        fn compare_x_ascending(_: void, a: Value, b: Value) bool {
            return a.x < b.x;
        }

        fn compare_x_descending(_: void, a: Value, b: Value) bool {
            return a.x > b.x;
        }
    };
}

test "radix_sort: smoke" {
    const Value = TestValueType(u8, 0);
    var values: [5]Value = .{
        Value{ .x = 3, .y = 0 },
        Value{ .x = 2, .y = 0 },
        Value{ .x = 3, .y = 1 },
        Value{ .x = 1, .y = 0 },
        Value{ .x = 5, .y = 0 },
    };
    const values_expected: [5]Value = .{
        Value{ .x = 1, .y = 0 },
        Value{ .x = 2, .y = 0 },
        Value{ .x = 3, .y = 0 },
        Value{ .x = 3, .y = 1 },
        Value{ .x = 5, .y = 0 },
    };
    var values_scratch: [5]Value = undefined;
    radix_sort(
        u8,
        Value,
        Value.key_from_value,
        &values,
        &values_scratch,
    );
    try std.testing.expectEqual(values_expected, values);
}

//  Ascending order + stability against a (x,y) baseline, with a large Value
//  payload to exercise the 11-bit radix path (since @sizeOf(Value) >= 128).
test "radix_sort: ascending & stable on many duplicates" {
    const Key = u32;
    const Value = TestValueType(Key, 128); // >=128 so radix_bits heuristic picks 11
    const allocator = std.testing.allocator;

    const n: usize = 2048;

    const values = try allocator.alloc(Value, n);
    defer allocator.free(values);

    const scratch = try allocator.alloc(Value, n);
    defer allocator.free(scratch);

    // Many duplicates; y = original index (used to check stability).
    for (values, 0..) |*v, i| {
        const k: Key = @intCast(i % 257);
        v.* = .{ .x = k, .y = @intCast(i) };
    }

    radix_sort(Key, Value, Value.key_from_value, values, scratch);

    // Verify that the order is `ascending` and `stable`.
    for (values[0 .. values.len - 1], values[1..]) |a, b| {
        switch (std.math.order(a.x, b.x)) {
            .eq => try std.testing.expect(a.y < b.y),
            .lt => try std.testing.expect(a.x < b.x),
            .gt => unreachable,
        }
    }
}

// All keys equal → every pass is "trivial". Sort should be a no-op on values,
// keep relative order (stability).
test "radix_sort: all-equal keys preserve relative order (stability)" {
    const Key = u64;
    const Value = TestValueType(Key, 8);
    const allocator = std.testing.allocator;

    const n: usize = 1024;

    const values = try allocator.alloc(Value, n);
    defer allocator.free(values);

    const scratch = try allocator.alloc(Value, n);
    defer allocator.free(scratch);

    // Fill scratch with a sentinel to detect writes.
    const sentinel: Value = .{ .x = 0xFFFF_FFFF_FFFF_FFFF, .y = 0xDEAD_BEEF };
    for (scratch) |*s| s.* = sentinel;

    // All keys identical; y = original index to check stability.
    for (values, 0..) |*v, i| {
        v.* = .{ .x = 42, .y = @intCast(i) };
    }

    radix_sort(Key, Value, Value.key_from_value, values, scratch);

    // Verify that the order is `ascending` and `stable`.
    for (values[0 .. values.len - 1], values[1..]) |a, b| {
        switch (std.math.order(a.x, b.x)) {
            .eq => try std.testing.expect(a.y < b.y),
            .lt => try std.testing.expect(a.x < b.x),
            .gt => unreachable,
        }
    }
}

test "fuzz radix_sort_stable" {
    inline for (.{
        .{ u3, 0 }, // Smaller than radix bits.
        .{ u256, 130 }, // Largest histogram, requires multiple passes and bit heuristic.
    }) |pair| {
        const Key = pair.@"0";
        const value_size_min = pair.@"1";
        const allocator = std.testing.allocator;

        const Value = TestValueType(Key, value_size_min);

        var prng = stdx.PRNG.from_seed_testing();

        const values_max = 1 << 18; // Explores uneven and even passes to test copy back.
        const values_all = try allocator.alloc(Value, values_max);
        defer allocator.free(values_all);

        const values_all_scratch = try allocator.alloc(Value, values_max);
        defer allocator.free(values_all_scratch);

        for (0..64) |_| {
            const values_count = prng.range_inclusive(u32, 2, values_max);
            const values = values_all[0..values_count];
            const values_scratch = values_all_scratch[0..values_count];

            {
                // Set up `values`.
                for (values) |*value| {
                    value.* = .{
                        .x = prng.int_inclusive(Key, @min(
                            std.math.maxInt(Key),
                            values_count * 2 - 1,
                        )),
                        .y = undefined,
                    };
                }

                // Sort algorithms often optimize the case of already-sorted
                // (or already-reverse-sorted) sub-arrays.
                const partitions_count = prng.range_inclusive(
                    u32,
                    1,
                    @max(values_count, 64) - 1,
                );
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
                            break :size prng.range_inclusive(
                                u32,
                                1,
                                values_count - partition_offset,
                            );
                        }
                    };

                    if (prng.chance(partition_sort_probability)) {
                        const partition = values[partition_offset..][0..partition_size];
                        if (prng.chance(partition_reverse_probability)) {
                            std.mem.sortUnstable(
                                Value,
                                partition,
                                {},
                                Value.compare_x_descending,
                            );
                        } else {
                            std.mem.sortUnstable(
                                Value,
                                partition,
                                {},
                                Value.compare_x_ascending,
                            );
                        }
                    }

                    partitions_remaining -= 1;
                    partition_offset += partition_size;
                }

                for (values, 0..) |*value, i| value.y = @intCast(i);
            }

            radix_sort(Key, Value, Value.key_from_value, values, values_scratch);

            // Verify that the order is `ascending` and `stable`.
            for (values[0 .. values.len - 1], values[1..]) |a, b| {
                switch (std.math.order(a.x, b.x)) {
                    .eq => try std.testing.expect(a.y < b.y),
                    .lt => try std.testing.expect(a.x < b.x),
                    .gt => unreachable,
                }
            }
        }
    }
}
