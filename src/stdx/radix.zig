const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx.zig");

// Optimizations
// - skip trivial passes
// - one pass histogram
// - insertion sort for small keys
// - unrolling
// - microptimizations
//      - pointers

// TODO: benchmark
// - remove inline for?
// - remvoe pointer types?
// - can we simplify the code?

pub fn sort(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    values: []Value,
    values_scratch: []Value,
) void {
    const radix_bits = if (@sizeOf(Value) >= 128) 11 else 8;
    const radix_size = 1 << radix_bits;
    const radix_mask = radix_size - 1;
    const radix_passes = ((@bitSizeOf(Key) - 1) / radix_bits) + 1; // TODO(TZ): clean-up.
    const count: u32 = @intCast(values.len);

    // TODO: insertion sort.
    // TODO: preconditions;
    assert(values.len <= values_scratch.len);

    if (count == 0) return;

    // Inline helper struct as it shares many parameters of the sort function.
    const context = struct {
        // Create histogram for every pass and parition.
        fn create_histogram(input: []Value) [radix_passes][radix_size]u32 {
            var histogram: [radix_passes][radix_size]u32 = .{.{0} ** radix_size} ** radix_passes;
            for (input) |*value| {
                var key = key_from_value(value);
                inline for (0..radix_passes) |pass| {
                    const partition: u32 = @intCast(key & radix_mask);
                    histogram[pass][partition] += 1;
                    key >>= radix_bits;
                }
            }
            return histogram;
        }

        // A pass is trival iff all frequencies are 0 or iff one bucket contains all elements.
        fn trivial_pass(frequencies: [radix_size]u32, elements: u32) bool {
            for (frequencies) |frequency| {
                if (frequency == 0) continue;
                return frequency == elements;
            }
            return true;
        }
    };

    const histogram = context.create_histogram(values);

    // TODO: understand again.
    const bits = std.math.log2(@bitSizeOf(Key));
    const shift_type = std.meta.Int(.unsigned, bits);

    // should we simply make sure to swap the intial values back
    var source: []Value = values;
    var target: []Value = values_scratch;
    var queue_ptrs: [radix_size][*]Value = undefined;
    var non_trivial_passes: u32 = 0;

    inline for (0..radix_passes) |pass| {
        // Because the loop is unrolled we need to have the following for control flow reasons.
        // A `continue` would be clearer but it does not work with `infline for`.
        const elements = if (context.trivial_pass(histogram[pass], count)) 0 else count;
        const target_offset = if (elements == 0) 0 else queue_ptrs.len;
        const shift: shift_type = @intCast(pass * radix_bits);

        // Count non-trivial passes so that we can swap the buffers in the correct order.
        non_trivial_passes += if (elements == 0) 0 else 1;

        // Build prefix sums.
        // We take direct pointers here as it generates much faster code.
        // TODO double check.
        // TODO create invariants.
        var next_offset: u32 = 0;
        for (0..target_offset) |i| {
            queue_ptrs[i] = target.ptr + next_offset;
            next_offset += histogram[pass][i];
        }

        const source_ptr: [*]const Value = source.ptr;

        for (0..elements) |i_i| {
            const value = &source_ptr[i_i];
            const key: Key = key_from_value(value);
            const index: usize = @intCast((key >> shift) & radix_mask);

            queue_ptrs[index][0] = value.*;
            queue_ptrs[index] += 1; // advance the pointer
            @prefetch(queue_ptrs[index] + 1, .{ .rw = .write, .locality = 3, .cache = .data });
        }

        if (elements != 0) std.mem.swap([]Value, &source, &target);
    }
    //
    if (non_trivial_passes % 2 != 0) {
        stdx.copy_disjoint(.exact, Value, values, values_scratch);
    }
}

const ratio = stdx.PRNG.ratio;

// TODO: test more of the keys (in particular edge case).
//       e.g. large keys
test "sort_stable" {
    const Value = struct {
        const Value = @This();

        x: u32, // TODO: rename to key.
        y: u32, // y ensures that values are distinct for the purpose of checking stability.

        inline fn key_from_value(value: *const Value) u32 {
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

    const allocator = std.testing.allocator;

    var prng = stdx.PRNG.from_seed(0);

    const values_max = 1 << 15;
    const values_all = try allocator.alloc(Value, values_max);
    defer allocator.free(values_all);

    const values_all_expected = try allocator.alloc(Value, values_max);
    defer allocator.free(values_all_expected);

    const values_scratch = try allocator.alloc(Value, values_max);
    defer allocator.free(values_scratch);

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

        //std.mem.sort(Value, values, {}, Value.compare_x_ascending);
        sort(u32, Value, Value.key_from_value, values, values_scratch);

        for (values, values_expected) |value, value_expected| {
            try std.testing.expectEqual(value.x, value_expected.x);
            try std.testing.expectEqual(value.y, value_expected.y);
        }
    }
}
