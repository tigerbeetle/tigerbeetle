//! Based on https://github.com/zhangyunhao116/pdqsort.
//! Adapted from https://github.com/alichraghi/zort/blob/main/src/pdq.zig.

const std = @import("std");

/// Pattern Defeating Quick Sort
pub fn pdq_sort(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
) void {
    // limit the number of imbalanced partitions
    const limit = std.math.floorPowerOfTwo(usize, items.len) + 1;

    recurse(TableKey, TableValue, key_from_value, compare, items, null, limit);
}

/// sorts `orig_items` recursively.
///
/// If the slice had a predecessor in the original array, it is specified as
/// `orig_pred` (must be the minimum value if exist).
///
/// `orig_limit` is the number of allowed imbalanced partitions before switching to `heapsort`.
/// If zero, this function will immediately switch to heapsort.
fn recurse(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    orig_items: []TableValue,
    orig_pred: ?TableKey,
    orig_limit: usize,
) void {
    var items = orig_items;
    var pred = orig_pred;
    var limit = orig_limit;

    // slices of up to this length get sorted using insertion sort.
    const max_insertion = 24;

    // True if the last partitioning was reasonably balanced.
    var was_balanced = true;

    // True if the last partitioning didn't shuffle elements (the slice was already partitioned).
    var was_partitioned = true;

    while (true) {
        // Very short slices get sorted using insertion sort.
        if (items.len <= max_insertion) {
            insertionSort(TableKey, TableValue, key_from_value, compare, items);
            return;
        }

        // If too many bad pivot choices were made, simply fall back to heapsort in order to
        // guarantee `O(n log n)` worst-case.
        if (limit == 0) {
            @setCold(true);
            heapSort(TableKey, TableValue, key_from_value, compare, items);
            return;
        }

        // If the last partitioning was imbalanced, try breaking patterns in the slice by shuffling
        // some elements around. Hopefully we'll choose a better pivot this time.
        if (!was_balanced) {
            breakPatterns(TableValue, items);
            limit -= 1;
        }

        // Choose a pivot and try guessing whether the slice is already sorted.
        var likely_sorted = false;
        const pivot_idx = chosePivot(TableKey, TableValue, key_from_value, compare, items, &likely_sorted);

        // If the last partitioning was decently balanced and didn't shuffle elements, and if pivot
        // selection predicts the slice is likely already sorted...
        if (was_balanced and was_partitioned and likely_sorted) {
            // Try identifying several out-of-order elements and shifting them to correct
            // positions. If the slice ends up being completely sorted, we're done.
            if (partialInsertionSort(TableKey, TableValue, key_from_value, compare, items)) return;
        }

        // If the chosen pivot is equal to the predecessor, then it's the smallest element in the
        // slice. Partition the slice into elements equal to and elements greater than the pivot.
        // This case is usually hit when the slice contains many duplicate elements.
        if (pred != null and compare(pred.?, key_from_value(&items[pivot_idx])) == .eq) {
            const mid = partitionEqual(TableKey, TableValue, key_from_value, compare, items, pivot_idx);
            items = items[mid..];
            continue;
        }

        // Partition the slice.
        const mid = partition(
            TableKey,
            TableValue,
            key_from_value,
            compare,
            items,
            pivot_idx,
            &was_partitioned,
        );

        const left = items[0..mid];
        const pivot = key_from_value(&items[mid]);
        const right = items[mid + 1 ..];

        if (left.len < right.len) {
            was_balanced = left.len >= items.len / 8;
            recurse(TableKey, TableValue, key_from_value, compare, left, pred, limit);
            items = right;
            pred = pivot;
        } else {
            was_balanced = right.len >= items.len / 8;
            recurse(TableKey, TableValue, key_from_value, compare, right, pivot, limit);
            items = left;
        }
    }
}

/// partitions `items` into elements smaller than `items[pivot_idx]`,
/// followed by elements greater than or equal to `items[pivot_idx]`.
///
/// Returns the new pivot index.
/// Sets was_partitioned to `true` if necessary.
fn partition(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
    pivot_idx: usize,
    was_partitioned: *bool,
) usize {
    const pivot = key_from_value(&items[pivot_idx]);

    // move pivot to the first place
    std.mem.swap(TableValue, &items[0], &items[pivot_idx]);

    var i: usize = 1;
    var j: usize = items.len - 1;

    while (i <= j and compare(key_from_value(&items[i]), pivot) == .lt) i += 1;
    while (i <= j and compare(key_from_value(&items[j]), pivot) != .lt) j -= 1;

    // Check if items are already partitioned (no item to swap)
    if (i > j) {
        // put pivot back to the middle
        std.mem.swap(TableValue, &items[j], &items[0]);
        was_partitioned.* = true;
        return j;
    }

    j = i - 1 + partitionBlock(
        TableKey,
        TableValue,
        key_from_value,
        compare,
        items[i .. j + 1],
        pivot,
    );

    // put pivot back to the middle
    std.mem.swap(TableValue, &items[j], &items[0]);
    was_partitioned.* = false;

    return j;
}

fn partitionBlock(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
    pivot: TableKey,
) usize {
    const BLOCK_LEN = 128;

    var left: usize = 0;
    var block_left: usize = BLOCK_LEN;
    var start_left: usize = 0;
    var end_left: usize = 0;
    var offsets_left: [BLOCK_LEN]usize = undefined;

    var right: usize = items.len;
    var block_right: usize = BLOCK_LEN;
    var start_right: usize = 0;
    var end_right: usize = 0;
    var offsets_right: [BLOCK_LEN]usize = undefined;

    while (true) {
        const is_done = (right - left) <= 2 * BLOCK_LEN;

        if (is_done) {
            var rem = right - left;

            if (start_left < end_left or start_right < end_right) {
                rem -= BLOCK_LEN;
            }

            if (start_left < end_left) {
                block_right = rem;
            } else if (start_right < end_right) {
                block_left = rem;
            } else {
                block_left = rem / 2;
                block_right = rem - block_left;
            }
            std.debug.assert(block_left <= BLOCK_LEN and block_right <= BLOCK_LEN);
        }

        if (start_left == end_left) {
            start_left = 0;
            end_left = 0;

            var elem: usize = left;
            var i: usize = 0;
            while (i < block_left) : (i += 1) {
                offsets_left[end_left] = left + i;

                if (compare(key_from_value(&items[elem]), pivot) != .lt) end_left += 1;

                elem += 1;
            }
        }

        if (start_right == end_right) {
            start_right = 0;
            end_right = 0;

            var elem: usize = right;
            var i: usize = 0;
            while (i < block_right) : (i += 1) {
                elem -= 1; // avoid owerflow

                offsets_right[end_right] = right - i - 1;

                if (compare(key_from_value(&items[elem]), pivot) == .lt) end_right += 1;
            }
        }

        const count = std.math.min(end_left - start_left, end_right - start_right);
        if (count > 0) {
            cyclicSwap(
                TableValue,
                items,
                offsets_left[start_left .. start_left + count],
                offsets_right[start_right .. start_right + count],
            );
            start_left += count;
            start_right += count;
        }

        if (start_left == end_left) left += block_left;

        if (start_right == end_right) right -= block_right;

        if (is_done) break;
    }

    if (start_left < end_left) {
        while (start_left < end_left) {
            end_left -= 1;
            std.mem.swap(TableValue, &items[offsets_left[end_left]], &items[right - 1]);
            right -= 1;
        }
        return right;
    } else if (start_right < end_right) {
        while (start_right < end_right) {
            end_right -= 1;
            std.mem.swap(TableValue, &items[left], &items[offsets_right[end_right]]);
            left += 1;
        }
        return left;
    } else {
        return left;
    }
}

fn cyclicSwap(comptime TableValue: type, items: []TableValue, is: []usize, js: []usize) void {
    const count = is.len;
    const tmp = items[is[0]];
    items[is[0]] = items[js[0]];

    var i: usize = 1;
    while (i < count) : (i += 1) {
        items[js[i - 1]] = items[is[i]];
        items[is[i]] = items[js[i]];
    }

    items[js[count - 1]] = tmp;
}

const XorShift = u64;
fn next(r: *XorShift) usize {
    r.* ^= r.* << 13;
    r.* ^= r.* << 17;
    r.* ^= r.* << 5;
    return r.*;
}

fn breakPatterns(comptime TableValue: type, items: []TableValue) void {
    @setCold(true);
    if (items.len < 8) return;

    var r: XorShift = @intCast(XorShift, items.len);

    const modulus = std.math.ceilPowerOfTwoAssert(usize, items.len);

    var idxs: [3]usize = undefined;
    idxs[0] = items.len / 4 * 2 - 1;
    idxs[1] = items.len / 4 * 2;
    idxs[2] = items.len / 4 * 2 + 1;

    for (idxs) |idx| {
        var other = next(&r) & (modulus - 1);

        if (other >= items.len) other -= items.len;

        std.mem.swap(TableValue, &items[idx], &items[other]);
    }
}

fn partitionEqual(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
    pivot_idx: usize,
) usize {
    std.mem.swap(TableValue, &items[0], &items[pivot_idx]);

    const pivot = &items[0];

    var i: usize = 1;
    var j: usize = items.len - 1;

    while (true) : ({
        i += 1;
        j -= 1;
    }) {
        while (i <= j and compare(key_from_value(pivot), key_from_value(&items[i])) != .lt) i += 1;
        while (i <= j and compare(key_from_value(pivot), key_from_value(&items[j])) == .lt) j -= 1;

        if (i > j) break;

        std.mem.swap(TableValue, &items[i], &items[j]);
    }

    return i;
}

// partially sorts a slice by shifting several out-of-order elements around.
// Returns `true` if the slice is sorted at the end. This function is `O(n)` worst-case.
fn partialInsertionSort(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
) bool {
    @setCold(true);
    // maximum number of adjacent out-of-order pairs that will get shifted
    const max_steps = 5;

    // if the slice is shorter than this, don't shift any elements
    const shortest_shifting = 50;

    var i: usize = 1;
    var k: usize = 0;
    while (k < max_steps) : (k += 1) {
        // Find the next pair of adjacent out-of-order elements.
        while (i < items.len and compare(key_from_value(&items[i]), key_from_value(&items[i - 1])) != .lt) i += 1;

        // Are we done?
        if (i == items.len) return true;

        // Don't shift elements on short arrays, that has a performance cost.
        if (items.len < shortest_shifting) return false;

        // Swap the found pair of elements. This puts them in correct order.
        std.mem.swap(TableValue, &items[i], &items[i - 1]);

        // Shift the smaller element to the left.
        shiftTail(TableKey, TableValue, key_from_value, compare, items[0..i]);
        // Shift the greater element to the right.
        shiftHead(TableKey, TableValue, key_from_value, compare, items[i..]);
    }

    return false;
}

fn shiftTail(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
) void {
    if (items.len >= 2) {
        var i: usize = items.len - 1;
        while (i >= 1) : (i -= 1) {
            if (compare(key_from_value(&items[i]), key_from_value(&items[i - 1])) == .lt) break;
            std.mem.swap(TableValue, &items[i], &items[i - 1]);
        }
    }
}

fn shiftHead(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
) void {
    if (items.len >= 2) {
        var i: usize = 1;
        while (i < items.len) : (i += 1) {
            if (compare(key_from_value(&items[i]), key_from_value(&items[i - 1])) != .lt) break;
            std.mem.swap(TableValue, &items[i], &items[i - 1]);
        }
    }
}

/// choses a pivot in `items`.
/// Swaps likely_sorted when `items` seems to be already sorted.
fn chosePivot(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
    likely_sorted: *bool,
) usize {
    // minimum length for using the Tukey's ninther method
    const shortest_ninther = 50;

    // max_swaps is the maximum number of swaps allowed in this function
    const max_swaps = 4 * 3;

    var swaps: usize = 0;

    var a = items.len / 4 * 1;
    var b = items.len / 4 * 2;
    var c = items.len / 4 * 3;

    if (items.len >= 8) {
        if (items.len >= shortest_ninther) {
            // Find medians in the neighborhoods of `a`, `b` and `c`
            a = sort3(TableKey, TableValue, key_from_value, compare, items, a - 1, a, a + 1, &swaps);
            b = sort3(TableKey, TableValue, key_from_value, compare, items, b - 1, b, b + 1, &swaps);
            c = sort3(TableKey, TableValue, key_from_value, compare, items, c - 1, c, c + 1, &swaps);
        }

        // Find the median among `a`, `b` and `c`
        b = sort3(TableKey, TableValue, key_from_value, compare, items, a, b, c, &swaps);
    }

    if (swaps < max_swaps) {
        likely_sorted.* = (swaps == 0);
        return b;
    } else {
        // The maximum number of swaps was performed, so items are likely
        // in reverse order. Reverse it to make sorting faster.
        std.mem.reverse(TableValue, items);

        likely_sorted.* = true;
        return items.len - 1 - b;
    }
}

fn sort3(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
    a: usize,
    b: usize,
    c: usize,
    swaps: *usize,
) usize {
    if (compare(key_from_value(&items[b]), key_from_value(&items[a])) == .lt) {
        swaps.* += 1;
        std.mem.swap(TableValue, &items[b], &items[a]);
    }

    if (compare(key_from_value(&items[c]), key_from_value(&items[b])) == .lt) {
        swaps.* += 1;
        std.mem.swap(TableValue, &items[c], &items[b]);
    }

    if (compare(key_from_value(&items[b]), key_from_value(&items[a])) == .lt) {
        swaps.* += 1;
        std.mem.swap(TableValue, &items[b], &items[a]);
    }

    return b;
}

/// Stable in-place sort. O(n) best case, O(pow(n, 2)) worst case. O(1) memory (no allocator required).
fn insertionSort(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    items: []TableValue,
) void {
    var i: usize = 1;
    while (i < items.len) : (i += 1) {
        const x = items[i];
        var j: usize = i;
        while (j > 0 and compare(key_from_value(&x), key_from_value(&items[j - 1])) == .lt) : (j -= 1) {
            items[j] = items[j - 1];
        }
        items[j] = x;
    }
}

fn heapSort(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    arr: []TableValue,
) void {
    if (arr.len == 0) return;

    var i = arr.len / 2;
    while (i > 0) : (i -= 1) {
        heapify(TableKey, TableValue, key_from_value, compare, arr, arr.len, i - 1);
    }

    i = arr.len - 1;
    while (i > 0) : (i -= 1) {
        std.mem.swap(TableValue, &arr[0], &arr[i]);
        heapify(TableKey, TableValue, key_from_value, compare, arr, i, 0);
    }
}

fn heapify(
    comptime TableKey: type,
    comptime TableValue: type,
    comptime key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    comptime compare: fn (lhs: TableKey, rhs: TableKey) callconv(.Inline) std.math.Order,
    arr: []TableValue,
    n: usize,
    i: usize,
) void {
    // in ASC this should be largest, in desc smallest. so i just named this los = largest or samallest
    var los = i;
    const left = 2 * i + 1;
    const right = 2 * i + 2;

    if (left < n and compare(key_from_value(&arr[los]), key_from_value(&arr[left])) == .lt)
        los = left;

    if (right < n and compare(key_from_value(&arr[los]), key_from_value(&arr[right])) == .lt)
        los = right;

    if (los != i) {
        std.mem.swap(TableValue, &arr[i], &arr[los]);
        heapify(TableKey, TableValue, key_from_value, compare, arr, n, los);
    }
}
