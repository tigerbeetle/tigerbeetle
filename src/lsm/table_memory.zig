const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const loser_tree = @import("./loser_tree.zig");
const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;

const SortedRun = loser_tree.SortedRun;
// `sort_suffix` breaks the `values` array into “sorted runs” (sorted sub‑arrays).
// Each SortedRun captures the start (min) and end (max) indices of one sorted run.
// We can exploit this knowledge to check if the full array is sorted in O(k) and avoid
// the final sort, which reduces tail latency.
// The number of calls to `sort_suffix` are determined by `constants.lsm_compaction_ops`.

// TODO:
// - Coalesce runs.
// - Maintain sorted flag acrros.
// - Tree of loosers not full K but next power two.
// - Analyze numbers again.

// 0. Outsorce file.
// 1. Absorb, maybe expensive but not so expensive, now you say merge cheaper 5-7 ms
// 2. Sorted flag, how much does it make.

// Why isSorted does not help with AccountEvent?
//   - boundary values?

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const TreeOfLosers = loser_tree.LooserTreeType(
            Key,
            Value,
            constants.lsm_compaction_ops,
            key_from_value,
        );
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: u32 = 0,
            /// When true, `values` is strictly ascending-ordered (no duplicates).
            sorted: bool = true,
        };

        const SortAlgorithm = union(enum) {
            radix: []Value,
            std,
        };

        const Mutability = union(enum) {
            mutable: struct {
                /// The offset (within `values`) of the unsorted suffix.
                /// - At the end of each beat, the mutable table's suffix is sorted and
                ///   deduplicated, and the suffix offset advances.
                /// - (At the end of the bar, the mutable table consists of a sequence of sorted
                ///   arrays, which is then itself finally sorted and deduplicated.)
                suffix_offset: u32 = 0,
            },
            immutable: struct {
                /// An empty table has nothing to flush.
                flushed: bool = true,
                /// This field is only used for assertions, to verify that we don't absorb the
                /// mutable table immediately prior to checkpoint.
                absorbed: bool = false,
                snapshot_min: u64 = 0,
            },
        };

        const sorted_runs_max = constants.lsm_compaction_ops;
        sorted_runs: [sorted_runs_max]SortedRun,
        sorted_runs_count: u16,

        values: []Value,
        tmp_buffer: []Value,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        pub fn init(
            table: *TableMemory,
            allocator: mem.Allocator,
            mutability: std.meta.Tag(Mutability),
            name: []const u8,
            options: struct {
                value_count_limit: u32,
            },
        ) !void {
            assert(options.value_count_limit <= Table.value_count_max);

            table.* = .{
                .value_context = .{},
                .mutability = switch (mutability) {
                    .mutable => .{ .mutable = .{} },
                    .immutable => .{ .immutable = .{} },
                },

                .sorted_runs = .{.{
                    .min = std.math.maxInt(u32),
                    .max = std.math.maxInt(u32),
                }} ** sorted_runs_max,
                .sorted_runs_count = 0,
                .name = name,

                .values = undefined,
                .tmp_buffer = undefined,
            };

            // TODO This would ideally be value_count_limit, but needs to be value_count_max to
            // ensure that memory table coalescing is deterministic even if the batch limit changes.
            table.values = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.values);

            table.tmp_buffer = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.tmp_buffer);
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            allocator.free(table.values);
            allocator.free(table.tmp_buffer);
        }

        pub fn reset(table: *TableMemory) void {
            const mutability: Mutability = switch (table.mutability) {
                .immutable => .{ .immutable = .{} },
                .mutable => .{ .mutable = .{} },
            };

            table.* = .{
                .sorted_runs = .{.{
                    .min = std.math.maxInt(u32),
                    .max = std.math.maxInt(u32),
                }} ** sorted_runs_max,
                .sorted_runs_count = 0,
                .values = table.values,
                .tmp_buffer = table.tmp_buffer,
                .value_context = .{},
                .mutability = mutability,
                .name = table.name,
            };
        }

        pub fn count(table: *const TableMemory) u32 {
            return table.value_context.count;
        }

        pub fn values_used(table: *const TableMemory) []Value {
            return table.values[0..table.count()];
        }

        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count < table.values.len);
            if (table.value_context.sorted) {
                table.value_context.sorted = table.value_context.count == 0 or
                    key_from_value(&table.values[table.value_context.count - 1]) <
                    key_from_value(value);
            } else {
                assert(table.value_context.count > 0);
            }

            table.values[table.value_context.count] = value.*;
            table.value_context.count += 1;
        }

        /// This must be called on sorted tables.
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            assert(table.value_context.count <= table.values.len);
            assert(table.value_context.sorted);

            return binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                table.values_used(),
                key,
                .{ .mode = .upper_bound },
            );
        }

        pub inline fn merge(input: []Value, batches: []SortedRun, output: []Value) u32 {
            assert(output.len == input.len);
            var tree = TreeOfLosers.init(input, batches);

            var target_index: u32 = 0;
            output[target_index] = tree.next();
            target_index += 1;

            for (1..input.len) |_| {
                const value_next = tree.next();
                const value_next_equal = target_index > 0 and
                    key_from_value(&output[target_index - 1]) ==
                    key_from_value(&value_next);

                if (value_next_equal) {
                    if (Table.usage == .secondary_index) {
                        assert(Table.tombstone(&output[target_index - 1]) !=
                            Table.tombstone(&value_next));
                        // annihilate both
                        target_index -= 1;
                    } else {
                        // overwrite old value - last writer wins
                        output[target_index - 1] = value_next;
                    }
                } else {
                    output[target_index] = value_next;
                    target_index += 1;
                }
            }

            return target_index;
        }
        /// Merge and sort the immutable/mutable tables (favoring values in the latter) into the
        /// immutable table. Then reset the mutable table.
        pub fn merge_from(
            table_immutable: *TableMemory,
            table_mutable: *TableMemory,
            snapshot_min: u64,
        ) void {
            assert(table_immutable.mutability == .immutable);
            maybe(table_immutable.mutability.immutable.absorbed);
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.value_context.sorted);

            assert((table_mutable.sorted_runs_count == 0) == (table_mutable.count() == 0));
            defer assert(table_immutable.value_context.sorted);

            //var timer = std.time.Timer.start() catch unreachable;

            //const expensive_sorted_check = table_mutable.value_context.sorted or std.sort.isSorted(
            //Value,
            //table_mutable.values_used(),
            //{},
            //sort_values_by_key_in_ascending_order,
            //);

            //const duration = timer.lap();
            //std.debug.print("duration of isSorted {} microseconds and we have {} sorted runs\n", .{
            //duration / std.time.ns_per_us,
            //table_mutable.sorted_runs_count,
            //});
            var already_sorted = (table_mutable.value_context.sorted) or (table_mutable.sorted_runs_count == 1);

            // check if sorted
            if (!already_sorted and table_mutable.sorted_runs_count > 1) {
                const values = table_mutable.values_used();
                const sorted_runs_count = table_mutable.sorted_runs_count;
                var sorted = true;
                for (
                    table_mutable.sorted_runs[0 .. sorted_runs_count - 1],
                    table_mutable.sorted_runs[1..sorted_runs_count],
                ) |sr_a, sr_b| {
                    sorted = sorted and
                        (key_from_value(&values[sr_a.max - 1]) < key_from_value(&values[sr_b.min]));
                }
                //assert(already_sorted == sorted);
                already_sorted = sorted;
            }
            //assert(expensive_sorted_check == already_sorted);
            // TODO: hack try to fix this correctly
            if (already_sorted) {
                //std.debug.print("hit sorted\n", .{});
                // sorted and no duplicates
                // here we should swap the table memory
                //stdx.copy_disjoint(
                //.inexact,
                //Value,
                //table_immutable.values[0..],
                //table_mutable.values[0..table_mutable.count()],
                //);

                // ugly swap
                const tmp_ptr = table_mutable.values.ptr;
                table_mutable.values.ptr = table_immutable.values.ptr;
                table_immutable.values.ptr = tmp_ptr;

                table_immutable.value_context.count = table_mutable.count();
                table_immutable.value_context.sorted = true;
            } else {
                //std.debug.print("hit unsorted\n", .{});
                const sorted_runs_count = table_mutable.sorted_runs_count;
                assert(table_mutable.count() > 0);
                assert(sorted_runs_count > 0);
                assert(table_mutable.sorted_runs[sorted_runs_count - 1].max == table_mutable.count());
                assert(table_mutable.sorted_runs[0].min == 0);

                // check if partially sorted.
                if (sorted_runs_count > 1) {
                    const values = table_mutable.values_used();
                    var almost_sorted = true;
                    for (
                        table_mutable.sorted_runs[0 .. sorted_runs_count - 1],
                        table_mutable.sorted_runs[1..sorted_runs_count],
                    ) |sr_a, sr_b| {
                        almost_sorted = almost_sorted and
                            (key_from_value(&values[sr_a.max - 1]) <= key_from_value(&values[sr_b.min]));
                        //if (almost_sorted) {
                        //std.debug.print("two sorted runs are almost sorted \n", .{});
                        //}
                    }
                }

                if (sorted_runs_count > 1) {
                    for (
                        table_mutable.sorted_runs[0 .. sorted_runs_count - 1],
                        table_mutable.sorted_runs[1..sorted_runs_count],
                    ) |sr_a, sr_b| {
                        assert(sr_a.max == sr_b.min);
                    }
                }

                const target_count = merge(
                    table_mutable.values_used(),
                    table_mutable.sorted_runs[0..],
                    table_immutable.values[0..table_mutable.count()],
                );

                // ----- Deduplicate
                // Merge values with identical keys (last one wins) and collapse tombstones for
                // secondary indexes.
                //const source_count: u32 = @intCast(table_mutable.count());
                //const values = table_immutable.values;
                //var source_index: u32 = 0;
                //var target_index: u32 = 0;
                //while (source_index < source_count) {
                //if (source_index != target_index) {
                //values[target_index] = values[source_index];
                //}

                //// If we're at the end of the source, there is no next value, so the next value
                //// can't be equal.
                //const value_next_equal = source_index + 1 < source_count and
                //key_from_value(&values[source_index]) ==
                //key_from_value(&values[source_index + 1]);

                //if (value_next_equal) {
                //if (Table.usage == .secondary_index) {
                //// Secondary index optimization --- cancel out put and remove.
                //// NB: while this prevents redundant tombstones from getting to disk, we
                //// still spend some extra CPU work to sort the entries in memory. Ideally,
                //// we annihilate tombstones immediately, before sorting, but that's tricky
                //// to do with scopes.
                //assert(Table.tombstone(&values[source_index]) !=
                //Table.tombstone(&values[source_index + 1]));
                //source_index += 2;
                //target_index += 0;
                //} else {
                //// The last value in a run of duplicates needs to be the one that ends up in
                //// target.
                //source_index += 1;
                //target_index += 0;
                //}
                //} else {
                //source_index += 1;
                //target_index += 1;
                //}
                //}

                // At this point, source_index and target_index are actually counts.
                // source_index will always be incremented after the final iteration as part of the
                // continue expression.
                // target_index will always be incremented, since either source_index runs out first
                // so value_next_equal is false, or a new value is hit, which will increment it.
                const source_count: u32 = @intCast(table_mutable.count());
                assert(target_count <= source_count);
                table_immutable.value_context.count = target_count;
                table_immutable.value_context.sorted = true;
            }

            if (constants.verify) {
                const target_count = table_immutable.count();
                if (0 < table_immutable.count()) {
                    const values = table_immutable.values_used();
                    for (
                        values[0 .. target_count - 1],
                        values[1..target_count],
                    ) |*value, *value_next| {
                        assert(key_from_value(value) < key_from_value(value_next));
                    }
                }
            }

            table_mutable.reset();
            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.value_context.count == 0,
                .absorbed = false,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count <= table.values.len);
            defer assert(table.value_context.sorted);

            table.sort();

            // If we have no values, then we can consider ourselves flushed right away.
            table.mutability = .{ .immutable = .{
                .flushed = table.value_context.count == 0,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn make_mutable(table: *TableMemory) void {
            assert(table.mutability == .immutable);
            assert(table.mutability.immutable.flushed == true);
            assert(table.value_context.count <= table.values.len);
            assert(table.value_context.sorted);

            table.* = .{
                .sorted_runs = .{.{
                    .min = std.math.maxInt(u32),
                    .max = std.math.maxInt(u32),
                }} ** sorted_runs_max,
                .sorted_runs_count = 0,
                .values = table.values,
                .tmp_buffer = table.tmp_buffer,
                .value_context = .{},
                .mutability = .{ .mutable = .{} },
                .name = table.name,
            };
        }

        /// Merge and sort the immutable/mutable tables (favoring values in the latter) into the
        /// immutable table. Then reset the mutable table.
        pub fn absorb(
            table_immutable: *TableMemory,
            table_mutable: *TableMemory,
            snapshot_min: u64,
        ) void {
            assert(table_immutable.mutability == .immutable);
            maybe(table_immutable.mutability.immutable.absorbed);
            assert(table_immutable.value_context.sorted);
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.value_context.sorted);

            const values_count_limit = table_immutable.values.len;
            assert(values_count_limit == values_count_limit);
            assert(table_immutable.count() <= values_count_limit);
            assert(table_mutable.count() <= values_count_limit);
            assert(table_immutable.count() + table_mutable.count() <= values_count_limit);

            stdx.copy_disjoint(
                .inexact,
                Value,
                table_immutable.values[table_immutable.count()..],
                table_mutable.values[0..table_mutable.count()],
            );

            const tables_combined_count = table_immutable.count() + table_mutable.count();
            table_immutable.value_context.count =
                sort_suffix_from_offset(table_immutable.values[0..tables_combined_count], 0, .std);
            assert(table_immutable.count() <= tables_combined_count);

            table_mutable.reset();
            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.value_context.count == 0,
                .absorbed = true,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn sort(table: *TableMemory) void {
            assert(table.mutability == .mutable);

            if (!table.value_context.sorted) {
                //assert((table.sorted_runs_count == 0) == (table.count() == 0));
                table.mutable_sort_suffix_from_offset(0, .std);
                table.value_context.sorted = true;

                if (table.sorted_runs_count == 0) {
                    table.mutability.mutable.suffix_offset = 0;
                } else {
                    // BUG: reset this here since we are simply going over k since we do not obey sorted_runs_count
                    // TODO: give our sorted runs from the outside in the tree and build own inside.
                    table.sorted_runs = .{.{
                        .min = std.math.maxInt(u32),
                        .max = std.math.maxInt(u32),
                    }} ** sorted_runs_max;
                    table.sorted_runs_count = 1;
                    table.sorted_runs[0] = .{
                        .min = 0,
                        .max = table.count(),
                    };
                }
            }
        }

        pub fn sort_suffix(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            assert(table.mutability.mutable.suffix_offset <= table.count());

            const run_min = table.mutability.mutable.suffix_offset;
            table.mutable_sort_suffix_from_offset(
                table.mutability.mutable.suffix_offset,
                //.std,
                .{ .radix = table.tmp_buffer },
            );
            const run_max = table.mutability.mutable.suffix_offset;
            assert(run_min <= run_max);

            if (run_min < run_max) {
                table.sorted_runs[table.sorted_runs_count] = .{ .min = run_min, .max = run_max };
                table.sorted_runs_count += 1;
            }
            table.value_context.sorted = (table.count() == 0);
            assert(table.mutability.mutable.suffix_offset == table.count());
        }

        fn mutable_sort_suffix_from_offset(table: *TableMemory, offset: u32, sort_algorithm: SortAlgorithm) void {
            assert(table.mutability == .mutable);
            assert(offset == table.mutability.mutable.suffix_offset or offset == 0);
            assert(offset <= table.count());

            const target_count = sort_suffix_from_offset(table.values_used(), offset, sort_algorithm);
            table.value_context.count = target_count;
            table.mutability = .{ .mutable = .{ .suffix_offset = target_count } };
        }

        pub fn count_frequency(
            input: []const Value,
            histogram: anytype,
        ) void {
            const RADIX_BITS = 8;
            const RADIX_SIZE = 1 << RADIX_BITS;
            const RADIX_LEVELS = ((@bitSizeOf(Key) - 1) / RADIX_BITS) + 1;
            const RADIX_MASK = RADIX_SIZE - 1;

            // make copy of value
            for (input) |*value| {
                var key = key_from_value(value);
                for (0..RADIX_LEVELS) |pass| {
                    const partition: usize = @intCast(key & RADIX_MASK);
                    histogram[pass][partition] += 1;
                    key >>= RADIX_BITS;
                }
            }
            for (0..RADIX_LEVELS) |pass| {
                var sum: usize = 0;
                for (histogram[pass]) |value| {
                    sum += value;
                }
                assert(sum == input.len);
            }
        }

        pub fn is_trivial(radix_frequencies: []usize, number_elements: usize) bool {
            for (radix_frequencies) |freq| {
                if (freq != 0) { // remove branch?
                    return freq == number_elements;
                }
            }
            assert(number_elements == 0);
            return true;
        }

        fn determine_shift_type(comptime bits: u16) type {
            return std.meta.Int(
                .unsigned,
                bits,
            );
        }

        pub inline fn radix_sort_unrolled(
            input: []Value,
            scratch_buffer: []Value,
        ) void {
            assert(input.len == scratch_buffer.len);

            if (input.len == 0) return;

            const RADIX_BITS = 8;
            const RADIX_SIZE = 1 << RADIX_BITS;
            const RADIX_LEVELS = ((@bitSizeOf(Key) - 1) / RADIX_BITS) + 1; // this is the max. iterations we need to do.
            const RADIX_MASK = RADIX_SIZE - 1;

            assert(std.math.isPowerOfTwo(RADIX_LEVELS));

            const bits = std.math.log2(@bitSizeOf(Key));
            const shift_type = determine_shift_type(bits);

            var histogram: [RADIX_LEVELS][RADIX_SIZE]usize = .{.{0} ** RADIX_SIZE} ** RADIX_LEVELS;
            count_frequency(input, &histogram);

            var source = &input;
            var target = &scratch_buffer;
            var queue_offsets: [RADIX_SIZE]usize = [_]usize{0} ** RADIX_SIZE;
            var trivial_passes: u32 = 0;

            inline for (0..RADIX_LEVELS) |pass| {
                const elements = if (is_trivial(&histogram[pass], source.len)) 0 else source.len;
                trivial_passes += if (elements == 0) 1 else 0;
                const target_offset = if (elements == 0) 0 else queue_offsets.len;
                const shift: shift_type = @intCast(pass * RADIX_BITS);

                var next_offset: usize = 0;
                for (0..target_offset) |i| {
                    queue_offsets[i] = next_offset;
                    next_offset += histogram[pass][i]; // build prefix sum
                }

                // todo try batch wise
                for (0..elements) |i_i| {
                    const value = &source.*[i_i];
                    const key: Key = key_from_value(value);
                    const index: usize = @intCast((key >> shift) & RADIX_MASK);
                    target.*[queue_offsets[index]] = value.*; // this copies it at the right location
                    queue_offsets[index] += 1;
                }

                // UGLY SWAP
                // swap the input pointer and output pointer
                if (elements == 0) {} else {
                    const tmp_ref = source;
                    source = target;
                    target = tmp_ref;
                }
            }
            if (trivial_passes % 2 != 0) {
                std.mem.copyForwards(Value, input, scratch_buffer);
            }
        }

        /// Returns the new length of `values`. (Values are deduplicated after sorting, so the
        /// returned count may be less than or equal to the original `values.len`.)
        fn sort_suffix_from_offset(values: []Value, offset: u32, sort_algorithm: SortAlgorithm) u32 {
            assert(offset <= values.len);

            switch (sort_algorithm) {
                .std => std.mem.sort(Value, values[offset..], {}, sort_values_by_key_in_ascending_order),
                .radix => |scratch_buffer| {
                    const slice_length: usize = values[offset..].len;
                    const tmp = scratch_buffer[0..slice_length];
                    assert(tmp.len == slice_length);
                    radix_sort_unrolled(values[offset..], tmp);
                    assert(std.sort.isSorted(Value, values[offset..], {}, sort_values_by_key_in_ascending_order));
                },
            }
            //std.mem.sort(Value, values[offset..], {}, sort_values_by_key_in_ascending_order);

            // Merge values with identical keys (last one wins) and collapse tombstones for
            // secondary indexes.
            const source_count: u32 = @intCast(values.len);
            var source_index: u32 = offset;
            var target_index: u32 = offset;
            while (source_index < source_count) {
                const value = values[source_index];
                values[target_index] = value;

                // If we're at the end of the source, there is no next value, so the next value
                // can't be equal.
                const value_next_equal = source_index + 1 < source_count and
                    key_from_value(&values[source_index]) ==
                    key_from_value(&values[source_index + 1]);

                if (value_next_equal) {
                    if (Table.usage == .secondary_index) {
                        // Secondary index optimization --- cancel out put and remove.
                        // NB: while this prevents redundant tombstones from getting to disk, we
                        // still spend some extra CPU work to sort the entries in memory. Ideally,
                        // we annihilate tombstones immediately, before sorting, but that's tricky
                        // to do with scopes.
                        assert(Table.tombstone(&values[source_index]) !=
                            Table.tombstone(&values[source_index + 1]));
                        source_index += 2;
                        target_index += 0;
                    } else {
                        // The last value in a run of duplicates needs to be the one that ends up in
                        // target.
                        source_index += 1;
                        target_index += 0;
                    }
                } else {
                    source_index += 1;
                    target_index += 1;
                }
            }

            // At this point, source_index and target_index are actually counts.
            // source_index will always be incremented after the final iteration as part of the
            // continue expression.
            // target_index will always be incremented, since either source_index runs out first
            // so value_next_equal is false, or a new value is hit, which will increment it.
            const target_count = target_index;
            assert(target_count <= source_count);
            assert(source_count == source_index);

            if (constants.verify) {
                if (offset < target_count) {
                    for (
                        values[offset .. target_count - 1],
                        values[offset + 1 .. target_count],
                    ) |*value, *value_next| {
                        assert(key_from_value(value) < key_from_value(value_next));
                    }
                }
            }
            return target_count;
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return key_from_value(&a) < key_from_value(&b);
        }

        pub fn key_min(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);

            return key_from_value(&values[0]);
        }

        pub fn key_max(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);

            return key_from_value(&values[values.len - 1]);
        }
    };
}

const TestTable = struct {
    const Key = u32;
    const Value = struct { key: Key, value: u32, tombstone: bool };
    const value_count_max = 16;
    const usage = .general;

    inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }
};

test "table_memory: unit" {
    const testing = std.testing;
    const TableMemory = TableMemoryType(TestTable);

    const allocator = testing.allocator;
    var table_memory: TableMemory = undefined;
    try table_memory.init(allocator, .mutable, "test", .{
        .value_count_limit = TestTable.value_count_max,
    });
    defer table_memory.deinit(allocator);

    table_memory.put(&.{ .key = 1, .value = 1, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 3, .tombstone = false });
    table_memory.put(&.{ .key = 5, .value = 5, .tombstone = false });

    assert(table_memory.count() == 3 and table_memory.value_context.count == 3);
    assert(table_memory.value_context.sorted);

    table_memory.put(&.{ .key = 0, .value = 0, .tombstone = false });
    table_memory.make_immutable(0);

    assert(table_memory.count() == 4 and table_memory.value_context.count == 4);
    assert(table_memory.key_min() == 0);
    assert(table_memory.key_max() == 5);
    assert(table_memory.value_context.sorted);

    // "Flush" and make mutable again
    table_memory.mutability.immutable.flushed = true;

    table_memory.make_mutable();
    assert(table_memory.count() == 0 and table_memory.value_context.count == 0);
    assert(table_memory.value_context.sorted);
    assert(table_memory.mutability == .mutable);
}
