const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;

const composite_key = @import("composite_key.zig");

const BatchSlice = struct {
    start: usize,
    end: usize,
};

// TODO: replace with constants.cache_line_bytes
// TODO handle composite_key better
const cache_line_bytes = 64;

pub fn LooserTreeType(
    comptime Key: type,
    comptime Value: type,
    comptime K: usize,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
) type {
    // Tree of loosers is binary tree, so requires K to be power two.
    assert(std.math.isPowerOfTwo(K));

    return struct {
        const Self = @This();

        const member_size = @sizeOf(Key) + @sizeOf(usize);
        const padding_bytes = if (member_size % 64 == 0) 0 else cache_line_bytes - (member_size % cache_line_bytes);
        const Node = struct {
            key: Key,
            batch_id: usize,
            padding: [padding_bytes]u8 = [_]u8{0} ** padding_bytes,
        };

        comptime {
            assert(@sizeOf(Node) % cache_line_bytes == 0);
        }

        // Prefetch two cache lines ahead to mitigate cache misses.
        // The loser tree merges from K sorted batches.
        // Although the access pattern is relatively predictable (cycling through K batches), the hardware prefetcher
        // struggles to recognize it due to the interleaved reads from different batches.
        // By prefetching explicitly, we ensure the necessary cache lines are loaded in advance.
        // As always prefetch distance is based on empirical results.
        const values_per_cache_line = cache_line_bytes / @sizeOf(Value);
        const prefetch_distance = (values_per_cache_line + 1) * 2;

        // Preallocate the tree structure.
        const height = std.math.log2(K);
        const inner_count = 2 * (K / 2) - 1;

        tree: [inner_count]Node align(64) = undefined,
        data: []Value,
        batch_views: []BatchSlice,
        winner: Node,

        // should we do slices?
        pub inline fn merge(input: []Value, batches: []BatchSlice, output: []Value) void {
            //assert(output.len == input.len);
            // TODO: check that input slices are combined <- output.len
            // TODO: check if batches are actually sorted
            // TODO: check if all values are sorted

            // populate_tree or make_tree
            // TODO: need to be able to remember state but batch slices are updated.
            var tree = init(input, batches);

            for (output) |*out| {
                out.* = tree.next();
            }
        }

        fn init(data: []Value, batches: []BatchSlice) Self {

            // Prepopulate the competing nodes from every valid sorted batch.
            var competitors: [K]Node = undefined;
            for (0..K) |batch_id| {
                // If the batch is empty, use a sentinel value to mark it as invalid.
                // The sentinel is `maxInt(Key)`, which is safe because the batch_id serves as a tiebreaker.
                // This ensures that sentinel values are never propagated to the final merge.
                if (batches[batch_id].start >= batches[batch_id].end) {
                    competitors[batch_id] = .{
                        .key = std.math.maxInt(Key),
                        .batch_id = K,
                    };
                } else {
                    competitors[batch_id] = .{
                        .key = key_from_value(&data[batches[batch_id].start]),
                        .batch_id = batch_id,
                    };
                }
            }

            // Populate the tree of losers with the competitors.
            // The tree is represented as a flat array for memory efficiency and ease of traversal.
            // We start with the first inner level above the leaves and work our way up.
            var tree: [inner_count]Node = undefined;
            inline for (1..height + 1) |level| {
                // Calculate the begin and end indexes (inclusive) of the current level in the tree.
                const begin = (K / (1 << level)) - 1;
                const end = (K / (1 << (level - 1))) - 1;
                for (begin..end, 0..) |tree_index, competitor_index| {
                    // Compare pairs of competitors at the current level.
                    // The winner (smaller key) is retained in the `competitors` array to proceed to the next level.
                    // The loser (larger key) is stored in the `tree` array at the current level.
                    // This process continues until only one winner remains in the `competitors` array.
                    const left_competitor = competitors[competitor_index * 2];
                    const right_competitor = competitors[competitor_index * 2 + 1];
                    if (left_competitor.key < right_competitor.key) {
                        competitors[competitor_index] = left_competitor;
                        tree[tree_index] = right_competitor;
                    } else {
                        competitors[competitor_index] = right_competitor;
                        tree[tree_index] = left_competitor;
                    }
                }
            }

            return .{
                .tree = tree,
                .data = data,
                .batch_views = batches,
                .winner = competitors[0],
            };
        }

        fn next(self: *Self) Value {
            const batch_id = self.winner.batch_id;

            // Increment the start position for the winning batch.
            self.batch_views[batch_id].start += 1;
            const current_pos = self.batch_views[batch_id].start;

            // If the current position is out of bounds, set the winner to a sentinel value.
            // The sentinel marks the batch as exhausted and ensures it is not chosen again.
            // TODO: potential_winner
            self.winner = if (current_pos < self.batch_views[batch_id].end) .{ .key = key_from_value(&self.data[current_pos]), .batch_id = batch_id } else .{ .key = std.math.maxInt(Key), .batch_id = K };
            // TODO: hoist the first calculation of the parent_id out of the loop to actually be the parent id here instead of leaf id
            var parent_id = (self.tree.len + batch_id);

            inline for (0..height) |_| {
                parent_id = (parent_id - 1) / 2;
                // Determine if the current winner is larger than the node in the tree.
                // If so, the node in the tree becomes the new winner, and the current winner is stored as the loser.
                const is_larger = self.winner.key > self.tree[parent_id].key;
                // Using `batch_id` as a tie breaker by preferring the key with the smaller batch_id.
                // This ensures stability when keys are equal.
                const tie_breaker = self.winner.key == self.tree[parent_id].key and self.winner.batch_id > self.tree[parent_id].batch_id;
                const has_new_winner = is_larger or tie_breaker;

                // Use branchless code to select the new winner.
                // This avoids branch mispredictions and improves performance.
                // TOOD: Maybe use @branchHint in zig 0.14 to write this more concise
                const winner_ptr: *Node = if (has_new_winner) &self.tree[parent_id] else &self.winner;

                // Manually swap the winner and loser to avoid inefficiencies in `std.mem.swap`.
                // This is a performance optimization for the specific struct layout.
                // TODO: Potentially revise this in zig 0.14.
                // TODO: own_memswap
                const tmp_batch_id = winner_ptr.batch_id;
                const tmp_key = winner_ptr.key;
                winner_ptr.batch_id = self.winner.batch_id;
                winner_ptr.key = self.winner.key;
                self.winner.key = tmp_key;
                self.winner.batch_id = tmp_batch_id;
            }

            // The hardware prefetcher cannot predict this access pattern, so we help it.
            // The pointer arithmetic might be "out of bounds", but since it is never dereferenced it is fine (see binary search)
            @prefetch(
                self.data.ptr + current_pos + prefetch_distance,
                .{ .rw = .read, .locality = 3, .cache = .data },
            );
            // Return the value corresponding to the last winner.
            return self.data[current_pos - 1];
        }
    };
}

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    const LooserTree = LooserTreeType(Key, Value, constants.lsm_compaction_ops, key_from_value);

    return struct {
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: u32 = 0,
            /// When true, `values` is strictly ascending-ordered (no duplicates).
            sorted: bool = true,
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

        values: []Value,
        values_scratch: []Value, // scratch values for immutable table only ugly hack.
        values_scratch_count: usize = 0,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        batch_slices: [constants.lsm_compaction_ops]BatchSlice,
        batch_slice_index: usize = 0,
        batch_start: usize = 0,

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
                .name = name,

                .batch_slices = [_]BatchSlice{.{ .start = 0, .end = 0 }} ** constants.lsm_compaction_ops,
                .batch_start = 0,
                .batch_slice_index = 0,
                // Allocated later
                .values = undefined,
                .values_scratch = undefined,
                .values_scratch_count = 0,
            };

            // TODO This would ideally be value_count_limit, but needs to be value_count_max to
            // ensure that memory table coalescing is deterministic even if the batch limit changes.
            table.values = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.values);

            table.values_scratch = try allocator.alloc(Value, Table.value_count_max);
            errdefer allocator.free(table.values_scratch);
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            allocator.free(table.values);
        }

        pub fn reset(table: *TableMemory) void {
            const mutability: Mutability = switch (table.mutability) {
                .immutable => .{ .immutable = .{} },
                .mutable => .{ .mutable = .{} },
            };

            table.* = .{
                .values = table.values,
                .values_scratch = table.values_scratch,
                .value_context = .{},
                .mutability = mutability,
                .batch_slices = [_]BatchSlice{.{ .start = 0, .end = 0 }} ** constants.lsm_compaction_ops,
                .batch_start = 0,
                .batch_slice_index = 0,
                .name = table.name,
                .values_scratch_count = 0,
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

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count <= table.values.len);
            //defer assert(table.value_context.sorted);

            // TODO: This is the final sort, can we make delay this without problems?
            //       i.e. make this only immutable on the second bar.
            //       and make the final sort more incremental
            //table.sort();

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
                .values = table.values,
                .values_scratch = table.values_scratch,
                .value_context = .{},
                .mutability = .{ .mutable = .{} },
                .batch_slices = [_]BatchSlice{.{ .start = 0, .end = 0 }} ** constants.lsm_compaction_ops,
                .batch_slice_index = 0,
                .values_scratch_count = 0,
                .batch_start = 0,
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
            //assert(table_immutable.value_context.sorted);
            assert(table_mutable.mutability == .mutable);
            maybe(table_mutable.value_context.sorted);

            const values_count_limit = table_immutable.values.len;
            assert(values_count_limit == values_count_limit);
            assert(table_immutable.count() <= values_count_limit);
            assert(table_mutable.count() <= values_count_limit);
            assert(table_immutable.count() + table_mutable.count() <= values_count_limit);

            // count how many full partitions the mutable table has.
            // if it is full we need to merge and sort two to make room for the immutable part.

            var mutable_parition_count: u32 = 0;

            for (table_mutable.batch_slices) |slice| {
                mutable_parition_count += if (slice.start == slice.end) 0 else 1;
            }

            if (mutable_parition_count == constants.lsm_compaction_ops) {
                // merge last two to preserve the other offsets.
                const min = table_mutable.batch_slices[constants.lsm_compaction_ops - 2].start;
                const max = table_mutable.batch_slices[constants.lsm_compaction_ops - 1].end;
                const element_count = sort_suffix_from_offset(table_mutable.values[min..max], 0);
                table_mutable.batch_slices[constants.lsm_compaction_ops - 2].end = min + element_count;
                table_mutable.value_context.count = @as(u32, @intCast(min)) + element_count;
                // reset last index
                table_mutable.batch_slices[constants.lsm_compaction_ops - 1] = .{
                    .start = 0,
                    .end = 0,
                };
            }

            // Perform the copy here
            stdx.copy_disjoint(
                .inexact,
                Value,
                table_immutable.values[table_immutable.count()..],
                table_mutable.values[0..table_mutable.count()], // this should be the new length!
            );

            const tables_combined_count = table_immutable.count() + table_mutable.count();

            table_immutable.batch_slices = [_]BatchSlice{.{ .start = 0, .end = 0 }} ** constants.lsm_compaction_ops;

            table_immutable.batch_slices[0] = .{
                .start = 0,
                .end = table_immutable.count(),
            };
            // copy over the entries now from begin to end -1

            for (table_mutable.batch_slices[0 .. constants.lsm_compaction_ops - 1], 1..) |slice, i| {
                const offset = @as(usize, @intCast(table_immutable.count()));
                table_immutable.batch_slices[i] = .{
                    .start = slice.start + offset,
                    .end = slice.end + offset,
                };
            }

            table_immutable.value_context.count = tables_combined_count;

            // We now that the first prefix must be from 0 - table_immutable.count right?
            // then we merge the other prefixes in or we only sort the new data?

            assert(table_immutable.count() <= tables_combined_count);

            table_mutable.reset();
            table_immutable.mutability = .{ .immutable = .{
                .flushed = table_immutable.value_context.count == 0,
                .absorbed = true,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn incremental_merge(table: *TableMemory, step: u64) void {
            assert(table.mutability == .immutable);

            // The idea is to make it incremental
            // pass in the values scratch + block_size
            // batch_slices should be updated still
            // input is also fine

            // TODO verify in forrest
            const merge_steps = @divExact(constants.lsm_compaction_ops, 2);
            const last_merge = (step == @divExact(constants.lsm_compaction_ops, 2) - 1);

            // here we take input.
            const table_count = table.count();
            const block_size = table_count / merge_steps;
            const begin = step * block_size;
            var end = begin + block_size;
            if (last_merge) {
                end = table_count;
            }

            LooserTree.merge(table.values[0..table_count], &table.batch_slices, table.values_scratch[begin..end]);

            if (last_merge) {
                // deduplicate
                const target_count = table.swap_and_deduplicate();
                table.value_context.count = target_count;
                table.value_context.sorted = true;
            }
        }

        pub fn sort(table: *TableMemory) void {
            if (table.mutability == .immutable) {
                unreachable;
                // Here we simply use the new function
                // This is the final sort
                //const target_count = merge_looser_tree(table);
                //const target_count = sort_suffix_from_offset(table.values_used(), 0);
                //table.value_context.count = target_count;
                //table.value_context.sorted = true;
                //return;
            }

            assert(table.mutability == .mutable);

            if (!table.value_context.sorted) {
                table.mutable_sort_suffix_from_offset(0);
                table.value_context.sorted = true;
            }
        }

        pub fn sort_suffix(table: *TableMemory) void {
            assert(table.mutability == .mutable);
            assert(table.mutability.mutable.suffix_offset <= table.count());

            const batch_slice_id = table.batch_slice_index;
            // check if before sorted
            table.mutable_sort_suffix_from_offset(table.mutability.mutable.suffix_offset);
            // just do prefix
            // set after to table.count
            table.batch_slices[batch_slice_id].start = table.batch_start;
            table.batch_slices[batch_slice_id].end = table.count();
            table.batch_start = table.count();
            table.batch_slice_index += 1;

            assert(table.mutability.mutable.suffix_offset == table.count());
        }

        fn mutable_sort_suffix_from_offset(table: *TableMemory, offset: u32) void {
            assert(table.mutability == .mutable);
            assert(offset == table.mutability.mutable.suffix_offset or offset == 0);
            assert(offset <= table.count());

            const target_count = sort_suffix_from_offset(table.values_used(), offset);
            table.value_context.count = target_count;
            table.mutability = .{ .mutable = .{ .suffix_offset = target_count } };
        }

        /// Returns the new length of `values`. (Values are deduplicated after sorting, so the
        /// returned count may be less than or equal to the original `values.len`.)
        fn swap_and_deduplicate(table: *TableMemory) u32 {
            assert(table.mutability == .immutable);

            const sorted_values = table.values_scratch;
            table.values_scratch = table.values;
            table.values = sorted_values;

            const source_count: u32 = table.count(); //@intCast(table.count());
            var source_index: u32 = 0;
            var target_index: u32 = 0;
            while (source_index < source_count) {
                // Minor optimization for deduplication
                if (source_index != target_index) {
                    table.values[target_index] = table.values[source_index];
                }
                //const value = table.values[source_index];
                //table.values[target_index] = value;

                // If we're at the end of the source, there is no next value, so the next value
                // can't be equal.
                const value_next_equal = source_index + 1 < source_count and
                    key_from_value(&table.values[source_index]) ==
                    key_from_value(&table.values[source_index + 1]);

                if (value_next_equal) {
                    if (Table.usage == .secondary_index) {
                        // Secondary index optimization --- cancel out put and remove.
                        // NB: while this prevents redundant tombstones from getting to disk, we
                        // still spend some extra CPU work to sort the entries in memory. Ideally,
                        // we annihilate tombstones immediately, before sorting, but that's tricky
                        // to do with scopes.
                        assert(Table.tombstone(&table.values[source_index]) !=
                            Table.tombstone(&table.values[source_index + 1]));
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
                if (0 < target_count) {
                    for (
                        table.values[0 .. target_count - 1],
                        table.values[1..target_count],
                    ) |*value, *value_next| {
                        assert(key_from_value(value) < key_from_value(value_next));
                    }
                }
            }
            return target_count;
        }

        /// Returns the new length of `values`. (Values are deduplicated after sorting, so the
        /// returned count may be less than or equal to the original `values.len`.)
        fn sort_suffix_from_offset(values: []Value, offset: u32) u32 {
            assert(offset <= values.len);

            std.mem.sort(Value, values[offset..], {}, sort_values_by_key_in_ascending_order);

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
