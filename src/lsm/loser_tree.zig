const std = @import("std");
const assert = std.debug.assert;

pub const SortedRun = struct {
    min: u32, // inclusive
    max: u32, // exclusive
};

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
        const prefetch_distance = (values_per_cache_line) * 2 + 1;

        // Preallocate the tree structure.
        const height = std.math.log2(K);
        const inner_count = 2 * (K / 2) - 1;

        tree: [inner_count]Node align(64) = undefined,
        data: []Value,
        batch_views: []SortedRun,
        winner: Node,

        // should we do slices?
        pub inline fn merge(input: []Value, batches: []SortedRun, output: []Value) void {
            assert(output.len == input.len);
            // TODO: check that input slices are combined <- output.len
            // TODO: check if batches are actually sorted
            // TODO: check if all values are sorted

            // populate_tree or make_tree
            var tree = init(input, batches);

            // TODO: can we just deduplicate here?
            // check if the next value is equal then the last.
            // if it is primary overwrite otherwise annihilate both
            for (output) |*out| {
                out.* = tree.next();
            }
        }

        fn init(data: []Value, batches: []SortedRun) Self {

            // Prepopulate the competing nodes from every valid sorted batch.
            var competitors: [K]Node = undefined;
            for (0..K) |batch_id| {
                // If the batch is empty, use a sentinel value to mark it as invalid.
                // The sentinel is `maxInt(Key)`, which is safe because the batch_id serves as a tiebreaker.
                // This ensures that sentinel values are never propagated to the final merge.
                if (batches[batch_id].min >= batches[batch_id].max) {
                    competitors[batch_id] = .{
                        .key = std.math.maxInt(Key),
                        .batch_id = std.math.maxInt(usize),
                    };
                } else {
                    competitors[batch_id] = .{
                        .key = key_from_value(&data[batches[batch_id].min]),
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
            self.batch_views[batch_id].min += 1;
            const current_pos = self.batch_views[batch_id].min;

            // If the current position is out of bounds, set the winner to a sentinel value.
            // The sentinel marks the batch as exhausted and ensures it is not chosen again.
            self.winner = if (current_pos < self.batch_views[batch_id].max) .{ .key = key_from_value(&self.data[current_pos]), .batch_id = batch_id } else .{ .key = std.math.maxInt(Key), .batch_id = std.math.maxInt(usize) };
            var parent_id = (inner_count + batch_id);

            for (0..height) |_| {
                parent_id = (parent_id - 1) / 2;
                const parent_ptr = &self.tree[parent_id];

                // Determine if the current winner is larger than the node in the tree.
                // If so, the node in the tree becomes the new winner, and the current winner is stored as the loser.
                const is_larger = self.winner.key > parent_ptr.key;
                // Using `batch_id` as a tie breaker by preferring the key with the smaller batch_id.
                // This ensures stability when keys are equal.
                const tie_breaker = self.winner.key == parent_ptr.key and self.winner.batch_id > parent_ptr.batch_id;
                const has_new_winner = is_larger or tie_breaker;

                // Use branchless code to select the new winner.
                // This avoids branch mispredictions and improves performance.
                // TOOD: Maybe use @branchHint in zig 0.14 to write this more concise
                const winner_ptr: *Node = if (has_new_winner) parent_ptr else &self.winner;

                // Manually swap the winner and loser to avoid inefficiencies in `std.mem.swap`.
                // This is a performance optimization for the specific struct layout.
                // TODO: Potentially revise this in zig 0.14.
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
