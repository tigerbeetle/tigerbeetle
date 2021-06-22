const std = @import("std");
const assert = std.debug.assert;

/// Marzullo's algorithm, invented by Keith Marzullo for his Ph.D. dissertation in 1984, is an
/// agreement algorithm used to select sources for estimating accurate time from a number of noisy
/// time sources. NTP uses a modified form of this called the Intersection algorithm, which returns
/// a larger interval for further statistical sampling. However, here we want the smallest interval.
pub const Marzullo = struct {

    /// The smallest interval consistent with the largest number of sources.
    pub const Interval = struct {
        /// The lower bound on the minimum clock offset.
        lower_bound: i64,

        /// The upper bound on the maximum clock offset.
        upper_bound: i64,

        /// The number of "true chimers" consistent with the largest number of sources.
        sources_true: u8,

        /// The number of "false chimers" falling outside this interval.
        /// Where `sources_false` plus `sources_true` always equals the total number of sources.
        sources_false: u8,

        /// Whether the largest number of sources in this interval also constitutes a majority.
        /// For example, if a replica is a member of a cluster of 5 replicas and samples the clock
        /// offsets of 4 remote replicas (so that `sources=4`), then a majority will consist of at
        /// least 3 of these remote replica sources being in agreement.
        /// This implies that a cluster of 3 replicas requires all 3 replicas to be alive in order
        /// to estimate a majority interval on the cluster time. We cannot have agreement if
        /// 1 replica is down, because there is no tiebreaker between the remaining two replicas.
        pub fn majority(self: *Interval) bool {
            const sources = self.sources_true + self.sources_false;
            return self.sources_true > @divTrunc(sources, 2);
        }
    };

    /// A tuple represents either the lower or upper end of a bound, and is fed as input to the
    /// Marzullo algorithm to compute the smallest interval across all tuples.
    /// For example, given a clock offset to a remote replica of 3s and a round trip time of 1s,
    /// we might create two tuples, the lower bound having an offset of 2.5s and the upper bound
    /// having an offset of 3.5s, to represent the error introduced by the round trip time.
    pub const Tuple = struct {
        source: u8,
        offset: i64,
        bound: enum {
            lower,
            upper,
        },
    };

    /// Returns the smallest interval consistent with the largest number of sources.
    pub fn smallest_interval(tuples: []Tuple) Interval {
        // There are two bounds (lower and upper) per clock offset sample.
        const bounds = 2;

        const sources = @divExact(tuples.len, bounds);
        assert(sources <= std.math.maxInt(u8));

        if (sources == 0) {
            assert(tuples.len == 0);
            return Interval{
                .lower_bound = 0,
                .upper_bound = 0,
                .sources_true = 0,
                .sources_false = 0,
            };
        }

        std.sort.insertionSort(Tuple, tuples, {}, less_than);

        // Double-check that our sort implementation is working correctly:
        // TODO Assert that each source appears at most once, so that we don't double-count sources.
        var last_tuple: ?Tuple = null;
        for (tuples) |b, i| {
            if (last_tuple) |a| {
                assert(a.offset <= b.offset);
                if (a.offset == b.offset and a.source == b.source) {
                    assert(a.bound == .lower and b.bound == .upper);
                }
            }
            last_tuple = b;
        }
        assert(last_tuple.?.bound == .upper);

        // Here is a description of the algorithm:
        // https://en.wikipedia.org/wiki/Marzullo%27s_algorithm#Method
        var best: i64 = 0;
        var count: i64 = 0;

        var interval = Interval{
            .lower_bound = undefined,
            .upper_bound = undefined,
            .sources_true = 0,
            .sources_false = 0,
        };

        for (tuples) |tuple, i| {
            // Update the current number of overlapping intervals:
            count -= switch (tuple.bound) {
                .lower => @as(i64, -1),
                .upper => @as(i64, 1),
            };
            // The last upper bound tuple will have a count of one less than the lower bound.
            // Therefore, we should never see count >= best for the last tuple:
            if (count > best) {
                best = count;
                interval.lower_bound = tuple.offset;
                interval.upper_bound = tuples[i + 1].offset;
            } else if (count == best and tuples[i + 1].bound == .upper) {
                // This is a tie for best overlap. Both intervals have the same number of sources.
                // We want to choose the smaller of the two intervals:
                const alternative = tuples[i + 1].offset - tuples[i].offset;
                if (alternative < interval.upper_bound - interval.lower_bound) {
                    interval.lower_bound = tuples[i].offset;
                    interval.upper_bound = tuples[i + 1].offset;
                }
            }
        }

        // The number of false sources (ones which do not overlap the optimal interval) is the
        // number of sources minus the value of `best`:
        assert(best >= 0);
        assert(best <= sources);
        interval.sources_true = @intCast(u8, best);
        interval.sources_false = @intCast(u8, sources - @intCast(u8, best));
        assert(interval.sources_true + interval.sources_false == sources);

        return interval;
    }

    /// Sorts the list of tuples by clock offset. If two tuples with the same offset but opposite
    /// bounds exist, indicating that one interval ends just as another begins, then a method of
    /// deciding which comes first is necessary. Such an occurrence can be considered an overlap
    /// with no duration, which can be found by the algorithm by sorting the lower bound before the
    /// upper bound. Alternatively, if such pathological overlaps are considered objectionable then
    /// they can be avoided by sorting the upper bound before the lower bound.
    fn less_than(context: void, a: Tuple, b: Tuple) bool {
        if (a.offset < b.offset) return true;
        if (b.offset < a.offset) return false;
        if (a.bound == .lower and b.bound == .upper) return true;
        if (b.bound == .lower and a.bound == .upper) return false;
        if (a.source < b.source) return true;
        if (b.source < a.source) return false;
        return false;
    }
};

fn test_smallest_interval_and_majority(
    bounds: []const i64,
    smallest_interval: Marzullo.Interval,
    majority: bool,
) !void {
    const testing = std.testing;

    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_allocator.deinit();
    const allocator = &arena_allocator.allocator;

    var tuples = try allocator.alloc(Marzullo.Tuple, bounds.len);
    for (bounds) |bound, i| {
        tuples[i] = .{
            .source = @intCast(u8, @divTrunc(i, 2)),
            .offset = bound,
            .bound = if (i % 2 == 0) .lower else .upper,
        };
    }

    var interval = Marzullo.smallest_interval(tuples);
    try testing.expectEqual(smallest_interval, interval);
    try testing.expectEqual(true, interval.majority());
}

test {
    const testing = std.testing;
    const Interval = Marzullo.Interval;
    const Tuple = Marzullo.Tuple;

    try test_smallest_interval_and_majority(
        &[_]i64{
            8,  12,
            11, 13,
            10, 12,
        },
        Interval{
            .lower_bound = 11,
            .upper_bound = 12,
            .sources_true = 3,
            .sources_false = 0,
        },
        true,
    );

    try test_smallest_interval_and_majority(
        &[_]i64{
            8,  12,
            11, 13,
            14, 15,
        },
        Interval{
            .lower_bound = 11,
            .upper_bound = 12,
            .sources_true = 2,
            .sources_false = 1,
        },
        true,
    );

    try test_smallest_interval_and_majority(
        &[_]i64{
            -10, 10,
            -1,  1,
            0,   0,
        },
        Interval{
            .lower_bound = 0,
            .upper_bound = 0,
            .sources_true = 3,
            .sources_false = 0,
        },
        true,
    );

    // The upper bound of the first interval overlaps inclusively with the lower of the last.
    try test_smallest_interval_and_majority(
        &[_]i64{
            8,  10,
            8,  12,
            10, 11,
        },
        Interval{
            .lower_bound = 10,
            .upper_bound = 10,
            .sources_true = 3,
            .sources_false = 0,
        },
        true,
    );

    // The first smallest interval is selected. The alternative with equal overlap is 10..12.
    // However, while this shares the same number of sources, it is not the smallest interval.
    try test_smallest_interval_and_majority(
        &[_]i64{
            8,  9,
            8,  12,
            10, 12,
        },
        Interval{
            .lower_bound = 8,
            .upper_bound = 9,
            .sources_true = 2,
            .sources_false = 1,
        },
        true,
    );

    // The last smallest interval is selected. The alternative with equal overlap is 7..9.
    // However, while this shares the same number of sources, it is not the smallest interval.
    try test_smallest_interval_and_majority(
        &[_]i64{
            7,  9,
            7,  12,
            10, 11,
        },
        Interval{
            .lower_bound = 10,
            .upper_bound = 11,
            .sources_true = 2,
            .sources_false = 1,
        },
        true,
    );

    // The same idea as the previous test, but with negative offsets.
    try test_smallest_interval_and_majority(
        &[_]i64{
            -9,  -7,
            -12, -7,
            -11, -10,
        },
        Interval{
            .lower_bound = -11,
            .upper_bound = -10,
            .sources_true = 2,
            .sources_false = 1,
        },
        true,
    );
}
