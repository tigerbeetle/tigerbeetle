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
    };

    /// A tuple represents either the lower or upper end of a bound, and is fed as input to the
    /// Marzullo algorithm to compute the smallest interval across all tuples.
    /// For example, given a clock offset to a remote replica of 3s, a round trip time of 1s, and
    /// a maximum tolerance between clocks of 100ms on either side, we might create two tuples, the
    /// lower bound having an offset of 2.4s and the upper bound having an offset of 3.6s,
    /// to represent the error introduced by the round trip time and by the clocks themselves.
    pub const Tuple = struct {
        /// An identifier, the index of the clock source in the list of clock sources:
        source: u8,
        offset: i64,
        bound: enum {
            lower,
            upper,
        },
    };

    /// Returns the smallest interval consistent with the largest number of sources.
    pub fn smallest_interval(tuples: []Tuple) Interval {
        // There are two bounds (lower and upper) per source clock offset sample.
        const sources = @as(u8, @intCast(@divExact(tuples.len, 2)));

        if (sources == 0) {
            return Interval{
                .lower_bound = 0,
                .upper_bound = 0,
                .sources_true = 0,
                .sources_false = 0,
            };
        }

        // Use a simpler sort implementation than the complexity of `std.mem.sort()` for safety:
        std.sort.insertion(Tuple, tuples, {}, less_than);

        // Here is a description of the algorithm:
        // https://en.wikipedia.org/wiki/Marzullo%27s_algorithm#Method
        var best: i64 = 0;
        var count: i64 = 0;
        var previous: ?Tuple = null;
        var interval: Interval = undefined;

        for (tuples, 0..) |tuple, i| {
            // Verify that our sort implementation is correct:
            if (previous) |p| {
                assert(p.offset <= tuple.offset);
                if (p.offset == tuple.offset) {
                    if (p.bound != tuple.bound) {
                        assert(p.bound == .lower and tuple.bound == .upper);
                    } else {
                        assert(p.source < tuple.source);
                    }
                }
            }
            previous = tuple;

            // Update the current number of overlapping intervals:
            switch (tuple.bound) {
                .lower => count += 1,
                .upper => count -= 1,
            }
            // The last upper bound tuple will have a count of one less than the lower bound.
            // Therefore, we should never see count >= best for the last tuple:
            if (count > best) {
                best = count;
                interval.lower_bound = tuple.offset;
                interval.upper_bound = tuples[i + 1].offset;
            } else if (count == best and tuples[i + 1].bound == .upper) {
                // This is a tie for best overlap. Both intervals have the same number of sources.
                // We want to choose the smaller of the two intervals:
                const alternative = tuples[i + 1].offset - tuple.offset;
                if (alternative < interval.upper_bound - interval.lower_bound) {
                    interval.lower_bound = tuple.offset;
                    interval.upper_bound = tuples[i + 1].offset;
                }
            }
        }
        assert(previous.?.bound == .upper);

        // The number of false sources (ones which do not overlap the optimal interval) is the
        // number of sources minus the value of `best`:
        assert(best <= sources);
        interval.sources_true = @as(u8, @intCast(best));
        interval.sources_false = @as(u8, @intCast(sources - @as(u8, @intCast(best))));
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
        _ = context;

        if (a.offset < b.offset) return true;
        if (b.offset < a.offset) return false;
        if (a.bound == .lower and b.bound == .upper) return true;
        if (b.bound == .lower and a.bound == .upper) return false;
        // Use the source index to break the tie and ensure the sort is fully specified and stable
        // so that different sort algorithms sort the same way:
        if (a.source < b.source) return true;
        if (b.source < a.source) return false;
        return false;
    }
};

fn test_smallest_interval(bounds: []const i64, smallest_interval: Marzullo.Interval) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var tuples = try allocator.alloc(Marzullo.Tuple, bounds.len);
    for (bounds, 0..) |bound, i| {
        tuples[i] = .{
            .source = @as(u8, @intCast(@divTrunc(i, 2))),
            .offset = bound,
            .bound = if (i % 2 == 0) .lower else .upper,
        };
    }

    var interval = Marzullo.smallest_interval(tuples);
    try std.testing.expectEqual(smallest_interval, interval);
}

test "marzullo" {
    try test_smallest_interval(
        &[_]i64{
            11, 13,
            10, 12,
            8,  12,
        },
        Marzullo.Interval{
            .lower_bound = 11,
            .upper_bound = 12,
            .sources_true = 3,
            .sources_false = 0,
        },
    );

    try test_smallest_interval(
        &[_]i64{
            8,  12,
            11, 13,
            14, 15,
        },
        Marzullo.Interval{
            .lower_bound = 11,
            .upper_bound = 12,
            .sources_true = 2,
            .sources_false = 1,
        },
    );

    try test_smallest_interval(
        &[_]i64{
            -10, 10,
            -1,  1,
            0,   0,
        },
        Marzullo.Interval{
            .lower_bound = 0,
            .upper_bound = 0,
            .sources_true = 3,
            .sources_false = 0,
        },
    );

    // The upper bound of the first interval overlaps inclusively with the lower of the last.
    try test_smallest_interval(
        &[_]i64{
            8,  12,
            10, 11,
            8,  10,
        },
        Marzullo.Interval{
            .lower_bound = 10,
            .upper_bound = 10,
            .sources_true = 3,
            .sources_false = 0,
        },
    );

    // The first smallest interval is selected. The alternative with equal overlap is 10..12.
    // However, while this shares the same number of sources, it is not the smallest interval.
    try test_smallest_interval(
        &[_]i64{
            8,  12,
            10, 12,
            8,  9,
        },
        Marzullo.Interval{
            .lower_bound = 8,
            .upper_bound = 9,
            .sources_true = 2,
            .sources_false = 1,
        },
    );

    // The last smallest interval is selected. The alternative with equal overlap is 7..9.
    // However, while this shares the same number of sources, it is not the smallest interval.
    try test_smallest_interval(
        &[_]i64{
            7,  9,
            7,  12,
            10, 11,
        },
        Marzullo.Interval{
            .lower_bound = 10,
            .upper_bound = 11,
            .sources_true = 2,
            .sources_false = 1,
        },
    );

    // The same idea as the previous test, but with negative offsets.
    try test_smallest_interval(
        &[_]i64{
            -9,  -7,
            -12, -7,
            -11, -10,
        },
        Marzullo.Interval{
            .lower_bound = -11,
            .upper_bound = -10,
            .sources_true = 2,
            .sources_false = 1,
        },
    );

    // A cluster of one with no remote sources.
    try test_smallest_interval(
        &[_]i64{},
        Marzullo.Interval{
            .lower_bound = 0,
            .upper_bound = 0,
            .sources_true = 0,
            .sources_false = 0,
        },
    );

    // A cluster of two with one remote source.
    try test_smallest_interval(
        &[_]i64{
            1, 3,
        },
        Marzullo.Interval{
            .lower_bound = 1,
            .upper_bound = 3,
            .sources_true = 1,
            .sources_false = 0,
        },
    );

    // A cluster of three with agreement.
    try test_smallest_interval(
        &[_]i64{
            1, 3,
            2, 2,
        },
        Marzullo.Interval{
            .lower_bound = 2,
            .upper_bound = 2,
            .sources_true = 2,
            .sources_false = 0,
        },
    );

    // A cluster of three with no agreement, still returns the smallest interval.
    try test_smallest_interval(
        &[_]i64{
            1, 3,
            4, 5,
        },
        Marzullo.Interval{
            .lower_bound = 4,
            .upper_bound = 5,
            .sources_true = 1,
            .sources_false = 1,
        },
    );
}
