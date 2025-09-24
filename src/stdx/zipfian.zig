//! Zipfian-distributed random number generation.
//!
//! In the Zipfian distribution a small percentage of candidate
//! items have a high probability of being selected, while most items
//! have a very low probability of being selected.
//! It is commonly understood to model the "80-20" Pareto principle,
//! and to be a discreet version of the Pareto distribution,
//! and terminology related to both are often used interchangeably.
//!
//! Zipfian numbers follow an inverse power law, where the 1st item
//! is selected with high probability, and subsequent items
//! quickly fall off in probability. The rate of the fall off
//! is tunable by the _skew_, also called `s`, or `theta`,
//! depending on the source.
//!
//! Reference:
//!
//! - https://en.wikipedia.org/wiki/Zipf's_law#Formal_definition
//!
//! Note that it is not actually possible to select a value for
//! theta that literally follows the "80-20" rule for arbitrary set sizes;
//! the proportion of items that cumulatively make up 80% probability will
//! change as the set grows.
//! A zipfian generator that can adaptively follow the 80-20 rule is left for future work.
//!
//! In practice these probabilities often need to be spread across e.g. a
//! table's keyspace, which involves some kind of mapping step from index to index.
//! Because that mapping is non-trivial to optimize, it is also provided here.
//!
//! The algorithm here is based on
//! "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994.
//! Per the paper it is adapted from Knuth vol 3.
//! This is also the algorithm used by YCSB's ZipfianGenerator.java.
//! Note that the code listing in the paper contains obvious errors,
//! corrected here and in YCSB.
//!
//! There are two generators here,
//! both of which generate random keys from 0 to a specified maximum.
//! In the basic `ZipfianGenerator`, key 0 has the highest probability,
//! 1 the next highest, etc.
//! The `ZipfianShuffled` generator instead spreads the distribution out
//! across the key space as if it were a shuffled deck.
//!
//! The `ZipfianGenerator` allows the key space to grow,
//! but the `ZipfianShuffled` does not - maintaining the illusion of a shuffled
//! deck while growing the keyspace involves tradeoffs in the quality
//! of the distribution. A previous revision of `ZipfianShuffled` _was_ growable,
//! at the cost of not preserving a true Zipfian distribution for the long tail
//! of unlikely items. Dig that out of commit history if it's ever needed.
//!
//! Both should pass a 2-sample Kolmogorov–Smirnov test.

const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const Random = std.Random;
const math = std.math;
const Snap = stdx.Snap;
const module_path = "src/stdx";
const snap = Snap.snap_fn(module_path);

/// The default "skew" of the distribution.
const theta_default = 0.99; // per YCSB

/// Generates Zipfian-distributed numbers from 0 to a specified maximum.
///
/// Many internal variables here are the same is in the paper, which I think
/// should reduce confusion if this subject needs to be revisited; the external
/// intended to be more understandable to the user and follow TigerStyle.
pub const ZipfianGenerator = struct {
    theta: f64,

    /// The number of items in the set.
    n: u64,
    /// The Riemann zeta function calculated up to `n`,
    /// aka the "generalized harmonic number" of order `theta` for `n`.
    /// This is a pre-calculated factor in the probability of any particular item
    /// being selected.
    /// It is expensive to calculate for large but useful values of `n`,
    /// but can be calculated incrementally as `n` grows.
    zetan: f64,

    /// Create a generator from `[0, items)` with `theta` equal to 0.99.
    pub fn init(items: u64) ZipfianGenerator {
        return ZipfianGenerator.init_theta(items, theta_default);
    }

    /// Create a generator from `[0, items)` with given `theta`.
    ///
    /// `theta` is the "skew" and is usually specified to be greater than 0 and less than 1,
    /// with YCSB using 0.99, though values greater than 1 also seem to generate reasonable
    /// distributions. `theta = 1` isn't allowed since it does not behave reasonably.
    pub fn init_theta(items: u64, theta: f64) ZipfianGenerator {
        assert(theta > 0.0);
        assert(theta != 1.0);
        return ZipfianGenerator{
            .theta = theta,
            .n = items,
            .zetan = zeta(items, theta),
        };
    }

    /// Note that the variables in this function are mostly named
    /// as in the reference paper and do not follow TigerStyle.
    pub fn next(self: *const ZipfianGenerator, prng: *stdx.PRNG) u64 {
        assert(self.n > 0);

        // Math voodoo, copied from the paper,
        // which doesn't explain it, but claims it is from Knuth volume 3.

        // NB: These depend only on zetan and could be cached for a minor speedup.
        const alpha = 1.0 / (1.0 - self.theta);
        const eta = (1.0 - math.pow(
            f64,
            2.0 / @as(f64, @floatFromInt(self.n)),
            1.0 - self.theta,
        )) /
            (1.0 - zeta(2.0, self.theta) / self.zetan);

        const u = random_f64(prng);
        const uz = u * self.zetan;

        if (uz < 1.0) {
            return 0;
        }

        if (uz < 1.0 + math.pow(f64, 0.5, self.theta)) {
            return 1;
        }

        return @as(u64, @intFromFloat(
            @as(f64, @floatFromInt(self.n)) *
                math.pow(f64, (eta * u) - eta + 1.0, alpha),
        ));
    }

    /// Grow the size of the random set.
    pub fn grow(self: *ZipfianGenerator, new_items: u64) void {
        const items = self.n + new_items;
        const zetan_new = zeta_incremental(self.n, new_items, self.zetan, self.theta);
        self.* = .{
            .theta = self.theta,
            .n = items,
            .zetan = zetan_new,
        };
    }
};

/// The Riemann zeta function up to `n`,
/// aka the "generalized harmonic number" of order 'theta' for `n`.
fn zeta(n: u64, theta: f64) f64 {
    var i: u64 = 1;
    var zeta_sum: f64 = 0.0;
    while (i <= n) : (i += 1) {
        zeta_sum += math.pow(f64, 1.0 / @as(f64, @floatFromInt(i)), theta);
    }
    return zeta_sum;
}

/// Incremental calculation of zeta.
fn zeta_incremental(
    n_previous: u64,
    n_additional: u64,
    zetan_previous: f64,
    theta: f64,
) f64 {
    const n_new = n_previous + n_additional;
    var i = n_previous + 1;
    var zeta_sum = zetan_previous;
    while (i <= n_new) : (i += 1) {
        zeta_sum += math.pow(f64, 1.0 / @as(f64, @floatFromInt(i)), theta);
    }
    return zeta_sum;
}

/// Generates Zipfian-distributed numbers from 0 to maximum,
/// but the probabilities of each number are "shuffled",
/// not clustered around 0.
///
/// This is used to simulate typical data access patterns in
/// some keyspace, where a few keys are hot and most are cold.
///
/// This behaves as if it maintains a shuffled mapping
/// from every index to a different index. Internally, it is implemented
/// with a bijective "hash" function (modular‑multiplication permutation)
///     f(i) = (a * i) mod N
/// with gcd(a, N) = 1, so every original (Zipfian) index i
/// maps to a unique “shuffled” index without collisions.
/// Refer to PR #3070 for further details: https://github.com/tigerbeetle/tigerbeetle/pull/3070
pub const ZipfianShuffled = struct {
    gen: ZipfianGenerator,
    a: u64,

    pub fn init(items: u64, prng: *stdx.PRNG) ZipfianShuffled {
        return ZipfianShuffled.init_theta(items, theta_default, prng);
    }

    pub fn init_theta(items: u64, theta: f64, prng: *stdx.PRNG) ZipfianShuffled {
        var zipf = ZipfianShuffled{
            .gen = ZipfianGenerator.init_theta(0, theta),
            .a = 0, // Correct a is determined in grow.
        };

        zipf.choose_shuffle_function(items, prng);

        return zipf;
    }

    fn transform(self: *const ZipfianShuffled, zipf_standard: u64) u64 {
        return (zipf_standard * self.a) % self.gen.n;
    }

    pub fn next(self: *const ZipfianShuffled, prng: *stdx.PRNG) u64 {
        const zipf_standard = self.gen.next(prng);
        const zipf_shuffled = self.transform(zipf_standard);
        return zipf_shuffled;
    }

    fn choose_shuffle_function(self: *ZipfianShuffled, new_items: u64, prng: *stdx.PRNG) void {
        if (new_items == 0) {
            return;
        }

        const old_n = self.gen.n;
        const new_n = old_n + new_items;

        self.gen.grow(new_items);

        assert(self.gen.n == new_n);

        // We try to find an `a` so that it satisifies gcd(a,N) == 1.
        // This allows us to generate a permutation with (a*zipf_standard) mod N.
        // This permutation maps one index to another without holes, i.e. is bijective.
        self.a = random_coprime(prng, self.gen.n);
    }

    fn random_coprime(prng: *stdx.PRNG, n: u64) u64 {
        // The bound is arbitrary but should be large enough to find a number that satisifies
        // the requirement (see https://en.wikipedia.org/wiki/Euler%27s_totient_function).
        for (0..100_000) |_| {
            const a = prng.range_inclusive(u64, 1, n);
            if (std.math.gcd(a, n) == 1) {
                return a;
            }
        } else {
            @panic("Did not find a random coprime (probabilistic)");
        }
    }
};

/// stdx.PRNG intentionally doesn't support generating floats, to ensure determinism. For
/// benchmarking purposes, using floats is OK though, so we fall back to std implementation here.
fn random_f64(prng: *stdx.PRNG) f64 {
    return std.Random.init(prng, stdx.PRNG.fill).float(f64);
}

test "zeta_incremental" {
    const Case = struct {
        n_start: u64,
        n_incremental: u64,
        theta: f64,
    };
    const cases = [_]Case{
        .{
            .n_start = 0,
            .n_incremental = 10,
            .theta = 0.99,
        },
        .{
            .n_start = 0,
            .n_incremental = 10,
            .theta = 1.01,
        },
        .{
            .n_start = 100,
            .n_incremental = 100,
            .theta = 0.99,
        },
    };

    for (cases) |case| {
        const n = case.n_start + case.n_incremental;
        const zeta_expected = zeta(n, case.theta);
        const zeta_actual_start = zeta(case.n_start, case.theta);
        const zeta_actual = zeta_incremental(
            case.n_start,
            case.n_incremental,
            zeta_actual_start,
            case.theta,
        );
        assert(zeta_expected == zeta_actual);
    }
}

// Testing that the grow function correctly calculates zeta incrementally.
test "zipfian-grow" {
    // Need to try multiple times to ensure they don't both coincidentally
    // pick the likely 0 value.
    var i: u64 = 10;
    while (i < 100) : (i += 1) {
        const expected = brk: {
            var prng = stdx.PRNG.from_seed(0);
            var zipf = ZipfianGenerator.init_theta(i, 0.9);
            break :brk zipf.next(&prng);
        };
        const actual = brk: {
            var prng = stdx.PRNG.from_seed(0);
            var zipf = ZipfianGenerator.init_theta(1, 0.9);
            zipf.grow(i - 1);
            break :brk zipf.next(&prng);
        };
        assert(expected == actual);
    }
}

// Test that ctors are all doing the same thing.
test "zipfian-ctors" {
    var prng = stdx.PRNG.from_seed(0);

    for ([_]u64{ 0, 1, 10, 999 }) |i| {
        {
            const zipf1 = ZipfianGenerator.init(i);
            const zipf2 = ZipfianGenerator.init_theta(i, theta_default);
            const szipf1 = ZipfianShuffled.init(i, &prng);
            const szipf2 = ZipfianShuffled.init_theta(i, theta_default, &prng);

            assert(zipf1.n == zipf2.n);
            assert(zipf1.n == szipf1.gen.n);
            assert(zipf1.n == szipf2.gen.n);

            assert(zipf1.zetan == zipf2.zetan);
            assert(zipf1.zetan == szipf1.gen.zetan);
            assert(zipf1.zetan == szipf2.gen.zetan);
        }

        {
            const zipf1 = ZipfianGenerator.init_theta(i, 0.89);
            const szipf1 = ZipfianShuffled.init_theta(i, 0.89, &prng);

            assert(zipf1.n == szipf1.gen.n);
            assert(zipf1.zetan == szipf1.gen.zetan);
        }
    }
}

test "zipfian-distribution" {
    const max_number = 10;

    var prng = stdx.PRNG.from_seed(42);
    const zipf = ZipfianGenerator.init(max_number);

    var distribution: [max_number]u32 = @splat(0);

    for (0..1000) |_| {
        const n = zipf.next(&prng);
        distribution[n] += 1;
    }

    try snap(@src(),
        \\{ 333, 170, 125, 90, 59, 61, 43, 47, 38, 34 }
    ).diff_fmt("{d}", .{distribution});
}

test "shuffled-zipfian-distribution" {
    const max_number = 10;

    var prng = stdx.PRNG.from_seed(42);
    const zipf_shuffled = ZipfianShuffled.init(max_number, &prng);

    var distribution: [max_number]u32 = @splat(0);

    for (0..1000) |_| {
        const n = zipf_shuffled.next(&prng);
        distribution[n] += 1;
    }

    try snap(@src(),
        \\{ 333, 34, 38, 47, 43, 61, 60, 89, 125, 170 }
    ).diff_fmt("{d}", .{distribution});
}

// Non-statistical smoke tests related to the shuffled hot items optimization.
// These could fail if that optimization is tweaked or if the prng changes.
// The standard zipf generator is tested, here we test the mapping of the shuffled one.
test "zipfian-shuffled" {
    const max = 100;
    var prng = stdx.PRNG.from_seed(0);
    const allocator = std.testing.allocator;
    var found = try allocator.alloc(bool, max);
    defer allocator.free(found);

    for (1..max) |items| {
        @memset(found, false);
        var zipf = ZipfianShuffled.init(items, &prng);

        for (0..items) |i| {
            const zipf_shuffled = zipf.transform(i);
            try std.testing.expect(!found[zipf_shuffled]);
            found[zipf_shuffled] = true;
        }
    }
}
