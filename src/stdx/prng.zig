//! TigerBeetle standard Pseudo Random Number generator.
//!
//! Import qualified and use `prng` for field/variable name:
//!
//! ```
//! prng: *stdx.PRNG
//! ```
//!
//! The implementation matches Zig's `std.Random.DefaultPrng`, but we avoid using that directly in
//! order to:
//! - remove floating point from the API, to ensure determinism
//! - isolate our test suite from stdlib API churn
//! - isolate TigerBeetle from the churn in the PRNG algorithms
//! - simplify and extend the API
//! - remove dynamic-dispatch idirection (a minor bonus).

const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const Snap = @import("../testing/snaptest.zig").Snap;
const snap = Snap.snap;

s: [4]u64,

const PRNG = @This();

/// A less than one rational number, used to specify probabilities.
pub const Ratio = struct {
    // Invariant: numerator ≤ denomitator.
    numerator: u64,
    // Invariant: denomitator ≠ 0.
    denominator: u64,

    pub fn format(
        r: Ratio,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        return writer.print("{d}/{d}", .{ r.numerator, r.denominator });
    }
};

/// Canonical constructor for Ratio. Import as `const ratio = stdx.PRNG.ratio`.
pub fn ratio(numerator: u64, denominator: u64) Ratio {
    assert(denominator > 0);
    assert(numerator <= denominator);
    return .{ .numerator = numerator, .denominator = denominator };
}

pub fn from_seed(seed: u64) PRNG {
    var s = seed;
    return .{ .s = .{
        split_mix_64(&s),
        split_mix_64(&s),
        split_mix_64(&s),
        split_mix_64(&s),
    } };
}

fn split_mix_64(s: *u64) u64 {
    s.* +%= 0x9e3779b97f4a7c15;

    var z = s.*;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

fn next(prng: *PRNG) u64 {
    const r = std.math.rotl(u64, prng.s[0] +% prng.s[3], 23) +% prng.s[0];

    const t = prng.s[1] << 17;

    prng.s[2] ^= prng.s[0];
    prng.s[3] ^= prng.s[1];
    prng.s[1] ^= prng.s[2];
    prng.s[0] ^= prng.s[3];

    prng.s[2] ^= t;

    prng.s[3] = math.rotl(u64, prng.s[3], 45);

    return r;
}

test next {
    var prng = from_seed(92);
    var distribution: [8]u32 = .{0} ** 8;
    for (0..1000) |_| {
        distribution[prng.next() % 8] += 1;
    }
    try snap(@src(),
        \\{ 134, 134, 117, 121, 117, 128, 131, 118 }
    ).diff_fmt("{d}", .{distribution});
}

pub fn fill(prng: *PRNG, target: []u8) void {
    var i: usize = 0;
    const aligned_len = target.len - (target.len & 7);

    // Complete 8 byte segments.
    while (i < aligned_len) : (i += 8) {
        var n = prng.next();
        comptime var j: usize = 0;
        inline while (j < 8) : (j += 1) {
            target[i + j] = @as(u8, @truncate(n));
            n >>= 8;
        }
    }

    // Remaining (cuts the stream).
    if (i != target.len) {
        var n = prng.next();
        while (i < target.len) : (i += 1) {
            target[i] = @as(u8, @truncate(n));
            n >>= 8;
        }
    }
}

test fill {
    const size_max = 128;
    var buffer_max: [size_max]u8 = undefined;
    var prng = from_seed(32);

    var distribution: [8]u32 = .{0} ** 8;
    for (0..size_max + 1) |size| {
        // Check that the entire buffer is filled, by filling it over a couple of times
        // and checking that each byte is non-zero at least once.
        var non_zero = std.StaticBitSet(size_max).initEmpty();
        for (0..3) |_| {
            const buffer = buffer_max[0..size];
            @memset(buffer, 0);
            prng.fill(buffer);
            for (buffer, 0..) |byte, i| {
                distribution[byte % 8] += 1;
                if (byte != 0) non_zero.set(i);
            }
        }
        for (0..size) |i| assert(non_zero.isSet(i));
    }

    try snap(@src(),
        \\{ 3120, 3084, 3089, 3103, 3092, 3120, 3074, 3086 }
    ).diff_fmt("{d}", .{distribution});
}

/// Genrate an unbiased, uniformly distributed integer r such that 0 ≤ r ≤ max.
///
/// No biased version is provided --- while biased generation is simpler&faster, the bias can be
/// quite high depending on max!
pub fn int_inclusive(prng: *PRNG, T: anytype, max: T) T {
    comptime assert(@typeInfo(T).Int.signedness == .unsigned);
    if (max == std.math.maxInt(T)) {
        return prng.int(T);
    }

    comptime assert(@typeInfo(T).Int.signedness == .unsigned);
    const bits = @typeInfo(T).Int.bits;
    const less_than = max + 1;

    // adapted from:
    //   http://www.pcg-random.org/posts/bounded-rands.html
    //   "Lemire's (with an extra tweak from Zig)"
    var x = prng.int(T);
    var m = math.mulWide(T, x, less_than);
    var l: T = @truncate(m);
    if (l < less_than) {
        var t = -%less_than;

        if (t >= less_than) {
            t -= less_than;
            if (t >= less_than) {
                t %= less_than;
            }
        }
        while (l < t) {
            x = prng.int(T);
            m = math.mulWide(T, x, less_than);
            l = @truncate(m);
        }
    }
    return @intCast(m >> bits);
}

test int_inclusive {
    var prng = from_seed(92);
    for (0..8) |max_usize| {
        const max: u8 = @intCast(max_usize);
        var distribution: [8]u32 = .{0} ** 8;
        for (0..100) |_| {
            distribution[prng.int_inclusive(u8, max)] += 1;
        }
        for (distribution[0 .. max + 1]) |d| assert(d > 0);
        for (distribution[max + 1 ..]) |d| assert(d == 0);
    }

    var distribution: [8]u32 = .{0} ** 8;
    for (0..1000) |_| {
        const n = prng.int_inclusive(u128, 7);
        distribution[@intCast(n)] += 1;
    }
    try snap(@src(),
        \\{ 123, 127, 115, 125, 125, 139, 111, 135 }
    ).diff_fmt("{d}", .{distribution});

    var large: u32 = 0;
    var small: u32 = 0;
    for (0..1000) |_| {
        if (prng.int_inclusive(u64, math.maxInt(u64) / 2) > math.maxInt(u64) / 4) {
            large += 1;
        } else {
            small += 1;
        }
    }
    try snap(@src(),
        \\large=506 small=494
    ).diff_fmt("large={} small={}", .{ large, small });
}

// Deliberately excluded from the API to normalize everything to closed ranges.
// Somewhat surprisingly, closed ranges are more conveinient for generating random numbers:
// - passing zero is not a subtle error
// - passing intMax allows generating any integer
// - at the call-site, inclusive is usually somewhat more obvious.
pub const int_exclusive = @compileError("intentionally not implemented");

/// Given a slice, generates a random valid index for the slice.
pub fn index(prng: *PRNG, slice: anytype) usize {
    assert(slice.len > 0);
    return prng.int_inclusive(usize, slice.len - 1);
}

test index {
    var prng = from_seed(92);

    var distribution: [8]u32 = .{0} ** 8;
    for (0..100) |_| {
        distribution[index(&prng, &distribution)] += 1;
    }
    try snap(@src(),
        \\{ 9, 13, 13, 11, 10, 16, 16, 12 }
    ).diff_fmt("{d}", .{distribution});
}

/// Generates a uniform, unbiased integer r such that max ≤ r ≤ max.
pub fn range_inclusive(prng: *PRNG, T: type, min: T, max: T) T {
    comptime assert(@typeInfo(T).Int.signedness == .unsigned);
    assert(min <= max);
    return min + prng.int_inclusive(T, max - min);
}

test range_inclusive {
    var prng = from_seed(92);
    for (0..8) |min| {
        for (min..8) |max| {
            var distribution: [8]u32 = .{0} ** 8;
            for (0..100) |_| {
                distribution[prng.range_inclusive(usize, min, max)] += 1;
            }
            for (distribution, 0..) |d, i| {
                assert((d > 0) == (min <= i and i <= max));
            }
        }
    }
}

/// Returns a uniformly distributed integer of type T.
///
/// That is, fills @sizeOf(T) bytes with random bits.
pub fn int(prng: *PRNG, T: anytype) T {
    comptime assert(@typeInfo(T).Int.signedness == .unsigned);
    if (T == u64) return prng.next();
    if (@sizeOf(T) < @sizeOf(u64)) return @truncate(prng.next());
    var result: T = undefined;
    prng.fill(std.mem.asBytes(&result));
    return result;
}

test int {
    try test_bytes_T(u8, snap(@src(),
        \\{ 134, 134, 117, 121, 117, 128, 131, 118 }
    ));
    try test_bytes_T(u64, snap(@src(),
        \\{ 134, 134, 117, 121, 117, 128, 131, 118 }
    ));
    try test_bytes_T(u128, snap(@src(),
        \\{ 130, 143, 107, 135, 111, 119, 132, 123 }
    ));
}

fn test_bytes_T(T: type, want: Snap) !void {
    var prng = PRNG.from_seed(92);
    var distribution: [8]u32 = .{0} ** 8;
    for (0..1000) |_| {
        distribution[@intCast(prng.int(T) % 8)] += 1;
    }
    try want.diff_fmt("{d}", .{distribution});
}

/// Returns true with probaility 0.5.
pub fn boolean(prng: *PRNG) bool {
    return prng.next() & 1 == 1;
}

test boolean {
    var prng = PRNG.from_seed(92);
    var heads: u32 = 0;
    var tails: u32 = 0;
    for (0..1000) |_| {
        if (prng.boolean()) heads += 1 else tails += 1;
    }
    try snap(@src(),
        \\heads = 501 tails = 499
    ).diff_fmt("heads = {} tails = {}", .{ heads, tails });
}

/// Returns true with the given rational probability.
pub fn chance(prng: *PRNG, probability: Ratio) bool {
    assert(probability.denominator > 0);
    assert(probability.numerator <= probability.denominator);
    return prng.int_inclusive(u64, probability.denominator - 1) < probability.numerator;
}

test chance {
    var prng = PRNG.from_seed(92);
    var balance: i32 = 0;
    for (0..1000) |_| {
        if (prng.chance(ratio(2, 7))) balance += 1 else balance -= 1;
        if (prng.chance(ratio(5, 7))) balance += 1 else balance -= 1;
    }
    try snap(@src(),
        \\balance = 46
    ).diff_fmt("balance = {d}", .{balance});
}

/// Returns a random value of an enum.
pub fn enum_uniform(prng: *PRNG, Enum: type) Enum {
    const values = std.enums.values(Enum);
    return values[prng.index(values)];
}

test enum_uniform {
    const E = enum(u8) { a, b, c = 8 }; // 8 tests that the discriminant is used properly.

    var prng = from_seed(92);
    var count: struct { a: u32 = 0, b: u32 = 0, c: u32 = 0 } = .{};
    for (0..1000) |_| {
        switch (prng.enum_uniform(E)) {
            inline else => |tag| @field(count, @tagName(tag)) += 1,
        }
    }

    try snap(@src(),
        \\a=318 b=323 c=359
    ).diff_fmt("a={} b={} c={}", .{ count.a, count.b, count.c });
}

pub fn EnumWeightsType(E: type) type {
    return std.enums.EnumFieldStruct(E, u64, null);
}

/// Returns a random value of an enum, where probability is proportional to weight.
pub fn enum_weighted(prng: *PRNG, Enum: type, weights: EnumWeightsType(Enum)) Enum {
    const fields = @typeInfo(Enum).Enum.fields;
    var total: u64 = 0;
    inline for (fields) |field| {
        total += @field(weights, field.name);
    }
    assert(total > 0);
    var pick = prng.int_inclusive(u64, total - 1);
    inline for (fields) |field| {
        const weight = @field(weights, field.name);
        if (pick < weight) return @as(Enum, @enumFromInt(field.value));
        pick -= weight;
    }
    unreachable;
}

test enum_weighted {
    const E = enum(u8) { a, b, c = 8 }; // 8 tests that the discriminant is used properly.

    var prng = from_seed(92);
    var count: struct { a: u32 = 0, b: u32 = 0, c: u32 = 0 } = .{};
    for (0..1000) |_| {
        switch (prng.enum_weighted(E, .{ .a = 0, .b = 1, .c = 2 })) {
            inline else => |tag| @field(count, @tagName(tag)) += 1,
        }
    }

    try snap(@src(),
        \\a=0 b=318 c=682
    ).diff_fmt("a={} b={} c={}", .{ count.a, count.b, count.c });
}

/// An iterator-style API for selecting a random combination of elements.
pub const Combination = struct {
    total: u32,
    sample: u32,

    taken: u32,
    seen: u32,

    pub fn init(options: struct { total: u32, sample: u32 }) Combination {
        assert(options.sample <= options.total);
        return .{
            .total = options.total,
            .sample = options.sample,
            .taken = 0,
            .seen = 0,
        };
    }

    pub fn done(combination: *const Combination) bool {
        return combination.taken == combination.sample and
            combination.seen == combination.total;
    }

    pub fn take(combination: *Combination, prng: *PRNG) bool {
        assert(combination.seen < combination.total);
        assert(combination.taken <= combination.sample);

        const n = combination.total - combination.seen;
        const k = combination.sample - combination.taken;
        const result = prng.chance(ratio(k, n));

        combination.seen += 1;
        if (result) combination.taken += 1;
        return result;
    }
};

test Combination {
    var prng = from_seed(92);

    const pool: [7]u8 = "abcdefg".*;
    var result: [3]u8 = undefined;
    var result_count: usize = 0;

    var e_taken_count: u32 = 0;
    for (0..1000) |_| {
        result_count = 0;
        var combination = Combination.init(.{ .total = pool.len, .sample = 3 });
        for (pool) |x| {
            if (combination.take(&prng)) {
                result[result_count] = x;
                result_count += 1;
            }
        }
        assert(combination.done());
        assert(result_count == 3);

        e_taken_count += @intFromBool(std.mem.indexOfScalar(u8, &result, 'e') != null);
    }

    try snap(@src(),
        \\e_taken_count = 432 expected_value=428
    ).diff_fmt("e_taken_count = {} expected_value={}", .{ e_taken_count, 1000 * 3 / 7 });
}

/// An iterator style API for selecting a single element out of the given weighted sequence,
/// without apriory knowledge about the total weight.
pub const Reservoir = struct {
    total: u64,

    pub fn init() Reservoir {
        return .{ .total = 0 };
    }

    pub fn replace(reservoir: *Reservoir, prng: *PRNG, weight: u64) bool {
        reservoir.total += weight;
        return prng.chance(ratio(weight, reservoir.total));
    }
};

test Reservoir {
    var prng = from_seed(92);
    const animals: []const []const u8 = &.{ "walrus", "kiwi", "capybara", "platypus" };
    var kiwi_count: u32 = 0;

    for (0..1000) |_| {
        var reservoir = Reservoir.init();
        var pick: ?[]const u8 = null;
        for (animals) |animal| {
            if (reservoir.replace(&prng, animal.len)) pick = animal;
        }
        assert(pick != null);
        kiwi_count += @intFromBool(std.mem.eql(u8, pick.?, "kiwi"));
    }

    var total_weight: u64 = 0;
    for (animals) |animal| total_weight += animal.len;
    const expected_value = 1000 * "kiwi".len / total_weight;

    try snap(@src(),
        \\kiwi_count = 141 expected_value=153
    ).diff_fmt("kiwi_count = {} expected_value={}", .{ kiwi_count, expected_value });
}

pub fn shuffle(prng: *PRNG, T: type, slice: []T) void {
    if (slice.len <= 1) return;

    for (0..slice.len - 1) |i| {
        const j = prng.range_inclusive(u64, i, slice.len - 1);
        std.mem.swap(T, &slice[i], &slice[j]);
    }
}

test shuffle {
    var prng = from_seed(92);
    var g_first_count: u32 = 0;

    for (0..1000) |_| {
        var buffer = "abcdefg".*;
        shuffle(&prng, u8, &buffer);
        g_first_count += @intFromBool(buffer[0] == 'g');
    }

    try snap(@src(),
        \\g_first_count = 144 expected_value=142
    ).diff_fmt("g_first_count = {} expected_value={}", .{ g_first_count, 1000 / 7 });
}

test "no floating point please" {
    const file_text = try std.fs.cwd().readFileAlloc(std.testing.allocator, @src().file, 64 * 1024);
    defer std.testing.allocator.free(file_text);

    assert(std.mem.indexOf(u8, file_text, "f" ++ "32") == null);
    assert(std.mem.indexOf(u8, file_text, "f" ++ "64") == null);
}
