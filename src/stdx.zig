//! Extensions to the standard library -- things which could have been in std, but aren't.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

pub inline fn div_ceil(numerator: anytype, denominator: anytype) @TypeOf(numerator, denominator) {
    comptime {
        switch (@typeInfo(@TypeOf(numerator))) {
            .Int => |int| assert(int.signedness == .unsigned),
            .ComptimeInt => assert(numerator >= 0),
            else => @compileError("div_ceil: invalid numerator type"),
        }

        switch (@typeInfo(@TypeOf(denominator))) {
            .Int => |int| assert(int.signedness == .unsigned),
            .ComptimeInt => assert(denominator > 0),
            else => @compileError("div_ceil: invalid denominator type"),
        }
    }

    assert(denominator > 0);

    if (numerator == 0) return 0;
    return @divFloor(numerator - 1, denominator) + 1;
}

test "div_ceil" {
    // Comptime ints.
    try std.testing.expectEqual(div_ceil(0, 8), 0);
    try std.testing.expectEqual(div_ceil(1, 8), 1);
    try std.testing.expectEqual(div_ceil(7, 8), 1);
    try std.testing.expectEqual(div_ceil(8, 8), 1);
    try std.testing.expectEqual(div_ceil(9, 8), 2);

    // Unsized ints
    const max = std.math.maxInt(u64);
    try std.testing.expectEqual(div_ceil(@as(u64, 0), 8), 0);
    try std.testing.expectEqual(div_ceil(@as(u64, 1), 8), 1);
    try std.testing.expectEqual(div_ceil(@as(u64, max), 2), max / 2 + 1);
    try std.testing.expectEqual(div_ceil(@as(u64, max) - 1, 2), max / 2);
    try std.testing.expectEqual(div_ceil(@as(u64, max) - 2, 2), max / 2);
}

pub const CopyPrecision = enum { exact, inexact };

pub inline fn copy_left(
    comptime precision: CopyPrecision,
    comptime T: type,
    target: []T,
    source: []const T,
) void {
    switch (precision) {
        .exact => assert(target.len == source.len),
        .inexact => assert(target.len >= source.len),
    }

    if (!disjoint_slices(T, T, target, source)) {
        assert(@ptrToInt(target.ptr) < @ptrToInt(source.ptr));
    }
    std.mem.copy(T, target, source);
}

test "copy_left" {
    const a = try std.testing.allocator.alloc(usize, 8);
    defer std.testing.allocator.free(a);

    for (a) |*v, i| v.* = i;
    copy_left(.exact, usize, a[0..6], a[2..]);
    try std.testing.expect(std.mem.eql(usize, a, &.{ 2, 3, 4, 5, 6, 7, 6, 7 }));
}

pub inline fn copy_right(
    comptime precision: CopyPrecision,
    comptime T: type,
    target: []T,
    source: []const T,
) void {
    switch (precision) {
        .exact => assert(target.len == source.len),
        .inexact => assert(target.len >= source.len),
    }

    if (!disjoint_slices(T, T, target, source)) {
        assert(@ptrToInt(target.ptr) > @ptrToInt(source.ptr));
    }
    std.mem.copyBackwards(T, target, source);
}

test "copy_right" {
    const a = try std.testing.allocator.alloc(usize, 8);
    defer std.testing.allocator.free(a);

    for (a) |*v, i| v.* = i;
    copy_right(.exact, usize, a[2..], a[0..6]);
    try std.testing.expect(std.mem.eql(usize, a, &.{ 0, 1, 0, 1, 2, 3, 4, 5 }));
}

pub inline fn copy_disjoint(
    comptime precision: CopyPrecision,
    comptime T: type,
    target: []T,
    source: []const T,
) void {
    switch (precision) {
        .exact => assert(target.len == source.len),
        .inexact => assert(target.len >= source.len),
    }

    assert(disjoint_slices(T, T, target, source));
    std.mem.copy(T, target, source);
}

pub inline fn disjoint_slices(comptime A: type, comptime B: type, a: []const A, b: []const B) bool {
    return @ptrToInt(a.ptr) + a.len * @sizeOf(A) <= @ptrToInt(b.ptr) or
        @ptrToInt(b.ptr) + b.len * @sizeOf(B) <= @ptrToInt(a.ptr);
}

test "disjoint_slices" {
    const a = try std.testing.allocator.alignedAlloc(u8, @sizeOf(u32), 8 * @sizeOf(u32));
    defer std.testing.allocator.free(a);

    const b = try std.testing.allocator.alloc(u32, 8);
    defer std.testing.allocator.free(b);

    try std.testing.expectEqual(true, disjoint_slices(u8, u32, a, b));
    try std.testing.expectEqual(true, disjoint_slices(u32, u8, b, a));

    try std.testing.expectEqual(true, disjoint_slices(u8, u8, a, a[0..0]));
    try std.testing.expectEqual(true, disjoint_slices(u32, u32, b, b[0..0]));

    try std.testing.expectEqual(false, disjoint_slices(u8, u8, a, a[0..1]));
    try std.testing.expectEqual(false, disjoint_slices(u8, u8, a, a[a.len - 1 .. a.len]));

    try std.testing.expectEqual(false, disjoint_slices(u32, u32, b, b[0..1]));
    try std.testing.expectEqual(false, disjoint_slices(u32, u32, b, b[b.len - 1 .. b.len]));

    try std.testing.expectEqual(false, disjoint_slices(u8, u32, a, std.mem.bytesAsSlice(u32, a)));
    try std.testing.expectEqual(false, disjoint_slices(u32, u8, b, std.mem.sliceAsBytes(b)));
}

/// `maybe` is the dual of `assert`: it signals that condition is sometimes true
///  and sometimes false.
///
/// Currently we use it for documentation, but maybe one day we plug it into
/// coverage.
pub fn maybe(ok: bool) void {
    assert(ok or !ok);
}

/// Signal that something is not yet fully implemented, and abort the process.
///
/// In VOPR, this will exit with status 0, to make it easy to find "real" failures by running
/// the simulator in a loop.
pub fn unimplemented(comptime message: []const u8) noreturn {
    const full_message = "unimplemented: " ++ message;
    const root = @import("root");
    if (@hasDecl(root, "Simulator")) {
        root.output.info(full_message, .{});
        root.output.info("not crashing in VOPR", .{});
        std.process.exit(0);
    }
    @panic(full_message);
}

/// Utility function for ad-hoc profiling.
///
/// A thin wrapper around `std.time.Timer` which handles the boilerplate of
/// printing to stderr and formatting times in some (unspecified) readable way.
pub fn timeit() TimeIt {
    return TimeIt{ .inner = std.time.Timer.start() catch unreachable };
}

const TimeIt = struct {
    inner: std.time.Timer,

    /// Prints elapesed time to stderr and resets the internal timer.
    pub fn lap(self: *TimeIt, comptime label: []const u8) void {
        const label_alignment = comptime " " ** (1 + (12 -| label.len));

        const nanos = self.inner.lap();
        std.debug.print(
            label ++ ":" ++ label_alignment ++ "{}\n",
            .{std.fmt.fmtDuration(nanos)},
        );
    }
};

pub const log = if (builtin.is_test)
    // Downgrade `err` to `warn` for tests.
    // Zig fails any test that does `log.err`, but we want to test those code paths here.
    struct {
        pub fn scoped(comptime scope: @Type(.EnumLiteral)) type {
            const base = std.log.scoped(scope);
            return struct {
                pub const err = warn;
                pub const warn = base.warn;
                pub const info = base.info;
                pub const debug = base.debug;
            };
        }
    }
else
    std.log;

pub inline fn hash_inline(value: anytype) u64 {
    return low_level_hash(0, switch (@typeInfo(@TypeOf(value))) {
        .Struct, .Int => std.mem.asBytes(&value),
        else => @compileError("unsupported hashing for " ++ @typeName(@TypeOf(value))),
    });
}

/// Inline version of Google Abseil "LowLevelHash" (inspired by wyhash).
/// https://github.com/abseil/abseil-cpp/blob/master/absl/hash/internal/low_level_hash.cc
inline fn low_level_hash(seed: u64, input: anytype) u64 {
    const salt = [_]u64{
        0xa0761d6478bd642f,
        0xe7037ed1a0b428db,
        0x8ebc6af09c88c6e3,
        0x589965cc75374cc3,
        0x1d8e4e27c47d124f,
    };

    var in: []const u8 = input;
    var state = seed ^ salt[0];
    const starting_len = input.len;

    if (in.len > 64) {
        var dup = [_]u64{ state, state };
        defer state = dup[0] ^ dup[1];

        while (in.len > 64) : (in = in[64..]) {
            for (@bitCast([2][4]u64, in[0..64].*)) |chunk, i| {
                const mix1 = @as(u128, chunk[0] ^ salt[(i * 2) + 1]) *% (chunk[1] ^ dup[i]);
                const mix2 = @as(u128, chunk[2] ^ salt[(i * 2) + 2]) *% (chunk[3] ^ dup[i]);
                dup[i] = @truncate(u64, mix1 ^ (mix1 >> 64));
                dup[i] ^= @truncate(u64, mix2 ^ (mix2 >> 64));
            }
        }
    }

    while (in.len > 16) : (in = in[16..]) {
        const chunk = @bitCast([2]u64, in[0..16].*);
        const mixed = @as(u128, chunk[0] ^ salt[1]) *% (chunk[1] ^ state);
        state = @truncate(u64, mixed ^ (mixed >> 64));
    }

    var chunk = std.mem.zeroes([2]u64);
    if (in.len > 8) {
        chunk[0] = @bitCast(u64, in[0..8].*);
        chunk[1] = @bitCast(u64, in[in.len - 8 ..][0..8].*);
    } else if (in.len > 3) {
        chunk[0] = @bitCast(u32, in[0..4].*);
        chunk[1] = @bitCast(u32, in[in.len - 4 ..][0..4].*);
    } else if (in.len > 0) {
        chunk[0] = (@as(u64, in[0]) << 16) | (@as(u64, in[in.len / 2]) << 8) | in[in.len - 1];
    }

    var mixed = @as(u128, chunk[0] ^ salt[1]) *% (chunk[1] ^ state);
    mixed = @truncate(u64, mixed ^ (mixed >> 64));
    mixed *%= (@as(u64, starting_len) ^ salt[1]);
    return @truncate(u64, mixed ^ (mixed >> 64));
}

test "hash_inline" {
    const Case = struct { seed: u64, hash: u64, b64: []const u8 };
    for ([_]Case{
        .{ .seed = 0xec42b7ab404b8acb, .hash = 0xe5a40d39ab796423, .b64 = "" },
        .{ .seed = 0, .hash = 0x1766974bf7527d81, .b64 = "ICAg" },
        .{ .seed = 0, .hash = 0x5c3bbbe230db17a8, .b64 = "YWFhYQ==" },
        .{ .seed = 0, .hash = 0xa6630143a7e6aa6f, .b64 = "AQID" },
        .{ .seed = 0, .hash = 0x8787cb2d04b0c984, .b64 = "AQIDBA==" },
        .{ .seed = 0, .hash = 0x33603654ff574ac2, .b64 = "dGhpcmRfcGFydHl8d3loYXNofDY0" },
        .{ .seed = 0xeeee074043a3ee0f, .hash = 0xa6564b468248c683, .b64 = "Zw==" },
        .{ .seed = 0x857902089c393de, .hash = 0xef192f401b116e1c, .b64 = "xmk=" },
        .{ .seed = 0x993df040024ca3af, .hash = 0xbe8dc0c54617639d, .b64 = "c1H/" },
        .{ .seed = 0xc4e4c2acea740e96, .hash = 0x93d7f665b5521c8e, .b64 = "SuwpzQ==" },
        .{ .seed = 0x6a214b3db872d0cf, .hash = 0x646d70bb42445f28, .b64 = "uqvy++M=" },
        .{ .seed = 0x44343db6a89dba4d, .hash = 0x96a7b1e3cc9bd426, .b64 = "RnzCVPgb" },
        .{ .seed = 0x77b5d6d1ae1dd483, .hash = 0x76020289ab0790c4, .b64 = "6OeNdlouYw==" },
        .{ .seed = 0x89ab8ecb44d221f1, .hash = 0x39f842e4133b9b44, .b64 = "M5/JmmYyDbc=" },
        .{ .seed = 0x60244b17577ca81b, .hash = 0x2b8d7047be4bcaab, .b64 = "MVijWiVdBRdY" },
        .{ .seed = 0x59a08dcee0717067, .hash = 0x99628abef6716a97, .b64 = "6V7Uq7LNxpu0VA==" },
        .{ .seed = 0xf5f20db3ade57396, .hash = 0x4432e02ba42b2740, .b64 = "EQ6CdEEhPdyHcOk=" },
        .{ .seed = 0xbf8dee0751ad3efb, .hash = 0x74d810efcad7918a, .b64 = "PqFB4fxnPgF+l+rc" },
        .{ .seed = 0x6b7a06b268d63e30, .hash = 0x88c84e986002507f, .b64 = "a5aPOFwq7LA7+zKvPA==" },
        .{ .seed = 0xb8c37f0ae0f54c82, .hash = 0x4f99acf193cf39b9, .b64 = "VOwY21wCGv5D+/qqOvs=" },
        .{ .seed = 0x9fcbed0c38e50eef, .hash = 0xd90e7a3655891e37, .b64 = "KdHmBTx8lHXYvmGJ+Vy7" },
        .{ .seed = 0x2af4bade1d8e3a1d, .hash = 0x3bb378b1d4df8fcf, .b64 = "qJkPlbHr8bMF7/cA6aE65Q==" },
        .{ .seed = 0x714e3aa912da2f2c, .hash = 0xf78e94045c052d47, .b64 = "ygvL0EhHZL0fIx6oHHtkxRQ=" },
        .{ .seed = 0xf5ee75e3cbb82c1c, .hash = 0x26da0b2130da6b40, .b64 = "c1rFXkt5YztwZCQRngncqtSs" },
        .{ .seed = 0x620e7007321b93b9, .hash = 0x30b4d426af8c6986, .b64 = "8hsQrzszzeNQSEcVXLtvIhm6mw==" },
        .{ .seed = 0xc08528cac2e551fc, .hash = 0x5413b4aaf3baaeae, .b64 = "ffUL4RocfyP4KfikGxO1yk7omDI=" },
        .{ .seed = 0x6a1debf9cc3ad39, .hash = 0x756ab265370a1597, .b64 = "OOB5TT00vF9Od/rLbAWshiErqhpV" },
        .{ .seed = 0x7e0a3c88111fc226, .hash = 0xdaf5f4b7d09814fb, .b64 = "or5wtXM7BFzTNpSzr+Lw5J5PMhVJ/Q==" },
        .{ .seed = 0x1301fef15df39edb, .hash = 0x8f874ae37742b75e, .b64 = "gk6pCHDUsoopVEiaCrzVDhioRKxb844=" },
        .{ .seed = 0x64e181f3d5817ab, .hash = 0x8fecd03956121ce8, .b64 = "TNctmwlC5QbEM6/No4R/La3UdkfeMhzs" },
        .{ .seed = 0xafafc44961078ecb, .hash = 0x229c292ea7a08285, .b64 = "SsQw9iAjhWz7sgcE9OwLuSC6hsM+BfHs2Q==" },
        .{ .seed = 0x4f7bb45549250094, .hash = 0xbb4bf0692d14bae, .b64 = "ZzO3mVCj4xTT2TT3XqDyEKj2BZQBvrS8RHg=" },
        .{ .seed = 0xa30061abaa2818c, .hash = 0x207b24ca3bdac1db, .b64 = "+klp5iPQGtppan5MflEls0iEUzqU+zGZkDJX" },
        .{ .seed = 0xd902ee3e44a5705f, .hash = 0x64f6cd6745d3825b, .b64 = "RO6bvOnlJc8I9eniXlNgqtKy0IX6VNg16NRmgg==" },
        .{ .seed = 0x316d36da516f583, .hash = 0xa2b2e1656b58df1e, .b64 = "ZJjZqId1ZXBaij9igClE3nyliU5XWdNRrayGlYA=" },
        .{ .seed = 0x402d83f9f834f616, .hash = 0xd01d30d9ee7a148, .b64 = "7BfkhfGMDGbxfMB8uyL85GbaYQtjr2K8g7RpLzr/" },
        .{ .seed = 0x9c604164c016b72c, .hash = 0x1cb4cd00ab804e3b, .b64 = "rycWk6wHH7htETQtje9PidS2YzXBx+Qkg2fY7ZYS7A==" },
        .{ .seed = 0x3f4507e01f9e73ba, .hash = 0x4697f2637fd90999, .b64 = "RTkC2OUK+J13CdGllsH0H5WqgspsSa6QzRZouqx6pvI=" },
        .{ .seed = 0xc3fe0d5be8d2c7c7, .hash = 0x8383a756b5688c07, .b64 = "tKjKmbLCNyrLCM9hycOAXm4DKNpM12oZ7dLTmUx5iwAi" },
        .{ .seed = 0x531858a40bfa7ea1, .hash = 0x695c29cb3696a975, .b64 = "VprUGNH+5NnNRaORxgH/ySrZFQFDL+4VAodhfBNinmn8cg==" },
        .{ .seed = 0x86689478a7a7e8fa, .hash = 0xda2e5a5a5e971521, .b64 = "gc1xZaY+q0nPcUvOOnWnT3bqfmT/geth/f7Dm2e/DemMfk4=" },
        .{ .seed = 0x4ec948b8e7f27288, .hash = 0x7935d4befa056b2b, .b64 = "Mr35fIxqx1ukPAL0su1yFuzzAU3wABCLZ8+ZUFsXn47UmAph" },
        .{ .seed = 0xce46c7213c10032, .hash = 0x38dd541ca95420fe, .b64 = "A9G8pw2+m7+rDtWYAdbl8tb2fT7FFo4hLi2vAsa5Y8mKH3CX3g==" },
        .{ .seed = 0xf63e96ee6f32a8b6, .hash = 0xcc06c7a4963f967f, .b64 = "DFaJGishGwEHDdj9ixbCoaTjz9KS0phLNWHVVdFsM93CvPft3hM=" },
        .{ .seed = 0x1cfe85e65fc5225, .hash = 0xbf0f6f66e232fb20, .b64 = "7+Ugx+Kr3aRNgYgcUxru62YkTDt5Hqis+2po81hGBkcrJg4N0uuy" },
        .{ .seed = 0x45c474f1cee1d2e8, .hash = 0xf7efb32d373fe71a, .b64 = "H2w6O8BUKqu6Tvj2xxaecxEI2wRgIgqnTTG1WwOgDSINR13Nm4d4Vg==" },
        .{ .seed = 0x6e024e14015f329c, .hash = 0xe2e64634b1c12660, .b64 = "1XBMnIbqD5jy65xTDaf6WtiwtdtQwv1dCVoqpeKj+7cTR1SaMWMyI04=" },
        .{ .seed = 0x760c40502103ae1c, .hash = 0x285b8fd1638e306d, .b64 = "znZbdXG2TSFrKHEuJc83gPncYpzXGbAebUpP0XxzH0rpe8BaMQ17nDbt" },
        .{ .seed = 0x17fd05c3c560c320, .hash = 0x658e8a4e3b714d6c, .b64 = "ylu8Atu13j1StlcC1MRMJJXIl7USgDDS22HgVv0WQ8hx/8pNtaiKB17hCQ==" },
        .{ .seed = 0x8b34200a6f8e90d9, .hash = 0xf391fb968e0eb398, .b64 = "M6ZVVzsd7vAvbiACSYHioH/440dp4xG2mLlBnxgiqEvI/aIEGpD0Sf4VS0g=" },
        .{ .seed = 0x6be89e50818bdf69, .hash = 0x744a9ea0cc144bf2, .b64 = "li3oFSXLXI+ubUVGJ4blP6mNinGKLHWkvGruun85AhVn6iuMtocbZPVhqxzn" },
        .{ .seed = 0xfb389773315b47d8, .hash = 0x12636f2be11012f1, .b64 = "kFuQHuUCqBF3Tc3hO4dgdIp223ShaCoog48d5Do5zMqUXOh5XpGK1t5XtxnfGA==" },
        .{ .seed = 0x4f2512a23f61efee, .hash = 0x29c57de825948f80, .b64 = "jWmOad0v0QhXVJd1OdGuBZtDYYS8wBVHlvOeTQx9ZZnm8wLEItPMeihj72E0nWY=" },
        .{ .seed = 0x59ccd92fc16c6fda, .hash = 0x58c6f99ab0d1c021, .b64 = "z+DHU52HaOQdW4JrZwDQAebEA6rm13Zg/9lPYA3txt3NjTBqFZlOMvTRnVzRbl23" },
        .{ .seed = 0x25c5a7f5bd330919, .hash = 0x13e7b5a7b82fe3bb, .b64 = "MmBiGDfYeTayyJa/tVycg+rN7f9mPDFaDc+23j0TlW9094er0ADigsl4QX7V3gG/qw==" },
        .{ .seed = 0x51df4174d34c97d7, .hash = 0x10fbc87901e02b63, .b64 = "774RK+9rOL4iFvs1q2qpo/JVc/I39buvNjqEFDtDvyoB0FXxPI2vXqOrk08VPfIHkmU=" },
        .{ .seed = 0x80ce6d76f89cb57, .hash = 0xa24c9184901b748b, .b64 = "+slatXiQ7/2lK0BkVUI1qzNxOOLP3I1iK6OfHaoxgqT63FpzbElwEXSwdsryq3UlHK0I" },
        .{ .seed = 0x20961c911965f684, .hash = 0xcac4fd4c5080e581, .b64 = "64mVTbQ47dHjHlOHGS/hjJwr/K2frCNpn87exOqMzNUVYiPKmhCbfS7vBUce5tO6Ec9osQ==" },
        .{ .seed = 0x4e5b926ec83868e7, .hash = 0xc38bdb7483ba68e1, .b64 = "fIsaG1r530SFrBqaDj1kqE0AJnvvK8MNEZbII2Yw1OK77v0V59xabIh0B5axaz/+a2V5WpA=" },
        .{ .seed = 0x3927b30b922eecef, .hash = 0xdb2a8069b2ceaffa, .b64 = "PGih0zDEOWCYGxuHGDFu9Ivbff/iE7BNUq65tycTR2R76TerrXALRosnzaNYO5fjFhTi+CiS" },
        .{ .seed = 0xbd0291284a49b61c, .hash = 0xdf9fe91d0d1c7887, .b64 = "RnpA/zJnEnnLjmICORByRVb9bCOgxF44p3VMiW10G7PvW7IhwsWajlP9kIwNA9FjAD2GoQHk2Q==" },
        .{ .seed = 0x73a77c575bcc956, .hash = 0xe83f49e96e2e6a08, .b64 = "qFklMceaTHqJpy2qavJE+EVBiNFOi6OxjOA3LeIcBop1K7w8xQi3TrDk+BrWPRIbfprszSaPfrI=" },
        .{ .seed = 0x766a0e2ade6d09a6, .hash = 0xc69e61b62ca2b62, .b64 = "cLbfUtLl3EcQmITWoTskUR8da/VafRDYF/ylPYwk7/zazk6ssyrzxMN3mmSyvrXR2yDGNZ3WDrTT" },
        .{ .seed = 0x2599f4f905115869, .hash = 0xb4a4f3f85f8298fe, .b64 = "s/Jf1+FbsbCpXWPTUSeWyMH6e4CvTFvPE5Fs6Z8hvFITGyr0dtukHzkI84oviVLxhM1xMxrMAy1dbw==" },
        .{ .seed = 0xd8256e5444d21e53, .hash = 0x167a1b39e1e95f41, .b64 = "FvyQ00+j7nmYZVQ8hI1Edxd0AWplhTfWuFGiu34AK5X8u2hLX1bE97sZM0CmeLe+7LgoUT1fJ/axybE=" },
        .{ .seed = 0xf664a91333fb8dfd, .hash = 0xf8a2a5649855ee41, .b64 = "L8ncxMaYLBH3g9buPu8hfpWZNlOF7nvWLNv9IozH07uQsIBWSKxoPy8+LW4tTuzC6CIWbRGRRD1sQV/4" },
        .{ .seed = 0x9625b859be372cd1, .hash = 0x27992565b595c498, .b64 = "CDK0meI07yrgV2kQlZZ+wuVqhc2NmzqeLH7bmcA6kchsRWFPeVF5Wqjjaj556ABeUoUr3yBmfU3kWOakkg==" },
        .{ .seed = 0x7b99940782e29898, .hash = 0x3e08cca5b71f9346, .b64 = "d23/vc5ONh/HkMiq+gYk4gaCNYyuFKwUkvn46t+dfVcKfBTYykr4kdvAPNXGYLjM4u1YkAEFpJP+nX7eOvs=" },
        .{ .seed = 0x4fe12fa5383b51a8, .hash = 0xad406b10c770a6d2, .b64 = "NUR3SRxBkxTSbtQORJpu/GdR6b/h6sSGfsMj/KFd99ahbh+9r7LSgSGmkGVB/mGoT0pnMTQst7Lv2q6QN6Vm" },
        .{ .seed = 0xe2ccb09ac0f5b4b6, .hash = 0xd1713ce6e552bcf2, .b64 = "2BOFlcI3Z0RYDtS9T9Ie9yJoXlOdigpPeeT+CRujb/O39Ih5LPC9hP6RQk1kYESGyaLZZi3jtabHs7DiVx/VDg==" },
        .{ .seed = 0x7d0a37adbd7b753b, .hash = 0x753b287194c73ad3, .b64 = "FF2HQE1FxEvWBpg6Z9zAMH+Zlqx8S1JD/wIlViL6ZDZY63alMDrxB0GJQahmAtjlm26RGLnjW7jmgQ4Ie3I+014=" },
        .{ .seed = 0xd3ae96ef9f7185f2, .hash = 0x5ae41a95f600af1c, .b64 = "tHmO7mqVL/PX11nZrz50Hc+M17Poj5lpnqHkEN+4bpMx/YGbkrGOaYjoQjgmt1X2QyypK7xClFrjeWrCMdlVYtbW" },
        .{ .seed = 0x4fb88ea63f79a0d8, .hash = 0x4a61163b86a8bb4c, .b64 = "/WiHi9IQcxRImsudkA/KOTqGe8/gXkhKIHkjddv5S9hi02M049dIK3EUyAEjkjpdGLUs+BN0QzPtZqjIYPOgwsYE9g==" },
        .{ .seed = 0xed564e259bb5ebe9, .hash = 0x42eeaa79e760c7e4, .b64 = "qds+1ExSnU11L4fTSDz/QE90g4Jh6ioqSh3KDOTOAo2pQGL1k/9CCC7J23YF27dUTzrWsCQA2m4epXoCc3yPHb3xElA=" },
        .{ .seed = 0x3e3256b60c428000, .hash = 0x698df622ef465b0a, .b64 = "8FVYHx40lSQPTHheh08Oq0/pGm2OlG8BEf8ezvAxHuGGdgCkqpXIueJBF2mQJhTfDy5NncO8ntS7vaKs7sCNdDaNGOEi" },
        .{ .seed = 0xfb05bad59ec8705, .hash = 0x157583111e1a6026, .b64 = "4ZoEIrJtstiCkeew3oRzmyJHVt/pAs2pj0HgHFrBPztbQ10NsQ/lM6DM439QVxpznnBSiHMgMQJhER+70l72LqFTO1JiIQ==" },
        .{ .seed = 0xafdc251dbf97b5f8, .hash = 0xaa1388f078e793e0, .b64 = "hQPtaYI+wJyxXgwD5n8jGIKFKaFA/P83KqCKZfPthnjwdOFysqEOYwAaZuaaiv4cDyi9TyS8hk5cEbNP/jrI7q6pYGBLbsM=" },
        .{ .seed = 0x10ec9c92ddb5dcbc, .hash = 0xf10d68d0f3309360, .b64 = "S4gpMSKzMD7CWPsSfLeYyhSpfWOntyuVZdX1xSBjiGvsspwOZcxNKCRIOqAA0moUfOh3I5+juQV4rsqYElMD/gWfDGpsWZKQ" },
        .{ .seed = 0x9a767d5822c7dac4, .hash = 0x2af056184457a3de, .b64 = "oswxop+bthuDLT4j0PcoSKby4LhF47ZKg8K17xxHf74UsGCzTBbOz0MM8hQEGlyqDT1iUiAYnaPaUpL2mRK0rcIUYA4qLt5uOw==" },
        .{ .seed = 0xee46254080d6e2db, .hash = 0x6d0058e1590b2489, .b64 = "0II/697p+BtLSjxj5989OXI004TogEb94VUnDzOVSgMXie72cuYRvTFNIBgtXlKfkiUjeqVpd4a+n5bxNOD1TGrjQtzKU5r7obo=" },
        .{ .seed = 0xbbb669588d8bf398, .hash = 0x638f287f68817f12, .b64 = "E84YZW2qipAlMPmctrg7TKlwLZ68l4L+c0xRDUfyyFrA4MAti0q9sHq3TDFviH0Y+Kq3tEE5srWFA8LM9oomtmvm5PYxoaarWPLc" },
        .{ .seed = 0xdc2afaa529beef44, .hash = 0xc46b71fecefd5467, .b64 = "x3pa4HIElyZG0Nj7Vdy9IdJIR4izLmypXw5PCmZB5y68QQ4uRaVVi3UthsoJROvbjDJkP2DQ6L/eN8pFeLFzNPKBYzcmuMOb5Ull7w==" },
        .{ .seed = 0xf1f67391d45013a8, .hash = 0x2c8e94679d964e0a, .b64 = "jVDKGYIuWOP/QKLdd2wi8B2VJA8Wh0c8PwrXJVM8FOGM3voPDVPyDJOU6QsBDPseoR8uuKd19OZ/zAvSCB+zlf6upAsBlheUKgCfKww=" },
        .{ .seed = 0x16fce2b8c65a3429, .hash = 0x8612b797ce22503a, .b64 = "mkquunhmYe1aR2wmUz4vcvLEcKBoe6H+kjUok9VUn2+eTSkWs4oDDtJvNCWtY5efJwg/j4PgjRYWtqnrCkhaqJaEvkkOwVfgMIwF3e+d" },
        .{ .seed = 0xf4b096699f49fe67, .hash = 0x59f929babfba7170, .b64 = "fRelvKYonTQ+s+rnnvQw+JzGfFoPixtna0vzcSjiDqX5s2Kg2//UGrK+AVCyMUhO98WoB1DDbrsOYSw2QzrcPe0+3ck9sePvb+Q/IRaHbw==" },
        .{ .seed = 0xca584c4bc8198682, .hash = 0x9527556923fb49a0, .b64 = "DUwXFJzagljo44QeJ7/6ZKw4QXV18lhkYT2jglMr8WB3CHUU4vdsytvw6AKv42ZcG6fRkZkq9fpnmXy6xG0aO3WPT1eHuyFirAlkW+zKtwg=" },
        .{ .seed = 0xed269fc3818b6aad, .hash = 0x1039ab644f5e150b, .b64 = "cYmZCrOOBBongNTr7e4nYn52uQUy2mfe48s50JXx2AZ6cRAt/xRHJ5QbEoEJOeOHsJyM4nbzwFm++SlT6gFZZHJpkXJ92JkR86uS/eV1hJUR" },
        .{ .seed = 0x33f253cbb8fe66a8, .hash = 0x7816c83f3aa05e6d, .b64 = "EXeHBDfhwzAKFhsMcH9+2RHwV+mJaN01+9oacF6vgm8mCXRd6jeN9U2oAb0of5c5cO4i+Vb/LlHZSMI490SnHU0bejhSCC2gsC5d2K30ER3iNA==" },
        .{ .seed = 0xd0b76b2c1523d99c, .hash = 0xf51d2f564518c619, .b64 = "FzkzRYoNjkxFhZDso94IHRZaJUP61nFYrh5MwDwv9FNoJ5jyNCY/eazPZk+tbmzDyJIGw2h3GxaWZ9bSlsol/vK98SbkMKCQ/wbfrXRLcDzdd/8=" },
        .{ .seed = 0xfd28f0811a2a237f, .hash = 0x67d494cff03ac004, .b64 = "Re4aXISCMlYY/XsX7zkIFR04ta03u4zkL9dVbLXMa/q6hlY/CImVIIYRN3VKP4pnd0AUr/ugkyt36JcstAInb4h9rpAGQ7GMVOgBniiMBZ/MGU7H" },
        .{ .seed = 0x6261fb136482e84, .hash = 0x2802d636ced1cfbb, .b64 = "ueLyMcqJXX+MhO4UApylCN9WlTQ+ltJmItgG7vFUtqs2qNwBMjmAvr5u0sAKd8jpzV0dDPTwchbIeAW5zbtkA2NABJV6hFM48ib4/J3A5mseA3cS8w==" },
        .{ .seed = 0x458efc750bca7c3a, .hash = 0xf64e20bad771cb12, .b64 = "6Si7Yi11L+jZMkwaN+GUuzXMrlvEqviEkGOilNq0h8TdQyYKuFXzkYc/q74gP3pVCyiwz9KpVGMM9vfnq36riMHRknkmhQutxLZs5fbmOgEO69HglCU=" },
        .{ .seed = 0xa7e69ff84e5e7c27, .hash = 0xb9a6cf84a83e15e, .b64 = "Q6AbOofGuTJOegPh9Clm/9crtUMQqylKrTc1fhfJo1tqvpXxhU4k08kntL1RG7woRnFrVh2UoMrL1kjin+s9CanT+y4hHwLqRranl9FjvxfVKm3yvg68" },
        .{ .seed = 0x3c59bfd0c29efe9e, .hash = 0x8da6630319609301, .b64 = "ieQEbIPvqY2YfIjHnqfJiO1/MIVRk0RoaG/WWi3kFrfIGiNLCczYoklgaecHMm/1sZ96AjO+a5stQfZbJQwS7Sc1ODABEdJKcTsxeW2hbh9A6CFzpowP1A==" },
        .{ .seed = 0x10befacc6afd298d, .hash = 0x40946a86e2a996f3, .b64 = "zQUv8hFB3zh2GGl3KTvCmnfzE+SUgQPVaSVIELFX5H9cE3FuVFGmymkPQZJLAyzC90Cmi8GqYCvPqTuAAB//XTJxy4bCcVArgZG9zJXpjowpNBfr3ngWrSE=" },
        .{ .seed = 0x41d5320b0a38efa7, .hash = 0xcab7f5997953fa76, .b64 = "US4hcC1+op5JKGC7eIs8CUgInjKWKlvKQkapulxW262E/B2ye79QxOexf188u2mFwwe3WTISJHRZzS61IwljqAWAWoBAqkUnW8SHmIDwHUP31J0p5sGdP47L" },
        .{ .seed = 0x58db1c7450fe17f3, .hash = 0x39129ca0e04fc465, .b64 = "9bHUWFna2LNaGF6fQLlkx1Hkt24nrkLE2CmFdWgTQV3FFbUe747SSqYw6ebpTa07MWSpWRPsHesVo2B9tqHbe7eQmqYebPDFnNqrhSdZwFm9arLQVs+7a3Ic6A==" },
        .{ .seed = 0x6098c055a335b7a6, .hash = 0x5238221fd685e1b8, .b64 = "Kb3DpHRUPhtyqgs3RuXjzA08jGb59hjKTOeFt1qhoINfYyfTt2buKhD6YVffRCPsgK9SeqZqRPJSyaqsa0ovyq1WnWW8jI/NhvAkZTVHUrX2pC+cD3OPYT05Dag=" },
        .{ .seed = 0x1bbacec67845a801, .hash = 0x175130c407dbcaab, .b64 = "gzxyMJIPlU+bJBwhFUCHSofZ/319LxqMoqnt3+L6h2U2+ZXJCSsYpE80xmR0Ta77Jq54o92SMH87HV8dGOaCTuAYF+lDL42SY1P316Cl0sZTS2ow3ZqwGbcPNs/1" },
        .{ .seed = 0xc419cfc7442190, .hash = 0x2f20e7536c0b0df, .b64 = "uR7V0TW+FGVMpsifnaBAQ3IGlr1wx5sKd7TChuqRe6OvUXTlD4hKWy8S+8yyOw8lQabism19vOQxfmocEOW/vzY0pEa87qHrAZy4s9fH2Bltu8vaOIe+agYohhYORQ==" },
        .{ .seed = 0xc95e510d94ba270c, .hash = 0x2742cb488a04ad56, .b64 = "1UR5eoo2aCwhacjZHaCh9bkOsITp6QunUxHQ2SfeHv0imHetzt/Z70mhyWZBalv6eAx+YfWKCUib2SHDtz/A2dc3hqUWX5VfAV7FQsghPUAtu6IiRatq4YSLpDvKZBQ=" },
        .{ .seed = 0xff1ae05c98089c3f, .hash = 0xd6afb593879ff93b, .b64 = "opubR7H63BH7OtY+Avd7QyQ25UZ8kLBdFDsBTwZlY6gA/u+x+czC9AaZMgmQrUy15DH7YMGsvdXnviTtI4eVI4aF1H9Rl3NXMKZgwFOsdTfdcZeeHVRzBBKX8jUfh1il" },
        .{ .seed = 0x90c02b8dceced493, .hash = 0xf50ad64caac0ca7f, .b64 = "DC0kXcSXtfQ9FbSRwirIn5tgPri0sbzHSa78aDZVDUKCMaBGyFU6BmrulywYX8yzvwprdLsoOwTWN2wMjHlPDqrvVHNEjnmufRDblW+nSS+xtKNs3N5xsxXdv6JXDrAB/Q==" },
        .{ .seed = 0x9f8a76697ab1aa36, .hash = 0x2ade95c4261364ae, .b64 = "BXRBk+3wEP3Lpm1y75wjoz+PgB0AMzLe8tQ1AYU2/oqrQB2YMC6W+9QDbcOfkGbeH+b7IBkt/gwCMw2HaQsRFEsurXtcQ3YwRuPz5XNaw5NAvrNa67Fm7eRzdE1+hWLKtA8=" },
        .{ .seed = 0x6ba1bf3d811a531d, .hash = 0x5c4f3299faacd07a, .b64 = "RRBSvEGYnzR9E45Aps/+WSnpCo/X7gJLO4DRnUqFrJCV/kzWlusLE/6ZU6RoUf2ROwcgEvUiXTGjLs7ts3t9SXnJHxC1KiOzxHdYLMhVvgNd3hVSAXODpKFSkVXND55G2L1W" },
        .{ .seed = 0x6a418974109c67b4, .hash = 0xfffe3bff0ae5e9bc, .b64 = "jeh6Qazxmdi57pa9S3XSnnZFIRrnc6s8QLrah5OX3SB/V2ErSPoEAumavzQPkdKF1/SfvmdL+qgF1C+Yawy562QaFqwVGq7+tW0yxP8FStb56ZRgNI4IOmI30s1Ei7iops9Uuw==" },
        .{ .seed = 0x8472f1c2b3d230a3, .hash = 0x1db785c0005166e4, .b64 = "6QO5nnDrY2/wrUXpltlKy2dSBcmK15fOY092CR7KxAjNfaY+aAmtWbbzQk3MjBg03x39afSUN1fkrWACdyQKRaGxgwq6MGNxI6W+8DLWJBHzIXrntrE/ml6fnNXEpxplWJ1vEs4=" },
        .{ .seed = 0x5e06068f884e73a7, .hash = 0xea000d962ad18418, .b64 = "0oPxeEHhqhcFuwonNfLd5jF3RNATGZS6NPoS0WklnzyokbTqcl4BeBkMn07+fDQv83j/BpGUwcWO05f3+DYzocfnizpFjLJemFGsls3gxcBYxcbqWYev51tG3lN9EvRE+X9+Pwww" },
        .{ .seed = 0x55290b1a8f170f59, .hash = 0xe42aef38359362d9, .b64 = "naSBSjtOKgAOg8XVbR5cHAW3Y+QL4Pb/JO9/oy6L08wvVRZqo0BrssMwhzBP401Um7A4ppAupbQeJFdMrysY34AuSSNvtNUy5VxjNECwiNtgwYHw7yakDUv8WvonctmnoSPKENegQg==" },
        .{ .seed = 0x5501cfd83dfe706a, .hash = 0xc8e95657348a3891, .b64 = "vPyl8DxVeRe1OpilKb9KNwpGkQRtA94UpAHetNh+95V7nIW38v7PpzhnTWIml5kw3So1Si0TXtIUPIbsu32BNhoH7QwFvLM+JACgSpc5e3RjsL6Qwxxi11npwxRmRUqATDeMUfRAjxg=" },
        .{ .seed = 0xe43ed13d13a66990, .hash = 0xc162eca864f238c6, .b64 = "QC9i2GjdTMuNC1xQJ74ngKfrlA4w3o58FhvNCltdIpuMhHP1YsDA78scQPLbZ3OCUgeQguYf/vw6zAaVKSgwtaykqg5ka/4vhz4hYqWU5ficdXqClHl+zkWEY26slCNYOM5nnDlly8Cj" },
        .{ .seed = 0xdf43bc375cf5283f, .hash = 0xbe1fb373e20579ad, .b64 = "7CNIgQhAHX27nxI0HeB5oUTnTdgKpRDYDKwRcXfSFGP1XeT9nQF6WKCMjL1tBV6x7KuJ91GZz11F4c+8s+MfqEAEpd4FHzamrMNjGcjCyrVtU6y+7HscMVzr7Q/ODLcPEFztFnwjvCjmHw==" },
        .{ .seed = 0x8112b806d288d7b5, .hash = 0x628a1d4f40aa6ffd, .b64 = "Qa/hC2RPXhANSospe+gUaPfjdK/yhQvfm4cCV6/pdvCYWPv8p1kMtKOX3h5/8oZ31fsmx4Axphu5qXJokuhZKkBUJueuMpxRyXpwSWz2wELx5glxF7CM0Fn+OevnkhUn5jsPlG2r5jYlVn8=" },
        .{ .seed = 0xd52a18abb001cb46, .hash = 0xa87bdb7456340f90, .b64 = "kUw/0z4l3a89jTwN5jpG0SHY5km/IVhTjgM5xCiPRLncg40aqWrJ5vcF891AOq5hEpSq0bUCJUMFXgct7kvnys905HjerV7Vs1Gy84tgVJ70/2+pAZTsB/PzNOE/G6sOj4+GbTzkQu819OLB" },
        .{ .seed = 0xe12b76a2433a1236, .hash = 0x5960ef3ba982c801, .b64 = "VDdfSDbO8Tdj3T5W0XM3EI7iHh5xpIutiM6dvcJ/fhe23V/srFEkDy5iZf/VnA9kfi2C79ENnFnbOReeuZW1b3MUXB9lgC6U4pOTuC+jHK3Qnpyiqzj7h3ISJSuo2pob7vY6VHZo6Fn7exEqHg==" },
        .{ .seed = 0x175bf7319cf1fa00, .hash = 0x5026586df9a431ec, .b64 = "Ldfvy3ORdquM/R2fIkhH/ONi69mcP1AEJ6n/oropwecAsLJzQSgezSY8bEiEs0VnFTBBsW+RtZY6tDj03fnb3amNUOq1b7jbqyQkL9hpl+2Z2J8IaVSeownWl+bQcsR5/xRktIMckC5AtF4YHfU=" },
        .{ .seed = 0xd63d57b3f67525ae, .hash = 0xfe4b8a20fdf0840b, .b64 = "BrbNpb42+VzZAjJw6QLirXzhweCVRfwlczzZ0VX2xluskwBqyfnGovz5EuX79JJ31VNXa5hTkAyQat3lYKRADTdAdwE5PqM1N7YaMqqsqoAAAeuYVXuk5eWCykYmClNdSspegwgCuT+403JigBzi" },
        .{ .seed = 0x933faea858832b73, .hash = 0xdcb761867da7072f, .b64 = "gB3NGHJJvVcuPyF0ZSvHwnWSIfmaI7La24VMPQVoIIWF7Z74NltPZZpx2f+cocESM+ILzQW9p+BC8x5IWz7N4Str2WLGKMdgmaBfNkEhSHQDU0IJEOnpUt0HmjhFaBlx0/LTmhua+rQ6Wup8ezLwfg==" },
        .{ .seed = 0x53d061e5f8e7c04f, .hash = 0xc10d4653667275b7, .b64 = "hTKHlRxx6Pl4gjG+6ksvvj0CWFicUg3WrPdSJypDpq91LUWRni2KF6+81ZoHBFhEBrCdogKqeK+hy9bLDnx7g6rAFUjtn1+cWzQ2YjiOpz4+ROBB7lnwjyTGWzJD1rXtlso1g2qVH8XJVigC5M9AIxM=" },
        .{ .seed = 0xdb4124556dd515e0, .hash = 0x727720deec13110b, .b64 = "IWQBelSQnhrr0F3BhUpXUIDauhX6f95Qp+A0diFXiUK7irwPG1oqBiqHyK/SH/9S+rln9DlFROAmeFdH0OCJi2tFm4afxYzJTFR4HnR4cG4x12JqHaZLQx6iiu6CE3rtWBVz99oAwCZUOEXIsLU24o2Y" },
        .{ .seed = 0x4fb31a0dd681ee71, .hash = 0x710b009662858dc9, .b64 = "TKo+l+1dOXdLvIrFqeLaHdm0HZnbcdEgOoLVcGRiCbAMR0j5pIFw8D36tefckAS1RCFOH5IgP8yiFT0Gd0a2hI3+fTKA7iK96NekxWeoeqzJyctc6QsoiyBlkZerRxs5RplrxoeNg29kKDTM0K94mnhD9g==" },
        .{ .seed = 0x27cc72eefa138e4c, .hash = 0xfbf8f7a3ecac1eb7, .b64 = "YU4e7G6EfQYvxCFoCrrT0EFgVLHFfOWRTJQJ5gxM3G2b+1kJf9YPrpsxF6Xr6nYtS8reEEbDoZJYqnlk9lXSkVArm88Cqn6d25VCx3+49MqC0trIlXtb7SXUUhwpJK16T0hJUfPH7s5cMZXc6YmmbFuBNPE=" },
        .{ .seed = 0x44bc2dfba4bd3ced, .hash = 0xb6fc4fcd0722e3df, .b64 = "/I/eImMwPo1U6wekNFD1Jxjk9XQVi1D+FPdqcHifYXQuP5aScNQfxMAmaPR2XhuOQhADV5tTVbBKwCDCX4E3jcDNHzCiPvViZF1W27txaf2BbFQdwKrNCmrtzcluBFYu0XZfc7RU1RmxK/RtnF1qHsq/O4pp" },
        .{ .seed = 0x242da1e3a439bed8, .hash = 0x7cb86dcc55104aac, .b64 = "CJTT9WGcY2XykTdo8KodRIA29qsqY0iHzWZRjKHb9alwyJ7RZAE3V5Juv4MY3MeYEr1EPCCMxO7yFXqT8XA8YTjaMp3bafRt17Pw8JC4iKJ1zN+WWKOESrj+3aluGQqn8z1EzqY4PH7rLG575PYeWsP98BugdA==" },
        .{ .seed = 0xdc559c746e35c139, .hash = 0x19e71e9b45c3a51e, .b64 = "ZlhyQwLhXQyIUEnMH/AEW27vh9xrbNKJxpWGtrEmKhd+nFqAfbeNBQjW0SfG1YI0xQkQMHXjuTt4P/EpZRtA47ibZDVS8TtaxwyBjuIDwqcN09eCtpC+Ls+vWDTLmBeDM3u4hmzz4DQAYsLiZYSJcldg9Q3wszw=" },
        .{ .seed = 0xd0b0350275b9989, .hash = 0x51de38573c2bea48, .b64 = "v2KU8y0sCrBghmnm8lzGJlwo6D6ObccAxCf10heoDtYLosk4ztTpLlpSFEyu23MLA1tJkcgRko04h19QMG0mOw/wc93EXAweriBqXfvdaP85sZABwiKO+6rtS9pacRVpYYhHJeVTQ5NzrvBvi1huxAr+xswhVMfL" },
        .{ .seed = 0xb04489e41d17730c, .hash = 0xa73ab6996d6df158, .b64 = "QhKlnIS6BuVCTQsnoE67E/yrgogE8EwO7xLaEGei26m0gEU4OksefJgppDh3X0x0Cs78Dr9IHK5b977CmZlrTRmwhlP8pM+UzXPNRNIZuN3ntOum/QhUWP8SGpirheXENWsXMQ/nxtxakyEtrNkKk471Oov9juP8oQ==" },
        .{ .seed = 0x2217285eb4572156, .hash = 0x55ef2b8c930817b2, .b64 = "/ZRMgnoRt+Uo6fUPr9FqQvKX7syhgVqWu+WUSsiQ68UlN0efSP6Eced5gJZL6tg9gcYJIkhjuQNITU0Q3TjVAnAcobgbJikCn6qZ6pRxKBY4MTiAlfGD3T7R7hwJwx554MAy++Zb/YUFlnCaCJiwQMnowF7aQzwYFCo=" },
        .{ .seed = 0x12c2e8e68aede73b, .hash = 0xb2850bf5fae87157, .b64 = "NB7tU5fNE8nI+SXGfipc7sRkhnSkUF1krjeo6k+8FITaAtdyz+o7mONgXmGLulBPH9bEwyYhKNVY0L+njNQrZ9YC2aXsFD3PdZsxAFaBT3VXEzh+NGBTjDASNL3mXyS8Yv1iThGfHoY7T4aR0NYGJ+k+pR6f+KrPC96M" },
        .{ .seed = 0x4d612125bdc4fd00, .hash = 0xecf3de1acd04651f, .b64 = "8T6wrqCtEO6/rwxF6lvMeyuigVOLwPipX/FULvwyu+1wa5sQGav/2FsLHUVn6cGSi0LlFwLewGHPFJDLR0u4t7ZUyM//x6da0sWgOa5hzDqjsVGmjxEHXiaXKW3i4iSZNuxoNbMQkIbVML+DkYu9ND0O2swg4itGeVSzXA==" },
        .{ .seed = 0x81826b553954464e, .hash = 0xcc0a40552559ff32, .b64 = "Ntf1bMRdondtMv1CYr3G80iDJ4WSAlKy5H34XdGruQiCrnRGDBa+eUi7vKp4gp3BBcVGl8eYSasVQQjn7MLvb3BjtXx6c/bCL7JtpzQKaDnPr9GWRxpBXVxKREgMM7d8lm35EODv0w+hQLfVSh8OGs7fsBb68nNWPLeeSOo=" },
        .{ .seed = 0xc2e5d345dc0ddd2d, .hash = 0xc385c374f20315b1, .b64 = "VsSAw72Ro6xks02kaiLuiTEIWBC5bgqr4WDnmP8vglXzAhixk7td926rm9jNimL+kroPSygZ9gl63aF5DCPOACXmsbmhDrAQuUzoh9ZKhWgElLQsrqo1KIjWoZT5b5QfVUXY9lSIBg3U75SqORoTPq7HalxxoIT5diWOcJQi" },
        .{ .seed = 0x3da6830a9e32631e, .hash = 0xb90208a4c7234183, .b64 = "j+loZ+C87+bJxNVebg94gU0mSLeDulcHs84tQT7BZM2rzDSLiCNxUedHr1ZWJ9ejTiBa0dqy2I2ABc++xzOLcv+//YfibtjKtYggC6/3rv0XCc7xu6d/O6xO+XOBhOWAQ+IHJVHf7wZnDxIXB8AUHsnjEISKj7823biqXjyP3g==" },
        .{ .seed = 0xc9ae5c8759b4877a, .hash = 0x58aa1ca7a4c075d9, .b64 = "f3LlpcPElMkspNtDq5xXyWU62erEaKn7RWKlo540gR6mZsNpK1czV/sOmqaq8XAQLEn68LKj6/cFkJukxRzCa4OF1a7cCAXYFp9+wZDu0bw4y63qbpjhdCl8GO6Z2lkcXy7KOzbPE01ukg7+gN+7uKpoohgAhIwpAKQXmX5xtd0=" },
    }) |case| {
        var buffer: [0x100]u8 = undefined;

        const b64 = std.base64.standard;
        const input = buffer[0..try b64.Decoder.calcSizeForSlice(case.b64)];
        try b64.Decoder.decode(input, case.b64);

        const hash = low_level_hash(case.seed, input);
        try std.testing.expectEqual(case.hash, hash);
    }
}

const math = std.math;
const mem = std.mem;

fn insertionContext(a: usize, b: usize, context: anytype) void {
    var i = a + 1;
    while (i < b) : (i += 1) {
        var j = i;
        while (j > a and context.lessThan(j, j - 1)) : (j -= 1) {
            context.swap(j, j - 1);
        }
    }
}

fn heapContext(a: usize, b: usize, context: anytype) void {
    // build the heap in linear time.
    var i = b / 2;
    while (i > a) : (i -= 1) {
        siftDown(i - 1, b, context);
    }

    // pop maximal elements from the heap.
    i = b;
    while (i > a) : (i -= 1) {
        context.swap(a, i - 1);
        siftDown(a, i - 1, context);
    }
}

fn siftDown(root: usize, n: usize, context: anytype) void {
    var node = root;
    while (true) {
        var child = 2 * node + 1;
        if (child >= n) break;

        // choose the greater child.
        if (child + 1 < n and context.lessThan(child, child + 1)) {
            child += 1;
        }

        // stop if the invariant holds at `node`.
        if (!context.lessThan(node, child)) break;

        // swap `node` with the greater child,
        // move one step down, and continue sifting.
        context.swap(node, child);
        node = child;
    }
}

const Hint = enum {
    increasing,
    decreasing,
    unknown,
};

/// Unstable in-place sort. O(n) best case, O(n*log(n)) worst case and average case.
/// O(log(n)) memory (no allocator required).
///
/// Sorts in ascending order with respect to the given `lessThan` function.
pub fn pdqContext(a: usize, b: usize, context: anytype) void {
    // slices of up to this length get sorted using insertion sort.
    const max_insertion = 24;
    // number of allowed imbalanced partitions before switching to heap sort.
    const max_limit = std.math.floorPowerOfTwo(usize, b) + 1;

    // set upper bound on stack memory usage.
    const Range = struct { a: usize, b: usize, limit: usize };
    const stack_size = math.log2(math.maxInt(usize) + 1);
    var stack: [stack_size]Range = undefined;
    var range = Range{ .a = a, .b = b, .limit = max_limit };
    var top: usize = 0;

    while (true) {
        var was_balanced = true;
        var was_partitioned = true;

        while (true) {
            const len = range.b - range.a;

            // very short slices get sorted using insertion sort.
            if (len <= max_insertion) {
                break insertionContext(range.a, range.b, context);
            }

            // if too many bad pivot choices were made, simply fall back to heapsort in order to
            // guarantee O(n*log(n)) worst-case.
            if (range.limit == 0) {
                break heapContext(range.a, range.b, context);
            }

            // if the last partitioning was imbalanced, try breaking patterns in the slice by shuffling
            // some elements around. Hopefully we'll choose a better pivot this time.
            if (!was_balanced) {
                breakPatterns(range.a, range.b, context);
                range.limit -= 1;
            }

            // choose a pivot and try guessing whether the slice is already sorted.
            var pivot: usize = 0;
            var hint = chosePivot(range.a, range.b, &pivot, context);

            if (hint == .decreasing) {
                // The maximum number of swaps was performed, so items are likely
                // in reverse order. Reverse it to make sorting faster.
                reverseRange(range.a, range.b, context);
                pivot = (range.b - 1) - (pivot - range.a);
                hint = .increasing;
            }

            // if the last partitioning was decently balanced and didn't shuffle elements, and if pivot
            // selection predicts the slice is likely already sorted...
            if (was_balanced and was_partitioned and hint == .increasing) {
                // try identifying several out-of-order elements and shifting them to correct
                // positions. If the slice ends up being completely sorted, we're done.
                if (partialInsertionSort(range.a, range.b, context)) break;
            }

            // if the chosen pivot is equal to the predecessor, then it's the smallest element in the
            // slice. Partition the slice into elements equal to and elements greater than the pivot.
            // This case is usually hit when the slice contains many duplicate elements.
            if (range.a > 0 and !context.lessThan(range.a - 1, pivot)) {
                range.a = partitionEqual(range.a, range.b, pivot, context);
                continue;
            }

            // partition the slice.
            var mid = pivot;
            was_partitioned = partition(range.a, range.b, &mid, context);

            const left_len = mid - range.a;
            const right_len = range.b - mid;
            const balanced_threshold = len / 8;
            if (left_len < right_len) {
                was_balanced = left_len >= balanced_threshold;
                stack[top] = .{ .a = range.a, .b = mid, .limit = range.limit };
                top += 1;
                range.a = mid + 1;
            } else {
                was_balanced = right_len >= balanced_threshold;
                stack[top] = .{ .a = mid + 1, .b = range.b, .limit = range.limit };
                top += 1;
                range.b = mid;
            }
        }

        top = math.sub(usize, top, 1) catch break;
        range = stack[top];
    }
}

/// partitions `items[a..b]` into elements smaller than `items[pivot]`,
/// followed by elements greater than or equal to `items[pivot]`.
///
/// sets the new pivot.
/// returns `true` if already partitioned.
fn partition(a: usize, b: usize, pivot: *usize, context: anytype) bool {
    // move pivot to the first place
    context.swap(a, pivot.*);

    var i = a + 1;
    var j = b - 1;

    while (i <= j and context.lessThan(i, a)) i += 1;
    while (i <= j and !context.lessThan(j, a)) j -= 1;

    // check if items are already partitioned (no item to swap)
    if (i > j) {
        // put pivot back to the middle
        context.swap(j, a);
        pivot.* = j;
        return true;
    }

    context.swap(i, j);
    i += 1;
    j -= 1;

    while (true) {
        while (i <= j and context.lessThan(i, a)) i += 1;
        while (i <= j and !context.lessThan(j, a)) j -= 1;
        if (i > j) break;

        context.swap(i, j);
        i += 1;
        j -= 1;
    }

    // TODO: Enable the BlockQuicksort optimization

    context.swap(j, a);
    pivot.* = j;
    return false;
}

/// partitions items into elements equal to `items[pivot]`
/// followed by elements greater than `items[pivot]`.
///
/// it assumed that `items[a..b]` does not contain elements smaller than the `items[pivot]`.
fn partitionEqual(a: usize, b: usize, pivot: usize, context: anytype) usize {
    // move pivot to the first place
    context.swap(a, pivot);

    var i = a + 1;
    var j = b - 1;

    while (true) {
        while (i <= j and !context.lessThan(a, i)) i += 1;
        while (i <= j and context.lessThan(a, j)) j -= 1;
        if (i > j) break;

        context.swap(i, j);
        i += 1;
        j -= 1;
    }

    return i;
}

/// partially sorts a slice by shifting several out-of-order elements around.
///
/// returns `true` if the slice is sorted at the end. This function is `O(n)` worst-case.
fn partialInsertionSort(a: usize, b: usize, context: anytype) bool {
    @setCold(true);

    // maximum number of adjacent out-of-order pairs that will get shifted
    const max_steps = 5;
    // if the slice is shorter than this, don't shift any elements
    const shortest_shifting = 50;

    var i = a + 1;
    for (@as([max_steps]u0, undefined)) |_| {
        // find the next pair of adjacent out-of-order elements.
        while (i < b and !context.lessThan(i, i - 1)) i += 1;

        // are we done?
        if (i == b) return true;

        // don't shift elements on short arrays, that has a performance cost.
        if (b - a < shortest_shifting) return false;

        // swap the found pair of elements. This puts them in correct order.
        context.swap(i, i - 1);

        // shift the smaller element to the left.
        if (i - a >= 2) {
            var j = i - 1;
            while (j >= 1) : (j -= 1) {
                if (!context.lessThan(j, j - 1)) break;
                context.swap(j, j - 1);
            }
        }

        // shift the greater element to the right.
        if (b - i >= 2) {
            var j = i + 1;
            while (j < b) : (j += 1) {
                if (!context.lessThan(j, j - 1)) break;
                context.swap(j, j - 1);
            }
        }
    }

    return false;
}

fn breakPatterns(a: usize, b: usize, context: anytype) void {
    @setCold(true);

    const len = b - a;
    if (len < 8) return;

    var rand = @intCast(u64, len);
    const modulus = math.ceilPowerOfTwoAssert(u64, len);

    var i = a + (len / 4) * 2 - 1;
    while (i <= a + (len / 4) * 2 + 1) : (i += 1) {
        // xorshift64
        rand ^= rand << 13;
        rand ^= rand >> 7;
        rand ^= rand << 17;

        var other = @intCast(usize, rand & (modulus - 1));
        if (other >= len) other -= len;
        context.swap(i, a + other);
    }
}

/// choses a pivot in `items[a..b]`.
/// swaps likely_sorted when `items[a..b]` seems to be already sorted.
fn chosePivot(a: usize, b: usize, pivot: *usize, context: anytype) Hint {
    // minimum length for using the Tukey's ninther method
    const shortest_ninther = 50;
    // max_swaps is the maximum number of swaps allowed in this function
    const max_swaps = 4 * 3;

    var len = b - a;
    var i = a + len / 4 * 1;
    var j = a + len / 4 * 2;
    var k = a + len / 4 * 3;
    var swaps: usize = 0;

    if (len >= 8) {
        if (len >= shortest_ninther) {
            // find medians in the neighborhoods of `i`, `j` and `k`
            i = sort3(i - 1, i, i + 1, &swaps, context);
            j = sort3(j - 1, j, j + 1, &swaps, context);
            k = sort3(k - 1, k, k + 1, &swaps, context);
        }

        // find the median among `i`, `j` and `k`
        j = sort3(i, j, k, &swaps, context);
    }

    pivot.* = j;
    return switch (swaps) {
        0 => .increasing,
        max_swaps => .decreasing,
        else => .unknown,
    };
}

fn sort3(a: usize, b: usize, c: usize, swaps: *usize, context: anytype) usize {
    if (context.lessThan(b, a)) {
        swaps.* += 1;
        context.swap(b, a);
    }

    if (context.lessThan(c, b)) {
        swaps.* += 1;
        context.swap(c, b);
    }

    if (context.lessThan(b, a)) {
        swaps.* += 1;
        context.swap(b, a);
    }

    return b;
}

fn reverseRange(a: usize, b: usize, context: anytype) void {
    var i = a;
    var j = b - 1;
    while (i < j) {
        context.swap(i, j);
        i += 1;
        j -= 1;
    }
}
