//! Extensions to the standard library -- things which could have been in std, but aren't.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

pub const BoundedArray = @import("./stdx/bounded_array.zig").BoundedArray;

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
        assert(@intFromPtr(target.ptr) < @intFromPtr(source.ptr));
    }
    std.mem.copy(T, target, source);
}

test "copy_left" {
    const a = try std.testing.allocator.alloc(usize, 8);
    defer std.testing.allocator.free(a);

    for (a, 0..) |*v, i| v.* = i;
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
        assert(@intFromPtr(target.ptr) > @intFromPtr(source.ptr));
    }
    std.mem.copyBackwards(T, target, source);
}

test "copy_right" {
    const a = try std.testing.allocator.alloc(usize, 8);
    defer std.testing.allocator.free(a);

    for (a, 0..) |*v, i| v.* = i;
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
    return @intFromPtr(a.ptr) + a.len * @sizeOf(A) <= @intFromPtr(b.ptr) or
        @intFromPtr(b.ptr) + b.len * @sizeOf(B) <= @intFromPtr(a.ptr);
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

/// Checks that a byteslice is zeroed.
pub fn zeroed(bytes: []const u8) bool {
    // This implementation already gets vectorized
    // https://godbolt.org/z/46cMsPKPc
    var byte_bits: u8 = 0;
    for (bytes) |byte| {
        byte_bits |= byte;
    }
    return byte_bits == 0;
}

const Cut = struct {
    prefix: []const u8,
    suffix: []const u8,
};

/// Splits the `haystack` around the first occurrence of `needle`, returning parts before and after.
///
/// This is a Zig version of Go's `string.Cut` / Rust's `str::split_once`. Cut turns out to be a
/// surprisingly versatile primitive for ad-hoc string processing. Often `std.mem.indexOf` and
/// `std.mem.split` can be replaced with a shorter and clearer code using  `cut`.
pub fn cut(haystack: []const u8, needle: []const u8) ?Cut {
    const index = std.mem.indexOf(u8, haystack, needle) orelse return null;

    return Cut{
        .prefix = haystack[0..index],
        .suffix = haystack[index + needle.len ..],
    };
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

    /// Prints elapsed time to stderr and resets the internal timer.
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

/// Compare two values by directly comparing the underlying memory.
///
/// Assert at compile time that this is a reasonable thing to do for a given `T`. That is, check
/// that:
///   - `T` doesn't have any non-deterministic padding,
///   - `T` doesn't embed any pointers.
pub fn equal_bytes(comptime T: type, a: *const T, b: *const T) bool {
    comptime assert(has_unique_representation(T));
    comptime assert(!has_pointers(T));
    comptime assert(@sizeOf(T) * 8 == @bitSizeOf(T));

    // Pick the biggest "word" for word-wise comparison, and don't try to early-return on the first
    // mismatch, so that a compiler can vectorize the loop.

    const Word = inline for (.{ u64, u32, u16, u8 }) |Word| {
        if (@alignOf(T) >= @alignOf(Word) and @sizeOf(T) % @sizeOf(Word) == 0) break Word;
    } else unreachable;

    const a_words = std.mem.bytesAsSlice(Word, std.mem.asBytes(a));
    const b_words = std.mem.bytesAsSlice(Word, std.mem.asBytes(b));
    assert(a_words.len == b_words.len);

    var total: Word = 0;
    for (a_words, 0..) |a_word, i| {
        const b_word = b_words[i];
        total |= a_word ^ b_word;
    }

    return total == 0;
}

fn has_pointers(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .Pointer => return true,
        // Be conservative.
        else => return true,

        .Bool, .Int, .Enum => return false,

        .Array => |info| return comptime has_pointers(info.child),
        .Struct => |info| {
            inline for (info.fields) |field| {
                if (comptime has_pointers(field.type)) return true;
            }
            return false;
        },
    }
}

/// Checks that a type does not have implicit padding.
pub fn no_padding(comptime T: type) bool {
    comptime switch (@typeInfo(T)) {
        .Int => return @bitSizeOf(T) == 8 * @sizeOf(T),
        .Array => |info| return no_padding(info.child),
        .Struct => |info| {
            switch (info.layout) {
                .Auto => return false,
                .Extern => {
                    for (info.fields) |field| {
                        if (!no_padding(field.type)) return false;
                    }

                    // Check offsets of u128 and pseudo-u256 fields.
                    for (info.fields) |field| {
                        if (field.type == u128) {
                            const offset = @offsetOf(T, field.name);
                            if (offset % @sizeOf(u128) != 0) return false;

                            if (@hasField(T, field.name ++ "_padding")) {
                                if (offset % @sizeOf(u256) != 0) return false;
                                if (offset + @sizeOf(u128) !=
                                    @offsetOf(T, field.name ++ "_padding"))
                                {
                                    return false;
                                }
                            }
                        }
                    }

                    var offset = 0;
                    for (info.fields) |field| {
                        const field_offset = @offsetOf(T, field.name);
                        if (offset != field_offset) return false;
                        offset += @sizeOf(field.type);
                    }
                    return offset == @sizeOf(T);
                },
                .Packed => return @bitSizeOf(T) == 8 * @sizeOf(T),
            }
        },
        .Enum => |info| {
            maybe(info.is_exhaustive);
            return no_padding(info.tag_type);
        },
        .Pointer => return false,
        .Union => return false,
        else => return false,
    };
}

test no_padding {
    comptime for (.{
        u8,
        extern struct { x: u8 },
        packed struct { x: u7, y: u1 },
        extern struct { x: extern struct { y: u64, z: u64 } },
        enum(u8) { x },
    }) |T| {
        assert(no_padding(T));
    };

    comptime for (.{
        u7,
        struct { x: u7 },
        struct { x: u8 },
        struct { x: u64, y: u32 },
        extern struct { x: extern struct { y: u64, z: u32 } },
        packed struct { x: u7 },
        enum(u7) { x },
    }) |T| {
        assert(!no_padding(T));
    };
}

pub inline fn hash_inline(value: anytype) u64 {
    comptime {
        assert(no_padding(@TypeOf(value)));
        assert(has_unique_representation(@TypeOf(value)));
    }
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
            for (@as([2][4]u64, @bitCast(in[0..64].*)), 0..) |chunk, i| {
                const mix1 = @as(u128, chunk[0] ^ salt[(i * 2) + 1]) *% (chunk[1] ^ dup[i]);
                const mix2 = @as(u128, chunk[2] ^ salt[(i * 2) + 2]) *% (chunk[3] ^ dup[i]);
                dup[i] = @as(u64, @truncate(mix1 ^ (mix1 >> 64)));
                dup[i] ^= @as(u64, @truncate(mix2 ^ (mix2 >> 64)));
            }
        }
    }

    while (in.len > 16) : (in = in[16..]) {
        const chunk = @as([2]u64, @bitCast(in[0..16].*));
        const mixed = @as(u128, chunk[0] ^ salt[1]) *% (chunk[1] ^ state);
        state = @as(u64, @truncate(mixed ^ (mixed >> 64)));
    }

    var chunk = std.mem.zeroes([2]u64);
    if (in.len > 8) {
        chunk[0] = @as(u64, @bitCast(in[0..8].*));
        chunk[1] = @as(u64, @bitCast(in[in.len - 8 ..][0..8].*));
    } else if (in.len > 3) {
        chunk[0] = @as(u32, @bitCast(in[0..4].*));
        chunk[1] = @as(u32, @bitCast(in[in.len - 4 ..][0..4].*));
    } else if (in.len > 0) {
        chunk[0] = (@as(u64, in[0]) << 16) | (@as(u64, in[in.len / 2]) << 8) | in[in.len - 1];
    }

    var mixed = @as(u128, chunk[0] ^ salt[1]) *% (chunk[1] ^ state);
    mixed = @as(u64, @truncate(mixed ^ (mixed >> 64)));
    mixed *%= (@as(u64, starting_len) ^ salt[1]);
    return @as(u64, @truncate(mixed ^ (mixed >> 64)));
}

test "hash_inline" {
    for (@import("testing/low_level_hash_vectors.zig").cases) |case| {
        var buffer: [0x100]u8 = undefined;

        const b64 = std.base64.standard;
        const input = buffer[0..try b64.Decoder.calcSizeForSlice(case.b64)];
        try b64.Decoder.decode(input, case.b64);

        const hash = low_level_hash(case.seed, input);
        try std.testing.expectEqual(case.hash, hash);
    }
}

/// Returns a copy of `base` with fields changed according to `diff`.
///
/// Intended exclusively for table-driven prototype-based tests. Write
/// updates explicitly in production code.
pub fn update(base: anytype, diff: anytype) @TypeOf(base) {
    assert(builtin.is_test);
    assert(@typeInfo(@TypeOf(base)) == .Struct);

    var updated = base;
    inline for (std.meta.fields(@TypeOf(diff))) |f| {
        @field(updated, f.name) = @field(diff, f.name);
    }
    return updated;
}

// std.SemanticVersion requires there be no extra characters after the
// major/minor/patch numbers. But when we try to parse `uname
// --kernel-release` (note: while Linux doesn't follow semantic
// versioning, it doesn't violate it either), some distributions have
// extra characters, such as this Fedora one: 6.3.8-100.fc37.x86_64, and
// this WSL one has more than three dots:
// 5.15.90.1-microsoft-standard-WSL2.
pub fn parse_dirty_semver(dirty_release: []const u8) !std.SemanticVersion {
    const release = blk: {
        var last_valid_version_character_index: usize = 0;
        var dots_found: u8 = 0;
        for (dirty_release) |c| {
            if (c == '.') dots_found += 1;
            if (dots_found == 3) {
                break;
            }

            if (c == '.' or (c >= '0' and c <= '9')) {
                last_valid_version_character_index += 1;
                continue;
            }

            break;
        }

        break :blk dirty_release[0..last_valid_version_character_index];
    };

    return std.SemanticVersion.parse(release);
}

test "stdx.zig: parse_dirty_semver" {
    const SemverTestCase = struct {
        dirty_release: []const u8,
        expected_version: std.SemanticVersion,
    };

    const cases = &[_]SemverTestCase{
        .{
            .dirty_release = "1.2.3",
            .expected_version = std.SemanticVersion{ .major = 1, .minor = 2, .patch = 3 },
        },
        .{
            .dirty_release = "1001.843.909",
            .expected_version = std.SemanticVersion{ .major = 1001, .minor = 843, .patch = 909 },
        },
        .{
            .dirty_release = "6.3.8-100.fc37.x86_64",
            .expected_version = std.SemanticVersion{ .major = 6, .minor = 3, .patch = 8 },
        },
        .{
            .dirty_release = "5.15.90.1-microsoft-standard-WSL2",
            .expected_version = std.SemanticVersion{ .major = 5, .minor = 15, .patch = 90 },
        },
    };
    for (cases) |case| {
        const version = try parse_dirty_semver(case.dirty_release);
        try std.testing.expectEqual(case.expected_version, version);
    }
}

// TODO(zig): Zig 0.11 doesn't have the statfs / fstatfs syscalls to get the type of a filesystem.
// Once those are available, this can be removed.
// The `statfs` definition used by the Linux kernel, and the magic number for tmpfs, from
// `man 2 fstatfs`.
const fsblkcnt64_t = u64;
const fsfilcnt64_t = u64;
const fsword_t = i64;
const fsid_t = u64;

pub const TmpfsMagic = 0x01021994;
pub const StatFs = extern struct {
    f_type: fsword_t,
    f_bsize: fsword_t,
    f_blocks: fsblkcnt64_t,
    f_bfree: fsblkcnt64_t,
    f_bavail: fsblkcnt64_t,
    f_files: fsfilcnt64_t,
    f_ffree: fsfilcnt64_t,
    f_fsid: fsid_t,
    f_namelen: fsword_t,
    f_frsize: fsword_t,
    f_flags: fsword_t,
    f_spare: [4]fsword_t,
};

pub fn fstatfs(fd: i32, statfs_buf: *StatFs) usize {
    return std.os.linux.syscall2(
        if (@hasField(std.os.linux.SYS, "fstatfs64")) .fstatfs64 else .fstatfs,
        @as(usize, @bitCast(@as(isize, fd))),
        @intFromPtr(statfs_buf),
    );
}

// TODO(Zig): https://github.com/ziglang/zig/issues/17592.
/// True if every value of the type `T` has a unique bit pattern representing it.
/// In other words, `T` has no unused bits and no padding.
pub fn has_unique_representation(comptime T: type) bool {
    switch (@typeInfo(T)) {
        else => return false, // TODO can we know if it's true for some of these types ?

        .AnyFrame,
        .Enum,
        .ErrorSet,
        .Fn,
        => return true,

        .Bool => return false,

        .Int => |info| return @sizeOf(T) * 8 == info.bits,

        .Pointer => |info| return info.size != .Slice,

        .Array => |info| return comptime has_unique_representation(info.child),

        .Struct => |info| {
            // Only consider packed structs unique if they are byte aligned.
            if (info.backing_integer) |backing_integer| {
                return @sizeOf(T) * 8 == @bitSizeOf(backing_integer);
            }

            var sum_size = @as(usize, 0);

            inline for (info.fields) |field| {
                const FieldType = field.type;
                if (comptime !has_unique_representation(FieldType)) return false;
                sum_size += @sizeOf(FieldType);
            }

            return @sizeOf(T) == sum_size;
        },

        .Vector => |info| return comptime has_unique_representation(info.child) and
            @sizeOf(T) == @sizeOf(info.child) * info.len,
    }
}

// Test vectors mostly from upstream, with some added to test the packed struct case.
test "has_unique_representation" {
    const TestStruct1 = struct {
        a: u32,
        b: u32,
    };

    try std.testing.expect(has_unique_representation(TestStruct1));

    const TestStruct2 = struct {
        a: u32,
        b: u16,
    };

    try std.testing.expect(!has_unique_representation(TestStruct2));

    const TestStruct3 = struct {
        a: u32,
        b: u32,
    };

    try std.testing.expect(has_unique_representation(TestStruct3));

    const TestStruct4 = struct { a: []const u8 };

    try std.testing.expect(!has_unique_representation(TestStruct4));

    const TestStruct5 = struct { a: TestStruct4 };

    try std.testing.expect(!has_unique_representation(TestStruct5));

    const TestStruct6 = packed struct {
        a: u32,
        b: u31,
    };

    try std.testing.expect(!has_unique_representation(TestStruct6));

    const TestStruct7 = struct {
        a: u64,
        b: TestStruct6,
    };

    try std.testing.expect(!has_unique_representation(TestStruct7));

    const TestStruct8 = packed struct {
        a: u32,
        b: u32,
    };

    try std.testing.expect(has_unique_representation(TestStruct8));

    const TestStruct9 = struct {
        a: u64,
        b: TestStruct8,
    };

    try std.testing.expect(has_unique_representation(TestStruct9));

    const TestStruct10 = packed struct {
        a: TestStruct8,
        b: TestStruct8,
    };

    try std.testing.expect(has_unique_representation(TestStruct10));

    const TestUnion1 = packed union {
        a: u32,
        b: u16,
    };

    try std.testing.expect(!has_unique_representation(TestUnion1));

    const TestUnion2 = extern union {
        a: u32,
        b: u16,
    };

    try std.testing.expect(!has_unique_representation(TestUnion2));

    const TestUnion3 = union {
        a: u32,
        b: u16,
    };

    try std.testing.expect(!has_unique_representation(TestUnion3));

    const TestUnion4 = union(enum) {
        a: u32,
        b: u16,
    };

    try std.testing.expect(!has_unique_representation(TestUnion4));

    inline for ([_]type{ i0, u8, i16, u32, i64 }) |T| {
        try std.testing.expect(has_unique_representation(T));
    }
    inline for ([_]type{ i1, u9, i17, u33, i24 }) |T| {
        try std.testing.expect(!has_unique_representation(T));
    }

    try std.testing.expect(!has_unique_representation([]u8));
    try std.testing.expect(!has_unique_representation([]const u8));

    try std.testing.expect(has_unique_representation(@Vector(4, u16)));
}

/// Construct a `union(Enum)` type, where each union "value" type is defined in terms of the
/// variant.
///
/// That is, `EnumUnionType(Enum, TypeForVariant)` is equivalent to:
///
///   union(Enum) {
///     // For every `e` in `Enum`:
///     e: TypeForVariant(e),
///   }
///
pub fn EnumUnionType(
    comptime Enum: type,
    comptime TypeForVariant: fn (comptime variant: Enum) type,
) type {
    const UnionField = std.builtin.Type.UnionField;

    var fields: []const UnionField = &[_]UnionField{};
    for (std.enums.values(Enum)) |enum_variant| {
        fields = fields ++ &[_]UnionField{.{
            .name = @tagName(enum_variant),
            .type = TypeForVariant(enum_variant),
            .alignment = @alignOf(TypeForVariant(enum_variant)),
        }};
    }

    return @Type(.{ .Union = .{
        .layout = .Auto,
        .fields = fields,
        .decls = &.{},
        .tag_type = Enum,
    } });
}
