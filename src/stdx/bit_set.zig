const stdx = @import("stdx.zig");
const std = @import("std");
const assert = std.debug.assert;

/// Use a dynamic bitset for larger sizes.
pub fn BitSetType(comptime with_capacity: u9) type {
    assert(with_capacity <= 256);

    return struct {
        // While mathematically 0 and 1 are symmetric, we intentionally bias the API to use zeros
        // default, as zero-initialization reduces binary size.
        bits: Word = 0,

        pub const Word = for (.{ u8, u16, u32, u64, u128, u256 }) |w| {
            if (@bitSizeOf(w) >= with_capacity) break w;
        } else unreachable;

        const BitSet = @This();

        pub fn is_set(bit_set: BitSet, index: usize) bool {
            assert(index < bit_set.capacity());
            return bit_set.bits & bit(index) != 0;
        }

        pub fn count(bit_set: BitSet) usize {
            return @popCount(bit_set.bits);
        }

        pub inline fn capacity(_: BitSet) usize {
            return with_capacity;
        }

        pub fn full(bit_set: BitSet) bool {
            return bit_set.count() == bit_set.capacity();
        }

        pub fn empty(bit_set: BitSet) bool {
            return bit_set.bits == 0;
        }

        pub fn first_set(bit_set: BitSet) ?usize {
            if (bit_set.bits == 0) return null;
            return @ctz(bit_set.bits);
        }

        pub fn first_unset(bit_set: BitSet) ?usize {
            const result = @ctz(~bit_set.bits);
            return if (result < bit_set.capacity()) result else null;
        }

        pub fn set(bit_set: *BitSet, index: usize) void {
            assert(index < bit_set.capacity());
            bit_set.bits |= bit(index);
        }

        pub fn unset(bit_set: *BitSet, index: usize) void {
            assert(index < bit_set.capacity());
            bit_set.bits &= ~bit(index);
        }

        pub fn set_value(bit_set: *BitSet, index: usize, value: bool) void {
            if (value) {
                bit_set.set(index);
            } else {
                bit_set.unset(index);
            }
        }

        fn bit(index: usize) Word {
            assert(index < with_capacity);
            return @as(Word, 1) << @intCast(index);
        }

        pub fn iterate(bit_set: BitSet) Iterator {
            return .{ .bits_remain = bit_set.bits };
        }

        pub const Iterator = struct {
            bits_remain: Word,

            pub fn next(it: *@This()) ?usize {
                const result = @ctz(it.bits_remain);
                if (result >= with_capacity) return null;
                it.bits_remain &= it.bits_remain - 1;
                return result;
            }
        };
    };
}

test BitSetType {
    var prng = stdx.PRNG.from_seed_testing();
    inline for (.{ 0, 1, 8, 32, 65, 255, 256 }) |N| {
        const BitSet = BitSetType(N);

        var set: BitSet = .{};
        var model = try std.DynamicBitSetUnmanaged.initEmpty(std.testing.allocator, N);
        defer model.deinit(std.testing.allocator);

        for (0..1000) |_| {
            switch (prng.enum_uniform(std.meta.DeclEnum(BitSet))) {
                .Word => {
                    const bit_size =
                        comptime if (N == 0) 8 else @max(8, try std.math.ceilPowerOfTwo(u16, N));
                    assert(BitSet.Word == std.meta.Int(.unsigned, bit_size));
                },
                .Iterator => {},
                .is_set => {
                    if (N > 0) {
                        const bit = prng.int_inclusive(usize, N - 1);
                        assert(set.is_set(bit) == model.isSet(bit));
                    }
                },
                .count => assert(set.count() == model.count()),
                .capacity => assert(set.capacity() == N),
                .full => assert(set.full() == (model.count() == N)),
                .empty => assert(set.empty() == (model.count() == 0)),
                .first_set => assert(set.first_set() == model.findFirstSet()),
                .first_unset => {
                    var it = model.iterator(.{ .kind = .unset });
                    assert(set.first_unset() == it.next());
                },
                .set => {
                    if (N > 0) {
                        const bit = prng.int_inclusive(usize, N - 1);
                        set.set(bit);
                        model.set(bit);
                    }
                },
                .unset => {
                    if (N > 0) {
                        const bit = prng.int_inclusive(usize, N - 1);
                        set.unset(bit);
                        model.unset(bit);
                    }
                },
                .set_value => {
                    if (N > 0) {
                        const bit = prng.int_inclusive(usize, N - 1);
                        const value = prng.boolean();
                        set.set_value(bit, value);
                        model.setValue(bit, value);
                    }
                },
                .iterate => {
                    var it_set = set.iterate();
                    var it_model = model.iterator(.{});
                    while (it_model.next()) |next| {
                        assert(next == it_set.next());
                    }
                    assert(it_set.next() == null);
                    assert(it_set.next() == null);
                },
            }
        }
    }
}
