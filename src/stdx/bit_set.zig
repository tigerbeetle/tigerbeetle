const std = @import("std");
const assert = std.debug.assert;

/// Take a u8 to limit to 256 items max (2^8 = 256).
/// Use dynamic bitset for larger sizes.
pub fn BitSetType(comptime with_capacity: u8) type {
    return struct {
        // While mathematicaly 0 and 1 are symmetric, we intentionally bias the API to use zeros
        // default, as zero-initalization requces binary size.
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
