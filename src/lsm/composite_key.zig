const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

pub fn CompositeKey(comptime Secondary: type) type {
    assert(Secondary == u128 or Secondary == u64);

    return extern struct {
        const Self = @This();

        pub const sentinel_key: Self = .{
            .secondary = math.maxInt(Secondary),
            .timestamp = math.maxInt(u64),
        };

        const tombstone_bit = 1 << 63;
        // If zeroed padding is needed after the timestamp field
        const pad = Secondary == u128;

        pub const Value = extern struct {
            secondary: Secondary,
            /// The most significant bit indicates if the value is a tombstone
            timestamp: u64,
            padding: (if (pad) u64 else void) = (if (pad) 0 else {}),

            comptime {
                assert(@sizeOf(Value) == @sizeOf(Secondary) * 2);
                assert(@alignOf(Value) == @alignOf(Secondary));
            }
        };

        secondary: Secondary,
        /// The most significant bit must be unset as it is used to indicate a tombstone
        timestamp: u64,
        padding: (if (pad) u64 else void) = (if (pad) 0 else {}),

        comptime {
            assert(@sizeOf(Self) == @sizeOf(Secondary) * 2);
            assert(@alignOf(Self) == @alignOf(Secondary));
        }

        // TODO: consider optimizing this by reinterpreting the raw memory in an advantageous way
        // This may require modifying the struct layout.
        pub fn compare_keys(a: Self, b: Self) math.Order {
            if (a.secondary < b.secondary) {
                return .lt;
            } else if (a.secondary > b.secondary) {
                return .gt;
            } else if (a.timestamp < b.timestamp) {
                return .lt;
            } else if (a.timestamp > b.timestamp) {
                return .gt;
            } else {
                return .eq;
            }
        }

        pub fn key_from_value(value: Value) Self {
            return .{
                .secondary = value.secondary,
                .timestamp = @truncate(u63, value.timestamp),
            };
        }

        pub fn tombstone(value: Value) bool {
            return value.timestamp & tombstone_bit != 0;
        }

        pub fn tombstone_from_key(key: Self) Value {
            return .{
                .secondary = key.secondary,
                .timestamp = key.timestamp | tombstone_bit,
            };
        }
    };
}
