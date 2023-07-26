const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("../stdx.zig");

pub fn CompositeKey(comptime Field: type) type {
    assert(Field == u128 or Field == u64);

    return extern struct {
        const Self = @This();

        pub const sentinel_key: Self = .{
            .field = math.maxInt(Field),
            .timestamp = math.maxInt(u64),
        };

        const tombstone_bit = 1 << 63;

        // The type if zeroed padding is needed after the timestamp field.
        const pad = switch (Field) {
            u128 => @as(u64, 0),
            u64 => [0]u8{},
            else => @compileError("invalid Field for CompositeKey: " ++ @typeName(Field)),
        };

        // u128 may be aligned to 8 instead of the expected 16.
        const field_bitsize_alignment = @divExact(@bitSizeOf(Field), 8);

        pub const Value = Self;

        field: Field align(field_bitsize_alignment),
        /// The most significant bit must be unset as it is used to indicate a tombstone.
        timestamp: u64,
        /// [0]u8 as zero-sized-type workaround for https://github.com/ziglang/zig/issues/16394.
        padding: @TypeOf(pad) = pad,

        comptime {
            assert(@sizeOf(Self) == @sizeOf(Field) * 2);
            assert(@alignOf(Self) >= @alignOf(Field));
            assert(@alignOf(Self) == field_bitsize_alignment);
            assert(stdx.no_padding(Self));
        }

        pub inline fn compare_keys(a: Self, b: Self) math.Order {
            var order = std.math.order(a.field, b.field);
            if (order == .eq) {
                order = std.math.order(a.timestamp, b.timestamp);
            }
            return order;
        }

        pub inline fn key_from_value(value: *const Value) Self {
            return .{
                .field = value.field,
                .timestamp = @truncate(u63, value.timestamp),
            };
        }

        pub inline fn tombstone(value: *const Value) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(key: Self) Value {
            return .{
                .field = key.field,
                .timestamp = key.timestamp | tombstone_bit,
            };
        }
    };
}
