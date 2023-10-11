const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("../stdx.zig");

pub fn CompositeKeyType(comptime Field: type) type {
    // The type if zeroed padding is needed.
    const pad = switch (Field) {
        u128 => @as(u64, 0),
        // [0]u8 as zero-sized-type workaround for https://github.com/ziglang/zig/issues/16394.
        u64 => [0]u8{},
        else => @compileError("invalid Field for CompositeKey: " ++ @typeName(Field)),
    };
    const Pad = @TypeOf(pad);

    return extern struct {
        const Self = @This();

        pub const sentinel_key: Key = key_from_value(&.{
            .field = math.maxInt(Field),
            .timestamp = math.maxInt(u64),
        });

        const tombstone_bit: u64 = 1 << 63;

        // u128 may be aligned to 8 instead of the expected 16.
        const field_bitsize_alignment = @divExact(@bitSizeOf(Field), 8);

        pub const Key = std.meta.Int(
            .unsigned,
            @bitSizeOf(u64) + @bitSizeOf(Field) + @bitSizeOf(Pad),
        );

        field: Field align(field_bitsize_alignment),
        /// The most significant bit must be unset as it is used to indicate a tombstone.
        timestamp: u64,
        padding: Pad = pad,

        comptime {
            assert(@sizeOf(Self) == @sizeOf(Field) * 2);
            assert(@sizeOf(Self) == @sizeOf(Key));
            assert(@alignOf(Self) >= @alignOf(Field));
            assert(@alignOf(Self) == field_bitsize_alignment);
            assert(stdx.no_padding(Self));
        }

        pub inline fn key_from_value(value: *const Self) Key {
            return @as(Key, value.timestamp & ~tombstone_bit) | (@as(Key, value.field) << 64);
        }

        pub inline fn tombstone(value: *const Self) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(key: Key) Self {
            const timestamp: u64 = @truncate(key);
            const field: Field = @truncate(key >> 64);
            assert(timestamp & tombstone_bit == 0);

            return .{
                .field = field,
                .timestamp = timestamp | tombstone_bit,
            };
        }
    };
}
