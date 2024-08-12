const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("../stdx.zig");

pub fn CompositeKeyType(comptime Field: type) type {
    // The type if zeroed padding is needed.
    const Pad = switch (Field) {
        u128 => u64,
        u64 => u0,
        else => @compileError("invalid Field for CompositeKey: " ++ @typeName(Field)),
    };

    return extern struct {
        const CompositeKey = @This();

        pub const sentinel_key: Key = key_from_value(&.{
            .field = math.maxInt(Field),
            .timestamp = math.maxInt(u64),
        });

        const tombstone_bit: u64 = 1 << (64 - 1);

        // u128 may be aligned to 8 instead of the expected 16.
        const field_bitsize_alignment = @divExact(@bitSizeOf(Field), 8);

        pub const Key = std.meta.Int(
            .unsigned,
            @bitSizeOf(u64) + @bitSizeOf(Field) + @bitSizeOf(Pad),
        );

        field: Field align(field_bitsize_alignment),
        /// The most significant bit must be unset as it is used to indicate a tombstone.
        timestamp: u64,
        padding: Pad = 0,

        comptime {
            assert(@sizeOf(CompositeKey) == @sizeOf(Field) * 2);
            assert(@sizeOf(CompositeKey) == @sizeOf(Key));
            assert(@alignOf(CompositeKey) >= @alignOf(Field));
            assert(@alignOf(CompositeKey) == field_bitsize_alignment);
            assert(stdx.no_padding(CompositeKey));
        }

        pub inline fn key_from_value(value: *const CompositeKey) Key {
            assert(value.padding == 0);
            return @as(Key, value.timestamp & ~tombstone_bit) | (@as(Key, value.field) << 64);
        }

        pub inline fn key_prefix(key: Key) Field {
            return @truncate(key >> 64);
        }

        pub inline fn tombstone(value: *const CompositeKey) bool {
            assert(value.padding == 0);
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(key: Key) CompositeKey {
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

pub fn is_composite_key(comptime Value: type) bool {
    if (@typeInfo(Value) == .Struct and
        @hasField(Value, "field") and
        @hasField(Value, "timestamp"))
    {
        const Field = std.meta.FieldType(Value, .field);
        return switch (Field) {
            u64, u128 => Value == CompositeKeyType(Field),
            else => false,
        };
    }

    return false;
}

fn composite_key_test(comptime CompositeKey: type) !void {
    {
        const a = CompositeKey.key_from_value(&.{ .field = 1, .timestamp = 100 });
        const b = CompositeKey.key_from_value(&.{ .field = 1, .timestamp = 101 });
        try std.testing.expect(a < b);
    }

    {
        const a = CompositeKey.key_from_value(&.{ .field = 1, .timestamp = 100 });
        const b = CompositeKey.key_from_value(&.{ .field = 2, .timestamp = 99 });
        try std.testing.expect(a < b);
    }

    {
        const a = CompositeKey.key_from_value(&.{
            .field = 1,
            .timestamp = @as(u64, 100) | CompositeKey.tombstone_bit,
        });
        const b = CompositeKey.key_from_value(&.{
            .field = 1,
            .timestamp = 100,
        });
        try std.testing.expect(a == b);
    }

    {
        const key = CompositeKey.key_from_value(&.{ .field = 1, .timestamp = 100 });
        const value = CompositeKey.tombstone_from_key(key);
        try std.testing.expect(CompositeKey.tombstone(&value));
        try std.testing.expect(value.timestamp == @as(u64, 100) | CompositeKey.tombstone_bit);
    }
}

test "composite_key" {
    try composite_key_test(CompositeKeyType(u64));
    try composite_key_test(CompositeKeyType(u128));
}
