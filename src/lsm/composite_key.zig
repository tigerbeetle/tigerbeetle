const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("stdx");

/// Combines a field (the key prefix) with a timestamp (the primary key).
/// - To keep alignment, it supports either `u64` or `u128` prefixes (which can be truncated
///   to smaller types to fit the correct field data type).
/// - "Deleted" values are denoted by a tombstone bit in the timestamp.
/// - It also supports composite keys without a prefix (`Field == void`), which is useful for
///   indexing flags that are only checked with "exists".
pub fn CompositeKeyType(comptime Field: type) type {
    // The type if zeroed padding is needed.
    const Pad = switch (Field) {
        void => u0,
        u64 => u0,
        u128 => u64,
        else => @compileError("invalid Field for CompositeKey: " ++ @typeName(Field)),
    };

    return extern struct {
        const CompositeKey = @This();

        pub const sentinel_key: Key = key_from_value(&.{
            .field = if (Field == void) {} else math.maxInt(Field),
            .timestamp = math.maxInt(u64),
        });

        const tombstone_bit: u64 = 1 << (64 - 1);

        // u128 may be aligned to 8 instead of the expected 16.
        const field_bitsize_alignment = @max(
            @divExact(@bitSizeOf(Field), 8),
            @divExact(@bitSizeOf(u64), 8),
        );

        pub const Key = std.meta.Int(
            .unsigned,
            @bitSizeOf(u64) + @bitSizeOf(Field) + @bitSizeOf(Pad),
        );

        field: Field align(field_bitsize_alignment),
        /// The most significant bit must be unset as it is used to indicate a tombstone.
        timestamp: u64,
        padding: Pad = 0,

        comptime {
            assert(@sizeOf(CompositeKey) == @sizeOf(Key));
            assert(@sizeOf(CompositeKey) == switch (Field) {
                void => @sizeOf(u64),
                u64 => @sizeOf(u128),
                u128 => @sizeOf(u256),
                else => unreachable,
            });
            assert(@alignOf(CompositeKey) >= @alignOf(Field));
            assert(@alignOf(CompositeKey) == field_bitsize_alignment);
            assert(stdx.no_padding(CompositeKey));
        }

        pub inline fn key_from_value(value: *const CompositeKey) Key {
            assert(value.padding == 0);
            if (Field == void) {
                comptime assert(Key == u64);
                return value.timestamp & ~tombstone_bit;
            } else {
                comptime assert(@sizeOf(Key) == @sizeOf(Field) * 2);
                return @as(Key, value.timestamp & ~tombstone_bit) | (@as(Key, value.field) << 64);
            }
        }

        pub inline fn key_prefix(key: Key) Field {
            return if (Field == void) {} else @truncate(key >> 64);
        }

        pub inline fn tombstone(value: *const CompositeKey) bool {
            assert(value.padding == 0);
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(key: Key) CompositeKey {
            const timestamp: u64 = @truncate(key);
            assert(timestamp & tombstone_bit == 0);

            return .{
                .field = key_prefix(key),
                .timestamp = timestamp | tombstone_bit,
            };
        }
    };
}

pub fn is_composite_key(comptime Value: type) bool {
    if (@typeInfo(Value) == .@"struct" and
        @hasField(Value, "field") and
        @hasField(Value, "timestamp"))
    {
        const Field = @FieldType(Value, "field");
        return switch (Field) {
            void, u64, u128 => Value == CompositeKeyType(Field),
            else => false,
        };
    }

    return false;
}

test "composite_key - u64 and u128" {
    inline for (.{ u128, u64 }) |Prefix| {
        const CompositeKey = CompositeKeyType(Prefix);

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
            const value = CompositeKey{ .field = 1, .timestamp = 100 };
            try std.testing.expect(!CompositeKey.tombstone(&value));
        }

        {
            const key = CompositeKey.key_from_value(&.{ .field = 1, .timestamp = 100 });
            const value = CompositeKey.tombstone_from_key(key);
            try std.testing.expect(CompositeKey.tombstone(&value));
            try std.testing.expect(value.timestamp == @as(u64, 100) | CompositeKey.tombstone_bit);
        }
    }
}

test "composite_key - void" {
    const CompositeKey = CompositeKeyType(void);

    {
        const a = CompositeKey.key_from_value(&.{ .field = {}, .timestamp = 100 });
        const b = CompositeKey.key_from_value(&.{ .field = {}, .timestamp = 101 });
        try std.testing.expect(a < b);
    }

    {
        const a = CompositeKey.key_from_value(&.{
            .field = {},
            .timestamp = @as(u64, 100) | CompositeKey.tombstone_bit,
        });
        const b = CompositeKey.key_from_value(&.{
            .field = {},
            .timestamp = 100,
        });
        try std.testing.expect(a == b);
    }

    {
        const value = CompositeKey{ .field = {}, .timestamp = 100 };
        try std.testing.expect(!CompositeKey.tombstone(&value));
    }

    {
        const key = CompositeKey.key_from_value(&.{ .field = {}, .timestamp = 100 });
        const value = CompositeKey.tombstone_from_key(key);
        try std.testing.expect(CompositeKey.tombstone(&value));
        try std.testing.expect(value.timestamp == @as(u64, 100) | CompositeKey.tombstone_bit);
    }
}
