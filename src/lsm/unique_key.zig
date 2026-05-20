const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("stdx");

/// Represents a 1:1 map from a unique key to a timestamp (the primary key).
/// Similarly to `composite_key`, it stores the field, the timestamp, and
/// padding if required, but differs in that it uses only the field, not the
/// timestamp for comparison.
/// - To keep alignment, it supports either `u64` or `u128` keys.
/// - "Deleted" values are denoted by a tombstone bit in the timestamp.
pub fn UniqueKeyType(comptime _Key: type) type {
    return extern struct {
        field: Key,
        timestamp: u64,
        padding: Pad = 0,

        pub const Key = _Key;

        const UniqueKey = @This();

        // The type if zeroed padding is needed.
        const Pad = switch (Key) {
            u64 => u0,
            u128 => u64,
            else => @compileError("invalid Field for UniqueKey: " ++ @typeName(Key)),
        };

        pub const sentinel_key = std.math.maxInt(Key);
        pub const tombstone_bit = 1 << (64 - 1);

        pub inline fn key_from_value(value: *const UniqueKey) Key {
            return value.field;
        }

        pub inline fn tombstone(value: *const UniqueKey) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(field: Key) UniqueKey {
            return .{
                .field = field,
                .timestamp = tombstone_bit,
            };
        }

        comptime {
            assert(@sizeOf(UniqueKey) == 2 * @sizeOf(Key));
            assert(@sizeOf(UniqueKey) == switch (Key) {
                u64 => @sizeOf(u128),
                u128 => @sizeOf(u256),
                else => unreachable,
            });
            assert(@alignOf(UniqueKey) == @alignOf(Key));
            assert(stdx.no_padding(UniqueKey));
        }
    };
}

pub fn is_unique_key(comptime Value: type) bool {
    if (@typeInfo(Value) == .@"struct" and
        @hasField(Value, "field") and
        @hasField(Value, "timestamp"))
    {
        const Field = @FieldType(Value, "field");
        return switch (Field) {
            u64, u128 => Value == UniqueKeyType(Field),
            else => false,
        };
    }

    return false;
}

comptime {
    assert(is_unique_key(UniqueKeyType(u64)));
    assert(is_unique_key(UniqueKeyType(u128)));

    const CompositeKeyType = @import("composite_key.zig").CompositeKeyType;
    assert(!is_unique_key(CompositeKeyType(void)));
    assert(!is_unique_key(CompositeKeyType(u64)));
    assert(!is_unique_key(CompositeKeyType(u128)));

    assert(!is_unique_key(u64));
    assert(!is_unique_key(u128));
    assert(!is_unique_key(struct { field: u64, timestamp: u64 }));
    assert(!is_unique_key(struct { field: u128, timestamp: u64 }));
}

test "unique_key - u64 and u128" {
    inline for (.{ u128, u64 }) |Prefix| {
        const UniqueKey = UniqueKeyType(Prefix);

        {
            const a = UniqueKey.key_from_value(&.{ .field = 1, .timestamp = 100 });
            const b = UniqueKey.key_from_value(&.{ .field = 1, .timestamp = 101 });
            try std.testing.expect(a == b);
        }

        {
            const a = UniqueKey.key_from_value(&.{ .field = 1, .timestamp = 100 });
            const b = UniqueKey.key_from_value(&.{ .field = 2, .timestamp = 100 });
            try std.testing.expect(a < b);
        }
    }
}
