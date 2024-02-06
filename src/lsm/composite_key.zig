const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const stdx = @import("../stdx.zig");

pub fn CompositeKeyType(comptime Prefix_: type, comptime Value_: type) type {
    if (Prefix_ != u64 and Prefix_ != u128) {
        @compileError("invalid Prefix for CompositeKey: " ++ @typeName(Prefix_));
    }
    if (Value_ != u64 and Value_ != u128) {
        @compileError("invalid Value for CompositeKey: " ++ @typeName(Value_));
    }

    // The schema requires the size to be a power of two,
    // so we may need to add a pad to the next power of two greater than the current size.
    const pad = pad: {
        const value_size = @sizeOf(Prefix_) + @sizeOf(Value_);
        const expected_size = 1 << std.math.log2_int_ceil(usize, value_size);
        assert(expected_size >= value_size);

        const pad_size = expected_size - value_size;
        break :pad [_]u8{0} ** pad_size;
    };
    const Pad = @TypeOf(pad);

    return extern struct {
        const CompositeKey = @This();

        pub const Prefix = Prefix_;
        pub const Value = Value_;

        pub const sentinel_key: Key = key_from_value(&.{
            .prefix = math.maxInt(Prefix),
            .value = math.maxInt(u64),
        });

        const value_shift = @bitSizeOf(Value);
        const tombstone_bit: Value = 1 << (value_shift - 1);

        pub const Key = std.meta.Int(
            .unsigned,
            @bitSizeOf(Prefix) + @bitSizeOf(Value) + @bitSizeOf(Pad),
        );

        // u128 may be aligned to 8 instead of the expected 16.
        prefix: Prefix align(8),
        /// The most significant bit must be unset as it is used to indicate a tombstone.
        value: Value align(8),
        padding: Pad = pad,

        comptime {
            assert(@sizeOf(CompositeKey) == 16 or @sizeOf(CompositeKey) == 32);
            assert(@alignOf(CompositeKey) >= @alignOf(Prefix));
            assert(@sizeOf(CompositeKey) * 8 == @bitSizeOf(CompositeKey));
            assert(@sizeOf(CompositeKey) == @sizeOf(Key));
            assert(stdx.no_padding(Key));
        }

        pub inline fn key_from_value(self: *const CompositeKey) Key {
            return @as(Key, self.value & ~tombstone_bit) | (@as(Key, self.prefix) << value_shift);
        }

        pub inline fn key_prefix(key: Key) Prefix {
            return @truncate(key >> value_shift);
        }

        pub inline fn key_value(key: Key) Value {
            return @truncate(key);
        }

        pub inline fn tombstone(self: *const CompositeKey) bool {
            return (self.value & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(key: Key) CompositeKey {
            const prefix: Prefix = key_prefix(key);
            const value: Value = key_value(key);
            assert(value & tombstone_bit == 0);

            return .{
                .prefix = prefix,
                .value = value | tombstone_bit,
            };
        }
    };
}

pub fn is_composite_key(comptime CompositeKey: type) bool {
    if (@typeInfo(CompositeKey) == .Struct and
        @hasField(CompositeKey, "prefix") and
        @hasField(CompositeKey, "value"))
    {
        const Prefix = std.meta.fieldInfo(CompositeKey, .prefix).type;
        const Value = std.meta.fieldInfo(CompositeKey, .value).type;
        if ((Prefix == u64 or Prefix == u128) and (Value == u64 or Value == u128)) {
            return CompositeKey == CompositeKeyType(Prefix, Value);
        }
    }

    return false;
}

fn composite_key_test(comptime CompositeKey: type) !void {
    {
        const a = CompositeKey.key_from_value(&.{ .prefix = 1, .value = 100 });
        const b = CompositeKey.key_from_value(&.{ .prefix = 1, .value = 101 });
        try std.testing.expect(a < b);
    }

    {
        const a = CompositeKey.key_from_value(&.{ .prefix = 1, .value = 100 });
        const b = CompositeKey.key_from_value(&.{ .prefix = 2, .value = 99 });
        try std.testing.expect(a < b);
    }

    {
        const a = CompositeKey.key_from_value(&.{
            .prefix = 1,
            .value = @as(CompositeKey.Value, 100) | CompositeKey.tombstone_bit,
        });
        const b = CompositeKey.key_from_value(&.{
            .prefix = 1,
            .value = 100,
        });
        try std.testing.expect(a == b);
    }

    {
        const key = CompositeKey.key_from_value(&.{ .prefix = 1, .value = 100 });
        const value = CompositeKey.tombstone_from_key(key);
        try std.testing.expect(CompositeKey.tombstone(&value));
        try std.testing.expect(
            value.value == @as(CompositeKey.Value, 100) | CompositeKey.tombstone_bit,
        );
    }
}

test "composite_key" {
    try composite_key_test(CompositeKeyType(u64, u64));
    try composite_key_test(CompositeKeyType(u128, u64));
    try composite_key_test(CompositeKeyType(u64, u128));
    try composite_key_test(CompositeKeyType(u128, u128));
}

test "is_composite_key" {
    try std.testing.expect(is_composite_key(CompositeKeyType(u64, u64)));
    try std.testing.expect(is_composite_key(CompositeKeyType(u128, u64)));
    try std.testing.expect(is_composite_key(CompositeKeyType(u64, u128)));
    try std.testing.expect(is_composite_key(CompositeKeyType(u128, u128)));

    try std.testing.expect(!is_composite_key(u64));
    try std.testing.expect(!is_composite_key(u128));

    try std.testing.expect(!is_composite_key(struct {
        prefix: u64,
        value: u64,
    }));
}
