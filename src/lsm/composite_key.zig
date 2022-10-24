const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

pub fn CompositeKeyType(comptime Field: type) type {
    assert(Field == u128 or Field == u64);

    // Because extern structs may not contain zero-sized fields, this is the cleanest way
    // to conditionally include a padding field based on the Field type.
    const CompositeKey = switch (Field) {
        u64 => extern struct {
            pub const Value = extern struct {
                field: u64,
                timestamp: u64,
            };

            field: u64,
            timestamp: u64,

            pub usingnamespace composite_key_decls(@This());
        },
        u128 => extern struct {
            pub const Value = extern struct {
                field: u128,
                timestamp: u64,
                padding: u64 = 0,
            };

            field: u128,
            /// The most significant bit must be unset as it is used to indicate a tombstone.
            timestamp: u64,
            padding: u64 = 0,

            pub usingnamespace composite_key_decls(@This());
        },
        else => unreachable,
    };

    assert(@sizeOf(CompositeKey) == @sizeOf(Field) * 2);
    assert(@sizeOf(CompositeKey) * 8 == @bitSizeOf(CompositeKey));

    assert(@sizeOf(CompositeKey.Value) == @sizeOf(Field) * 2);
    assert(@sizeOf(CompositeKey.Value) * 8 == @bitSizeOf(CompositeKey.Value));

    switch (Field) {
        u128 => {
            // The alignment of u128 struct fields is 16 as required by the C ABI
            // but only 8 for u128 values.
            assert(@alignOf(u128) == 8);
            assert(@alignOf(CompositeKey) == 16);
            assert(@alignOf(CompositeKey.Value) == 16);
        },
        else => {
            assert(@alignOf(CompositeKey) == @alignOf(Field));
            assert(@alignOf(CompositeKey.Value) == @alignOf(Field));
        },
    }

    return CompositeKey;
}

fn composite_key_decls(comptime CompositeKey: type) type {
    return struct {
        pub const sentinel_key: CompositeKey = .{
            .field = math.maxInt(std.meta.fieldInfo(CompositeKey, .field).field_type),
            .timestamp = math.maxInt(u64),
        };

        const tombstone_bit = 1 << 63;

        pub inline fn compare_keys(a: CompositeKey, b: CompositeKey) math.Order {
            if (a.field < b.field) {
                return .lt;
            } else if (a.field > b.field) {
                return .gt;
            } else if (a.timestamp < b.timestamp) {
                return .lt;
            } else if (a.timestamp > b.timestamp) {
                return .gt;
            } else {
                return .eq;
            }
        }

        pub inline fn key_from_value(value: *const CompositeKey.Value) CompositeKey {
            return .{
                .field = value.field,
                .timestamp = @truncate(u63, value.timestamp),
            };
        }

        pub inline fn tombstone(value: *const CompositeKey.Value) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub inline fn tombstone_from_key(key: CompositeKey) CompositeKey.Value {
            return .{
                .field = key.field,
                .timestamp = key.timestamp | tombstone_bit,
            };
        }
    };
}
