const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const Elem = std.meta.Elem;

const binary_search = @import("lsm/binary_search.zig");

/// Tree LSM is a sorted array with a monocle and a top hat.
///
/// We want to iterate it in both directions:
/// - For CDC, you want to learn about all new objects with timestamp>thershold.
/// - For paginated timelines, you want to learn about past objects with timestamp<thersold.
///
/// Sadly, this can't be implemented via just a single branch near the end of the system, we need to
/// change a whole bunch of `<` to `>` throughout the stack.
///
/// Direction encapsulate the logic of "if ascending use < if descending use >". The mnemonic is
/// that usual comparison is horizontal along a number line, but Direciton-aware is vertical.
///
/// In other words, `key_min` and `key_max` track natural ordering, while `key_lower` and
/// `key_upper` are direction-aware.
pub const Direction = enum(u1) {
    ascending = 0,
    descending = 1,

    pub fn reverse(d: Direction) Direction {
        return switch (d) {
            .ascending => .descending,
            .descending => .ascending,
        };
    }

    pub inline fn cmp(
        d: Direction,
        a: anytype,
        comptime op: enum { @"<", @"<=" },
        b: @TypeOf(a),
    ) bool {
        return switch (op) {
            .@"<" => switch (d) {
                .ascending => a < b,
                .descending => a > b,
            },
            .@"<=" => switch (d) {
                .ascending => a <= b,
                .descending => a >= b,
            },
        };
    }

    pub inline fn lower(d: Direction, a: anytype, b: @TypeOf(a)) @TypeOf(a) {
        return if (d.cmp(a, .@"<", b)) a else b;
    }

    pub inline fn upper(d: Direction, a: anytype, b: @TypeOf(a)) @TypeOf(a) {
        return if (d.cmp(a, .@"<", b)) b else a;
    }

    pub inline fn slice_peek(d: Direction, slice: anytype) *const Elem(@TypeOf(slice)) {
        assert(slice.len > 0);
        return switch (d) {
            .ascending => &slice[0],
            .descending => &slice[slice.len - 1],
        };
    }

    pub inline fn slice_pop(
        d: Direction,
        slice: anytype,
    ) struct { Elem(@TypeOf(slice)), @TypeOf(slice) } {
        assert(slice.len > 0);
        return switch (d) {
            .ascending => .{ slice[0], slice[1..] },
            .descending => .{ slice[slice.len - 1], slice[0 .. slice.len - 1] },
        };
    }

    pub inline fn slice_lower_bound(
        direction: Direction,
        comptime Key: type,
        comptime Value: type,
        comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
        slice: []const Value,
        key: Key,
    ) []const Value {
        maybe(slice.len == 0);
        switch (direction) {
            .ascending => {
                const start = binary_search.binary_search_values_upsert_index(
                    Key,
                    Value,
                    key_from_value,
                    slice,
                    key,
                    .{ .mode = .lower_bound },
                );

                return if (start == slice.len) &.{} else slice[start..];
            },
            .descending => {
                const end = end: {
                    const index = binary_search.binary_search_values_upsert_index(
                        Key,
                        Value,
                        key_from_value,
                        slice,
                        key,
                        .{ .mode = .upper_bound },
                    );
                    break :end index + @intFromBool(
                        index < slice.len and key_from_value(&slice[index]) <= key,
                    );
                };

                return if (end == 0) &.{} else slice[0..end];
            },
        }
    }
};
