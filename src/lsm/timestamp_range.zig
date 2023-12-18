const std = @import("std");

pub const TimestampRange = struct {
    pub const timestamp_min = 1;
    pub const timestamp_max = std.math.maxInt(u64) - 1;

    min: u64, // Inclusive.
    max: u64, // Inclusive.

    pub inline fn all() TimestampRange {
        return .{
            .min = timestamp_min,
            .max = timestamp_max,
        };
    }

    pub inline fn gte(initial: u64) TimestampRange {
        return .{
            .min = initial,
            .max = timestamp_max,
        };
    }

    pub inline fn lte(final: u64) TimestampRange {
        return .{
            .min = timestamp_min,
            .max = final,
        };
    }
};
