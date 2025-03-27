const std = @import("std");

pub const TimestampRange = struct {
    /// The minimum timestamp allowed (inclusive).
    pub const timestamp_min = 1;

    /// The maximum timestamp allowed (inclusive).
    /// It is `maxInt(u63)` because the most significant bit of the `u64` timestamp
    /// is used as the tombstone flag.
    pub const timestamp_max = std.math.maxInt(u63);

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

    pub inline fn valid(timestamp: u64) bool {
        return timestamp >= timestamp_min and
            timestamp <= timestamp_max;
    }
};
