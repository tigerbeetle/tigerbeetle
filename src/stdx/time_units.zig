const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx.zig");

/// A moment in monotonic time not anchored to any particular epoch.
///
/// The absolute value of `ns` is meaningless, but it is possible to compute `Duration` between
/// two `Instant`s sourced from the same clock.
///
/// See also `DateTimeUTC`.
pub const Instant = struct {
    ns: u64,

    pub fn add(now: Instant, duration: Duration) Instant {
        return .{ .ns = now.ns + duration.ns };
    }

    pub fn duration_since(now: Instant, earlier: Instant) Duration {
        assert(now.ns >= earlier.ns);
        const elapsed_ns = now.ns - earlier.ns;
        return .{ .ns = elapsed_ns };
    }
};

/// Non-negative time difference between two `Instant`s.
pub const Duration = struct {
    ns: u64,

    pub fn ms(amount_ms: u64) Duration {
        return .{ .ns = amount_ms * std.time.ns_per_ms };
    }

    pub fn seconds(amount_seconds: u64) Duration {
        return .{ .ns = amount_seconds * std.time.ns_per_s };
    }

    pub fn minutes(amount_minutes: u64) Duration {
        return .{ .ns = amount_minutes * std.time.ns_per_min };
    }

    // Duration in microseconds, μs, 1/1_000_000 of a second.
    pub fn to_us(duration: Duration) u64 {
        return @divFloor(duration.ns, std.time.ns_per_us);
    }

    // Duration in milliseconds, ms, 1/1_000 of a second.
    pub fn to_ms(duration: Duration) u64 {
        return @divFloor(duration.ns, std.time.ns_per_ms);
    }

    pub fn min(lhs: Duration, rhs: Duration) Duration {
        return .{ .ns = @min(lhs.ns, rhs.ns) };
    }

    pub fn max(lhs: Duration, rhs: Duration) Duration {
        return .{ .ns = @max(lhs.ns, rhs.ns) };
    }

    pub const sort = struct {
        pub fn asc(ctx: void, lhs: Duration, rhs: Duration) bool {
            return std.sort.asc(u64)(ctx, lhs.ns, rhs.ns);
        }
    };

    // Human readable format like `1.123s`.
    // NB: this is a lossy operation, durations are rounded to look nice.
    pub fn format(
        duration: Duration,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try std.fmt.fmtDuration(duration.ns).format(fmt, options, writer);
    }

    pub fn parse_flag_value(
        string: []const u8,
        static_diagnostic: *?[]const u8,
    ) error{InvalidFlagValue}!Duration {
        assert(string.len > 0);
        var string_remaining = string;

        var result: Duration = .{ .ns = 0 };
        while (string_remaining.len > 0) {
            string_remaining, const component =
                try parse_flag_value_component(string_remaining, static_diagnostic);
            result.ns +|= component.ns;
        }

        if (result.ns >= 1_000 * std.time.ns_per_day) {
            static_diagnostic.* = "duration too large:";
            return error.InvalidFlagValue;
        }
        return result;
    }

    fn parse_flag_value_component(
        string: []const u8,
        static_diagnostic: *?[]const u8,
    ) error{InvalidFlagValue}!struct { []const u8, Duration } {
        const split_index = for (string, 0..) |c, index| {
            if (std.ascii.isDigit(c)) {
                // Numeric part continues.
            } else break index;
        } else {
            static_diagnostic.* = "missing unit; must be one of: d/h/m/s/ms/us/ns:";
            return error.InvalidFlagValue;
        };

        if (split_index == 0) {
            static_diagnostic.* = "missing value:";
            return error.InvalidFlagValue;
        }

        const string_amount = string[0..split_index];
        const string_remaining = string[split_index..];
        assert(string_amount.len > 0);
        assert(string_remaining.len > 0);

        const amount = std.fmt.parseInt(u64, string_amount, 10) catch |err| switch (err) {
            error.Overflow => {
                static_diagnostic.* = "integer overflow:";
                return error.InvalidFlagValue;
            },
            error.InvalidCharacter => unreachable,
        };

        const Unit = enum(u64) {
            ns = 1,
            us = std.time.ns_per_us,
            ms = std.time.ns_per_ms,
            s = std.time.ns_per_s,
            m = std.time.ns_per_min,
            h = std.time.ns_per_hour,
            d = std.time.ns_per_day,
        };

        inline for (comptime std.enums.values(Unit)) |unit| {
            if (stdx.cut_prefix(string_remaining, @tagName(unit))) |suffix| {
                return .{ suffix, .{ .ns = amount *| @intFromEnum(unit) } };
            }
        } else {
            static_diagnostic.* = "unknown unit; must be one of: d/h/m/s/ms/us/ns:";
            return error.InvalidFlagValue;
        }
    }
};

test "Instant/Duration" {
    const instant_1: Instant = .{ .ns = 100 * std.time.ns_per_day };
    const instant_2: Instant = .{ .ns = 100 * std.time.ns_per_day + std.time.ns_per_s };
    assert(instant_1.duration_since(instant_1).ns == 0);
    assert(instant_2.duration_since(instant_1).ns == std.time.ns_per_s);

    const duration = instant_2.duration_since(instant_1);
    assert(duration.ns == 1_000_000_000);
    assert(duration.to_us() == 1_000_000);
    assert(duration.to_ms() == 1_000);

    assert(Duration.ms(1).ns == std.time.ns_per_ms);
    assert(Duration.seconds(1).ns == std.time.ns_per_s);
    assert(Duration.minutes(1).ns == std.time.ns_per_min);
}

test "Duration.parse_flag_value" {
    try stdx.parse_flag_value_fuzz(Duration, Duration.parse_flag_value, .{
        .ok = &.{
            .{ "1h", .{ .ns = std.time.ns_per_hour } },
            .{ "1m", .{ .ns = std.time.ns_per_min } },
            .{ "1h2m", .{ .ns = std.time.ns_per_hour + 2 * std.time.ns_per_min } },
            .{ "1ms2us3ns", .{ .ns = std.time.ns_per_ms + 2 * std.time.ns_per_us + 3 } },
        },
        .err = &.{
            .{ "h", "missing value" },
            .{ "1", "missing unit" },
            .{ "h1", "missing value" },
            .{ "1H", "unknown unit; must be one of: d/h/m/s/ms/us/ns" },
            .{ "1h2x", "unknown unit" },
            .{ "1_0h", "unknown unit" },
            .{ "1h 2m", "missing value" },
            .{ "18446744073709551616ns", "integer overflow" },
            .{ "1844674407370955161s", "duration too large" },
        },
    });
}

/// DateTime in UTC, intended primarily for logging.
///
/// NB: this is a pure function of a timestamp. To convert timestamp to UTC, no knowledge of
/// timezones or leap seconds is necessary.
pub const DateTimeUTC = struct {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    millisecond: u16,

    pub fn now() DateTimeUTC {
        const timestamp_ms = std.time.milliTimestamp();
        assert(timestamp_ms > 0);
        return DateTimeUTC.from_timestamp_ms(@intCast(timestamp_ms));
    }

    pub fn from_timestamp_s(timestamp_s: u64) DateTimeUTC {
        return DateTimeUTC.from_timestamp_ms(timestamp_s * std.time.ms_per_s);
    }

    pub fn from_timestamp_ms(timestamp_ms: u64) DateTimeUTC {
        const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @divTrunc(timestamp_ms, 1000) };
        const year_day = epoch_seconds.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        const time = epoch_seconds.getDaySeconds();

        return DateTimeUTC{
            .year = year_day.year,
            .month = month_day.month.numeric(),
            .day = month_day.day_index + 1,
            .hour = time.getHoursIntoDay(),
            .minute = time.getMinutesIntoHour(),
            .second = time.getSecondsIntoMinute(),
            .millisecond = @intCast(@mod(timestamp_ms, 1000)),
        };
    }

    pub fn format(
        datetime: DateTimeUTC,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
            datetime.year,
            datetime.month,
            datetime.day,
            datetime.hour,
            datetime.minute,
            datetime.second,
            datetime.millisecond,
        });
    }
};
