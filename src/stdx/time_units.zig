const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx.zig");

/// A moment in monotonic time not anchored to any particular epoch.
///
/// The absolute value of `ns` is meaningless, but it is possible to compute `Duration` between
/// two `Instant`s sourced from the same clock.
///
/// See also `InstantUnix`.
pub const Instant = struct {
    ns: u64,

    pub fn add(instant: Instant, duration: Duration) Instant {
        return .{ .ns = instant.ns + duration.ns };
    }

    pub fn until(earlier: Instant, later: Instant) Duration {
        assert(earlier.ns <= later.ns);

        const elapsed_ns = later.ns - earlier.ns;
        return .{ .ns = elapsed_ns };
    }
};

/// Non-negative time difference between two `Instant`s.
pub const Duration = struct {
    ns: u64,

    pub fn us(amount_us: u64) Duration {
        return .{ .ns = amount_us * std.time.ns_per_us };
    }

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

    pub fn clamp(duration: Duration, clamp_min: Duration, clamp_max: Duration) Duration {
        assert(clamp_min.ns <= clamp_max.ns);
        if (duration.ns < clamp_min.ns) return clamp_min;
        if (duration.ns > clamp_max.ns) return clamp_max;
        return duration;
    }

    pub const sort = struct {
        pub fn asc(ctx: void, lhs: Duration, rhs: Duration) bool {
            return std.sort.asc(u64)(ctx, lhs.ns, rhs.ns);
        }
    };

    // Human readable format like `1.123s`.
    // NB: this is a lossy operation, durations are rounded to look nice.
    pub fn format(duration: Duration, writer: *std.Io.Writer) !void {
        try std.Io.Duration.fromNanoseconds(duration.ns).format(writer);
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

        const amount = stdx.parse_int(u64, string_amount, .{
            .base = 10,
            .allow_separators = true,
        }) catch |err| switch (err) {
            error.Overflow => {
                static_diagnostic.* = "integer overflow:";
                return error.InvalidFlagValue;
            },
            error.LeadingZero => {
                static_diagnostic.* = "leading zero disallowed:";
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
    assert(instant_1.until(instant_1).ns == 0);
    assert(instant_1.until(instant_2).ns == std.time.ns_per_s);

    const duration = instant_1.until(instant_2);
    assert(duration.ns == 1_000_000_000);
    assert(duration.to_us() == 1_000_000);
    assert(duration.to_ms() == 1_000);

    assert(Duration.ms(1).ns == std.time.ns_per_ms);
    assert(Duration.seconds(1).ns == std.time.ns_per_s);
    assert(Duration.minutes(1).ns == std.time.ns_per_min);
}

test "Duration.parse_flag_value" {
    try stdx.Flags.parse_flag_value_fuzz(Duration, Duration.parse_flag_value, .{
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
            .{ "0024h", "leading zero disallowed" },
        },
    });
}

const DateTimeUTC = struct {
    year: u16,
    month: enum(u4) { Jan = 0, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec },
    day: u8,
    week_day: enum(u3) { Mon = 0, Tue, Wed, Thu, Fri, Sat, Sun },
    hour: u8,
    minute: u8,
    second: u8,
    millisecond: u16,

    pub fn format(datetime: DateTimeUTC, writer: *std.Io.Writer) !void {
        const buffer: [24]u8 =
            format_fixed_width(4, datetime.year) ++ "-".* ++
            format_fixed_width(2, @intFromEnum(datetime.month) + 1) ++ "-".* ++
            format_fixed_width(2, datetime.day) ++ " ".* ++
            format_fixed_width(2, datetime.hour) ++ ":".* ++
            format_fixed_width(2, datetime.minute) ++ ":".* ++
            format_fixed_width(2, datetime.second) ++ ".".* ++
            format_fixed_width(3, datetime.millisecond) ++ "Z".*;
        try writer.writeAll(&buffer);
    }

    /// RFC 1123 date and time.
    /// Example: `Sun, 28 Aug 2022 08:49:37 GMT`.
    /// https://www.rfc-editor.org/info/rfc1123/#page-55 (Section 5.2.14)
    pub fn format_rfc1123(datetime: DateTimeUTC, buffer: *[29]u8) void {
        buffer.* =
            @tagName(datetime.week_day)[0..3].* ++ ", ".* ++
            format_fixed_width(2, datetime.day) ++ " ".* ++
            @tagName(datetime.month)[0..3].* ++ " ".* ++
            format_fixed_width(4, datetime.year) ++ " ".* ++
            format_fixed_width(2, datetime.hour) ++ ":".* ++
            format_fixed_width(2, datetime.minute) ++ ":".* ++
            format_fixed_width(2, datetime.second) ++ " GMT".*;
    }

    /// ISO 8601 basic format date and time. Example: `20220828T084937Z`.
    pub fn format_iso8601(datetime: DateTimeUTC, buffer: *[16]u8) void {
        var date: [8]u8 = undefined;
        datetime.format_iso8601_date(&date);
        buffer.* = date ++ "T".* ++
            format_fixed_width(2, datetime.hour) ++
            format_fixed_width(2, datetime.minute) ++
            format_fixed_width(2, datetime.second) ++ "Z".*;
    }

    /// ISO 8601 basic format calendar date. Example: `20220828`.
    pub fn format_iso8601_date(datetime: DateTimeUTC, buffer: *[8]u8) void {
        buffer.* =
            format_fixed_width(4, datetime.year) ++
            format_fixed_width(2, @intFromEnum(datetime.month) + 1) ++
            format_fixed_width(2, datetime.day);
    }

    /// Formats `value` as exactly `width` decimal digits, left-padded with zeros.
    fn format_fixed_width(comptime width: comptime_int, value: u64) [width]u8 {
        assert(value < std.math.pow(u64, 10, width));
        var result: [width]u8 = undefined;
        var rest = value;
        inline for (1..width + 1) |offset| {
            const index = width - offset;
            const digit: u8 = @intCast(rest % 10);
            rest = @divFloor(rest, 10);
            result[index] = '0' + digit;
        }
        assert(rest == 0);
        return result;
    }
};

/// A moment in non-monotonic Unix time.
/// Timestamp is relative to epoch 1970-01-01.
///
/// See also `Instant`.
pub const InstantUnix = struct {
    ns: u64,

    pub fn from_seconds(timestamp_s: u64) InstantUnix {
        return InstantUnix{ .ns = timestamp_s * std.time.ns_per_s };
    }

    pub fn to_seconds(instant: InstantUnix) u64 {
        return @divFloor(instant.ns, std.time.ns_per_s);
    }

    pub fn date_time(instant: InstantUnix) DateTimeUTC {
        const timestamp_ms = @divTrunc(instant.ns, std.time.ns_per_ms);
        const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @divTrunc(timestamp_ms, 1000) };
        const epoch_day = @divTrunc(instant.ns, std.time.ns_per_s * std.time.s_per_day);
        const year_day = epoch_seconds.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        const month = month_day.month.numeric();
        assert(month >= 1);
        assert(month <= 12);
        const time = epoch_seconds.getDaySeconds();
        // 1970-01-01 was a Thursday (= index 3 when Monday = index 0).
        const weekday_index: u3 = @intCast((epoch_day + 3) % 7);

        return .{
            .year = year_day.year,
            .month = @enumFromInt(month - 1), // Zero-indexed month.
            .day = month_day.day_index + 1,
            .week_day = @enumFromInt(weekday_index), // Zero-indexed week.
            .hour = time.getHoursIntoDay(),
            .minute = time.getMinutesIntoHour(),
            .second = time.getSecondsIntoMinute(),
            .millisecond = @intCast(@mod(timestamp_ms, 1000)),
        };
    }

    pub fn add(instant: InstantUnix, duration: Duration) InstantUnix {
        return .{ .ns = instant.ns + duration.ns };
    }

    pub fn format(instant: InstantUnix, writer: *std.Io.Writer) !void {
        _ = instant;
        _ = writer;
        @compileError("convert to DateTime first");
    }
};

test "InstantUnix formats" {
    const expectFmt = std.testing.expectFmt;
    const expectEqualStrings = std.testing.expectEqualStrings;

    var rfc1123: [29]u8 = undefined;
    var iso8601: [16]u8 = undefined;
    var iso8601_date: [8]u8 = undefined;

    {
        // TigerBeetle birthday
        const date_time = InstantUnix.from_seconds(1661676577).date_time();

        try expectFmt("2022-08-28 08:49:37.000Z", "{f}", .{
            date_time,
        });

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("20220828T084937Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("20220828", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Sun, 28 Aug 2022 08:49:37 GMT", &rfc1123);
    }
    const instant_min = InstantUnix{ .ns = 0 };
    {
        // Epoch timestamp.
        const date_time = instant_min.date_time();
        try expectFmt("1970-01-01 00:00:00.000Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19700101T000000Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19700101", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Thu, 01 Jan 1970 00:00:00 GMT", &rfc1123);
    }

    const one_year = Duration{ .ns = std.time.ns_per_day * 365 };
    const instant_min_plus_year = instant_min.add(one_year);
    {
        // One year after minimum timestamp.
        const date_time = instant_min_plus_year.date_time();
        try expectFmt("1971-01-01 00:00:00.000Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19710101T000000Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19710101", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Fri, 01 Jan 1971 00:00:00 GMT", &rfc1123);
    }

    const instant_last_ns = InstantUnix{ .ns = instant_min_plus_year.ns - 1 };
    {
        // Check that ns -= 1 flips every counter correctly.
        const date_time = instant_last_ns.date_time();
        try expectFmt("1970-12-31 23:59:59.999Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19701231T235959Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19701231", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Thu, 31 Dec 1970 23:59:59 GMT", &rfc1123);
    }

    {
        // Maximum timestamp.
        const date_time = (InstantUnix{ .ns = std.math.maxInt(u64) }).date_time();
        try expectFmt("2554-07-21 23:34:33.709Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("25540721T233433Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("25540721", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Sun, 21 Jul 2554 23:34:33 GMT", &rfc1123);
    }
    // Test vectors from RFC3339
    // https://www.rfc-editor.org/info/rfc3339/#section-5.8
    {
        // 1985-04-12T23:20:50.52Z
        const date_time = (InstantUnix{ .ns = 482196050520 * std.time.ns_per_ms }).date_time();
        try expectFmt("1985-04-12 23:20:50.520Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19850412T232050Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19850412", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Fri, 12 Apr 1985 23:20:50 GMT", &rfc1123);
    }
    {
        // 1996-12-19T16:39:57-08:00, which is 1996-12-20T00:39:57Z.
        const date_time = InstantUnix.from_seconds(851042397).date_time();
        try expectFmt("1996-12-20 00:39:57.000Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19961220T003957Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19961220", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Fri, 20 Dec 1996 00:39:57 GMT", &rfc1123);
    }

    const instant_before_leap_second = InstantUnix.from_seconds(662687999);
    {
        // 1990-12-31T23:59:59Z
        const date_time = instant_before_leap_second.date_time();
        try expectFmt("1990-12-31 23:59:59.000Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19901231T235959Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19901231", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Mon, 31 Dec 1990 23:59:59 GMT", &rfc1123);
    }

    const instant_after_leap_second = instant_before_leap_second.add(Duration.seconds(1));
    {
        // 1990-12-31T23:59:60Z and 1990-12-31T15:59:60-08:00 (leap second).
        // Unix time does not count leap seconds, so 23:59:60 shares its timestamp with the
        // following 00:00:00.
        const date_time = instant_after_leap_second.date_time();
        try expectFmt("1991-01-01 00:00:00.000Z", "{f}", .{date_time});

        date_time.format_iso8601(&iso8601);
        try expectEqualStrings("19910101T000000Z", &iso8601);

        date_time.format_iso8601_date(&iso8601_date);
        try expectEqualStrings("19910101", &iso8601_date);

        date_time.format_rfc1123(&rfc1123);
        try expectEqualStrings("Tue, 01 Jan 1991 00:00:00 GMT", &rfc1123);
    }
}

test "InstantUnix formats fuzz" {
    var prng = stdx.PRNG.from_seed_testing();

    const Context = struct {
        fn check(ns: u64) anyerror!void {
            const instant: InstantUnix = .{ .ns = ns };

            const date_time = instant.date_time();
            try std.testing.expect(date_time.year >= 1970);
            try std.testing.expect(date_time.year <= 2554);
            try std.testing.expect(date_time.day >= 1 and date_time.day <= 31);
            try std.testing.expect(date_time.hour < 24);
            try std.testing.expect(date_time.minute < 60);
            try std.testing.expect(date_time.second < 60);
            try std.testing.expect(date_time.millisecond < 1000);

            var utc_buffer: [24]u8 = undefined;
            const utc = try std.fmt.bufPrint(&utc_buffer, "{f}", .{date_time});
            try std.testing.expectEqual(utc_buffer.len, utc.len);

            var iso8601_basic_buffer: [16]u8 = undefined;
            date_time.format_iso8601(&iso8601_basic_buffer);

            var date_buffer: [8]u8 = undefined;
            date_time.format_iso8601_date(&date_buffer);
            try std.testing.expectStringStartsWith(&iso8601_basic_buffer, &date_buffer);

            var rfc1123_buffer: [29]u8 = undefined;
            date_time.format_rfc1123(&rfc1123_buffer);
        }
    };

    const ns_per_s = std.time.ns_per_s;
    const ns_per_day = ns_per_s * std.time.s_per_day;
    const ns_max = std.math.maxInt(u64);
    for (0..100_000) |_| {
        const offset = prng.int_inclusive(u64, std.time.ns_per_ms);
        const ns: u64 = b: switch (prng.chances(.{
            .random = 5,
            .day_boundary = 2,
            .second_boundary = 2,
            .u64_boundary = 1,
        })) {
            .random => break :b prng.int(u64),
            .day_boundary => {
                const day = prng.int_inclusive(u64, @divFloor(ns_max, ns_per_day));
                const ns = day * ns_per_day;

                break :b if (prng.boolean()) ns -| offset else ns +| offset;
            },
            .second_boundary => {
                const second = prng.int_inclusive(u64, @divFloor(ns_max, ns_per_s));
                const ns = second * ns_per_s;

                break :b if (prng.boolean()) ns -| offset else ns +| offset;
            },
            .u64_boundary => {
                break :b if (prng.boolean()) ns_max - offset else offset;
            },
        };
        try Context.check(ns);
    }
}
