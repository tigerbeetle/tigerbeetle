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

    pub fn add(now: Instant, duration: Duration) Instant {
        return .{ .ns = now.ns + duration.ns };
    }

    pub fn elapsed(earlier: Instant, now: Instant) Duration {
        assert(now.ns >= earlier.ns);
        const elapsed_ns = now.ns - earlier.ns;
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
    assert(instant_1.elapsed(instant_1).ns == 0);
    assert(instant_1.elapsed(instant_2).ns == std.time.ns_per_s);

    const duration = instant_1.elapsed(instant_2);
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

/// A moment in non-monotonic Unix time.
/// Timestamp is relative to epoch 1970-01-01.
///
/// See also `Instant`.
pub const InstantUnix = struct {
    ns: u64,

    /// RFC 3339 human-readable date and time with milliseconds.
    /// Example: `2022-08-28 08:49:37.000Z`.
    /// https://www.rfc-editor.org/rfc/rfc3339#section-5.6
    const DateTimeUTC = struct {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u16,

        pub fn format(datetime: DateTimeUTC, writer: *std.Io.Writer) !void {
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

    /// ISO 8601 basic format date and time. Example: `20220828T084937Z`.
    const DateTimeISO8601Basic = struct {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,

        pub fn format(datetime: DateTimeISO8601Basic, writer: *std.Io.Writer) !void {
            try writer.print("{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
                datetime.year,
                datetime.month,
                datetime.day,
                datetime.hour,
                datetime.minute,
                datetime.second,
            });
        }
    };

    /// ISO 8601 basic format calendar date. Example: `20220828`.
    const DateISO8601Basic = struct {
        year: u16,
        month: u8,
        day: u8,

        pub fn format(date: DateISO8601Basic, writer: *std.Io.Writer) !void {
            try writer.print("{d:0>4}{d:0>2}{d:0>2}", .{
                date.year,
                date.month,
                date.day,
            });
        }
    };

    /// RFC 1123 date and time.
    /// Example: `Sun, 28 Aug 2022 08:49:37 GMT`.
    /// https://www.rfc-editor.org/info/rfc1123/#page-55 (Section 5.2.14)
    const DateTimeRFC1123 = struct {
        weekday_index: u8, // Mon = 0
        year: u16,
        month_index: u8, // Jan = 0
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,

        const days = [_][]const u8{ "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun" };
        const months = [_][]const u8{
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        };

        pub fn format(datetime: DateTimeRFC1123, writer: *std.Io.Writer) !void {
            assert(datetime.weekday_index < days.len);
            assert(datetime.month_index < months.len);

            try writer.print("{s}, {d:0>2} {s} {d:0>4} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
                days[datetime.weekday_index],
                datetime.day,
                months[datetime.month_index],
                datetime.year,
                datetime.hour,
                datetime.minute,
                datetime.second,
            });
        }
    };

    pub fn from_seconds(timestamp_s: u64) InstantUnix {
        return InstantUnix{ .ns = timestamp_s * std.time.ns_per_s };
    }

    pub fn to_seconds(instant: InstantUnix) u64 {
        return @divFloor(instant.ns, std.time.ns_per_s);
    }

    pub fn date_time(instant: InstantUnix) DateTimeUTC {
        const timestamp_ms = @divTrunc(instant.ns, std.time.ns_per_ms);
        const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @divTrunc(timestamp_ms, 1000) };
        const year_day = epoch_seconds.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        const time = epoch_seconds.getDaySeconds();

        return .{
            .year = year_day.year,
            .month = month_day.month.numeric(),
            .day = month_day.day_index + 1,
            .hour = time.getHoursIntoDay(),
            .minute = time.getMinutesIntoHour(),
            .second = time.getSecondsIntoMinute(),
            .millisecond = @intCast(@mod(timestamp_ms, 1000)),
        };
    }

    pub fn date_time_iso8601_basic(instant: InstantUnix) DateTimeISO8601Basic {
        const date_time_utc = instant.date_time();
        return .{
            .year = date_time_utc.year,
            .month = date_time_utc.month,
            .day = date_time_utc.day,
            .hour = date_time_utc.hour,
            .minute = date_time_utc.minute,
            .second = date_time_utc.second,
        };
    }

    pub fn date_iso8601_basic(instant: InstantUnix) DateISO8601Basic {
        const date_time_utc = instant.date_time();
        return .{
            .year = date_time_utc.year,
            .month = date_time_utc.month,
            .day = date_time_utc.day,
        };
    }

    pub fn date_time_rfc1123(instant: InstantUnix) DateTimeRFC1123 {
        const date_time_utc = instant.date_time();
        assert(date_time_utc.month >= 1);
        assert(date_time_utc.month <= 12);

        // 1970-01-01 was a Thursday (= index 3 when Monday = index 0).
        const epoch_day = @divTrunc(instant.ns, std.time.ns_per_s * std.time.s_per_day);
        return .{
            .weekday_index = @intCast((epoch_day + 3) % 7),
            .year = date_time_utc.year,
            .month_index = date_time_utc.month - 1,
            .day = date_time_utc.day,
            .hour = date_time_utc.hour,
            .minute = date_time_utc.minute,
            .second = date_time_utc.second,
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
    const tigerbeetle_birthday = InstantUnix.from_seconds(1661676577);

    const expectFmt = std.testing.expectFmT;
    try expectFmt("2022-08-28 08:49:37.000Z", "{f}", .{
        tigerbeetle_birthday.date_time(),
    });
    try expectFmt("20220828T084937Z", "{f}", .{
        tigerbeetle_birthday.date_time_iso8601_basic(),
    });
    try expectFmt("20220828", "{f}", .{
        tigerbeetle_birthday.date_iso8601_basic(),
    });
    try expectFmt("Sun, 28 Aug 2022 08:49:37 GMT", "{f}", .{
        tigerbeetle_birthday.date_time_rfc1123(),
    });

    const instant_min = InstantUnix{ .ns = 0 };
    try expectFmt("1970-01-01 00:00:00.000Z", "{f}", .{instant_min.date_time()});
    try expectFmt("19700101T000000Z", "{f}", .{instant_min.date_time_iso8601_basic()});
    try expectFmt("19700101", "{f}", .{instant_min.date_iso8601_basic()});
    try expectFmt("Thu, 01 Jan 1970 00:00:00 GMT", "{f}", .{
        instant_min.date_time_rfc1123(),
    });

    const instant_max = InstantUnix{ .ns = std.math.maxInt(u64) };
    try expectFmt("2554-07-21 23:34:33.709Z", "{f}", .{instant_max.date_time()});
    try expectFmt("25540721T233433Z", "{f}", .{instant_max.date_time_iso8601_basic()});
    try expectFmt("25540721", "{f}", .{instant_max.date_iso8601_basic()});
    try expectFmt("Sun, 21 Jul 2554 23:34:33 GMT", "{f}", .{
        instant_max.date_time_rfc1123(),
    });
}

test "InstantUnix formats fuzz" {
    var prng = stdx.PRNG.from_seed_testing();

    const Context = struct {
        fn check(ns: u64) anyerror!void {
            const instant: InstantUnix = .{ .ns = ns };

            const date_time = instant.date_time();
            try std.testing.expect(date_time.year >= 1970);
            try std.testing.expect(date_time.year <= 2554);
            try std.testing.expect(date_time.month >= 1 and date_time.month <= 12);
            try std.testing.expect(date_time.day >= 1 and date_time.day <= 31);
            try std.testing.expect(date_time.hour < 24);
            try std.testing.expect(date_time.minute < 60);
            try std.testing.expect(date_time.second < 60);
            try std.testing.expect(date_time.millisecond < 1000);

            var utc_buffer: [24]u8 = undefined;
            const utc = try std.fmt.bufPrint(&utc_buffer, "{}", .{date_time});
            try std.testing.expectEqual(utc_buffer.len, utc.len);

            var iso8601_basic_buffer: [16]u8 = undefined;
            const iso8601_basic = try std.fmt.bufPrint(
                &iso8601_basic_buffer,
                "{}",
                .{instant.date_time_iso8601_basic()},
            );
            try std.testing.expectEqual(iso8601_basic_buffer.len, iso8601_basic.len);

            var date_buffer: [8]u8 = undefined;
            const date = try std.fmt.bufPrint(&date_buffer, "{}", .{instant.date_iso8601_basic()});
            try std.testing.expectEqual(date_buffer.len, date.len);

            var rfc1123_buffer: [29]u8 = undefined;
            const rfc1123 = try std.fmt.bufPrint(
                &rfc1123_buffer,
                "{}",
                .{instant.date_time_rfc1123()},
            );
            try std.testing.expectEqual(rfc1123_buffer.len, rfc1123.len);

            try std.testing.expectStringStartsWith(iso8601_basic, date);
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
