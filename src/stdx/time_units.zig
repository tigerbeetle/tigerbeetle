const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx.zig");

/// A moment in time not anchored to any particular epoch.
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

    pub fn parse_flag_value(string: []const u8) union(enum) { ok: Duration, err: []const u8 } {
        if (string.len == 0) return .{ .err = "expected a duration, but found nothing" };

        var duration_ns: u64 = 0;

        var string_remaining = string;
        while (string_remaining.len > 0) {
            const value_size = for (string_remaining, 0..) |character, i| {
                if (!std.ascii.isDigit(character)) break i;
            } else return .{ .err = "missing unit; must be one of: d/h/m/s/ms/us/ns" };
            if (value_size == 0) return .{ .err = "missing value" };

            const value = std.fmt.parseInt(u64, string_remaining[0..value_size], 10) catch |err| {
                switch (err) {
                    error.Overflow => return .{ .err = "integer overflow" },
                    error.InvalidCharacter => unreachable,
                }
            };

            for ([_]struct { ns: u64, label: []const u8 }{
                .{ .ns = 1, .label = "ns" },
                .{ .ns = std.time.ns_per_us, .label = "us" },
                .{ .ns = std.time.ns_per_ms, .label = "ms" },
                .{ .ns = std.time.ns_per_s, .label = "s" },
                .{ .ns = std.time.ns_per_min, .label = "m" },
                .{ .ns = std.time.ns_per_hour, .label = "h" },
                .{ .ns = std.time.ns_per_day, .label = "d" },
            }) |unit| {
                if (stdx.cut_prefix(string_remaining[value_size..], unit.label)) |suffix| {
                    duration_ns +|= unit.ns *| value;
                    string_remaining = suffix;
                    break;
                }
            } else {
                return .{ .err = "unknown unit; must be one of: d/h/m/s/ms/us/ns" };
            }
        }
        if (duration_ns >= 1_000 * std.time.ns_per_day) {
            return .{ .err = "duration too large" };
        }
        return .{ .ok = .{ .ns = duration_ns } };
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
    for ([_]struct { []const u8, u64 }{
        .{ "1h", std.time.ns_per_hour },
        .{ "1m", std.time.ns_per_min },
        .{ "1h2m", std.time.ns_per_hour + 2 * std.time.ns_per_min },
        .{ "1ms2us3ns", std.time.ns_per_ms + 2 * std.time.ns_per_us + 3 },
    }) |pair| {
        try std.testing.expectEqual(Duration.parse_flag_value(pair.@"0").ok.ns, pair.@"1");
    }

    for ([_][]const u8{
        "",
        "h",
        "1",
        "h1",
        "1H",
        "1h2x",
        "1h 2m",
        "18446744073709551616ns",
        "1844674407370955161s",
    }) |string| {
        try std.testing.expect(Duration.parse_flag_value(string) == .err);
    }
}

test "Duration.parse_flag_value fuzz" {
    const test_count = 1024;
    const input_size_max = 32;
    const alphabet = " \t\n.-e[]0123456789abcdhmuns";

    var prng = stdx.PRNG.from_seed_testing();

    var input_buffer: [input_size_max]u8 = @splat(0);
    for (0..test_count) |_| {
        const input_size = prng.int_inclusive(usize, input_size_max);
        const input = input_buffer[0..input_size];
        for (input) |*c| {
            c.* = alphabet[prng.index(alphabet)];
        }

        const result = Duration.parse_flag_value(input);
        switch (result) {
            .ok => |duration| {
                var buffer: [64]u8 = undefined;
                _ = std.fmt.bufPrint(&buffer, "{}", .{duration}) catch unreachable;
                // Round-trip not guaranteed.
            },
            .err => {},
        }
    }
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
