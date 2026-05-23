const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;

pub const RatchetDirection = enum {
    above,
    below,
    equal,

    fn operator_char(direction: RatchetDirection) u8 {
        return switch(direction)  {
            .above => '>',
            .below => '<',
            .equal => '=',
        };
    }

    fn from_char(char: u8) RatchetDirection {
        return switch (char) {
            '>' => .above,
            '<' => .below,
            '=' => .equal,
            else => @panic("unknown ratchet direction operator")
        };
    }
};

pub const Metric = struct {
    name: []const u8,
    direction: RatchetDirection,
    bound: f64,
    epsilon: ?f64 = null,

    pub fn below(name: []const u8, metric: f64) Metric {
        return .{.name = name, .direction = .below, .bound = metric};
    }

    pub fn above(name: []const u8, metric: f64) Metric {
        return .{.name = name, .direction = .above, .bound = metric};
    }

    pub fn equal(name: []const u8, metric: f64) Metric {
        return .{.name = name, .direction = .equal, .bound = metric};
    }

    fn print(spec: *const Metric, writer: anytype) !void {
        const operator = spec.direction.operator_char();
        if (spec.epsilon) |epsilon| {
            try writer.print("{s}{c}[{d}]{d}", . { spec.name, operator, epsilon, spec.bound });
        } else {
            try writer.print("{s}{c}{d}", .{ spec.name, operator, spec.bound });
        }
    }

    fn sorted_by_name(_: void, lhs: Metric, rhs: Metric) bool {
        return std.mem.order(u8, lhs.name, rhs.name) != .gt;
    }
};

pub fn ratchet_write(
    writer: anytype,
    comptime ratchet_name: []const u8,
    comptime fmt: []const u8,
    params: anytype, 
    ratchet_specs: []const Metric
) !void {
    assert(ratchet_specs.len > 0);
    assert(std.sort.isSorted(Metric, ratchet_specs, {}, Metric.sorted_by_name));
    maybe(std.meta.fields(@TypeOf(params)).len == 0);
    // we use a custom format (instead of json/zon) to reduce
    // noise in the output and improve readability for humans
    const separator = if (fmt.len == 0) "//" else " //";
    try writer.print("{s} // " ++ fmt ++ separator, .{ratchet_name} ++ params);
    for (ratchet_specs) |ratchet| {
        try writer.print(" ", .{});
        try ratchet.print(writer);
    }
    try writer.print("\n", .{});
}

test "ratchet_write accepts empty parameters" {
    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    try ratchet_write(output.writer(), @src().fn_name, "", .{}, &.{
        .equal("checksum", 100),
        .below("time_ms", 5.5)
    });

    try std.testing.expectEqualStrings(
        "test.ratchet_write accepts empty parameters // // checksum=100 time_ms<5.5\n",
        output.getWritten(),
    );
}

test "ratchet_write prints parameters" {
    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    try ratchet_write(
        output.writer(),
        @src().fn_name,
        "{s} factor={d}",
        .{ "random", 3.14 },
        &.{ .{ .name = "time_s", .direction = .below, .bound = 3.9, .epsilon = 0.03 } });

    try std.testing.expectEqualStrings(
        "test.ratchet_write prints parameters " ++
            "// random factor=3.14 // time_s<[0.03]3.9\n",
        output.getWritten(),
    );
}

const RatchetLine = struct {
    bench_name: []const u8,
    params: []const u8,
    metrics: []const u8,

    const RatchetKey = struct {
        bench_name: []const u8,
        params: []const u8,
    };

    const RatchetMetricData = struct {
        metrics: []const u8,
        matched: bool = false,
    };

    const RatchetKeyContext = struct {
        pub fn hash(_: RatchetKeyContext, key: RatchetKey) u64 {
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(key.bench_name);
            hasher.update("\x00");
            hasher.update(key.params);
            return hasher.final();
        }

        pub fn eql(_: RatchetKeyContext, a: RatchetKey, b: RatchetKey) bool {
            return std.mem.eql(u8, a.bench_name, b.bench_name) and
                std.mem.eql(u8, a.params, b.params);
        }
    };
};

fn read_ratchet_line(line: []const u8) struct { RatchetLine, []const u8 } {
    const separator = "//";
    const bench_name, var tail = stdx.cut(line, separator).?;
    const params, tail = stdx.cut(tail, separator).?;
    const metrics, tail = stdx.cut(tail, "\n") orelse .{ tail, "" };
    // include newline in the tail
    if (tail.len > 0) {
        tail.ptr -= 1;
        tail.len += 1; 
    }
    const ratchet_line: RatchetLine = .{
        .bench_name = std.mem.trim(u8, bench_name, " "),
        .params = std.mem.trim(u8, params, " "),
        .metrics = std.mem.trim(u8, metrics, " "),
    };
    return .{ ratchet_line, tail };
}

test read_ratchet_line {
    const line = "benchmark: test // params // some data \nnext line\n";
    const ratchet, const next_line = read_ratchet_line(line);
    try std.testing.expectEqualStrings(ratchet.bench_name, "benchmark: test");
    try std.testing.expectEqualStrings(ratchet.params, "params");
    try std.testing.expectEqualStrings(ratchet.metrics, "some data");
    try std.testing.expectEqualStrings(next_line, "\nnext line\n");
}

/// Searches ratchet log lines in both logs, reporting cases where
/// - baseline_metric * (1 + epsilon) < worktree_metric,
/// - worktree removes metrics
/// - worktree removes ratchets
/// Returns the number of failing cases, and outputs them to failing_writer
pub fn report_regressions(
    comptime marker: []const u8,
    gpa: std.mem.Allocator,
    input_baseline: []const u8,
    input_worktree: []const u8,
    failing_writer: anytype, // std writer
    options: struct {
        report_gone_ratchets: bool = true,
        report_gone_metrics: bool = true,
        epsilon_percent: f64 = 3.0,
    },
) u32 {
    const Map = std.HashMap(
        RatchetLine.RatchetKey,
        RatchetLine.RatchetMetricData,
        RatchetLine.RatchetKeyContext,
        std.hash_map.default_max_load_percentage,
    );

    var worktree_ratchets = Map.init(gpa);
    defer worktree_ratchets.deinit();

    var worktree_tail = input_worktree;
    while (next_ratchet_line(marker, &worktree_tail)) |ratchet_line| {
        const result = worktree_ratchets.getOrPut(.{
            .bench_name = ratchet_line.bench_name,
            .params = ratchet_line.params,
        }) catch unreachable;
        result.value_ptr.* = .{ .metrics = ratchet_line.metrics };
    }

    var failing_counter: u32 = 0;
    var baseline_tail = input_baseline;
    while (next_ratchet_line(marker, &baseline_tail)) |baseline| {
        const worktree = worktree_ratchets.getPtr(.{
            .bench_name = baseline.bench_name,
            .params = baseline.params,
        }) orelse {
            if (options.report_gone_ratchets) {
                failing_writer.print("ratchet gone: {s} // {s}\n", .{
                    baseline.bench_name,
                    baseline.params,
                }) catch unreachable;
                failing_counter += 1;
            }
            continue;
        };
        if (worktree.matched) {
            failing_writer.print("duplicate ratchet: {s} // {s}\n", .{
                baseline.bench_name,
                baseline.params
            }) catch unreachable;
            failing_counter += 1;
            continue;
        }
        worktree.matched = true;

        var baseline_metrics = baseline.metrics;
        while (next_metric(&baseline_metrics)) |baseline_metric| {
            const worktree_metric = find_metric(
                worktree.metrics,
                baseline_metric.name,
            ) orelse {
                if (options.report_gone_metrics) {
                    failing_writer.print("metric gone: {s} // {s} // {s}\n", .{
                        baseline.bench_name,
                        baseline.params,
                        baseline_metric.name,
                    }) catch unreachable;
                    failing_counter += 1;
                }
                continue;
            };

            if (baseline_metric.direction == .equal) continue;

            const epsilon_percent = if (worktree_metric.epsilon) |epsilon|
                epsilon * 100.0
            else
                options.epsilon_percent;
            const metric_epsilon_factor = 1.0 + epsilon_percent / 100.0;
            const max_allowed = baseline_metric.bound * metric_epsilon_factor;
            if (worktree_metric.bound > max_allowed) {
                const worse_percent = 100.0 * (
                    worktree_metric.bound / baseline_metric.bound - 1
                ); 
                failing_writer.print(
                    "regression: {s} // {s} // {s}: " ++
                        "baseline={d:.2} worktree={d:.2} epsilon={d:.2}% " ++
                        "worse_by={d:.2}%\n",
                    .{
                        baseline.bench_name,
                        baseline.params,
                        baseline_metric.name,
                        baseline_metric.bound,
                        worktree_metric.bound,
                        epsilon_percent,
                        worse_percent,
                    },
                ) catch unreachable;
                failing_counter += 1;
            }
        }
    }

    return failing_counter;
}

fn next_ratchet_line(
    comptime marker: []const u8,
    tail: *[]const u8,
) ?RatchetLine {
    const head, tail.* = stdx.cut(tail.*, marker) orelse return null;
    assert(head.len == 0 or head[head.len - 1] == '\n');
    const ratchet, tail.* = read_ratchet_line(tail.*);
    return ratchet;
}

fn find_metric(metrics: []const u8, name: []const u8) ?Metric {
    var tail = metrics;
    while (next_metric(&tail)) |metric| {
        if (std.mem.eql(u8, metric.name, name)) return metric;
    }
    return null;
}

fn next_metric(tail: *[]const u8) ?Metric {
    tail.* = std.mem.trimLeft(u8, tail.*, " ");
    if (tail.*.len == 0) return null;

    const token, tail.* = stdx.cut(tail.*, " ") orelse .{ tail.*, "" };
    return parse_metric(token) catch unreachable;
}

fn parse_metric(token: []const u8) !Metric {
    assert(token.len > 0);
    assert(std.ascii.isAlphabetic(token[0]));

    const operator_index = std.mem.indexOfAny(u8, token, "<>=") orelse return error.InvalidMetric;
    assert(operator_index > 0);
    assert(operator_index + 1 < token.len);

    var text_value = token[operator_index + 1..];
    var epsilon: ?f64 = null;

    if (text_value[0] == '[') {
        const text_epsilon, text_value = stdx.cut(text_value[1..], "]")
            orelse return error.InvalidMetric;
        assert(text_epsilon.len > 0);
        assert(text_value.len > 0);
        epsilon = try std.fmt.parseFloat(f64, text_epsilon);
    }

    assert(text_value.len > 0);
    assert(std.ascii.isDigit(text_value[0]));
    return .{
        .name = token[0..operator_index],
        .direction = RatchetDirection.from_char(token[operator_index]),
        .bound = try std.fmt.parseFloat(f64, text_value),
        .epsilon = epsilon,
    };
}

test "next_metric: fuzz" {
    const names = [_][]const u8{
        "checksum",
        "elapsed_ns",
        "iops",
        "latency_ms",
        "latency_p100",
        "p50",
        "time_s",
    };
    const directions = std.enums.values(RatchetDirection);

    var prng = stdx.PRNG.from_seed_testing();
    var metrics: [16]Metric = undefined;
    var text_buffer: [4096]u8 = undefined;

    for (0..1000) |_| {
        const metrics_count = prng.range_inclusive(usize, 1, metrics.len);
        var text = std.io.fixedBufferStream(&text_buffer);

        for (metrics[0..metrics_count]) |*metric| {
            metric.* = .{
                .name = names[prng.index(&names)],
                .direction = directions[prng.index(directions)],
                .bound = @floatFromInt(prng.range_inclusive(u64, 1, 1_000_000)),
                .epsilon = null,
            };
            if (prng.boolean()) {
                const epsilon_bps = prng.range_inclusive(u64, 1, 10_000);
                metric.epsilon = @as(f64, @floatFromInt(epsilon_bps)) / 10_000.0;
            }

            try metric.print(text.writer());
            const spaces = prng.range_inclusive(usize, 1, 3);
            for (0..spaces) |_| try text.writer().writeByte(' ');
        }

        const metrics_text = text.getWritten();
        var tail: []const u8 = metrics_text;
        for (metrics[0..metrics_count]) |expected| {
            const parsed = next_metric(&tail).?;
            try expect_metric_equal(expected, parsed);
        }
        try std.testing.expect(next_metric(&tail) == null);
    }
}

fn expect_metric_equal(expected: Metric, actual: Metric) !void {
    try std.testing.expectEqualStrings(expected.name, actual.name);
    try std.testing.expectEqual(expected.direction, actual.direction);
    try std.testing.expectEqual(expected.bound, actual.bound);

    if (expected.epsilon) |expected_epsilon| {
        try std.testing.expect(actual.epsilon != null);
        try std.testing.expectEqual(expected_epsilon, actual.epsilon.?);
    } else {
        try std.testing.expect(actual.epsilon == null);
    }
}

test "report_regressions reports regressions" {
    const baseline =
        \\RATCHET bench_a // size=10 // checksum=1 elapsed_ns<100.00
        \\RATCHET bench_b // // elapsed_ns=200 
        \\
    ;
    const worktree =
        \\noise before
        \\RATCHET bench_a // size=10 // checksum=2 elapsed_ns<[0.05]110.00
        \\RATCHET bench_b // // elapsed_ns=206 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
        "RATCHET",
        std.testing.allocator,
        baseline,
        worktree,
        output.writer(),
        .{ .epsilon_percent = 3.0 },
    );

    try std.testing.expectEqual(@as(u32, 1), failures);
    try std.testing.expectEqualStrings(
        "regression: bench_a // size=10 // elapsed_ns: " ++
            "baseline=100.00 worktree=110.00 epsilon=5.00% worse_by=10.00%\n",
        output.getWritten(),
    );
}

test "report_regressions gone ratchets and gone metrics" {
    const baseline =
        \\RATCHET bench_a // mode=fast // elapsed_ns<100 iops<1000 
        \\RATCHET bench_b // mode=slow // elapsed_ns<200 
        \\
    ;
    const worktree =
        \\RATCHET bench_a // mode=fast // elapsed_ns<99 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
        "RATCHET",
        std.testing.allocator,
        baseline,
        worktree,
        output.writer(),
        .{},
    );

    try std.testing.expectEqual(@as(u32, 2), failures);
    try std.testing.expectEqualStrings(
        "metric gone: bench_a // mode=fast // iops\n" ++
            "ratchet gone: bench_b // mode=slow\n",
        output.getWritten(),
    );
}

test "report_regressions reports duplicate baselines" {
    const baseline =
        \\RATCHET bench // mode=fast // elapsed_ns<100 
        \\RATCHET bench // mode=fast // elapsed_ns<100 
        \\
    ;
    const worktree =
        \\RATCHET bench // mode=fast // elapsed_ns<100 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
        "RATCHET",
        std.testing.allocator,
        baseline,
        worktree,
        output.writer(),
        .{},
    );

    try std.testing.expectEqual(@as(u32, 1), failures);
    try std.testing.expectEqualStrings(
        "duplicate ratchet: bench // mode=fast\n",
        output.getWritten(),
    );
}

test "report_regressions can ignore removals" {
    const baseline =
        \\RATCHET bench // // elapsed_ns<100 iops>[0.05]1000 
        \\RATCHET removed // // elapsed_ns<1 
        \\
    ;
    const worktree =
        \\RATCHET bench // // elapsed_ns<102 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
        "RATCHET",
        std.testing.allocator,
        baseline,
        worktree,
        output.writer(),
        .{
            .report_gone_ratchets = false,
            .report_gone_metrics = false,
            .epsilon_percent = 1.0,
        },
    );

    try std.testing.expectEqual(@as(u32, 1), failures);
    try std.testing.expectEqualStrings(
        "regression: bench //  // elapsed_ns: " ++
            "baseline=100.00 worktree=102.00 epsilon=1.00% worse_by=2.00%\n",
        output.getWritten(),
    );
}
