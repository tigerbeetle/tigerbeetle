const std = @import("std");
const StructField = std.builtin.Type.StructField;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;


pub const RatchetDirection = union(enum) {
    above,
    below,
    exact,
    above_epsilon: f64,
    below_epsilon: f64,
    exact_epsilon: f64,

    fn as_string(direction: RatchetDirection) []const u8 {
        return switch(direction)  {
            .above => ">",
            .below => "<",
            .exact => "=",
            // TODO
            .above_epsilon => ">",
            .below_epsilon => "<",
            .exact_epsilon => "=",
        };
    }
};

pub const RatchetSpec = struct {
    []const u8,
    RatchetDirection,
    f64,
};


pub fn ratchet_write(
    writer: anytype,
    comptime ratchet_name: []const u8,
    comptime fmt: []const u8,
    params: anytype, 
    ratchet_specs: []RatchetSpec
) !void {
    assert(std.meta.fields(@TypeOf(ratchet_specs)).len > 0);
    maybe(std.meta.fields(@TypeOf(params)).len == 0);
    // we use a custom format (instead of json/zon) to reduce
    // noise in the output and improve readability for humans
    try writer.print("{s} //" ++ fmt ++ " // ", .{ratchet_name} ++ params);
    const SortContext = struct {
        fn less_than(_: @This(), lhs: *const RatchetSpec, rhs: *const RatchetSpec) bool {
            return std.mem.order(lhs[0], rhs[1]) != .gt;
        }
    };
    std.mem.sort(RatchetSpec, ratchet_specs, .{}, SortContext.less_than);
    for (&ratchet_specs) |ratchet| {
        try writer.print(" {s}{s}{d:.2}\n", .{ ratchet[0], ratchet[1].as_string(), ratchet[2] });
    }
}

test "ratchet_write accepts empty parameters" {
    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    ratchet_write(
        @src().fn_name,
        output.writer(),
        .{},
        .{ .time_ms = 10, .checksum = 1.61 },
    ) catch unreachable;

    try std.testing.expectEqualStrings(
        "test.ratchet_write accepts empty parameters // // checksum=1.61 time_ms=10 \n",
        output.getWritten(),
    );
}

test "ratchet_write prints parameters" {
    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    try ratchet_write(
        @src().fn_name,
        output.writer(),
        .{ .factor = 50.3, .strategy = "random" },
        .{ .time_s = 3.9 },
    );

    try std.testing.expectEqualStrings(
        "test.ratchet_write prints parameters " ++
            "// factor=50.30 strategy=random // time_s=3.90 \n",
        output.getWritten(),
    );
}

fn lexicographic_fields(comptime T: type) [std.meta.fields(T).len]StructField {
    return comptime blk: {
        assert(@typeInfo(T) == .@"struct");
        var fields = std.meta.fields(T)[0..].*;
        if (fields.len == 0) break :blk fields;
        for (1..fields.len) |i| {
            var j = i;
            while (j > 0) : (j -= 1) {
                const left = &fields[j - 1];
                const right = &fields[j];
                if (std.mem.order(u8, left.name, right.name) != .gt) break;
                std.mem.swap(StructField, left, right);
            }
        }
        break :blk fields;
    };
}

test lexicographic_fields {
    // structs
    const StructType = struct { ba: u8, aa: u8, zz: u128, a: i16 };
    const expected_struct = [_][]const u8{ "a", "aa", "ba", "zz" };
    inline for (comptime lexicographic_fields(StructType), 0..) |field, i| {
        try std.testing.expectEqualStrings(expected_struct[i], field.name);
    }
    // tuples
    const expected_tuple = [_][]const u8{ "0", "1", "2" };
    const TupleType = @TypeOf(.{ 'a', 'b', 'c' });
    inline for (comptime lexicographic_fields(TupleType), 0..) |field, i| {
        try std.testing.expectEqualStrings(expected_tuple[i], field.name);
    }
    // empty
    const empty_fields = comptime lexicographic_fields(@TypeOf(.{}));
    try std.testing.expectEqual(empty_fields.len, 0);
}

fn format_string_from_type(comptime T: type) []const u8 {
    const info = @typeInfo(T);
    return switch (info) {
        .array => |value| {
            assert(value.child == u8);
            return "{s}";
        },
        .pointer => |value| {
            assert(value.child == u8 or (@typeInfo(value.child) == .array and
                @typeInfo(value.child).array.child == u8));
            return "{s}";
        },
        .bool => "{}",
        .int, .comptime_int => "{d}",
        .float, .comptime_float => "{d:.2}",
        else => {
            @panic("invalid ratchet type");
        },
    };
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
    const epsilon_factor = (1.0 + options.epsilon_percent / 100.0);
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

            const max_allowed = baseline_metric.value * epsilon_factor;
            if (worktree_metric.value > max_allowed) {
                const worse_percent = 100.0 * (
                    worktree_metric.value / baseline_metric.value - 1
                ); 
                failing_writer.print(
                    "regression: {s} // {s} // {s}: " ++
                        "baseline={d:.2} worktree={d:.2} epsilon={d:.2}% " ++
                        "worse_by={d:.2}%\n",
                    .{
                        baseline.bench_name,
                        baseline.params,
                        baseline_metric.name,
                        baseline_metric.value,
                        worktree_metric.value,
                        options.epsilon_percent,
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

const Metric = struct {
    name: []const u8,
    value: f64,
};

fn next_metric(tail: *[]const u8) ?Metric {
    while (tail.len > 0) {
        const token, tail.* = stdx.cut(tail.*, " ") orelse .{ tail.*, "" };
        if (token.len == 0) continue;
        const name, const value_text = stdx.cut(token, "=") orelse continue;
        const value = std.fmt.parseFloat(f64, value_text) catch continue;
        return .{ .name = name, .value = value };
    }
    return null;
}

fn find_metric(metrics: []const u8, name: []const u8) ?Metric {
    var tail = metrics;
    while (next_metric(&tail)) |metric| {
        if (std.mem.eql(u8, metric.name, name)) return metric;
    }
    return null;
}

test "report_regressions reports regressions" {
    const baseline =
        \\RATCHET bench_a // size=10 // checksum=1 elapsed_ns=100 
        \\RATCHET bench_b // // elapsed_ns=200 
        \\
    ;
    const worktree =
        \\noise before
        \\RATCHET bench_a // size=10 // checksum=1 elapsed_ns=104 
        \\RATCHET bench_b // // elapsed_ns=206 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
        std.testing.allocator,
        baseline,
        worktree,
        output.writer(),
        .{ .epsilon_percent = 3.0 },
    );

    try std.testing.expectEqual(@as(u32, 1), failures);
    try std.testing.expectEqualStrings(
        "regression: bench_a // size=10 // elapsed_ns: " ++
            "baseline=100.00 worktree=104.00 epsilon=3.00% worse_by=4.00%\n",
        output.getWritten(),
    );
}

test "report_regressions gone ratchets and gone metrics" {
    const baseline =
        \\RATCHET bench_a // mode=fast // elapsed_ns=100 iops=1000 
        \\RATCHET bench_b // mode=slow // elapsed_ns=200 
        \\
    ;
    const worktree =
        \\RATCHET bench_a // mode=fast // elapsed_ns=99 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
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
        \\RATCHET bench // mode=fast // elapsed_ns=100 
        \\RATCHET bench // mode=fast // elapsed_ns=100 
        \\
    ;
    const worktree =
        \\RATCHET bench // mode=fast // elapsed_ns=100 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
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
        \\RATCHET bench // // elapsed_ns=100 iops=1000 
        \\RATCHET removed // // elapsed_ns=1 
        \\
    ;
    const worktree =
        \\RATCHET bench // // elapsed_ns=102 
        \\
    ;

    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    const failures = report_regressions(
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
